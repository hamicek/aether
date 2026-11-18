package ingress

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// TraceHeader is the response header carrying the trace id of the request. Every response has it,
// including the ones the ingress produces itself, so a report from a browser ("it failed, here is
// the header") leads straight to the log lines and the bus messages of that one request.
const TraceHeader = "X-Aether-Trace"

// defaultMaxBodyBytes caps a forwarded request body so a single request cannot buffer unbounded
// memory into the ether. The demo value is generous; a real deployment tunes it per site.
const defaultMaxBodyBytes = 10 << 20 // 10 MiB

// DefaultTimeout bounds how long the ingress waits for a web thrall's reply before returning 504.
const DefaultTimeout = 5 * time.Second

// Options configure a Handler. App is the ether app the target thralls live in (the call subject is
// aether.<app>.<thrall>.call). Timeout, MaxBodyBytes and Log fall back to sane defaults when zero.
type Options struct {
	App          string
	Timeout      time.Duration
	MaxBodyBytes int64
	Log          *slog.Logger
}

// handler is the ingress HTTP handler: resolve Host -> thrall, forward as an http_request call, and
// write the http_response back.
type handler struct {
	client  *thrall.Client
	routes  RouteProvider
	timeout time.Duration
	maxBody int64
	log     *slog.Logger
}

// NewHandler builds the ingress HTTP handler over a NATS connection and a route provider. It owns no
// socket - the caller serves it (the edge's http.Server, or httptest in a test).
func NewHandler(nc *nats.Conn, routes RouteProvider, opts Options) http.Handler {
	h := &handler{
		client:  thrall.NewClient(nc, opts.App),
		routes:  routes,
		timeout: opts.Timeout,
		maxBody: opts.MaxBodyBytes,
		log:     opts.Log,
	}
	if h.timeout <= 0 {
		h.timeout = DefaultTimeout
	}
	if h.maxBody <= 0 {
		h.maxBody = defaultMaxBodyBytes
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	return h
}

// failure is a non-success the ingress itself answers with, as opposed to a response a web thrall
// produced. cause is a short stable token for logs and dashboards; err carries the detail.
type failure struct {
	status int
	cause  string
	err    error
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The ingress is the edge that starts the operation, so it mints the trace: the web thrall's
	// handler sees it as ctx.Trace and passes it on to whatever it calls.
	trace := thrall.NewTrace()
	w.Header().Set(TraceHeader, trace)

	target, ok := h.routes.Resolve(r.Host)
	if !ok {
		// Default-deny: a host with no explicit route does not reach the ether at all.
		h.fail(w, r, trace, "", failure{status: http.StatusNotFound, cause: "no_route"})
		return
	}

	req, err := requestToWire(r, h.maxBody)
	if err != nil {
		h.fail(w, r, trace, target, failure{status: http.StatusBadRequest, cause: "bad_request", err: err})
		return
	}

	// The call goes through the SDK client, so it is the same envelope a thrall's own call
	// produces: it carries the trace and the instant the ingress stops waiting, and a thrall that
	// only gets to it after that drops it instead of doing work nobody is waiting for.
	resp, err := thrall.CallInto[wire.HTTPResponse](h.client, target, wire.OpHTTPRequest, req, h.timeout, thrall.WithTrace(trace))
	if err != nil {
		h.fail(w, r, trace, target, failureForCall(err))
		return
	}
	if !forwardableStatus(resp.Status) {
		// A reply that carries no status, or one HTTP cannot express, is an upstream fault - not
		// something to forward as a 200 (see AE-091 review) or to hand to the HTTP server, which
		// rejects a status outside its range by panicking.
		h.fail(w, r, trace, target, failure{status: http.StatusBadGateway, cause: "invalid_reply",
			err: fmt.Errorf("http_response status %d is not a final HTTP status", resp.Status)})
		return
	}
	writeResponse(w, resp, trace)
}

// fail answers a request the ingress could not forward and says why in the log.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, trace, target string, f failure) {
	attrs := []any{
		slog.Int("status", f.status), slog.String("cause", f.cause),
		slog.String("host", r.Host), slog.String("trace", trace),
	}
	if target != "" {
		attrs = append(attrs, slog.String("thrall", target))
	}
	if f.err != nil {
		attrs = append(attrs, slog.Any("err", f.err))
	}
	h.log.Warn("ingress request failed", attrs...)
	http.Error(w, http.StatusText(f.status), f.status)
}

// failureForCall maps a failed call to the web thrall to what the HTTP client is told:
//
//   - nobody subscribed (the thrall is not running) -> 503;
//   - no reply within the timeout -> 504, and so is a call the thrall dropped because its deadline
//     had passed: that is the same timeout seen from the thrall's side;
//   - any other error reply -> 502. A refusal included: a web thrall answers its own "no" as an
//     HTTP response, so an error reply here is the thrall failing, whatever its type.
//   - anything else (an undecodable reply) -> 502.
func failureForCall(err error) failure {
	var ce *thrall.CallError
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return failure{status: http.StatusServiceUnavailable, cause: "thrall_unavailable", err: err}
	case errors.Is(err, nats.ErrTimeout):
		return failure{status: http.StatusGatewayTimeout, cause: "thrall_timeout", err: err}
	case errors.As(err, &ce) && ce.Type == thrall.ErrDeadlineExceeded:
		return failure{status: http.StatusGatewayTimeout, cause: "thrall_timeout", err: err}
	case errors.As(err, &ce):
		return failure{status: http.StatusBadGateway, cause: "thrall_error", err: err}
	default:
		return failure{status: http.StatusBadGateway, cause: "invalid_reply", err: err}
	}
}

// forwardableStatus reports whether a web thrall's reply status can be written as the final status
// of an HTTP response. Informational (1xx) statuses are not final, 0 means the reply carried none.
func forwardableStatus(status int) bool { return status >= 200 && status <= 599 }

// requestToWire translates an inbound *http.Request into the http_request payload: method, path, raw
// query, headers and a size-capped body. http.Header is already a map[string][]string, so repeated
// headers are preserved.
func requestToWire(r *http.Request, maxBody int64) (wire.HTTPRequest, error) {
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
		if err != nil {
			return wire.HTTPRequest{}, err
		}
		body = b
	}
	return wire.HTTPRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Query:   r.URL.RawQuery,
		Headers: map[string][]string(r.Header),
		Body:    body,
	}, nil
}

// writeResponse writes an http_response back to the client: headers (each value preserved, so a
// repeated Set-Cookie survives), then the status, then the body. The trace header is the ingress's
// to state, so it is set last and wins over anything the thrall put under the same name.
func writeResponse(w http.ResponseWriter, resp wire.HTTPResponse, trace string) {
	header := w.Header()
	for key, values := range resp.Headers {
		for _, v := range values {
			header.Add(key, v)
		}
	}
	header.Set(TraceHeader, trace)
	w.WriteHeader(resp.Status)
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}
