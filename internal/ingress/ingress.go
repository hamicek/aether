package ingress

import (
	"errors"
	"fmt"
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

// Options configure a Handler. App is the ether app the target thralls live in (the call subject is
// aether.<app>.<thrall>.call). The limits fall back to the Default* constants when zero.
type Options struct {
	App string
	// Timeout is how long the ingress waits for a web thrall's reply (then 504); the call carries
	// it as its deadline.
	Timeout time.Duration
	// MaxBodyBytes is the largest request body accepted (then 413). It must not exceed what one bus
	// message carries; zero means BodyLimit for a bus with the NATS default max payload. Run derives
	// it from the live connection.
	MaxBodyBytes int64
	// WriteTimeout bounds writing a response once the ingress has it.
	WriteTimeout time.Duration
	// TrustedProxies are the peers whose forwarding headers are believed when establishing the
	// client address and scheme of a request. Nil trusts nobody; Run defaults to loopback.
	TrustedProxies TrustedProxies
	Log            *slog.Logger
}

// handler is the ingress HTTP handler: resolve Host -> thrall, forward as an http_request call, and
// write the http_response back.
type handler struct {
	client       *thrall.Client
	routes       RouteProvider
	timeout      time.Duration
	maxBody      int64
	writeTimeout time.Duration
	trusted      TrustedProxies
	log          *slog.Logger
}

// NewHandler builds the ingress HTTP handler over a NATS connection and a route provider. It owns no
// socket - the caller serves it (NewServer on a Listen listener, or httptest in a test).
func NewHandler(nc *nats.Conn, routes RouteProvider, opts Options) http.Handler {
	h := &handler{
		client:       thrall.NewClient(nc, opts.App),
		routes:       routes,
		timeout:      opts.Timeout,
		maxBody:      opts.MaxBodyBytes,
		writeTimeout: opts.WriteTimeout,
		trusted:      opts.TrustedProxies,
		log:          opts.Log,
	}
	if h.timeout <= 0 {
		h.timeout = DefaultTimeout
	}
	if h.maxBody <= 0 {
		h.maxBody = BodyLimit(natsDefaultMaxPayload, DefaultMaxHeaderBytes)
	}
	if h.writeTimeout <= 0 {
		h.writeTimeout = DefaultWriteTimeout
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	return h
}

// retryAfterSeconds is what a 503 tells the client to wait before trying again.
const retryAfterSeconds = "1"

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

	target, found, err := h.routes.Resolve(r.Host)
	if err != nil {
		// The route table is out of reach. That says nothing about whether the site exists, so it is
		// "try again", not "not found".
		h.fail(w, r, trace, "", failure{status: http.StatusServiceUnavailable, cause: "routes_unavailable", err: err})
		return
	}
	if !found {
		// Default-deny: a host with no explicit route does not reach the ether at all.
		h.fail(w, r, trace, "", failure{status: http.StatusNotFound, cause: "no_route"})
		return
	}

	// The limit is checked before anything is forwarded: the thrall never sees a part of a request.
	body, failed := readBody(w, r, h.maxBody)
	if failed != nil {
		h.fail(w, r, trace, target, *failed)
		return
	}
	req := requestToWire(r, body, h.trusted.facts(r))

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
	h.startWriting(w)
	writeResponse(w, resp, trace)
}

// startWriting starts the write clock: from here the client has the write timeout to take the
// response. A client that stops reading is cut off instead of holding the connection (and its slot
// under the connection cap) for good. A writer that cannot take a deadline (a test recorder) is
// left alone.
func (h *handler) startWriting(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(h.writeTimeout))
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
	if f.status == http.StatusServiceUnavailable {
		// Both 503s are passing states: a thrall being restarted, a registry being asked again.
		w.Header().Set("Retry-After", retryAfterSeconds)
	}
	h.startWriting(w)
	http.Error(w, http.StatusText(f.status), f.status)
}

// failureForCall maps a failed call to the web thrall to what the HTTP client is told:
//
//   - nobody subscribed (the thrall is not running) -> 503;
//   - no reply within the timeout -> 504, and so is a call the thrall dropped because its deadline
//     had passed: that is the same timeout seen from the thrall's side;
//   - a response too large for one bus message -> 502. The thrall produced it but could not send
//     it, and says so instead (a thrall on an SDK older than that answer drops the reply, and the
//     request ends as the 504 above);
//   - any other error reply -> 502. A refusal included: a web thrall answers its own "no" as an
//     HTTP response, so an error reply here is the thrall failing, whatever its type.
//   - anything else (an undecodable reply) -> 502.
//
// A request that passed the body limit and still did not fit one bus message (its headers took
// more room than the limit reserves for them) is a 413: nothing was sent.
func failureForCall(err error) failure {
	var ce *thrall.CallError
	switch {
	case errors.Is(err, nats.ErrMaxPayload):
		return failure{status: http.StatusRequestEntityTooLarge, cause: "request_too_large", err: err}
	case errors.Is(err, nats.ErrNoResponders):
		return failure{status: http.StatusServiceUnavailable, cause: "thrall_unavailable", err: err}
	case errors.Is(err, nats.ErrTimeout):
		return failure{status: http.StatusGatewayTimeout, cause: "thrall_timeout", err: err}
	case errors.As(err, &ce) && ce.Type == thrall.ErrDeadlineExceeded:
		return failure{status: http.StatusGatewayTimeout, cause: "thrall_timeout", err: err}
	case errors.As(err, &ce) && ce.Type == thrall.ErrReplyTooLarge:
		return failure{status: http.StatusBadGateway, cause: "response_too_large", err: err}
	case errors.As(err, &ce):
		return failure{status: http.StatusBadGateway, cause: "thrall_error", err: err}
	default:
		return failure{status: http.StatusBadGateway, cause: "invalid_reply", err: err}
	}
}

// forwardableStatus reports whether a web thrall's reply status can be written as the final status
// of an HTTP response. Informational (1xx) statuses are not final, 0 means the reply carried none.
func forwardableStatus(status int) bool { return status >= 200 && status <= 599 }

// requestToWire translates an inbound *http.Request, its already read body and the facts the ingress
// established about it into the http_request payload. Repeated headers are preserved; the
// forwarding headers are replaced by the ingress's own (see forwardedHeaders).
func requestToWire(r *http.Request, body []byte, facts requestFacts) wire.HTTPRequest {
	return wire.HTTPRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		Query:    r.URL.RawQuery,
		Host:     facts.host,
		Scheme:   facts.scheme,
		ClientIP: facts.clientIP,
		Headers:  forwardedHeaders(r.Header, facts),
		Body:     body,
	}
}

// bodyTooLargeError says how a request body exceeded the limit, for the log line of a 413.
type bodyTooLargeError struct {
	limit    int64
	declared int64 // the Content-Length the client stated, 0 when it sent the body without one
}

func (e *bodyTooLargeError) Error() string {
	if e.declared > 0 {
		return fmt.Sprintf("request body of %d bytes exceeds the limit of %d bytes one bus message carries", e.declared, e.limit)
	}
	return fmt.Sprintf("request body exceeds the limit of %d bytes one bus message carries", e.limit)
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
