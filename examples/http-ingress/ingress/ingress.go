package ingress

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// defaultMaxBodyBytes caps a forwarded request body so a single request cannot buffer unbounded
// memory into the ether. The demo value is generous; a real deployment tunes it per site.
const defaultMaxBodyBytes = 10 << 20 // 10 MiB

// defaultTimeout bounds how long the ingress waits for a web thrall's reply before returning 504.
const defaultTimeout = 5 * time.Second

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
	nc      *nats.Conn
	routes  RouteProvider
	app     string
	timeout time.Duration
	maxBody int64
	log     *slog.Logger
}

// NewHandler builds the ingress HTTP handler over a NATS connection and a route provider. It owns no
// socket - the caller serves it (the edge's http.Server, or httptest in a test).
func NewHandler(nc *nats.Conn, routes RouteProvider, opts Options) http.Handler {
	h := &handler{nc: nc, routes: routes, app: opts.App, timeout: opts.Timeout, maxBody: opts.MaxBodyBytes, log: opts.Log}
	if h.timeout <= 0 {
		h.timeout = defaultTimeout
	}
	if h.maxBody <= 0 {
		h.maxBody = defaultMaxBodyBytes
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	thrall, ok := h.routes.Resolve(r.Host)
	if !ok {
		// Default-deny: a host with no explicit route does not reach the ether at all.
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	req, err := requestToWire(r, h.maxBody)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	env := wire.Envelope{
		V: 1, ID: nats.NewInbox(), Trace: nats.NewInbox(),
		Kind: wire.KindCall, To: thrall, Op: wire.OpHTTPRequest,
		Payload: mustMarshal(req), TS: time.Now().UnixMilli(),
	}
	msg, err := h.nc.Request(wire.Call(h.app, thrall), mustMarshal(env), h.timeout)
	if err != nil {
		status := statusForError(err)
		h.log.Warn("ingress call failed", slog.String("host", r.Host), slog.String("thrall", thrall), slog.Any("err", err))
		http.Error(w, http.StatusText(status), status)
		return
	}

	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.Status == "error" {
		// The thrall replied an application error (or garbage): the upstream failed.
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}

	var resp wire.HTTPResponse
	if err := json.Unmarshal(reply.Payload, &resp); err != nil || resp.Status == 0 {
		// A reply that is not a valid HTTPResponse, or carries no status, is an upstream fault -
		// not something to forward as a 200 or an invalid HTTP status (see AE-091 review).
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	writeResponse(w, resp)
}

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
// repeated Set-Cookie survives), then the status, then the body.
func writeResponse(w http.ResponseWriter, resp wire.HTTPResponse) {
	header := w.Header()
	for key, values := range resp.Headers {
		for _, v := range values {
			header.Add(key, v)
		}
	}
	w.WriteHeader(resp.Status)
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}

// statusForError maps a transport-level failure of the call to an HTTP status: no responders (the
// thrall is not running) -> 503, a reply timeout -> 504, any other transport error -> 502.
func statusForError(err error) int {
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return http.StatusServiceUnavailable
	case errors.Is(err, nats.ErrTimeout):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
