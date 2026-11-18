package thrall

// Web thrall: the "a thrall is a web server" helper. A web thrall is an ordinary genserver whose
// single operation is a whole HTTP request/response (op http_request). It never owns a socket or a
// port - a separate ingress translates HTTP <-> envelope and calls it over NATS - so it inherits
// the heartbeat, restart and fencing every thrall gets. The developer writes only an HTTP handler;
// StartWeb wires it to the http_request op and runs the normal lifecycle. Whether the requests
// share the serialized mailbox or run concurrently is the handler's choice of shape (see WebDef).
//
// This is the mirror image of the Edge/[[edge.http]] models: there the edge owns the socket and
// maps a route to an operation; here the thrall is HTTP-native and the edge is a dumb forwarder.

import (
	"encoding/json"
	"fmt"

	"github.com/hamicek/aether/internal/wire"
)

// HTTPRequest and HTTPResponse are the http_request contract, re-exported from internal/wire as type
// aliases so a web thrall can name them without importing an internal package (which is not
// importable from outside the module). They ARE the wire types, so there is one source of truth and
// no conversion on the boundary.
type (
	HTTPRequest  = wire.HTTPRequest
	HTTPResponse = wire.HTTPResponse
)

// OpHTTPRequest is the op a web thrall answers, re-exported for callers that build a request by hand
// (e.g. an ingress or a test).
const OpHTTPRequest = wire.OpHTTPRequest

// Response constructors, re-exported from wire so handlers need not reach into an internal package.
// Header builds a single-value header map; Text/HTML/JSONResponse build a response with the matching
// content type (JSONResponse returns an error only when the value cannot be marshalled).
var (
	Header       = wire.Header
	TextResponse = wire.TextResponse
	HTMLResponse = wire.HTMLResponse
	JSONResponse = wire.JSONResponse
)

// A web thrall is written in one of two shapes, and the shape decides how its requests run:
//
//   - Handle threads a state through the requests, GenServer style: each request gets the state
//     the previous one returned. That only means something one request at a time, so a Handle
//     thrall is always serialized by the mailbox - a slow request holds every other one.
//   - Serve has no next state, and up to WebDef.Concurrency of its requests run at the same time.
//
// WebHandler is the Handle shape: given the request and the current state, return the response
// and the next state. It mirrors the GenServer call shape, so a web thrall can hold state (a
// counter, a cache) across requests, serialized by the mailbox.
type WebHandler[S any] func(req HTTPRequest, state S, ctx *Ctx) (resp HTTPResponse, next S, err error)

// WebServer is the Serve shape: given the request and the value Init returned, return the
// response. Requests are served concurrently, so the mailbox no longer stands between them:
//
//   - shared is the one value Init built, handed to every request. The runtime never replaces it
//     (there is no "next state" to return), so what it holds after Init is what every request
//     sees. Treat it as read-only, or make whatever in it changes safe for concurrent use
//     yourself: a sync.Mutex or atomic around the field, a type that is already safe (a
//     thrall.Client, a *nats.Conn, an *http.Client, a sync.Map). Configuration, parsed templates,
//     embedded files and clients need nothing.
//   - State that must change one request at a time belongs in an ordinary thrall that the handler
//     calls: its mailbox is the lock, and that is what a GenServer is for.
//   - ctx is this request's own (its Trace and MsgID are the request's); do not keep it or share
//     it with another request.
//   - Requests are independent: nothing orders one against another, not even two from one client.
type WebServer[S any] func(req HTTPRequest, shared S, ctx *Ctx) (HTTPResponse, error)

// DefaultWebConcurrency is how many requests a Serve web thrall handles at once when
// WebDef.Concurrency is left at zero. A request mostly waits on calls to other thralls, so the
// number bounds how many of those are in flight (and how much memory replies in transit may
// take), not CPU: it is set high enough that a few slow requests do not hold up the rest of a
// site, and low enough that a stalled dependency cannot be flooded.
const DefaultWebConcurrency = 32

// WebDef defines a web thrall. Exactly one of Handle and Serve is required; Init is optional (the
// state defaults to its zero value, the common case for a stateless web thrall), and Version is
// the usual self-declared build reported to the lord.
type WebDef[S any] struct {
	Name string
	Init func(ctx *Ctx) (S, error)
	// Handle serves requests one at a time, threading the state (see WebHandler).
	Handle WebHandler[S]
	// Serve serves requests concurrently over the value Init returned (see WebServer).
	Serve WebServer[S]
	// Concurrency is the most requests a Serve thrall handles at the same time; further requests
	// wait for a free slot and are dropped unexecuted if their caller's deadline passes first.
	// Zero means DefaultWebConcurrency. One serializes the requests through the mailbox, exactly
	// as Handle does. It applies to Serve only: a Handle thrall is serialized by definition, so
	// setting it there is a mistake StartWeb refuses rather than ignores.
	Concurrency int
	Version     string
}

// concurrency validates the definition and returns how many requests it serves at once.
func (def WebDef[S]) concurrency() (int, error) {
	switch {
	case def.Handle == nil && def.Serve == nil:
		return 0, fmt.Errorf("web thrall %q: one of Handle and Serve is required", def.Name)
	case def.Handle != nil && def.Serve != nil:
		return 0, fmt.Errorf("web thrall %q: Handle and Serve are two ways to write the same thrall - set one", def.Name)
	case def.Concurrency < 0:
		return 0, fmt.Errorf("web thrall %q: Concurrency must not be negative, got %d", def.Name, def.Concurrency)
	case def.Handle != nil && def.Concurrency > 1:
		return 0, fmt.Errorf("web thrall %q: Concurrency %d needs Serve - Handle threads its state from one request to the next, which only works one request at a time", def.Name, def.Concurrency)
	case def.Handle != nil:
		return serialized, nil
	case def.Concurrency == 0:
		return DefaultWebConcurrency, nil
	default:
		return def.Concurrency, nil
	}
}

// StartWeb connects a web thrall to the ether and runs its lifecycle. It registers the
// http_request handler on the ordinary thrall lifecycle, so everything a thrall gets (heartbeat,
// the deadline gate, drain, fencing) applies; what differs between the two handler shapes is only
// whether the requests go through the serialized mailbox or run concurrently.
func StartWeb[S any](def WebDef[S]) error {
	concurrency, err := def.concurrency()
	if err != nil {
		return err
	}
	init := def.Init
	if init == nil {
		init = func(*Ctx) (S, error) { var zero S; return zero, nil }
	}
	handler := webCallFn(def.Handle)
	if def.Serve != nil {
		handler = webServeFn(def.Serve)
	}
	return run(Def[S]{
		Name:       def.Name,
		Init:       init,
		HandleCall: map[string]CallFn[S]{wire.OpHTTPRequest: handler},
		Version:    def.Version,
	}, concurrency)
}

// webCallFn adapts a Handle-shape HTTP handler to a thrall CallFn: decode the http_request payload
// into an HTTPRequest, run the handler, and return the HTTPResponse as the reply. A payload that
// is not a valid HTTPRequest is a handler error (the caller sent something malformed), not an
// escalation - one bad request must not restart the web thrall. Factored out of StartWeb so it is
// unit-testable without a NATS connection.
func webCallFn[S any](handle WebHandler[S]) CallFn[S] {
	return func(payload json.RawMessage, state S, ctx *Ctx) (any, S, error) {
		req, err := decodeHTTPRequest(payload)
		if err != nil {
			return nil, state, err
		}
		resp, next, err := handle(req, state, ctx)
		if err != nil {
			return nil, next, err
		}
		return resp, next, nil
	}
}

// webServeFn adapts a Serve-shape HTTP handler to a thrall CallFn. The state it is given is the
// shared value, and it is returned as it came: a Serve handler has no next state, whether its
// requests run through the pool or (at Concurrency 1) through the mailbox.
func webServeFn[S any](serve WebServer[S]) CallFn[S] {
	return func(payload json.RawMessage, shared S, ctx *Ctx) (any, S, error) {
		req, err := decodeHTTPRequest(payload)
		if err != nil {
			return nil, shared, err
		}
		resp, err := serve(req, shared, ctx)
		if err != nil {
			return nil, shared, err
		}
		return resp, shared, nil
	}
}

func decodeHTTPRequest(payload json.RawMessage) (HTTPRequest, error) {
	var req HTTPRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return HTTPRequest{}, fmt.Errorf("http_request: decode payload: %w", err)
	}
	return req, nil
}
