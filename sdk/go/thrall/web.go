package thrall

// Web thrall: the "a thrall is a web server" helper. A web thrall is an ordinary genserver whose
// single operation is a whole HTTP request/response (op http_request). It never owns a socket or a
// port - a separate ingress translates HTTP <-> envelope and calls it over NATS - so it inherits
// the serialized mailbox, heartbeat, restart and fencing every thrall gets. The developer writes
// only an HTTP handler; StartWeb wires it to the http_request op and runs the normal lifecycle.
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

// WebHandler is the one function a web thrall implements: given the request and the current state,
// return the response and the next state. It mirrors the GenServer call shape, so a web thrall can
// still hold state (a counter, a cache) across requests, serialized by the mailbox.
type WebHandler[S any] func(req HTTPRequest, state S, ctx *Ctx) (resp HTTPResponse, next S, err error)

// WebDef defines a web thrall. Only Handle is required; Init is optional (the state defaults to its
// zero value, the common case for a stateless web thrall), and Version is the usual self-declared
// build reported to the lord.
type WebDef[S any] struct {
	Name    string
	Init    func(ctx *Ctx) (S, error)
	Handle  WebHandler[S]
	Version string
}

// StartWeb connects a web thrall to the ether and runs its lifecycle. It is a thin wrapper over
// Start that registers the http_request handler, so everything Start provides (durability,
// heartbeat, drain, fencing) applies unchanged.
func StartWeb[S any](def WebDef[S]) error {
	if def.Handle == nil {
		return fmt.Errorf("web thrall %q: Handle is required", def.Name)
	}
	init := def.Init
	if init == nil {
		init = func(*Ctx) (S, error) { var zero S; return zero, nil }
	}
	return Start(Def[S]{
		Name:       def.Name,
		Init:       init,
		HandleCall: map[string]CallFn[S]{wire.OpHTTPRequest: webCallFn(def.Handle)},
		Version:    def.Version,
	})
}

// webCallFn adapts an HTTP handler to a thrall CallFn: decode the http_request payload into an
// HTTPRequest, run the handler, and return the HTTPResponse as the reply. A payload that is not a
// valid HTTPRequest is a handler error (the caller sent something malformed), not an escalation -
// one bad request must not restart the web thrall. Factored out of StartWeb so it is unit-testable
// without a NATS connection.
func webCallFn[S any](handle WebHandler[S]) CallFn[S] {
	return func(payload json.RawMessage, state S, ctx *Ctx) (any, S, error) {
		var req HTTPRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, state, fmt.Errorf("http_request: decode payload: %w", err)
		}
		resp, next, err := handle(req, state, ctx)
		if err != nil {
			return nil, next, err
		}
		return resp, next, nil
	}
}
