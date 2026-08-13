// Package edge holds the translation logic of the built-in HTTP ingress: matching an incoming
// request to a configured route, building the ether call/cast envelope, and mapping the outcome
// back to an HTTP status. It depends only on the wire contract, not on the manifest or the lord,
// so it stays a small, pure, unit-testable core; the HTTP server and NATS I/O live in the
// `aether _edge` subcommand that drives it.
package edge

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// EnvSpec is the environment variable through which the lord hands an edge process its Spec as JSON.
const EnvSpec = "AETHER_EDGE_SPEC"

// SpecFromEnv reads and parses the edge Spec the lord injected via EnvSpec.
func SpecFromEnv() (Spec, error) {
	raw := os.Getenv(EnvSpec)
	if raw == "" {
		return Spec{}, fmt.Errorf("%s not set", EnvSpec)
	}
	var s Spec
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return Spec{}, fmt.Errorf("parse %s: %w", EnvSpec, err)
	}
	if s.Addr == "" || len(s.Routes) == 0 {
		return Spec{}, fmt.Errorf("edge spec incomplete (missing addr or routes)")
	}
	return s, nil
}

// Spec is the runtime configuration of one HTTP edge server. The lord transports it to the spawned
// `aether _edge` process as JSON (env AETHER_EDGE_SPEC); it mirrors the manifest's [[edge.http]]
// section but is decoupled from TOML parsing.
type Spec struct {
	Name   string           `json:"name"`
	Addr   string           `json:"addr"`
	Routes map[string]Route `json:"routes"` // key: "METHOD /path"
}

// Route is a single HTTP route's target on the ether.
type Route struct {
	Thrall string `json:"thrall"`
	Op     string `json:"op"`
	Kind   string `json:"kind"` // call (wait for reply) | cast (fire-and-forget)
}

// Router resolves an incoming request's method and path to a configured route.
type Router struct {
	routes map[string]Route
}

// NewRouter builds a router from a spec. The route key is "METHOD /path", so the method is part of
// the match (GET and POST on the same path are distinct routes).
func NewRouter(s Spec) *Router {
	return &Router{routes: s.Routes}
}

// Match returns the route for a method+path, or false if none is configured.
func (r *Router) Match(method, path string) (Route, bool) {
	route, ok := r.routes[method+" "+path]
	return route, ok
}

// BuildEnvelope assembles the call/cast envelope for a matched route. The payload is the request
// body; if the body is empty, the query parameters are used instead (as a JSON object of string
// values), falling back to "{}". A fresh Trace is minted per request so the logical operation can
// be followed across hops.
func BuildEnvelope(route Route, body []byte, query url.Values) wire.Envelope {
	kind := wire.KindCall
	if route.Kind == "cast" {
		kind = wire.KindCast
	}
	return wire.Envelope{
		V:       1,
		ID:      nats.NewInbox(),
		Trace:   nats.NewInbox(),
		Kind:    kind,
		To:      route.Thrall,
		Op:      route.Op,
		Payload: payloadFrom(body, query),
		TS:      time.Now().UnixMilli(),
	}
}

// payloadFrom prefers the request body; when it is empty it projects the query parameters into a
// flat JSON object (last value wins per key), and defaults to an empty object. Query values are
// always JSON strings (`?by=2` -> `{"by":"2"}`) - a handler needing typed fields should take a JSON
// body; typed query coercion is out of scope.
func payloadFrom(body []byte, query url.Values) json.RawMessage {
	if len(body) > 0 {
		return json.RawMessage(body)
	}
	if len(query) > 0 {
		fields := make(map[string]string, len(query))
		for k := range query {
			fields[k] = query.Get(k)
		}
		if data, err := json.Marshal(fields); err == nil {
			return data
		}
	}
	return json.RawMessage("{}")
}

// StatusForReply maps a completed call's reply to an HTTP status: an application-level error reply
// from the thrall becomes 502 (the upstream thrall failed), a successful reply becomes 200.
func StatusForReply(reply wire.Envelope) int {
	if reply.Status == "error" {
		return http.StatusBadGateway
	}
	return http.StatusOK
}

// StatusForError maps a transport-level failure of a call to an HTTP status: no responders (the
// target thrall is not running or not subscribed) becomes 503, a timeout becomes 504, and any
// other transport error becomes 502.
func StatusForError(err error) int {
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return http.StatusServiceUnavailable
	case errors.Is(err, nats.ErrTimeout):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}
