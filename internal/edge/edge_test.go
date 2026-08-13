package edge

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

func testSpec() Spec {
	return Spec{
		Name: "api",
		Addr: ":8080",
		Routes: map[string]Route{
			"GET /value":      {Thrall: "counter", Op: "value", Kind: "call"},
			"POST /increment": {Thrall: "counter", Op: "increment", Kind: "cast"},
		},
	}
}

func TestRouterMatch(t *testing.T) {
	r := NewRouter(testSpec())

	if route, ok := r.Match(http.MethodGet, "/value"); !ok || route.Op != "value" {
		t.Errorf("GET /value = %+v (ok=%v), want value", route, ok)
	}
	// Method is part of the match: POST /value is not configured even though GET /value is.
	if _, ok := r.Match(http.MethodPost, "/value"); ok {
		t.Errorf("POST /value matched, want miss")
	}
	if _, ok := r.Match(http.MethodGet, "/missing"); ok {
		t.Errorf("GET /missing matched, want miss")
	}
}

func TestBuildEnvelopeCall(t *testing.T) {
	route := Route{Thrall: "counter", Op: "value", Kind: "call"}
	env := BuildEnvelope(route, []byte(`{"by":2}`), nil)

	if env.Kind != wire.KindCall {
		t.Errorf("kind = %q, want call", env.Kind)
	}
	if env.To != "counter" || env.Op != "value" {
		t.Errorf("to/op = %q/%q, want counter/value", env.To, env.Op)
	}
	if string(env.Payload) != `{"by":2}` {
		t.Errorf("payload = %s, want the request body", env.Payload)
	}
	if env.V != 1 || env.ID == "" || env.Trace == "" || env.TS == 0 {
		t.Errorf("envelope header incomplete: %+v", env)
	}
}

func TestBuildEnvelopeCast(t *testing.T) {
	route := Route{Thrall: "counter", Op: "increment", Kind: "cast"}
	env := BuildEnvelope(route, nil, nil)

	if env.Kind != wire.KindCast {
		t.Errorf("kind = %q, want cast", env.Kind)
	}
	if string(env.Payload) != "{}" {
		t.Errorf("empty body payload = %s, want {}", env.Payload)
	}
}

func TestBuildEnvelopeQueryFallback(t *testing.T) {
	route := Route{Thrall: "counter", Op: "value", Kind: "call"}
	// No body -> query becomes the payload.
	env := BuildEnvelope(route, nil, url.Values{"id": {"7"}})
	if string(env.Payload) != `{"id":"7"}` {
		t.Errorf("payload = %s, want {\"id\":\"7\"} from query", env.Payload)
	}

	// A body takes precedence over query.
	env = BuildEnvelope(route, []byte(`{"id":"body"}`), url.Values{"id": {"query"}})
	if string(env.Payload) != `{"id":"body"}` {
		t.Errorf("payload = %s, want the body to win over query", env.Payload)
	}
}

func TestStatusForReply(t *testing.T) {
	if got := StatusForReply(wire.Envelope{Status: "ok"}); got != http.StatusOK {
		t.Errorf("ok reply -> %d, want 200", got)
	}
	if got := StatusForReply(wire.Envelope{Status: "error"}); got != http.StatusBadGateway {
		t.Errorf("error reply -> %d, want 502", got)
	}
}

func TestStatusForError(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nats.ErrNoResponders, http.StatusServiceUnavailable}, // 503
		{nats.ErrTimeout, http.StatusGatewayTimeout},          // 504
		{nats.ErrConnectionClosed, http.StatusBadGateway},     // 502 - other transport error
	}
	for _, tc := range cases {
		if got := StatusForError(tc.err); got != tc.want {
			t.Errorf("StatusForError(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}
