package lord

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/edge"
)

// TestNewSynthesizesEdgeChildren proves the lord turns each [[edge.http]] entry into a supervised
// singleton child that runs the internal _edge subcommand and carries its route table via env.
func TestNewSynthesizesEdgeChildren(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App: "demo",
		Edge: Edge{HTTP: []EdgeHTTPSpec{
			{Name: "api", Addr: ":18080", Routes: map[string]EdgeRoute{
				"GET /value": {Thrall: "counter", Op: "value", Kind: "call"},
			}},
			{Name: "admin", Addr: ":18081", Routes: map[string]EdgeRoute{
				"POST /reset": {Thrall: "counter", Op: "reset", Kind: "cast"},
			}},
		}},
	}
	m.applyDefaults()

	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(l.children) != 2 {
		t.Fatalf("children = %d, want 2 edge servers", len(l.children))
	}

	byName := map[string]*child{}
	for _, ch := range l.children {
		byName[ch.spec.Name] = ch
	}
	api := byName["api"]
	if api == nil {
		t.Fatal("edge server 'api' not synthesized")
	}
	if api.spec.Scope != "singleton" {
		t.Errorf("edge scope = %q, want singleton (it binds a port)", api.spec.Scope)
	}
	if !strings.Contains(api.spec.Cmd, "_edge") {
		t.Errorf("edge cmd = %q, want it to invoke the _edge subcommand", api.spec.Cmd)
	}

	specJSON, ok := envValue(api.env(), edge.EnvSpec)
	if !ok {
		t.Fatal("AETHER_EDGE_SPEC not injected into the edge env")
	}
	var runtime edge.Spec
	if err := json.Unmarshal([]byte(specJSON), &runtime); err != nil {
		t.Fatalf("injected edge spec is not valid JSON: %v", err)
	}
	if runtime.Addr != ":18080" || len(runtime.Routes) != 1 {
		t.Errorf("edge spec = %+v, want addr :18080 with 1 route", runtime)
	}
	if r := runtime.Routes["GET /value"]; r.Thrall != "counter" || r.Kind != "call" {
		t.Errorf("route = %+v, want counter/call", r)
	}
}
