package thrall

import (
	"errors"
	"strings"
	"testing"
)

// The lifecycle of StartEdge (init -> run -> drain -> stop) is proven by a real run in
// examples/webserver-custom; here we cover the validation guards that fire before any dial, the way
// the FSM/event behaviours are unit-tested.

// StartEdge must reject a definition without a run-loop - there would be nothing to run.
func TestStartEdgeRequiresRun(t *testing.T) {
	t.Setenv("AETHER_NATS_URL", "nats://127.0.0.1:1") // never dialled: the guard fires first
	t.Setenv("AETHER_APP", "test")
	err := StartEdge(EdgeDef{Name: "gw"})
	if err == nil || !strings.Contains(err.Error(), "Run is required") {
		t.Fatalf("want a 'Run is required' error, got %v", err)
	}
}

// Without the injected bus env an edge is being run outside `aether up` - fail with a clear message.
func TestStartEdgeRequiresBusEnv(t *testing.T) {
	t.Setenv("AETHER_NATS_URL", "")
	t.Setenv("AETHER_APP", "")
	err := StartEdge(EdgeDef{Name: "gw", Run: func(*Ctx, <-chan struct{}) error { return nil }})
	if err == nil || !strings.Contains(err.Error(), "AETHER_NATS_URL") {
		t.Fatalf("want a missing-env error, got %v", err)
	}
}

// A run-loop that returns an error must make StartEdge return it - so the process exits non-zero and
// the lord restarts it per policy. A swallowed error would be an exit 0, i.e. a "clean stop" that a
// transient thrall would not be restarted from.
func TestStartEdgeRunErrorPropagates(t *testing.T) {
	_, srv := embeddedNATS(t)
	t.Setenv("AETHER_NATS_URL", srv.ClientURL())
	t.Setenv("AETHER_APP", "test")

	boom := errors.New("run boom")
	err := StartEdge(EdgeDef{
		Name: "gw",
		Run:  func(_ *Ctx, _ <-chan struct{}) error { return boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("StartEdge returned %v, want the run-loop error propagated", err)
	}
}
