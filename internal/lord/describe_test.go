package lord

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
)

// TestSetStatusWritesDescribeToRegistry proves the lord folds a thrall's last-known
// self-description into its registry entry on every status write, so `aether ps` / `aether describe`
// can read the version and ops without asking the thrall directly. It also proves a status write
// after a describe-less beat keeps the last-known fields rather than blanking them.
func TestSetStatusWritesDescribeToRegistry(t *testing.T) {
	eth := startEmbedded(t)
	reg, err := registry.Open(eth.Conn())
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	l := &Lord{log: obs.NewLogger(), id: "lord-test", reg: reg, metrics: newLordMetrics()}
	l.children = []*child{{spec: ThrallSpec{Name: "counter", Metadata: map[string]string{"site": "A"}, ExpectedVersion: "3.1.4"}}}

	l.metrics.recordHeartbeat("counter", wire.HeartbeatMetrics{ProcessedTotal: 1, Describe: &wire.ThrallDescribe{
		CallOps: []string{"get", "value"}, CastOps: []string{"inc"}, Version: "3.1.4"}})
	l.setStatus("counter", 4321, "ready")

	e, ok, err := reg.Get("counter")
	if err != nil || !ok {
		t.Fatalf("registry.Get: ok=%v err=%v", ok, err)
	}
	if e.Version != "3.1.4" {
		t.Errorf("version = %q, want 3.1.4", e.Version)
	}
	if e.Metadata["site"] != "A" {
		t.Errorf("metadata = %v, want site=A (from the manifest spec)", e.Metadata)
	}
	if e.ExpectedVersion != "3.1.4" {
		t.Errorf("expected_version = %q, want 3.1.4 (from the manifest spec)", e.ExpectedVersion)
	}
	if len(e.CallOps) != 2 || e.CallOps[0] != "get" || e.CallOps[1] != "value" {
		t.Errorf("call ops = %v, want [get value]", e.CallOps)
	}
	if len(e.CastOps) != 1 || e.CastOps[0] != "inc" {
		t.Errorf("cast ops = %v, want [inc]", e.CastOps)
	}

	// A subsequent status write (e.g. the reaper marking stale) must keep the description.
	l.setStatus("counter", 4321, "stale")
	if e, _, _ := reg.Get("counter"); e.Version != "3.1.4" || len(e.CallOps) != 2 {
		t.Errorf("description lost on a later status write: %+v", e)
	}
}

// TestCheckEdgeOpsWarnsOnUnknownOp proves the async validation flags an edge route whose op the
// thrall does not report (against the right call/cast set), leaves a valid route alone, is
// idempotent (warns once), and skips a thrall that reports no ops.
func TestCheckEdgeOpsWarnsOnUnknownOp(t *testing.T) {
	l := &Lord{
		log:          obs.NewLogger(),
		edgeOpWarned: map[string]bool{},
		manifest: &Manifest{Edge: Edge{HTTP: []EdgeHTTPSpec{{
			Name: "api", Addr: ":8080",
			Routes: map[string]EdgeRoute{
				"GET /value":      {Thrall: "counter", Op: "get", Kind: "call"},  // valid call op
				"POST /increment": {Thrall: "counter", Op: "incr", Kind: "cast"}, // typo: incr, not inc
				"POST /bad-kind":  {Thrall: "counter", Op: "get", Kind: "cast"},  // op exists but as a call, not a cast
			},
		}}}},
	}
	d := &wire.ThrallDescribe{CallOps: []string{"get"}, CastOps: []string{"inc"}}

	l.checkEdgeOps("counter", d)
	if !l.edgeOpWarned["api\x00POST /increment"] {
		t.Error("a route targeting an unreported op must be flagged")
	}
	if !l.edgeOpWarned["api\x00POST /bad-kind"] {
		t.Error("a cast route targeting a call-only op must be flagged")
	}
	if l.edgeOpWarned["api\x00GET /value"] {
		t.Error("a valid route must not be flagged")
	}

	// Idempotent: a second pass must not panic and keeps the flags.
	l.checkEdgeOps("counter", d)

	// A thrall that reports no ops (event manager / edge) is skipped, not falsely flagged.
	l2 := &Lord{log: obs.NewLogger(), edgeOpWarned: map[string]bool{}, manifest: l.manifest}
	l2.checkEdgeOps("counter", &wire.ThrallDescribe{Version: "1.0"})
	if len(l2.edgeOpWarned) != 0 {
		t.Errorf("a describe with no ops must not flag anything, got %v", l2.edgeOpWarned)
	}
}

// TestCheckExpectedVersion proves the rollout mismatch alarm: it warns once when the reported
// version differs from the manifest's expected_version (including a reported-empty version), stays
// silent on a match or when no expectation is declared, and re-arms when the reported version
// changes.
func TestCheckExpectedVersion(t *testing.T) {
	newLord := func(specs ...ThrallSpec) *Lord {
		children := make([]*child, len(specs))
		for i, s := range specs {
			children[i] = &child{spec: s}
		}
		return &Lord{log: obs.NewLogger(), versionWarned: map[string]bool{}, children: children}
	}

	// Match -> no alarm.
	l := newLord(ThrallSpec{Name: "counter", ExpectedVersion: "1.4.0"})
	l.checkExpectedVersion("counter", &wire.ThrallDescribe{Version: "1.4.0"})
	if len(l.versionWarned) != 0 {
		t.Errorf("a matching version must not warn, got %v", l.versionWarned)
	}

	// Mismatch -> warned once, keyed by reported version.
	l = newLord(ThrallSpec{Name: "counter", ExpectedVersion: "1.4.0"})
	d := &wire.ThrallDescribe{Version: "1.3.0"}
	l.checkExpectedVersion("counter", d)
	l.checkExpectedVersion("counter", d) // idempotent
	if !l.versionWarned["counter\x001.3.0"] || len(l.versionWarned) != 1 {
		t.Errorf("a mismatch must warn once per reported version, got %v", l.versionWarned)
	}
	// A reported version that changes re-arms the alarm.
	l.checkExpectedVersion("counter", &wire.ThrallDescribe{Version: "1.2.0"})
	if !l.versionWarned["counter\x001.2.0"] {
		t.Error("a changed reported version must re-warn")
	}

	// Reported-empty against a set expectation is itself a mismatch.
	l = newLord(ThrallSpec{Name: "counter", ExpectedVersion: "1.4.0"})
	l.checkExpectedVersion("counter", &wire.ThrallDescribe{})
	l.checkExpectedVersion("counter", nil) // nil describe = reported empty, must not panic
	if !l.versionWarned["counter\x00"] {
		t.Errorf("an empty reported version against an expectation must warn, got %v", l.versionWarned)
	}

	// No expectation declared -> no check at all.
	l = newLord(ThrallSpec{Name: "counter"})
	l.checkExpectedVersion("counter", &wire.ThrallDescribe{Version: "9.9.9"})
	if len(l.versionWarned) != 0 {
		t.Errorf("a thrall without expected_version must not be checked, got %v", l.versionWarned)
	}
}

// TestProbeReportsVersionAndLastError drives a real re-exec'd thrall end-to-end: its self-declared
// version reaches the registry over the heartbeat, and a plain (non-fatal) handler error is
// reported as last_error on a later beat while the thrall keeps running.
func TestProbeReportsVersionAndLastError(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	reg, err := registry.Open(nc)
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	waitFor(t, 5*time.Second, "version and ops in the registry", func() bool {
		e, ok, _ := reg.Get("probe")
		return ok && e.Version == "probe-1.0" && len(e.CallOps) > 0
	})

	cast(t, nc, app, "probe", "fail")
	waitFor(t, 5*time.Second, "last error reported on a heartbeat", func() bool {
		e, ok, _ := reg.Get("probe")
		return ok && strings.Contains(e.LastError, "cast handler boom")
	})
}

// TestEscalateEmitsDyingBreath proves the crash path publishes a final heartbeat carrying the
// escalation reason before the process exits. The test watches the thrall's heartbeat subject
// directly, so it observes the dying breath regardless of how fast the lord reaps and restarts the
// process (the registry copy is transient - a restart resets last_error, like processed_total).
func TestEscalateEmitsDyingBreath(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	got := make(chan string, 8)
	sub, err := nc.Subscribe(wire.Heartbeat("probe"), func(m *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(m.Data, &e) != nil {
			return
		}
		var hm wire.HeartbeatMetrics
		if json.Unmarshal(e.Payload, &hm) != nil || hm.Describe == nil || hm.Describe.LastError == "" {
			return
		}
		select {
		case got <- hm.Describe.LastError:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	cast(t, nc, app, "probe", "escalate")

	select {
	case le := <-got:
		if !strings.Contains(le, "cast asked to crash") {
			t.Fatalf("dying-breath last_error = %q, want the escalate reason", le)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no dying-breath heartbeat carrying the escalate reason")
	}
}
