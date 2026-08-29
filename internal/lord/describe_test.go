package lord

import (
	"testing"

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
	l.children = []*child{{spec: ThrallSpec{Name: "counter", Metadata: map[string]string{"site": "A"}}}}

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
