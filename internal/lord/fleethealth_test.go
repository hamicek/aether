package lord

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/wire"
	"github.com/nats-io/nats.go"
)

// TestFleetHealthBuilder proves the curated summary is built from the lord's own state: app,
// lord id, the publish interval, and each thrall with its topology defaults filled.
func TestFleetHealthBuilder(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "demo", "one_for_one", spec("counter", "permanent", "local"))
	m.Observability.FleetHealthIntervalMs = 2000
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	h := l.fleetHealth()
	if h.App != "demo" {
		t.Errorf("App = %q, want demo", h.App)
	}
	if h.LordID == "" {
		t.Error("LordID is empty")
	}
	if h.IntervalMs != 2000 {
		t.Errorf("IntervalMs = %d, want 2000", h.IntervalMs)
	}
	if h.TS == 0 {
		t.Error("TS not set")
	}
	if len(h.Thralls) != 1 || h.Thralls[0].Name != "counter" {
		t.Fatalf("thralls = %+v, want one counter", h.Thralls)
	}
	if th := h.Thralls[0]; th.Scope != "local" || th.Restart != "permanent" {
		t.Errorf("thrall defaults not filled: %+v", th)
	}
}

// TestFleetHealthCarriesDescribe proves a thrall's self-description (version, ops, last error)
// reaches the fleet summary, so a fleet-wide view shows what build runs and what last failed.
func TestFleetHealthCarriesDescribe(t *testing.T) {
	eth := startEmbedded(t)
	s := spec("counter", "permanent", "local")
	s.Metadata = map[string]string{"site": "A", "plc": "10.0.0.5"}
	s.ExpectedVersion = "2.8.0"
	m := manifest(t, "demo", "one_for_one", s)
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	l.metrics.recordHeartbeat("counter", wire.HeartbeatMetrics{ProcessedTotal: 5, Describe: &wire.ThrallDescribe{
		CallOps: []string{"get"}, CastOps: []string{"inc"}, Version: "2.7.0",
		LastError: "handler_error: boom", LastErrorMs: 1700000000000}})

	h := l.fleetHealth()
	if len(h.Thralls) != 1 {
		t.Fatalf("thralls = %d, want 1", len(h.Thralls))
	}
	th := h.Thralls[0]
	if th.Version != "2.7.0" {
		t.Errorf("version = %q, want 2.7.0", th.Version)
	}
	if len(th.CallOps) != 1 || th.CallOps[0] != "get" || len(th.CastOps) != 1 || th.CastOps[0] != "inc" {
		t.Errorf("ops = call %v cast %v, want [get] / [inc]", th.CallOps, th.CastOps)
	}
	if th.LastError != "handler_error: boom" || th.LastErrorMs != 1700000000000 {
		t.Errorf("last error = %q @ %d, want the reported one", th.LastError, th.LastErrorMs)
	}
	// Metadata is a manifest fact the lord attaches directly (never round-tripped through the thrall).
	if th.Metadata["site"] != "A" || th.Metadata["plc"] != "10.0.0.5" {
		t.Errorf("metadata = %v, want site=A plc=10.0.0.5", th.Metadata)
	}
	// expected_version rides the fleet too (reported 2.7.0 vs expected 2.8.0 = a mismatch a consumer can render).
	if th.ExpectedVersion != "2.8.0" {
		t.Errorf("expected_version = %q, want 2.8.0", th.ExpectedVersion)
	}
}

// TestFleetHealthPublishes proves that with fleet_health on, the lord publishes its summary on the
// fleet subject, carrying its identity.
func TestFleetHealthPublishes(t *testing.T) {
	eth := startEmbedded(t)
	got := make(chan fleet.Health, 1)
	sub, err := eth.Conn().Subscribe(wire.FleetHealthAll(), func(msg *nats.Msg) {
		var h fleet.Health
		if json.Unmarshal(msg.Data, &h) == nil {
			select {
			case got <- h:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	m := manifest(t, "demo", "one_for_one", spec("counter", "permanent", "local"))
	m.Observability.FleetHealth = true
	m.Observability.FleetHealthIntervalMs = 500
	startLord(t, eth, m)

	select {
	case h := <-got:
		if h.App != "demo" || h.LordID == "" {
			t.Errorf("published health = %+v, want app=demo and a lord id", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no fleet health published within 5s")
	}
}

// TestFleetHealthSilentWhenDisabled proves the default is silent: with fleet_health off, nothing is
// published on the fleet subject (opt-in, no traffic for plain runs).
func TestFleetHealthSilentWhenDisabled(t *testing.T) {
	eth := startEmbedded(t)
	gotAny := make(chan struct{}, 1)
	sub, err := eth.Conn().Subscribe(wire.FleetHealthAll(), func(*nats.Msg) {
		select {
		case gotAny <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	m := manifest(t, "demo", "one_for_one", spec("counter", "permanent", "local"))
	// FleetHealth stays false; an interval is set to prove it is the flag, not the interval, that gates.
	m.Observability.FleetHealthIntervalMs = 500
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := l.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer l.Stop()

	select {
	case <-gotAny:
		t.Fatal("fleet health published despite fleet_health = false")
	case <-time.After(1500 * time.Millisecond):
		// silent as expected
	}
}
