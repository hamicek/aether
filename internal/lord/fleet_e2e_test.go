package lord

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/wire"
	"github.com/nats-io/nats.go"
)

// findNode returns the aggregator view for an app, or false if absent.
func findNode(snap []fleet.NodeView, app string) (fleet.NodeView, bool) {
	for _, n := range snap {
		if n.App == app {
			return n, true
		}
	}
	return fleet.NodeView{}, false
}

// TestFleetEndToEnd proves L1+L2 together on one bus (one NATS account, as an external cluster
// presents): a real lord publishes its health, an aggregator assembles the fleet, a second lord's
// summary shows up alongside it, a lord that stops publishing goes stale while the live one stays
// live, and fleet health never lands on the raw supervision subject.
//
// The second lord is published synthetically rather than run as a second full process: within one
// account, what the aggregator receives is exactly a fleet.Health message on the bus, which is what
// we publish. This test does NOT cover crossing a leaf-node account boundary - fleet health does not
// yet cross the leaf (a stream export of aether._fleet.> is a planned follow-up); the isolation
// asserted here is that fleet health uses a subject separate from aether._lord.>, not an
// account-export boundary.
func TestFleetEndToEnd(t *testing.T) {
	eth := startEmbedded(t)

	agg := fleet.NewAggregator()
	if _, err := agg.Subscribe(eth.Conn()); err != nil {
		t.Fatalf("aggregator subscribe: %v", err)
	}

	// Subject-separation guard: watch the raw supervision namespace and flag any fleet-shaped payload
	// there. This proves fleet health uses a subject distinct from aether._lord.> - not an
	// account-export boundary (crossing accounts is out of scope; see the function doc).
	leakedSupervision := make(chan struct{}, 1)
	subLord, err := eth.Conn().Subscribe("aether._lord.>", func(m *nats.Msg) {
		var h fleet.Health
		if json.Unmarshal(m.Data, &h) == nil && h.App != "" && len(h.Thralls) > 0 {
			select {
			case leakedSupervision <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("subscribe _lord: %v", err)
	}
	defer subLord.Unsubscribe()

	// A real lord for site-a - the genuine L1 publish path.
	m := manifest(t, "site-a", "one_for_one", spec("counter-a", "permanent", "local"))
	m.Observability.FleetHealth = true
	m.Observability.FleetHealthIntervalMs = 500
	startLord(t, eth, m)

	// A second lord (site-b), published once, with a short interval so it goes stale quickly.
	siteB, _ := json.Marshal(fleet.Health{App: "site-b", LordID: "node-b-1", IntervalMs: 500})
	if err := eth.Conn().Publish(wire.FleetHealth("site-b", "node-b-1"), siteB); err != nil {
		t.Fatalf("publish site-b: %v", err)
	}

	// AC #1 + #2: both lords appear in the fleet.
	waitFor(t, 5*time.Second, "both lords in fleet", func() bool {
		snap := agg.Snapshot()
		_, aOK := findNode(snap, "site-a")
		_, bOK := findNode(snap, "site-b")
		return aOK && bOK
	})

	// AC #5: site-b stops publishing -> goes stale; site-a (live lord) stays live.
	waitFor(t, 5*time.Second, "site-b goes stale", func() bool {
		b, ok := findNode(agg.Snapshot(), "site-b")
		return ok && b.Stale
	})
	if a, ok := findNode(agg.Snapshot(), "site-a"); !ok || a.Stale {
		t.Errorf("site-a should stay live while its lord publishes; got ok=%v stale=%v", ok, a.Stale)
	}

	// AC #3: raw supervision (aether._lord.>) never carried the curated fleet summary.
	select {
	case <-leakedSupervision:
		t.Fatal("fleet health leaked onto aether._lord.> - supervision must stay separate")
	default:
	}
}
