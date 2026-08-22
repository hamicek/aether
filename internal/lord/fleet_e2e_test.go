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

// TestFleetEndToEnd proves L1+L2 together on one bus: a real lord publishes its health, an
// aggregator assembles the fleet, a second node's summary shows up alongside it, a node that stops
// publishing goes stale while the live one stays live, and the raw supervision channel never
// carries fleet data.
//
// The second node is published synthetically rather than by a second full lord: the real multi-node
// topology gives each lord its own bus joined by a leaf/cluster, so what an aggregator actually
// receives is exactly a fleet.Health message arriving on its bus - which is what we publish here.
func TestFleetEndToEnd(t *testing.T) {
	eth := startEmbedded(t)

	agg := fleet.NewAggregator()
	if _, err := agg.Subscribe(eth.Conn()); err != nil {
		t.Fatalf("aggregator subscribe: %v", err)
	}

	// Isolation guard: watch the raw supervision namespace and flag any fleet-shaped payload there.
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

	// A second node (site-b), published once, with a short interval so it goes stale quickly.
	siteB, _ := json.Marshal(fleet.Health{App: "site-b", LordID: "node-b-1", IntervalMs: 500})
	if err := eth.Conn().Publish(wire.FleetHealth("site-b", "node-b-1"), siteB); err != nil {
		t.Fatalf("publish site-b: %v", err)
	}

	// AC #1 + #2: both nodes appear in the fleet.
	waitFor(t, 5*time.Second, "both nodes in fleet", func() bool {
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
