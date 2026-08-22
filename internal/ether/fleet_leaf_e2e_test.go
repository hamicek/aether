package ether

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/wire"
	"github.com/nats-io/nats.go"
)

// startFleetLeafHub boots a hub whose center account (HUB) imports each site's fleet-health stream
// (aether._fleet.<app>.>) in addition to its data plane. This is the operator-authored hub side that
// AE-069 documents: the spoke exports its fleet health (leafConfig), and the hub imports it so an
// aggregator in HUB sees the whole fleet across the leaf. Returns the leaf URL and a HUB connection.
func startFleetLeafHub(t *testing.T) (leafURL string, hub *nats.Conn) {
	t.Helper()
	leafPort := freePort(t)
	cfg := fmt.Sprintf(`
server_name: hub
jetstream { domain: hub }
leafnodes { listen: 127.0.0.1:%d }
accounts {
  HUB {
    jetstream: enabled
    users: [ { user: local, password: local } ]
    imports: [
      { stream: { account: SITE_A, subject: "aether._fleet.sitea.>" } }
      { stream: { account: SITE_B, subject: "aether._fleet.siteb.>" } }
    ]
  }
  SITE_A {
    jetstream: enabled
    users: [ { user: leafA, password: leafA } ]
    exports: [ { stream: "aether._fleet.sitea.>" } ]
  }
  SITE_B {
    jetstream: enabled
    users: [ { user: leafB, password: leafB } ]
    exports: [ { stream: "aether._fleet.siteb.>" } ]
  }
  SYS {}
}
system_account: SYS
no_auth_user: local
`, leafPort)

	srv := startServerFromConfig(t, cfg)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect hub: %v", err)
	}
	t.Cleanup(nc.Close)
	return fmt.Sprintf("nats-leaf://127.0.0.1:%d", leafPort), nc
}

// publishHealthOn publishes a fleet.Health summary on the spoke's own bus, the way a lord's
// publishHealth loop does - so this test exercises the real subject and the real leaf export/import,
// without standing up a full lord.
func publishHealthOn(t *testing.T, eth *Ether, app, lordID string) {
	t.Helper()
	data, _ := json.Marshal(fleet.Health{App: app, LordID: lordID, IntervalMs: 500})
	if err := eth.Conn().Publish(wire.FleetHealth(app, lordID), data); err != nil {
		t.Fatalf("publish health on %s: %v", app, err)
	}
	eth.Conn().Flush()
}

// TestFleetHealthCrossesLeaf (AC #1, #2) proves a spoke's curated fleet health reaches an aggregator
// on the hub across the leaf boundary, via the stream export/import - the whole point of AE-069.
func TestFleetHealthCrossesLeaf(t *testing.T) {
	leafURL, hub := startFleetLeafHub(t)
	spokeA := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	agg := fleet.NewAggregator()
	if _, err := agg.Subscribe(hub); err != nil {
		t.Fatalf("aggregator subscribe on hub: %v", err)
	}

	// Retry to absorb leaf interest propagation: keep publishing until the hub-side aggregator sees it.
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		publishHealthOn(t, spokeA, "sitea", "node-a-1")
		time.Sleep(150 * time.Millisecond)
		for _, n := range agg.Snapshot() {
			if n.App == "sitea" && n.LordID == "node-a-1" {
				return // fleet health crossed the leaf
			}
		}
	}
	t.Fatal("spoke fleet health never reached the hub aggregator across the leaf")
}

// TestFleetTwoSpokesIsolated (AC #2) proves the hub sees two spokes at once, while the spokes stay
// isolated: an aggregator on site A's own bus sees only site A, never site B's fleet health.
func TestFleetTwoSpokesIsolated(t *testing.T) {
	leafURL, hub := startFleetLeafHub(t)
	spokeA := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")
	spokeB := startSpoke(t, leafURL, "SITE_B", "siteb", "sb", "leafB")

	hubAgg := fleet.NewAggregator()
	if _, err := hubAgg.Subscribe(hub); err != nil {
		t.Fatalf("hub aggregator subscribe: %v", err)
	}
	// An aggregator bound to site A's own bus - it must never see site B (accounts are isolated).
	siteAAgg := fleet.NewAggregator()
	if _, err := siteAAgg.Subscribe(spokeA.Conn()); err != nil {
		t.Fatalf("site A aggregator subscribe: %v", err)
	}

	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		publishHealthOn(t, spokeA, "sitea", "node-a-1")
		publishHealthOn(t, spokeB, "siteb", "node-b-1")
		time.Sleep(150 * time.Millisecond)
		apps := map[string]bool{}
		for _, n := range hubAgg.Snapshot() {
			apps[n.App] = true
		}
		if apps["sitea"] && apps["siteb"] {
			// The hub sees both; site A must see only itself.
			for _, n := range siteAAgg.Snapshot() {
				if n.App == "siteb" {
					t.Fatal("site A saw site B's fleet health; sites must stay isolated")
				}
			}
			return
		}
	}
	t.Fatal("hub did not see both spokes' fleet health within 5s")
}

// TestFleetSupervisionDoesNotCrossLeaf (AC #3) proves the isolation invariant holds: even while
// fleet health crosses, the raw supervision subject aether._lord.> published on the spoke never
// reaches the hub - it is not exported.
func TestFleetSupervisionDoesNotCrossLeaf(t *testing.T) {
	leafURL, hub := startFleetLeafHub(t)
	spokeA := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	gotLord := make(chan struct{}, 1)
	if _, err := hub.Subscribe("aether._lord.>", func(*nats.Msg) {
		select {
		case gotLord <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatalf("subscribe _lord on hub: %v", err)
	}
	agg := fleet.NewAggregator()
	if _, err := agg.Subscribe(hub); err != nil {
		t.Fatalf("aggregator subscribe: %v", err)
	}
	hub.Flush()

	// Publish both a supervision message and fleet health on the spoke, repeatedly.
	fleetSeen := false
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) && !fleetSeen {
		spokeA.Conn().Publish("aether._lord.counter.hb", []byte("beat"))
		publishHealthOn(t, spokeA, "sitea", "node-a-1")
		time.Sleep(150 * time.Millisecond)
		for _, n := range agg.Snapshot() {
			if n.App == "sitea" {
				fleetSeen = true
			}
		}
	}
	if !fleetSeen {
		t.Fatal("fleet health did not cross the leaf (precondition for the isolation check)")
	}
	// Fleet crossed; supervision must not have.
	select {
	case <-gotLord:
		t.Fatal("supervision (aether._lord.>) crossed the leaf to the hub; it must stay node-local")
	default:
	}
}
