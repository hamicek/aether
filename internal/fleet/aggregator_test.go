package fleet

import (
	"testing"
	"time"
)

// TestAggregatorLiveView proves the aggregator holds every publishing node keyed by (app, lord_id),
// sorted, and reflects additions.
func TestAggregatorLiveView(t *testing.T) {
	a := NewAggregator()
	a.Ingest(Health{App: "site-b", LordID: "h2-2", IntervalMs: 1000})
	a.Ingest(Health{App: "site-a", LordID: "h1-1", IntervalMs: 1000})

	snap := a.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snap))
	}
	if snap[0].App != "site-a" || snap[1].App != "site-b" {
		t.Errorf("not sorted by app: %q, %q", snap[0].App, snap[1].App)
	}
	for _, n := range snap {
		if n.Stale {
			t.Errorf("node %s freshly ingested but marked stale", n.App)
		}
	}
}

// TestAggregatorStaleness proves a node goes stale after missing staleMultiple of its own publish
// interval, and comes back live on the next update.
func TestAggregatorStaleness(t *testing.T) {
	now := time.Unix(1000, 0)
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Ingest(Health{App: "site-a", LordID: "h1-1", IntervalMs: 1000}) // 1s interval -> stale after 3s

	if a.Snapshot()[0].Stale {
		t.Fatal("node stale immediately after ingest")
	}

	now = now.Add(2 * time.Second) // within 3x interval
	if a.Snapshot()[0].Stale {
		t.Error("node stale at 2s (threshold is 3s)")
	}

	now = now.Add(2 * time.Second) // total 4s > 3s
	if !a.Snapshot()[0].Stale {
		t.Error("node not stale at 4s (threshold is 3s)")
	}

	a.Ingest(Health{App: "site-a", LordID: "h1-1", IntervalMs: 1000}) // fresh update
	if a.Snapshot()[0].Stale {
		t.Error("node still stale after a fresh update")
	}
}

// TestAggregatorEviction proves a node silent far beyond staleness is dropped entirely, so a
// restarted lord (new pid -> new key) does not leave its old key lingering forever.
func TestAggregatorEviction(t *testing.T) {
	now := time.Unix(1000, 0)
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Ingest(Health{App: "a", LordID: "old-1", IntervalMs: 1000}) // 1s interval -> evict after 10s
	if len(a.Snapshot()) != 1 {
		t.Fatalf("want 1 node, got %d", len(a.Snapshot()))
	}

	now = now.Add(11 * time.Second) // old-1 now silent beyond the eviction window
	a.Ingest(Health{App: "a", LordID: "new-2", IntervalMs: 1000})

	snap := a.Snapshot()
	if len(snap) != 1 || snap[0].LordID != "new-2" {
		t.Fatalf("old node not evicted on restart: %+v", snap)
	}
}

// TestAggregatorStaleFallback proves a summary with no interval uses the 5s fallback rather than
// looking permanently live.
func TestAggregatorStaleFallback(t *testing.T) {
	now := time.Unix(1000, 0)
	a := NewAggregator()
	a.now = func() time.Time { return now }
	a.Ingest(Health{App: "x", LordID: "y", IntervalMs: 0}) // no interval -> 5s fallback, 3x = 15s

	now = now.Add(10 * time.Second)
	if a.Snapshot()[0].Stale {
		t.Error("stale at 10s with 15s threshold")
	}
	now = now.Add(10 * time.Second) // 20s > 15s
	if !a.Snapshot()[0].Stale {
		t.Error("not stale at 20s with 15s threshold")
	}
}
