package lord

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
)

// TestConfiguredHeartbeatIntervalPropagates proves the whole chain end to end: a manifest
// [liveness] interval reaches the thrall (via env injection) and the thrall beats at that faster
// rate - so a tightened config really does speed up detection.
func TestConfiguredHeartbeatIntervalPropagates(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "hbcfg", "one_for_one", spec("fast", "permanent", "local"))
	m.Liveness = Liveness{HeartbeatIntervalMs: 200, StaleAfterMisses: 3}
	startLord(t, eth, m)
	waitReady(t, eth, "fast")

	var count int32
	sub, err := eth.Conn().Subscribe(wire.Heartbeat("fast"), func(*nats.Msg) { atomic.AddInt32(&count, 1) })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	time.Sleep(1200 * time.Millisecond)
	// At 200ms the thrall beats ~6x in 1.2s; the default 2s would give ~0-1.
	if n := atomic.LoadInt32(&count); n < 3 {
		t.Errorf("heartbeats in ~1.2s = %d, want >= 3 (200ms interval - config not propagated to the thrall?)", n)
	}
}

// metricValue extracts the value of a single exposition line by its `name{labels}` prefix.
func metricValue(t *testing.T, exposition, seriesPrefix string) (float64, bool) {
	t.Helper()
	for _, line := range strings.Split(exposition, "\n") {
		if strings.HasPrefix(line, seriesPrefix+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, seriesPrefix)), 64)
			if err != nil {
				t.Fatalf("bad metric line %q: %v", line, err)
			}
			return v, true
		}
	}
	return 0, false
}

// TestHeartbeatMissMarksStaleAndRecovers drives the real reaper against an embedded NATS
// server and a real beating thrall. A miss is forced by backdating the thrall's last-seen
// timestamp (a hung process the OS watcher cannot observe); the reaper must mark it stale
// and count the miss, then flip it back to ready when heartbeats resume on their own.
func TestHeartbeatMissMarksStaleAndRecovers(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "hb", "one_for_one", spec("beater", "permanent", "local"))

	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	// Short, deterministic reaper timing so the test does not wait real heartbeat intervals.
	l.hbCheckEvery = 30 * time.Millisecond
	l.hbMissAfter = 150 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	if err := l.Start(ctx); err != nil {
		cancel()
		t.Fatalf("lord.Start: %v", err)
	}
	t.Cleanup(func() { l.Stop(); cancel() })

	waitReady(t, eth, "beater")

	reg, err := registry.Open(eth.Conn())
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}

	// Force a miss: backdate last-seen far beyond hbMissAfter. The reaper (or a direct check)
	// then observes no recent heartbeat.
	l.mu.Lock()
	l.lastSeen["beater"] = time.Now().Add(-time.Second)
	l.mu.Unlock()
	l.checkHeartbeats(time.Now())

	waitFor(t, 2*time.Second, "beater marked stale", func() bool {
		e, ok, err := reg.Get("beater")
		return err == nil && ok && e.Status == "stale"
	})

	// The miss must be counted exactly once for this outage.
	if out := scrape(t, l); !strings.Contains(out, `aether_heartbeat_misses_total{name="beater"} 1`) {
		t.Errorf("heartbeat miss not counted once:\n%s", out)
	}

	// Recovery: the thrall keeps beating (every 2s), so within a few seconds it flips back to
	// ready on its own.
	waitFor(t, 4*time.Second, "beater recovers to ready", func() bool {
		e, ok, err := reg.Get("beater")
		return err == nil && ok && e.Status == "ready"
	})
}

// TestLivenessStatusWritesAreOrderedBySeq proves the fix for the reaper/heartbeat race: a
// resuming heartbeat and the reaper can decide "ready" and "stale" concurrently, and their
// registry writes happen outside l.mu. Each decision carries a version stamped under l.mu, so
// a write that arrives out of order (an older "stale" landing after a newer "ready") is
// dropped rather than leaving the registry stuck on stale until the next heartbeat.
func TestLivenessStatusWritesAreOrderedBySeq(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "hbord", "one_for_one", spec("x", "permanent", "local"))
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}

	reg, err := registry.Open(eth.Conn())
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}

	// A newer "ready" decision (seq 5) followed by an older, reordered "stale" decision (seq 4):
	// the stale write must be dropped, so the registry stays on ready.
	l.applyStatus("x", 100, "ready", 5)
	l.applyStatus("x", 100, "stale", 4)
	if e, ok, err := reg.Get("x"); err != nil || !ok {
		t.Fatalf("registry.Get: ok=%v err=%v", ok, err)
	} else if e.Status != "ready" {
		t.Fatalf("status = %q, want ready (an older stale write must not clobber a newer ready)", e.Status)
	}

	// A genuinely newer "stale" decision (seq 6) does take effect.
	l.applyStatus("x", 100, "stale", 6)
	if e, _, _ := reg.Get("x"); e.Status != "stale" {
		t.Fatalf("status = %q, want stale (a newer decision must win)", e.Status)
	}
}

// TestHeartbeatMetricsRecorded proves the lord folds a real thrall's self-reported mailbox
// metrics (carried on the heartbeat) into the /metrics exposition.
func TestHeartbeatMetricsRecorded(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "hbm", "one_for_one", spec("worker", "permanent", "local"))
	l := startLord(t, eth, m)

	waitReady(t, eth, "worker")

	// Drive some work so processed_total climbs.
	for i := 0; i < 3; i++ {
		cast(t, eth.Conn(), "hbm", "worker", "inc")
	}

	// A heartbeat carrying the metrics arrives within one interval (2s) plus slack.
	waitFor(t, 5*time.Second, "worker processed metrics recorded", func() bool {
		v, ok := metricValue(t, scrape(t, l), `aether_processed_total{name="worker"}`)
		return ok && v >= 3
	})

	out := scrape(t, l)
	if _, ok := metricValue(t, out, `aether_mailbox_depth{name="worker"}`); !ok {
		t.Errorf("mailbox depth not reported:\n%s", out)
	}
	if !strings.Contains(out, `aether_mailbox_latency_ms{name="worker"}`) {
		t.Errorf("mailbox latency not reported:\n%s", out)
	}
}
