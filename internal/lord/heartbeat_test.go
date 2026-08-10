package lord

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/registry"
)

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
