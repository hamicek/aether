//go:build soak

package lord

import (
	"syscall"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/lordlease"
)

// TestSoakLordDeathReapsOrphanThrall proves the lord-liveness fencing (AE-031). AE-013's
// process-group kill only fires when the lord actively kills its children (a graceful shutdown);
// when the lord process itself is SIGKILLed, its non-singleton local thrall is left orphaned in
// its OWN process group and the process-group kill never runs. The thrall must instead
// self-terminate by verifying its lord's KV lease: a replacement lord stamps a new epoch (or the
// lease TTL-expires), so the orphan detects its lord is gone and exits. This test kills only the
// lord process each round and asserts the orphaned local thrall reaps itself within the bound,
// leaving exactly one live instance (no lingering duplicate).
func TestSoakLordDeathReapsOrphanThrall(t *testing.T) {
	const app = "lordorphan"
	eth := startEmbedded(t)
	nc := eth.Conn()
	started := subscribeLifecycle(t, nc, probeStartedSubject)
	url := eth.URL()

	// The orphan self-terminates once its lord's lease is superseded by a replacement (or expires),
	// within the lease window plus margin.
	const reapBound = 2*lordlease.TTL + 4*time.Second

	curHost := startLordHostScope(t, url, app, "local")
	t.Cleanup(func() { killNode(curHost) })
	curPID := nextLifecycle(t, started, 10*time.Second, "first local instance").PID

	const rounds = 3
	var maxReap time.Duration
	for round := 0; round < rounds; round++ {
		orphanPID := curPID
		orphanHost := curHost
		killAt := time.Now()

		// Kill ONLY the lord process (positive pid), NOT its group: the local probe is left
		// orphaned in its own process group - exactly the window the process-group kill misses.
		if err := syscall.Kill(orphanHost.Process.Pid, syscall.SIGKILL); err != nil {
			t.Fatalf("round %d: kill lord process: %v", round, err)
		}
		go orphanHost.Wait()

		// A replacement lord (same app) establishes a new lease epoch and starts a fresh instance.
		curHost = startLordHostScope(t, url, app, "local")
		var next lifecycleEvent
		deadline := time.Now().Add(reapBound)
		for {
			e := nextLifecycle(t, started, time.Until(deadline), "replacement local instance")
			if e.PID != orphanPID {
				next = e
				break
			}
		}

		// Fencing: the orphaned instance must self-terminate on its own within the bound.
		var deathAt time.Time
		reapDeadline := killAt.Add(reapBound)
		for {
			if !alive(orphanPID) {
				deathAt = time.Now()
				break
			}
			if time.Now().After(reapDeadline) {
				t.Fatalf("round %d: orphaned local thrall pid=%d still alive %s after its lord was killed (bar %s)",
					round, orphanPID, time.Since(killAt), reapBound)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if reap := deathAt.Sub(killAt); reap > maxReap {
			maxReap = reap
		}
		t.Logf("round %d: orphan pid=%d reaped in %s, new pid=%d", round, orphanPID, deathAt.Sub(killAt), next.PID)
		curPID = next.PID
	}

	// Steady state: exactly one live instance remains (the current one).
	if !alive(curPID) {
		t.Errorf("final: expected the current instance pid=%d to be live", curPID)
	}
	t.Logf("lord-death orphan fencing: %d rounds, max reap=%s (lease=%s)", rounds, maxReap, lordlease.TTL)
}
