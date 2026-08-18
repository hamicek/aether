//go:build soak

package lord

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/singleton"
	"github.com/hamicek/aether/internal/soak"
)

// runLordHost is the process behind AETHER_LORD_HOST=1: a standalone lord on the
// external bus running one singleton probe. The failover test spawns two of these as
// OS processes and SIGKILLs whole nodes - the faithful way to test a distributed
// singleton, since the lock is held by the lord (which the in-process harness cannot
// kill). Its children inherit its process group, so killing the group takes the probe
// with it (a node crash, not an orphaned singleton).
func runLordHost() {
	url := os.Getenv("AETHER_LORD_BUS")
	app := os.Getenv("AETHER_LORD_APP")
	if url == "" || app == "" {
		os.Exit(3)
	}
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "external", URL: url})
	if err != nil {
		os.Exit(4)
	}
	exe, err := os.Executable()
	if err != nil {
		os.Exit(5)
	}
	// Scope is singleton by default (the failover/orphan tests), or "local" for the
	// lord-liveness orphan test, which needs a plain non-singleton thrall.
	scope := os.Getenv("AETHER_LORD_SCOPE")
	if scope == "" {
		scope = "singleton"
	}
	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls: []ThrallSpec{{
			Name: "single", Restart: "permanent", Scope: scope,
			Cmd: "AETHER_SOAK_PROBE=1 " + exe,
		}},
	}
	l, err := New(m, eth)
	if err != nil {
		os.Exit(6)
	}
	if err := l.Start(context.Background()); err != nil {
		os.Exit(7)
	}
	select {} // blocks until the test SIGKILLs this node's process group
}

// --- helpers ---

func alive(pid int) bool { return pid > 0 && syscall.Kill(pid, 0) == nil }

// pgidOf returns the process-group id of pid (the group leader = the lord host that
// spawned it), so the test can SIGKILL the whole node.
func pgidOf(pid int) int {
	out, err := exec.Command("ps", "-o", "pgid=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0
	}
	return v
}

// startLordHost re-execs the test binary as a lord node (a singleton probe) in its own group.
func startLordHost(t *testing.T, url, app string) *exec.Cmd {
	return startLordHostScope(t, url, app, "singleton")
}

// startLordHostScope re-execs the test binary as a lord node running a probe of the given scope
// (singleton or local), in its own process group.
func startLordHostScope(t *testing.T, url, app, scope string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(),
		"AETHER_LORD_HOST=1", "AETHER_LORD_BUS="+url, "AETHER_LORD_APP="+app, "AETHER_LORD_SCOPE="+scope)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start lord host: %v", err)
	}
	return cmd
}

// killNode SIGKILLs a lord host's process group and reaps it. Since AE-013 a thrall runs in
// its OWN process group (child.spawn sets Setpgid so the lord can kill a child's subtree
// without suicide), so this reaps the lord host but NOT its singleton probe - a node is a
// process tree spanning two groups. Use crashNode to bring the whole node down.
func killNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	go cmd.Wait()
}

// killProbeGroup SIGKILLs the process group of a singleton probe (its own group leader since
// AE-013). A dead pid yields pgid 0 and is skipped, so it is safe to call on a stale pid.
func killProbeGroup(probePID int) {
	if pg := pgidOf(probePID); pg > 0 {
		_ = syscall.Kill(-pg, syscall.SIGKILL)
	}
}

// crashNode simulates a full node crash: it kills both the lord host's group and its singleton
// probe's group, so the probe dies WITH the node rather than being orphaned (which the lord-side
// lock cannot cover). Read the probe pid before calling - the probe must still be live.
func crashNode(cmd *exec.Cmd, probePID int) {
	killNode(cmd)
	killProbeGroup(probePID)
}

func failoverRounds(d time.Duration) int {
	n := int(d / (12 * time.Second))
	if n < 2 {
		n = 2
	}
	if n > 15 {
		n = 15
	}
	return n
}

// TestSoakSingletonFailover repeatedly kills the lord node holding the singleton and
// checks a new instance takes over within the bar, with never two live at once.
func TestSoakSingletonFailover(t *testing.T) {
	// This test runs two lord hosts for one app and expects cross-lord singleton failover. That
	// topology is now refused at startup (one lord per app, AE-062) - and never actually worked:
	// the second lord supersedes the per-app lord-liveness epoch and the singleton self-terminates
	// (DESIGN 14). Reworking or removing this for the single-lord / leaf-isolation model is a
	// follow-up; skipping so it does not fail on the enforced refusal.
	t.Skip("two lords per app is now refused (AE-062); singleton soak needs a single-lord rework")

	cfg := resolveSoakConfig(t)
	t.Logf("soak singleton: profile=%s duration=%s seed=%d", cfg.profile, cfg.duration, cfg.seed)

	const app = "single"
	eth := startEmbedded(t)
	nc := eth.Conn()
	started := subscribeLifecycle(t, nc, probeStartedSubject)
	url := eth.URL()

	// Two lord nodes race for the singleton; one holds it, the other stands by.
	hosts := []*exec.Cmd{startLordHost(t, url, app), startLordHost(t, url, app)}
	seen := []int{}
	t.Cleanup(func() {
		for _, h := range hosts {
			killNode(h)
		}
		// Probes live in their own groups, so killing the host groups leaves them orphaned;
		// reap any still-live probe explicitly (a stale pid yields pgid 0 and is skipped).
		for _, p := range seen {
			killProbeGroup(p)
		}
	})

	report := soak.Report{Profile: cfg.profile, Duration: cfg.duration, Seed: cfg.seed, Bars: soak.DefaultBars()}

	first := nextLifecycle(t, started, 10*time.Second, "first singleton instance")
	curPID := first.PID
	seen = append(seen, curPID)
	maxLive := 1
	var failoverMax time.Duration

	for round := 0; round < failoverRounds(cfg.duration); round++ {
		// Identify the node that holds the live probe by walking the probe's process tree up to
		// its lord host (robust to the probe living in its own group since AE-013), not by pgid.
		hostPIDs := make([]int, len(hosts))
		for i, h := range hosts {
			if h.Process != nil {
				hostPIDs[i] = h.Process.Pid
			}
		}
		holder := lordHostAncestor(curPID, hostPIDs)
		killedIdx := -1
		for i, h := range hosts {
			if h.Process != nil && h.Process.Pid == holder {
				killedIdx = i
				break
			}
		}
		if killedIdx < 0 {
			t.Fatalf("round %d: could not map live probe pid=%d to a lord host", round, curPID)
		}
		killed := hosts[killedIdx]
		hosts[killedIdx] = startLordHost(t, url, app) // spin up a replacement standby

		// Crash the whole node - lord host AND its probe's group - so the probe dies with the
		// node (a process tree) rather than being orphaned. Read the probe pid while it is live.
		crashPID := curPID
		killAt := time.Now()
		crashNode(killed, crashPID)

		// Wait for a new instance (a different PID) to come up = failover.
		var next lifecycleEvent
		deadline := time.Now().Add(8 * time.Second)
		for {
			e := nextLifecycle(t, started, time.Until(deadline), "failover instance")
			if e.PID != curPID {
				next = e
				break
			}
		}
		failover := time.Since(killAt)
		if failover > failoverMax {
			failoverMax = failover
		}

		// One live instance: the crashed node's probe must be dead by the time the new one is up.
		if alive(curPID) {
			t.Errorf("round %d: old instance pid=%d still alive when new pid=%d started",
				round, curPID, next.PID)
		}
		seen = append(seen, next.PID)
		live := 0
		for _, p := range seen {
			if alive(p) {
				live++
			}
		}
		if live > maxLive {
			maxLive = live
		}

		curPID = next.PID
		report.Failovers++
	}

	report.FailoverMax = failoverMax
	report.MaxLiveInstances = maxLive
	finishSoak(t, report, cfg.reportPath)
}

// ppidOf returns the parent pid of pid (0 on error).
func ppidOf(pid int) int {
	out, err := exec.Command("ps", "-o", "ppid=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0
	}
	return v
}

// lordHostAncestor walks up the process tree from pid until it hits one of the known lord-host
// pids, and returns it (0 if none). It is robust to whether `sh -c` execs into or forks the
// probe, so it does not depend on a fixed number of hops between the probe and its lord host.
func lordHostAncestor(pid int, hosts []int) int {
	for hop := 0; hop < 8 && pid > 1; hop++ {
		for _, h := range hosts {
			if pid == h {
				return h
			}
		}
		pid = ppidOf(pid)
	}
	return 0
}

// TestSoakSingletonOrphanFencing kills ONLY the lord process holding the singleton (not its
// whole group), so the probe is left orphaned - the case the lord-side lock cannot cover. It
// proves the thrall's own fencing: the orphan self-terminates once it loses the lock, so two
// live instances never persist. Without fencing the orphan would run forever and this test
// would fail (the standby lord starts a second instance after the lock expires).
func TestSoakSingletonOrphanFencing(t *testing.T) {
	// Same reason as TestSoakSingletonFailover: two lord hosts for one app is now refused at
	// startup (AE-062). Needs a single-lord / leaf-isolation rework - follow-up.
	t.Skip("two lords per app is now refused (AE-062); singleton soak needs a single-lord rework")

	const app = "orphan"
	eth := startEmbedded(t)
	nc := eth.Conn()
	started := subscribeLifecycle(t, nc, probeStartedSubject)
	url := eth.URL()

	// Two lord nodes race for the singleton; one holds it, the other stands by to take over.
	hosts := []*exec.Cmd{startLordHost(t, url, app), startLordHost(t, url, app)}
	t.Cleanup(func() {
		for _, h := range hosts {
			killNode(h)
		}
	})

	// The orphan must self-terminate within the lock TTL (until the key expires) plus the
	// fencing lease, with margin. The two-instance overlap is bounded by this same window.
	const reapBound = 2*singleton.TTL + 4*time.Second

	first := nextLifecycle(t, started, 10*time.Second, "first singleton instance")
	curPID := first.PID
	var maxReap, maxOverlap time.Duration

	const rounds = 3
	for round := 0; round < rounds; round++ {
		hostPIDs := make([]int, len(hosts))
		for i, h := range hosts {
			if h.Process != nil {
				hostPIDs[i] = h.Process.Pid
			}
		}
		holder := lordHostAncestor(curPID, hostPIDs)
		killedIdx := -1
		for i, h := range hosts {
			if h.Process != nil && h.Process.Pid == holder {
				killedIdx = i
				break
			}
		}
		if killedIdx < 0 {
			t.Fatalf("round %d: could not map live probe pid=%d to a lord host", round, curPID)
		}

		orphanPID := curPID
		killAt := time.Now()
		// Kill ONLY the lord process (positive pid), NOT its group: the probe is left orphaned,
		// exactly the window the lord-side lock cannot close.
		if err := syscall.Kill(hosts[killedIdx].Process.Pid, syscall.SIGKILL); err != nil {
			t.Fatalf("round %d: kill lord process: %v", round, err)
		}
		killed := hosts[killedIdx]
		go killed.Wait()
		hosts[killedIdx] = startLordHost(t, url, app) // replacement standby for the next round

		// Failover: the standby brings up a new instance (different PID) after the lock expires.
		var next lifecycleEvent
		deadline := time.Now().Add(reapBound)
		for {
			e := nextLifecycle(t, started, time.Until(deadline), "failover instance after orphan")
			if e.PID != orphanPID {
				next = e
				break
			}
		}
		newUpAt := time.Now()

		// Fencing: the orphaned instance must self-terminate on its own within the bound.
		var deathAt time.Time
		reapDeadline := killAt.Add(reapBound)
		for {
			if !alive(orphanPID) {
				deathAt = time.Now()
				break
			}
			if time.Now().After(reapDeadline) {
				t.Fatalf("round %d fencing: orphaned instance pid=%d still alive %s after its lord was killed (bar %s)",
					round, orphanPID, time.Since(killAt), reapBound)
			}
			time.Sleep(50 * time.Millisecond)
		}

		reap := deathAt.Sub(killAt)
		overlap := deathAt.Sub(newUpAt) // time both old and new were live at once
		if overlap < 0 {
			overlap = 0
		}
		if reap > maxReap {
			maxReap = reap
		}
		if overlap > maxOverlap {
			maxOverlap = overlap
		}
		t.Logf("round %d: orphan pid=%d reaped in %s, new pid=%d, overlap=%s", round, orphanPID, reap, next.PID, overlap)

		curPID = next.PID
	}

	// Steady state: exactly one live instance remains.
	if !alive(curPID) {
		t.Errorf("final: expected the current instance pid=%d to be live", curPID)
	}
	t.Logf("orphan fencing: %d rounds, max reap=%s, max overlap=%s (lease=%s)", rounds, maxReap, maxOverlap, singleton.TTL)
}
