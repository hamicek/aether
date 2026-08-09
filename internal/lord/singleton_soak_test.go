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
	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls: []ThrallSpec{{
			Name: "single", Restart: "permanent", Scope: "singleton",
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

// startLordHost re-execs the test binary as a lord node in its own process group.
func startLordHost(t *testing.T, url, app string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), "AETHER_LORD_HOST=1", "AETHER_LORD_BUS="+url, "AETHER_LORD_APP="+app)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start lord host: %v", err)
	}
	return cmd
}

// killNode SIGKILLs a lord host's whole process group (lord + its singleton probe) and
// reaps it.
func killNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	go cmd.Wait()
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
	cfg := resolveSoakConfig(t)
	t.Logf("soak singleton: profile=%s duration=%s seed=%d", cfg.profile, cfg.duration, cfg.seed)

	const app = "single"
	eth := startEmbedded(t)
	nc := eth.Conn()
	started := subscribeLifecycle(t, nc, probeStartedSubject)
	url := eth.URL()

	// Two lord nodes race for the singleton; one holds it, the other stands by.
	hosts := []*exec.Cmd{startLordHost(t, url, app), startLordHost(t, url, app)}
	t.Cleanup(func() {
		for _, h := range hosts {
			killNode(h)
		}
	})

	report := soak.Report{Profile: cfg.profile, Duration: cfg.duration, Seed: cfg.seed, Bars: soak.DefaultBars()}

	first := nextLifecycle(t, started, 10*time.Second, "first singleton instance")
	curPID := first.PID
	seen := []int{curPID}
	maxLive := 1
	var failoverMax time.Duration

	for round := 0; round < failoverRounds(cfg.duration); round++ {
		// Identify and kill the node whose group owns the live probe.
		holderPgid := pgidOf(curPID)
		var killed *exec.Cmd
		for i, h := range hosts {
			if h.Process != nil && h.Process.Pid == holderPgid {
				killed = h
				hosts[i] = startLordHost(t, url, app) // spin up a replacement standby
				break
			}
		}
		if killed == nil {
			t.Fatalf("round %d: could not map live probe pid=%d (pgid=%d) to a lord host", round, curPID, holderPgid)
		}
		killAt := time.Now()
		killNode(killed)

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

		// Fencing: the killed instance must be dead by the time the new one is up.
		if alive(curPID) {
			t.Errorf("round %d fencing: old instance pid=%d still alive when new pid=%d started",
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
