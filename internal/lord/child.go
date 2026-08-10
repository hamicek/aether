package lord

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

const (
	defaultGrace   = 5 * time.Second        // how long to wait for a controlled shutdown after ctl:drain
	sigtermGrace   = 2 * time.Second        // how long to wait after SIGTERM before sending SIGKILL
	restartBackoff = 300 * time.Millisecond // pause before restart (a crash-loop does not burn CPU)
)

// child = a single running thrall process supervised by the lord.
type child struct {
	spec         ThrallSpec
	natsURL      string
	app          string
	caPath       string // TLS CA path injected to the thrall (empty = none)
	nkeySeed     string // nkey seed path injected to the thrall (empty = none)
	hbIntervalMs int    // heartbeat interval (ms) injected so the thrall beats at the configured rate

	dynamic bool        // started at runtime (ctx.StartChild), not from the manifest
	live    atomic.Bool // the process is currently running
	retired atomic.Bool // stopped on purpose (StopChild) -> must not be restarted

	mu     sync.Mutex
	cmd    *exec.Cmd
	done   chan struct{} // closed as soon as the current process generation ends
	gen    uint64        // process generation; grows with each spawn (resolves restart races)
	starts []time.Time   // start times used to evaluate the restart-intensity window
}

// env builds the environment injected into a thrall process: the bus address and identity,
// the durable flag, the (paths to) TLS/nkey material for a secured bus, and the lord's
// logging configuration so the thrall logs in the same format and level. Secrets stay in
// files - only their paths are passed, never the key material itself.
func (c *child) env() []string {
	env := append(os.Environ(),
		"AETHER_NATS_URL="+c.natsURL,
		"AETHER_APP="+c.app,
		"AETHER_NAME="+c.spec.Name,
	)
	if c.spec.Durable {
		env = append(env, "AETHER_DURABLE=1")
	}
	if c.caPath != "" {
		env = append(env, "AETHER_NATS_CA="+c.caPath)
	}
	if c.nkeySeed != "" {
		env = append(env, "AETHER_NATS_NKEY_SEED="+c.nkeySeed)
	}
	for _, key := range []string{obs.EnvLogLevel, obs.EnvLogFormat} {
		if v := os.Getenv(key); v != "" {
			env = append(env, key+"="+v)
		}
	}
	// Appended last, so the manifest-configured interval is authoritative: it overrides any
	// AETHER_HEARTBEAT_INTERVAL_MS inherited from the lord's own environment (last value wins),
	// keeping the thrall's beat and the lord's reaper threshold derived from the same source.
	if c.hbIntervalMs > 0 {
		env = append(env, obs.EnvHeartbeatIntervalMs+"="+strconv.Itoa(c.hbIntervalMs))
	}
	return env
}

// spawn starts a thrall as an OS process with an injected environment. `ctx` is
// the process context (NOT the signal one) - children must not die on SIGINT before
// the graceful drain completes; the final SIGKILL is driven by the lord via cancelling this ctx.
func (c *child) spawn(ctx context.Context) (gen uint64, err error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", c.spec.Cmd)
	cmd.Env = c.env()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the thrall in its own process group: the command is `sh -c <cmd>`, so the real
	// thrall is a grandchild of the lord. Killing only `sh` would orphan it (and leave it
	// holding the inherited stdout/stderr). We put the whole tree in one group and signal
	// the group (negative pid) on cancel/drain, so no thrall survives its lord.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Context cancel (procCancel = hard shutdown) kills the group, not just sh; WaitDelay
	// bounds how long Wait blocks on inherited pipes if a process ignores the signal.
	cmd.Cancel = func() error { killGroup(cmd.Process, syscall.SIGKILL); return nil }
	cmd.WaitDelay = sigtermGrace

	done := make(chan struct{})
	c.mu.Lock()
	c.cmd = cmd
	c.done = done
	c.gen++
	gen = c.gen
	c.starts = append(c.starts, time.Now())
	c.mu.Unlock()

	c.live.Store(true)
	if err := cmd.Start(); err != nil {
		c.live.Store(false)
		return gen, err
	}
	return gen, nil
}

// wait blocks until the process ends, closes `done` and returns whether it was an abnormal exit.
func (c *child) wait() (abnormal bool) {
	c.mu.Lock()
	cmd := c.cmd
	done := c.done
	c.mu.Unlock()

	err := cmd.Wait()
	c.live.Store(false)
	close(done)
	return err != nil
}

// currentGen returns the current process generation (to recognize stale exit events).
func (c *child) currentGen() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// running reports whether the process is currently running.
func (c *child) running() bool { return c.live.Load() }

// kill hard-terminates the process group (SIGKILL) - used when the lord loses the singleton lock.
func (c *child) kill() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		killGroup(c.cmd.Process, syscall.SIGKILL)
	}
}

// killGroup signals the whole process group of a spawned thrall (negative pid). Thralls
// are spawned with Setpgid, so the group holds `sh` and the real thrall grandchild; a
// plain Process.Kill would hit only `sh` and orphan the thrall.
func killGroup(proc *os.Process, sig syscall.Signal) {
	if proc == nil {
		return
	}
	_ = syscall.Kill(-proc.Pid, sig)
}

// pid returns the PID of the current process generation (0 if not running).
func (c *child) pid() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		return c.cmd.Process.Pid
	}
	return 0
}

// restartsWithin returns the number of starts within the last window (for restart-intensity).
func (c *child) restartsWithin(window time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-window)
	n := 0
	for _, t := range c.starts {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

// requestDrain performs a controlled shutdown: it sends ctl:drain over the ether and waits
// for the thrall to exit on its own (drains the mailbox, calls terminate, disconnects). If it
// does not fit within grace, it escalates to SIGTERM and finally to SIGKILL.
func (c *child) requestDrain(nc *nats.Conn, grace time.Duration) {
	c.mu.Lock()
	done := c.done
	var proc *os.Process
	if c.cmd != nil {
		proc = c.cmd.Process
	}
	name := c.spec.Name
	c.mu.Unlock()
	if done == nil || proc == nil {
		return // the child is not running (e.g. a singleton waiter) - nothing to drain
	}

	msg, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
	_ = nc.Publish(wire.Ctl(name), msg)
	_ = nc.Flush()

	// 1) wait for a controlled shutdown
	select {
	case <-done:
		return
	case <-time.After(grace):
	}

	// 2) escalation: SIGTERM to the whole group (sh + the real thrall grandchild)
	killGroup(proc, syscall.SIGTERM)
	select {
	case <-done:
		return
	case <-time.After(sigtermGrace):
	}

	// 3) last resort: SIGKILL to the whole group
	killGroup(proc, syscall.SIGKILL)
	<-done
}
