//go:build soak

package lord

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/hamicek/aether/internal/ether"
)

// Shared soak helpers for spawning and killing a lord as its own OS process (a "lord host").
// Used by the sustained-load / chaos suite (soak_test.go) and the lord-liveness orphan test
// (lordliveness_soak_test.go). Killing a real lord process is the faithful way to exercise the
// KV lease/lock fencing, which an in-process lord cannot.

// runLordHost is the process behind AETHER_LORD_HOST=1: a standalone lord on the external bus
// running one probe. Its child inherits its process group, so killing the group takes the probe
// with it (a node crash, not an orphaned thrall).
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
	// Scope is "singleton" by default, or "local" for the lord-liveness orphan test, which needs
	// a plain non-singleton thrall.
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

// alive reports whether pid is a live process.
func alive(pid int) bool { return pid > 0 && syscall.Kill(pid, 0) == nil }

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

// killNode SIGKILLs a lord host's process group and reaps it. Since AE-013 a thrall runs in its
// OWN process group (child.spawn sets Setpgid so the lord can kill a child's subtree without
// suicide), so this reaps the lord host but NOT its probe - a node is a process tree spanning two
// groups.
func killNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	go cmd.Wait()
}
