package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These tests run the real `aether up` as a process, because what they assert is the process's
// exit status - the one thing the supervisor above a node (systemd, a container runtime) acts on.
// TestMain re-executes this test binary as the CLI when asked to, so no separate build is needed.

const runAsCLI = "AETHER_TEST_RUN_AS_CLI"

func TestMain(m *testing.M) {
	if os.Getenv(runAsCLI) == "1" {
		main() // os.Args is [binary, "up", "-f", manifest], exactly as for the real binary
		return
	}
	os.Exit(m.Run())
}

// output collects a process's log while the test reads it concurrently.
type output struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *output) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

// startNode runs `aether up` on a manifest with one thrall that can never be kept running, with
// the given on_give_up line, and returns the process and its log.
func startNode(t *testing.T, onGiveUp string) (*exec.Cmd, *output) {
	t.Helper()
	dir := t.TempDir()
	manifest := filepath.Join(dir, "aether.toml")
	body := "app = \"exitcode\"\nrestart_intensity = { max = 1, within_ms = 60000 }\n" + onGiveUp +
		"\n[[thrall]]\nname = \"broken\"\ncmd  = \"sh -c 'exit 1'\"\n"
	if err := os.WriteFile(manifest, []byte(body), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	cmd := exec.Command(os.Args[0], "up", "-f", manifest)
	cmd.Dir = dir // the endpoint file is written to the working directory
	cmd.Env = append(os.Environ(), runAsCLI+"=1")
	log := &output{}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start aether up: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd, log
}

// waitExit waits for the process to end and returns its exit status.
func waitExit(t *testing.T, cmd *exec.Cmd, log *output) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatalf("wait: %v", err)
		}
		return cmd.ProcessState.ExitCode()
	case <-time.After(30 * time.Second):
		t.Fatalf("aether up did not exit; log:\n%s", log)
		return -1
	}
}

func awaitLog(t *testing.T, log *output, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(log.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("the log never said %q:\n%s", want, log)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestUpExitsNonZeroWhenTheLordGivesUp: with on_give_up = "exit" a crash loop the lord gives up
// on ends the process with its own exit status, after saying why.
func TestUpExitsNonZeroWhenTheLordGivesUp(t *testing.T) {
	cmd, log := startNode(t, `on_give_up = "exit"`)
	if code := waitExit(t, cmd, log); code != exitGaveUp {
		t.Fatalf("exit status = %d, want %d; log:\n%s", code, exitGaveUp, log)
	}
	for _, want := range []string{"restart-intensity exceeded - giving up", "node failed", `gave up on thrall \"broken\"`} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("the log lacks %q:\n%s", want, log)
		}
	}
	if _, err := os.Stat(filepath.Join(cmd.Dir, endpointFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the endpoint file was left behind (stat: %v): a failed node must clean up like a stopped one", err)
	}
}

// TestUpKeepsRunningAfterAGiveUpByDefault: without the setting the node behaves as it always
// did - the lord gives up on the thrall, stays up, and a requested shutdown is a clean exit.
func TestUpKeepsRunningAfterAGiveUpByDefault(t *testing.T) {
	cmd, log := startNode(t, "")
	awaitLog(t, log, "restart-intensity exceeded - giving up")

	time.Sleep(time.Second) // long enough for an exit to have happened, had the give-up caused one
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the node is no longer running after a give-up: %v; log:\n%s", err, log)
	}
	if strings.Contains(log.String(), "node failed") {
		t.Errorf("the node reported a failure with the default on_give_up:\n%s", log)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	if code := waitExit(t, cmd, log); code != 0 {
		t.Fatalf("exit status after a requested shutdown = %d, want 0; log:\n%s", code, log)
	}
}
