package lord

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// These integration tests drive the real lord against an embedded NATS server. The lord spawns
// thralls as OS processes, so we need a real thrall process: TestMain re-execs this very test
// binary with AETHER_PROBE=1, which runs a minimal thrall through the actual Go SDK. That way
// the tests exercise the whole stack (lord -> ether -> registry -> SDK), not a mock.

// soakDispatch is a seam for the soak suite (build tag `soak`): its file registers
// a richer probe here in an init(). When the re-exec is a soak probe, the hook runs
// it and reports true so TestMain returns without entering the test runner. Nil
// without the tag, so this file's behavior is unchanged in normal CI.
var soakDispatch func() bool

func TestMain(m *testing.M) {
	if os.Getenv("AETHER_PROBE") == "1" {
		runProbe()
		return
	}
	if soakDispatch != nil && soakDispatch() {
		return
	}
	os.Exit(m.Run())
}

// runProbe is the thrall behind the AETHER_PROBE=1 re-exec: state is an int counter, `pid`
// exposes the OS pid (so a test can detect a restart), and `crash` exits abnormally to trigger
// supervision. On init it announces itself so the singleton test can count live instances.
func runProbe() {
	def := thrall.Def[int]{
		// Name empty -> taken from AETHER_NAME injected by the lord.
		Init: func(ctx *thrall.Ctx) (int, error) {
			_ = ctx.NATS.Publish("test.probe.started", []byte(ctx.Name))
			_ = ctx.NATS.Flush()
			return 0, nil
		},
		HandleCall: map[string]thrall.CallFn[int]{
			"get": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return s, s, nil },
			"pid": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return os.Getpid(), s, nil },
		},
		HandleCast: map[string]thrall.CastFn[int]{
			"inc":   func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { return s + 1, nil },
			"crash": func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { os.Exit(1); return s, nil },
		},
	}
	if err := thrall.Start(def); err != nil {
		os.Exit(2)
	}
}

// --- harness ---

func probeCmd(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return "AETHER_PROBE=1 " + exe
}

func startEmbedded(t *testing.T) *ether.Ether {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether.Start: %v", err)
	}
	t.Cleanup(eth.Stop)
	return eth
}

func startLord(t *testing.T, eth *ether.Ether, m *Manifest) *Lord {
	t.Helper()
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := l.Start(ctx); err != nil {
		cancel()
		t.Fatalf("lord.Start: %v", err)
	}
	t.Cleanup(func() {
		l.Stop()
		cancel()
	})
	return l
}

func spec(name, restart, scope string) ThrallSpec {
	return ThrallSpec{Name: name, Restart: restart, Scope: scope}
}

func manifest(t *testing.T, app, strategy string, specs ...ThrallSpec) *Manifest {
	t.Helper()
	cmd := probeCmd(t)
	for i := range specs {
		specs[i].Cmd = cmd
	}
	return &Manifest{App: app, Strategy: strategy, Thralls: specs}
}

// waitFor polls cond every 20ms until it holds or the timeout elapses. Polling (not a fixed
// sleep) keeps the tests deterministic against process startup and restart timing.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout after %s waiting for %s", timeout, desc)
}

func waitReady(t *testing.T, eth *ether.Ether, name string) {
	t.Helper()
	reg, err := registry.Open(eth.Conn())
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	waitFor(t, 5*time.Second, "thrall "+name+" ready in registry", func() bool {
		e, ok, err := reg.Get(name)
		return err == nil && ok && e.Status == "ready" && e.PID > 0
	})
}

func callInt(t *testing.T, nc *nats.Conn, app, name, op string) int {
	t.Helper()
	v, ok := tryCallInt(nc, app, name, op)
	if !ok {
		t.Fatalf("call %s.%s failed", name, op)
	}
	return v
}

func tryCallInt(nc *nats.Conn, app, name, op string) (int, bool) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: op,
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(wire.Call(app, name), data, 500*time.Millisecond)
	if err != nil {
		return 0, false
	}
	var reply wire.Envelope
	if json.Unmarshal(msg.Data, &reply) != nil || reply.Status == "error" {
		return 0, false
	}
	var v int
	if json.Unmarshal(reply.Payload, &v) != nil {
		return 0, false
	}
	return v, true
}

func cast(t *testing.T, nc *nats.Conn, app, name, op string) {
	t.Helper()
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCast, To: name, Op: op,
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(e)
	if err := nc.Publish(wire.Cast(app, name), data); err != nil {
		t.Fatalf("cast %s.%s: %v", name, op, err)
	}
	_ = nc.Flush()
}

// --- tests ---

func TestStartRegisterAndCall(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()

	// Lord started the thrall, it registered and is addressable via registry lookup.
	waitReady(t, eth, "probe")

	// End-to-end request/reply and fire-and-forget over the ether.
	if got := callInt(t, nc, app, "probe", "get"); got != 0 {
		t.Fatalf("initial get: got %d, want 0", got)
	}
	cast(t, nc, app, "probe", "inc")
	waitFor(t, 2*time.Second, "cast inc applied", func() bool {
		v, ok := tryCallInt(nc, app, "probe", "get")
		return ok && v == 1
	})
}

func TestOneForOneRestart(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	pid1 := callInt(t, nc, app, "probe", "pid")
	cast(t, nc, app, "probe", "crash")

	var pid2 int
	waitFor(t, 10*time.Second, "one_for_one restart with a fresh pid", func() bool {
		v, ok := tryCallInt(nc, app, "probe", "pid")
		if ok && v != pid1 {
			pid2 = v
			return true
		}
		return false
	})
	if pid2 == 0 || pid2 == pid1 {
		t.Fatalf("expected a restarted thrall with a new pid, got %d (was %d)", pid2, pid1)
	}
}

func TestOneForAllRestart(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_all",
		spec("a", "permanent", "local"),
		spec("b", "permanent", "local"),
	))
	nc := eth.Conn()
	waitReady(t, eth, "a")
	waitReady(t, eth, "b")

	pidA1 := callInt(t, nc, app, "a", "pid")
	pidB1 := callInt(t, nc, app, "b", "pid")

	// Crash one sibling; one_for_all must restart the whole group.
	cast(t, nc, app, "a", "crash")

	waitFor(t, 15*time.Second, "one_for_all restart of both siblings", func() bool {
		va, oka := tryCallInt(nc, app, "a", "pid")
		vb, okb := tryCallInt(nc, app, "b", "pid")
		return oka && okb && va != pidA1 && vb != pidB1
	})
}

func TestSingletonFencing(t *testing.T) {
	const app = "itest"
	eth1 := startEmbedded(t)

	// A second lord against the SAME NATS, connected as an external client.
	eth2, err := ether.Start(context.Background(), ether.Config{Mode: "external", URL: eth1.URL()})
	if err != nil {
		t.Fatalf("second ether: %v", err)
	}
	t.Cleanup(eth2.Stop)

	// Count how many singleton instances actually boot, and how many lords win the lock.
	var started, acquired int32
	if _, err := eth1.Conn().Subscribe("test.probe.started", func(*nats.Msg) {
		atomic.AddInt32(&started, 1)
	}); err != nil {
		t.Fatalf("subscribe started: %v", err)
	}
	if _, err := eth1.Conn().Subscribe(wire.Events, func(m *nats.Msg) {
		var ev struct {
			Event string `json:"event"`
			Name  string `json:"name"`
		}
		if json.Unmarshal(m.Data, &ev) == nil && ev.Event == "lock_acquired" && ev.Name == "counter-single" {
			atomic.AddInt32(&acquired, 1)
		}
	}); err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	_ = eth1.Conn().Flush()

	startLord(t, eth1, manifest(t, app, "one_for_one", spec("counter-single", "permanent", "singleton")))
	startLord(t, eth2, manifest(t, app, "one_for_one", spec("counter-single", "permanent", "singleton")))

	// One instance must come up (whichever lord wins the distributed KV lock).
	waitFor(t, 5*time.Second, "singleton instance to start", func() bool {
		return atomic.LoadInt32(&started) >= 1
	})

	// Prove the negative: the other lord is fenced out. Bounded grace, since asserting that a
	// second instance never appears is inherently a wait for absence.
	time.Sleep(1 * time.Second)

	if n := atomic.LoadInt32(&started); n != 1 {
		t.Fatalf("singleton fencing: expected exactly 1 instance, got %d", n)
	}
	if n := atomic.LoadInt32(&acquired); n != 1 {
		t.Fatalf("singleton fencing: expected exactly 1 lock acquisition, got %d", n)
	}
}
