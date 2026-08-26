package lord

import (
	"context"
	"encoding/json"
	"os"
	"strings"
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
		// Idempotent is opt-in per thrall; the idempotence e2e sets AETHER_PROBE_IDEM to turn it on.
		Idempotent: os.Getenv("AETHER_PROBE_IDEM") == "1",
		HandleCall: map[string]thrall.CallFn[int]{
			"get": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return s, s, nil },
			"pid": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return os.Getpid(), s, nil },
			// inc_get increments and returns the new value: a second call with the same idempotency
			// key must return the same value (from cache) with the state incremented only once.
			"inc_get": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return s + 1, s + 1, nil },
			// call_escalate crashes from a call handler: the caller must get the "escalated"
			// error reply before the process dies, not a timeout.
			"call_escalate": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) {
				return nil, s, thrall.Escalate("call asked to crash")
			},
		},
		HandleCast: map[string]thrall.CastFn[int]{
			"inc":   func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { return s + 1, nil },
			"crash": func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { os.Exit(1); return s, nil },
			// escalate is the typed let-it-crash path: returning Escalate self-terminates the
			// thrall abnormally, so the lord restarts it per policy (contrast with `crash`,
			// which reaches for a raw os.Exit).
			"escalate": func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) {
				return s, thrall.Escalate("cast asked to crash")
			},
			// emit_trace publishes the trace of the message currently being handled to a
			// per-thrall subject, so a test can observe what reached the handler via ctx.
			"emit_trace": func(_ json.RawMessage, s int, ctx *thrall.Ctx) (int, error) {
				_ = ctx.NATS.Publish("test.trace."+ctx.Name, []byte(ctx.Trace))
				_ = ctx.NATS.Flush()
				return s, nil
			},
			// relay forwards to the "sink" thrall via ctx.Cast, which must propagate the
			// current trace to the downstream message (cross-hop propagation).
			"relay": func(_ json.RawMessage, s int, ctx *thrall.Ctx) (int, error) {
				_ = ctx.Cast("sink", "emit_trace", map[string]any{})
				return s, nil
			},
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

// callErrType issues a call and returns the error reply's type (e.g. "escalated"), reporting
// false if the call succeeded, timed out, or returned a non-error reply.
func callErrType(nc *nats.Conn, app, name, op string) (string, bool) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: op,
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(wire.Call(app, name), data, 500*time.Millisecond)
	if err != nil {
		return "", false
	}
	var reply wire.Envelope
	if json.Unmarshal(msg.Data, &reply) != nil || reply.Status != "error" || reply.Error == nil {
		return "", false
	}
	return reply.Error.Type, true
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

// castIdem publishes a cast carrying an idempotency key (each send still gets a unique envelope
// ID), so two calls with the same key are a genuine duplicate on an idempotent thrall.
func castIdem(t *testing.T, nc *nats.Conn, app, name, op, idem string) {
	t.Helper()
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Idem: idem, Kind: wire.KindCast, To: name, Op: op,
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(e)
	if err := nc.Publish(wire.Cast(app, name), data); err != nil {
		t.Fatalf("cast %s.%s: %v", name, op, err)
	}
	_ = nc.Flush()
}

// callIntIdem issues a call carrying an idempotency key and returns the integer reply.
func callIntIdem(nc *nats.Conn, app, name, op, idem string) (int, bool) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Idem: idem, Kind: wire.KindCall, To: name, Op: op,
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

// A cast handler returning Escalate self-terminates the thrall abnormally, so a permanent
// thrall is restarted through init - the typed let-it-crash path, with no manual os.Exit in
// the handler. The restart is fresh state from init, not a resumed old state.
func TestEscalateRestartsThrall(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	pid1 := callInt(t, nc, app, "probe", "pid")
	cast(t, nc, app, "probe", "inc") // dirty the state so a clean restart is observable
	cast(t, nc, app, "probe", "escalate")

	var pid2 int
	waitFor(t, 10*time.Second, "restart with a fresh pid after escalation", func() bool {
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
	if got := callInt(t, nc, app, "probe", "get"); got != 0 {
		t.Fatalf("state after escalate-restart = %d, want a clean 0 from init", got)
	}
}

// An escalation from a call handler must reply the caller a distinguishable "escalated" error
// before the process dies, so the caller learns of the restart instead of waiting out the
// request timeout. Afterwards the permanent thrall comes back with a fresh pid.
func TestEscalateCallerGetsError(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	pid1 := callInt(t, nc, app, "probe", "pid")

	errType, ok := callErrType(nc, app, "probe", "call_escalate")
	if !ok {
		t.Fatal("expected an error reply from an escalating call, got none (caller hung until timeout?)")
	}
	if errType != "escalated" {
		t.Fatalf("error reply type = %q, want %q", errType, "escalated")
	}

	waitFor(t, 10*time.Second, "restart with a fresh pid after a call escalation", func() bool {
		v, ok := tryCallInt(nc, app, "probe", "pid")
		return ok && v != pid1
	})
}

// A durable cast that escalates must not be left pending for redelivery (which would crash the
// thrall again on every restart until restart-intensity gives up). The SDK acks the poison cast
// before the crash, so after the restart the durable consumer has nothing ack-pending and the
// thrall comes back clean. Asserting on the consumer's ack-pending count is deterministic;
// waiting for an actual redelivery is not, since AckWait is 30s.
func TestEscalateDurableCastNotRedelivered(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "probe", Cmd: probeCmd(t), Restart: "permanent", Scope: "local", Durable: true}},
	}
	startLord(t, eth, m)
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	pid1 := callInt(t, nc, app, "probe", "pid")
	cast(t, nc, app, "probe", "escalate") // durable cast: stored in the stream, delivered once

	waitFor(t, 10*time.Second, "durable thrall restarts after escalation", func() bool {
		v, ok := tryCallInt(nc, app, "probe", "pid")
		return ok && v != pid1
	})

	// The poison cast was acked before the crash: the durable consumer has nothing pending and
	// nothing ack-pending, so it will never be redelivered. Without the ack-before-crash it would
	// sit ack-pending until AckWait and then loop.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	stream := wire.Stream(app, "probe")
	waitFor(t, 5*time.Second, "poison cast acked (nothing pending redelivery)", func() bool {
		ci, err := js.ConsumerInfo(stream, "probe")
		return err == nil && ci.NumAckPending == 0 && ci.NumPending == 0
	})

	if got := callInt(t, nc, app, "probe", "get"); got != 0 {
		t.Fatalf("state after durable escalate-restart = %d, want a clean 0 from init", got)
	}
}

// On an idempotent thrall, two casts carrying the same idempotency key are processed once; a
// distinct key is processed normally. The get call is ordered after the casts on the probe's
// single subscription, so the observed state is deterministic.
func TestIdempotentCastDeduped(t *testing.T) {
	const app = "itest"
	t.Setenv("AETHER_PROBE_IDEM", "1")
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	castIdem(t, nc, app, "probe", "inc", "op-1")
	castIdem(t, nc, app, "probe", "inc", "op-1") // duplicate key -> skipped
	castIdem(t, nc, app, "probe", "inc", "op-2") // distinct key -> processed

	if v := callInt(t, nc, app, "probe", "get"); v != 2 {
		t.Fatalf("state = %d, want 2 (op-1 applied once + op-2 once)", v)
	}
}

// A non-idempotent thrall does not dedup: the same key is processed every time (the opt-in
// default is off, existing behavior is unchanged).
func TestNonIdempotentThrallDoesNotDedup(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t) // AETHER_PROBE_IDEM unset -> Idempotent false
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	castIdem(t, nc, app, "probe", "inc", "op-1")
	castIdem(t, nc, app, "probe", "inc", "op-1") // same key, but no dedup -> applied again

	if v := callInt(t, nc, app, "probe", "get"); v != 2 {
		t.Fatalf("state = %d, want 2 (no dedup without opt-in)", v)
	}
}

// A duplicate call returns the first reply from cache and runs the handler once: two inc_get
// calls with the same key both return 1, the state incremented a single time.
func TestIdempotentCallReturnsCachedReply(t *testing.T) {
	const app = "itest"
	t.Setenv("AETHER_PROBE_IDEM", "1")
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	v1, ok1 := callIntIdem(nc, app, "probe", "inc_get", "call-1")
	v2, ok2 := callIntIdem(nc, app, "probe", "inc_get", "call-1") // duplicate -> cached reply
	if !ok1 || !ok2 || v1 != 1 || v2 != 1 {
		t.Fatalf("duplicate call replies = (%d,%v),(%d,%v), want both 1 (handler ran once)", v1, ok1, v2, ok2)
	}
	if v3, _ := callIntIdem(nc, app, "probe", "inc_get", "call-2"); v3 != 2 {
		t.Fatalf("distinct-key call = %d, want 2 (state incremented once more)", v3)
	}
}

// A duplicate durable cast (same idempotency key, two stored JetStream messages) is processed
// once and both messages are acked - dedup does not break the at-least-once drain.
func TestIdempotentDurableCastDeduped(t *testing.T) {
	const app = "itest"
	t.Setenv("AETHER_PROBE_IDEM", "1")
	eth := startEmbedded(t)
	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "probe", Cmd: probeCmd(t), Restart: "permanent", Scope: "local", Durable: true}},
	}
	startLord(t, eth, m)
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	castIdem(t, nc, app, "probe", "inc", "d-1")
	castIdem(t, nc, app, "probe", "inc", "d-1") // duplicate durable cast, same key

	// Wait for both stored messages to be delivered and acked, then the state is final.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	stream := wire.Stream(app, "probe")
	waitFor(t, 10*time.Second, "both durable casts delivered and acked", func() bool {
		ci, err := js.ConsumerInfo(stream, "probe")
		return err == nil && ci.NumPending == 0 && ci.NumAckPending == 0 && ci.Delivered.Consumer == 2
	})
	if v := callInt(t, nc, app, "probe", "get"); v != 1 {
		t.Fatalf("state = %d, want 1 (duplicate durable cast processed once)", v)
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

// TestSecondLordRefusesForSameApp: one lord per app is enforced. A second lord for an app whose
// lord-liveness lease another lord is actively renewing refuses to start, rather than stomp the
// lease and reap each other's thralls into a crash-loop (the old TestSingletonFencing started two
// same-app lords and only looked like it worked - the singleton instance actually self-terminated
// on the superseded epoch, which the test missed by counting starts, not liveness). Cross-node
// single-instance is via leaf isolation (DESIGN §11b), not lords racing on one bus.
func TestSecondLordRefusesForSameApp(t *testing.T) {
	const app = "itest"
	eth1 := startEmbedded(t)
	startLord(t, eth1, manifest(t, app, "one_for_one", spec("counter-single", "permanent", "singleton")))

	eth2, err := ether.Start(context.Background(), ether.Config{Mode: "external", URL: eth1.URL()})
	if err != nil {
		t.Fatalf("second ether: %v", err)
	}
	t.Cleanup(eth2.Stop)
	l2, err := New(manifest(t, app, "one_for_one", spec("counter-single", "permanent", "singleton")), eth2)
	if err != nil {
		t.Fatalf("second lord New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l2.Start(ctx); err == nil || !strings.Contains(err.Error(), "one lord per app") {
		t.Fatalf("a second lord for the same app should refuse, got: %v", err)
	}
}

// TestSingletonUnderSingleLordStaysAlive: under one lord a singleton starts AND stays alive - the
// liveness the old two-lord test never checked. With no competing lord there is no superseded
// lord-liveness epoch, so the instance does not self-exit; started stays 1 (no reap+restart) and
// the instance still answers a call.
func TestSingletonUnderSingleLordStaysAlive(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)

	var started int32
	if _, err := eth.Conn().Subscribe("test.probe.started", func(*nats.Msg) {
		atomic.AddInt32(&started, 1)
	}); err != nil {
		t.Fatalf("subscribe started: %v", err)
	}
	_ = eth.Conn().Flush()

	startLord(t, eth, manifest(t, app, "one_for_one", spec("counter-single", "permanent", "singleton")))
	waitFor(t, 5*time.Second, "singleton instance to start", func() bool {
		return atomic.LoadInt32(&started) >= 1
	})

	// Past a couple of lord-liveness verify ticks (TTL/3): if it were going to reap, it would have.
	time.Sleep(2 * time.Second)
	if n := atomic.LoadInt32(&started); n != 1 {
		t.Fatalf("expected exactly 1 start (no reap+restart), got %d", n)
	}
	if _, ok := tryCallInt(eth.Conn(), app, "counter-single", "get"); !ok {
		t.Fatal("the singleton must still be alive, but a call got no responders")
	}
}
