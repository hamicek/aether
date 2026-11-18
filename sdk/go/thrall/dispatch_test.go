package thrall

// End-to-end tests of the dispatch behavior added with the public client: typed refusals and
// deadline drops. They run the real Start/StartFSM against an embedded ether (rather than a
// re-exec'd process), so the thrall under test is the production code path from the subscription
// to the reply, and the caller is the production Client.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// dispatchEther starts an embedded ether for one test.
func dispatchEther(t *testing.T) *ether.Ether {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether.Start: %v", err)
	}
	t.Cleanup(eth.Stop)
	return eth
}

// runThrall runs a thrall the way the lord would - by injecting its identity through the
// environment - and stops it with a drain on cleanup. start is Start or StartFSM bound to its
// definition. It returns once the thrall answers on its call subject.
func runThrall(t *testing.T, eth *ether.Ether, app, name string, start func() error) {
	t.Helper()
	t.Setenv("AETHER_NATS_URL", eth.URL())
	t.Setenv("AETHER_APP", app)
	t.Setenv("AETHER_NAME", name)
	t.Setenv(obs.EnvHeartbeatIntervalMs, "100") // the floor: tests read the heartbeat's metrics

	done := make(chan error, 1)
	go func() { done <- start() }()
	t.Cleanup(func() {
		drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
		_ = eth.Conn().Publish(wire.Ctl(name), drain)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("thrall %s stopped with: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("thrall %s did not stop on drain", name)
		}
	})

	// The thrall is ready once its call subject has a subscriber: any reply, even "unknown op".
	probe := NewClient(eth.Conn(), app)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := probe.Call(name, "_ready_probe", nil, 200*time.Millisecond)
		var ce *CallError
		if err == nil || errors.As(err, &ce) {
			return
		}
		select {
		case startErr := <-done:
			t.Fatalf("thrall %s failed to start: %v", name, startErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("thrall %s did not become ready: %v", name, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// heartbeats subscribes to a thrall's heartbeat and returns a function that waits for the first
// beat satisfying want, failing the test if none arrives.
func heartbeats(t *testing.T, nc *nats.Conn, name string) func(what string, want func(wire.HeartbeatMetrics) bool) wire.HeartbeatMetrics {
	t.Helper()
	sub, err := nc.SubscribeSync(wire.Heartbeat(name))
	if err != nil {
		t.Fatalf("subscribe heartbeat: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return func(what string, want func(wire.HeartbeatMetrics) bool) wire.HeartbeatMetrics {
		t.Helper()
		var last wire.HeartbeatMetrics
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			msg, err := sub.NextMsg(500 * time.Millisecond)
			if err != nil {
				continue
			}
			var e wire.Envelope
			if json.Unmarshal(msg.Data, &e) != nil || json.Unmarshal(e.Payload, &last) != nil {
				continue
			}
			if want(last) {
				return last
			}
		}
		t.Fatalf("no heartbeat with %s; last was %+v (describe %+v)", what, last, last.Describe)
		return last
	}
}

type withdrawRequest struct {
	Amount int `json:"amount"`
}

// accountDef is a thrall with a domain rule to refuse by: a withdrawal may not exceed the balance.
// "broken" fails the ordinary way, as the control for "a refusal is not a failure".
func accountDef(balance int) Def[int] {
	return Def[int]{
		Init: func(*Ctx) (int, error) { return balance, nil },
		HandleCall: map[string]CallFn[int]{
			"balance": func(_ json.RawMessage, s int, _ *Ctx) (any, int, error) { return s, s, nil },
			"withdraw": func(p json.RawMessage, s int, _ *Ctx) (any, int, error) {
				var req withdrawRequest
				if err := json.Unmarshal(p, &req); err != nil {
					return nil, s, err
				}
				if req.Amount > s {
					// The returned state must be ignored for a refusal, like for any error.
					return nil, -1, Refusef("insufficient_funds", "the balance is %d, the withdrawal %d", s, req.Amount)
				}
				return s - req.Amount, s - req.Amount, nil
			},
			"broken": func(_ json.RawMessage, s int, _ *Ctx) (any, int, error) {
				return nil, s, errors.New("disk on fire")
			},
		},
		HandleCast: map[string]CastFn[int]{
			"spend": func(p json.RawMessage, s int, _ *Ctx) (int, error) {
				var req withdrawRequest
				_ = json.Unmarshal(p, &req)
				if req.Amount > s {
					return -1, Refuse("insufficient_funds", "not enough to spend")
				}
				return s - req.Amount, nil
			},
		},
	}
}

// TestRefusalRoundTripAcrossApps is the whole refusal story over a real bus: a caller in one app
// reaches a thrall in another, the thrall refuses with a code, the caller gets a typed error - and
// the thrall is none the worse for it (state untouched, no last error, counted as handled).
func TestRefusalRoundTripAcrossApps(t *testing.T) {
	eth := dispatchEther(t)
	nc := eth.Conn()
	awaitBeat := heartbeats(t, nc, "account")
	runThrall(t, eth, "ae", "account", func() error { return Start(accountDef(3)) })

	// The caller lives in app "ujito"; the shared thrall in app "ae".
	caller := NewClient(nc, "ujito")
	if _, err := caller.Call("account", "balance", nil, clientTimeout); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("the thrall must not be reachable in the caller's own app, got %v", err)
	}
	account := caller.With(InApp("ae"))

	_, err := CallInto[int](account, "account", "withdraw", withdrawRequest{Amount: 5}, clientTimeout)
	refusal, ok := AsRefusal(err)
	if !ok {
		t.Fatalf("an overdraft must be refused, got %T: %v", err, err)
	}
	if refusal.Type != ErrRefused || refusal.Code != "insufficient_funds" || refusal.Message != "the balance is 3, the withdrawal 5" {
		t.Fatalf("refusal = %+v", refusal)
	}
	if refusal.Retryable {
		t.Fatal("a refusal is not retryable: the same request gets the same answer")
	}

	// The refusal changed nothing: the state the handler returned alongside it was discarded.
	if got, err := CallInto[int](account, "account", "balance", nil, clientTimeout); err != nil || got != 3 {
		t.Fatalf("balance after a refusal = %d, %v; want 3", got, err)
	}
	// A refused cast is likewise a no-op.
	if err := account.Cast("account", "spend", withdrawRequest{Amount: 9}); err != nil {
		t.Fatal(err)
	}
	if got, err := CallInto[int](account, "account", "balance", nil, clientTimeout); err != nil || got != 3 {
		t.Fatalf("balance after a refused cast = %d, %v; want 3", got, err)
	}
	// An allowed request still goes through.
	if got, err := CallInto[int](account, "account", "withdraw", withdrawRequest{Amount: 2}, clientTimeout); err != nil || got != 1 {
		t.Fatalf("an allowed withdrawal = %d, %v; want 1", got, err)
	}

	// A refusal is a handled message and not a failure: after several of them the thrall reports
	// no last error. (4 calls + 1 cast above, plus the readiness probes, are all "processed".)
	beat := awaitBeat("the refusals processed", func(m wire.HeartbeatMetrics) bool { return m.ProcessedTotal >= 6 })
	if beat.Describe == nil {
		t.Fatal("heartbeat without a self-description")
	}
	if beat.Describe.LastError != "" {
		t.Fatalf("a refusal must not become the thrall's last error, got %q", beat.Describe.LastError)
	}
	if beat.ExpiredTotal != 0 {
		t.Fatalf("expired_total = %d, want 0", beat.ExpiredTotal)
	}

	// The control: an ordinary handler error IS recorded, and reads as a failure, not a refusal.
	_, err = account.Call("account", "broken", nil, clientTimeout)
	var failed *CallError
	if !errors.As(err, &failed) || failed.Type != ErrHandler || failed.Refused() {
		t.Fatalf("a handler failure = %+v (%v)", failed, err)
	}
	awaitBeat("the handler failure as last error", func(m wire.HeartbeatMetrics) bool {
		return m.Describe != nil && m.Describe.LastError == "handler_error: disk on fire"
	})
}

// oldEnvelope and oldWireError are the wire shapes as an SDK built before this change knows them:
// no deadline, no error code. They stand in for an old peer on either side of a call.
type oldWireError struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type oldEnvelope struct {
	V       int             `json:"v"`
	ID      string          `json:"id,omitempty"`
	Trace   string          `json:"trace,omitempty"`
	Kind    string          `json:"kind"`
	To      string          `json:"to,omitempty"`
	Op      string          `json:"op,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Status  string          `json:"status,omitempty"`
	Error   *oldWireError   `json:"error,omitempty"`
	TS      int64           `json:"ts,omitempty"`
}

// oldCall makes a call exactly as the previous SDK did, including how it turned an error reply
// into an error: fmt.Errorf("%s: %s", type, message).
func oldCall(t *testing.T, nc *nats.Conn, app, target, op string, payload any) (json.RawMessage, string) {
	t.Helper()
	body, _ := json.Marshal(payload)
	req, _ := json.Marshal(oldEnvelope{V: 1, ID: nats.NewInbox(), Trace: nats.NewInbox(), Kind: "call", To: target, Op: op, Payload: body, TS: time.Now().UnixMilli()})
	msg, err := nc.Request("aether."+app+"."+target+".call", req, clientTimeout)
	if err != nil {
		t.Fatalf("old call %s/%s: %v", target, op, err)
	}
	var reply oldEnvelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("an old caller could not decode the reply: %v", err)
	}
	if reply.Status == "error" {
		return nil, reply.Error.Type + ": " + reply.Error.Message
	}
	return reply.Payload, ""
}

// TestOldCallerAgainstNewThrall: a caller built before this change sends an envelope without a
// deadline and decodes error replies without a code. It keeps working: its calls are handled as
// before, and a refusal reaches it as the error string it has always got for a failed call.
func TestOldCallerAgainstNewThrall(t *testing.T) {
	eth := dispatchEther(t)
	nc := eth.Conn()
	runThrall(t, eth, "ae", "account", func() error { return Start(accountDef(3)) })

	payload, errText := oldCall(t, nc, "ae", "account", "balance", nil)
	if errText != "" || string(payload) != "3" {
		t.Fatalf("old call = %s, error %q", payload, errText)
	}

	_, errText = oldCall(t, nc, "ae", "account", "withdraw", withdrawRequest{Amount: 5})
	if errText != "refused: the balance is 3, the withdrawal 5" {
		t.Fatalf("an old caller's view of a refusal = %q", errText)
	}
	// What an old caller matched on - the message at the end of the text - still holds.
	if !strings.HasSuffix(errText, "the balance is 3, the withdrawal 5") {
		t.Fatalf("the message is no longer the suffix of the error text: %q", errText)
	}

	_, errText = oldCall(t, nc, "ae", "account", "broken", nil)
	if errText != "handler_error: disk on fire" {
		t.Fatalf("an old caller's view of a failure = %q", errText)
	}
}

// ledgerDef is a thrall whose mailbox can be held up on purpose: "hold" blocks until released,
// so calls sent meanwhile queue behind it - the situation in which a caller times out first.
// "write" counts its executions, which is how a test sees whether a queued call ran.
func ledgerDef(release <-chan struct{}, entered chan<- struct{}, writes *atomic.Int32) Def[struct{}] {
	return Def[struct{}]{
		Init: func(*Ctx) (struct{}, error) { return struct{}{}, nil },
		HandleCall: map[string]CallFn[struct{}]{
			"hold": func(_ json.RawMessage, s struct{}, _ *Ctx) (any, struct{}, error) {
				entered <- struct{}{}
				<-release
				return "released", s, nil
			},
			"write": func(_ json.RawMessage, s struct{}, _ *Ctx) (any, struct{}, error) {
				return writes.Add(1), s, nil
			},
		},
	}
}

// holdMailbox occupies the thrall's mailbox with a "hold" call and returns once the handler is
// running, so anything sent afterwards waits behind it.
func holdMailbox(t *testing.T, client *Client, target string, entered <-chan struct{}) {
	t.Helper()
	go func() { _, _ = client.Call(target, "hold", nil, 10*time.Second) }()
	select {
	case <-entered:
	case <-time.After(clientTimeout):
		t.Fatal("the hold call never reached its handler")
	}
}

// TestTimedOutCallIsNotExecutedLater is the scenario the deadline exists for: a call waits in the
// mailbox behind a slow handler, its caller times out, and when the thrall finally gets to it, it
// must not run - otherwise the caller's retry would execute the write a second time.
func TestTimedOutCallIsNotExecutedLater(t *testing.T) {
	t.Setenv(wire.EnvDeadlineSkewMs, "0") // caller and thrall share this process's clock
	eth := dispatchEther(t)
	nc := eth.Conn()
	awaitBeat := heartbeats(t, nc, "ledger")

	release, entered := make(chan struct{}), make(chan struct{}, 1)
	var writes atomic.Int32
	runThrall(t, eth, "ae", "ledger", func() error { return Start(ledgerDef(release, entered, &writes)) })
	client := NewClient(nc, "ae")

	holdMailbox(t, client, "ledger", entered)

	// The caller gives up after 100 ms while its call is still queued...
	if _, err := client.Call("ledger", "write", nil, 100*time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("the queued call = %v, want a timeout", err)
	}
	// ...and only then does the thrall get to it.
	time.Sleep(50 * time.Millisecond)
	close(release)

	// The mailbox has drained once a fresh call is answered; it is the first write that runs.
	got, err := CallInto[int32](client, "ledger", "write", nil, clientTimeout)
	if err != nil {
		t.Fatalf("a fresh call after the hold: %v", err)
	}
	if got != 1 || writes.Load() != 1 {
		t.Fatalf("the timed-out call was executed later: %d writes, want only the fresh one", writes.Load())
	}

	// The drop is visible in the thrall's metrics, and is not counted as a processed message.
	beat := awaitBeat("the dropped call", func(m wire.HeartbeatMetrics) bool { return m.ExpiredTotal == 1 })
	if beat.MailboxDepth != 0 {
		t.Fatalf("a dropped call must leave the mailbox depth, got %d", beat.MailboxDepth)
	}
	if beat.Describe == nil || beat.Describe.LastError != "" {
		t.Fatalf("a dropped call is not a handler failure, describe = %+v", beat.Describe)
	}
}

// rawCall sends a hand-built call envelope and returns the decoded reply, for tests that need a
// deadline the Client would never stamp (already past, or none at all).
func rawCall(t *testing.T, nc *nats.Conn, app string, e wire.Envelope) wire.Envelope {
	t.Helper()
	msg, err := nc.Request(wire.Call(app, e.To), mustJSON(e), clientTimeout)
	if err != nil {
		t.Fatalf("raw call %s/%s: %v", e.To, e.Op, err)
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

// TestDeadlineToleranceAndCompatibility pins what decides a drop: the deadline against the
// thrall's clock plus the skew tolerance. A call overdue by less than the tolerance runs, one
// overdue by more is refused unexecuted with a retryable error, and an envelope without a deadline
// (an old caller) runs no matter how long ago it was sent.
func TestDeadlineToleranceAndCompatibility(t *testing.T) {
	eth := dispatchEther(t) // default tolerance: wire.DefaultDeadlineSkewMs
	nc := eth.Conn()
	var writes atomic.Int32
	runThrall(t, eth, "ae", "ledger", func() error {
		return Start(ledgerDef(make(chan struct{}), make(chan struct{}, 1), &writes))
	})

	call := func(deadline int64) wire.Envelope {
		return rawCall(t, nc, "ae", wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: "ledger", Op: "write",
			TS: time.Now().Add(-time.Hour).UnixMilli(), Deadline: deadline})
	}
	now := time.Now().UnixMilli()

	if reply := call(0); reply.Status != wire.StatusOK {
		t.Fatalf("a call without a deadline must run however old it is, got %+v", reply.Error)
	}
	if reply := call(now + 5000); reply.Status != wire.StatusOK {
		t.Fatalf("a call before its deadline must run, got %+v", reply.Error)
	}
	if reply := call(now - 50); reply.Status != wire.StatusOK {
		t.Fatalf("a call overdue by less than the skew tolerance must run, got %+v", reply.Error)
	}
	if writes.Load() != 3 {
		t.Fatalf("writes = %d, want 3", writes.Load())
	}

	reply := call(now - 2000)
	if reply.Status != wire.StatusError || reply.Error == nil {
		t.Fatalf("a call overdue beyond the tolerance must be answered with an error, got %+v", reply)
	}
	if reply.Error.Type != wire.ErrDeadlineExceeded || !reply.Error.Retryable || reply.Error.Code != "" {
		t.Fatalf("drop reply = %+v", reply.Error)
	}
	if !strings.Contains(reply.Error.Message, "expired") {
		t.Fatalf("drop message = %q", reply.Error.Message)
	}
	if writes.Load() != 3 {
		t.Fatalf("the expired call ran: writes = %d, want 3", writes.Load())
	}
}

// TestDeadlineGateLogsTheDrop: a dropped call is said so in the thrall's log, with the op, the
// trace and how late it was - the trail an operator follows from "my request timed out".
func TestDeadlineGateLogsTheDrop(t *testing.T) {
	var buf bytes.Buffer
	stats := &mailboxStats{}
	gate := deadlineGate{skew: 250 * time.Millisecond, stats: stats, log: obs.NewWithWriter(&buf)}
	var reply wire.Envelope
	respond := func(r wire.Envelope) { reply = r }

	stats.begin()
	live := wire.Envelope{V: 1, ID: "c-1", Kind: wire.KindCall, Op: "write", Deadline: time.Now().Add(time.Second).UnixMilli()}
	if gate.drop(live, respond) {
		t.Fatal("a live call was dropped")
	}
	if buf.Len() != 0 || reply.Kind != "" {
		t.Fatalf("a live call must pass silently, logged %q, replied %+v", buf.String(), reply)
	}

	expired := wire.Envelope{V: 1, ID: "c-2", Trace: "t-77", Kind: wire.KindCall, Op: "write", Deadline: time.Now().Add(-time.Second).UnixMilli()}
	if !gate.drop(expired, respond) {
		t.Fatal("an expired call was not dropped")
	}
	for _, want := range []string{"dropping expired call", "op=write", "trace=t-77", "overdue_ms="} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("drop log lacks %q: %s", want, buf.String())
		}
	}
	if reply.ID != "c-2" || reply.Error == nil || reply.Error.Type != wire.ErrDeadlineExceeded {
		t.Fatalf("drop reply = %+v", reply)
	}
	if m := stats.snapshot(); m.ExpiredTotal != 1 || m.ProcessedTotal != 0 || m.MailboxDepth != 0 {
		t.Fatalf("stats after a drop = %+v", m)
	}
}

func TestDeadlineSkewFromEnv(t *testing.T) {
	cases := map[string]time.Duration{
		"":     wire.DefaultDeadlineSkewMs * time.Millisecond,
		"0":    0,
		"1500": 1500 * time.Millisecond,
		"-5":   wire.DefaultDeadlineSkewMs * time.Millisecond,
		"soon": wire.DefaultDeadlineSkewMs * time.Millisecond,
	}
	for value, want := range cases {
		t.Setenv(wire.EnvDeadlineSkewMs, value)
		if got := deadlineSkew(); got != want {
			t.Errorf("%s=%q: skew = %v, want %v", wire.EnvDeadlineSkewMs, value, got, want)
		}
	}
}

// TestPackageCallUsesTheThrallsConnection: inside a running thrall the package-level Call works
// without any setup and goes through the same client code - it carries a deadline and yields the
// typed error.
func TestPackageCallUsesTheThrallsConnection(t *testing.T) {
	eth := dispatchEther(t)
	runThrall(t, eth, "ae", "account", func() error { return Start(accountDef(3)) })

	raw, err := Call("account", "balance", nil, clientTimeout)
	if err != nil || string(raw) != "3" {
		t.Fatalf("package-level Call = %s, %v", raw, err)
	}
	_, err = Call("account", "withdraw", withdrawRequest{Amount: 5}, clientTimeout)
	if code := RefusalCode(err); code != "insufficient_funds" {
		t.Fatalf("package-level Call refusal code = %q (%v)", code, err)
	}
}

// TestFSMRefusalAndDeadline: the state-machine behaviour answers the same way as the GenServer -
// a reaction can refuse (no transition, no data change), and an expired call never becomes an event.
func TestFSMRefusalAndDeadline(t *testing.T) {
	t.Setenv(wire.EnvDeadlineSkewMs, "0")
	eth := dispatchEther(t)
	nc := eth.Conn()
	awaitBeat := heartbeats(t, nc, "door")

	door := FSM[int]{
		Initial: "locked",
		Init:    func(*Ctx) (int, error) { return 0, nil },
		States: map[string]State[int]{
			"locked": {On: map[string]Reaction[int]{
				"open": {Fn: func(ev Event, attempts int, _ *Ctx) (Outcome[int], error) {
					var req struct {
						Code string `json:"code"`
					}
					_ = json.Unmarshal(ev.Payload, &req)
					if req.Code != "1234" {
						return Outcome[int]{Next: "open", Data: attempts + 1}, Refuse("wrong_code", "that is not the code")
					}
					return Outcome[int]{Next: "open", Data: attempts, Reply: "opened"}, nil
				}},
			}},
			"open": {On: map[string]Reaction[int]{}},
		},
	}
	runThrall(t, eth, "ae", "door", func() error { return StartFSM(door) })
	client := NewClient(nc, "ae")

	state := func() string {
		t.Helper()
		got, err := CallInto[stateReply](client, "door", fsmStateOp, nil, clientTimeout)
		if err != nil {
			t.Fatalf("_state: %v", err)
		}
		return got.State
	}

	_, err := client.Call("door", "open", map[string]string{"code": "0000"}, clientTimeout)
	if refusal, ok := AsRefusal(err); !ok || refusal.Code != "wrong_code" || refusal.Message != "that is not the code" {
		t.Fatalf("a wrong code = %+v (%v), want a refusal", refusal, err)
	}
	if got := state(); got != "locked" {
		t.Fatalf("a refused event must not transition: state = %q", got)
	}

	// An expired call is dropped before it reaches the machine: the right code, sent too late.
	expired := rawCall(t, nc, "ae", wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: "door", Op: "open",
		Payload: json.RawMessage(`{"code":"1234"}`), Deadline: time.Now().Add(-time.Second).UnixMilli()})
	if expired.Error == nil || expired.Error.Type != wire.ErrDeadlineExceeded {
		t.Fatalf("an expired call to an FSM = %+v", expired)
	}
	if got := state(); got != "locked" {
		t.Fatalf("an expired call must not become an event: state = %q", got)
	}
	awaitBeat("the dropped call", func(m wire.HeartbeatMetrics) bool { return m.ExpiredTotal == 1 })

	if got, err := CallInto[string](client, "door", "open", map[string]string{"code": "1234"}, clientTimeout); err != nil || got != "opened" {
		t.Fatalf("the right code in time = %q, %v", got, err)
	}
	if got := state(); got != "open" {
		t.Fatalf("state = %q, want open", got)
	}
}
