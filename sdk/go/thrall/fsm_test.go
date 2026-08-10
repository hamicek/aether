package thrall

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// turnstile is a small, domain-neutral machine used across the FSM tests: locked <-> unlocked,
// data counts completed pushes. "unlocked" has a coin reaction whose guard always rejects, to
// exercise guard handling; "boom" triggers a handler error.
func turnstile() FSM[int] {
	return FSM[int]{
		Name:    "turnstile",
		Initial: "locked",
		Init:    func(*Ctx) (int, error) { return 0, nil },
		States: map[string]State[int]{
			"locked": {On: map[string]Reaction[int]{
				"coin": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
					return Outcome[int]{Next: "unlocked", Data: d}, nil
				}},
			}},
			"unlocked": {On: map[string]Reaction[int]{
				"push": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
					return Outcome[int]{Next: "locked", Data: d + 1, Reply: d + 1}, nil
				}},
				"coin": {
					Guard: func(int, Event) bool { return false },
					Fn:    func(_ Event, d int, _ *Ctx) (Outcome[int], error) { return Outcome[int]{Data: d}, nil },
				},
				"boom": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
					return Outcome[int]{}, testError{}
				}},
			}},
		},
	}
}

// testError is a trivial error so a reaction can fail.
type testError struct{}

func (testError) Error() string { return "boom" }

func newRunner(def FSM[int]) *fsmRunner[int] {
	ctx := &Ctx{Log: obs.NewLogger(), Name: def.Name, App: "test"}
	data, _ := def.Init(ctx)
	r := &fsmRunner[int]{def: def, ctx: ctx, log: ctx.Log, stats: &mailboxStats{}, cur: def.Initial, data: data}
	// Arm the initial state's timeout, as StartFSM does.
	r.mu.Lock()
	r.armLocked(def.States[r.cur].Timeout)
	r.mu.Unlock()
	return r
}

// waitState polls until the runner reaches want, or fails after timeout.
func waitState(t *testing.T, r *fsmRunner[int], want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, _ := r.snapshot(); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := r.snapshot()
	t.Fatalf("timeout waiting for state %q, still in %q", want, got)
}

// timeoutMachine: "waiting" auto-transitions to "expired" after a state timeout (op "tick"),
// unless "ping" moves it to "active" (which has no timeout) first.
func timeoutMachine(after time.Duration) FSM[int] {
	return FSM[int]{
		Name:    "tmo",
		Initial: "waiting",
		Init:    func(*Ctx) (int, error) { return 0, nil },
		States: map[string]State[int]{
			"waiting": {
				On: map[string]Reaction[int]{
					"ping": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
						return Outcome[int]{Next: "active", Data: d}, nil
					}},
					"tick": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
						return Outcome[int]{Next: "expired", Data: d + 1}, nil
					}},
				},
				Timeout: &StateTimeout[int]{After: after, Op: "tick"},
			},
			"active":  {On: map[string]Reaction[int]{}},
			"expired": {On: map[string]Reaction[int]{}},
		},
	}
}

// call dispatches a call event and returns the reply envelope.
func call(r *fsmRunner[int], op string) wire.Envelope {
	var reply wire.Envelope
	req := wire.Envelope{V: 1, Kind: wire.KindCall, Op: op}
	r.dispatch(Event{Op: op, Kind: wire.KindCall}, "", func(rep wire.Envelope) { reply = rep }, req)
	return reply
}

// cast dispatches a cast event (no reply).
func cast(r *fsmRunner[int], op string) {
	r.dispatch(Event{Op: op, Kind: wire.KindCast}, "", nil, wire.Envelope{V: 1, Kind: wire.KindCast, Op: op})
}

func TestFSMStartsInInitialState(t *testing.T) {
	r := newRunner(turnstile())
	if got, _ := r.snapshot(); got != "locked" {
		t.Errorf("initial state = %q, want locked", got)
	}
}

func TestFSMTransitionOnCall(t *testing.T) {
	r := newRunner(turnstile())
	reply := call(r, "coin")
	if reply.Status != "ok" {
		t.Fatalf("coin reply status = %q, want ok", reply.Status)
	}
	if got, _ := r.snapshot(); got != "unlocked" {
		t.Errorf("after coin, state = %q, want unlocked", got)
	}
}

func TestFSMTransitionMutatesDataAndReplies(t *testing.T) {
	r := newRunner(turnstile())
	call(r, "coin") // -> unlocked
	reply := call(r, "push")
	if reply.Status != "ok" {
		t.Fatalf("push reply status = %q, want ok", reply.Status)
	}
	var pushes int
	if err := json.Unmarshal(reply.Payload, &pushes); err != nil || pushes != 1 {
		t.Errorf("push reply payload = %s (err %v), want 1", reply.Payload, err)
	}
	if got, d := r.snapshot(); got != "locked" || d != 1 {
		t.Errorf("after push, state=%q data=%d, want locked/1", got, d)
	}
}

func TestFSMUnhandledEventOnCallErrors(t *testing.T) {
	r := newRunner(turnstile())
	reply := call(r, "push") // no "push" reaction in "locked"
	if reply.Status != "error" || reply.Error == nil || reply.Error.Type != "no_transition" {
		t.Fatalf("unhandled call = %+v, want error/no_transition", reply)
	}
	if got, _ := r.snapshot(); got != "locked" {
		t.Errorf("unhandled event changed state to %q", got)
	}
}

func TestFSMGuardRejectsTransition(t *testing.T) {
	r := newRunner(turnstile())
	call(r, "coin")          // -> unlocked
	reply := call(r, "coin") // guard always false in unlocked
	if reply.Status != "error" || reply.Error == nil || reply.Error.Type != "guard_rejected" {
		t.Fatalf("guarded call = %+v, want error/guard_rejected", reply)
	}
	if got, _ := r.snapshot(); got != "unlocked" {
		t.Errorf("guard-rejected event changed state to %q", got)
	}
}

func TestFSMHandlerErrorReplies(t *testing.T) {
	r := newRunner(turnstile())
	call(r, "coin") // -> unlocked
	reply := call(r, "boom")
	if reply.Status != "error" || reply.Error == nil || reply.Error.Type != "handler_error" {
		t.Fatalf("boom reply = %+v, want error/handler_error", reply)
	}
}

func TestFSMReservedStateOp(t *testing.T) {
	r := newRunner(turnstile())
	call(r, "coin") // -> unlocked
	reply := call(r, fsmStateOp)
	if reply.Status != "ok" {
		t.Fatalf("_state reply status = %q, want ok", reply.Status)
	}
	var sr stateReply
	if err := json.Unmarshal(reply.Payload, &sr); err != nil || sr.State != "unlocked" {
		t.Errorf("_state payload = %s (err %v), want state=unlocked", reply.Payload, err)
	}
}

func TestFSMCastTransitionsWithoutReply(t *testing.T) {
	r := newRunner(turnstile())
	cast(r, "coin") // must not panic on nil respond
	if got, _ := r.snapshot(); got != "unlocked" {
		t.Errorf("after cast coin, state = %q, want unlocked", got)
	}
}

func TestFSMStateTimeoutFiresTransition(t *testing.T) {
	r := newRunner(timeoutMachine(30 * time.Millisecond))
	waitState(t, r, "expired", time.Second)
	if _, d := r.snapshot(); d != 1 {
		t.Errorf("timeout data = %d, want 1", d)
	}
}

func TestFSMEventCancelsStateTimeout(t *testing.T) {
	r := newRunner(timeoutMachine(30 * time.Millisecond))
	cast(r, "ping") // -> active (no timeout) before the 30ms elapses
	if got, _ := r.snapshot(); got != "active" {
		t.Fatalf("after ping, state = %q, want active", got)
	}
	// The waiting-state timer must not fire "tick" now that we left the state.
	time.Sleep(80 * time.Millisecond)
	if got, _ := r.snapshot(); got != "active" {
		t.Errorf("state timeout fired after leaving the state: state = %q", got)
	}
}

func TestFSMOutcomeReArmsTimeoutWhileStaying(t *testing.T) {
	// A state that, on "poke", stays put but re-arms its timeout; the timeout fires "done".
	def := FSM[int]{
		Name: "rearm", Initial: "idle", Init: func(*Ctx) (int, error) { return 0, nil },
		States: map[string]State[int]{
			"idle": {On: map[string]Reaction[int]{
				"poke": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
					return Outcome[int]{Data: d + 1, Timeout: &StateTimeout[int]{After: 30 * time.Millisecond, Op: "done"}}, nil
				}},
				"done": {Fn: func(_ Event, d int, _ *Ctx) (Outcome[int], error) {
					return Outcome[int]{Next: "finished", Data: d}, nil
				}},
			}},
			"finished": {On: map[string]Reaction[int]{}},
		},
	}
	r := newRunner(def)
	cast(r, "poke") // stays in idle, arms a 30ms timeout
	if got, _ := r.snapshot(); got != "idle" {
		t.Fatalf("after poke, state = %q, want idle", got)
	}
	waitState(t, r, "finished", time.Second)
	if _, d := r.snapshot(); d != 1 {
		t.Errorf("data after re-armed timeout = %d, want 1", d)
	}
}
