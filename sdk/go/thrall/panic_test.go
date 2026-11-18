package thrall

// What a panicking handler costs: one request of a web thrall, the process of any other thrall -
// and in the second case the caller is told at once. End to end over an embedded ether, like the
// other dispatch tests; where the thrall is meant to crash, the test stands in for os.Exit.

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// captureExit replaces the process exit for one test and returns the channel the exit status
// arrives on. A thrall under test that "exits" keeps running in the test binary; the test only
// needs to know that it would have exited, and how.
func captureExit(t *testing.T) <-chan int {
	t.Helper()
	exits := make(chan int, 4)
	original := exitProcess
	exitProcess = func(code int) { exits <- code }
	t.Cleanup(func() { exitProcess = original })
	return exits
}

func awaitExit(t *testing.T, exits <-chan int) int {
	t.Helper()
	select {
	case code := <-exits:
		return code
	case <-time.After(clientTimeout):
		t.Fatal("the thrall did not exit")
		return 0
	}
}

// brittle is a web handler body with three ways to end badly, shared by both handler shapes.
func brittle(req HTTPRequest) (HTTPResponse, error) {
	switch req.Path {
	case "/panic":
		var missing map[string]int
		missing["x"] = 1 // a real runtime panic: assignment to entry in nil map
	case "/error":
		return HTTPResponse{}, errors.New("upstream is down")
	case "/refuse":
		return HTTPResponse{}, Refuse("not_for_you", "no")
	}
	return TextResponse(200, "ok"), nil
}

// TestWebHandlerPanicCostsOneRequest: a panic in a web handler answers that request with a 500,
// is counted and recorded as the last error, and the very next request is served - in every
// dispatch mode. With Handle the state is what it was before the request that panicked.
func TestWebHandlerPanicCostsOneRequest(t *testing.T) {
	defs := map[string]func() error{
		"serve": func() error {
			return StartWeb(WebDef[struct{}]{
				Serve: func(req HTTPRequest, _ struct{}, _ *Ctx) (HTTPResponse, error) { return brittle(req) },
			})
		},
		"serve at concurrency 1": func() error {
			return StartWeb(WebDef[struct{}]{
				Concurrency: 1,
				Serve:       func(req HTTPRequest, _ struct{}, _ *Ctx) (HTTPResponse, error) { return brittle(req) },
			})
		},
		"handle": func() error {
			return StartWeb(WebDef[int]{
				Handle: func(req HTTPRequest, hits int, _ *Ctx) (HTTPResponse, int, error) {
					hits++
					resp, err := brittle(req)
					if err == nil {
						resp = TextResponse(200, strconv.Itoa(hits))
					}
					return resp, hits, err
				},
			})
		},
	}
	for name, start := range defs {
		t.Run(name, func(t *testing.T) {
			exits := captureExit(t)
			eth := dispatchEther(t)
			awaitBeat := heartbeats(t, eth.Conn(), "web")
			runThrall(t, eth, "ae", "web", start)
			client := NewClient(eth.Conn(), "ae")

			first := mustOK(t, "a request before the panic", awaitOutcome(t, "before", get(client, "web", "/", clientTimeout)))

			crashed := awaitOutcome(t, "the panicking request", get(client, "web", "/panic", clientTimeout, WithTrace("t-panic")))
			if crashed.err != nil {
				t.Fatalf("the panicking request must be answered as HTTP, got error %v", crashed.err)
			}
			if crashed.resp.Status != 500 || string(crashed.resp.Body) != "Internal Server Error" {
				t.Fatalf("the panicking request = %d %q, want a bare 500", crashed.resp.Status, crashed.resp.Body)
			}

			after := mustOK(t, "the request after the panic", awaitOutcome(t, "after", get(client, "web", "/", clientTimeout)))
			if name == "handle" && (string(first.Body) != "1" || string(after.Body) != "2") {
				t.Errorf("the panicking request must leave the state alone: got %q then %q, want 1 then 2", first.Body, after.Body)
			}

			// Counted, and no longer held: the request that panicked ended like any other.
			beat := awaitBeat("the panic counted and nothing held", func(m wire.HeartbeatMetrics) bool {
				return m.PanicsTotal == 1 && m.InFlight == 0 && m.MailboxDepth == 0
			})
			if beat.Describe == nil || !strings.HasPrefix(beat.Describe.LastError, "panic: assignment to entry in nil map") {
				t.Errorf("last error = %+v, want the panic", beat.Describe)
			}
			select {
			case code := <-exits:
				t.Fatalf("a web thrall must survive a handler panic, it exited with %d", code)
			default:
			}
		})
	}
}

// TestWebHandlerFailuresAreAnsweredAsHTTP: a web handler that returns an error is a 500 from the
// thrall itself (recorded as its last error, not counted as a panic), while a refusal stays the
// typed error reply it is for every thrall.
func TestWebHandlerFailuresAreAnsweredAsHTTP(t *testing.T) {
	eth := dispatchEther(t)
	awaitBeat := heartbeats(t, eth.Conn(), "web")
	runThrall(t, eth, "ae", "web", func() error {
		return StartWeb(WebDef[struct{}]{
			Serve: func(req HTTPRequest, _ struct{}, _ *Ctx) (HTTPResponse, error) { return brittle(req) },
		})
	})
	client := NewClient(eth.Conn(), "ae")

	failed := awaitOutcome(t, "the failing request", get(client, "web", "/error", clientTimeout))
	if failed.err != nil || failed.resp.Status != 500 || string(failed.resp.Body) != "Internal Server Error" {
		t.Fatalf("a returned error = %d %q (err %v), want a bare 500", failed.resp.Status, failed.resp.Body, failed.err)
	}
	beat := awaitBeat("the failure as last error", func(m wire.HeartbeatMetrics) bool {
		return m.Describe != nil && m.Describe.LastError == "handler_error: upstream is down"
	})
	if beat.PanicsTotal != 0 {
		t.Errorf("a returned error is not a panic: panics_total = %d", beat.PanicsTotal)
	}

	refused := awaitOutcome(t, "the refused request", get(client, "web", "/refuse", clientTimeout))
	if RefusalCode(refused.err) != "not_for_you" {
		t.Errorf("a refusal must stay a typed error reply, got %+v / %v", refused.resp, refused.err)
	}
	mustOK(t, "a request afterwards", awaitOutcome(t, "afterwards", get(client, "web", "/", clientTimeout)))
}

// TestPanicInOrdinaryThrallCrashesAndTellsTheCaller: a GenServer does not carry on after a panic -
// its state may be half changed - but it dies properly: the caller is answered "escalated" at
// once, the reason reaches the lord on a last heartbeat, and the process exits with the status of
// an unrecovered panic.
func TestPanicInOrdinaryThrallCrashesAndTellsTheCaller(t *testing.T) {
	exits := captureExit(t)
	eth := dispatchEther(t)
	awaitBeat := heartbeats(t, eth.Conn(), "ledger")
	runThrall(t, eth, "ae", "ledger", func() error {
		return Start(Def[[]int]{
			Init: func(*Ctx) ([]int, error) { return nil, nil },
			HandleCall: map[string]CallFn[[]int]{
				"first": func(_ json.RawMessage, s []int, _ *Ctx) (any, []int, error) { return s[0], s, nil },
			},
			HandleCast: map[string]CastFn[[]int]{
				"first": func(_ json.RawMessage, s []int, _ *Ctx) ([]int, error) { _ = s[0]; return s, nil },
			},
		})
	})
	client := NewClient(eth.Conn(), "ae")

	started := time.Now()
	_, err := client.Call("ledger", "first", nil, 5*time.Second)
	var ce *CallError
	if !errors.As(err, &ce) || ce.Type != wire.ErrEscalated {
		t.Fatalf("the caller of a panicking call = %v, want an %s error reply", err, wire.ErrEscalated)
	}
	if !strings.HasPrefix(ce.Message, "panic: runtime error: index out of range") {
		t.Errorf("the reply must say what happened, got %q", ce.Message)
	}
	if waited := time.Since(started); waited > 4*time.Second {
		t.Errorf("the caller waited %v: it must be answered, not left to time out", waited)
	}
	if code := awaitExit(t, exits); code != exitPanic {
		t.Errorf("exit status after a call panic = %d, want %d", code, exitPanic)
	}
	beat := awaitBeat("the panic as last error", func(m wire.HeartbeatMetrics) bool {
		return m.Describe != nil && strings.HasPrefix(m.Describe.LastError, "panic: ")
	})
	if beat.PanicsTotal != 0 {
		t.Errorf("a thrall that crashes on a panic contains none: panics_total = %d", beat.PanicsTotal)
	}

	// A cast has no caller to tell; the thrall crashes all the same.
	if err := client.Cast("ledger", "first", nil); err != nil {
		t.Fatalf("cast: %v", err)
	}
	if code := awaitExit(t, exits); code != exitPanic {
		t.Errorf("exit status after a cast panic = %d, want %d", code, exitPanic)
	}
}

// TestPanicInFSMCrashesAndTellsTheCaller: the state machine follows the GenServer rule.
func TestPanicInFSMCrashesAndTellsTheCaller(t *testing.T) {
	exits := captureExit(t)
	eth := dispatchEther(t)
	machine := FSM[[]int]{
		Initial: "idle",
		Init:    func(*Ctx) ([]int, error) { return nil, nil },
		States: map[string]State[[]int]{
			"idle": {On: map[string]Reaction[[]int]{
				"first": {Fn: func(_ Event, data []int, _ *Ctx) (Outcome[[]int], error) {
					return Outcome[[]int]{Data: data, Reply: data[0]}, nil
				}},
			}},
		},
	}
	runThrall(t, eth, "ae", "machine", func() error { return StartFSM(machine) })

	_, err := NewClient(eth.Conn(), "ae").Call("machine", "first", nil, 5*time.Second)
	var ce *CallError
	if !errors.As(err, &ce) || ce.Type != wire.ErrEscalated || !strings.HasPrefix(ce.Message, "panic: ") {
		t.Fatalf("the caller of a panicking FSM event = %v, want an %s reply naming the panic", err, wire.ErrEscalated)
	}
	if code := awaitExit(t, exits); code != exitPanic {
		t.Errorf("exit status = %d, want %d", code, exitPanic)
	}
}

// explode panics from a named function, so the test can look for it in the recorded stack.
func explode() { panic("kaboom") }

// TestPanicIsLoggedWithItsTraceAndStack: the log line of a handler panic carries what is needed
// to find it - the operation, the trace of the request and the stack down to the panicking frame.
func TestPanicIsLoggedWithItsTraceAndStack(t *testing.T) {
	if p := guard(func() {}); p != nil {
		t.Fatalf("a handler that returns is not a panic: %+v", p)
	}
	p := guard(explode)
	if p == nil {
		t.Fatal("the panic was not caught")
	}
	if p.reason() != "panic: kaboom" {
		t.Errorf("reason = %q", p.reason())
	}

	var buf bytes.Buffer
	logPanic(obs.NewWithWriter(&buf), "handler panicked", OpHTTPRequest, "t-42", p)
	for _, want := range []string{"handler panicked", "op=http_request", "trace=t-42", "panic=kaboom", "thrall.explode"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the panic log lacks %q: %s", want, buf.String())
		}
	}
}
