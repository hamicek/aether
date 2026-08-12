package thrall

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// newEventRunner builds an eventRunner directly (as StartEvent does internally), so the dispatch
// can be exercised without a live NATS server - the same pattern the FSM tests use.
func newEventRunner(handlers []Handler) *eventRunner {
	ctx := &Ctx{Log: obs.NewLogger(), Name: "bus", App: "test"}
	states := make([]any, len(handlers))
	for i, h := range handlers {
		if h.Init != nil {
			states[i], _ = h.Init(ctx)
		}
	}
	return &eventRunner{
		def:    EventManager{Name: "bus", Handlers: handlers},
		ctx:    ctx,
		log:    ctx.Log,
		states: states,
		stats:  &mailboxStats{},
	}
}

// emit dispatches one cast event to the manager.
func emit(r *eventRunner, op string) {
	r.dispatch(Event{Op: op, Kind: wire.KindCast}, "")
}

// stateOf reads a handler's current state under the runner's lock.
func stateOf(r *eventRunner, i int) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[i]
}

// recorder is a handler that appends its name to a shared slice on every event, so a test can
// assert both that it ran and in what order relative to its siblings.
func recorder(name string, seq *[]string) Handler {
	return Handler{
		Name: name,
		HandleEvent: func(_ Event, s any, _ *Ctx) (any, error) {
			*seq = append(*seq, name)
			return s, nil
		},
	}
}

func TestEventDispatchOrder(t *testing.T) {
	var seq []string
	r := newEventRunner([]Handler{recorder("a", &seq), recorder("b", &seq), recorder("c", &seq)})
	emit(r, "ping")
	if got := strings.Join(seq, ","); got != "a,b,c" {
		t.Fatalf("dispatch order = %q, want a,b,c", got)
	}
}

func TestEventAllHandlersSeeEvent(t *testing.T) {
	var seq []string
	handlers := []Handler{recorder("a", &seq), recorder("b", &seq), recorder("c", &seq)}
	r := newEventRunner(handlers)
	emit(r, "ping")
	if len(seq) != len(handlers) {
		t.Fatalf("%d handlers ran, want %d - an event must reach every handler", len(seq), len(handlers))
	}
}

func TestEventStateIsolation(t *testing.T) {
	counter := func(name string, step int) Handler {
		return Handler{
			Name: name,
			Init: func(*Ctx) (any, error) { return 0, nil },
			HandleEvent: func(_ Event, s any, _ *Ctx) (any, error) {
				return s.(int) + step, nil
			},
		}
	}
	r := newEventRunner([]Handler{counter("a", 1), counter("b", 10)})
	emit(r, "tick")
	emit(r, "tick")
	if got := stateOf(r, 0).(int); got != 2 {
		t.Errorf("handler a state = %d, want 2 (independent of its sibling)", got)
	}
	if got := stateOf(r, 1).(int); got != 20 {
		t.Errorf("handler b state = %d, want 20 (independent of its sibling)", got)
	}
}

func TestEventHandlerErrorIsolated(t *testing.T) {
	healthy := 0
	bad := Handler{Name: "bad", HandleEvent: func(Event, any, *Ctx) (any, error) { return nil, testError{} }}
	good := Handler{
		Name: "good",
		Init: func(*Ctx) (any, error) { return 0, nil },
		HandleEvent: func(_ Event, s any, _ *Ctx) (any, error) {
			healthy++
			return s.(int) + 1, nil
		},
	}
	r := newEventRunner([]Handler{bad, good})
	emit(r, "e")
	emit(r, "e")
	if healthy != 2 {
		t.Errorf("healthy handler ran %d times, want 2 - a failing sibling must not skip it", healthy)
	}
	if got := stateOf(r, 1).(int); got != 2 {
		t.Errorf("healthy handler state = %d, want 2", got)
	}
}

func TestEventHandlerPanicIsolated(t *testing.T) {
	healthy := 0
	boom := Handler{Name: "boom", HandleEvent: func(Event, any, *Ctx) (any, error) { panic("boom") }}
	good := Handler{Name: "good", HandleEvent: func(_ Event, s any, _ *Ctx) (any, error) { healthy++; return s, nil }}
	r := newEventRunner([]Handler{boom, good})
	emit(r, "e") // must not crash the process despite the panicking handler
	if healthy != 1 {
		t.Errorf("healthy handler ran %d times, want 1 - a panicking sibling must not take down the bus", healthy)
	}
}

// StartEvent must reject a manager with no handlers - it would silently swallow every event.
func TestStartEventRejectsNoHandlers(t *testing.T) {
	t.Setenv("AETHER_NATS_URL", "nats://127.0.0.1:1") // never dialled: the guard fires first
	t.Setenv("AETHER_APP", "test")
	err := StartEvent(EventManager{Name: "empty"})
	if err == nil || !strings.Contains(err.Error(), "at least one handler") {
		t.Fatalf("want an 'at least one handler' error, got %v", err)
	}
}

// StartEvent must reject duplicate handler names - the state slice is positional, but duplicate
// names make logs and any future by-name addressing ambiguous.
func TestStartEventRejectsDuplicateHandler(t *testing.T) {
	t.Setenv("AETHER_NATS_URL", "nats://127.0.0.1:1")
	t.Setenv("AETHER_APP", "test")
	dup := Handler{Name: "x", HandleEvent: func(Event, any, *Ctx) (any, error) { return nil, nil }}
	err := StartEvent(EventManager{Name: "dupes", Handlers: []Handler{dup, dup}})
	if err == nil || !strings.Contains(err.Error(), "duplicate handler") {
		t.Fatalf("want a 'duplicate handler' error, got %v", err)
	}
}
