package thrall

// EventManager is the third thrall behaviour (alongside the GenServer Def and the FSM),
// analogous to OTP's gen_event: an event manager holding an ordered set of handlers. One
// incoming event (an async cast to the manager's name) is dispatched to EVERY handler, in
// registration order, under the same serialized mailbox - so handlers observe events in a
// stable order and their state needs no locks. This is what raw NATS fan-out (N independent
// subscribers) does NOT give: co-located, ordered handlers that can each keep their own state.
//
// v1 scope: handlers are declared statically; events are async (cast). A call to an event
// manager is answered with an error rather than a silent timeout - synchronous events and
// runtime add/remove of handlers are deliberate follow-ups.

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// Handler is one reaction registered in an event manager. Each handler keeps its OWN state (of
// any type - handlers are heterogeneous), initialised by Init and threaded through HandleEvent,
// which returns the handler's new state. A handler that returns an error (or panics) is logged
// and skipped for that event; the other handlers still run.
type Handler struct {
	Name        string
	Init        func(ctx *Ctx) (any, error)
	HandleEvent func(ev Event, state any, ctx *Ctx) (any, error)
}

// EventManager defines an event-manager thrall: a name and an ordered list of handlers.
type EventManager struct {
	Name      string
	Handlers  []Handler
	Terminate func(reason string)
}

// StartEvent connects an event-manager thrall to the ether and runs its lifecycle. It mirrors
// Start / StartFSM but fans each event out to all handlers; the shared plumbing (connect,
// heartbeat, ctl, durable consumer, fencing) is reused. Events are async (cast); a call to an
// event manager is answered with an error - v1 events are fire-and-forget.
func StartEvent(def EventManager) error {
	url := os.Getenv("AETHER_NATS_URL")
	app := os.Getenv("AETHER_APP")
	envName := os.Getenv("AETHER_NAME")
	if url == "" || app == "" {
		return fmt.Errorf("missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`")
	}
	name := def.Name
	if name == "" {
		name = envName
	}
	if len(def.Handlers) == 0 {
		return fmt.Errorf("event manager %q: at least one handler is required", name)
	}
	seen := make(map[string]bool, len(def.Handlers))
	for _, h := range def.Handlers {
		if h.Name == "" {
			return fmt.Errorf("event manager %q: a handler has an empty name", name)
		}
		if seen[h.Name] {
			return fmt.Errorf("event manager %q: duplicate handler %q", name, h.Name)
		}
		seen[h.Name] = true
		if h.HandleEvent == nil {
			return fmt.Errorf("event manager %q: handler %q has no HandleEvent", name, h.Name)
		}
	}
	durable := os.Getenv("AETHER_DURABLE") == "1"

	opts, err := connectOptions(name)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return err
	}
	sharedConn = nc
	log := obs.NewLogger().With(slog.String("component", "thrall"), slog.String("app", app), slog.String("name", name))
	ctx := &Ctx{NATS: nc, Name: name, App: app, Log: log}

	// Each handler gets its own initial state, held parallel to def.Handlers.
	states := make([]any, len(def.Handlers))
	for i, h := range def.Handlers {
		if h.Init == nil {
			continue
		}
		s, err := h.Init(ctx)
		if err != nil {
			return fmt.Errorf("event manager %q: handler %q init: %w", name, h.Name, err)
		}
		states[i] = s
	}

	m := &eventRunner{def: def, ctx: ctx, log: log, states: states, stats: &mailboxStats{}}

	stop := make(chan struct{})
	stopped := false

	if durable {
		callSub, err := nc.SubscribeSync(wire.Call(app, name))
		if err != nil {
			return err
		}
		go verbLoop(callSub, stop, m.onCallMsg)

		infoSub, err := nc.SubscribeSync(wire.Info(app, name))
		if err != nil {
			return err
		}
		go verbLoop(infoSub, stop, func(_ *nats.Msg) {}) // info is out-of-band; not an event yet

		go consumeDurableCast(nc, app, name, log, stop, m.onCastData)
	} else {
		dataSub, err := nc.SubscribeSync(wire.Data(app, name))
		if err != nil {
			return err
		}
		go verbLoop(dataSub, stop, func(msg *nats.Msg) {
			switch lastToken(msg.Subject) {
			case "call":
				m.onCallMsg(msg)
			case "cast":
				m.onCastData(msg.Data)
			}
		})
	}

	if _, err := nc.Subscribe(wire.Ctl(name), func(msg *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(msg.Data, &e) == nil && (e.Op == "drain" || e.Op == "shutdown") {
			if !stopped { // callbacks of a single subject are serialized -> no lock
				stopped = true
				close(stop)
			}
		}
	}); err != nil {
		return err
	}

	go heartbeat(nc, name, m.stats, stop)

	if err := startFencingIfSingleton(nc, name, log, stop); err != nil {
		return err
	}
	if err := startLordLivenessFencing(nc, name, log, stop); err != nil {
		return err
	}

	<-stop
	if def.Terminate != nil {
		def.Terminate("drain")
	}
	return nc.Drain()
}

// eventRunner holds the serialized handler states and the dispatch. All handler state mutation
// happens under mu, so events never run concurrently and each handler keeps GenServer-like
// isolation.
type eventRunner struct {
	def   EventManager
	ctx   *Ctx
	log   *slog.Logger
	stats *mailboxStats

	mu     sync.Mutex
	states []any // parallel to def.Handlers
}

// onCallMsg answers a call to the event manager with an error: v1 events are async (cast). A
// clear reply beats a silent timeout on the caller.
func (m *eventRunner) onCallMsg(msg *nats.Msg) {
	var e wire.Envelope
	if json.Unmarshal(msg.Data, &e) != nil {
		return
	}
	_ = msg.Respond(mustJSON(errReply(e, "events_async", "event manager is async: use cast, not call")))
}

// onCastData turns a cast into an event and fans it out to every handler.
func (m *eventRunner) onCastData(data []byte) {
	var e wire.Envelope
	if json.Unmarshal(data, &e) != nil {
		return
	}
	m.dispatch(Event{Op: e.Op, Payload: e.Payload, Kind: wire.KindCast}, e.Trace)
}

// dispatch delivers one event to all handlers in registration order, under the serialized
// mailbox. A handler that returns an error or panics is logged and skipped (its state is left
// unchanged); the remaining handlers still run.
func (m *eventRunner) dispatch(ev Event, trace string) {
	start := m.stats.begin()
	defer m.stats.end(start)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx.Trace = orNewTrace(trace)
	m.log.Debug("event", slog.String("op", ev.Op), slog.Int("handlers", len(m.def.Handlers)), slog.String("trace", m.ctx.Trace))
	for i, h := range m.def.Handlers {
		next, err := m.runHandler(h, ev, m.states[i])
		if err != nil {
			m.log.Error("event handler failed", slog.String("handler", h.Name), slog.String("op", ev.Op), slog.Any("err", err))
			continue // isolate: a failing handler does not affect the others or their state
		}
		m.states[i] = next
	}
}

// runHandler invokes one handler, converting a panic into an error so a single misbehaving
// handler cannot take down the whole manager. Caller holds mu.
func (m *eventRunner) runHandler(h Handler, ev Event, state any) (next any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h.HandleEvent(ev, state, m.ctx)
}
