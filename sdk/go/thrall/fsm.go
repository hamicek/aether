package thrall

// FSM is the second thrall behaviour (alongside the GenServer Def): a finite state machine,
// analogous to OTP's gen_statem. A thrall is always in exactly one named state; an incoming
// message is dispatched to the current state's reaction for that op. Reactions may carry a
// guard and return a transition. Processing is serialized like any thrall (one event at a
// time), so the machine's data needs no locks. Events on the wire are ordinary call/cast -
// the envelope is unchanged, so FSM thralls interoperate with GenServer callers.

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// fsmStateOp is a reserved call op the SDK answers itself, returning the current state, so the
// machine is observable from outside without any application code.
const fsmStateOp = "_state"

// EventTimeout is the Kind of an event delivered by a state timeout (vs KindCall / KindCast).
const EventTimeout = "timeout"

// Event is one input to the machine: an op (from the message or the state timeout), its
// payload, and how it arrived (call | cast | timeout).
type Event struct {
	Op      string
	Payload json.RawMessage
	Kind    string
}

// StateTimeout arms a timeout on a state: if no transition OUT of the state happens within
// After, the SDK delivers a timeout event with op Op to the current state (so a reaction for
// that op can fire the transition). It is a first-class mailbox event, not a manual ticker.
type StateTimeout[D any] struct {
	After time.Duration
	Op    string
}

// Outcome is what a reaction returns: the next state ("" = stay), the new data, (for a call
// event) the reply, and optionally a state timeout to (re-)arm - even while staying.
type Outcome[D any] struct {
	Next    string
	Data    D
	Reply   any
	Timeout *StateTimeout[D]
}

// Reaction handles one op in one state. Guard (optional) gates the transition on the current
// data and event; when it returns false the reaction does not fire and the event is treated as
// unhandled with reason "guard_rejected".
type Reaction[D any] struct {
	Guard func(data D, ev Event) bool
	Fn    func(ev Event, data D, ctx *Ctx) (Outcome[D], error)
}

// State is one named state: the reactions it has (keyed by op) and an optional state timeout
// armed when the machine enters the state.
type State[D any] struct {
	On      map[string]Reaction[D]
	Timeout *StateTimeout[D]
}

// FSM defines a state-machine thrall with extended data of type D.
type FSM[D any] struct {
	Name      string
	Initial   string
	Init      func(ctx *Ctx) (D, error)
	States    map[string]State[D]
	Terminate func(reason, state string, data D)
	// Version is the machine's self-declared build, reported to the lord in the heartbeat's
	// self-description (see thrall.Def.Version). Optional; empty means unversioned.
	Version string
}

// describe builds the FSM's self-description: the union of every state's reaction ops (each is
// dispatchable as a call or a cast, so it appears in both sets), plus the reserved _state call op.
// The developer declares no operations - they are the reaction keys already present in the states.
func (def FSM[D]) describe() *wire.ThrallDescribe {
	seen := make(map[string]struct{})
	for _, st := range def.States {
		for op := range st.On {
			seen[op] = struct{}{}
		}
	}
	events := make([]string, 0, len(seen))
	for op := range seen {
		events = append(events, op)
	}
	callOps := append(events[:len(events):len(events)], fsmStateOp)
	sort.Strings(callOps)
	castOps := append([]string(nil), events...)
	sort.Strings(castOps)
	return &wire.ThrallDescribe{CallOps: callOps, CastOps: castOps, Version: def.Version}
}

// stateReply is the payload returned by the reserved _state introspection op.
type stateReply struct {
	State string `json:"state"`
}

// StartFSM connects a state-machine thrall to the ether and runs its lifecycle. It mirrors
// Start (GenServer) but dispatches per current state. It has its own serialized dispatch so
// that internal events (state timeouts, added in a later step) enter the same mailbox as
// call/cast; the shared plumbing (connect, heartbeat, ctl, durable consumer) is reused.
func StartFSM[D any](def FSM[D]) error {
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
	if def.Initial == "" {
		return fmt.Errorf("fsm %q: Initial state is required", name)
	}
	if def.Init == nil {
		return fmt.Errorf("fsm %q: Init is required", name)
	}
	if _, ok := def.States[def.Initial]; !ok {
		return fmt.Errorf("fsm %q: initial state %q is not in states", name, def.Initial)
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
	ctx := &Ctx{NATS: nc, Name: name, App: app, Log: log, SingletonEpoch: singletonEpochFromEnv()}

	data, err := def.Init(ctx)
	if err != nil {
		return err
	}

	m := &fsmRunner[D]{def: def, ctx: ctx, log: log, cur: def.Initial, data: data, stats: &mailboxStats{}}

	// Arm the initial state's timeout before any events can arrive.
	m.mu.Lock()
	m.armLocked(def.States[m.cur].Timeout)
	m.mu.Unlock()

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
		go verbLoop(infoSub, stop, func(_ *nats.Msg) {}) // info is out-of-band; not an FSM event yet

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
				m.onCastData(msg.Data, nil)
			}
		})
	}

	if _, err := nc.Subscribe(wire.Ctl(name), func(msg *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(msg.Data, &e) == nil && (e.Op == "drain" || e.Op == "shutdown") {
			if !stopped {
				stopped = true
				close(stop)
			}
		}
	}); err != nil {
		return err
	}

	fsmDescribe := def.describe()
	go heartbeat(nc, name, m.stats, func() *wire.ThrallDescribe {
		d := *fsmDescribe
		return &d
	}, stop)

	if err := startFencingIfSingleton(nc, name, log, stop); err != nil {
		return err
	}
	if err := startLordLivenessFencing(nc, name, log, stop); err != nil {
		return err
	}

	<-stop
	m.mu.Lock()
	m.stopTimer()
	m.mu.Unlock()
	if def.Terminate != nil {
		cur, d := m.snapshot()
		def.Terminate("drain", cur, d)
	}
	return nc.Drain()
}

// fsmRunner holds the serialized machine state and dispatch. All state mutation happens under
// mu, so call/cast (and later timeout) events never run concurrently.
type fsmRunner[D any] struct {
	def   FSM[D]
	ctx   *Ctx
	log   *slog.Logger
	stats *mailboxStats

	mu         sync.Mutex
	cur        string
	data       D
	timer      *time.Timer // the current state's timeout timer (nil = none)
	timeoutGen uint64      // bumped on every (re)arm so a stale fire is ignored
}

func (m *fsmRunner[D]) snapshot() (string, D) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur, m.data
}

// onCallMsg turns a call message into an event and dispatches it, replying with the outcome.
func (m *fsmRunner[D]) onCallMsg(msg *nats.Msg) {
	var e wire.Envelope
	if json.Unmarshal(msg.Data, &e) != nil {
		return
	}
	m.dispatch(Event{Op: e.Op, Payload: e.Payload, Kind: wire.KindCall}, e.Trace, func(reply wire.Envelope) {
		_ = msg.Respond(mustJSON(reply))
	}, e)
}

// onCastData turns a cast into an event and dispatches it (no reply). The ackDurable hook (used
// by the plain thrall's escalate-before-crash path) is unused here: FSM dispatch has no
// escalation, so a durable cast is always acked by the consume loop after this returns.
func (m *fsmRunner[D]) onCastData(data []byte, _ func()) {
	var e wire.Envelope
	if json.Unmarshal(data, &e) != nil {
		return
	}
	m.dispatch(Event{Op: e.Op, Payload: e.Payload, Kind: wire.KindCast}, e.Trace, nil, e)
}

// dispatch serializes stat accounting and locking around dispatchLocked. respond is non-nil
// only for call events; req is the originating envelope (for building the reply).
func (m *fsmRunner[D]) dispatch(ev Event, trace string, respond func(wire.Envelope), req wire.Envelope) {
	start := m.stats.begin()
	defer m.stats.end(start)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchLocked(ev, trace, respond, req)
}

// dispatchLocked runs one event against the current state. Caller holds mu.
func (m *fsmRunner[D]) dispatchLocked(ev Event, trace string, respond func(wire.Envelope), req wire.Envelope) {
	m.ctx.Trace = orNewTrace(trace)
	m.ctx.MsgID = req.ID // empty for a timeout event (no originating envelope); mirrors the TS/Python FSM
	m.log.Debug("fsm event", slog.String("state", m.cur), slog.String("op", ev.Op), slog.String("kind", ev.Kind), slog.String("trace", m.ctx.Trace))

	// Reserved introspection op: answer the current state, never a transition.
	if ev.Kind == wire.KindCall && ev.Op == fsmStateOp {
		if respond != nil {
			respond(okReply(req, stateReply{State: m.cur}))
		}
		return
	}

	st := m.def.States[m.cur]
	r, ok := st.On[ev.Op]
	if !ok {
		m.unhandled(ev, respond, req, "no_transition", "no transition for op "+ev.Op+" in state "+m.cur)
		return
	}
	if r.Guard != nil && !r.Guard(m.data, ev) {
		m.unhandled(ev, respond, req, "guard_rejected", "guard rejected op "+ev.Op+" in state "+m.cur)
		return
	}

	out, err := r.Fn(ev, m.data, m.ctx)
	if err != nil {
		m.log.Error("fsm handler failed", slog.String("state", m.cur), slog.String("op", ev.Op), slog.Any("err", err))
		if respond != nil {
			respond(errReply(req, "handler_error", err.Error()))
		}
		return
	}
	m.data = out.Data
	if respond != nil {
		respond(okReply(req, out.Reply))
	}
	if out.Next != "" && out.Next != m.cur {
		m.enter(out.Next, out.Timeout)
	} else if out.Timeout != nil {
		// Staying in the state but (re-)arming its timeout to a new duration.
		m.armLocked(out.Timeout)
	}
}

// unhandled reports an event the current state does not act on: it is logged (and, for a call,
// answered with an error) rather than silently lost or crashing.
func (m *fsmRunner[D]) unhandled(ev Event, respond func(wire.Envelope), req wire.Envelope, typ, message string) {
	m.log.Warn("fsm unhandled event", slog.String("state", m.cur), slog.String("op", ev.Op), slog.String("kind", ev.Kind), slog.String("reason", typ))
	if respond != nil {
		respond(errReply(req, typ, message))
	}
}

// enter transitions to a new state and arms that state's timeout (or the override). Caller
// holds mu.
func (m *fsmRunner[D]) enter(next string, override *StateTimeout[D]) {
	from := m.cur
	if _, ok := m.def.States[next]; !ok {
		m.log.Warn("fsm transition to unknown state", slog.String("from", from), slog.String("to", next))
	}
	m.cur = next
	m.log.Info("fsm transition", slog.String("from", from), slog.String("to", next))
	to := override
	if to == nil {
		to = m.def.States[next].Timeout
	}
	m.armLocked(to)
}

// armLocked stops any pending timeout and arms a new one (nil = disarm). A generation token is
// bumped so a timer that fires after being superseded is recognized as stale and ignored.
// Caller holds mu.
func (m *fsmRunner[D]) armLocked(to *StateTimeout[D]) {
	m.timeoutGen++
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
	if to == nil {
		return
	}
	gen := m.timeoutGen
	op := to.Op
	m.timer = time.AfterFunc(to.After, func() { m.onTimeout(gen, op) })
}

// onTimeout delivers a state-timeout event into the serialized mailbox, unless a later (re)arm
// or transition has superseded this timer (stale generation).
func (m *fsmRunner[D]) onTimeout(gen uint64, op string) {
	start := m.stats.begin()
	defer m.stats.end(start)
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.timeoutGen {
		return // superseded - a newer arm or transition happened first
	}
	m.dispatchLocked(Event{Op: op, Kind: EventTimeout}, "", nil, wire.Envelope{})
}

// stopTimer disarms any pending timeout (on shutdown). Caller holds mu.
func (m *fsmRunner[D]) stopTimer() {
	m.timeoutGen++
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
}
