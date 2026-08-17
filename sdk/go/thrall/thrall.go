// Package thrall is the Go SDK for building thralls (genservers) in the ether (NATS).
//
// It mirrors the TS and Python SDKs: the same JSON envelope, the same subjects and the
// same GenServer semantics. A non-durable thrall reads call/cast/info from a single
// wildcard subscription; a durable thrall (AETHER_DURABLE=1) reads call/info over core,
// but cast from a durable JetStream consumer with ack (survives a crash). State is
// protected by a mutex = a serialized mailbox.
package thrall

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/fencing"
	"github.com/hamicek/aether/internal/lordlease"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/singleton"
	"github.com/hamicek/aether/internal/wire"
)

// mailboxStats collects the thrall's self-metrics reported on each heartbeat: how many
// messages it currently holds (depth), how long the most recent handler took (latency), and
// how many it has processed in total. begin/end bracket every handled message.
type mailboxStats struct {
	depth     atomic.Int64
	processed atomic.Uint64
	lastNs    atomic.Int64 // duration of the most recent handler, in nanoseconds
}

func (s *mailboxStats) begin() time.Time {
	s.depth.Add(1)
	return time.Now()
}

func (s *mailboxStats) end(start time.Time) {
	s.lastNs.Store(int64(time.Since(start)))
	s.processed.Add(1)
	s.depth.Add(-1)
}

func (s *mailboxStats) snapshot() wire.HeartbeatMetrics {
	return wire.HeartbeatMetrics{
		MailboxDepth:     int(s.depth.Load()),
		MailboxLatencyMs: float64(s.lastNs.Load()) / float64(time.Millisecond),
		ProcessedTotal:   s.processed.Load(),
	}
}

// Ctx is passed to init and to every handler. WE DO NOT HIDE NATS BEHIND THE THRALL -
// a thrall has full access to JetStream, KV and its own subjects via Ctx.NATS.
type Ctx struct {
	NATS *nats.Conn
	Name string
	App  string
	// Log is the thrall's structured logger, pre-tagged with app and name and configured
	// from the logging env the lord injected - handlers should log through it.
	Log *slog.Logger
	// Trace is the correlation id of the message currently being handled. The SDK sets it
	// before each handler runs; Ctx.Call/Ctx.Cast propagate it to downstream messages so one
	// operation can be followed across processes. Handlers may include it in their own logs.
	Trace string
	// MsgID is the id of the message currently being handled (the envelope's id). The SDK sets
	// it before each handler runs. Unlike Trace (which spans a whole operation), MsgID is unique
	// per message, so a handler can pass it as Append's dedup key to make a redelivered command
	// idempotent - see DedupKey and the command-key pattern in DESIGN.md.
	MsgID string
}

// Handler shapes hold the GenServer semantics:
//
//	CallFn: (payload, state) -> (reply, new_state, err)
//	CastFn: (payload, state) -> (new_state, err)
type CallFn[S any] func(payload json.RawMessage, state S, ctx *Ctx) (reply any, next S, err error)
type CastFn[S any] func(payload json.RawMessage, state S, ctx *Ctx) (next S, err error)

// Def is the definition of a thrall with state of type S.
type Def[S any] struct {
	Name       string
	Init       func(ctx *Ctx) (S, error)
	HandleCall map[string]CallFn[S]
	HandleCast map[string]CastFn[S]
	Terminate  func(reason string, state S)
}

var sharedConn *nats.Conn // for Call/Cast from this thrall

// Start connects the thrall to the ether and runs its lifecycle. It blocks until a
// controlled shutdown (ctl:drain), then calls Terminate and disconnects.
// connectOptions builds the NATS connect options for a thrall: its name plus, when
// the lord injected them, the TLS CA and nkey seed for a secured bus. Absent env =
// no option, so a thrall on an unsecured bus connects exactly as before.
func connectOptions(name string) ([]nats.Option, error) {
	opts := []nats.Option{nats.Name(name)}
	if ca := os.Getenv("AETHER_NATS_CA"); ca != "" {
		opts = append(opts, nats.RootCAs(ca))
	}
	if seed := os.Getenv("AETHER_NATS_NKEY_SEED"); seed != "" {
		opt, err := nats.NkeyOptionFromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("nkey seed %q: %w", seed, err)
		}
		opts = append(opts, opt)
	}
	return opts, nil
}

func Start[S any](def Def[S]) error {
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
	if def.Init == nil {
		return fmt.Errorf("thrall %q: Init is required", name)
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

	state, err := def.Init(ctx)
	if err != nil {
		return err
	}

	// Serialized mailbox: a mutex around every state change (call and cast).
	var mu sync.Mutex
	stats := &mailboxStats{}

	processCall := func(msg *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(msg.Data, &e) != nil {
			return
		}
		start := stats.begin()
		defer stats.end(start)
		mu.Lock()
		defer mu.Unlock()
		ctx.Trace = orNewTrace(e.Trace)
		ctx.MsgID = e.ID
		log.Debug("handling call", slog.String("op", e.Op), slog.String("trace", ctx.Trace))
		h, ok := def.HandleCall[e.Op]
		if !ok {
			_ = msg.Respond(mustJSON(errReply(e, "unknown_op", "unknown call op: "+e.Op)))
			return
		}
		reply, next, herr := h(e.Payload, state, ctx)
		if herr != nil {
			_ = msg.Respond(mustJSON(errReply(e, "handler_error", herr.Error())))
			return
		}
		state = next
		_ = msg.Respond(mustJSON(okReply(e, reply)))
	}

	processCast := func(data []byte) {
		var e wire.Envelope
		if json.Unmarshal(data, &e) != nil {
			return
		}
		start := stats.begin()
		defer stats.end(start)
		mu.Lock()
		defer mu.Unlock()
		ctx.Trace = orNewTrace(e.Trace)
		ctx.MsgID = e.ID
		log.Debug("handling cast", slog.String("op", e.Op), slog.String("trace", ctx.Trace))
		h, ok := def.HandleCast[e.Op]
		if !ok {
			return
		}
		next, herr := h(e.Payload, state, ctx)
		if herr != nil {
			log.Error("cast handler failed", slog.String("op", e.Op), slog.Any("err", herr))
			return
		}
		state = next
	}

	stop := make(chan struct{})
	stopped := false

	if durable {
		// Durable: call/info over core, cast via a durable JetStream consumer.
		callSub, err := nc.SubscribeSync(wire.Call(app, name))
		if err != nil {
			return err
		}
		go verbLoop(callSub, stop, processCall)

		infoSub, err := nc.SubscribeSync(wire.Info(app, name))
		if err != nil {
			return err
		}
		go verbLoop(infoSub, stop, func(_ *nats.Msg) {}) // TODO handleInfo

		go consumeDurableCast(nc, app, name, log, stop, processCast)
	} else {
		// Non-durable: a single wildcard subscription (call/cast/info) -> FIFO.
		dataSub, err := nc.SubscribeSync(wire.Data(app, name))
		if err != nil {
			return err
		}
		go verbLoop(dataSub, stop, func(msg *nats.Msg) {
			switch lastToken(msg.Subject) {
			case "call":
				processCall(msg)
			case "cast":
				processCast(msg.Data)
			}
		})
	}

	// ctl: controlled shutdown from the lord (drain / shutdown)
	_, err = nc.Subscribe(wire.Ctl(name), func(m *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(m.Data, &e) == nil && (e.Op == "drain" || e.Op == "shutdown") {
			if !stopped { // callbacks of a single subject are serialized -> no lock
				stopped = true
				close(stop)
			}
		}
	})
	if err != nil {
		return err
	}

	go heartbeat(nc, name, stats, stop)

	if err := startFencingIfSingleton(nc, name, log, stop); err != nil {
		return err
	}
	if err := startLordLivenessFencing(nc, name, log, stop); err != nil {
		return err
	}

	<-stop
	if def.Terminate != nil {
		mu.Lock()
		s := state
		mu.Unlock()
		def.Terminate("drain", s)
	}
	return nc.Drain()
}

// verbLoop reads messages from a synchronous subscription and passes them to the handler
// (serialized - one message at a time), until stop arrives.
func verbLoop(sub *nats.Subscription, stop <-chan struct{}, handle func(*nats.Msg)) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			return
		}
		handle(msg)
	}
}

// consumeDurableCast reads casts from a durable JetStream consumer with explicit ack.
// While the thrall is down, casts accumulate in the stream (the lord created it) and are
// drained on its return. At-least-once -> handlers must be idempotent.
func consumeDurableCast(nc *nats.Conn, app, name string, log *slog.Logger, stop <-chan struct{}, processCast func([]byte)) {
	js, err := nc.JetStream()
	if err != nil {
		log.Error("jetstream unavailable", slog.Any("err", err))
		return
	}
	sub, err := js.PullSubscribe(wire.Cast(app, name), name,
		nats.BindStream(wire.Stream(app, name)), nats.DeliverAll())
	if err != nil {
		log.Error("durable consumer setup failed", slog.Any("err", err))
		return
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
		if err != nil {
			continue // timeout / no messages
		}
		for _, m := range msgs {
			processCast(m.Data) // process ...
			_ = m.Ack()         // ... and only then ack
		}
	}
}

// Call = synchronous request/reply (GenServer.call) to another thrall. Called outside a
// handler (standalone) it mints a fresh trace; from within a handler use Ctx.Call to
// propagate the current trace instead.
func Call(target, op string, payload any, timeout time.Duration) (json.RawMessage, error) {
	if sharedConn == nil {
		return nil, fmt.Errorf("no connection - call Start() first")
	}
	return doCall(sharedConn, os.Getenv("AETHER_APP"), newTrace(), target, op, payload, timeout)
}

// Cast = fire-and-forget (GenServer.cast) to another thrall. Mints a fresh trace; from within
// a handler use Ctx.Cast to propagate the current trace.
func Cast(target, op string, payload any) error {
	if sharedConn == nil {
		return fmt.Errorf("no connection - call Start() first")
	}
	return doCast(sharedConn, os.Getenv("AETHER_APP"), newTrace(), target, op, payload)
}

// Call is the trace-propagating request/reply from inside a handler: the downstream message
// carries the trace of the message currently being handled.
func (c *Ctx) Call(target, op string, payload any, timeout time.Duration) (json.RawMessage, error) {
	return doCall(c.NATS, c.App, orNewTrace(c.Trace), target, op, payload, timeout)
}

// Cast is the trace-propagating fire-and-forget from inside a handler.
func (c *Ctx) Cast(target, op string, payload any) error {
	return doCast(c.NATS, c.App, orNewTrace(c.Trace), target, op, payload)
}

func doCall(nc *nats.Conn, app, trace, target, op string, payload any, timeout time.Duration) (json.RawMessage, error) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: trace, Kind: wire.KindCall, To: target, Op: op, Payload: mustMarshal(payload), TS: time.Now().UnixMilli()}
	msg, err := nc.Request(wire.Call(app, target), mustJSON(req), timeout)
	if err != nil {
		return nil, err
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, err
	}
	if reply.Status == "error" {
		return nil, fmt.Errorf("%s: %s", reply.Error.Type, reply.Error.Message)
	}
	return reply.Payload, nil
}

func doCast(nc *nats.Conn, app, trace, target, op string, payload any) error {
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: trace, Kind: wire.KindCast, To: target, Op: op, Payload: mustMarshal(payload), TS: time.Now().UnixMilli()}
	return nc.Publish(wire.Cast(app, target), mustJSON(e))
}

// newTrace mints a fresh correlation id for an edge (a message that starts a new operation).
func newTrace() string { return nats.NewInbox() }

// orNewTrace returns the given trace, or a fresh one when it is empty (the message came in
// without a trace, so this thrall is the edge that starts one).
func orNewTrace(trace string) string {
	if trace == "" {
		return newTrace()
	}
	return trace
}

// StartChild asks the lord to spawn a new thrall at runtime - a child not in the
// manifest (a driver per connection, a worker per request). The lord supervises it
// one_for_one, like a manifest child but outside any group strategy. Returns the
// child's name once its process has been started.
func (c *Ctx) StartChild(spec wire.SpawnSpec, timeout time.Duration) (string, error) {
	reply, err := c.lordControl(wire.OpSpawn, spec, timeout)
	if err != nil {
		return "", err
	}
	var out wire.SpawnReply
	if err := json.Unmarshal(reply, &out); err != nil {
		return "", err
	}
	return out.Name, nil
}

// StopChild asks the lord to drain and stop a dynamic child started via StartChild.
// Static (manifest) children cannot be stopped this way.
func (c *Ctx) StopChild(name string, timeout time.Duration) error {
	_, err := c.lordControl(wire.OpStop, wire.StopSpec{Name: name}, timeout)
	return err
}

// lordControl sends a spawn/stop request on the lord's control channel and returns the
// reply payload, or an error carrying the lord's refusal.
func (c *Ctx) lordControl(op string, payload any, timeout time.Duration) (json.RawMessage, error) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCtl, Op: op, Payload: mustMarshal(payload), TS: time.Now().UnixMilli()}
	msg, err := c.NATS.Request(wire.LordCtl(), mustJSON(req), timeout)
	if err != nil {
		return nil, err
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, err
	}
	if reply.Status == "error" {
		return nil, fmt.Errorf("%s: %s", reply.Error.Type, reply.Error.Message)
	}
	return reply.Payload, nil
}

func heartbeat(nc *nats.Conn, name string, stats *mailboxStats, stop <-chan struct{}) {
	tick := func() {
		hb := wire.Envelope{V: 1, Kind: wire.KindHB, To: name, TS: time.Now().UnixMilli(),
			Payload: mustMarshal(stats.snapshot())}
		_ = nc.Publish(wire.Heartbeat(name), mustJSON(hb))
	}
	tick()
	// Beat at the interval the lord injected (AETHER_HEARTBEAT_INTERVAL_MS); default 2s. The lord
	// derives its reaper threshold from the same value, so they never drift.
	t := time.NewTicker(obs.HeartbeatInterval())
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			tick()
		}
	}
}

// startFencingIfSingleton starts the singleton fencing loop when the thrall is a singleton (the
// lord injected AETHER_SINGLETON_*); it is a no-op otherwise. Shared by Start and StartFSM. The loop
// itself lives in internal/fencing, shared with the edge subcommand (cmd/aether/edge.go).
func startFencingIfSingleton(nc *nats.Conn, name string, log *slog.Logger, stop <-chan struct{}) error {
	cfg, ok := fencing.ConfigFromEnv("AETHER_SINGLETON_EPOCH", "AETHER_SINGLETON_KEY")
	if !ok {
		return nil
	}
	mgr, err := singleton.Open(nc)
	if err != nil {
		return fmt.Errorf("singleton fencing: open lock bucket: %w", err)
	}
	verify := func() (bool, error) { return mgr.Verify(cfg.Key, cfg.Epoch) }
	go fencing.Loop("singleton fencing", verify, singleton.TTL/3, singleton.TTL, log, stop, fencing.ExitOnLost("singleton fencing", name, log))
	return nil
}

// startLordLivenessFencing starts the lord-liveness fencing loop for EVERY thrall the lord
// spawned (the lord injected AETHER_LORD_*); it is a no-op for a thrall started outside a lord.
// Unlike singleton fencing it is not conditional on scope: any thrall self-terminates when its
// lord is gone or was replaced, closing the "no thrall survives its lord" invariant for a lord
// crash (an external SIGKILL, where the process-group kill never runs). Shared by Start and StartFSM.
func startLordLivenessFencing(nc *nats.Conn, name string, log *slog.Logger, stop <-chan struct{}) error {
	cfg, ok := fencing.ConfigFromEnv("AETHER_LORD_EPOCH", "AETHER_LORD_KEY")
	if !ok {
		return nil
	}
	mgr, err := lordlease.Open(nc)
	if err != nil {
		return fmt.Errorf("lord-liveness fencing: open lease bucket: %w", err)
	}
	verify := func() (bool, error) { return mgr.Verify(cfg.Key, cfg.Epoch) }
	go fencing.Loop("lord-liveness fencing", verify, lordlease.TTL/3, lordlease.TTL, log, stop, fencing.ExitOnLost("lord-liveness fencing", name, log))
	return nil
}

func okReply(req wire.Envelope, payload any) wire.Envelope {
	return wire.Envelope{V: 1, ID: req.ID, Kind: wire.KindReply, Status: "ok", Payload: mustMarshal(payload)}
}

func errReply(req wire.Envelope, typ, message string) wire.Envelope {
	return wire.Envelope{V: 1, ID: req.ID, Kind: wire.KindReply, Status: "error",
		Error: &wire.WireError{Type: typ, Message: message, Retryable: false}}
}

func lastToken(subject string) string {
	for i := len(subject) - 1; i >= 0; i-- {
		if subject[i] == '.' {
			return subject[i+1:]
		}
	}
	return subject
}

func mustJSON(e wire.Envelope) []byte {
	b, _ := json.Marshal(e)
	return b
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
