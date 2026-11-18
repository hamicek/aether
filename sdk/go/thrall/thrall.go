// Package thrall is the Go SDK for the ether (NATS): building thralls (genservers) and calling
// them.
//
// It mirrors the TS and Python SDKs: the same JSON envelope, the same subjects and the
// same GenServer semantics. A non-durable thrall reads call/cast/info from a single
// wildcard subscription; a durable thrall (AETHER_DURABLE=1) reads call/info over core,
// but cast from a durable JetStream consumer with ack (survives a crash). State is
// protected by a mutex = a serialized mailbox.
//
// The calling side needs no thrall: a Client is built from a bare NATS connection (client.go), and
// a handler's Ctx and the package-level Call are that same client. A call ends in a reply, a typed
// *CallError when the target answered with an error (a refusal carries a stable Code - see Refuse),
// or the NATS error when nobody answered. The wire contract itself is re-exported in wire.go.
package thrall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
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
// messages it currently holds (depth), how many of those have a handler running right now
// (running), how long the most recent handler took (latency), how many it has processed in total,
// and how many calls it dropped unexecuted because their deadline had passed.
//
// A message is accounted in three steps: begin when it is received, started when its turn comes
// and its handler is about to run, end when the handler has returned. A call dropped before its
// turn ends with drop instead of started/end. So depth - running is what is still waiting.
type mailboxStats struct {
	depth     atomic.Int64
	running   atomic.Int64
	processed atomic.Uint64
	expired   atomic.Uint64
	panics    atomic.Uint64 // handler crashes contained by a web thrall
	lastNs    atomic.Int64  // duration of the most recent handler, in nanoseconds
}

func (s *mailboxStats) begin() time.Time {
	s.depth.Add(1)
	return time.Now()
}

// started marks the message's turn: it stops waiting and its handler runs. Paired with end.
func (s *mailboxStats) started() {
	s.running.Add(1)
}

func (s *mailboxStats) end(start time.Time) {
	s.lastNs.Store(int64(time.Since(start)))
	s.processed.Add(1)
	s.running.Add(-1)
	s.depth.Add(-1)
}

// drop accounts a message that left the mailbox without being handled (an expired call): it no
// longer counts toward the depth, and it is not a processed message nor a latency sample.
func (s *mailboxStats) drop() {
	s.expired.Add(1)
	s.depth.Add(-1)
}

func (s *mailboxStats) snapshot() wire.HeartbeatMetrics {
	// running is read before depth: a message that starts in between then shows as held and not
	// yet running, never as more running than held.
	running := s.running.Load()
	return wire.HeartbeatMetrics{
		MailboxDepth:     int(s.depth.Load()),
		MailboxLatencyMs: float64(s.lastNs.Load()) / float64(time.Millisecond),
		ProcessedTotal:   s.processed.Load(),
		ExpiredTotal:     s.expired.Load(),
		InFlight:         int(running),
		PanicsTotal:      s.panics.Load(),
	}
}

// Ctx is passed to init and to every handler. WE DO NOT HIDE NATS BEHIND THE THRALL -
// a thrall has full access to JetStream, KV and its own subjects via Ctx.NATS.
//
// Trace and MsgID describe the message being handled, so a Ctx is only valid for the handler
// invocation it was passed to: do not keep it for later. A serialized thrall reuses one Ctx for
// every message; a concurrent web thrall (WebDef.Serve) hands each request its own.
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
	// SingletonEpoch is this thrall's fencing epoch when it is a singleton (0 otherwise), constant
	// for the process lifetime. Singleton fencing only bounds LIVENESS overlap, not write access;
	// for strict single-writer against an external resource, send this epoch with every write and
	// have the resource reject a lower one - see the write-side fencing token pattern in DESIGN.md.
	SingletonEpoch uint64
}

// singletonEpochFromEnv reads the fencing epoch the lord injected for a singleton thrall (0 for a
// non-singleton, which carries no fencing env). Exposed on Ctx.SingletonEpoch.
func singletonEpochFromEnv() uint64 {
	if cfg, ok := fencing.ConfigFromEnv("AETHER_SINGLETON_EPOCH", "AETHER_SINGLETON_KEY"); ok {
		return cfg.Epoch
	}
	return 0
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
	// Idempotent turns on in-memory dedup of call/cast by idempotency key (Ctx.Call/Cast with
	// WithIdempotencyKey, else the envelope ID): a duplicate cast is skipped and a duplicate call
	// returns the first reply. In-memory only - it does not survive a restart. See AE-077.
	Idempotent bool
	// IdempotencyMax bounds the dedup cache (0 = a sensible default). Ignored unless Idempotent.
	IdempotencyMax int
	// Version is the thrall's self-declared build (e.g. "1.4.0"), reported to the lord in the
	// heartbeat's self-description and surfaced by `aether ps` / `aether describe`. It lives with
	// the code, so it answers "what actually runs on this node" - unlike a manifest, it cannot go
	// stale against the binary. Optional; empty means unversioned.
	Version string
}

// opNames returns the sorted keys of a handler map (the operations the thrall answers), or nil for
// an empty map so the describe field is omitted on the wire. Generic over the call/cast handler
// value types, which differ.
func opNames[V any](handlers map[string]V) []string {
	if len(handlers) == 0 {
		return nil
	}
	ops := make([]string, 0, len(handlers))
	for op := range handlers {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

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
	return run(def, dispatch{concurrency: serialized})
}

// serialized is the concurrency of a GenServer: one message at a time.
const serialized = 1

// dispatch is how a thrall's calls are run. Its zero-ish value - serialized, no failureReply - is
// the GenServer every thrall is; StartWeb is the only caller that asks for anything else.
type dispatch struct {
	// concurrency is how many call handlers may run at once. At serialized: one mailbox, one
	// handler at a time, each handler's returned state replacing the previous one. Above it -
	// reachable only through WebDef.Serve - calls leave the mailbox for a callPool, and the state
	// Init returned becomes a value shared by every handler that the runtime never replaces (a
	// handler's returned state is ignored, which is why the only public way in is a handler shape
	// that has none).
	concurrency int
	// failureReply, when set, is the payload a caller is answered with - as a successful reply -
	// when a call handler fails: it returned a plain error, or it panicked. The thrall then
	// survives a panic. Nil is the GenServer rule: a plain error is a handler_error reply and a
	// panic ends the process (see panic.go).
	failureReply func() any
}

// run is the lifecycle behind Start and StartWeb.
func run[S any](def Def[S], how dispatch) error {
	concurrency := how.concurrency
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
	processClient.Store(NewClient(nc, app))
	log := obs.NewLogger().With(slog.String("component", "thrall"), slog.String("app", app), slog.String("name", name))
	ctx := &Ctx{NATS: nc, Name: name, App: app, Log: log, SingletonEpoch: singletonEpochFromEnv()}

	state, err := def.Init(ctx)
	if err != nil {
		return err
	}

	// Serialized mailbox: a mutex around every state change (call and cast).
	var mu sync.Mutex
	stats := &mailboxStats{}

	// The last handler failure, reported in the self-description. It has its own lock because the
	// heartbeat goroutine reads it while a handler writes it under the mailbox lock.
	lastErr := &lastError{}

	// The self-description attached to every heartbeat: the static ops/version plus the current
	// last error, read fresh per beat so a failure shows up on the next beat.
	staticDescribe := wire.ThrallDescribe{
		CallOps: opNames(def.HandleCall),
		CastOps: opNames(def.HandleCast),
		Version: def.Version,
	}
	if concurrency > serialized {
		staticDescribe.Concurrency = concurrency
	}
	describe := func() *wire.ThrallDescribe {
		d := staticDescribe
		d.LastError, d.LastErrorMs = lastErr.get()
		return &d
	}
	// dyingBreath publishes one final heartbeat carrying the last error and flushes it, so the lord
	// learns WHY a thrall escalated before it sees the process exit. Used only on the crash path.
	dyingBreath := func() {
		publishHeartbeat(nc, name, stats, describe)
		_ = nc.Flush()
	}

	// Turns away calls whose caller has stopped waiting, before their handler runs.
	gate := newDeadlineGate(stats, log)

	// Optional in-memory dedup by idempotency key. Accessed only under mu (the mailbox lock),
	// so it needs no lock of its own. Nil unless the thrall opts in.
	var dedup *dedupCache
	if def.Idempotent {
		dedup = newDedupCache(def.IdempotencyMax)
	}

	// answerFailedCall answers a call whose handler returned an error and reports whether it did:
	// false means the handler succeeded and the caller is still owed its reply. Shared by the
	// mailbox and the concurrent pool, so a failure means the same thing on both.
	answerFailedCall := func(e wire.Envelope, trace string, herr error, respond func(wire.Envelope)) bool {
		if herr == nil {
			return false
		}
		if esc, ok := asEscalate(herr); ok {
			// Reply the caller before we crash, so it learns of the restart instead of
			// hanging until timeout; flush so the reply leaves before the process exits.
			respond(errReply(e, wire.ErrEscalated, esc.Reason))
			lastErr.set("escalated: " + esc.Reason)
			dyingBreath() // let the lord record the reason before the process dies
			log.Error("handler escalated - self-terminating for restart", slog.String("op", e.Op), slog.String("reason", esc.Reason))
			exitProcess(1)
			return true
		}
		if ref, ok := asRefuse(herr); ok {
			// A refusal is the domain saying no, not the thrall failing: the caller gets the code,
			// the state stays as it was, and nothing is recorded as this thrall's last error.
			log.Debug("call refused", slog.String("op", e.Op), slog.String("code", ref.Code), slog.String("trace", trace))
			respond(refusedReply(e, ref))
			return true
		}
		lastErr.set("handler_error: " + herr.Error())
		if how.failureReply != nil {
			// A web thrall answers its own failures as HTTP: nobody downstream has to guess what
			// an error reply means, and the failure is on record here, with the request's trace.
			log.Error("handler failed", slog.String("op", e.Op), slog.String("trace", trace), slog.Any("err", herr))
			respond(okReply(e, how.failureReply()))
			return true
		}
		respond(errReply(e, wire.ErrHandler, herr.Error()))
		return true
	}

	// answerPanickedCall deals with a call handler that panicked (see panic.go for the rule).
	// A web thrall answers the one request and lives on; any other thrall tells the caller and
	// then crashes, the same way an escalation does.
	answerPanickedCall := func(e wire.Envelope, trace string, p *handlerPanic, respond func(wire.Envelope)) {
		lastErr.set(p.reason())
		if how.failureReply != nil {
			stats.panics.Add(1)
			logPanic(log, "handler panicked - the request is answered with a failure, the thrall carries on", e.Op, trace, p)
			respond(okReply(e, how.failureReply()))
			return
		}
		logPanic(log, "handler panicked - self-terminating for restart", e.Op, trace, p)
		respond(errReply(e, wire.ErrEscalated, p.reason()))
		dyingBreath() // flushes the reply too, and lets the lord record the reason
		exitProcess(exitPanic)
	}

	processCall := func(msg *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(msg.Data, &e) != nil {
			return
		}
		respond := replier(nc, msg, e, lastErr, log)
		start := stats.begin()
		mu.Lock()
		defer mu.Unlock()
		if gate.drop(e, respond) {
			return
		}
		stats.started()
		defer stats.end(start)
		ctx.Trace = orNewTrace(e.Trace)
		ctx.MsgID = e.ID
		log.Debug("handling call", slog.String("op", e.Op), slog.String("trace", ctx.Trace))
		h, ok := def.HandleCall[e.Op]
		if !ok {
			respond(errReply(e, wire.ErrUnknownOp, "unknown call op: "+e.Op))
			return
		}
		dkey := ""
		if dedup != nil {
			dkey = dedupKey(e) // empty (no idem and no ID) -> not deduped, never collide on ""
		}
		if dkey != "" {
			if cached, seen := dedup.get(dkey); seen {
				// A duplicate call: return the first reply instead of re-running the handler.
				respond(okReply(e, json.RawMessage(cached)))
				return
			}
		}
		var (
			reply any
			next  S
			herr  error
		)
		if p := guard(func() { reply, next, herr = h(e.Payload, state, ctx) }); p != nil {
			answerPanickedCall(e, ctx.Trace, p, respond)
			return
		}
		if answerFailedCall(e, ctx.Trace, herr, respond) {
			return
		}
		state = next
		if dkey != "" {
			dedup.put(dkey, mustMarshal(reply)) // cache only a successful reply
		}
		respond(okReply(e, reply))
	}

	// onCall is what a received call goes to: the mailbox above, or - for a concurrent web thrall -
	// a pool that runs up to concurrency handlers at once over the state Init returned. That state
	// is captured here, before any message is read, and never reassigned, so the handlers share it
	// without the mailbox lock; each gets its own Ctx, since Trace and MsgID differ per request.
	// Idempotency dedup belongs to the mailbox and is not consulted here (StartWeb never asks for it).
	onCall := processCall
	var pool *callPool
	if concurrency > serialized {
		shared := state
		// The mailbox keeps rewriting ctx (a stray cast still sets its trace), so the pooled
		// requests are cut from a copy taken before anything runs.
		base := *ctx
		pool = newCallPool(concurrency, gate, stats)
		onCall = func(msg *nats.Msg) {
			var e wire.Envelope
			if json.Unmarshal(msg.Data, &e) != nil {
				return
			}
			respond := replier(nc, msg, e, lastErr, log)
			pool.submit(e, respond, func() {
				reqCtx := base
				reqCtx.Trace = orNewTrace(e.Trace)
				reqCtx.MsgID = e.ID
				log.Debug("handling call", slog.String("op", e.Op), slog.String("trace", reqCtx.Trace))
				h, ok := def.HandleCall[e.Op]
				if !ok {
					respond(errReply(e, wire.ErrUnknownOp, "unknown call op: "+e.Op))
					return
				}
				var (
					reply any
					herr  error
				)
				if p := guard(func() { reply, _, herr = h(e.Payload, shared, &reqCtx) }); p != nil {
					answerPanickedCall(e, reqCtx.Trace, p, respond)
					return
				}
				if answerFailedCall(e, reqCtx.Trace, herr, respond) {
					return
				}
				respond(okReply(e, reply))
			})
		}
	}

	// ackDurable acknowledges the source JetStream message (durable cast); it is nil for a
	// non-durable core cast, which needs no ack. On escalation the poison cast is acked before
	// the crash, so it is not redelivered into a loop after the restart.
	processCast := func(data []byte, ackDurable func()) {
		var e wire.Envelope
		if json.Unmarshal(data, &e) != nil {
			return
		}
		start := stats.begin()
		defer stats.end(start)
		mu.Lock()
		defer mu.Unlock()
		stats.started()
		ctx.Trace = orNewTrace(e.Trace)
		ctx.MsgID = e.ID
		log.Debug("handling cast", slog.String("op", e.Op), slog.String("trace", ctx.Trace))
		h, ok := def.HandleCast[e.Op]
		if !ok {
			return
		}
		dkey := ""
		if dedup != nil {
			dkey = dedupKey(e) // empty (no idem and no ID) -> not deduped, never collide on ""
		}
		if dkey != "" {
			if _, seen := dedup.get(dkey); seen {
				return // already processed; a durable cast is acked by the consume loop
			}
		}
		var (
			next S
			herr error
		)
		if p := guard(func() { next, herr = h(e.Payload, state, ctx) }); p != nil {
			// Nobody waits for a cast, so there is no one to answer; the thrall crashes as it
			// would have. The cast is deliberately not acked: a panic is a bug, not a decision
			// about this message, so a durable cast stays at-least-once and is redelivered.
			lastErr.set(p.reason())
			logPanic(log, "cast handler panicked - self-terminating for restart", e.Op, ctx.Trace, p)
			dyingBreath()
			exitProcess(exitPanic)
			return
		}
		if esc, ok := asEscalate(herr); ok {
			// Ack the poison cast before crashing so JetStream does not redeliver it into a
			// crash loop; a non-durable cast (ackDurable nil) is simply dropped, as it is today.
			if ackDurable != nil {
				ackDurable()
			}
			lastErr.set("escalated: " + esc.Reason)
			dyingBreath() // let the lord record the reason before the process dies
			log.Error("cast handler escalated - self-terminating for restart", slog.String("op", e.Op), slog.String("reason", esc.Reason))
			exitProcess(1)
			return
		}
		if ref, ok := asRefuse(herr); ok {
			// Nobody waits for a cast, so a refused one is only noted: it is a normal outcome, not
			// a failure, and the state stays as it was.
			log.Info("cast refused", slog.String("op", e.Op), slog.String("code", ref.Code), slog.String("reason", ref.Message))
			return
		}
		if herr != nil {
			lastErr.set("handler_error: " + herr.Error())
			log.Error("cast handler failed", slog.String("op", e.Op), slog.Any("err", herr))
			return
		}
		state = next
		if dkey != "" {
			dedup.put(dkey, nil) // mark processed only after success
		}
	}

	stop := make(chan struct{})
	stopped := false

	// The goroutines that read messages off the bus. Drain waits for them to finish before it
	// waits for the work they handed over: a reader notices stop only between two reads, so one
	// more message may still be taken after stop, and it must be finished like any other.
	var readers sync.WaitGroup
	read := func(loop func()) {
		readers.Add(1)
		go func() {
			defer readers.Done()
			loop()
		}()
	}

	if durable {
		// Durable: call/info over core, cast via a durable JetStream consumer.
		callSub, err := nc.SubscribeSync(wire.Call(app, name))
		if err != nil {
			return err
		}
		read(func() { verbLoop(callSub, stop, onCall) })

		infoSub, err := nc.SubscribeSync(wire.Info(app, name))
		if err != nil {
			return err
		}
		read(func() { verbLoop(infoSub, stop, func(_ *nats.Msg) {}) }) // TODO handleInfo

		read(func() { consumeDurableCast(nc, app, name, log, stop, processCast) })
	} else {
		// Non-durable: a single wildcard subscription (call/cast/info) -> FIFO.
		dataSub, err := nc.SubscribeSync(wire.Data(app, name))
		if err != nil {
			return err
		}
		read(func() {
			verbLoop(dataSub, stop, func(msg *nats.Msg) {
				switch lastToken(msg.Subject) {
				case "call":
					onCall(msg)
				case "cast":
					processCast(msg.Data, nil) // core cast: no ack
				}
			})
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

	go heartbeat(nc, name, stats, describe, stop)

	if err := startFencingIfSingleton(nc, name, log, stop); err != nil {
		return err
	}
	if err := startLordLivenessFencing(nc, name, log, stop); err != nil {
		return err
	}

	<-stop
	// Drain: nothing new is read from here on. What was already received is finished first - a
	// pooled call that is running or still waiting for a slot (the latter runs or expires by its
	// deadline), then the handler holding the mailbox - and only then is the thrall terminated.
	readers.Wait()
	if pool != nil {
		pool.wait()
	}
	mu.Lock()
	final := state
	mu.Unlock()
	if def.Terminate != nil {
		def.Terminate("drain", final)
	}
	return closeConn(nc)
}

// drainFlushTimeout bounds how long a stopping thrall waits for its last replies to reach the bus.
const drainFlushTimeout = 2 * time.Second

// closeConn ends the thrall's connection after a drain. nats.Conn.Drain only starts the drain and
// returns, and the process exits right after Start does, so the replies of the handlers that were
// just waited for are flushed first: otherwise the last answer of a drained thrall can be lost
// with the process.
func closeConn(nc *nats.Conn) error {
	_ = nc.FlushTimeout(drainFlushTimeout)
	return nc.Drain()
}

// replier builds the function that answers the call carried by msg. A reply larger than one bus
// message cannot be sent at all; dropping it would leave the caller waiting out its timeout with
// nothing to go on, so it is told what happened instead.
func replier(nc *nats.Conn, msg *nats.Msg, e wire.Envelope, lastErr *lastError, log *slog.Logger) func(wire.Envelope) {
	return func(reply wire.Envelope) {
		data := mustJSON(reply)
		if err := msg.Respond(data); errors.Is(err, nats.ErrMaxPayload) {
			tooLarge := tooLargeReply(e, len(data), nc.MaxPayload())
			lastErr.set(wire.ErrReplyTooLarge + ": " + tooLarge.Error.Message)
			log.Error("reply too large for the bus", slog.String("op", e.Op), slog.String("trace", e.Trace),
				slog.Int("bytes", len(data)), slog.Int64("max_payload", nc.MaxPayload()))
			_ = msg.Respond(mustJSON(tooLarge))
		}
	}
}

// stopping reports whether the thrall has been told to stop.
func stopping(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// verbLoop reads messages from a synchronous subscription and passes them to the handler
// (serialized - one message at a time), until stop arrives. A stop does not wait out the read
// in progress: the subscription is closed, which makes a blocked read return at once, so an idle
// thrall drains immediately. Messages the subscription still buffered are not handled; a message
// already taken is.
func verbLoop(sub *nats.Subscription, stop <-chan struct{}, handle func(*nats.Msg)) {
	returned := make(chan struct{})
	defer close(returned)
	go func() {
		select {
		case <-stop:
			_ = sub.Unsubscribe()
		case <-returned:
		}
	}()
	for {
		if stopping(stop) {
			return
		}
		msg, err := sub.NextMsg(verbPollInterval)
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			return // the subscription was closed: a stop, or the connection went away
		}
		handle(msg)
	}
}

// verbPollInterval is how long one read waits for a message. It no longer bounds how fast a stop
// is noticed (the stop closes the subscription); it only keeps the loop turning.
const verbPollInterval = 500 * time.Millisecond

// Durable consumer tuning. Fetching in batches amortizes the per-message server round-trip
// (a Fetch(1) loop pays one round-trip per cast, which caps drain throughput). The batch is
// processed and acked in arrival order, so FIFO holds in the happy path; AckWait bounds how
// long the last message in a batch may sit unacked while earlier ones process, and
// MaxAckPending (>= batch) bounds the in-flight unacked messages the server will hand out.
const (
	durableBatchSize     = 128
	durableFetchMaxWait  = 500 * time.Millisecond // one fetch; a stop cancels it sooner
	durableAckWait       = 30 * time.Second
	durableMaxAckPending = 512
)

// consumeDurableCast reads casts from a durable JetStream consumer with explicit ack.
// While the thrall is down, casts accumulate in the stream (the lord created it) and are
// drained on its return. At-least-once -> handlers must be idempotent.
func consumeDurableCast(nc *nats.Conn, app, name string, log *slog.Logger, stop <-chan struct{}, processCast func([]byte, func())) {
	js, err := nc.JetStream()
	if err != nil {
		log.Error("jetstream unavailable", slog.Any("err", err))
		return
	}
	sub, err := js.PullSubscribe(wire.Cast(app, name), name,
		nats.BindStream(wire.Stream(app, name)), nats.DeliverAll(),
		nats.AckWait(durableAckWait), nats.MaxAckPending(durableMaxAckPending))
	if err != nil {
		// A durable consumer created by an earlier SDK version already exists with a different
		// ack config, and nats.go refuses to reconcile it (it errors instead of updating). Attach
		// to it as-is: draining continues (the batched fetch still applies; only the consumer's
		// stored ack bounds keep their old values, which recreating the consumer would refresh).
		// This mirrors the TS SDK's attach-if-exists behavior; a fresh deployment never hits it.
		sub, err = js.PullSubscribe(wire.Cast(app, name), name,
			nats.BindStream(wire.Stream(app, name)), nats.DeliverAll())
		if err != nil {
			log.Error("durable consumer setup failed", slog.Any("err", err))
			return
		}
	}
	// A fetch in progress is abandoned the moment stop arrives, so an idle durable thrall drains
	// at once instead of waiting out the fetch.
	untilStop, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-untilStop.Done():
		}
	}()
	for {
		if stopping(stop) {
			return
		}
		fetch, done := context.WithTimeout(untilStop, durableFetchMaxWait)
		msgs, err := sub.Fetch(durableBatchSize, nats.Context(fetch))
		done()
		if err != nil {
			continue // timeout / no messages / stopping
		}
		for _, m := range msgs {
			// A drain does not wait for the rest of the batch: with a backlog that is up to
			// durableBatchSize handlers, far past the lord's grace period. The casts not yet
			// handled stay unacknowledged and are redelivered to the next process.
			if stopping(stop) {
				return
			}
			m := m
			// On escalation processCast acks (synchronously) before it crashes, so the poison
			// cast is not redelivered; the happy path returns here and we ack in arrival order.
			processCast(m.Data, func() { _ = m.AckSync() }) // process ...
			_ = m.Ack()                                     // ... and only then ack (in arrival order -> FIFO)
		}
	}
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
	if reply.Status == wire.StatusError {
		return nil, callErrorFrom(reply.Error)
	}
	return reply.Payload, nil
}

// lastError holds the reason of a thrall's most recent handler failure. It has its own lock because
// the heartbeat goroutine reads it while a handler (under the mailbox lock) writes it.
type lastError struct {
	mu  sync.Mutex
	msg string
	ms  int64
}

func (e *lastError) set(msg string) {
	e.mu.Lock()
	e.msg, e.ms = msg, time.Now().UnixMilli()
	e.mu.Unlock()
}

func (e *lastError) get() (string, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.msg, e.ms
}

// publishHeartbeat sends one liveness beat carrying the thrall's metrics and self-description.
// Shared by the periodic loop and the crash-path dying breath. describe may return nil to attach none.
func publishHeartbeat(nc *nats.Conn, name string, stats *mailboxStats, describe func() *wire.ThrallDescribe) {
	m := stats.snapshot()
	m.Describe = describe()
	hb := wire.Envelope{V: 1, Kind: wire.KindHB, To: name, TS: time.Now().UnixMilli(),
		Payload: mustMarshal(m)}
	_ = nc.Publish(wire.Heartbeat(name), mustJSON(hb))
}

// heartbeat publishes the thrall's liveness beat at the lord-configured interval. describe returns
// the self-description to attach to each beat (a fresh value per tick, so a mutable field such as the
// last error is picked up as it changes); it may return nil to attach none.
func heartbeat(nc *nats.Conn, name string, stats *mailboxStats, describe func() *wire.ThrallDescribe, stop <-chan struct{}) {
	tick := func() { publishHeartbeat(nc, name, stats, describe) }
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

func lastToken(subject string) string {
	for i := len(subject) - 1; i >= 0; i-- {
		if subject[i] == '.' {
			return subject[i+1:]
		}
	}
	return subject
}
