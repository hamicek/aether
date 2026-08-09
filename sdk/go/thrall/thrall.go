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
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// Ctx is passed to init and to every handler. WE DO NOT HIDE NATS BEHIND THE THRALL -
// a thrall has full access to JetStream, KV and its own subjects via Ctx.NATS.
type Ctx struct {
	NATS *nats.Conn
	Name string
	App  string
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
	ctx := &Ctx{NATS: nc, Name: name, App: app}

	state, err := def.Init(ctx)
	if err != nil {
		return err
	}

	// Serialized mailbox: a mutex around every state change (call and cast).
	var mu sync.Mutex

	processCall := func(msg *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(msg.Data, &e) != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
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
		mu.Lock()
		defer mu.Unlock()
		h, ok := def.HandleCast[e.Op]
		if !ok {
			return
		}
		next, herr := h(e.Payload, state, ctx)
		if herr != nil {
			fmt.Fprintf(os.Stderr, "[%s] cast %s failed: %v\n", name, e.Op, herr)
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

		go consumeDurableCast(nc, app, name, stop, processCast)
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

	go heartbeat(nc, name, stop)

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
func consumeDurableCast(nc *nats.Conn, app, name string, stop <-chan struct{}, processCast func([]byte)) {
	js, err := nc.JetStream()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] jetstream: %v\n", name, err)
		return
	}
	sub, err := js.PullSubscribe(wire.Cast(app, name), name,
		nats.BindStream(wire.Stream(app, name)), nats.DeliverAll())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] durable consumer: %v\n", name, err)
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

// Call = synchronous request/reply (GenServer.call) to another thrall.
func Call(target, op string, payload any, timeout time.Duration) (json.RawMessage, error) {
	if sharedConn == nil {
		return nil, fmt.Errorf("no connection - call Start() first")
	}
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: target, Op: op, Payload: mustMarshal(payload), TS: time.Now().UnixMilli()}
	msg, err := sharedConn.Request(wire.Call(os.Getenv("AETHER_APP"), target), mustJSON(req), timeout)
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

// Cast = fire-and-forget (GenServer.cast) to another thrall.
func Cast(target, op string, payload any) error {
	if sharedConn == nil {
		return fmt.Errorf("no connection - call Start() first")
	}
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCast, To: target, Op: op, Payload: mustMarshal(payload), TS: time.Now().UnixMilli()}
	return sharedConn.Publish(wire.Cast(os.Getenv("AETHER_APP"), target), mustJSON(e))
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

func heartbeat(nc *nats.Conn, name string, stop <-chan struct{}) {
	tick := func() {
		hb := wire.Envelope{V: 1, Kind: wire.KindHB, To: name, TS: time.Now().UnixMilli()}
		_ = nc.Publish(wire.Heartbeat(name), mustJSON(hb))
	}
	tick()
	t := time.NewTicker(2 * time.Second)
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
