package thrall

// The client side of the SDK: calling and casting to thralls. Everything that sends a message -
// a standalone Client, a handler's Ctx, the package-level Call/Cast - goes through the code in this
// file, so they produce the same envelope and read replies the same way.

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// SendOption customizes an outgoing call/cast: an idempotency key, the trace to carry, the app the
// target lives in. Options given to a single send override the defaults a Client was built with.
type SendOption func(*sendOpts)

type sendOpts struct {
	idem  string
	trace string
	app   string
}

// WithIdempotencyKey attaches a stable idempotency key to a call/cast. On an idempotent thrall a
// retry carrying the same key is not re-processed: a duplicate cast is skipped and a duplicate
// call returns the first reply. See AE-077.
func WithIdempotencyKey(key string) SendOption {
	return func(o *sendOpts) { o.idem = key }
}

// WithTrace makes the message carry the given correlation id instead of minting a fresh one. A
// handler's Ctx.Call already propagates the trace of the message it is handling; this is for code
// that holds a trace but no Ctx (a library called from a handler, an edge that received a trace
// from outside). An empty trace is ignored.
func WithTrace(trace string) SendOption {
	return func(o *sendOpts) {
		if trace != "" {
			o.trace = trace
		}
	}
}

// InApp addresses the target in another app: the message goes to aether.<app>.<target>.<verb>
// instead of the sender's own app. This is how an application reaches a shared thrall deployed in
// a different app namespace. An empty app is ignored.
func InApp(app string) SendOption {
	return func(o *sendOpts) {
		if app != "" {
			o.app = app
		}
	}
}

func (o sendOpts) with(opts []SendOption) sendOpts {
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Caller is the request/reply surface shared by a Client and a handler's Ctx. A library that calls
// thralls should accept a Caller: handed a Ctx it propagates the trace of the message being
// handled, handed a Client it works anywhere a NATS connection exists.
type Caller interface {
	Call(target, op string, payload any, timeout time.Duration, opts ...SendOption) (json.RawMessage, error)
}

var (
	_ Caller = (*Client)(nil)
	_ Caller = (*Ctx)(nil)
)

// Client calls and casts to thralls over a NATS connection. It needs nothing else: no lord, no
// environment, no running thrall - so a gateway, a CLI, a test or a library can talk to the ether
// with the same envelope a thrall's own Ctx.Call produces. A Client is immutable and safe for
// concurrent use.
type Client struct {
	nc       *nats.Conn
	app      string
	defaults sendOpts
}

// NewClient builds a client that addresses thralls in app by default. opts become the defaults of
// every send (e.g. WithTrace for a client that lives for one request); InApp per send, or a second
// client, reaches another app.
func NewClient(nc *nats.Conn, app string, opts ...SendOption) *Client {
	return &Client{nc: nc, app: app, defaults: sendOpts{}.with(opts)}
}

// With returns a copy of the client with further default options, leaving the receiver untouched.
// The typical use is binding a per-request trace to a long-lived client:
// client.With(thrall.WithTrace(trace)).
func (c *Client) With(opts ...SendOption) *Client {
	return &Client{nc: c.nc, app: c.app, defaults: c.defaults.with(opts)}
}

// App is the app this client addresses when a send does not say otherwise.
func (c *Client) App() string { return c.app }

// Call is a synchronous request/reply (GenServer.call). It returns the reply payload, or:
//   - a *CallError when the target answered with an error reply (a refusal, a handler failure, an
//     unknown op, an expired deadline) - inspect it with errors.As, AsRefusal or RefusalCode;
//   - the NATS error when no reply arrived (nats.ErrTimeout, nats.ErrNoResponders).
//
// The envelope carries the instant this call stops waiting (now + timeout), so a thrall that only
// gets to it later drops it instead of executing a request nobody is waiting for.
func (c *Client) Call(target, op string, payload any, timeout time.Duration, opts ...SendOption) (json.RawMessage, error) {
	o := c.defaults.with(opts)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("call %s/%s: encode payload: %w", target, op, err)
	}
	now := time.Now()
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: orNewTrace(o.trace), Idem: o.idem, Kind: wire.KindCall,
		To: target, Op: op, Payload: body, TS: now.UnixMilli(), Deadline: wire.DeadlineAfter(now, timeout)}
	msg, err := c.nc.Request(wire.Call(c.appFor(o), target), mustJSON(req), timeout)
	if err != nil {
		return nil, err
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, fmt.Errorf("call %s/%s: decode reply: %w", target, op, err)
	}
	if reply.Status == wire.StatusError {
		return nil, callErrorFrom(reply.Error)
	}
	return reply.Payload, nil
}

// Cast is fire-and-forget (GenServer.cast): it returns once the message is handed to the
// connection, and says nothing about whether the target received or handled it.
func (c *Client) Cast(target, op string, payload any, opts ...SendOption) error {
	o := c.defaults.with(opts)
	e, err := newCastEnvelope(orNewTrace(o.trace), target, op, payload, o.idem)
	if err != nil {
		return err
	}
	return c.nc.Publish(wire.Cast(c.appFor(o), target), mustJSON(e))
}

// CastConfirmed is a cast to a durable thrall that returns only once the message is durably stored
// in the target's mailbox. A nil error means stored; any error means the message may not be stored
// and the caller decides whether to retry (with the same WithIdempotencyKey, so a retry of a
// message that did land is deduplicated). A target without a durable mailbox fails with
// ErrNotDurable and nothing is sent. See confirm.go.
func (c *Client) CastConfirmed(target, op string, payload any, timeout time.Duration, opts ...SendOption) error {
	o := c.defaults.with(opts)
	return doCastConfirmed(c.nc, c.appFor(o), orNewTrace(o.trace), target, op, payload, timeout, o.idem)
}

func (c *Client) appFor(o sendOpts) string {
	if o.app != "" {
		return o.app
	}
	return c.app
}

// CallInto is the typed form of a call: it marshals the request, calls the target and unmarshals
// the reply payload into T. c is a Client or a handler's Ctx. Errors are those of Call, plus a
// decode error when the reply does not fit T.
func CallInto[T any](c Caller, target, op string, payload any, timeout time.Duration, opts ...SendOption) (T, error) {
	var out T
	raw, err := c.Call(target, op, payload, timeout, opts...)
	if err != nil {
		return out, err
	}
	if len(raw) == 0 {
		return out, nil // a reply without a payload decodes to the zero value
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("call %s/%s: decode reply payload: %w", target, op, err)
	}
	return out, nil
}

// newCastEnvelope builds the envelope shared by the plain and the confirmed cast.
func newCastEnvelope(trace, target, op string, payload any, idem string) (wire.Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return wire.Envelope{}, fmt.Errorf("cast %s/%s: encode payload: %w", target, op, err)
	}
	return wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: trace, Idem: idem, Kind: wire.KindCast, To: target, Op: op,
		Payload: body, TS: time.Now().UnixMilli()}, nil
}

// NewTrace mints a fresh correlation id. A call or cast without a trace mints one by itself; this
// is for an edge that starts an operation and has to know its trace before sending anything - to
// log it, or to hand it back to whoever is waiting outside the ether (an HTTP response header) -
// and then passes it on with WithTrace.
func NewTrace() string { return nats.NewInbox() }

// orNewTrace returns the given trace, or a fresh one when it is empty (the message came in
// without a trace, so this sender is the edge that starts one).
func orNewTrace(trace string) string {
	if trace == "" {
		return NewTrace()
	}
	return trace
}

// processClient is the client of the thrall running in this process, set by Start/StartFSM/... It
// backs the package-level Call/Cast, which exist for code that runs inside a thrall but outside a
// handler (a timer, a goroutine). It is atomic because that code reads it from its own goroutine.
var processClient atomic.Pointer[Client]

func ownClient() (*Client, error) {
	c := processClient.Load()
	if c == nil {
		return nil, fmt.Errorf("no connection - call Start() first")
	}
	return c, nil
}

// Call = synchronous request/reply (GenServer.call) to another thrall of this thrall's app, over
// the thrall's own connection. Called outside a handler it mints a fresh trace; from within a
// handler use Ctx.Call to propagate the current trace, and outside a thrall use a Client.
func Call(target, op string, payload any, timeout time.Duration, opts ...SendOption) (json.RawMessage, error) {
	c, err := ownClient()
	if err != nil {
		return nil, err
	}
	return c.Call(target, op, payload, timeout, opts...)
}

// Cast = fire-and-forget (GenServer.cast) to another thrall. Mints a fresh trace; from within
// a handler use Ctx.Cast to propagate the current trace.
func Cast(target, op string, payload any, opts ...SendOption) error {
	c, err := ownClient()
	if err != nil {
		return err
	}
	return c.Cast(target, op, payload, opts...)
}

// CastConfirmed is a cast to a durable thrall that returns only once the message is durably
// stored in the target's mailbox (see Client.CastConfirmed). Mints a fresh trace; from within a
// handler use Ctx.CastConfirmed.
func CastConfirmed(target, op string, payload any, timeout time.Duration, opts ...SendOption) error {
	c, err := ownClient()
	if err != nil {
		return err
	}
	return c.CastConfirmed(target, op, payload, timeout, opts...)
}

// client is the Ctx's view of the ether: its connection and app, carrying the trace of the message
// currently being handled. It is built per send because the trace changes with every message.
func (c *Ctx) client() *Client {
	return &Client{nc: c.NATS, app: c.App, defaults: sendOpts{trace: c.Trace}}
}

// Call is the trace-propagating request/reply from inside a handler: the downstream message
// carries the trace of the message currently being handled. See Client.Call for the errors.
func (c *Ctx) Call(target, op string, payload any, timeout time.Duration, opts ...SendOption) (json.RawMessage, error) {
	return c.client().Call(target, op, payload, timeout, opts...)
}

// Cast is the trace-propagating fire-and-forget from inside a handler.
func (c *Ctx) Cast(target, op string, payload any, opts ...SendOption) error {
	return c.client().Cast(target, op, payload, opts...)
}

// CastConfirmed is the trace-propagating confirmed cast from inside a handler.
func (c *Ctx) CastConfirmed(target, op string, payload any, timeout time.Duration, opts ...SendOption) error {
	return c.client().CastConfirmed(target, op, payload, timeout, opts...)
}
