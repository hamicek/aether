package thrall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

const clientTimeout = 2 * time.Second

// clientConn starts an embedded ether for one test and returns its connection.
func clientConn(t *testing.T) *nats.Conn {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether.Start: %v", err)
	}
	t.Cleanup(eth.Stop)
	return eth.Conn()
}

// stubThrall answers calls on a thrall's call subject the way a real thrall would, built only from
// the SDK's public wire surface (CallSubject, Envelope, OKReply, ErrorReply) - which is exactly
// what a library outside this module has for writing a test double. reply returns the envelope to
// answer with; the request envelopes are delivered on the returned channel.
func stubThrall(t *testing.T, nc *nats.Conn, app, name string, reply func(req Envelope) Envelope) <-chan Envelope {
	t.Helper()
	seen := make(chan Envelope, 16)
	sub, err := nc.Subscribe(CallSubject(app, name), func(m *nats.Msg) {
		var req Envelope
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("stub %s: decode request: %v", name, err)
			return
		}
		seen <- req
		data, _ := json.Marshal(reply(req))
		_ = m.Respond(data)
	})
	if err != nil {
		t.Fatalf("stub %s: subscribe: %v", name, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return seen
}

func nextRequest(t *testing.T, seen <-chan Envelope) Envelope {
	t.Helper()
	select {
	case req := <-seen:
		return req
	case <-time.After(clientTimeout):
		t.Fatal("the stub thrall received no request")
		return Envelope{}
	}
}

type balance struct {
	Owner  string `json:"owner"`
	Amount int    `json:"amount"`
}

// TestClientCallNeedsOnlyAConnection: a Client built from a bare connection makes a call that a
// thrall cannot tell from one made by another thrall, and returns the reply payload.
func TestClientCallNeedsOnlyAConnection(t *testing.T) {
	nc := clientConn(t)
	seen := stubThrall(t, nc, "shop", "account", func(req Envelope) Envelope {
		return OKReply(req, balance{Owner: "ada", Amount: 7})
	})

	before := time.Now()
	raw, err := NewClient(nc, "shop").Call("account", "balance", map[string]string{"owner": "ada"}, clientTimeout)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(raw) != `{"owner":"ada","amount":7}` {
		t.Fatalf("reply payload = %s", raw)
	}

	req := nextRequest(t, seen)
	if req.V != 1 || req.Kind != KindCall || req.To != "account" || req.Op != "balance" {
		t.Fatalf("request envelope = %+v", req)
	}
	if string(req.Payload) != `{"owner":"ada"}` {
		t.Fatalf("request payload = %s", req.Payload)
	}
	if req.ID == "" || req.Trace == "" {
		t.Fatalf("a call must carry an id and a (fresh) trace, got id=%q trace=%q", req.ID, req.Trace)
	}
	if req.Idem != "" {
		t.Fatalf("idem = %q, want none", req.Idem)
	}
	// The deadline is the send time plus the timeout: when this caller stops waiting.
	earliest, latest := before.Add(clientTimeout).UnixMilli(), time.Now().Add(clientTimeout).UnixMilli()
	if req.Deadline < earliest || req.Deadline > latest {
		t.Fatalf("deadline = %d, want within [%d, %d]", req.Deadline, earliest, latest)
	}
	if req.Deadline != req.TS+clientTimeout.Milliseconds() {
		t.Fatalf("deadline %d is not ts %d + timeout", req.Deadline, req.TS)
	}
}

// TestClientAddressesAnotherApp: the app a call goes to is explicit - the client's own by default,
// another one per send (InApp) or per derived client.
func TestClientAddressesAnotherApp(t *testing.T) {
	nc := clientConn(t)
	answerFrom := func(app string) func(Envelope) Envelope {
		return func(req Envelope) Envelope { return OKReply(req, app) }
	}
	stubThrall(t, nc, "ujito", "store", answerFrom("ujito"))
	stubThrall(t, nc, "ae", "store", answerFrom("ae"))

	own := NewClient(nc, "ujito")
	cases := []struct {
		name   string
		caller Caller
		opts   []SendOption
		want   string
	}{
		{"own app by default", own, nil, "ujito"},
		{"another app for one call", own, []SendOption{InApp("ae")}, "ae"},
		{"a client bound to another app", own.With(InApp("ae")), nil, "ae"},
		{"an empty app keeps the default", own, []SendOption{InApp("")}, "ujito"},
		{"from a handler's Ctx", &Ctx{NATS: nc, App: "ujito"}, []SendOption{InApp("ae")}, "ae"},
	}
	for _, tc := range cases {
		got, err := CallInto[string](tc.caller, "store", "whoami", nil, clientTimeout, tc.opts...)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: answered by app %q, want %q", tc.name, got, tc.want)
		}
	}
	if own.App() != "ujito" {
		t.Errorf("With must not change the client it derives from: App() = %q", own.App())
	}
}

// TestClientCarriesTraceAndIdempotencyKey: the caller's trace and idempotency key reach the target,
// whether set per send, as a client default, or taken from the message a Ctx is handling.
func TestClientCarriesTraceAndIdempotencyKey(t *testing.T) {
	nc := clientConn(t)
	seen := stubThrall(t, nc, "shop", "account", func(req Envelope) Envelope { return OKReply(req, nil) })
	client := NewClient(nc, "shop")

	if _, err := client.Call("account", "withdraw", nil, clientTimeout, WithTrace("t-req-1"), WithIdempotencyKey("withdraw-42")); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace != "t-req-1" || req.Idem != "withdraw-42" {
		t.Fatalf("per-send options: trace=%q idem=%q", req.Trace, req.Idem)
	}

	perRequest := client.With(WithTrace("t-req-2"))
	if _, err := perRequest.Call("account", "balance", nil, clientTimeout); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace != "t-req-2" {
		t.Fatalf("client default trace: got %q", req.Trace)
	}
	if _, err := perRequest.Call("account", "balance", nil, clientTimeout, WithTrace("t-override")); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace != "t-override" {
		t.Fatalf("a per-send trace must override the client default, got %q", req.Trace)
	}

	ctx := &Ctx{NATS: nc, App: "shop", Trace: "t-handled"}
	if _, err := ctx.Call("account", "balance", nil, clientTimeout); err != nil {
		t.Fatalf("Ctx.Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace != "t-handled" {
		t.Fatalf("Ctx.Call must propagate the handled message's trace, got %q", req.Trace)
	}

	// An empty trace is not a trace: the call mints its own rather than sending none.
	if _, err := client.Call("account", "balance", nil, clientTimeout, WithTrace("")); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace == "" {
		t.Fatal("a call without a caller trace must mint one")
	}

	// An edge that has to know the trace before it sends (to log it, to return it) mints it itself.
	minted := NewTrace()
	if minted == "" || minted == NewTrace() {
		t.Fatalf("NewTrace must mint a fresh, non-empty trace each time, got %q", minted)
	}
	if _, err := client.Call("account", "balance", nil, clientTimeout, WithTrace(minted)); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if req := nextRequest(t, seen); req.Trace != minted {
		t.Fatalf("a minted trace must travel as given: got %q, want %q", req.Trace, minted)
	}
}

// TestCallIntoDecodesTheReply: the typed form marshals the request and unmarshals the reply into T.
func TestCallIntoDecodesTheReply(t *testing.T) {
	nc := clientConn(t)
	stubThrall(t, nc, "shop", "account", func(req Envelope) Envelope {
		switch req.Op {
		case "balance":
			var in balance
			_ = json.Unmarshal(req.Payload, &in)
			return OKReply(req, balance{Owner: in.Owner, Amount: 7})
		case "ping":
			return Envelope{V: 1, ID: req.ID, Kind: KindReply, Status: StatusOK} // a reply with no payload at all
		default:
			return OKReply(req, "not a balance")
		}
	})
	client := NewClient(nc, "shop")

	got, err := CallInto[balance](client, "account", "balance", balance{Owner: "ada"}, clientTimeout)
	if err != nil {
		t.Fatalf("CallInto: %v", err)
	}
	if got != (balance{Owner: "ada", Amount: 7}) {
		t.Fatalf("CallInto = %+v", got)
	}

	if got, err := CallInto[balance](client, "account", "ping", nil, clientTimeout); err != nil || got != (balance{}) {
		t.Fatalf("a reply without a payload must decode to the zero value, got %+v, %v", got, err)
	}

	if _, err := CallInto[balance](client, "account", "wrong_shape", nil, clientTimeout); err == nil {
		t.Fatal("a reply that does not fit T must be an error")
	}
}

// TestCallReturnsTypedError: an error reply becomes a *CallError exposing type, code and message,
// which still reads as the plain "<type>: <message>" error it always was - also through wrapping.
func TestCallReturnsTypedError(t *testing.T) {
	nc := clientConn(t)
	stubThrall(t, nc, "shop", "account", func(req Envelope) Envelope {
		switch req.Op {
		case "withdraw":
			return ErrorReply(req, WireError{Type: ErrRefused, Code: "insufficient_funds", Message: "the balance is 3, the withdrawal 5"})
		case "expired":
			return ErrorReply(req, WireError{Type: ErrDeadlineExceeded, Message: "call expired 9 ms before it could be handled", Retryable: true})
		default:
			return ErrorReply(req, WireError{Type: ErrHandler, Message: "disk on fire"})
		}
	})
	client := NewClient(nc, "shop")

	_, err := client.Call("account", "withdraw", nil, clientTimeout)
	var refused *CallError
	if !errors.As(err, &refused) {
		t.Fatalf("a refusal must be a *CallError, got %T: %v", err, err)
	}
	if refused.Type != ErrRefused || refused.Code != "insufficient_funds" || refused.Message != "the balance is 3, the withdrawal 5" || refused.Retryable {
		t.Fatalf("refusal = %+v", refused)
	}
	if !refused.Refused() {
		t.Fatal("Refused() = false for a refusal")
	}
	if got := err.Error(); got != "refused: the balance is 3, the withdrawal 5" {
		t.Fatalf("error text = %q", got)
	}

	// The same answers through the helpers, and through the wrapping a client library adds.
	wrapped := fmt.Errorf("account withdraw: %w", err)
	if r, ok := AsRefusal(wrapped); !ok || r.Code != "insufficient_funds" {
		t.Fatalf("AsRefusal(wrapped) = %+v, %v", r, ok)
	}
	if code := RefusalCode(wrapped); code != "insufficient_funds" {
		t.Fatalf("RefusalCode(wrapped) = %q", code)
	}

	_, err = client.Call("account", "broken", nil, clientTimeout)
	var failed *CallError
	if !errors.As(err, &failed) || failed.Type != ErrHandler || failed.Code != "" || failed.Refused() {
		t.Fatalf("a handler failure = %+v (%v)", failed, err)
	}
	if _, ok := AsRefusal(err); ok {
		t.Fatal("a handler failure must not read as a refusal")
	}
	if code := RefusalCode(err); code != "" {
		t.Fatalf("RefusalCode of a failure = %q, want none", code)
	}
	if got := err.Error(); got != "handler_error: disk on fire" {
		t.Fatalf("error text = %q", got)
	}

	_, err = client.Call("account", "expired", nil, clientTimeout)
	var expired *CallError
	if !errors.As(err, &expired) || expired.Type != ErrDeadlineExceeded || !expired.Retryable {
		t.Fatalf("a dropped call = %+v (%v)", expired, err)
	}
}

// TestCallToOldThrall: a thrall built before refusals existed answers with the shapes of that time.
// A new caller reads them as the generic error type, never as a refusal, and never crashes on a
// malformed error reply.
func TestCallToOldThrall(t *testing.T) {
	nc := clientConn(t)
	oldReplies := map[string]string{
		// What every SDK sent for a failed handler before this change: no "code".
		"rule":   `{"v":1,"id":"x","kind":"reply","status":"error","error":{"type":"handler_error","message":"the balance is 3, the withdrawal 5","retryable":false}}`,
		"nodata": `{"v":1,"id":"x","kind":"reply","status":"error"}`,
		"ok":     `{"v":1,"id":"x","kind":"reply","status":"ok","payload":{"owner":"ada","amount":7}}`,
	}
	sub, err := nc.Subscribe(wire.Call("legacy", "account"), func(m *nats.Msg) {
		var req wire.Envelope
		_ = json.Unmarshal(m.Data, &req)
		_ = m.Respond([]byte(oldReplies[req.Op]))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	client := NewClient(nc, "legacy")

	_, err = client.Call("account", "rule", nil, clientTimeout)
	var ce *CallError
	if !errors.As(err, &ce) || ce.Type != ErrHandler || ce.Code != "" {
		t.Fatalf("old error reply = %+v (%v)", ce, err)
	}
	if RefusalCode(err) != "" {
		t.Fatal("an old thrall's error must not read as a refusal")
	}
	if got := err.Error(); got != "handler_error: the balance is 3, the withdrawal 5" {
		t.Fatalf("error text = %q", got)
	}

	if _, err := client.Call("account", "nodata", nil, clientTimeout); !errors.As(err, &ce) {
		t.Fatalf("an error reply without an error object must still be a *CallError, got %T: %v", err, err)
	}

	got, err := CallInto[balance](client, "account", "ok", nil, clientTimeout)
	if err != nil || got != (balance{Owner: "ada", Amount: 7}) {
		t.Fatalf("old ok reply = %+v, %v", got, err)
	}
}

// TestCallTransportErrors: when no reply arrives the NATS error comes back as it is, so a caller
// tells "nobody there" and "no answer in time" from an error reply.
func TestCallTransportErrors(t *testing.T) {
	nc := clientConn(t)
	client := NewClient(nc, "shop")

	_, err := client.Call("nobody", "anything", nil, clientTimeout)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("a call to a thrall that does not exist = %v, want ErrNoResponders", err)
	}
	var ce *CallError
	if errors.As(err, &ce) {
		t.Fatal("a transport error must not be a *CallError")
	}

	sub, err := nc.Subscribe(CallSubject("shop", "mute"), func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if _, err := client.Call("mute", "anything", nil, 50*time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("a call nobody answers = %v, want ErrTimeout", err)
	}
}

// TestCallRejectsUnencodablePayload: a payload that cannot be marshalled is the caller's error and
// nothing is sent - it used to go out as an empty payload.
func TestCallRejectsUnencodablePayload(t *testing.T) {
	nc := clientConn(t)
	seen := stubThrall(t, nc, "shop", "account", func(req Envelope) Envelope { return OKReply(req, nil) })
	client := NewClient(nc, "shop")

	if _, err := client.Call("account", "balance", make(chan int), clientTimeout); err == nil {
		t.Fatal("Call with an unencodable payload must fail")
	}
	if err := client.Cast("account", "note", make(chan int)); err == nil {
		t.Fatal("Cast with an unencodable payload must fail")
	}
	select {
	case req := <-seen:
		t.Fatalf("nothing may be sent, but the thrall received %+v", req)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestClientCast: a cast from a client lands on the target's cast subject with the same envelope a
// thrall's own cast produces - and without a deadline, since nobody waits for it.
func TestClientCast(t *testing.T) {
	nc := clientConn(t)
	sub, err := nc.SubscribeSync(CastSubject("ae", "audit"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	client := NewClient(nc, "shop")
	if err := client.Cast("audit", "note", map[string]int{"n": 1}, InApp("ae"), WithTrace("t-9"), WithIdempotencyKey("note-1")); err != nil {
		t.Fatalf("Cast: %v", err)
	}
	msg, err := sub.NextMsg(clientTimeout)
	if err != nil {
		t.Fatalf("the cast did not arrive: %v", err)
	}
	var e Envelope
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		t.Fatal(err)
	}
	if e.Kind != KindCast || e.To != "audit" || e.Op != "note" || e.Trace != "t-9" || e.Idem != "note-1" || string(e.Payload) != `{"n":1}` {
		t.Fatalf("cast envelope = %+v", e)
	}
	if e.Deadline != 0 {
		t.Fatalf("a cast must not carry a deadline, got %d", e.Deadline)
	}
}

// TestPackageCallNeedsARunningThrall: the package-level Call/Cast belong to a thrall process; before
// one is started they fail with a clear error instead of dereferencing a missing connection.
func TestPackageCallNeedsARunningThrall(t *testing.T) {
	previous := processClient.Swap(nil)
	t.Cleanup(func() { processClient.Store(previous) })

	if _, err := Call("account", "balance", nil, clientTimeout); err == nil {
		t.Fatal("Call before Start must fail")
	}
	if err := Cast("account", "note", nil); err == nil {
		t.Fatal("Cast before Start must fail")
	}
	if err := CastConfirmed("account", "note", nil, clientTimeout); err == nil {
		t.Fatal("CastConfirmed before Start must fail")
	}
}

// TestRefuseAlwaysCarriesACode: a refusal without a code would be indistinguishable, through
// RefusalCode, from "not a refusal" - so an empty code becomes UnspecifiedRefusal.
func TestRefuseAlwaysCarriesACode(t *testing.T) {
	for _, err := range []error{Refuse("", "no"), Refusef("", "no %d", 1)} {
		ref, ok := asRefuse(err)
		if !ok || ref.Code != UnspecifiedRefusal {
			t.Fatalf("refusal without a code = %+v, want code %q", ref, UnspecifiedRefusal)
		}
	}
	ref, _ := asRefuse(fmt.Errorf("wrapped: %w", Refuse("day_closed", "the day is closed")))
	if ref == nil || ref.Code != "day_closed" || ref.Message != "the day is closed" {
		t.Fatalf("a wrapped refusal = %+v", ref)
	}
	reply := refusedReply(Envelope{V: 1, ID: "c-1"}, &RefuseError{Code: "day_closed", Message: "the day is closed"})
	if reply.ID != "c-1" || reply.Error == nil || reply.Error.Type != ErrRefused || reply.Error.Code != "day_closed" || reply.Error.Retryable {
		t.Fatalf("refusal reply = %+v (error %+v)", reply, reply.Error)
	}
}
