package thrall

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

const confirmTimeout = 2 * time.Second

// confirmEnv starts an embedded ether and returns it with a connection and a JetStream context.
// Each test gets its own ether, so mailbox streams (and the mailbox cache, keyed per connection)
// never leak between tests.
func confirmEnv(t *testing.T) (*ether.Ether, *nats.Conn, nats.JetStreamContext) {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether.Start: %v", err)
	}
	t.Cleanup(eth.Stop)
	nc := eth.Conn()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	return eth, nc, js
}

// provisionMailbox creates the durable mailbox the lord would (a WorkQueue stream over the
// target's cast subject); tweak adjusts the config for tests that need a stream to refuse writes.
func provisionMailbox(t *testing.T, js nats.JetStreamContext, app, name string, tweak func(*nats.StreamConfig)) {
	t.Helper()
	cfg := &nats.StreamConfig{
		Name:      wire.Stream(app, name),
		Subjects:  []string{wire.Cast(app, name)},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.MemoryStorage,
	}
	if tweak != nil {
		tweak(cfg)
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
}

func storedCount(t *testing.T, js nats.JetStreamContext, app, name string) uint64 {
	t.Helper()
	info, err := js.StreamInfo(wire.Stream(app, name))
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	return info.State.Msgs
}

func storedEnvelope(t *testing.T, js nats.JetStreamContext, app, name string, seq uint64) wire.Envelope {
	t.Helper()
	msg, err := js.GetMsg(wire.Stream(app, name), seq)
	if err != nil {
		t.Fatalf("GetMsg: %v", err)
	}
	var e wire.Envelope
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		t.Fatalf("decode stored envelope: %v", err)
	}
	return e
}

// TestCastConfirmedStoresInMailbox proves a nil error means the cast is in the target's mailbox,
// carrying the same envelope a plain cast would.
func TestCastConfirmedStoresInMailbox(t *testing.T) {
	_, nc, js := confirmEnv(t)
	provisionMailbox(t, js, "cf", "sink", nil)

	if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", map[string]float64{"temp": 71.4}, confirmTimeout, ""); err != nil {
		t.Fatalf("CastConfirmed: %v", err)
	}

	if n := storedCount(t, js, "cf", "sink"); n != 1 {
		t.Fatalf("stored messages = %d, want 1", n)
	}
	e := storedEnvelope(t, js, "cf", "sink", 1)
	if e.Kind != wire.KindCast || e.To != "sink" || e.Op != "sample" || e.Trace != "t-1" {
		t.Fatalf("stored envelope = %+v, want a cast to sink/sample with trace t-1", e)
	}
	var payload map[string]float64
	if err := json.Unmarshal(e.Payload, &payload); err != nil || payload["temp"] != 71.4 {
		t.Fatalf("stored payload = %s (err %v), want temp=71.4", e.Payload, err)
	}
}

// TestCastConfirmedRejectsNonDurableTarget proves a target without a mailbox fails with
// ErrNotDurable before anything is sent, so its core subscription never processes the message.
func TestCastConfirmedRejectsNonDurableTarget(t *testing.T) {
	_, nc, _ := confirmEnv(t)
	sub, err := nc.SubscribeSync(wire.Cast("cf", "plain"))
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	err = doCastConfirmed(nc, "cf", "t-1", "plain", "sample", nil, confirmTimeout, "")
	if !errors.Is(err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", err)
	}

	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if msg, err := sub.NextMsg(200 * time.Millisecond); err == nil {
		t.Fatalf("non-durable target received %q, want nothing sent", msg.Data)
	}
}

// TestCastConfirmedDeduplicatesRetryByIdempotencyKey proves a retry carrying the same key lands
// once in the mailbox (within its duplicate window), while a different key is a new message.
func TestCastConfirmedDeduplicatesRetryByIdempotencyKey(t *testing.T) {
	_, nc, js := confirmEnv(t)
	provisionMailbox(t, js, "cf", "sink", nil)

	for _, key := range []string{"sample-1", "sample-1", "sample-2"} {
		if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", nil, confirmTimeout, key); err != nil {
			t.Fatalf("CastConfirmed(%s): %v", key, err)
		}
	}

	if n := storedCount(t, js, "cf", "sink"); n != 2 {
		t.Fatalf("stored messages = %d, want 2 (the retry of sample-1 deduplicated)", n)
	}
}

// TestCastConfirmedFailsWhenStreamRefuses proves a write the mailbox does not store is reported
// as an error, never as success.
func TestCastConfirmedFailsWhenStreamRefuses(t *testing.T) {
	_, nc, js := confirmEnv(t)
	provisionMailbox(t, js, "cf", "sink", func(cfg *nats.StreamConfig) {
		cfg.MaxMsgs = 1
		cfg.Discard = nats.DiscardNew
	})

	if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", nil, confirmTimeout, ""); err != nil {
		t.Fatalf("first CastConfirmed: %v", err)
	}
	if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", nil, confirmTimeout, ""); err == nil {
		t.Fatal("second CastConfirmed succeeded on a full mailbox, want an error")
	}
	if n := storedCount(t, js, "cf", "sink"); n != 1 {
		t.Fatalf("stored messages = %d, want 1", n)
	}
}

// TestCastConfirmedFailsWhenBusIsDown proves that once the bus is gone the confirmed cast reports
// an error, even for a target whose mailbox is already cached as known.
func TestCastConfirmedFailsWhenBusIsDown(t *testing.T) {
	eth, nc, js := confirmEnv(t)
	provisionMailbox(t, js, "cf", "sink", nil)
	if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", nil, confirmTimeout, ""); err != nil {
		t.Fatalf("CastConfirmed before outage: %v", err)
	}

	eth.Stop()

	if err := doCastConfirmed(nc, "cf", "t-1", "sink", "sample", nil, confirmTimeout, ""); err == nil {
		t.Fatal("CastConfirmed succeeded with the bus down, want an error")
	}
}

// TestCtxCastConfirmedPropagatesTrace proves the handler-side variant carries the trace of the
// message being handled, like Ctx.Cast.
func TestCtxCastConfirmedPropagatesTrace(t *testing.T) {
	_, nc, js := confirmEnv(t)
	provisionMailbox(t, js, "cf", "sink", nil)
	ctx := &Ctx{NATS: nc, App: "cf", Name: "driver", Trace: "t-handler"}

	if err := ctx.CastConfirmed("sink", "sample", nil, confirmTimeout, WithIdempotencyKey("k-1")); err != nil {
		t.Fatalf("Ctx.CastConfirmed: %v", err)
	}

	e := storedEnvelope(t, js, "cf", "sink", 1)
	if e.Trace != "t-handler" || e.Idem != "k-1" {
		t.Fatalf("stored envelope trace=%q idem=%q, want t-handler / k-1", e.Trace, e.Idem)
	}
}
