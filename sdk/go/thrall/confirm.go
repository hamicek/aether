package thrall

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// A plain cast is a core NATS publish: the sender learns nothing about whether the durable
// mailbox stored it, so a message sent while the bus is unreachable (or once the client's
// reconnect buffer overflows) is lost without the sender knowing. A confirmed cast publishes
// through JetStream instead and returns only after the target's mailbox stream acknowledged the
// write, so the durability guarantee starts at the sender rather than at the stream.

// ErrNotDurable is returned by a confirmed cast whose target has no durable mailbox. Nothing is
// sent: a JetStream publish to such a target would reach its core subscription, be processed and
// never be acknowledged, so the sender would see a timeout, retry and get it processed twice.
var ErrNotDurable = errors.New("target has no durable mailbox")

// mailboxKey identifies a mailbox as seen through one connection: whether a stream exists is a
// property of the cluster a connection talks to, so the same name on another connection is a
// different fact.
type mailboxKey struct {
	nc     *nats.Conn
	stream string
}

// knownMailboxes caches mailboxes confirmed to exist, so a sender pays the stream lookup once per
// target rather than on every message. Only a positive answer is cached: a target provisioned as
// durable later must work immediately. A mailbox deleted after it was cached makes the publish
// fail (no stream acknowledges it), never succeed silently. Entries are never evicted, so a closed
// connection stays referenced; that is bounded by connections x targets, and a thrall holds one
// connection for its lifetime, so it is not worth a weak-reference scheme.
var knownMailboxes sync.Map // mailboxKey -> struct{}

func doCastConfirmed(nc *nats.Conn, app, trace, target, op string, payload any, timeout time.Duration, idem string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("confirmed cast to %q: %w", target, err)
	}
	stream := wire.Stream(app, target)
	if err := ensureMailbox(ctx, js, nc, stream); err != nil {
		return fmt.Errorf("confirmed cast to %q: %w", target, err)
	}

	pubOpts := []nats.PubOpt{nats.Context(ctx), nats.ExpectStream(stream)}
	if idem != "" {
		pubOpts = append(pubOpts, nats.MsgId(idem))
	}
	e := newCastEnvelope(trace, target, op, payload, idem)
	if _, err := js.Publish(wire.Cast(app, target), mustJSON(e), pubOpts...); err != nil {
		return fmt.Errorf("confirmed cast to %q: %w", target, err)
	}
	return nil
}

// ensureMailbox verifies the target's mailbox stream exists, consulting the cache first.
func ensureMailbox(ctx context.Context, js nats.JetStreamContext, nc *nats.Conn, stream string) error {
	key := mailboxKey{nc: nc, stream: stream}
	if _, ok := knownMailboxes.Load(key); ok {
		return nil
	}
	if _, err := js.StreamInfo(stream, nats.Context(ctx)); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrNotDurable
		}
		return fmt.Errorf("look up mailbox: %w", err)
	}
	knownMailboxes.Store(key, struct{}{})
	return nil
}
