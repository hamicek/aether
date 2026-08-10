package thrall

// Event-sourced rebuild: a thrall Appends domain events to its retention event log and, in
// init, Rebuilds its state by replaying that log through a fold function ("log is truth, state
// is a projection"). The event log is a separate retention stream (provisioned opt-in by the
// lord), independent of the WorkQueue durable mailbox. At-least-once + replay -> the fold must
// be idempotent.

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// Append persists a domain event to this thrall's event log (a JetStream publish that waits for
// the stream's ack, so the event is durable). Requires the thrall to have opted into an event
// log (manifest event_log = true), so the lord has provisioned the retention stream.
func (c *Ctx) Append(event any) error {
	js, err := c.NATS.JetStream()
	if err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := js.Publish(wire.EventLog(c.App, c.Name), data); err != nil {
		return fmt.Errorf("append to event log: %w", err)
	}
	return nil
}

// Rebuild reconstructs a thrall's state by replaying its event log in order from the beginning.
// Call it from init: it reads every persisted event (DeliverAll) into fold, starting from
// `initial`, and returns the reconstructed state before normal operation begins. An empty log
// yields `initial`. The fold receives each raw event payload; it must be idempotent (the log is
// at-least-once and may be replayed). Ordering is stream order (single-writer = append order).
func Rebuild[S any](ctx *Ctx, initial S, fold func(payload json.RawMessage, state S) (S, error)) (S, error) {
	state := initial
	js, err := ctx.NATS.JetStream()
	if err != nil {
		return state, err
	}
	stream := wire.EventLogStream(ctx.App, ctx.Name)
	si, err := js.StreamInfo(stream)
	if err != nil {
		return state, fmt.Errorf("event log stream %q (is event_log enabled?): %w", stream, err)
	}
	last := si.State.LastSeq
	if last == 0 || si.State.Msgs == 0 {
		return state, nil // nothing to replay: empty log, or retention purged every message
	}

	// Ephemeral consumer over the whole log; we read up to the last sequence captured above.
	sub, err := js.PullSubscribe(wire.EventLog(ctx.App, ctx.Name), "",
		nats.BindStream(stream), nats.DeliverAll())
	if err != nil {
		return state, fmt.Errorf("replay consumer: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	var seq uint64
	for seq < last {
		msgs, err := sub.Fetch(256, nats.MaxWait(5*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				break // no more messages available (should not happen before `last`)
			}
			return state, fmt.Errorf("replay fetch: %w", err)
		}
		for _, m := range msgs {
			state, err = fold(m.Data, state)
			if err != nil {
				return state, fmt.Errorf("fold event: %w", err)
			}
			if meta, err := m.Metadata(); err == nil {
				seq = meta.Sequence.Stream
			}
			_ = m.Ack()
		}
	}
	return state, nil
}
