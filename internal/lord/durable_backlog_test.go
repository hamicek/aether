package lord

import (
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// TestDurableBacklogReported proves the lord samples a durable consumer's pending count and
// exposes it as aether_durable_backlog. The scenario is built directly on JetStream (stream +
// durable consumer + unacked messages) so the backlog is deterministic, independent of a live
// thrall's drain timing.
func TestDurableBacklogReported(t *testing.T) {
	eth := startEmbedded(t)
	const app = "dbl"

	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "q", Cmd: "true", Restart: "permanent", Scope: "local", Durable: true}},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}

	// Provision the durable stream (as Start would) and the consumer (as the thrall's
	// PullSubscribe would), then enqueue messages nobody has fetched yet.
	if err := l.provisionStream(l.children[0]); err != nil {
		t.Fatalf("provisionStream: %v", err)
	}
	js, err := eth.Conn().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	stream := wire.Stream(app, "q")
	castSubj := wire.Cast(app, "q")
	if _, err := js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       "q",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: castSubj,
		DeliverPolicy: nats.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("AddConsumer: %v", err)
	}
	const pending = 4
	for i := 0; i < pending; i++ {
		if _, err := js.Publish(castSubj, []byte(`{"v":1,"kind":"cast","op":"inc"}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	l.pollDurableBacklogOnce()

	v, ok := metricValue(t, scrape(t, l), `aether_durable_backlog{name="q"}`)
	if !ok {
		t.Fatalf("durable backlog not reported")
	}
	if v != pending {
		t.Errorf("durable backlog = %v, want %d", v, pending)
	}
}
