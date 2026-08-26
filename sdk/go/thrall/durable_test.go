package thrall

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// durableCastEnv provisions the durable cast stream the lord would create (stream over the
// cast subject) on an embedded NATS with JetStream, and returns the connection plus a
// JetStream context for publishing numbered casts.
func durableCastEnv(t *testing.T, app, name string) (*nats.Conn, nats.JetStreamContext) {
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
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      wire.Stream(app, name),
		Subjects:  []string{wire.Cast(app, name)},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	return nc, js
}

// TestDurableCastPreservesFIFO enqueues more casts than a single batch holds and proves the
// batched consumer drains all of them, in arrival order, across batch boundaries.
func TestDurableCastPreservesFIFO(t *testing.T) {
	const (
		app   = "dur"
		name  = "q"
		total = 500 // > durableBatchSize, so FIFO is exercised across multiple batches
	)
	nc, js := durableCastEnv(t, app, name)

	for i := 0; i < total; i++ {
		payload := []byte(fmt.Sprintf(`{"v":1,"kind":"cast","op":"inc","payload":{"n":%d}}`, i))
		if _, err := js.Publish(wire.Cast(app, name), payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var (
		mu   sync.Mutex
		got  []int
		done = make(chan struct{})
	)
	processCast := func(data []byte, _ func()) {
		var e struct {
			Payload struct {
				N int `json:"n"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(data, &e); err != nil {
			t.Errorf("unmarshal cast: %v", err)
			return
		}
		mu.Lock()
		got = append(got, e.Payload.N)
		reached := len(got) == total
		mu.Unlock()
		if reached {
			close(done)
		}
	}

	stop := make(chan struct{})
	go consumeDurableCast(nc, app, name, discardLogger(), stop, processCast)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		mu.Lock()
		n := len(got)
		mu.Unlock()
		t.Fatalf("timed out: drained %d/%d casts", n, total)
	}
	close(stop)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != total {
		t.Fatalf("drained %d casts, want %d (no-loss violated)", len(got), total)
	}
	for i, n := range got {
		if n != i {
			t.Fatalf("cast at position %d = %d, want %d (FIFO violated)", i, n, i)
		}
	}
}

// TestDurableCastAttachesToPreexistingConsumer proves upgrade safety: a durable consumer left
// by an earlier SDK version (created without the ack tuning) has a different stored ack config,
// which nats.go refuses to reconcile. The consumer must still attach and drain rather than erroring
// out and going silent.
func TestDurableCastAttachesToPreexistingConsumer(t *testing.T) {
	const (
		app   = "dur"
		name  = "legacy"
		total = 50
	)
	nc, js := durableCastEnv(t, app, name)

	// Create the consumer the pre-AE-065 way: no AckWait/MaxAckPending, so it takes the server
	// defaults that differ from the tuned values consumeDurableCast now requests.
	if _, err := js.AddConsumer(wire.Stream(app, name), &nats.ConsumerConfig{
		Durable:       name,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: wire.Cast(app, name),
		DeliverPolicy: nats.DeliverAllPolicy,
	}); err != nil {
		t.Fatalf("AddConsumer (legacy): %v", err)
	}
	for i := 0; i < total; i++ {
		if _, err := js.Publish(wire.Cast(app, name), []byte(fmt.Sprintf(`{"payload":{"n":%d}}`, i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var (
		mu   sync.Mutex
		got  int
		done = make(chan struct{})
	)
	processCast := func([]byte, func()) {
		mu.Lock()
		got++
		reached := got == total
		mu.Unlock()
		if reached {
			close(done)
		}
	}

	stop := make(chan struct{})
	go consumeDurableCast(nc, app, name, discardLogger(), stop, processCast)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		mu.Lock()
		n := got
		mu.Unlock()
		t.Fatalf("attach-and-drain stalled at %d/%d - consumeDurableCast likely errored on the pre-existing consumer", n, total)
	}
	close(stop)
}

// TestDurableCastStopsOnEmptyStream proves the consumer neither blocks nor processes anything
// on an empty stream, and returns promptly once stop is closed.
func TestDurableCastStopsOnEmptyStream(t *testing.T) {
	const app, name = "dur", "idle"
	nc, _ := durableCastEnv(t, app, name)

	stop := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		consumeDurableCast(nc, app, name, discardLogger(), stop, func([]byte, func()) {
			t.Error("no cast should be processed on an empty stream")
		})
		close(returned)
	}()

	// Let the loop spin through at least one empty fetch before we ask it to stop.
	time.Sleep(50 * time.Millisecond)
	close(stop)

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("consumeDurableCast did not return after stop")
	}
}
