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

// TestDurableCastStopsInsideABatch: a drain must not make a durable thrall work through the batch
// it has already fetched. With a backlog that is up to 128 handlers, which outlasts the lord's
// grace period, so the process would be killed inside a handler and never terminate properly. The
// consumer stops after the cast it is handling; the rest of the batch stays unacknowledged and is
// redelivered to the next process.
func TestDurableCastStopsInsideABatch(t *testing.T) {
	const (
		app     = "dur"
		name    = "draining"
		backlog = 40 // well inside one batch
	)
	nc, js := durableCastEnv(t, app, name)
	for i := 0; i < backlog; i++ {
		if _, err := js.Publish(wire.Cast(app, name), []byte(fmt.Sprintf(`{"payload":{"n":%d}}`, i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	stop := make(chan struct{})
	handled := 0 // touched only by the consumer goroutine until it has returned
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumeDurableCast(nc, app, name, discardLogger(), stop, func([]byte, func()) {
			handled++
			if handled == 1 {
				close(stop) // the drain arrives while the first cast of the batch is being handled
			}
		})
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeDurableCast did not return after stop")
	}
	if handled != 1 {
		t.Fatalf("the consumer handled %d casts after the drain began, want it to stop after the one in hand", handled)
	}

	// The cast in hand was acknowledged; the rest of the fetched batch was not, so it comes back.
	var pending int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ci, err := js.ConsumerInfo(wire.Stream(app, name), name)
		if err != nil {
			t.Fatalf("ConsumerInfo: %v", err)
		}
		if pending = ci.NumAckPending + int(ci.NumPending); pending == backlog-1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("casts still owed after the drain = %d, want %d (all but the one handled)", pending, backlog-1)
}

// TestDurableCastIdleStopIsImmediate: an idle durable consumer returns as soon as it is told to
// stop, instead of waiting out the fetch it is blocked in.
func TestDurableCastIdleStopIsImmediate(t *testing.T) {
	nc, _ := durableCastEnv(t, "dur", "idle")
	stop := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumeDurableCast(nc, "dur", "idle", discardLogger(), stop, func([]byte, func()) {})
	}()
	time.Sleep(150 * time.Millisecond) // the consumer exists and is inside a fetch

	began := time.Now()
	close(stop)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeDurableCast did not return after stop")
	}
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Errorf("an idle durable consumer took %v to stop, want it to stop at once", took)
	}
}
