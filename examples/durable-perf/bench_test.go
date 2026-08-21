//go:build durableperf

// Durable cast throughput harness. See doc.go for what it measures and why.
package durableperf

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

const (
	perfApp  = "durperf"
	perfName = "counter"
)

// counterDef is a durable counter: `inc` casts drive throughput, `get` reads the drained count.
func counterDef() thrall.Def[int] {
	return thrall.Def[int]{
		Name: perfName,
		Init: func(_ *thrall.Ctx) (int, error) { return 0, nil },
		HandleCall: map[string]thrall.CallFn[int]{
			"get": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return s, s, nil },
		},
		HandleCast: map[string]thrall.CastFn[int]{
			"inc": func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { return s + 1, nil },
		},
	}
}

func castCount() int {
	if v := os.Getenv("AETHER_PERF_CASTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 20000
}

// TestDurableThroughput preloads a durable stream with N casts, then attaches the thrall and
// times how long it takes to drain them all. The reported casts/s is the durable consumer's
// drain rate; compare a tuned (batched) build against a durableBatchSize=1 build to see the
// AE-065 gain.
func TestDurableThroughput(t *testing.T) {
	n := castCount()

	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether start: %v", err)
	}
	defer eth.Stop()

	nc, err := nats.Connect(eth.URL())
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// Provision the cast stream (as the lord would) and preload N casts before any consumer
	// attaches, so the timed window below is pure drain, not publish.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     wire.Stream(perfApp, perfName),
		Subjects: []string{wire.Cast(perfApp, perfName)},
		Storage:  nats.FileStorage,
	}); err != nil {
		t.Fatalf("add stream: %v", err)
	}
	castSubj := wire.Cast(perfApp, perfName)
	payload := []byte(`{"v":1,"kind":"cast","op":"inc","payload":{}}`)
	for i := 0; i < n; i++ {
		if _, err := js.PublishAsync(castSubj, payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(60 * time.Second):
		t.Fatal("preload publish did not complete")
	}
	t.Logf("preloaded %d casts", n)

	// Attach the durable thrall as a spawned process would: wiring from the environment.
	os.Setenv("AETHER_NATS_URL", eth.URL())
	os.Setenv("AETHER_APP", perfApp)
	os.Setenv("AETHER_NAME", perfName)
	os.Setenv("AETHER_DURABLE", "1")

	start := time.Now()
	// thrall.Start blocks for the life of the process; not joined (ends when the test binary exits).
	go func() { _ = thrall.Start(counterDef()) }()

	// Poll `get` until the drained count reaches N, then compute the drain rate.
	deadline := time.Now().Add(5 * time.Minute)
	var drained int
	for drained < n {
		if time.Now().After(deadline) {
			t.Fatalf("drain stalled at %d/%d casts", drained, n)
		}
		drained = getCount(t, nc)
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)

	rate := float64(n) / elapsed.Seconds()
	t.Logf("drained %d casts in %s = %.0f casts/s (batch build)", n, elapsed.Round(time.Millisecond), rate)
}

// getCount issues a `get` call and returns the counter, or 0 until the thrall is subscribed.
func getCount(t *testing.T, nc *nats.Conn) int {
	t.Helper()
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: perfName, Op: "get", Payload: json.RawMessage("{}")}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(wire.Call(perfApp, perfName), data, 500*time.Millisecond)
	if err != nil {
		return 0 // not yet subscribed, or a transient timeout under load
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return 0
	}
	var count int
	if err := json.Unmarshal(reply.Payload, &count); err != nil {
		t.Fatalf("decode get reply %q: %v", string(reply.Payload), err)
	}
	return count
}
