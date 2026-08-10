package thrall

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

// embeddedCtx starts an embedded NATS with JetStream, provisions the retention event log stream
// (as the lord would), and returns a Ctx wired to it.
func embeddedCtx(t *testing.T, app, name string) *Ctx {
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
		Name:      wire.EventLogStream(app, name),
		Subjects:  []string{wire.EventLog(app, name)},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	return &Ctx{NATS: nc, App: app, Name: name}
}

func TestRebuildEmptyLogReturnsInitial(t *testing.T) {
	ctx := embeddedCtx(t, "es", "empty")
	got, err := Rebuild(ctx, 7, func(json.RawMessage, int) (int, error) { return 0, nil })
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got != 7 {
		t.Errorf("empty log rebuild = %d, want 7 (initial)", got)
	}
}

func TestAppendAndRebuild(t *testing.T) {
	ctx := embeddedCtx(t, "es", "acct")
	for _, delta := range []int{10, 5, 3} {
		if err := ctx.Append(map[string]int{"delta": delta}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	fold := func(payload json.RawMessage, balance int) (int, error) {
		var e struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(payload, &e); err != nil {
			return balance, err
		}
		return balance + e.Delta, nil
	}
	got, err := Rebuild(ctx, 0, fold)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if got != 18 {
		t.Errorf("rebuilt balance = %d, want 18", got)
	}
}

// TestRebuildBoundedLog checks the purged-log edge case: a stream that keeps only the last few
// messages (retention MaxMsgs) still rebuilds from the retained ones, reaching LastSeq, and does
// not hang on the fetch wait.
func TestRebuildBoundedLog(t *testing.T) {
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
	const app, name = "es", "bounded"
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      wire.EventLogStream(app, name),
		Subjects:  []string{wire.EventLog(app, name)},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
		MaxMsgs:   2, // keep only the last 2 events
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	ctx := &Ctx{NATS: nc, App: app, Name: name}
	for _, n := range []int{1, 2, 3, 4, 5} {
		if err := ctx.Append(map[string]int{"n": n}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	fold := func(payload json.RawMessage, acc []int) ([]int, error) {
		var e struct {
			N int `json:"n"`
		}
		_ = json.Unmarshal(payload, &e)
		return append(acc, e.N), nil
	}
	start := time.Now()
	got, err := Rebuild(ctx, []int{}, fold)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if want := []int{4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("bounded rebuild = %v, want %v (only the retained tail)", got, want)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("rebuild of a purged log took %s - it should not hang on the fetch wait", elapsed)
	}
}

func TestRebuildPreservesOrder(t *testing.T) {
	ctx := embeddedCtx(t, "es", "seq")
	for i := 0; i < 5; i++ {
		if err := ctx.Append(map[string]int{"n": i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	fold := func(payload json.RawMessage, acc []int) ([]int, error) {
		var e struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(payload, &e); err != nil {
			return acc, err
		}
		return append(acc, e.N), nil
	}
	got, err := Rebuild(ctx, []int{}, fold)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if want := []int{0, 1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("replay order = %v, want %v", got, want)
	}
}
