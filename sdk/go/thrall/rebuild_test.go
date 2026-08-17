package thrall

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
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

// TestAppendDedupKey proves DedupKey deduplicates within the stream's duplicate window: two
// Appends with the same key land as one message, a different key as another.
func TestAppendDedupKey(t *testing.T) {
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
	const app, name = "es", "dedup"
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:       wire.EventLogStream(app, name),
		Subjects:   []string{wire.EventLog(app, name)},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.MemoryStorage,
		Duplicates: time.Minute, // explicit dedup window, as the lord provisions
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	ctx := &Ctx{NATS: nc, App: app, Name: name}

	// Same key twice -> one record.
	for i := 0; i < 2; i++ {
		if err := ctx.Append(map[string]int{"delta": 10}, DedupKey("cmd-1")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	// A different key -> a second record.
	if err := ctx.Append(map[string]int{"delta": 20}, DedupKey("cmd-2")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	si, err := js.StreamInfo(wire.EventLogStream(app, name))
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if si.State.Msgs != 2 {
		t.Errorf("stream msgs = %d, want 2 (the duplicate cmd-1 append deduplicated)", si.State.Msgs)
	}
}

// TestCommandKeyDedupEndToEnd drives the command-key pattern through a live thrall: a cast
// handler appends with DedupKey(ctx.MsgID), so redelivery of the SAME message (same envelope id)
// writes one event, while a distinct message writes another. It also proves ctx.MsgID reaches
// the handler. Casts are republished until one is appended (readiness) - since the dedup key is
// the constant envelope id, extra deliveries collapse to a single record.
func TestCommandKeyDedupEndToEnd(t *testing.T) {
	nc, srv := embeddedNATS(t)
	const app, name = "test", "acct"
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:       wire.EventLogStream(app, name),
		Subjects:   []string{wire.EventLog(app, name)},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.MemoryStorage,
		Duplicates: time.Minute,
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}

	var mu sync.Mutex
	var seenMsgID string
	def := Def[int]{
		Name: name,
		Init: func(*Ctx) (int, error) { return 0, nil },
		HandleCast: map[string]CastFn[int]{
			"move": func(payload json.RawMessage, state int, ctx *Ctx) (int, error) {
				mu.Lock()
				seenMsgID = ctx.MsgID
				mu.Unlock()
				// Command-key: the message id makes a redelivered command idempotent.
				if err := ctx.Append(payload, DedupKey(ctx.MsgID)); err != nil {
					return state, err
				}
				return state, nil
			},
		},
	}
	t.Setenv("AETHER_NATS_URL", srv.ClientURL())
	t.Setenv("AETHER_APP", app)
	go func() { _ = Start(def) }()

	streamMsgs := func() uint64 {
		si, err := js.StreamInfo(wire.EventLogStream(app, name))
		if err != nil {
			t.Fatalf("StreamInfo: %v", err)
		}
		return si.State.Msgs
	}
	// castUntil republishes the same envelope until the stream reaches wantMsgs (or times out),
	// tolerating the thrall not yet being subscribed and, thanks to the constant dedup key,
	// collapsing every delivery of this id to a single record.
	castUntil := func(id string, delta, wantMsgs int) {
		t.Helper()
		env := wire.Envelope{V: 1, ID: id, Kind: wire.KindCast, Op: "move",
			Payload: json.RawMessage(fmt.Sprintf(`{"delta":%d}`, delta))}
		data, _ := json.Marshal(env)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if err := nc.Publish(wire.Cast(app, name), data); err != nil {
				t.Fatalf("publish cast: %v", err)
			}
			if streamMsgs() >= uint64(wantMsgs) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("stream never reached %d msgs (got %d) for id %q", wantMsgs, streamMsgs(), id)
	}

	castUntil("cmd-1", 10, 1) // readiness + first record; duplicates of cmd-1 collapse to one
	castUntil("cmd-2", 20, 2) // a distinct command adds a second record

	if got := streamMsgs(); got != 2 {
		t.Errorf("event log msgs = %d, want 2 (cmd-1 deduplicated, cmd-2 distinct)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if seenMsgID == "" {
		t.Error("ctx.MsgID was not exposed to the handler")
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
