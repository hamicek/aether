package lord

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// castTraced publishes a cast carrying an explicit trace (empty = none, exercising the edge).
func castTraced(t *testing.T, nc *nats.Conn, app, name, op, trace string) {
	t.Helper()
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: trace, Kind: wire.KindCast, To: name, Op: op,
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(e)
	if err := nc.Publish(wire.Cast(app, name), data); err != nil {
		t.Fatalf("cast %s.%s: %v", name, op, err)
	}
	_ = nc.Flush()
}

// awaitTrace subscribes to a thrall's emit_trace subject and returns the next observed trace.
func awaitTrace(t *testing.T, nc *nats.Conn, name string) <-chan string {
	t.Helper()
	ch := make(chan string, 1)
	sub, err := nc.Subscribe("test.trace."+name, func(m *nats.Msg) {
		select {
		case ch <- string(m.Data):
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return ch
}

func recvTrace(t *testing.T, ch <-chan string, desc string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", desc)
		return ""
	}
}

// TestTraceReachesHandler proves the trace on an incoming message is exposed to the handler
// via ctx (the SDK sets ctx.Trace before dispatch).
func TestTraceReachesHandler(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "tr", "one_for_one", spec("sink", "permanent", "local"))
	startLord(t, eth, m)
	waitReady(t, eth, "sink")

	got := awaitTrace(t, eth.Conn(), "sink")
	castTraced(t, eth.Conn(), "tr", "sink", "emit_trace", "trace-123")
	if v := recvTrace(t, got, "handler trace"); v != "trace-123" {
		t.Errorf("handler saw trace %q, want %q", v, "trace-123")
	}
}

// TestTraceEdgeMintsWhenAbsent proves a message that arrives without a trace makes the thrall
// the edge: it mints a non-empty trace rather than passing an empty one on.
func TestTraceEdgeMintsWhenAbsent(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "tr", "one_for_one", spec("sink", "permanent", "local"))
	startLord(t, eth, m)
	waitReady(t, eth, "sink")

	got := awaitTrace(t, eth.Conn(), "sink")
	castTraced(t, eth.Conn(), "tr", "sink", "emit_trace", "") // no trace
	if v := recvTrace(t, got, "minted trace"); v == "" {
		t.Error("edge did not mint a trace for a message without one")
	}
}

// TestTracePropagatesAcrossHop proves ctx.Cast from inside a handler carries the current trace
// to the downstream thrall: relay(trace=T) -> sink must observe T.
func TestTracePropagatesAcrossHop(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "tr", "one_for_one",
		spec("relay", "permanent", "local"),
		spec("sink", "permanent", "local"))
	startLord(t, eth, m)
	waitReady(t, eth, "relay")
	waitReady(t, eth, "sink")

	got := awaitTrace(t, eth.Conn(), "sink")
	castTraced(t, eth.Conn(), "tr", "relay", "relay", "trace-hop")
	if v := recvTrace(t, got, "propagated trace"); v != "trace-hop" {
		t.Errorf("sink saw trace %q, want %q (not propagated across the hop)", v, "trace-hop")
	}
}
