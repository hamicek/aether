package thrall_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// fakeLord subscribes to the lord control channel and replies as configured, so the SDK
// client side (StartChild/StopChild) can be tested without the real lord.
func fakeLord(t *testing.T, nc *nats.Conn, reply func(e wire.Envelope) wire.Envelope) {
	t.Helper()
	sub, err := nc.Subscribe(wire.LordCtl(), func(m *nats.Msg) {
		var e wire.Envelope
		_ = json.Unmarshal(m.Data, &e)
		r := reply(e)
		data, _ := json.Marshal(r)
		_ = m.Respond(data)
	})
	if err != nil {
		t.Fatalf("fake lord subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func testConn(t *testing.T) *nats.Conn {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether start: %v", err)
	}
	t.Cleanup(eth.Stop)
	return eth.Conn()
}

// TestStartChildSuccess: StartChild sends a spawn request and returns the name from the
// lord's ok reply.
func TestStartChildSuccess(t *testing.T) {
	nc := testConn(t)
	var gotOp string
	var gotSpec wire.SpawnSpec
	fakeLord(t, nc, func(e wire.Envelope) wire.Envelope {
		gotOp = e.Op
		_ = json.Unmarshal(e.Payload, &gotSpec)
		return wire.Envelope{V: 1, ID: e.ID, Kind: wire.KindReply, Status: "ok",
			Payload: mustMarshalReply(wire.SpawnReply{Name: gotSpec.Name})}
	})

	ctx := &thrall.Ctx{NATS: nc, App: "demo"}
	name, err := ctx.StartChild(wire.SpawnSpec{Name: "worker-1", Cmd: "./w", Restart: "transient"}, time.Second)
	if err != nil {
		t.Fatalf("StartChild: %v", err)
	}
	if name != "worker-1" {
		t.Fatalf("name = %q, want worker-1", name)
	}
	if gotOp != wire.OpSpawn {
		t.Fatalf("op = %q, want %q", gotOp, wire.OpSpawn)
	}
	if gotSpec.Cmd != "./w" || gotSpec.Restart != "transient" {
		t.Fatalf("spec not carried through: %+v", gotSpec)
	}
}

// TestStartChildError: the lord's error reply surfaces as a Go error.
func TestStartChildError(t *testing.T) {
	nc := testConn(t)
	fakeLord(t, nc, func(e wire.Envelope) wire.Envelope {
		return wire.Envelope{V: 1, ID: e.ID, Kind: wire.KindReply, Status: "error",
			Error: &wire.WireError{Type: "spawn_failed", Message: "name already exists"}}
	})

	ctx := &thrall.Ctx{NATS: nc, App: "demo"}
	_, err := ctx.StartChild(wire.SpawnSpec{Name: "dup", Cmd: "./w"}, time.Second)
	if err == nil {
		t.Fatal("expected an error from the lord's refusal")
	}
}

// TestStopChild: StopChild sends a stop request and succeeds on an ok reply.
func TestStopChild(t *testing.T) {
	nc := testConn(t)
	var gotOp, gotName string
	fakeLord(t, nc, func(e wire.Envelope) wire.Envelope {
		gotOp = e.Op
		var s wire.StopSpec
		_ = json.Unmarshal(e.Payload, &s)
		gotName = s.Name
		return wire.Envelope{V: 1, ID: e.ID, Kind: wire.KindReply, Status: "ok"}
	})

	ctx := &thrall.Ctx{NATS: nc, App: "demo"}
	if err := ctx.StopChild("worker-1", time.Second); err != nil {
		t.Fatalf("StopChild: %v", err)
	}
	if gotOp != wire.OpStop || gotName != "worker-1" {
		t.Fatalf("stop request wrong: op=%q name=%q", gotOp, gotName)
	}
}

// TestStartChildTimeout: with no lord answering, StartChild returns a timeout error.
func TestStartChildTimeout(t *testing.T) {
	nc := testConn(t)
	ctx := &thrall.Ctx{NATS: nc, App: "demo"}
	if _, err := ctx.StartChild(wire.SpawnSpec{Name: "x", Cmd: "./x"}, 200*time.Millisecond); err == nil {
		t.Fatal("expected a timeout with no lord present")
	}
}

func mustMarshalReply(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
