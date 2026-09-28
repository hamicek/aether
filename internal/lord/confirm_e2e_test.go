package lord

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// End-to-end tests of the confirmed cast (AE-083) against the real lord and a real probe thrall
// process: the mailbox is the one the lord provisions, and the message is consumed by the actual
// SDK drain, not read back from the stream by the test.

const confirmTimeout = 5 * time.Second

// A confirmed cast to a lord-provisioned durable thrall is processed by it, and a retry carrying
// the same idempotency key is stored once, so the (non-idempotent) thrall handles it once.
func TestConfirmedCastDeliveredToDurableThrall(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "probe", Cmd: probeCmd(t), Restart: "permanent", Scope: "local", Durable: true}},
	})
	nc := eth.Conn()
	waitReady(t, eth, "probe")
	sender := &thrall.Ctx{NATS: nc, App: app, Name: "sender"}

	for _, key := range []string{"e-1", "e-1", "e-2"} {
		if err := sender.CastConfirmed("probe", "inc", nil, confirmTimeout, thrall.WithIdempotencyKey(key)); err != nil {
			t.Fatalf("CastConfirmed(%s): %v", key, err)
		}
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	waitFor(t, 10*time.Second, "both stored casts delivered and acked", func() bool {
		ci, err := js.ConsumerInfo(wire.Stream(app, "probe"), "probe")
		return err == nil && ci.NumPending == 0 && ci.NumAckPending == 0 && ci.Delivered.Consumer == 2
	})
	if v := callInt(t, nc, app, "probe", "get"); v != 2 {
		t.Fatalf("state = %d, want 2 (the retry of e-1 stored and processed once)", v)
	}
}

// A confirmed cast to a thrall the lord runs without a mailbox fails with ErrNotDurable, and the
// thrall never processes it.
func TestConfirmedCastRefusesNonDurableThrall(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, app, "one_for_one", spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")
	sender := &thrall.Ctx{NATS: nc, App: app, Name: "sender"}

	err := sender.CastConfirmed("probe", "inc", nil, confirmTimeout)
	if !errors.Is(err, thrall.ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", err)
	}

	// A call is ordered after any cast the thrall received on its data subscription, so a zero
	// here proves the refused cast never reached it.
	if v := callInt(t, nc, app, "probe", "get"); v != 0 {
		t.Fatalf("state = %d, want 0 (nothing sent to a non-durable thrall)", v)
	}
}

// The point of the confirmed cast: the sender gets its confirmation while the durable thrall is
// down, because the mailbox, not the thrall, stores the message.
func TestConfirmedCastStoredWhileThrallIsDown(t *testing.T) {
	const app = "itest"
	eth := startEmbedded(t)
	startLord(t, eth, &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "probe", Cmd: probeCmd(t), Restart: "temporary", Scope: "local", Durable: true}},
	})
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	// Take the thrall down for good: a temporary thrall is not restarted after it crashes.
	cast(t, nc, app, "probe", "crash")
	reg, err := registry.Open(nc)
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	waitFor(t, 10*time.Second, "probe down", func() bool {
		e, ok, err := reg.Get("probe")
		return err == nil && ok && e.Status == "down"
	})

	sender := &thrall.Ctx{NATS: nc, App: app, Name: "sender"}
	if err := sender.CastConfirmed("probe", "inc", nil, confirmTimeout, thrall.WithIdempotencyKey("down-1")); err != nil {
		t.Fatalf("CastConfirmed with the thrall down: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	last, err := js.GetLastMsg(wire.Stream(app, "probe"), wire.Cast(app, "probe"))
	if err != nil {
		t.Fatalf("GetLastMsg: %v", err)
	}
	var e wire.Envelope
	if err := json.Unmarshal(last.Data, &e); err != nil {
		t.Fatalf("decode stored envelope: %v", err)
	}
	if e.Op != "inc" || e.Idem != "down-1" {
		t.Fatalf("last stored cast = %s/%q, want inc/down-1 waiting in the mailbox", e.Op, e.Idem)
	}
}
