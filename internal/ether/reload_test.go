package ether

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/natstest"
	"github.com/nats-io/nats.go"
)

// TestReloadRotatesTLSCert proves a TLS certificate can be rotated on a running secured bus: after
// the operator replaces the cert files and the server reloads, a live connection keeps working, a
// new client verifying against the new CA connects, and the old CA no longer verifies the server.
func TestReloadRotatesTLSCert(t *testing.T) {
	certFile, keyFile, caFile, seedFile := natstest.Files(t)
	cfg := Config{
		Mode:     "embedded",
		StoreDir: t.TempDir(),
		Security: &Security{
			Listen:  fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			TLSCert: certFile, TLSKey: keyFile, CA: caFile, NkeySeed: seedFile,
		},
	}
	eth, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("start secured embedded: %v", err)
	}
	t.Cleanup(eth.Stop)
	url := eth.URL()

	// A live client connected with the original CA, kept open across the rotation.
	o1, _ := ClientOptions(caFile, seedFile)
	live, err := nats.Connect(url, o1...)
	if err != nil {
		t.Fatalf("initial connect: %v", err)
	}
	defer live.Close()

	// The operator renews the cert in place; the node reloads it.
	newCA := natstest.RotateCert(t, certFile, keyFile)
	if err := eth.Reload(cfg); err != nil {
		t.Fatalf("reload: %v", err)
	}

	// The live connection survives and still works (round-trip through the server).
	if !live.IsConnected() {
		t.Fatalf("live connection dropped after reload")
	}
	sub, err := live.SubscribeSync("probe")
	if err != nil {
		t.Fatalf("subscribe on live conn: %v", err)
	}
	_ = live.Publish("probe", []byte("x"))
	_ = live.Flush()
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("live connection not working after reload: %v", err)
	}

	// A new client must verify against the rotated CA; the old CA no longer matches the new cert.
	oNew, _ := ClientOptions(newCA, seedFile)
	nc, err := nats.Connect(url, append(oNew, nats.Timeout(2*time.Second))...)
	if err != nil {
		t.Fatalf("connect with rotated CA should succeed: %v", err)
	}
	nc.Close()

	oOld, _ := ClientOptions(caFile, seedFile)
	if nc, err := nats.Connect(url, append(oOld, nats.Timeout(2*time.Second))...); err == nil {
		nc.Close()
		t.Fatalf("old CA should no longer verify the rotated server cert")
	}
}

// TestReloadRejectsUnsecured proves Reload is only meaningful for a secured embedded bus.
func TestReloadRejectsUnsecured(t *testing.T) {
	eth, err := Start(context.Background(), Config{Mode: "embedded", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("start plain embedded: %v", err)
	}
	t.Cleanup(eth.Stop)
	if err := eth.Reload(Config{Mode: "embedded"}); err == nil {
		t.Fatalf("reload without [nats.security] should error")
	}
}
