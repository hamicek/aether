package ether

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/lordlease"
	"github.com/hamicek/aether/internal/natstest"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// TestStartEmbeddedRoleMode drives the full start path in the least-privilege tier: the lord
// connects as the lord role (full rights - it can publish a lifecycle event), the injected URL is
// dialable, and a thrall-role client reaches the bus through it.
func TestStartEmbeddedRoleMode(t *testing.T) {
	certFile, keyFile, caFile, lordSeed, thrallSeed, operatorSeed := natstest.RoleFiles(t)
	cfg := Config{
		Mode:     "embedded",
		StoreDir: t.TempDir(),
		Security: &Security{
			Listen: fmt.Sprintf("0.0.0.0:%d", freePort(t)), TLSCert: certFile, TLSKey: keyFile, CA: caFile,
			LordNkey: lordSeed, ThrallNkey: thrallSeed, OperatorNkey: operatorSeed,
		},
	}
	eth, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("start role-mode secured embedded: %v", err)
	}
	t.Cleanup(eth.Stop)

	if strings.Contains(eth.URL(), "0.0.0.0") {
		t.Fatalf("injected URL not dialable: %s", eth.URL())
	}
	// The lord has full rights: publishing a lifecycle event must succeed.
	if err := eth.Conn().Publish("aether._lord.events", []byte("x")); err != nil {
		t.Fatalf("lord publish events: %v", err)
	}

	thrallCA, thrallSeedPath := cfg.ClientCredentials(RoleThrall)
	opts, err := ClientOptions(thrallCA, thrallSeedPath)
	if err != nil {
		t.Fatalf("thrall client options: %v", err)
	}
	nc, err := nats.Connect(eth.URL(), opts...)
	if err != nil {
		t.Fatalf("thrall connect via injected URL: %v", err)
	}
	nc.Close()
}

// TestThrallRoleJetStreamAndKVIntact is the safety proof for the deny-based model: a thrall-role
// client can still do the JetStream and KV work the durable mailbox, event log and fencing rely on
// (create a stream, publish, read a KV bucket), while only the fencing WRITE it must never do is
// denied. This guards against the permission set accidentally breaking durability or fencing.
func TestThrallRoleJetStreamAndKVIntact(t *testing.T) {
	certFile, keyFile, caFile, lordSeed, thrallSeed, operatorSeed := natstest.RoleFiles(t)
	sec := &Security{
		Listen: fmt.Sprintf("127.0.0.1:%d", freePort(t)), TLSCert: certFile, TLSKey: keyFile, CA: caFile,
		LordNkey: lordSeed, ThrallNkey: thrallSeed, OperatorNkey: operatorSeed,
	}
	opts, err := securedServerOptions(sec, t.TempDir())
	if err != nil {
		t.Fatalf("securedServerOptions: %v", err)
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("server did not start")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	url := srv.ClientURL()

	connect := func(seed string) *nats.Conn {
		o, _ := ClientOptions(caFile, seed)
		o = append(o, nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		nc, err := nats.Connect(url, o...)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		return nc
	}

	// The lord (full rights) provisions a fencing KV bucket and writes a lease key, as it does at runtime.
	lord := connect(lordSeed)
	defer lord.Close()
	ljs, err := lord.JetStream()
	if err != nil {
		t.Fatalf("lord JetStream: %v", err)
	}
	kv, err := ljs.CreateKeyValue(&nats.KeyValueConfig{Bucket: lordlease.Bucket})
	if err != nil {
		t.Fatalf("lord create KV %q: %v", lordlease.Bucket, err)
	}
	if _, err := kv.Put("epoch", []byte("1")); err != nil {
		t.Fatalf("lord KV put: %v", err)
	}

	thrall := connect(thrallSeed)
	defer thrall.Close()
	tjs, err := thrall.JetStream(nats.MaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("thrall JetStream: %v", err)
	}

	// Positive: a thrall can create a stream and publish (durable-mailbox path) - $JS.> is allowed.
	if _, err := tjs.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"aether.demo.x.>"}}); err != nil {
		t.Fatalf("thrall AddStream (durable path must work): %v", err)
	}
	if _, err := tjs.Publish("aether.demo.x.1", []byte("m")); err != nil {
		t.Fatalf("thrall publish to a stream subject: %v", err)
	}

	// Positive: a thrall can READ the fencing lease (fencing verification path).
	tkv, err := tjs.KeyValue(lordlease.Bucket)
	if err != nil {
		t.Fatalf("thrall open KV %q (fencing read must work): %v", lordlease.Bucket, err)
	}
	if _, err := tkv.Get("epoch"); err != nil {
		t.Fatalf("thrall KV get (fencing read must work): %v", err)
	}

	// Negative: a thrall must NOT be able to WRITE the fencing lease (that would let it reap the app).
	if _, err := tkv.Put("epoch2", []byte("2")); err == nil {
		t.Fatalf("thrall KV put to the fencing bucket should be denied, but it succeeded")
	}
}
