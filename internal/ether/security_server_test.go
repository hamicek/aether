package ether

import (
	"fmt"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/natstest"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// TestSecuredServerEnforcesAuthAndTLS drives the production securedServerOptions: it starts a
// networked embedded server with server-TLS + nkey and proves that only a client presenting both
// the CA and the authorized nkey seed connects; missing either is rejected.
func TestSecuredServerEnforcesAuthAndTLS(t *testing.T) {
	certFile, keyFile, caFile, seedFile := natstest.Files(t)
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	sec := &Security{Listen: listen, TLSCert: certFile, TLSKey: keyFile, CA: caFile, NkeySeed: seedFile}

	opts, err := securedServerOptions(sec, t.TempDir())
	if err != nil {
		t.Fatalf("securedServerOptions: %v", err)
	}
	if !opts.TLS || opts.TLSConfig == nil {
		t.Fatalf("expected TLS enabled on the server options")
	}
	if len(opts.Nkeys) != 1 {
		t.Fatalf("expected exactly one authorized nkey, got %d", len(opts.Nkeys))
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new secured server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("secured server did not start in time")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	url := srv.ClientURL()

	// CA + nkey seed -> connects, and the bus works.
	full, err := ClientOptions(caFile, seedFile)
	if err != nil {
		t.Fatalf("client options: %v", err)
	}
	nc, err := nats.Connect(url, full...)
	if err != nil {
		t.Fatalf("authenticated connect failed: %v", err)
	}
	sub, err := nc.SubscribeSync("probe")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Publish("probe", []byte("ok")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("expected delivered message: %v", err)
	}
	nc.Close()

	// Reject cases: each drops one requirement and must fail to connect.
	reject := func(name string, opts ...nats.Option) {
		t.Run(name, func(t *testing.T) {
			opts = append(opts, nats.Timeout(2*time.Second))
			if nc, err := nats.Connect(url, opts...); err == nil {
				nc.Close()
				t.Fatalf("expected %s to be rejected, but it connected", name)
			}
		})
	}
	caOnly, _ := ClientOptions(caFile, "")
	seedOnly, _ := ClientOptions("", seedFile)
	reject("no credentials")
	reject("CA but no nkey", caOnly...)
	reject("nkey but no CA (untrusted TLS)", seedOnly...)
}
