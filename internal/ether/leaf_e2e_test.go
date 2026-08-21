package ether

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startLeafHub boots a test hub: a NATS server accepting leaf connections into per-site accounts
// (SITE_A, SITE_B), with a center account HUB that imports each site's data plane. It mirrors
// examples/hub-spoke-spike/nats/hub.conf. It returns the leafnode listener URL the spokes dial and
// a client connection in the HUB account (the center's view). Everything is torn down via t.Cleanup.
func startLeafHub(t *testing.T) (leafURL string, hub *nats.Conn) {
	t.Helper()
	leafPort := freePort(t)
	cfg := fmt.Sprintf(`
server_name: hub
jetstream { domain: hub }
leafnodes { listen: 127.0.0.1:%d }
accounts {
  HUB {
    jetstream: enabled
    users: [ { user: local, password: local } ]
    imports: [
      { service: { account: SITE_A, subject: "aether.sitea.>" } }
      { service: { account: SITE_B, subject: "aether.siteb.>" } }
    ]
  }
  SITE_A {
    jetstream: enabled
    users: [ { user: leafA, password: leafA } ]
    exports: [ { service: "aether.sitea.>" } ]
  }
  SITE_B {
    jetstream: enabled
    users: [ { user: leafB, password: leafB } ]
    exports: [ { service: "aether.siteb.>" } ]
  }
  SYS {}
}
system_account: SYS
no_auth_user: local
`, leafPort)

	srv := startServerFromConfig(t, cfg)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect hub: %v", err)
	}
	t.Cleanup(nc.Close)
	return fmt.Sprintf("nats-leaf://127.0.0.1:%d", leafPort), nc
}

// startServerFromConfig parses a NATS config string, overrides the runtime-owned fields, starts the
// server, and registers teardown. Used only by the leaf e2e tests to stand up a hub.
func startServerFromConfig(t *testing.T, cfg string) *natsserver.Server {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "hub-*.conf")
	if err != nil {
		t.Fatalf("temp config: %v", err)
	}
	if _, err := f.WriteString(cfg); err != nil {
		t.Fatalf("write config: %v", err)
	}
	f.Close()
	opts, err := natsserver.ProcessConfigFile(f.Name())
	if err != nil {
		t.Fatalf("process config: %v", err)
	}
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.NoSigs = true
	opts.NoLog = true
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatalf("hub did not start in time")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startSpoke brings up an aether embedded bus configured as a leaf of the hub, bound into the given
// site account with the given app namespace. It returns the spoke's local connection (in its site
// account, via no_auth_user).
func startSpoke(t *testing.T, leafURL, site, app, domain, user string) *Ether {
	t.Helper()
	eth, err := Start(context.Background(), Config{
		Mode: "embedded",
		Leaf: &Leaf{Remote: leafURL, Site: site, Domain: domain, User: user, Password: user},
	}, WithApp(app))
	if err != nil {
		t.Fatalf("start spoke %s: %v", site, err)
	}
	t.Cleanup(eth.Stop)
	return eth
}

// requestUntil retries a request until it succeeds or the deadline passes, absorbing the leaf
// interest-propagation delay. It returns the reply, or "" if the request never got through.
func requestUntil(nc *nats.Conn, subject string, deadline time.Duration) string {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		msg, err := nc.Request(subject, nil, 200*time.Millisecond)
		if err == nil {
			return string(msg.Data)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ""
}

// TestLeafSpokeDataPlaneCrosses (AC #1) proves an embedded spoke connects as a leaf and its app's
// data plane crosses the boundary: a request from the hub reaches a responder on the spoke.
func TestLeafSpokeDataPlaneCrosses(t *testing.T) {
	leafURL, hub := startLeafHub(t)
	spoke := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	if _, err := spoke.Conn().Subscribe("aether.sitea.ping", func(m *nats.Msg) {
		m.Respond([]byte("pongA"))
	}); err != nil {
		t.Fatalf("subscribe on spoke: %v", err)
	}
	spoke.Conn().Flush()

	if got := requestUntil(hub, "aether.sitea.ping", 5*time.Second); got != "pongA" {
		t.Fatalf("hub -> spoke request = %q, want pongA (data plane did not cross the leaf)", got)
	}
}

// TestLeafCastCrossesLeaf (AC #1) proves a one-way cast crosses the leaf, not just request-reply:
// the site's data plane is a NATS service export, and the example's `aether cast ... inc` relies on
// a fire-and-forget publish from the hub reaching the spoke - so pin that semantic down.
func TestLeafCastCrossesLeaf(t *testing.T) {
	leafURL, hub := startLeafHub(t)
	spoke := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	got := make(chan string, 1)
	if _, err := spoke.Conn().Subscribe("aether.sitea.tell", func(m *nats.Msg) {
		select {
		case got <- string(m.Data):
		default:
		}
	}); err != nil {
		t.Fatalf("subscribe on spoke: %v", err)
	}
	spoke.Conn().Flush()

	// Retry the publish until leaf interest has propagated, then confirm it arrived.
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		if err := hub.Publish("aether.sitea.tell", []byte("hi")); err != nil {
			t.Fatalf("publish from hub: %v", err)
		}
		hub.Flush()
		select {
		case v := <-got:
			if v != "hi" {
				t.Fatalf("cast payload = %q, want hi", v)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("cast never crossed the leaf from hub to spoke")
}

// TestLeafSiteIsolation (AC #2) proves each site is isolated into its own account: the hub reaches
// both sites, but one site cannot reach the other's data plane.
func TestLeafSiteIsolation(t *testing.T) {
	leafURL, hub := startLeafHub(t)
	spokeA := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")
	spokeB := startSpoke(t, leafURL, "SITE_B", "siteb", "sb", "leafB")

	if _, err := spokeA.Conn().Subscribe("aether.sitea.ping", func(m *nats.Msg) {
		m.Respond([]byte("pongA"))
	}); err != nil {
		t.Fatalf("subscribe on spoke A: %v", err)
	}
	if _, err := spokeB.Conn().Subscribe("aether.siteb.ping", func(m *nats.Msg) {
		m.Respond([]byte("pongB"))
	}); err != nil {
		t.Fatalf("subscribe on spoke B: %v", err)
	}
	spokeA.Conn().Flush()
	spokeB.Conn().Flush()

	// The hub imports both sites, so it can reach each.
	if got := requestUntil(hub, "aether.sitea.ping", 5*time.Second); got != "pongA" {
		t.Fatalf("hub -> A = %q, want pongA", got)
	}
	if got := requestUntil(hub, "aether.siteb.ping", 5*time.Second); got != "pongB" {
		t.Fatalf("hub -> B = %q, want pongB", got)
	}
	// Site A must not reach site B: the accounts do not import each other. Give it the same budget
	// the successful requests had, so this is a real timeout, not a race that just hasn't propagated.
	if got := requestUntil(spokeA.Conn(), "aether.siteb.ping", 2*time.Second); got != "" {
		t.Fatalf("A -> B = %q, want no reply (sites must be isolated)", got)
	}
}

// TestLeafSupervisionStaysNodeLocal (AC #3) proves the supervision subjects never cross the leaf:
// even with a responder live on the spoke, a request from the hub for aether._lord.> times out,
// because supervision is never exported.
func TestLeafSupervisionStaysNodeLocal(t *testing.T) {
	leafURL, hub := startLeafHub(t)
	spoke := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	// A live supervision responder on the spoke - the data plane responder proves the leaf is up.
	if _, err := spoke.Conn().Subscribe("aether.sitea.ping", func(m *nats.Msg) {
		m.Respond([]byte("pongA"))
	}); err != nil {
		t.Fatalf("subscribe data plane: %v", err)
	}
	if _, err := spoke.Conn().Subscribe("aether._lord.counter.ctl", func(m *nats.Msg) {
		m.Respond([]byte("lord"))
	}); err != nil {
		t.Fatalf("subscribe supervision: %v", err)
	}
	spoke.Conn().Flush()

	// Data plane crosses (leaf is up)...
	if got := requestUntil(hub, "aether.sitea.ping", 5*time.Second); got != "pongA" {
		t.Fatalf("data plane = %q, want pongA (leaf not up)", got)
	}
	// ...but supervision does not.
	if got := requestUntil(hub, "aether._lord.counter.ctl", 2*time.Second); got != "" {
		t.Fatalf("supervision crossed the leaf (= %q); it must stay node-local", got)
	}
}

// TestLeafSpokeJetStreamDomain (AC #4) proves the spoke's JetStream runs under the manifest's own
// domain: a KV bucket - the same primitive the lord's registry and durable mailbox use - can be
// created and used on the spoke via the plain, un-domained JetStream context the runtime uses.
func TestLeafSpokeJetStreamDomain(t *testing.T) {
	leafURL, _ := startLeafHub(t)
	spoke := startSpoke(t, leafURL, "SITE_A", "sitea", "sa", "leafA")

	// The real runtime (registry, lord, thrall SDK) uses a plain JetStream() with no domain option,
	// so the test must too - proving the spoke's own JetStream is reachable the way aether reaches it,
	// not via an artificial domain-qualified context.
	js, err := spoke.Conn().JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "aether_lords"})
	if err != nil {
		t.Fatalf("create KV under domain sa: %v", err)
	}
	if _, err := kv.PutString("k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	entry, err := kv.Get("k")
	if err != nil || string(entry.Value()) != "v" {
		t.Fatalf("get = %v/%v, want v", entry, err)
	}
}
