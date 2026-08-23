package ether

import (
	"fmt"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/natstest"
	"github.com/hamicek/aether/internal/wire"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// TestRolePermissionsEnforced starts a least-privilege secured server (three role identities) and
// proves the deny-based matrix at the bus: a thrall may run its data plane, drive dynamic children
// and beat its own heartbeat but cannot command a sibling or forge lifecycle events; an operator may
// call/cast but cannot drive supervision; the lord may do anything. Enforcement is checked by
// publishing as each role and seeing whether a privileged (lord) subscriber receives it.
func TestRolePermissionsEnforced(t *testing.T) {
	certFile, keyFile, caFile, lordSeed, thrallSeed, operatorSeed := natstest.RoleFiles(t)
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	sec := &Security{
		Listen: listen, TLSCert: certFile, TLSKey: keyFile, CA: caFile,
		LordNkey: lordSeed, ThrallNkey: thrallSeed, OperatorNkey: operatorSeed,
	}

	opts, err := securedServerOptions(sec, t.TempDir())
	if err != nil {
		t.Fatalf("securedServerOptions: %v", err)
	}
	if len(opts.Nkeys) != 3 {
		t.Fatalf("expected three role identities, got %d", len(opts.Nkeys))
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("server did not start in time")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	url := srv.ClientURL()

	connect := func(seed string) *nats.Conn {
		o, err := ClientOptions(caFile, seed)
		if err != nil {
			t.Fatalf("client options: %v", err)
		}
		// Swallow async permission-violation errors so a denied publish does not fail the test noisily.
		o = append(o, nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		nc, err := nats.Connect(url, o...)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		return nc
	}
	lord := connect(lordSeed)
	thrall := connect(thrallSeed)
	operator := connect(operatorSeed)
	t.Cleanup(func() { lord.Close(); thrall.Close(); operator.Close() })

	// assertPub publishes subj from pub and checks whether the lord subscriber receives it. The lord
	// may subscribe anything, so delivery reflects the publisher's permission alone.
	assertPub := func(name string, pub *nats.Conn, subj string, wantDelivered bool) {
		t.Helper()
		sub, err := lord.SubscribeSync(subj)
		if err != nil {
			t.Fatalf("%s: lord subscribe %q: %v", name, subj, err)
		}
		_ = lord.Flush()
		_ = pub.Publish(subj, []byte("x"))
		_ = pub.Flush()
		_, err = sub.NextMsg(300 * time.Millisecond)
		delivered := err == nil
		_ = sub.Unsubscribe()
		if delivered != wantDelivered {
			t.Errorf("%s: publish %q delivered=%v, want %v", name, subj, delivered, wantDelivered)
		}
	}

	// Thrall: allowed first (connection stays up), then denied.
	assertPub("thrall data plane", thrall, "aether.demo.counter.cast", true)
	assertPub("thrall LordCtl for dynamic children", thrall, wire.LordCtl(), true)
	assertPub("thrall own heartbeat", thrall, wire.Heartbeat("counter"), true)
	assertPub("thrall sibling ctl", thrall, wire.Ctl("counter"), false)
	assertPub("thrall lifecycle events", thrall, wire.Events, false)

	// Operator: allowed call, then denied supervision.
	assertPub("operator data plane", operator, "aether.demo.counter.call", true)
	assertPub("operator LordCtl", operator, wire.LordCtl(), false)
	assertPub("operator sibling ctl", operator, wire.Ctl("counter"), false)
	assertPub("operator lifecycle events", operator, wire.Events, false)

	// Lord: full rights.
	assertPub("lord lifecycle events", lord, wire.Events, true)
	assertPub("lord thrall ctl", lord, wire.Ctl("counter"), true)
}

func TestRolePermissionsShape(t *testing.T) {
	if rolePermissions(RoleLord) != nil {
		t.Errorf("lord should have nil (full) permissions")
	}
	for _, r := range []Role{RoleThrall, RoleOperator} {
		p := rolePermissions(r)
		if p == nil || p.Publish == nil {
			t.Fatalf("%s should have publish permissions", r)
		}
		if len(p.Publish.Deny) == 0 {
			t.Errorf("%s publish deny list is empty", r)
		}
	}
}
