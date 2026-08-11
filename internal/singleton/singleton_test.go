package singleton

import (
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// embeddedManager starts an in-process NATS server with JetStream and returns an opened
// lock Manager plus the raw connection (for out-of-band tampering in tests).
func embeddedManager(t *testing.T) (*Manager, *nats.Conn) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(t.TempDir(), "js"),
		NoSigs:    true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("NATS did not start in time")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	m, err := Open(nc)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	return m, nc
}

func TestAcquireStampsEpoch(t *testing.T) {
	m, _ := embeddedManager(t)

	lock, ok, err := m.TryAcquire("svc", "lord-a")
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if lock.Epoch() == 0 {
		t.Fatalf("expected a non-zero epoch after acquire")
	}

	// The epoch is the fencing token and must not move across renewals.
	epoch := lock.Epoch()
	for i := 0; i < 3; i++ {
		if err := lock.Renew(); err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if lock.Epoch() != epoch {
			t.Fatalf("renew %d changed the epoch: %d -> %d", i, epoch, lock.Epoch())
		}
	}
}

func TestVerifyTracksOwnership(t *testing.T) {
	m, _ := embeddedManager(t)

	lock, ok, err := m.TryAcquire("svc", "lord-a")
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	// A held, renewed lock verifies against its own epoch.
	if err := lock.Renew(); err != nil {
		t.Fatalf("renew: %v", err)
	}
	valid, err := m.Verify("svc", lock.Epoch())
	if err != nil {
		t.Fatalf("verify held: %v", err)
	}
	if !valid {
		t.Fatalf("held lock should verify against its epoch")
	}

	// A stale epoch does not verify.
	valid, err = m.Verify("svc", lock.Epoch()+1)
	if err != nil {
		t.Fatalf("verify stale: %v", err)
	}
	if valid {
		t.Fatalf("a mismatched epoch must not verify")
	}

	// Once released (key gone), verify reports lost with a nil error.
	if err := lock.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	valid, err = m.Verify("svc", lock.Epoch())
	if err != nil {
		t.Fatalf("verify after release: %v", err)
	}
	if valid {
		t.Fatalf("a released lock must not verify")
	}
}

func TestReacquireGetsFreshEpoch(t *testing.T) {
	m, _ := embeddedManager(t)

	first, ok, err := m.TryAcquire("svc", "lord-a")
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, ok, err := m.TryAcquire("svc", "lord-a")
	if err != nil || !ok {
		t.Fatalf("second acquire: ok=%v err=%v", ok, err)
	}

	// A takeover (even by the same holder) must carry a new, higher epoch, so the old
	// generation's fencing token no longer verifies.
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("re-acquire epoch must grow: first=%d second=%d", first.Epoch(), second.Epoch())
	}
	valid, err := m.Verify("svc", first.Epoch())
	if err != nil {
		t.Fatalf("verify old epoch: %v", err)
	}
	if valid {
		t.Fatalf("the superseded epoch must not verify after re-acquire")
	}
}

func TestSecondAcquireIsBlockedWhileHeld(t *testing.T) {
	m, _ := embeddedManager(t)

	if _, ok, err := m.TryAcquire("svc", "lord-a"); err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}
	lock, ok, err := m.TryAcquire("svc", "lord-b")
	if err != nil {
		t.Fatalf("second acquire error: %v", err)
	}
	if ok || lock != nil {
		t.Fatalf("a held lock must not be acquired by another holder")
	}
}
