package lordlease

import (
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// embeddedManager starts an in-process NATS server with JetStream and returns an opened
// lease Manager plus the raw connection (for out-of-band tampering in tests).
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

// TestEstablishStampsEpoch: Establish hands out a non-zero epoch that stays fixed across renewals.
func TestEstablishStampsEpoch(t *testing.T) {
	m, _ := embeddedManager(t)

	lease, err := m.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	if lease.Epoch() == 0 {
		t.Fatal("expected a non-zero epoch after establish")
	}
	epoch := lease.Epoch()
	for i := 0; i < 3; i++ {
		if err := lease.Renew(); err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if lease.Epoch() != epoch {
			t.Fatalf("renew %d changed the epoch: %d -> %d", i, epoch, lease.Epoch())
		}
	}
}

// TestVerifyMatchesOwnEpoch: a thrall holding the lord's epoch verifies alive; a stale epoch
// (an orphan of a previous lord) verifies not-alive.
func TestVerifyMatchesOwnEpoch(t *testing.T) {
	m, _ := embeddedManager(t)

	lease, err := m.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish: %v", err)
	}

	ok, err := m.Verify("demo", lease.Epoch())
	if err != nil || !ok {
		t.Fatalf("verify own epoch: ok=%v err=%v", ok, err)
	}
	if ok, err := m.Verify("demo", lease.Epoch()-1); err != nil || ok {
		t.Fatalf("verify a stale epoch should be not-alive: ok=%v err=%v", ok, err)
	}
}

// TestReestablishRaisesEpoch: a replacement lord (a restart) gets a strictly higher epoch, so
// an orphan of the previous lord sees a mismatch and would self-terminate.
func TestReestablishRaisesEpoch(t *testing.T) {
	m, _ := embeddedManager(t)

	first, err := m.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish 1: %v", err)
	}
	second, err := m.Establish("demo", "lord-b")
	if err != nil {
		t.Fatalf("establish 2: %v", err)
	}
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("re-establish must raise the epoch: %d -> %d", first.Epoch(), second.Epoch())
	}
	// The old lord's epoch no longer verifies against the live lease.
	if ok, _ := m.Verify("demo", first.Epoch()); ok {
		t.Fatal("an orphan of the previous lord must not verify against the new lease")
	}
}

// TestVerifyGoneWhenLeaseExpires: once the lord stops renewing, the key TTL-expires and Verify
// reports not-alive (nil error) - a thrall reads this as "lord dead".
func TestVerifyGoneWhenLeaseExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the KV TTL")
	}
	m, _ := embeddedManager(t)

	lease, err := m.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	// Stop renewing and wait past the TTL; the key must expire.
	deadline := time.Now().Add(TTL + 3*time.Second)
	for time.Now().Before(deadline) {
		ok, err := m.Verify("demo", lease.Epoch())
		if err == nil && !ok {
			return // expired -> lord gone, as expected
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("lease did not expire after the lord stopped renewing")
}

// TestLiveHolderDetectsRenewingLord: an actively-renewed lease is reported live, with the holder -
// the case where a second lord for the app must refuse to start.
func TestLiveHolderDetectsRenewingLord(t *testing.T) {
	m, _ := embeddedManager(t)
	lease, err := m.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish: %v", err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = lease.Renew()
			}
		}
	}()
	holder, err := m.LiveHolder("demo", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("LiveHolder: %v", err)
	}
	if holder != "lord-a" {
		t.Errorf("a renewing lease should be live with holder %q, got %q", "lord-a", holder)
	}
}

// TestLiveHolderIgnoresDeadPredecessor: an existing but un-renewed lease (a crashed predecessor,
// key not yet TTL-expired) is reported not-live, so a restart may take it over.
func TestLiveHolderIgnoresDeadPredecessor(t *testing.T) {
	m, _ := embeddedManager(t)
	if _, err := m.Establish("demo", "lord-a"); err != nil {
		t.Fatalf("establish: %v", err)
	}
	holder, err := m.LiveHolder("demo", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("LiveHolder: %v", err)
	}
	if holder != "" {
		t.Errorf("an un-renewed lease must be reported not-live (takeover ok), got holder %q", holder)
	}
}

// TestLiveHolderNoLease: no lease key -> not-live, without waiting out the window.
func TestLiveHolderNoLease(t *testing.T) {
	m, _ := embeddedManager(t)
	start := time.Now()
	holder, err := m.LiveHolder("demo", 5*time.Second)
	if err != nil {
		t.Fatalf("LiveHolder: %v", err)
	}
	if holder != "" {
		t.Errorf("no lease should be not-live, got holder %q", holder)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("no-lease check must return at once, took %s", elapsed)
	}
}
