package thrall

import (
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/fencing"
	"github.com/hamicek/aether/internal/lordlease"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/singleton"
)

// embeddedNATS starts an in-process JetStream server and returns a connection to it plus
// the server handle (so a test can shut it down to simulate an unreachable bus).
func embeddedNATS(t *testing.T) (*nats.Conn, *natsserver.Server) {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(t.TempDir(), "js"),
		NoSigs:    true,
	})
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
	return nc, srv
}

// TestSingletonEpochFromEnv proves ctx.SingletonEpoch is read from the lord-injected env for a
// singleton (both AETHER_SINGLETON_* present) and is 0 for a non-singleton (no env).
func TestSingletonEpochFromEnv(t *testing.T) {
	if got := singletonEpochFromEnv(); got != 0 {
		t.Errorf("no fencing env: epoch = %d, want 0", got)
	}
	t.Setenv("AETHER_SINGLETON_KEY", "demo")
	t.Setenv("AETHER_SINGLETON_EPOCH", "42")
	if got := singletonEpochFromEnv(); got != 42 {
		t.Errorf("with fencing env: epoch = %d, want 42", got)
	}
}

// runFencing starts the fencing loop with a valid held lock and returns a channel that
// receives the loss reason (nil onLost side effect replaced by the channel) and a stop func.
func runFencing(t *testing.T, nc *nats.Conn) (mgr *singleton.Manager, lock *singleton.Lock, lost chan string, stop chan struct{}) {
	t.Helper()
	mgr, err := singleton.Open(nc)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	lock, ok, err := mgr.TryAcquire("single", "lord-a")
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	lost = make(chan string, 1)
	stop = make(chan struct{})
	epoch := lock.Epoch()
	verify := func() (bool, error) { return mgr.Verify("single", epoch) }
	go fencing.Loop("singleton fencing", verify, fenceInterval, fenceLease, obs.NewLogger(), stop,
		func(reason string) { lost <- reason })
	t.Cleanup(func() { close(stop) })
	return mgr, lock, lost, stop
}

// fenceInterval / fenceLease mirror the production cadence (singleton and lord-liveness share
// the same TTL) so the tests exercise the shared fencing loop at its real timing.
const (
	fenceLease    = singleton.TTL
	fenceInterval = singleton.TTL / 3
)

func TestFencingStaysWhileHeld(t *testing.T) {
	nc, _ := embeddedNATS(t)
	_, lock, lost, _ := runFencing(t, nc)

	// Keep the lock alive by renewing it while fencing runs; nothing should fire.
	deadline := time.After(fenceLease + 2*fenceInterval)
	renew := time.NewTicker(fenceInterval)
	defer renew.Stop()
	for {
		select {
		case reason := <-lost:
			t.Fatalf("fencing fired while the lock was held: %s", reason)
		case <-renew.C:
			if err := lock.Renew(); err != nil {
				t.Fatalf("renew: %v", err)
			}
		case <-deadline:
			return // survived the lease window without a false positive
		}
	}
}

// TestLordLivenessFencingFiresWhenLordReplaced: the same shared fencing loop, driven by the
// lord-liveness lease, self-terminates a thrall when a replacement lord stamps a new epoch - the
// crash/fast-restart case for a non-singleton thrall.
func TestLordLivenessFencingFiresWhenLordReplaced(t *testing.T) {
	nc, _ := embeddedNATS(t)
	mgr, err := lordlease.Open(nc)
	if err != nil {
		t.Fatalf("open lease manager: %v", err)
	}
	lease, err := mgr.Establish("demo", "lord-a")
	if err != nil {
		t.Fatalf("establish: %v", err)
	}

	lost := make(chan string, 1)
	stop := make(chan struct{})
	defer close(stop)
	epoch := lease.Epoch()
	verify := func() (bool, error) { return mgr.Verify("demo", epoch) }
	go fencing.Loop("lord-liveness fencing", verify, fenceInterval, fenceLease, obs.NewLogger(), stop,
		func(reason string) { lost <- reason })

	// A replacement lord (a restart) stamps a new epoch; the running thrall still holds the old.
	if _, err := mgr.Establish("demo", "lord-b"); err != nil {
		t.Fatalf("re-establish: %v", err)
	}

	select {
	case <-lost:
	case <-time.After(fenceLease + 2*fenceInterval):
		t.Fatal("lord-liveness fencing did not fire after the lord was replaced")
	}
}

func TestFencingFiresOnEpochTakeover(t *testing.T) {
	nc, _ := embeddedNATS(t)
	mgr, lock, lost, _ := runFencing(t, nc)

	// Simulate a takeover: release and re-acquire, which stamps a new epoch. The running
	// fencing loop still holds the old epoch and must detect the loss.
	if err := lock.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok, err := mgr.TryAcquire("single", "lord-b"); err != nil || !ok {
		t.Fatalf("takeover acquire: ok=%v err=%v", ok, err)
	}

	select {
	case <-lost:
	case <-time.After(fenceLease + 2*fenceInterval):
		t.Fatal("fencing did not fire after an epoch takeover")
	}
}

func TestFencingFiresOnPurge(t *testing.T) {
	nc, _ := embeddedNATS(t)
	_, lock, lost, _ := runFencing(t, nc)

	// The key disappears entirely (expiry/release) -> lock lost.
	if err := lock.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	select {
	case <-lost:
	case <-time.After(fenceLease + 2*fenceInterval):
		t.Fatal("fencing did not fire after the lock key was purged")
	}
}

func TestFencingFiresAfterLeaseWhenUnreachable(t *testing.T) {
	nc, srv := embeddedNATS(t)
	_, _, lost, _ := runFencing(t, nc)

	// Make the KV unverifiable by killing the server. Fencing must NOT fire before the
	// lease elapses, but must fire once it does.
	srv.Shutdown()
	srv.WaitForShutdown()

	select {
	case reason := <-lost:
		t.Fatalf("fencing fired before the lease elapsed: %s", reason)
	case <-time.After(fenceLease - fenceInterval):
		// good: still within the lease, no fire yet
	}
	select {
	case <-lost:
	case <-time.After(fenceLease + 2*fenceInterval):
		t.Fatal("fencing did not fire after the lease elapsed with an unreachable bus")
	}
}
