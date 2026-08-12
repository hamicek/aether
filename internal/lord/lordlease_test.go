package lord

import (
	"testing"
	"time"

	"github.com/hamicek/aether/internal/lordlease"
)

// TestLordLivenessLeaseEstablishedAndRenewed: the lord publishes its liveness lease at Start
// (under the app key, carrying its epoch) and keeps renewing it, so a thrall's verify against
// the injected epoch keeps succeeding while the lord is up.
func TestLordLivenessLeaseEstablishedAndRenewed(t *testing.T) {
	eth := startEmbedded(t)
	l := startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))

	if l.lordEpoch == 0 {
		t.Fatal("lord did not establish a liveness epoch")
	}
	leases, err := lordlease.Open(eth.Conn())
	if err != nil {
		t.Fatalf("open lease manager: %v", err)
	}
	if ok, err := leases.Verify("demo", l.lordEpoch); err != nil || !ok {
		t.Fatalf("lease should verify right after Start: ok=%v err=%v", ok, err)
	}
	// Past a couple of renew intervals it must still verify - i.e. the lease is being renewed,
	// not just written once and left to expire.
	time.Sleep(lordLeaseRenew*2 + 500*time.Millisecond)
	if ok, err := leases.Verify("demo", l.lordEpoch); err != nil || !ok {
		t.Fatalf("lease should still verify after renews: ok=%v err=%v", ok, err)
	}
}

// TestChildEnvInjectsLordEpoch: every thrall the lord spawns carries the lord-liveness fencing
// env; a child with no lord epoch (a unit-test child) carries none.
func TestChildEnvInjectsLordEpoch(t *testing.T) {
	withLord := (&child{lordKey: "demo", lordEpoch: 42}).env()
	assertEnv(t, withLord, "AETHER_LORD_BUCKET="+lordlease.Bucket)
	assertEnv(t, withLord, "AETHER_LORD_KEY=demo")
	assertEnv(t, withLord, "AETHER_LORD_EPOCH=42")

	withoutLord := (&child{}).env()
	for _, e := range withoutLord {
		if len(e) >= len("AETHER_LORD_") && e[:len("AETHER_LORD_")] == "AETHER_LORD_" {
			t.Fatalf("a child with no lord epoch must not inject %q", e)
		}
	}
}

func assertEnv(t *testing.T, env []string, want string) {
	t.Helper()
	for _, e := range env {
		if e == want {
			return
		}
	}
	t.Fatalf("env is missing %q", want)
}
