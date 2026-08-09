package lord

import (
	"context"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/natstest"
	"github.com/hamicek/aether/internal/registry"
)

// securedEther connects the lord's system connection to a secured bus.
func securedEther(t *testing.T, sec natstest.Secured) *ether.Ether {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{
		Mode: "external",
		URL:  sec.URL,
		TLS:  ether.TLS{CA: sec.CAFile},
		Auth: ether.Auth{NkeySeed: sec.SeedFile},
	})
	if err != nil {
		t.Fatalf("secured ether: %v", err)
	}
	t.Cleanup(eth.Stop)
	return eth
}

// securedManifest carries the CA/seed in its [nats] config, so the lord injects them
// into thralls.
func securedManifest(t *testing.T, app string, sec natstest.Secured, specs ...ThrallSpec) *Manifest {
	t.Helper()
	cmd := probeCmd(t)
	for i := range specs {
		specs[i].Cmd = cmd
	}
	return &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  specs,
		Nats: ether.Config{
			Mode: "external",
			URL:  sec.URL,
			TLS:  ether.TLS{CA: sec.CAFile},
			Auth: ether.Auth{NkeySeed: sec.SeedFile},
		},
	}
}

// TestSecuredThrallConnects proves the lord injects the credentials and the Go SDK
// thrall uses them to join a secured bus, with call/cast working end to end.
func TestSecuredThrallConnects(t *testing.T) {
	const app = "sec"
	sec := natstest.SecuredServer(t)
	eth := securedEther(t, sec)
	startLord(t, eth, securedManifest(t, app, sec, spec("probe", "permanent", "local")))
	nc := eth.Conn()

	waitReady(t, eth, "probe")
	if got := callInt(t, nc, app, "probe", "get"); got != 0 {
		t.Fatalf("call over the secured bus: got %d, want 0", got)
	}
	cast(t, nc, app, "probe", "inc")
	waitFor(t, 2*time.Second, "cast applied over the secured bus", func() bool {
		v, ok := tryCallInt(nc, app, "probe", "get")
		return ok && v == 1
	})
}

// TestThrallWithoutCredentialsCannotJoinSecuredBus is the negative control: with no
// security block in the manifest the lord injects nothing, so the thrall cannot
// authenticate to the secured bus and never registers. `temporary` keeps it from
// restart-storming after the failed connect.
func TestThrallWithoutCredentialsCannotJoinSecuredBus(t *testing.T) {
	const app = "sec"
	sec := natstest.SecuredServer(t)
	eth := securedEther(t, sec)

	m := &Manifest{
		App:      app,
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{spec("probe", "temporary", "local")},
	}
	m.Thralls[0].Cmd = probeCmd(t)
	startLord(t, eth, m)

	if readyWithin(t, eth, "probe", 2*time.Second) {
		t.Fatal("probe joined the secured bus without credentials")
	}
}

// readyWithin polls the registry and reports whether name becomes ready within d.
func readyWithin(t *testing.T, eth *ether.Ether, name string, d time.Duration) bool {
	t.Helper()
	reg, err := registry.Open(eth.Conn())
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		e, ok, err := reg.Get(name)
		if err == nil && ok && e.Status == "ready" && e.PID > 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
