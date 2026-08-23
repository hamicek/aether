package lord

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/ether"
)

// TestLordInjectsSecurityCredentials proves the lord sources a thrall's bus credentials from the
// active [nats.security] block (not the client-side tls/auth fields) and passes them on as env
// paths, so a secured embedded server and its thralls authenticate with the same identity.
func TestLordInjectsSecurityCredentials(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App:      "sec",
		Strategy: "one_for_one",
		Nats: ether.Config{Mode: "embedded", Security: &ether.Security{
			Listen: "0.0.0.0:4222", TLSCert: "server.pem", TLSKey: "server-key.pem", CA: "ca.pem", NkeySeed: "user.nk",
		}},
		Thralls: []ThrallSpec{{Name: "worker", Cmd: "true", Restart: "permanent", Scope: "local"}},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}

	var c *child
	for _, ch := range l.children {
		if ch.spec.Name == "worker" {
			c = ch
		}
	}
	if c == nil {
		t.Fatalf("worker child not found")
	}
	if c.caPath != "ca.pem" || c.nkeySeed != "user.nk" {
		t.Fatalf("child credentials = (%q, %q), want (ca.pem, user.nk)", c.caPath, c.nkeySeed)
	}

	env := strings.Join(c.env(), "\n")
	for _, want := range []string{"AETHER_NATS_CA=ca.pem", "AETHER_NATS_NKEY_SEED=user.nk"} {
		if !strings.Contains(env, want) {
			t.Fatalf("thrall env missing %q", want)
		}
	}
}

// TestLordInjectsThrallRole proves that in the least-privilege tier the lord injects the THRALL
// seed into a thrall - never the lord identity - so a thrall runs with thrall-scoped permissions.
func TestLordInjectsThrallRole(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App:      "sec",
		Strategy: "one_for_one",
		Nats: ether.Config{Mode: "embedded", Security: &ether.Security{
			Listen: "0.0.0.0:4222", TLSCert: "server.pem", TLSKey: "server-key.pem", CA: "ca.pem",
			LordNkey: "lord.nk", ThrallNkey: "thrall.nk", OperatorNkey: "operator.nk",
		}},
		Thralls: []ThrallSpec{{Name: "worker", Cmd: "true", Restart: "permanent", Scope: "local"}},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	var c *child
	for _, ch := range l.children {
		if ch.spec.Name == "worker" {
			c = ch
		}
	}
	if c == nil {
		t.Fatalf("worker child not found")
	}
	if c.nkeySeed != "thrall.nk" {
		t.Fatalf("thrall got seed %q, want thrall.nk (never lord.nk)", c.nkeySeed)
	}
}
