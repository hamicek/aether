package ether

import (
	"strings"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// findAccount returns the named account from the parsed options, or fails the test.
func findAccount(t *testing.T, opts *natsserver.Options, name string) *natsserver.Account {
	t.Helper()
	for _, a := range opts.Accounts {
		if a.GetName() == name {
			return a
		}
	}
	t.Fatalf("account %q not found in %d accounts", name, len(opts.Accounts))
	return nil
}

// TestLeafOptions proves the builder renders a spoke bound into its site's account: the app's
// data plane is exported, supervision is not (node-local by construction), JetStream runs under
// the per-node domain, and the leaf remote binds to the site account with folded-in credentials.
func TestLeafOptions(t *testing.T) {
	leaf := &Leaf{
		Remote:   "nats-leaf://hub.internal:7422",
		Site:     "SITE_A",
		Domain:   "sa",
		User:     "leafA",
		Password: "leafA",
	}
	opts, err := leafOptions(leaf, "demo", t.TempDir())
	if err != nil {
		t.Fatalf("leafOptions: %v", err)
	}

	if opts.JetStreamDomain != "sa" {
		t.Errorf("JetStreamDomain = %q, want sa", opts.JetStreamDomain)
	}
	if opts.SystemAccount != "SYS" {
		t.Errorf("SystemAccount = %q, want SYS", opts.SystemAccount)
	}
	if opts.NoAuthUser != "local" {
		t.Errorf("NoAuthUser = %q, want local", opts.NoAuthUser)
	}

	site := findAccount(t, opts, "SITE_A")
	if !site.IsExportService("aether.demo.increment") {
		t.Errorf("SITE_A does not export the app data plane aether.demo.>")
	}
	// Supervision must stay node-local: it is never exported, so it cannot cross the leaf.
	if site.IsExportService("aether._lord.counter.ctl") {
		t.Errorf("SITE_A exports a supervision subject; it must stay node-local")
	}

	if len(opts.LeafNode.Remotes) != 1 {
		t.Fatalf("leaf remotes = %d, want 1", len(opts.LeafNode.Remotes))
	}
	rem := opts.LeafNode.Remotes[0]
	if rem.LocalAccount != "SITE_A" {
		t.Errorf("remote LocalAccount = %q, want SITE_A", rem.LocalAccount)
	}
	if len(rem.URLs) != 1 {
		t.Fatalf("remote URLs = %d, want 1", len(rem.URLs))
	}
	if got := rem.URLs[0].String(); !strings.Contains(got, "leafA:leafA@hub.internal:7422") {
		t.Errorf("remote URL = %q, want folded-in credentials", got)
	}
}

// TestLeafOptionsNkeyCredentials proves an nkey/creds file path lands on the leaf remote and the
// URL is left untouched (no credentials folded in) when user/password are absent.
func TestLeafOptionsNkeyCredentials(t *testing.T) {
	leaf := &Leaf{
		Remote: "nats-leaf://hub.internal:7422",
		Site:   "SITE_A",
		Domain: "sa",
		Nkey:   "/etc/aether/siteA.nk",
	}
	opts, err := leafOptions(leaf, "demo", t.TempDir())
	if err != nil {
		t.Fatalf("leafOptions: %v", err)
	}
	rem := opts.LeafNode.Remotes[0]
	if rem.Credentials != "/etc/aether/siteA.nk" {
		t.Errorf("remote Credentials = %q, want the nkey path", rem.Credentials)
	}
	if got := rem.URLs[0].String(); strings.Contains(got, "@") {
		t.Errorf("remote URL = %q, want no embedded credentials", got)
	}
}

// TestLeafOptionsRequiresApp proves the builder fails fast when the manifest has no app name -
// the export subject aether.<app>.> would be undefined.
func TestLeafOptionsRequiresApp(t *testing.T) {
	leaf := &Leaf{Remote: "nats-leaf://hub:7422", Site: "SITE_A", Domain: "sa"}
	if _, err := leafOptions(leaf, "", t.TempDir()); err == nil {
		t.Fatalf("expected error for empty app, got nil")
	}
}
