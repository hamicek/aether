package ether

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// leafOptions builds the embedded server options for a leaf spoke. It renders the proven
// spoke-side NATS config - a per-site account with a service export of the app's data plane,
// a per-node JetStream domain, and the leaf link to the hub - parses it, then overrides the
// runtime-owned fields (bind address, store dir). The config-file path is deliberate: enabling
// JetStream per account programmatically is not supported before the server starts, whereas the
// config parser sets it up exactly as the hub-spoke spike proved (examples/hub-spoke-spike).
//
// Isolation and supervision fall out of what is exported: the data plane aether.<app>.> (a service
// export) and the curated fleet-health summary aether._fleet.<app>.> (a stream export - a one-way
// push the hub imports) are what may cross the leaf; the supervision subjects aether._lord.> are
// never exported, so they stay node-local by construction - no allow/deny policy needed. The hub
// side (the matching stream import into the center account) stays operator-authored NATS config.
func leafOptions(leaf *Leaf, app, storeDir string) (*natsserver.Options, error) {
	// Re-validate here, not only at the manifest: leaf and app are both rendered verbatim into the
	// generated config, and this builder is reachable directly (tests, other callers), not solely
	// through a pre-validated manifest.
	if err := leaf.validate(); err != nil {
		return nil, err
	}
	if app == "" {
		return nil, fmt.Errorf("nats.leaf: the manifest has no app name; the leaf export subject aether.<app>.> is undefined")
	}
	if !isSubjectToken(app) {
		return nil, fmt.Errorf("nats.leaf: app %q must be a plain subject token (letters, digits, _ or -)", app)
	}
	remoteURL, err := leafRemoteURL(leaf)
	if err != nil {
		return nil, err
	}
	cfg := leafConfig(leaf, app, remoteURL)

	// The parser needs a file; the server reads it once here (we never trigger a reload), so a
	// temp file removed straight after is enough and leaves nothing behind in the store dir.
	f, err := os.CreateTemp("", "aether-leaf-*.conf")
	if err != nil {
		return nil, fmt.Errorf("nats.leaf: create temp config: %w", err)
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err := f.WriteString(cfg); err != nil {
		f.Close()
		return nil, fmt.Errorf("nats.leaf: write config: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("nats.leaf: write config: %w", err)
	}

	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("nats.leaf: build server config: %w", err)
	}
	// Runtime-owned fields: bind loopback on a free port, our signal handling, our store dir.
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.NoSigs = true
	opts.StoreDir = storeDir
	// ConfigFile is set by the parser and would be re-read on a reload we never do; clear it so a
	// stray SIGHUP could not reach a config file we have already deleted.
	opts.ConfigFile = ""
	return opts, nil
}

// leafConfig renders the spoke-side NATS config: a per-site account that exports the app's data
// plane (aether.<app>.>, a service export) and its curated fleet-health summary
// (aether._fleet.<app>.>, a stream export the hub imports), a per-node JetStream domain, and the
// leaf link to the hub. Supervision (aether._lord.>) is deliberately not exported, so it stays
// node-local. Split out from leafOptions so the generated config is unit-testable without a server.
func leafConfig(leaf *Leaf, app, remoteURL string) string {
	// nkey/creds file for the leaf link; user:pass (embedded in the URL) covers a dev cluster.
	var creds string
	if leaf.Nkey != "" {
		creds = fmt.Sprintf("\n      credentials: %q", leaf.Nkey)
	}
	return fmt.Sprintf(`
jetstream { domain: %s }
accounts {
  %s {
    jetstream: enabled
    users: [ { user: local, password: local } ]
    exports: [
      { service: "aether.%s.>" }
      { stream: "aether._fleet.%s.>" }
    ]
  }
  SYS {}
}
system_account: SYS
no_auth_user: local
leafnodes {
  remotes: [
    {
      url: %q
      account: %s%s
    }
  ]
}
`, leaf.Domain, leaf.Site, app, app, remoteURL, leaf.Site, creds)
}

// leafRemoteURL returns the hub leafnode URL, folding in user/password credentials when the
// manifest supplies them and the URL does not already carry its own. Parsing also fails fast on
// a malformed remote rather than deferring to an opaque server error at connect time.
func leafRemoteURL(leaf *Leaf) (string, error) {
	u, err := url.Parse(leaf.Remote)
	if err != nil {
		return "", fmt.Errorf("nats.leaf.remote %q: %w", leaf.Remote, err)
	}
	if leaf.User != "" && u.User == nil {
		// url.UserPassword with an empty password renders "user:@host" (password-present but empty),
		// which NATS reads differently from "no password"; use url.User for the user-only case.
		if leaf.Password != "" {
			u.User = url.UserPassword(leaf.User, leaf.Password)
		} else {
			u.User = url.User(leaf.User)
		}
	}
	return u.String(), nil
}

// isSubjectToken reports whether s is a single NATS subject token safe to render into a config
// export subject (no dots, wildcards, spaces or quotes that would change the subject's meaning).
func isSubjectToken(s string) bool {
	if s == "" {
		return false
	}
	return !strings.ContainsAny(s, ". *>\"\r\n\t{}")
}
