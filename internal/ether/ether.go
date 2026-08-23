// Package ether manages the bus (the ether): an embedded NATS server, or a
// connection to an external cluster. The interface is the same for both modes -
// neither thrall nor lord can tell the difference, because both speak only NATS.
package ether

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Config corresponds to the [nats] section in the manifest.
type Config struct {
	Mode string `toml:"mode"` // "embedded" | "external"
	URL  string `toml:"url"`  // for external: "nats://a:4222,nats://b:4222"
	// StoreDir is the JetStream storage directory for the embedded server. Empty
	// (the default) uses an ephemeral temp dir removed on Stop; a fixed path keeps
	// the durable mailbox across a lord/process restart. Ignored in external mode.
	StoreDir string `toml:"store_dir"`
	TLS      TLS    `toml:"tls"`  // client-side transport security (external mode)
	Auth     Auth   `toml:"auth"` // client authentication (external mode)
	// Leaf, when present, makes the embedded server a leaf node of a central hub
	// (the spoke side of a hub-spoke topology). Valid only for embedded mode; nil =
	// a standalone embedded bus, as before.
	Leaf *Leaf `toml:"leaf"`
	// Security, when present, exposes the embedded server on the network with server-side
	// TLS and mandatory nkey authentication (the server side of the bus). Valid only for
	// embedded mode, and not together with a leaf (securing the leaf spoke is a later
	// increment). nil = today's behaviour: loopback bind, no auth. Unrelated to TLS/Auth
	// above, which are the client side (connecting out to an external bus).
	Security *Security `toml:"security"`
}

// Leaf configures the embedded server as a NATS leaf node of a central hub. It binds
// this node's bus into a per-site account on the hub and gives its JetStream a per-node
// domain, closing the gap where an embedded bus could not be a spoke. aether owns only
// this spoke-side intent - which hub, which site, which JS domain, which credential; the
// hub side stays operator-authored NATS config (mode = "external").
type Leaf struct {
	Remote string `toml:"remote"` // the hub's leafnode listener, e.g. "nats-leaf://hub.internal:7422"
	Site   string `toml:"site"`   // the account this node binds to = its isolation unit on the hub
	Domain string `toml:"domain"` // JetStream domain, unique per node (else the leaf's and hub's streams collide)
	// Credentials for the leaf link. User/Password suit a dev cluster (and may also be
	// embedded directly in Remote); Nkey is the path to an nkey seed file for production.
	User     string `toml:"user"`
	Password string `toml:"password"`
	Nkey     string `toml:"nkey"`
}

// TLS configures client-side transport security for an external bus. CA is the path
// to a PEM file used to verify the server's certificate (server TLS). Empty = no TLS.
type TLS struct {
	CA string `toml:"ca"`
}

// Auth configures client authentication to an external bus. NkeySeed is the path to
// an nkey seed file the client signs the server nonce with. Empty = no auth.
type Auth struct {
	NkeySeed string `toml:"nkey_seed"`
}

// Security configures the embedded server for a network bind with server-side TLS and
// mandatory nkey authentication. Present = the bus is exposed on Listen, encrypted, and
// every client must authenticate; absent = the loopback, no-auth default. When present,
// all fields are required (no half-secured bus).
//
// Authentication has two mutually exclusive shapes. NkeySeed is the *simple* tier: one shared
// identity with full rights (encrypted and authenticated, but no role separation). The per-role
// seeds (LordNkey / ThrallNkey / OperatorNkey, all three together) are the *least-privilege* tier:
// each of the lord, thralls and operator gets its own identity with a role-scoped permission set,
// so a thrall cannot drive supervision (see securedServerOptions). Set one shape or the other, never
// a mix. mTLS is a later opt-in.
type Security struct {
	Listen  string `toml:"listen"`   // network bind, e.g. "0.0.0.0:4222"
	TLSCert string `toml:"tls_cert"` // path to the server certificate (PEM)
	TLSKey  string `toml:"tls_key"`  // path to the server private key (PEM)
	CA      string `toml:"ca"`       // path to the CA clients verify the server against (the lord distributes it)

	// Simple tier: one shared identity, full rights, no role separation.
	NkeySeed string `toml:"nkey_seed"`

	// Least-privilege tier: one nkey seed per role (all three required together, and exclusive
	// with NkeySeed). The recommended path - the role permissions are what enforce aether._lord.>
	// as node-local on a networked bus.
	LordNkey     string `toml:"lord_nkey"`
	ThrallNkey   string `toml:"thrall_nkey"`
	OperatorNkey string `toml:"operator_nkey"`
}

// validate checks the security section's invariants. Like Leaf.validate it lives on the type
// so it can run wherever the section is consumed, and it is strict: a present [nats.security]
// must be fully specified, because a partially configured secured bus is a security hazard,
// not a convenience.
func (s *Security) validate() error {
	if s.Listen == "" {
		return fmt.Errorf("nats.security.listen is required (the network bind, e.g. \"0.0.0.0:4222\")")
	}
	if _, _, err := net.SplitHostPort(s.Listen); err != nil {
		return fmt.Errorf("nats.security.listen %q: %w", s.Listen, err)
	}
	if s.TLSCert == "" || s.TLSKey == "" {
		return fmt.Errorf("nats.security: tls_cert and tls_key are both required (server-side TLS)")
	}
	if s.CA == "" {
		return fmt.Errorf("nats.security.ca is required (clients verify the server against it)")
	}
	// Authentication is either the simple shared identity or the full per-role trio, never a mix
	// and never partial - a half-configured identity set is a hazard, not a convenience.
	roles := 0
	for _, seed := range []string{s.LordNkey, s.ThrallNkey, s.OperatorNkey} {
		if seed != "" {
			roles++
		}
	}
	switch {
	case s.NkeySeed != "" && roles > 0:
		return fmt.Errorf("nats.security: nkey_seed (one shared identity) and the per-role seeds (lord_nkey/thrall_nkey/operator_nkey) are mutually exclusive")
	case s.NkeySeed == "" && roles == 0:
		return fmt.Errorf("nats.security: authentication is mandatory - set nkey_seed (one shared identity) or all three of lord_nkey/thrall_nkey/operator_nkey")
	case s.NkeySeed == "" && roles != 3:
		return fmt.Errorf("nats.security: per-role authentication requires all three of lord_nkey, thrall_nkey and operator_nkey")
	}
	return nil
}

// roleMode reports whether the per-role (least-privilege) identities are in use rather than the
// single shared seed. Meaningful only after validate has passed (which guarantees exactly one shape).
func (s *Security) roleMode() bool { return s.NkeySeed == "" }

// seedFor returns the nkey seed path a given role authenticates with: its own per-role seed in the
// least-privilege tier, or the shared seed in the simple tier.
func (s *Security) seedFor(r Role) string {
	if !s.roleMode() {
		return s.NkeySeed
	}
	switch r {
	case RoleLord:
		return s.LordNkey
	case RoleThrall:
		return s.ThrallNkey
	case RoleOperator:
		return s.OperatorNkey
	default:
		return ""
	}
}

// clientOptions turns the TLS/auth config into nats options. An empty field adds no
// option, so a config without a security block connects exactly as before.
func (c Config) clientOptions() ([]nats.Option, error) {
	ca, seed := c.ClientCredentials()
	return ClientOptions(ca, seed)
}

// ClientCredentials returns the CA and nkey seed paths a client uses to reach this bus, from the
// right source for the mode: an external bus uses the client-side TLS/Auth fields; a secured
// embedded bus uses its own Security fields (the same identity the embedded server authorizes).
// An unsecured bus returns empty paths. It is the one place the lord (injecting into thralls) and
// the operator CLI (writing the endpoint file) derive credentials, so both stay consistent with
// however the bus is secured.
func (c Config) ClientCredentials() (ca, nkeySeed string) {
	if c.Security != nil {
		return c.Security.CA, c.Security.NkeySeed
	}
	return c.TLS.CA, c.Auth.NkeySeed
}

// ClientOptions builds the nats options for a secured bus from credential paths: a CA the
// client verifies the server against and an nkey seed it authenticates with. An empty path
// adds no option, so an empty pair connects unsecured. Shared by the lord (via Config) and the
// operator CLI, so every client constructs the same options in one place.
func ClientOptions(caPath, nkeySeed string) ([]nats.Option, error) {
	var opts []nats.Option
	if caPath != "" {
		opts = append(opts, nats.RootCAs(caPath))
	}
	if nkeySeed != "" {
		opt, err := nats.NkeyOptionFromSeed(nkeySeed)
		if err != nil {
			return nil, fmt.Errorf("nkey seed %q: %w", nkeySeed, err)
		}
		opts = append(opts, opt)
	}
	return opts, nil
}

// Validate rejects a semantically broken [nats] config. It runs after the manifest applies
// defaults (so an empty Mode has already become "embedded"), and is the single place the bus
// config's validity lives - the manifest validation calls it. A leaf makes sense only for the
// embedded spoke; the hub side is external, bring-your-own NATS config.
func (c Config) Validate() error {
	switch c.Mode {
	case "", "embedded", "external":
	default:
		return fmt.Errorf("nats: mode must be \"embedded\" or \"external\", got %q", c.Mode)
	}
	if c.Leaf != nil {
		if c.Mode == "external" {
			return fmt.Errorf("nats.leaf requires mode = \"embedded\" (the hub side is external, bring-your-own NATS config)")
		}
		if err := c.Leaf.validate(); err != nil {
			return err
		}
	}
	if c.Security != nil {
		if c.Mode == "external" {
			return fmt.Errorf("nats.security requires mode = \"embedded\" (it secures the embedded server; an external bus is operator-secured via nats.tls / nats.auth)")
		}
		if c.Leaf != nil {
			return fmt.Errorf("nats.security together with nats.leaf is not supported yet (securing the leaf spoke is a later increment)")
		}
		if err := c.Security.validate(); err != nil {
			return err
		}
	}
	return nil
}

// validate checks the leaf section's own invariants, independent of the surrounding mode. It lives
// on Leaf (not inline in Config.Validate) so leafOptions can re-run it: the builder is reachable
// directly, not only through the manifest, and its fields are rendered verbatim into a generated
// NATS config - so the config-injection guards must hold at the render site too, not on trust.
func (l *Leaf) validate() error {
	if l.Remote == "" {
		return fmt.Errorf("nats.leaf.remote is required (the hub's leafnode listener)")
	}
	if l.Site == "" {
		return fmt.Errorf("nats.leaf.site is required (the account this node binds to)")
	}
	if l.Domain == "" {
		return fmt.Errorf("nats.leaf.domain is required (JetStream domain, unique per node)")
	}
	// site and domain are rendered verbatim into the generated NATS config, so they must be plain
	// identifiers - a stray brace or space would corrupt the config, not just fail to match.
	if !isConfigIdent(l.Site) {
		return fmt.Errorf("nats.leaf.site %q must be a plain identifier (letters, digits, _ or -)", l.Site)
	}
	if !isConfigIdent(l.Domain) {
		return fmt.Errorf("nats.leaf.domain %q must be a plain identifier (letters, digits, _ or -)", l.Domain)
	}
	// SYS is the system account the leaf config always defines; a site of the same name would render
	// two SYS blocks and fail the server with an opaque error instead of this clear one.
	if l.Site == "SYS" {
		return fmt.Errorf("nats.leaf.site %q is reserved (the system account); pick another site name", l.Site)
	}
	// A password without a user cannot be applied to the leaf link; fail loudly rather than drop it.
	if l.Password != "" && l.User == "" {
		return fmt.Errorf("nats.leaf.password is set without nats.leaf.user")
	}
	return nil
}

// isConfigIdent reports whether s is a non-empty run of characters safe to render
// verbatim into a NATS config file (an account name, a JetStream domain).
func isConfigIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// Ether holds the running bus and the system connection for the lord and registry.
type Ether struct {
	mode      string
	srv       *natsserver.Server // embedded mode only
	nc        *nats.Conn
	url       string
	storeDir  string // JetStream storage (embedded)
	ephemeral bool   // true -> storeDir is a temp dir we own and remove on Stop
}

// StartOption tunes how the bus is brought up. Options carry runtime context the [nats]
// config alone does not - today the app name, which the leaf spoke exports as its data plane.
type StartOption func(*startOptions)

type startOptions struct {
	app string // the manifest's app namespace; exported over the leaf as aether.<app>.>
}

// WithApp supplies the manifest's app name. It is required for an embedded leaf spoke (the
// export subject aether.<app>.> is otherwise undefined) and ignored by the other modes.
func WithApp(app string) StartOption {
	return func(o *startOptions) { o.app = app }
}

// Start brings up an embedded NATS server (default), or connects to an external URL.
func Start(ctx context.Context, cfg Config, opts ...StartOption) (*Ether, error) {
	var so startOptions
	for _, opt := range opts {
		opt(&so)
	}
	switch cfg.Mode {
	case "", "embedded":
		return startEmbedded(ctx, cfg, so)
	case "external":
		return startExternal(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown nats mode %q", cfg.Mode)
	}
}

// resolveStoreDir returns the JetStream storage directory for the embedded server
// and whether it is an ephemeral temp dir we own. A configured StoreDir is kept
// across restarts (persistent durable mailbox); an empty one falls back to a temp
// dir removed on Stop (the ephemeral default).
func resolveStoreDir(cfg Config) (dir string, ephemeral bool, err error) {
	if cfg.StoreDir != "" {
		if err := os.MkdirAll(cfg.StoreDir, 0o755); err != nil {
			return "", false, fmt.Errorf("store_dir %q: %w", cfg.StoreDir, err)
		}
		return cfg.StoreDir, false, nil
	}
	dir, err = os.MkdirTemp("", "aether-js-")
	if err != nil {
		return "", false, err
	}
	return dir, true, nil
}

// embeddedOptions builds the NATS server options for the embedded bus. Without a leaf it is a
// standalone single-account server (today's behaviour); with [nats.leaf] it is a spoke bound
// into its site's account, exporting its app's data plane over a leaf link to the hub.
func embeddedOptions(cfg Config, so startOptions, storeDir string) (*natsserver.Options, error) {
	if cfg.Leaf != nil {
		return leafOptions(cfg.Leaf, so.app, storeDir)
	}
	if cfg.Security != nil {
		return securedServerOptions(cfg.Security, storeDir)
	}
	return &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,       // -1 = pick a free port
		JetStream: true,     // durable mailbox / KV registry
		StoreDir:  storeDir, // JetStream storage
		NoSigs:    true,     // our runtime handles signals; otherwise the server would call Shutdown()
		//        a second time concurrently with our eth.Stop() -> panic "close of nil channel".
	}, nil
}

func startEmbedded(_ context.Context, cfg Config, so startOptions) (*Ether, error) {
	storeDir, ephemeral, err := resolveStoreDir(cfg)
	if err != nil {
		return nil, err
	}
	// cleanup removes the store dir only when we own it (ephemeral temp), so a
	// failed start never deletes a user-supplied persistent directory.
	cleanup := func() {
		if ephemeral {
			os.RemoveAll(storeDir)
		}
	}
	opts, err := embeddedOptions(cfg, so, storeDir)
	if err != nil {
		cleanup()
		return nil, err
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		cleanup()
		return nil, err
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		cleanup()
		return nil, fmt.Errorf("embedded NATS did not start in time")
	}
	// When the bus is secured, the lord's own system connection must authenticate too - it is
	// just another client of its embedded server. Local processes (the lord here, thralls later)
	// dial a loopback-resolved URL: a wildcard bind (0.0.0.0) is not a dialable destination.
	url := srv.ClientURL()
	var connOpts []nats.Option
	if cfg.Security != nil {
		url = dialableURL(url)
		secure, err := ClientOptions(cfg.Security.CA, cfg.Security.NkeySeed)
		if err != nil {
			srv.Shutdown()
			cleanup()
			return nil, err
		}
		connOpts = append(connOpts, secure...)
	}
	nc, err := nats.Connect(url, connOpts...)
	if err != nil {
		srv.Shutdown()
		cleanup()
		return nil, err
	}
	return &Ether{mode: "embedded", srv: srv, nc: nc, url: url, storeDir: storeDir, ephemeral: ephemeral}, nil
}

func startExternal(_ context.Context, cfg Config) (*Ether, error) {
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	secure, err := cfg.clientOptions()
	if err != nil {
		return nil, err
	}
	opts = append(opts, secure...)

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, err
	}
	return &Ether{mode: "external", nc: nc, url: cfg.URL}, nil
}

// Conn returns the system NATS connection (lord, registry, observability).
func (e *Ether) Conn() *nats.Conn { return e.nc }

// URL returns the bus address; injected into thralls as AETHER_NATS_URL.
func (e *Ether) URL() string { return e.url }

// Stop closes the connection and (in embedded mode) stops the server. An ephemeral
// store dir is removed; a persistent one (configured store_dir) is kept so the
// durable mailbox survives to the next start.
func (e *Ether) Stop() {
	if e.nc != nil {
		_ = e.nc.Drain()
	}
	if e.srv != nil {
		e.srv.Shutdown()
		e.srv.WaitForShutdown()
	}
	if e.ephemeral && e.storeDir != "" {
		os.RemoveAll(e.storeDir)
	}
}
