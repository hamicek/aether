// Package ether manages the bus (the ether): an embedded NATS server, or a
// connection to an external cluster. The interface is the same for both modes -
// neither thrall nor lord can tell the difference, because both speak only NATS.
package ether

import (
	"context"
	"fmt"
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

// clientOptions turns the TLS/auth config into nats options. An empty field adds no
// option, so a config without a security block connects exactly as before.
func (c Config) clientOptions() ([]nats.Option, error) {
	var opts []nats.Option
	if c.TLS.CA != "" {
		opts = append(opts, nats.RootCAs(c.TLS.CA))
	}
	if c.Auth.NkeySeed != "" {
		opt, err := nats.NkeyOptionFromSeed(c.Auth.NkeySeed)
		if err != nil {
			return nil, fmt.Errorf("nkey seed %q: %w", c.Auth.NkeySeed, err)
		}
		opts = append(opts, opt)
	}
	return opts, nil
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

// Start brings up an embedded NATS server (default), or connects to an external URL.
func Start(ctx context.Context, cfg Config) (*Ether, error) {
	switch cfg.Mode {
	case "", "embedded":
		return startEmbedded(ctx, cfg)
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

func startEmbedded(_ context.Context, cfg Config) (*Ether, error) {
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
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,       // -1 = pick a free port
		JetStream: true,     // durable mailbox / KV registry
		StoreDir:  storeDir, // JetStream storage
		NoSigs:    true,     // our runtime handles signals; otherwise the server would call Shutdown()
		//        a second time concurrently with our eth.Stop() -> panic "close of nil channel".
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
	url := srv.ClientURL()
	nc, err := nats.Connect(url)
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
