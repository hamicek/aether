package ingress

import (
	"errors"
	"flag"
	"io"
	"net/netip"
	"testing"
	"time"
)

// TestParseFlagsAcceptsDeployedCommandLine pins the command line the deploy tooling generates today
// (registry mode behind a reverse proxy). Whatever else the ingress grows, this line has to keep
// parsing to the same meaning - both build paths share ParseFlags, so it guards them together.
func TestParseFlagsAcceptsDeployedCommandLine(t *testing.T) {
	cfg, err := ParseFlags([]string{
		"-addr", "127.0.0.1:7462", "-registry", "-registry-scope", "public", "-registry-ttl", "2s",
	}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.Addr != "127.0.0.1:7462" || !cfg.UseRegistry || cfg.RegistryScope != "public" || cfg.RegistryTTL != 2*time.Second {
		t.Errorf("deployed command line parsed to %+v", cfg)
	}
	if cfg.RegistrySubject != DefaultRegistrySubject {
		t.Errorf("registry subject = %q, want the family default", cfg.RegistrySubject)
	}
	if cfg.RegistryStale != DefaultRegistryStale || cfg.RegistryCache != DefaultRegistryCacheSize {
		t.Errorf("registry defaults: stale %s, cache %d", cfg.RegistryStale, cfg.RegistryCache)
	}
	// A command line that names no limit runs with every limit at its safe default.
	want := Config{
		Timeout: DefaultTimeout, ReadHeaderTimeout: DefaultReadHeaderTimeout, ReadTimeout: DefaultReadTimeout,
		WriteTimeout: DefaultWriteTimeout, IdleTimeout: DefaultIdleTimeout,
		MaxConns: DefaultMaxConns, MaxHeaderBytes: DefaultMaxHeaderBytes,
	}
	if cfg.Timeout != want.Timeout || cfg.ReadHeaderTimeout != want.ReadHeaderTimeout || cfg.ReadTimeout != want.ReadTimeout ||
		cfg.WriteTimeout != want.WriteTimeout || cfg.IdleTimeout != want.IdleTimeout ||
		cfg.MaxConns != want.MaxConns || cfg.MaxHeaderBytes != want.MaxHeaderBytes || cfg.MaxBodyBytes != 0 {
		t.Errorf("default limits = %+v", cfg)
	}
}

func TestParseFlagsLimits(t *testing.T) {
	cfg, err := ParseFlags([]string{
		"-timeout", "8s", "-read-header-timeout", "2s", "-read-timeout", "10s", "-write-timeout", "11s",
		"-idle-timeout", "1m", "-max-conns", "32", "-max-header-bytes", "16384", "-max-body-bytes", "4096",
	}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.Timeout != 8*time.Second || cfg.ReadHeaderTimeout != 2*time.Second || cfg.ReadTimeout != 10*time.Second ||
		cfg.WriteTimeout != 11*time.Second || cfg.IdleTimeout != time.Minute ||
		cfg.MaxConns != 32 || cfg.MaxHeaderBytes != 16384 || cfg.MaxBodyBytes != 4096 {
		t.Errorf("limits parsed to %+v", cfg)
	}

	// A limit can be tuned, not switched off.
	for _, args := range [][]string{
		{"-timeout", "0"}, {"-read-header-timeout", "0"}, {"-read-timeout", "-1s"}, {"-write-timeout", "0"},
		{"-idle-timeout", "0"}, {"-max-conns", "0"}, {"-max-header-bytes", "0"}, {"-max-body-bytes", "-1"},
		{"-registry-ttl", "0"}, {"-registry-stale", "-1s"}, {"-registry-cache-size", "0"},
	} {
		if _, err := ParseFlags(args, io.Discard); err == nil {
			t.Errorf("%v must be rejected", args)
		}
	}
}

func TestParseFlagsRegistryStaleCanBeSwitchedOff(t *testing.T) {
	cfg, err := ParseFlags([]string{"-registry", "-registry-stale", "0", "-registry-cache-size", "16"}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.RegistryStale != 0 || cfg.RegistryCache != 16 {
		t.Errorf("parsed to stale %s, cache %d", cfg.RegistryStale, cfg.RegistryCache)
	}
}

func TestParseFlagsTrustedProxies(t *testing.T) {
	loopback := netip.MustParseAddr("127.0.0.1")
	bridge := netip.MustParseAddr("172.17.0.1")

	cfg, err := ParseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if !cfg.TrustedProxies.contains(loopback) || !cfg.TrustedProxies.contains(netip.MustParseAddr("::1")) || cfg.TrustedProxies.contains(bridge) {
		t.Errorf("default trust = %v, want loopback only", cfg.TrustedProxies)
	}

	cfg, err = ParseFlags([]string{"-trusted-proxies", "172.17.0.0/16, 10.1.2.3"}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if !cfg.TrustedProxies.contains(bridge) || !cfg.TrustedProxies.contains(netip.MustParseAddr("10.1.2.3")) || cfg.TrustedProxies.contains(loopback) {
		t.Errorf("explicit trust = %v, want exactly what was listed", cfg.TrustedProxies)
	}

	cfg, err = ParseFlags([]string{"-trusted-proxies", ""}, io.Discard)
	if err != nil || len(cfg.TrustedProxies) != 0 {
		t.Errorf("an empty list must trust nobody: %v, %v", cfg.TrustedProxies, err)
	}
	if _, err := ParseFlags([]string{"-trusted-proxies", "caddy.internal"}, io.Discard); err == nil {
		t.Error("a trusted proxy that is not an address must be rejected")
	}
}

func TestParseFlagsStaticRoutes(t *testing.T) {
	cfg, err := ParseFlags([]string{"-addr", ":7391", "-routes", "routes.json"}, io.Discard)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.UseRegistry || cfg.RoutesPath != "routes.json" {
		t.Errorf("static command line parsed to %+v", cfg)
	}
}

func TestParseFlagsRejectsUnknownFlagAndReportsHelp(t *testing.T) {
	if _, err := ParseFlags([]string{"-no-such-flag"}, io.Discard); err == nil {
		t.Error("an unknown flag must be an error")
	}
	if _, err := ParseFlags([]string{"-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h = %v, want flag.ErrHelp", err)
	}
}
