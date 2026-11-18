package ingress

import (
	"errors"
	"flag"
	"io"
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
