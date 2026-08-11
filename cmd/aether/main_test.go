package main

import (
	"os"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/natstest"
)

// TestDial proves the CLI authenticates to a secured bus (server TLS + nkey) when given the
// credentials, and is refused without them (no silent fallback to unsecured).
func TestDial(t *testing.T) {
	sec := natstest.SecuredServer(t)

	t.Run("with credentials connects and round-trips", func(t *testing.T) {
		nc, err := dial(endpoint{URL: sec.URL, CA: sec.CAFile, NkeySeed: sec.SeedFile})
		if err != nil {
			t.Fatalf("dial with credentials: %v", err)
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync("cli.ping")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		_ = nc.Flush()
		if err := nc.Publish("cli.ping", []byte("hi")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		_ = nc.Flush()
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("no round-trip: %v", err)
		}
		if string(msg.Data) != "hi" {
			t.Fatalf("round-trip payload = %q, want %q", msg.Data, "hi")
		}
	})

	t.Run("without credentials is refused", func(t *testing.T) {
		if nc, err := dial(endpoint{URL: sec.URL}); err == nil {
			nc.Close()
			t.Fatal("expected connect to be refused without credentials, but it succeeded")
		}
	})

	t.Run("CA without nkey is refused", func(t *testing.T) {
		if nc, err := dial(endpoint{URL: sec.URL, CA: sec.CAFile}); err == nil {
			nc.Close()
			t.Fatal("expected connect to be refused without an nkey, but it succeeded")
		}
	})
}

// TestResolveEndpointPrecedence checks each credential field layers flag > .aether-endpoint > env.
func TestResolveEndpointPrecedence(t *testing.T) {
	dir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	t.Setenv("AETHER_NATS_CA", "envCA")
	t.Setenv("AETHER_NATS_NKEY_SEED", "envSeed")

	// The endpoint file carries a URL (so resolveEndpoint does not fatal) and file-level creds.
	writeEndpoint(endpoint{URL: "nats://file:4222", App: "a", CA: "fileCA", NkeySeed: "fileSeed"})

	// A flag wins over the file and env.
	if ep := resolveEndpoint("", "", "flagCA", "flagSeed"); ep.CA != "flagCA" || ep.NkeySeed != "flagSeed" {
		t.Fatalf("flag should win: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}

	// No flag: the file wins over env.
	if ep := resolveEndpoint("", "", "", ""); ep.CA != "fileCA" || ep.NkeySeed != "fileSeed" {
		t.Fatalf("file should win over env: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}

	// No flag and the file omits creds: env fills in.
	writeEndpoint(endpoint{URL: "nats://file:4222", App: "a"})
	if ep := resolveEndpoint("", "", "", ""); ep.CA != "envCA" || ep.NkeySeed != "envSeed" {
		t.Fatalf("env should fill when the file omits creds: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}
}
