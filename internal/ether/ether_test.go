package ether_test

import (
	"context"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/natstest"
)

// TestExternalSecuredConnect proves the external connect authenticates to a secured
// bus (nkey + server TLS) and round-trips a message, while a connect missing either
// the nkey or the CA is refused.
func TestExternalSecuredConnect(t *testing.T) {
	sec := natstest.SecuredServer(t)

	t.Run("with CA and nkey connects and round-trips", func(t *testing.T) {
		eth, err := ether.Start(context.Background(), ether.Config{
			Mode: "external",
			URL:  sec.URL,
			TLS:  ether.TLS{CA: sec.CAFile},
			Auth: ether.Auth{NkeySeed: sec.SeedFile},
		})
		if err != nil {
			t.Fatalf("secured connect: %v", err)
		}
		defer eth.Stop()

		nc := eth.Conn()
		sub, err := nc.SubscribeSync("secure.ping")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := nc.Publish("secure.ping", []byte("hi")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("round-trip over secured bus: %v", err)
		}
		if got := string(msg.Data); got != "hi" {
			t.Fatalf("payload = %q, want %q", got, "hi")
		}
	})

	t.Run("without nkey is refused", func(t *testing.T) {
		eth, err := ether.Start(context.Background(), ether.Config{
			Mode: "external",
			URL:  sec.URL,
			TLS:  ether.TLS{CA: sec.CAFile},
		})
		if err == nil {
			eth.Stop()
			t.Fatal("expected connect to be refused without an nkey, but it succeeded")
		}
	})

	t.Run("without CA is refused", func(t *testing.T) {
		eth, err := ether.Start(context.Background(), ether.Config{
			Mode: "external",
			URL:  sec.URL,
			Auth: ether.Auth{NkeySeed: sec.SeedFile},
		})
		if err == nil {
			eth.Stop()
			t.Fatal("expected connect to be refused without the CA, but it succeeded")
		}
	})
}

// TestClientOptions covers the shared options builder directly: an empty pair adds no
// option, an invalid nkey seed path is an error, and valid paths yield options.
func TestClientOptions(t *testing.T) {
	t.Run("empty pair adds no option", func(t *testing.T) {
		opts, err := ether.ClientOptions("", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(opts) != 0 {
			t.Fatalf("expected no options for an empty pair, got %d", len(opts))
		}
	})

	t.Run("invalid nkey seed is an error", func(t *testing.T) {
		if _, err := ether.ClientOptions("", "/does/not/exist.nk"); err == nil {
			t.Fatal("expected an error for an unreadable nkey seed, got nil")
		}
	})

	t.Run("valid credentials yield options", func(t *testing.T) {
		sec := natstest.SecuredServer(t)
		opts, err := ether.ClientOptions(sec.CAFile, sec.SeedFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(opts) != 2 {
			t.Fatalf("expected CA + nkey options, got %d", len(opts))
		}
	})
}
