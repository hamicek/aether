package thrall_test

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/sdk/go/thrall"
)

// A thrall definition without an Init must be rejected with a clear error before any
// connection is attempted - not crash with a nil pointer dereference (SIGSEGV).
func TestStartRequiresInit(t *testing.T) {
	t.Setenv("AETHER_NATS_URL", "nats://127.0.0.1:1") // never dialled: the guard fires first
	t.Setenv("AETHER_APP", "test")

	err := thrall.Start(thrall.Def[int]{Name: "no-init"})
	if err == nil {
		t.Fatal("expected an error for a nil Init, got nil")
	}
	if !strings.Contains(err.Error(), "Init is required") {
		t.Fatalf("expected an %q error, got: %v", "Init is required", err)
	}
}
