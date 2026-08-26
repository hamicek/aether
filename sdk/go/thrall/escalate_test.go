package thrall

import (
	"errors"
	"fmt"
	"testing"
)

// Escalate carries its reason and is recognized as an escalation - the typed "let it crash"
// signal a handler returns instead of a plain error. The full dispatch -> reply -> exit ->
// restart behavior is proven end-to-end in internal/lord (a real re-exec'd thrall process).
func TestEscalateCarriesReason(t *testing.T) {
	err := Escalate("mailbox poisoned")

	esc, ok := asEscalate(err)
	if !ok {
		t.Fatalf("asEscalate did not recognize an escalation: %v", err)
	}
	if esc.Reason != "mailbox poisoned" {
		t.Fatalf("reason = %q, want %q", esc.Reason, "mailbox poisoned")
	}
	if got := err.Error(); got != "escalate: mailbox poisoned" {
		t.Fatalf("Error() = %q, want %q", got, "escalate: mailbox poisoned")
	}
}

// An escalation stays recognizable through error wrapping (errors.As), so a handler may add
// context with %w and still ask for a crash.
func TestEscalateThroughWrap(t *testing.T) {
	wrapped := fmt.Errorf("while handling deposit: %w", Escalate("balance underflow"))

	esc, ok := asEscalate(wrapped)
	if !ok {
		t.Fatalf("asEscalate did not see through the wrap: %v", wrapped)
	}
	if esc.Reason != "balance underflow" {
		t.Fatalf("reason = %q, want %q", esc.Reason, "balance underflow")
	}
}

// A plain error is not an escalation: it keeps its old meaning (reply the caller an error /
// log a cast), so dispatch must not mistake it for a crash request.
func TestPlainErrorIsNotEscalate(t *testing.T) {
	if _, ok := asEscalate(errors.New("just a handler error")); ok {
		t.Fatal("a plain error was misread as an escalation")
	}
	if _, ok := asEscalate(nil); ok {
		t.Fatal("nil was misread as an escalation")
	}
}
