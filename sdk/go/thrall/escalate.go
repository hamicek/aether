package thrall

import (
	"errors"
	"os"
)

// exitProcess terminates the thrall process. It is a package var so dispatch tests can
// observe escalation without killing the test binary; production uses os.Exit.
var exitProcess = os.Exit

// EscalateError is the typed "let it crash" signal. A handler that returns it (via Escalate)
// asks the runtime to terminate the thrall with an abnormal exit, so the lord restarts it
// through init per the thrall's restart policy - real OTP semantics without a manual
// panic/os.Exit in application code. A plain error keeps its old meaning: reply the caller an
// error (call) or log it (cast), and keep living.
type EscalateError struct {
	Reason string
}

func (e *EscalateError) Error() string { return "escalate: " + e.Reason }

// Escalate builds the typed crash signal for a handler to return. The reason is surfaced to a
// call caller (as the "escalated" error reply) and logged locally before the process exits.
func Escalate(reason string) error {
	return &EscalateError{Reason: reason}
}

// asEscalate reports whether err is (or wraps) an escalation, returning the signal for its reason.
func asEscalate(err error) (*EscalateError, bool) {
	var esc *EscalateError
	if errors.As(err, &esc) {
		return esc, true
	}
	return nil, false
}
