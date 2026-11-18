package thrall

import (
	"errors"
	"fmt"

	"github.com/hamicek/aether/internal/wire"
)

// RefuseError is the typed "no" of a handler: the request is understood and the thrall is healthy,
// but a rule of its domain forbids what was asked (not enough funds, the day is already closed).
// A handler returns it via Refuse. It is the third outcome beside a plain error ("this request
// failed") and Escalate ("crash me"), and the only one of the three that is not a failure:
//
//   - the caller gets an error reply of type ErrRefused carrying Code and Message, which its SDK
//     surfaces as a typed error (in Go a *CallError with Refused() true);
//   - the thrall's state is left as it was, exactly as for a plain error;
//   - nothing is recorded as the thrall's last error, and the refusal counts as a handled message.
//
// Code is a stable, machine-readable identifier that callers branch on, so it is part of the
// thrall's contract: name it for the rule ("insufficient_funds"), never reword it. Message is for
// people and may change freely.
type RefuseError struct {
	Code    string
	Message string
}

func (e *RefuseError) Error() string { return "refused (" + e.Code + "): " + e.Message }

// UnspecifiedRefusal is the code of a refusal made without one. A refusal always carries a code -
// that is what tells a caller "the thrall said no" from "the call failed" (RefusalCode returns ""
// only for the latter) - so Refuse("", ...) is given this one rather than an empty code.
const UnspecifiedRefusal = "refused"

// Refuse builds the refusal for a handler to return. code should name the rule; an empty code
// becomes UnspecifiedRefusal.
//
// A refusal is never forwarded implicitly. A handler that calls another thrall and returns that
// call's error gets a plain handler failure even when the other thrall refused, because a code
// from someone else's contract must not pose as this thrall's answer. To pass a downstream refusal
// on, say so: if r, ok := thrall.AsRefusal(err); ok { return nil, s, thrall.Refuse(r.Code, r.Message) }.
func Refuse(code, message string) error {
	if code == "" {
		code = UnspecifiedRefusal
	}
	return &RefuseError{Code: code, Message: message}
}

// Refusef is Refuse with a formatted message.
func Refusef(code, format string, args ...any) error {
	return Refuse(code, fmt.Sprintf(format, args...))
}

// asRefuse reports whether err is (or wraps) a refusal, returning it for its code and message.
func asRefuse(err error) (*RefuseError, bool) {
	var ref *RefuseError
	if errors.As(err, &ref) {
		return ref, true
	}
	return nil, false
}

// refusedReply is the error reply that carries a refusal to the caller.
func refusedReply(req wire.Envelope, ref *RefuseError) wire.Envelope {
	return ErrorReply(req, wire.WireError{Type: wire.ErrRefused, Code: ref.Code, Message: ref.Message})
}
