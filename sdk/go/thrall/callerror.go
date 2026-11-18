package thrall

import (
	"errors"

	"github.com/hamicek/aether/internal/wire"
)

// CallError is the error a call returns when the target answered with an error reply. It carries
// the reply's error as the target sent it, so a caller can tell the outcomes apart instead of
// matching error text:
//
//   - Type ErrRefused: the thrall declined the request by a rule of its domain. Code is the
//     thrall's stable machine code for the rule and Message the human sentence. This is a normal
//     answer, not a fault.
//   - Type ErrHandler / ErrUnknownOp / ErrEscalated: the request failed in the thrall.
//   - Type ErrDeadlineExceeded: the thrall got to the call after its deadline and dropped it
//     unexecuted (Retryable is true).
//   - Type ErrReplyTooLarge: the handler ran, but its reply did not fit one bus message.
//
// A thrall built with an SDK that predates refusals reports everything as ErrHandler with an empty
// Code. A call that got no reply at all does not produce a CallError but the NATS error
// (nats.ErrTimeout, nats.ErrNoResponders).
type CallError struct {
	Type      string
	Code      string
	Message   string
	Retryable bool
}

// Error renders "<type>: <message>", the text a failed call has always had, so code that logs or
// compares the string keeps working. The code is a field, not part of the text.
func (e *CallError) Error() string { return e.Type + ": " + e.Message }

// Refused reports whether the target declined the request (as opposed to failing at it).
func (e *CallError) Refused() bool { return e.Type == wire.ErrRefused }

// AsRefusal returns the refusal behind err (looking through wrapping), or false when err is not a
// refusal - a failure of the thrall, a transport error, or nil.
func AsRefusal(err error) (*CallError, bool) {
	var ce *CallError
	if errors.As(err, &ce) && ce.Refused() {
		return ce, true
	}
	return nil, false
}

// RefusalCode returns the code of the refusal behind err, or "" when err is not a refusal. It makes
// mapping refusals to outcomes a plain switch or table lookup on the code.
func RefusalCode(err error) string {
	if ce, ok := AsRefusal(err); ok {
		return ce.Code
	}
	return ""
}

// callErrorFrom turns the error of an error reply into a CallError. A reply with status "error"
// but no error object is malformed; it still has to surface as an error, typed as a handler
// failure since nothing says otherwise.
func callErrorFrom(we *wire.WireError) *CallError {
	if we == nil {
		return &CallError{Type: wire.ErrHandler, Message: "error reply without detail"}
	}
	return &CallError{Type: we.Type, Code: we.Code, Message: we.Message, Retryable: we.Retryable}
}
