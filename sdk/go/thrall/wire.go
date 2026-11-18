package thrall

// The wire contract, re-exported from internal/wire so code outside this module (a library, an
// application, a test double standing in for a thrall) can name it without mirroring it. These
// ARE the wire types and values - aliases, not copies - so there stays one source of truth. To
// call a thrall use a Client; what is here is for the rarer cases that work at the envelope level:
// answering as a thrall in a test, or bridging the ether to another transport.

import (
	"encoding/json"
	"fmt"

	"github.com/hamicek/aether/internal/wire"
)

// Envelope is the uniform JSON envelope of every message in the ether; WireError is the error
// inside an error reply.
type (
	Envelope  = wire.Envelope
	WireError = wire.WireError
)

// Message kinds (Envelope.Kind).
const (
	KindCall  = wire.KindCall
	KindCast  = wire.KindCast
	KindReply = wire.KindReply
)

// Reply statuses (Envelope.Status).
const (
	StatusOK    = wire.StatusOK
	StatusError = wire.StatusError
)

// Error types of an error reply (WireError.Type, CallError.Type).
const (
	ErrHandler          = wire.ErrHandler
	ErrUnknownOp        = wire.ErrUnknownOp
	ErrEscalated        = wire.ErrEscalated
	ErrRefused          = wire.ErrRefused
	ErrDeadlineExceeded = wire.ErrDeadlineExceeded
	ErrReplyTooLarge    = wire.ErrReplyTooLarge
)

// CallSubject is the subject a thrall receives calls on: aether.<app>.<name>.call.
func CallSubject(app, name string) string { return wire.Call(app, name) }

// CastSubject is the subject a thrall receives casts on: aether.<app>.<name>.cast.
func CastSubject(app, name string) string { return wire.Cast(app, name) }

// OKReply builds the successful reply to a call envelope. The payload is marshalled to JSON; a
// value that cannot be marshalled yields a reply with an empty payload.
func OKReply(req Envelope, payload any) Envelope {
	return Envelope{V: 1, ID: req.ID, Kind: wire.KindReply, Status: wire.StatusOK, Payload: mustMarshal(payload)}
}

// ErrorReply builds the error reply to a call envelope, carrying werr as given - including the
// Code of a refusal (Type ErrRefused).
func ErrorReply(req Envelope, werr WireError) Envelope {
	return Envelope{V: 1, ID: req.ID, Kind: wire.KindReply, Status: wire.StatusError, Error: &werr}
}

func okReply(req wire.Envelope, payload any) wire.Envelope { return OKReply(req, payload) }

func errReply(req wire.Envelope, typ, message string) wire.Envelope {
	return ErrorReply(req, wire.WireError{Type: typ, Message: message})
}

// tooLargeReply builds the reply sent in place of one that did not fit a bus message. The text is
// part of the wire contract's golden fixtures, so every SDK words it the same way.
func tooLargeReply(req wire.Envelope, size int, maxPayload int64) wire.Envelope {
	return errReply(req, wire.ErrReplyTooLarge,
		fmt.Sprintf("reply of %d bytes does not fit one bus message (at most %d bytes)", size, maxPayload))
}

func mustJSON(e wire.Envelope) []byte {
	b, _ := json.Marshal(e)
	return b
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
