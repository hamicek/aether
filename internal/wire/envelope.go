// Package wire defines the envelope and subject conventions shared with the SDKs.
// The Envelope shape must stay in sync with the TS side (sdk/ts/src/envelope.ts).
package wire

import "encoding/json"

// Kind distinguishes the message type on the wire.
type Kind = string

const (
	KindCall  Kind = "call"
	KindCast  Kind = "cast"
	KindReply Kind = "reply"
	KindHB    Kind = "hb"
	KindCtl   Kind = "ctl"
)

// Envelope is the uniform JSON envelope for all communication in the ether.
type Envelope struct {
	V       int             `json:"v"`
	ID      string          `json:"id,omitempty"`
	Kind    Kind            `json:"kind"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Op      string          `json:"op,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Status  string          `json:"status,omitempty"` // reply: "ok" | "error"
	Error   *WireError      `json:"error,omitempty"`
	TS      int64           `json:"ts,omitempty"`
}

// WireError carries an error inside a reply message.
type WireError struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}
