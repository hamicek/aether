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
	V  int    `json:"v"`
	ID string `json:"id,omitempty"`
	// Trace is a correlation id propagated across hops (call/cast chains), distinct from ID
	// which correlates a single request with its reply. It lets one logical operation be
	// followed across process boundaries; an edge (CLI, first message) mints it, handlers pass
	// it to downstream calls, and it is logged so a trace and a log line can be joined.
	Trace   string          `json:"trace,omitempty"`
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

// HeartbeatMetrics are a thrall's self-reported runtime samples, carried in the payload of a
// heartbeat envelope. The lord aggregates them into the /metrics exposition. Field order is
// part of the wire contract - the TS and Python SDKs mirror it exactly.
//
// mailbox_depth is the number of messages the thrall currently holds (received, handler not
// yet returned); mailbox_latency_ms is the duration of the most recent handler (including the
// wait for the serialized mailbox lock); processed_total is the cumulative count of handled
// messages since the thrall started (it resets on restart, like any process-local counter).
type HeartbeatMetrics struct {
	MailboxDepth     int     `json:"mailbox_depth"`
	MailboxLatencyMs float64 `json:"mailbox_latency_ms"`
	ProcessedTotal   uint64  `json:"processed_total"`
}
