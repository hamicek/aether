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
	Trace string `json:"trace,omitempty"`
	// Idem is an optional caller-supplied idempotency key. On an idempotent thrall a call/cast
	// carrying the same Idem is deduplicated: a duplicate cast is skipped and a duplicate call
	// returns the first reply. Empty = no explicit idempotency (the receiver falls back to ID,
	// which still catches an exact redelivery of the same envelope). See AE-077.
	Idem    string          `json:"idem,omitempty"`
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
	// Describe is the thrall's self-description (its operations, version and last failure). It is
	// optional so an older SDK - or a heartbeat that predates the feature - omits it entirely and
	// the lord's liveness path is unaffected. Static fields (ops, version) repeat on every beat by
	// design: at this runtime's scale (tens of processes) the bytes are negligible and it keeps the
	// lord self-healing (it always holds the current description, with no one-shot to miss).
	Describe *ThrallDescribe `json:"describe,omitempty"`
}

// ThrallDescribe is a thrall's self-report of its own contract and most recent failure, carried in
// the optional Describe field of a heartbeat. The SDKs derive it - the developer declares nothing
// beyond an optional Version - so the "a handler is just a map of functions" ergonomics stay intact.
//
// CallOps and CastOps are the handler-map keys, kept apart so an edge route (which is itself a call
// or a cast) can be validated against the right set. Version is the thrall's self-declared build,
// which travels with the code and so answers "what actually runs" more honestly than a manifest.
// LastError / LastErrorMs carry the reason and time of the most recent handler failure or
// escalation; both are empty until one happens and, like ProcessedTotal, reset on restart. Metadata
// is deliberately absent: it is a deployment fact the operator declares in the manifest, so the lord
// fills it in from the ThrallSpec rather than round-tripping it through the thrall.
type ThrallDescribe struct {
	CallOps     []string `json:"call_ops,omitempty"`
	CastOps     []string `json:"cast_ops,omitempty"`
	Version     string   `json:"version,omitempty"`
	LastError   string   `json:"last_error,omitempty"`
	LastErrorMs int64    `json:"last_error_ms,omitempty"`
}
