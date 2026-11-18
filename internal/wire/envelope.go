// Package wire defines the envelope and subject conventions shared with the SDKs.
// The Envelope shape must stay in sync with the TS side (sdk/ts/src/envelope.ts).
package wire

import (
	"encoding/json"
	"time"
)

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
	// Deadline is the wall-clock instant (Unix milliseconds) at which the caller of a call stops
	// waiting for the reply: the send time plus its timeout. A thrall drops a call whose deadline
	// has passed before it starts handling it, so a request that timed out on the caller's side is
	// not executed later from the mailbox. Zero = no deadline (an older caller, or any message
	// that is not a call): the message is handled whenever it is dequeued, as before. It is
	// compared against the receiver's clock, hence the skew tolerance - see Expired.
	Deadline int64 `json:"deadline,omitempty"`
}

// WireError carries an error inside a reply message.
type WireError struct {
	// Type classifies the error for the caller; the Err* constants below are the types the
	// runtime itself produces.
	Type string `json:"type"`
	// Code is the stable machine-readable code of a refusal (Type == ErrRefused), chosen by the
	// refusing thrall and part of its contract. Empty for every other type, and omitted then, so
	// an error reply from an older thrall and a non-refusal reply keep their previous shape.
	Code      string `json:"code,omitempty"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// Reply statuses.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Error types the runtime puts into WireError.Type. An FSM additionally answers "no_transition"
// and "guard_rejected" for an event its current state does not act on.
const (
	// ErrHandler: the handler returned a plain error - the thrall is alive but this request failed.
	ErrHandler = "handler_error"
	// ErrUnknownOp: the thrall has no call handler for the op.
	ErrUnknownOp = "unknown_op"
	// ErrEscalated: the handler asked to be crashed; the thrall restarts after replying.
	ErrEscalated = "escalated"
	// ErrRefused: the thrall deliberately declined the request (a domain rule said no). It is a
	// normal outcome, not a failure of the thrall; Code says which rule.
	ErrRefused = "refused"
	// ErrDeadlineExceeded: the call's deadline had passed before the thrall could start handling
	// it, so it was dropped unexecuted. Retryable, because nothing happened.
	ErrDeadlineExceeded = "deadline_exceeded"
	// ErrReplyTooLarge: the handler ran, but its reply is larger than one bus message carries and
	// could not be sent; the caller gets this instead of waiting out its timeout. Not retryable:
	// the same request produces the same reply.
	ErrReplyTooLarge = "reply_too_large"
)

// DefaultDeadlineSkewMs is how far past a call's deadline the receiver's clock may read before the
// call is dropped as expired. The deadline is stamped by the caller's clock and judged by the
// callee's, so the tolerance has to absorb the difference between the two. On one host they are the
// same clock; across nodes kept by NTP the difference is tens of milliseconds, which this covers
// with a wide margin. The price of the margin is that a call which expired less than the tolerance
// ago is still executed - small next to the window a deadline cannot close at all (a handler that
// started just before the deadline), which is what an idempotency key is for.
//
// The failure modes of a clock that is off by more than the tolerance are asymmetric and safe: a
// callee running ahead refuses live calls immediately with ErrDeadlineExceeded (loud, nothing
// executed, retryable); a callee running behind executes an expired call, which is simply the
// behavior without a deadline.
const DefaultDeadlineSkewMs = 250

// EnvDeadlineSkewMs overrides DefaultDeadlineSkewMs for a thrall process. A thrall inherits the
// lord's environment, so setting it on the lord applies to every thrall it spawns. 0 = judge the
// deadline exactly (sound when every caller shares the thrall's clock).
const EnvDeadlineSkewMs = "AETHER_DEADLINE_SKEW_MS"

// DeadlineAfter is the deadline a caller stamps on a call it will wait timeout for, starting now.
// A non-positive timeout yields 0 (no deadline), never an already expired one.
func DeadlineAfter(now time.Time, timeout time.Duration) int64 {
	if timeout <= 0 {
		return 0
	}
	return now.Add(timeout).UnixMilli()
}

// Expired reports whether the envelope's deadline has passed at now by more than skew, and by how
// much it is overdue. An envelope without a deadline never expires.
func (e Envelope) Expired(now time.Time, skew time.Duration) (overdue time.Duration, expired bool) {
	if e.Deadline <= 0 {
		return 0, false
	}
	overdue = time.Duration(now.UnixMilli()-e.Deadline) * time.Millisecond
	return overdue, overdue > skew
}

// HeartbeatMetrics are a thrall's self-reported runtime samples, carried in the payload of a
// heartbeat envelope. The lord aggregates them into the /metrics exposition. Field order is
// part of the wire contract - the TS and Python SDKs mirror it exactly.
//
// mailbox_depth is the number of messages the thrall currently holds (received, handler not
// yet returned); mailbox_latency_ms is the duration of the most recent handler (including the
// wait for the serialized mailbox lock); processed_total is the cumulative count of handled
// messages since the thrall started (it resets on restart, like any process-local counter).
// expired_total is the cumulative count of calls dropped unexecuted because their deadline had
// passed; such a call is not counted in processed_total. It is omitted while zero, so the payload
// of a thrall that never dropped a call is unchanged from before the field existed.
//
// in_flight is the number of handlers running at this instant. mailbox_depth counts every message
// received and not yet finished, so mailbox_depth - in_flight is what is waiting for its turn. A
// serialized thrall reports 0 or 1 here; a concurrent web thrall up to its concurrency (see
// ThrallDescribe.Concurrency). Omitted while zero.
type HeartbeatMetrics struct {
	MailboxDepth     int     `json:"mailbox_depth"`
	MailboxLatencyMs float64 `json:"mailbox_latency_ms"`
	ProcessedTotal   uint64  `json:"processed_total"`
	ExpiredTotal     uint64  `json:"expired_total,omitempty"`
	InFlight         int     `json:"in_flight,omitempty"`
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
//
// Concurrency is how many handlers the thrall may run at once. It is omitted by a serialized
// thrall (the GenServer default: one message at a time) and set only by a web thrall that serves
// requests concurrently, so its presence is what tells an operator that the thrall's handlers
// interleave.
type ThrallDescribe struct {
	CallOps     []string `json:"call_ops,omitempty"`
	CastOps     []string `json:"cast_ops,omitempty"`
	Version     string   `json:"version,omitempty"`
	LastError   string   `json:"last_error,omitempty"`
	LastErrorMs int64    `json:"last_error_ms,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
}
