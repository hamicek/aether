// Envelope = the uniform JSON envelope for all communication in the ether.
// Must stay in sync with the Go side (internal/wire/envelope.go).

export type Kind = "call" | "cast" | "reply" | "hb" | "ctl";

export interface Envelope {
  v: 1;
  id?: string;
  // Correlation id propagated across hops (call/cast chains), distinct from id (which
  // correlates a request with its reply). Mirrors wire.Envelope.Trace on the Go side.
  trace?: string;
  // Optional caller-supplied idempotency key. On an idempotent thrall a call/cast carrying the
  // same idem is deduplicated (duplicate cast skipped, duplicate call returns the first reply).
  // Empty = no explicit idempotency. Mirrors wire.Envelope.Idem on the Go side. See AE-077.
  idem?: string;
  kind: Kind;
  from?: string;
  to?: string;
  op?: string;
  payload?: unknown;
  status?: "ok" | "error";
  error?: WireError;
  ts?: number;
  // The instant (Unix ms) at which the caller of a call stops waiting: its send time plus its
  // timeout. A thrall drops a call whose deadline has passed before it starts handling it. Absent
  // = no deadline (an older caller, or any message that is not a call). Mirrors
  // wire.Envelope.Deadline on the Go side; see deadline.ts.
  deadline?: number;
}

export interface WireError {
  // Classifies the error for the caller; ErrorType lists the types the runtime itself produces.
  type: string;
  // The stable machine-readable code of a refusal (type "refused"), chosen by the refusing thrall
  // and part of its contract. Absent for every other type, so an error reply from an older thrall
  // and a non-refusal reply keep their previous shape. Mirrors wire.WireError.Code.
  code?: string;
  message: string;
  retryable: boolean;
}

// ErrorType = the error types the runtime puts into WireError.type (mirrors the wire.Err*
// constants). An FSM additionally answers "no_transition" and "guard_rejected".
export const ErrorType = {
  // The handler threw a plain error - the thrall is alive but this request failed.
  handler: "handler_error",
  // The thrall has no call handler for the op.
  unknownOp: "unknown_op",
  // The handler asked to be crashed; the thrall restarts after replying.
  escalated: "escalated",
  // The thrall deliberately declined the request (a domain rule said no): a normal outcome, not a
  // failure of the thrall. WireError.code says which rule.
  refused: "refused",
  // The call's deadline had passed before the thrall could start handling it, so it was dropped
  // unexecuted. Retryable, because nothing happened.
  deadlineExceeded: "deadline_exceeded",
  // The handler ran, but its reply is larger than one bus message carries and could not be sent;
  // the caller gets this instead of waiting out its timeout. Not retryable.
  replyTooLarge: "reply_too_large",
} as const;

// ThrallDescribe = a thrall's self-report of its own contract and most recent failure, carried in
// the optional `describe` field of a heartbeat payload. Mirrors wire.ThrallDescribe on the Go side.
// call_ops / cast_ops are the handler-map keys (kept apart so an edge route can be validated against
// the right set); version is the thrall's self-declared build; last_error / last_error_ms carry the
// reason and time of the most recent handler failure (empty until one happens). Fields are optional
// so an empty value is omitted, matching the Go omitempty tags. Metadata is not here: it is a
// deployment fact the lord fills in from the manifest, not something the thrall reports.
// concurrency is how many handlers the thrall may run at once: omitted by a serialized thrall, set
// only by a web thrall that serves requests concurrently.
export interface ThrallDescribe {
  call_ops?: string[];
  cast_ops?: string[];
  version?: string;
  last_error?: string;
  last_error_ms?: number;
  concurrency?: number;
}

// HeartbeatMetrics = a thrall's self-reported runtime samples, the payload of a heartbeat. Mirrors
// wire.HeartbeatMetrics: mailbox_depth is the number of messages currently held, mailbox_latency_ms
// the duration of the most recent handler, processed_total the cumulative count of handled
// messages, and expired_total the cumulative count of calls dropped unexecuted past their deadline
// (omitted while zero, so the payload of a thrall that never dropped a call is unchanged).
// in_flight is the number of handlers running at this instant (omitted while zero): mailbox_depth
// counts every message received and not yet finished, so mailbox_depth - in_flight is waiting.
// panics_total is the cumulative count of handler crashes the thrall contained and survived - an
// exception out of a web handler, answered with a 500 (omitted while zero; an ordinary thrall
// never reports it).
export interface HeartbeatMetrics {
  mailbox_depth: number;
  mailbox_latency_ms: number;
  processed_total: number;
  expired_total?: number;
  in_flight?: number;
  panics_total?: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(e: Envelope): Uint8Array {
  return encoder.encode(JSON.stringify(e));
}

export function decode(data: Uint8Array): Envelope {
  return JSON.parse(decoder.decode(data)) as Envelope;
}
