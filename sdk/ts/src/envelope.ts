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
}

export interface WireError {
  type: string;
  message: string;
  retryable: boolean;
}

// ThrallDescribe = a thrall's self-report of its own contract and most recent failure, carried in
// the optional `describe` field of a heartbeat payload. Mirrors wire.ThrallDescribe on the Go side.
// call_ops / cast_ops are the handler-map keys (kept apart so an edge route can be validated against
// the right set); version is the thrall's self-declared build; last_error / last_error_ms carry the
// reason and time of the most recent handler failure (empty until one happens). Fields are optional
// so an empty value is omitted, matching the Go omitempty tags. Metadata is not here: it is a
// deployment fact the lord fills in from the manifest, not something the thrall reports.
export interface ThrallDescribe {
  call_ops?: string[];
  cast_ops?: string[];
  version?: string;
  last_error?: string;
  last_error_ms?: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(e: Envelope): Uint8Array {
  return encoder.encode(JSON.stringify(e));
}

export function decode(data: Uint8Array): Envelope {
  return JSON.parse(decoder.decode(data)) as Envelope;
}
