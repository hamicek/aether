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

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(e: Envelope): Uint8Array {
  return encoder.encode(JSON.stringify(e));
}

export function decode(data: Uint8Array): Envelope {
  return JSON.parse(decoder.decode(data)) as Envelope;
}
