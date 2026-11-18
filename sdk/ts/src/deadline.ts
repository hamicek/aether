// Call deadlines, mirroring internal/wire (DeadlineAfter, Envelope.Expired) and the Go SDK's
// deadline gate. A caller stamps a call with the instant it stops waiting; a thrall that only gets
// to the call after that instant drops it instead of executing a request nobody is waiting for -
// otherwise the caller's retry would run the write a second time.
//
// The deadline is stamped by the caller's clock and judged by the callee's, so the callee allows a
// skew tolerance. On one host the two are the same clock; across nodes kept by NTP they differ by
// tens of milliseconds, which the default covers with a wide margin. A callee whose clock runs
// ahead by more than the tolerance refuses live calls at once (loud, nothing executed, retryable);
// one that runs behind executes an expired call, which is the behavior without a deadline.
//
// A deadline narrows the window, it does not close it: a handler that starts just before the
// deadline can still finish after it. A caller that must not double-write sends an idempotency key
// as well.

import { ErrorType, type Envelope } from "./envelope";

export const DEFAULT_DEADLINE_SKEW_MS = 250;

// deadlineSkewMs resolves this thrall's skew tolerance: AETHER_DEADLINE_SKEW_MS when it is a
// non-negative integer (0 = judge the deadline exactly), else the default. A thrall inherits the
// lord's environment, so setting it on the lord applies to every thrall it spawns.
export function deadlineSkewMs(): number {
  const raw = process.env.AETHER_DEADLINE_SKEW_MS;
  if (raw && /^\d+$/.test(raw.trim())) return parseInt(raw, 10);
  return DEFAULT_DEADLINE_SKEW_MS;
}

// deadlineAfter is the deadline a caller stamps on a call it will wait timeoutMs for, starting at
// nowMs. A non-positive timeout yields undefined (no deadline), never an already expired one. The
// result is a whole number of milliseconds: the wire field is an integer, and a fractional value
// (a timeout computed as budget / 3, say) would make the whole envelope undecodable to a Go thrall.
export function deadlineAfter(nowMs: number, timeoutMs: number): number | undefined {
  return timeoutMs > 0 ? Math.round(nowMs + timeoutMs) : undefined;
}

// overdueMs returns how far past its deadline (beyond the skew tolerance) the envelope is at nowMs,
// as the number of milliseconds since the deadline, or undefined when it has not expired. An
// envelope without a deadline never expires.
export function overdueMs(e: Envelope, nowMs: number, skewMs: number): number | undefined {
  if (!e.deadline || e.deadline <= 0) return undefined;
  const overdue = nowMs - e.deadline;
  return overdue > skewMs ? overdue : undefined;
}

// MAX_QUEUED_WAIT_MS is the longest a waiting call is given a timer for (24 hours). A deadline
// further away is not worth timing - and must not be: setTimeout with a delay above 2^31-1 ms
// fires at once. Such a call waits like one without a deadline; the gate still judges it when its
// turn comes. The same bound as the Go and Python SDKs.
export const MAX_QUEUED_WAIT_MS = 24 * 60 * 60 * 1000;

// expiresInMs is how long from nowMs a thrall would start dropping the call: its deadline plus the
// skew tolerance. undefined for a call that needs no timer: one without a deadline, which never
// expires, or one whose deadline is more than MAX_QUEUED_WAIT_MS away.
export function expiresInMs(e: Envelope, nowMs: number, skewMs: number): number | undefined {
  if (!e.deadline || e.deadline <= 0) return undefined;
  const remaining = e.deadline - nowMs;
  if (remaining > MAX_QUEUED_WAIT_MS) return undefined;
  return remaining + skewMs;
}

// expiredReply is the reply to a call dropped past its deadline. The caller is normally gone, but
// one whose clock disagrees with ours is still waiting and should learn at once that nothing ran -
// hence retryable.
export function expiredReply(req: Envelope, overdue: number): Envelope {
  return {
    v: 1,
    id: req.id,
    kind: "reply",
    status: "error",
    error: {
      type: ErrorType.deadlineExceeded,
      message: `call expired ${Math.round(overdue)} ms before it could be handled`,
      retryable: true,
    },
  };
}
