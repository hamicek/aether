import type { NatsConnection, NatsError } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";

export interface CallOpts {
  timeoutMs?: number;
  // Correlation id to stamp on the outgoing message. Omitted -> a fresh trace is minted (edge);
  // Ctx.call/Ctx.cast pass the current message's trace here to propagate it across a hop.
  trace?: string;
  // Stable idempotency key. On an idempotent thrall a retry carrying the same key is not
  // re-processed: a duplicate call returns the first reply, a duplicate cast is skipped. See AE-077.
  idempotencyKey?: string;
}

// Shared connection for client calls (set by start(), or set manually by a
// standalone client via useConnection()).
let shared: NatsConnection | null = null;

export function useConnection(nc: NatsConnection): void {
  shared = nc;
}

function conn(): NatsConnection {
  if (!shared) {
    throw new Error("no connection - call start() or useConnection(nc)");
  }
  return shared;
}

let seq = 0;
function nextId(): string {
  return `${Date.now().toString(36)}-${(seq++).toString(36)}`;
}

// newTrace mints a fresh correlation id for an edge (a message that starts a new operation).
export function newTrace(): string {
  return `t-${nextId()}`;
}

// orNewTrace returns the given trace, or a fresh one when it is empty.
export function orNewTrace(trace?: string): string {
  return trace && trace.length > 0 ? trace : newTrace();
}

function app(): string {
  return process.env.AETHER_APP ?? "";
}

// call = synchronous request/reply with a timeout (GenServer.call).
export async function call<R = unknown>(
  target: string,
  op: string,
  payload: unknown = {},
  opts: CallOpts = {},
): Promise<R> {
  const req: Envelope = { v: 1, id: nextId(), trace: orNewTrace(opts.trace), idem: opts.idempotencyKey, kind: "call", to: target, op, payload, ts: Date.now() };
  const msg = await conn().request(subjects.call(app(), target), encode(req), {
    timeout: opts.timeoutMs ?? 5000,
  });
  const reply = decode(msg.data);
  if (reply.status === "error") {
    throw new Error(`${reply.error?.type}: ${reply.error?.message}`);
  }
  return reply.payload as R;
}

// cast = fire-and-forget (GenServer.cast). Pass opts.trace to propagate a trace (Ctx.cast
// does this); omitted -> a fresh trace is minted.
export function cast(target: string, op: string, payload: unknown = {}, opts: { trace?: string; idempotencyKey?: string } = {}): void {
  conn().publish(subjects.cast(app(), target), encode(castEnvelope(target, op, payload, opts)));
}

// castEnvelope builds the envelope shared by the plain and the confirmed cast.
function castEnvelope(target: string, op: string, payload: unknown, opts: { trace?: string; idempotencyKey?: string }): Envelope {
  return { v: 1, id: nextId(), trace: orNewTrace(opts.trace), idem: opts.idempotencyKey, kind: "cast", to: target, op, payload, ts: Date.now() };
}

export interface CastConfirmedOpts {
  // Upper bound for the whole send (mailbox lookup + stored ack). Default 5000, like call.
  timeoutMs?: number;
  trace?: string;
  // Stable idempotency key, also sent as Nats-Msg-Id: a retry carrying the same key lands once
  // in the mailbox within its duplicate window.
  idempotencyKey?: string;
}

// NotDurableError = a confirmed cast whose target has no durable mailbox. Nothing is sent: a
// JetStream publish to such a target would reach its core subscription, be processed and never
// be acknowledged, so the sender would time out, retry and get it processed twice.
export class NotDurableError extends Error {
  constructor(readonly target: string) {
    super(`confirmed cast to "${target}": target has no durable mailbox`);
    this.name = "NotDurableError";
  }
}

// JetStream API error code for "stream not found" (nats.js exposes no named constant for it).
const STREAM_NOT_FOUND = 10059;

// Mailboxes confirmed to exist, per connection (whether a stream exists is a property of the
// cluster a connection talks to). Only a positive answer is cached, so a target provisioned as
// durable later works immediately; a mailbox deleted after it was cached makes the publish fail,
// never succeed silently.
const knownMailboxes = new WeakMap<NatsConnection, Set<string>>();

// castConfirmed = a cast to a durable thrall that resolves only once the message is durably
// stored in the target's mailbox. Resolving means stored; a rejection means the message may not
// be stored and the caller decides whether to retry (with the same idempotencyKey, so a retry of
// a message that did land is deduplicated). A target without a mailbox rejects with
// NotDurableError. Mirrors the Go SDK CastConfirmed.
export async function castConfirmed(target: string, op: string, payload: unknown = {}, opts: CastConfirmedOpts = {}): Promise<void> {
  await sendConfirmedCast(conn(), app(), target, op, payload, opts);
}

// sendConfirmedCast is castConfirmed over an explicit connection and app.
export async function sendConfirmedCast(
  nc: NatsConnection,
  appName: string,
  target: string,
  op: string,
  payload: unknown,
  opts: CastConfirmedOpts,
): Promise<void> {
  const deadline = Date.now() + (opts.timeoutMs ?? 5000);
  const stream = subjects.stream(appName, target);
  await ensureMailbox(nc, stream, target, deadline);
  await nc.jetstream().publish(subjects.cast(appName, target), encode(castEnvelope(target, op, payload, opts)), {
    // Only the target's mailbox may acknowledge the write, never another stream on the subject.
    expect: { streamName: stream },
    timeout: remainingMs(deadline),
    ...(opts.idempotencyKey ? { msgID: opts.idempotencyKey } : {}),
  });
}

async function ensureMailbox(nc: NatsConnection, stream: string, target: string, deadline: number): Promise<void> {
  if (knownMailboxes.get(nc)?.has(stream)) return;
  const jsm = await nc.jetstreamManager({ timeout: remainingMs(deadline) });
  try {
    await jsm.streams.info(stream);
  } catch (err) {
    if ((err as NatsError).api_error?.err_code === STREAM_NOT_FOUND) throw new NotDurableError(target);
    throw err;
  }
  let known = knownMailboxes.get(nc);
  if (!known) {
    known = new Set();
    knownMailboxes.set(nc, known);
  }
  known.add(stream);
}

function remainingMs(deadline: number): number {
  return Math.max(1, deadline - Date.now());
}

// SpawnSpec = the request to spawn a child at runtime. Mirrors internal/wire.SpawnSpec:
// the subset of a manifest thrall relevant to a dynamic child (always local, single).
export interface SpawnSpec {
  name: string;
  cmd: string;
  restart?: string; // permanent | transient | temporary (default permanent)
  durable?: boolean; // true -> casts go through JetStream
  eventLog?: boolean; // true -> provision an event-sourcing log (Append/Rebuild)
}

// lordControl sends a spawn/stop request on the lord's control channel and returns the
// reply payload, or throws with the lord's refusal. `nc` is passed explicitly so this
// works both from a thrall's ctx and from a standalone client connection.
async function lordControl(
  nc: NatsConnection,
  op: "spawn" | "stop",
  payload: unknown,
  opts: CallOpts = {},
): Promise<unknown> {
  const req: Envelope = { v: 1, id: nextId(), kind: "ctl", op, payload, ts: Date.now() };
  const msg = await nc.request(subjects.lordCtl(), encode(req), { timeout: opts.timeoutMs ?? 5000 });
  const reply = decode(msg.data);
  if (reply.status === "error") {
    throw new Error(`${reply.error?.type}: ${reply.error?.message}`);
  }
  return reply.payload;
}

// startChild asks the lord to spawn a new thrall at runtime - a child not in the
// manifest (a driver per connection, a worker per request). The lord supervises it
// one_for_one, outside any group strategy. Returns the child's name.
export async function startChild(nc: NatsConnection, spec: SpawnSpec, opts: CallOpts = {}): Promise<string> {
  // Map to the wire shape (snake_case keys the lord unmarshals); undefined fields drop out.
  const payload = {
    name: spec.name,
    cmd: spec.cmd,
    restart: spec.restart,
    durable: spec.durable,
    event_log: spec.eventLog,
  };
  const reply = (await lordControl(nc, "spawn", payload, opts)) as { name: string };
  return reply.name;
}

// stopChild asks the lord to drain and stop a dynamic child started via startChild.
export async function stopChild(nc: NatsConnection, name: string, opts: CallOpts = {}): Promise<void> {
  await lordControl(nc, "stop", { name }, opts);
}
