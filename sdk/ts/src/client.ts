import type { NatsConnection, NatsError } from "nats";
import { decode, encode, ErrorType, type Envelope, type WireError } from "./envelope";
import { subjects } from "./subjects";
import { deadlineAfter } from "./deadline";

export interface CallOpts {
  timeoutMs?: number;
  // Correlation id to stamp on the outgoing message. Omitted -> a fresh trace is minted (edge);
  // Ctx.call/Ctx.cast pass the current message's trace here to propagate it across a hop.
  trace?: string;
  // Stable idempotency key. On an idempotent thrall a retry carrying the same key is not
  // re-processed: a duplicate call returns the first reply, a duplicate cast is skipped. See AE-077.
  idempotencyKey?: string;
  // The app the target lives in: the message goes to aether.<app>.<target>.<verb> instead of the
  // sender's own app. This is how an application reaches a shared thrall deployed in another app.
  app?: string;
}

export type CastOpts = Pick<CallOpts, "trace" | "idempotencyKey" | "app">;

// CallError = the error a call rejects with when the target answered with an error reply. It
// carries the reply's error as the target sent it, so a caller tells the outcomes apart instead of
// matching error text:
//
//   - type "refused": the thrall declined the request by a rule of its domain. `code` is the
//     thrall's stable machine code for the rule and `reason` the human sentence. A normal answer,
//     not a fault.
//   - type "handler_error" / "unknown_op" / "escalated": the request failed in the thrall.
//   - type "deadline_exceeded": the thrall got to the call after its deadline and dropped it
//     unexecuted (`retryable` is true).
//
// A thrall built with an SDK that predates refusals reports everything as "handler_error" with an
// empty code. A call that got no reply at all rejects with the NATS error (timeout, no responders),
// not a CallError. `message` stays "<type>: <reason>", the text a failed call has always had.
// Mirrors the Go SDK's CallError.
export class CallError extends Error {
  readonly type: string;
  readonly code: string;
  readonly reason: string;
  readonly retryable: boolean;

  constructor(error: WireError | undefined) {
    const type = error?.type ?? ErrorType.handler;
    const reason = error?.message ?? "error reply without detail";
    super(`${type}: ${reason}`);
    this.name = "CallError";
    this.type = type;
    this.code = error?.code ?? "";
    this.reason = reason;
    this.retryable = error?.retryable ?? false;
  }

  // refused = the target declined the request (as opposed to failing at it).
  get refused(): boolean {
    return this.type === ErrorType.refused;
  }
}

// asRefusal returns the refusal behind err, or null when err is not one - a failure of the thrall,
// a transport error, or anything else.
export function asRefusal(err: unknown): CallError | null {
  return err instanceof CallError && err.refused ? err : null;
}

// refusalCode returns the code of the refusal behind err, or "" when err is not a refusal. It makes
// mapping refusals to outcomes a plain switch or table lookup on the code.
export function refusalCode(err: unknown): string {
  return asRefusal(err)?.code ?? "";
}

// Client calls and casts to thralls over a NATS connection. It needs nothing else: no lord, no
// environment, no running thrall - so a gateway, a CLI, a test or a library can talk to the ether
// with the same envelope a thrall's own ctx.call produces. `app` is the app it addresses by
// default; `defaults.trace` is carried by every send that does not name its own. A Client is
// immutable. Mirrors the Go SDK's Client.
export class Client {
  constructor(
    private readonly nc: NatsConnection,
    readonly app: string,
    private readonly defaults: { trace?: string } = {},
  ) {}

  // with returns a copy of the client with another default app and/or trace, leaving this one
  // untouched - e.g. binding a per-request trace to a long-lived client: client.with({ trace }).
  with(overrides: { app?: string; trace?: string }): Client {
    return new Client(this.nc, overrides.app || this.app, { trace: overrides.trace || this.defaults.trace });
  }

  // call = synchronous request/reply (GenServer.call). Resolves with the reply payload; rejects
  // with a CallError when the target answered with an error reply, or with the NATS error when no
  // reply arrived. The envelope carries the instant this call stops waiting, so a thrall that only
  // gets to it later drops it instead of executing a request nobody is waiting for.
  async call<R = unknown>(target: string, op: string, payload: unknown = {}, opts: CallOpts = {}): Promise<R> {
    const timeoutMs = opts.timeoutMs ?? DEFAULT_CALL_TIMEOUT_MS;
    const now = Date.now();
    const req: Envelope = {
      v: 1,
      id: nextId(),
      trace: orNewTrace(opts.trace || this.defaults.trace),
      idem: opts.idempotencyKey,
      kind: "call",
      to: target,
      op,
      payload,
      ts: now,
      deadline: deadlineAfter(now, timeoutMs),
    };
    const msg = await this.nc.request(subjects.call(opts.app || this.app, target), encode(req), { timeout: timeoutMs });
    const reply = decode(msg.data);
    if (reply.status === "error") throw new CallError(reply.error);
    return reply.payload as R;
  }

  // cast = fire-and-forget (GenServer.cast): it says nothing about whether the target received
  // or handled the message.
  cast(target: string, op: string, payload: unknown = {}, opts: CastOpts = {}): void {
    this.nc.publish(subjects.cast(opts.app || this.app, target), encode(castEnvelope(target, op, payload, this.sendOpts(opts))));
  }

  // castConfirmed = a cast to a durable thrall that resolves only once the message is durably
  // stored in the target's mailbox; see castConfirmed below.
  async castConfirmed(target: string, op: string, payload: unknown = {}, opts: CastConfirmedOpts = {}): Promise<void> {
    await sendConfirmedCast(this.nc, opts.app || this.app, target, op, payload, this.sendOpts(opts));
  }

  private sendOpts<T extends CastOpts>(opts: T): T {
    return { ...opts, trace: opts.trace || this.defaults.trace };
  }
}

const DEFAULT_CALL_TIMEOUT_MS = 5000;

// The connection of the thrall running in this process (set by start(), or manually by a
// standalone script via useConnection()). It backs the module-level call/cast, which address the
// app named by AETHER_APP. Code that is not a thrall should build its own Client instead.
let shared: NatsConnection | null = null;

export function useConnection(nc: NatsConnection): void {
  shared = nc;
}

// own is the client over the shared connection. The app is read per send, as it always was, so a
// script may set AETHER_APP after useConnection().
function own(): Client {
  if (!shared) {
    throw new Error("no connection - call start() or useConnection(nc)");
  }
  return new Client(shared, process.env.AETHER_APP ?? "");
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

// call = synchronous request/reply with a timeout (GenServer.call) to another thrall, over this
// process's shared connection. See Client.call for what it resolves and rejects with.
export function call<R = unknown>(target: string, op: string, payload: unknown = {}, opts: CallOpts = {}): Promise<R> {
  return own().call<R>(target, op, payload, opts);
}

// cast = fire-and-forget (GenServer.cast). Pass opts.trace to propagate a trace (Ctx.cast
// does this); omitted -> a fresh trace is minted.
export function cast(target: string, op: string, payload: unknown = {}, opts: CastOpts = {}): void {
  own().cast(target, op, payload, opts);
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
  // The app the target lives in, when it is not the sender's own (see CallOpts.app).
  app?: string;
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
  await own().castConfirmed(target, op, payload, opts);
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
  if (reply.status === "error") throw new CallError(reply.error);
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
