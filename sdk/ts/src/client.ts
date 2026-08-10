import type { NatsConnection } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";

export interface CallOpts {
  timeoutMs?: number;
  // Correlation id to stamp on the outgoing message. Omitted -> a fresh trace is minted (edge);
  // Ctx.call/Ctx.cast pass the current message's trace here to propagate it across a hop.
  trace?: string;
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
  const req: Envelope = { v: 1, id: nextId(), trace: orNewTrace(opts.trace), kind: "call", to: target, op, payload, ts: Date.now() };
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
export function cast(target: string, op: string, payload: unknown = {}, opts: { trace?: string } = {}): void {
  const e: Envelope = { v: 1, id: nextId(), trace: orNewTrace(opts.trace), kind: "cast", to: target, op, payload, ts: Date.now() };
  conn().publish(subjects.cast(app(), target), encode(e));
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
