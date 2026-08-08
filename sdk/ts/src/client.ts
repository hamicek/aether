import type { NatsConnection } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";

export interface CallOpts {
  timeoutMs?: number;
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
  const req: Envelope = { v: 1, id: nextId(), kind: "call", to: target, op, payload, ts: Date.now() };
  const msg = await conn().request(subjects.call(app(), target), encode(req), {
    timeout: opts.timeoutMs ?? 5000,
  });
  const reply = decode(msg.data);
  if (reply.status === "error") {
    throw new Error(`${reply.error?.type}: ${reply.error?.message}`);
  }
  return reply.payload as R;
}

// cast = fire-and-forget (GenServer.cast).
export function cast(target: string, op: string, payload: unknown = {}): void {
  const e: Envelope = { v: 1, id: nextId(), kind: "cast", to: target, op, payload, ts: Date.now() };
  conn().publish(subjects.cast(app(), target), encode(e));
}
