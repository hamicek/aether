import { AckPolicy, DeliverPolicy, type NatsConnection } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv, type Env } from "./connection";
import { useConnection, startChild, stopChild, type SpawnSpec, type CallOpts } from "./client";
import { newLogger, type Logger } from "./log";

// Handler shapes hold the GenServer semantics:
//   handleCall: (payload, state) => [reply, newState]
//   handleCast: (payload, state) => newState
export type CallHandler<S> = (
  payload: unknown,
  state: S,
  ctx: Ctx,
) => Promise<[unknown, S]> | [unknown, S];

export type CastHandler<S> = (
  payload: unknown,
  state: S,
  ctx: Ctx,
) => Promise<S> | S;

export interface Ctx {
  // WE DO NOT HIDE NATS BEHIND THE THRALL - full access to JetStream, KV, its own subjects.
  nats: NatsConnection;
  name: string;
  app: string;
  // Structured logger pre-tagged with app and name, configured from the logging env the
  // lord injected - handlers should log through it.
  log: Logger;
  // Dynamic supervisor: ask the lord to spawn/stop a child at runtime. Mirrors the Go
  // SDK ctx.StartChild/StopChild; the lord supervises a dynamic child one_for_one,
  // outside any manifest group strategy.
  startChild: (spec: SpawnSpec, opts?: CallOpts) => Promise<string>;
  stopChild: (name: string, opts?: CallOpts) => Promise<void>;
}

export interface ThrallDef<S> {
  name: string;
  init: (ctx: Ctx) => Promise<S> | S;
  handleCall?: Record<string, CallHandler<S>>;
  handleCast?: Record<string, CastHandler<S>>;
  terminate?: (reason: string, state: S) => void | Promise<void>;
}

// defThrall is just a typed definition - the actual run is started by start().
export function defThrall<S>(def: ThrallDef<S>): ThrallDef<S> {
  return def;
}

// start connects the thrall to the ether and runs its lifecycle.
export async function start<S>(def: ThrallDef<S>): Promise<void> {
  const env: Env = readEnv();
  const name = def.name || env.name;
  const durable = process.env.AETHER_DURABLE === "1";
  const nc = await open(env);
  useConnection(nc); // so this thrall can call()/cast() other thralls
  const log = newLogger({ component: "thrall", app: env.app, name });
  const ctx: Ctx = {
    nats: nc,
    name,
    app: env.app,
    log,
    startChild: (spec, opts) => startChild(nc, spec, opts),
    stopChild: (childName, opts) => stopChild(nc, childName, opts),
  };

  let state: S = await def.init(ctx);

  // Mailbox: serialized processing (1 message at a time) = the GenServer guarantee.
  let tail: Promise<void> = Promise.resolve();
  const serialize = (job: () => Promise<void>): Promise<void> => {
    tail = tail.then(job, job);
    return tail;
  };

  // handleCall/handleCast over the serialized mailbox (shared by the core and durable branches).
  const onCall = (e: Envelope, respond: (data: Uint8Array) => void): Promise<void> =>
    serialize(async () => {
      const handler = def.handleCall?.[e.op ?? ""];
      if (!handler) {
        respond(encode(errReply(e, "unknown_op", `unknown call op: ${e.op}`)));
        return;
      }
      try {
        const [reply, next] = await handler(e.payload, state, ctx);
        state = next;
        respond(encode(okReply(e, reply)));
      } catch (err) {
        respond(encode(errReply(e, "handler_error", String(err))));
      }
    });

  const onCast = (e: Envelope): Promise<void> =>
    serialize(async () => {
      const handler = def.handleCast?.[e.op ?? ""];
      if (!handler) return;
      try {
        state = await handler(e.payload, state, ctx);
      } catch (err) {
        log.error("cast handler failed", { op: e.op, err: String(err) });
      }
    });

  if (durable) {
    // Durable thrall: call/info over core (synchronous), cast via JetStream (survives a crash).
    subscribeVerb(nc, subjects.call(env.app, name), (e, msg) => onCall(e, (d) => msg.respond(d)));
    subscribeVerb(nc, subjects.info(env.app, name), () => {}); // TODO handleInfo
    void consumeDurableCast(nc, env.app, name, onCast);
  } else {
    // Non-durable thrall: a single wildcard subscription over call/cast/info (FIFO).
    subscribeData(nc, env.app, name, onCall, onCast);
  }

  // ctl: controlled shutdown from the lord (drain / shutdown)
  const ctlSub = nc.subscribe(subjects.ctl(name));
  void (async () => {
    for await (const msg of ctlSub) {
      const e = decode(msg.data);
      if (e.op === "drain" || e.op === "shutdown") {
        await tail; // finish draining the in-flight mailbox
        await def.terminate?.(e.op, state);
        await nc.drain();
        process.exit(0);
      }
    }
  })();

  startHeartbeat(nc, name);
}

// subscribeData: a single wildcard subscription (call/cast/info) for a non-durable thrall.
function subscribeData(
  nc: NatsConnection,
  app: string,
  name: string,
  onCall: (e: Envelope, respond: (d: Uint8Array) => void) => void,
  onCast: (e: Envelope) => void,
): void {
  const sub = nc.subscribe(subjects.data(app, name));
  void (async () => {
    for await (const msg of sub) {
      const verb = msg.subject.slice(msg.subject.lastIndexOf(".") + 1);
      const e = decode(msg.data);
      if (verb === "call") onCall(e, (d) => msg.respond(d));
      else if (verb === "cast") onCast(e);
    }
  })();
}

// subscribeVerb: a core subscription on one specific subject.
function subscribeVerb(
  nc: NatsConnection,
  subject: string,
  handle: (e: Envelope, msg: { respond: (d: Uint8Array) => void }) => void,
): void {
  const sub = nc.subscribe(subject);
  void (async () => {
    for await (const msg of sub) {
      handle(decode(msg.data), msg);
    }
  })();
}

// consumeDurableCast: reads casts from a durable JetStream consumer with explicit ack.
// While the thrall is down, casts accumulate in the stream (the lord created it) and are
// drained on its return. At-least-once -> handlers must be idempotent.
async function consumeDurableCast(
  nc: NatsConnection,
  app: string,
  name: string,
  onCast: (e: Envelope) => Promise<void>,
): Promise<void> {
  const stream = subjects.stream(app, name);
  const jsm = await nc.jetstreamManager();
  try {
    await jsm.consumers.add(stream, {
      durable_name: name,
      ack_policy: AckPolicy.Explicit,
      filter_subject: subjects.cast(app, name),
      deliver_policy: DeliverPolicy.All,
    });
  } catch {
    // the durable consumer already exists (survived the thrall crash) - just attach to it
  }

  const js = nc.jetstream();
  const consumer = await js.consumers.get(stream, name);
  const messages = await consumer.consume();
  for await (const m of messages) {
    await onCast(decode(m.data)); // process (serialized) ...
    m.ack(); //                     ... and only then ack (at-least-once)
  }
}

function okReply(req: Envelope, payload: unknown): Envelope {
  return { v: 1, id: req.id, kind: "reply", status: "ok", payload };
}

function errReply(req: Envelope, type: string, message: string): Envelope {
  return {
    v: 1,
    id: req.id,
    kind: "reply",
    status: "error",
    error: { type, message, retryable: false },
  };
}

function startHeartbeat(nc: NatsConnection, name: string): void {
  const tick = () => {
    const hb: Envelope = { v: 1, kind: "hb", to: name, ts: Date.now() };
    nc.publish(subjects.hb(name), encode(hb));
  };
  tick();
  const timer = setInterval(tick, 2000);
  timer.unref?.();
}
