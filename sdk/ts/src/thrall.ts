import { AckPolicy, DeliverPolicy, nanos, type NatsConnection } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv, type Env } from "./connection";
import { useConnection, startChild, stopChild, call, cast, orNewTrace, type SpawnSpec, type CallOpts } from "./client";
import { newLogger, type Logger } from "./log";
import { appendEvent, type AppendOpts } from "./rebuild";
import { heartbeatIntervalMs } from "./heartbeat";
import { startFencingIfSingleton, startLordLivenessFencing, fenceConfigFromEnv } from "./fencing";
import { DedupCache, dedupKey } from "./dedup";

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

// EscalateError is the typed "let it crash" signal. A handler that throws it (via escalate())
// asks the runtime to terminate the thrall with an abnormal exit, so the lord restarts it
// through init per the thrall's restart policy - real OTP semantics without a manual
// process.exit in application code. A plain thrown error keeps its old meaning: reply the
// caller an error (call) or log it (cast), and keep living.
export class EscalateError extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(`escalate: ${reason}`);
    this.name = "EscalateError";
    this.reason = reason;
  }
}

// escalate is the typed let-it-crash path a handler calls to ask for a restart. The reason is
// surfaced to a call caller (as the "escalated" error reply) and logged before the process exits.
export function escalate(reason: string): never {
  throw new EscalateError(reason);
}

// asEscalate returns the escalation signal if err is one, else null (a plain error).
export function asEscalate(err: unknown): EscalateError | null {
  return err instanceof EscalateError ? err : null;
}

// exitProcess terminates the thrall process on escalation. A module-local seam (mirrors the Go
// SDK's exitProcess var) so it stays swappable; production exits for real.
let exitProcess: (code: number) => void = (code) => process.exit(code);

export interface Ctx {
  // WE DO NOT HIDE NATS BEHIND THE THRALL - full access to JetStream, KV, its own subjects.
  nats: NatsConnection;
  name: string;
  app: string;
  // Structured logger pre-tagged with app and name, configured from the logging env the
  // lord injected - handlers should log through it.
  log: Logger;
  // trace is the correlation id of the message currently being handled; ctx.call/ctx.cast
  // propagate it to downstream messages so one operation can be followed across processes.
  trace: string;
  // msgId is the id of the message currently being handled (the envelope's id). Unlike trace
  // (which spans a whole operation), it is unique per message, so a handler can pass it as the
  // append dedup key to make a redelivered command idempotent - see the command-key pattern.
  msgId: string;
  // singletonEpoch is this thrall's fencing epoch when it is a singleton (0 otherwise), constant
  // for the process lifetime. Singleton fencing only bounds liveness overlap, not write access; for
  // strict single-writer against an external resource, send this epoch with every write and have the
  // resource reject a lower one - the write-side fencing token pattern (DESIGN §14).
  singletonEpoch: number;
  call: <R = unknown>(target: string, op: string, payload?: unknown, opts?: CallOpts) => Promise<R>;
  cast: (target: string, op: string, payload?: unknown, opts?: { idempotencyKey?: string }) => void;
  // append persists a domain event to this thrall's event log (opt-in event_log). Rebuild
  // replays it in init. Mirrors the Go SDK ctx.Append. Pass dedupKey to deduplicate the event
  // within the stream's duplicate window (Nats-Msg-Id).
  append: (event: unknown, opts?: AppendOpts) => Promise<void>;
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
  // idempotent turns on in-memory dedup of call/cast by idempotency key (opts.idempotencyKey,
  // else the envelope id): a duplicate cast is skipped and a duplicate call returns the first
  // reply. In-memory only - it does not survive a restart. See AE-077.
  idempotent?: boolean;
  // idempotencyMax bounds the dedup cache (undefined/0 = a sensible default). Ignored unless idempotent.
  idempotencyMax?: number;
}

// defThrall is just a typed definition - the actual run is started by start().
export function defThrall<S>(def: ThrallDef<S>): ThrallDef<S> {
  return def;
}

// start connects the thrall to the ether and runs its lifecycle.
export async function start<S>(def: ThrallDef<S>): Promise<void> {
  const env: Env = readEnv();
  const name = def.name || env.name;
  if (typeof def.init !== "function") throw new Error(`thrall ${name}: init is required`);
  const durable = process.env.AETHER_DURABLE === "1";
  const nc = await open(env);
  useConnection(nc); // so this thrall can call()/cast() other thralls
  const log = newLogger({ component: "thrall", app: env.app, name });
  const ctx: Ctx = {
    nats: nc,
    name,
    app: env.app,
    log,
    trace: "",
    msgId: "",
    singletonEpoch: fenceConfigFromEnv()?.epoch ?? 0,
    call: (target, op, payload = {}, opts = {}) => call(target, op, payload, { ...opts, trace: ctx.trace }),
    cast: (target, op, payload = {}, opts = {}) => cast(target, op, payload, { ...opts, trace: ctx.trace }),
    append: (event, opts) => appendEvent(nc, env.app, name, event, opts),
    startChild: (spec, opts) => startChild(nc, spec, opts),
    stopChild: (childName, opts) => stopChild(nc, childName, opts),
  };

  let state: S = await def.init(ctx);

  // Optional in-memory dedup by idempotency key. Accessed only from the serialized mailbox, so
  // it needs no lock of its own. Undefined unless the thrall opts in.
  const dedup = def.idempotent ? new DedupCache(def.idempotencyMax ?? 0) : undefined;

  // Mailbox: serialized processing (1 message at a time) = the GenServer guarantee.
  let tail: Promise<void> = Promise.resolve();
  const serialize = (job: () => Promise<void>): Promise<void> => {
    tail = tail.then(job, job);
    return tail;
  };

  // Self-metrics reported on each heartbeat (mirrors the Go SDK mailboxStats): depth = messages
  // currently held, lastMs = duration of the most recent handler, processed = cumulative count.
  const stats = { depth: 0, processed: 0, lastMs: 0 };
  const beginJob = (): number => {
    stats.depth++;
    return performance.now();
  };
  const endJob = (start: number): void => {
    stats.lastMs = performance.now() - start;
    stats.processed++;
    stats.depth--;
  };
  const snapshot = () => ({
    mailbox_depth: stats.depth,
    mailbox_latency_ms: stats.lastMs,
    processed_total: stats.processed,
  });

  // handleCall/handleCast over the serialized mailbox (shared by the core and durable branches).
  // beginJob runs at enqueue time (before the serialized job), so mailbox_depth counts messages
  // waiting in the promise chain - matching the Go/Python SDKs, where begin() runs before the
  // mailbox lock.
  const onCall = (e: Envelope, respond: (data: Uint8Array) => void): Promise<void> => {
    const start = beginJob();
    return serialize(async () => {
      ctx.trace = orNewTrace(e.trace);
      ctx.msgId = e.id ?? "";
      log.debug("handling call", { op: e.op, trace: ctx.trace });
      try {
        const handler = def.handleCall?.[e.op ?? ""];
        if (!handler) {
          respond(encode(errReply(e, "unknown_op", `unknown call op: ${e.op}`)));
          return;
        }
        if (dedup) {
          const [cached, seen] = dedup.get(dedupKey(e));
          if (seen) {
            // A duplicate call: return the first reply instead of re-running the handler.
            respond(encode(okReply(e, cached)));
            return;
          }
        }
        try {
          const [reply, next] = await handler(e.payload, state, ctx);
          state = next;
          if (dedup) dedup.put(dedupKey(e), reply); // cache only a successful reply
          respond(encode(okReply(e, reply)));
        } catch (err) {
          const esc = asEscalate(err);
          if (esc) {
            // Reply the caller before we crash, so it learns of the restart instead of
            // hanging until timeout; flush so the reply leaves before the process exits.
            respond(encode(errReply(e, "escalated", esc.reason)));
            await nc.flush();
            log.error("handler escalated - self-terminating for restart", { op: e.op, reason: esc.reason });
            exitProcess(1);
            return;
          }
          respond(encode(errReply(e, "handler_error", String(err))));
        }
      } finally {
        endJob(start);
      }
    });
  };

  // ackDurable acknowledges the source JetStream message (durable cast); undefined for a
  // non-durable core cast, which needs no ack. On escalation the poison cast is acked before
  // the crash, so it is not redelivered into a loop after the restart.
  const onCast = (e: Envelope, ackDurable?: () => Promise<void>): Promise<void> => {
    const start = beginJob();
    return serialize(async () => {
      ctx.trace = orNewTrace(e.trace);
      ctx.msgId = e.id ?? "";
      log.debug("handling cast", { op: e.op, trace: ctx.trace });
      try {
        const handler = def.handleCast?.[e.op ?? ""];
        if (!handler) return;
        if (dedup) {
          const [, seen] = dedup.get(dedupKey(e));
          if (seen) return; // already processed; a durable cast is acked by the consume loop
        }
        try {
          state = await handler(e.payload, state, ctx);
          if (dedup) dedup.put(dedupKey(e), undefined); // mark processed only after success
        } catch (err) {
          const esc = asEscalate(err);
          if (esc) {
            // Ack the poison cast before crashing so JetStream does not redeliver it into a
            // crash loop; a non-durable cast (no ackDurable) is simply dropped, as it is today.
            if (ackDurable) await ackDurable();
            log.error("cast handler escalated - self-terminating for restart", { op: e.op, reason: esc.reason });
            exitProcess(1);
            return;
          }
          log.error("cast handler failed", { op: e.op, err: String(err) });
        }
      } finally {
        endJob(start);
      }
    });
  };

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

  startHeartbeat(nc, name, snapshot);
  await startFencingIfSingleton(nc, name, log);
  await startLordLivenessFencing(nc, name, log);
}

// subscribeData: a single wildcard subscription (call/cast/info) for a non-durable thrall.
export function subscribeData(
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
export function subscribeVerb(
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

// Durable consumer tuning (mirrors the Go/Python SDKs). Unlike those two, the TS SDK never
// fetched one message at a time - consume() already pulls in batches internally - so there is
// no batch size to set here; what was missing is the ack bounds. ack_wait bounds how long a
// delivered-but-unacked message may sit before redelivery; max_ack_pending caps the in-flight
// unacked messages the server hands the consumer. Both are consumer config, so they apply when
// the durable consumer is first created (an existing one, survived a crash, keeps its config).
const durableAckWaitMs = 30_000;
const durableMaxAckPending = 512;

// consumeDurableCast: reads casts from a durable JetStream consumer with explicit ack.
// While the thrall is down, casts accumulate in the stream (the lord created it) and are
// drained on its return. At-least-once -> handlers must be idempotent.
export async function consumeDurableCast(
  nc: NatsConnection,
  app: string,
  name: string,
  onCast: (e: Envelope, ackDurable?: () => Promise<void>) => Promise<void>,
): Promise<void> {
  const stream = subjects.stream(app, name);
  const jsm = await nc.jetstreamManager();
  try {
    await jsm.consumers.add(stream, {
      durable_name: name,
      ack_policy: AckPolicy.Explicit,
      filter_subject: subjects.cast(app, name),
      deliver_policy: DeliverPolicy.All,
      ack_wait: nanos(durableAckWaitMs),
      max_ack_pending: durableMaxAckPending,
    });
  } catch {
    // the durable consumer already exists (survived the thrall crash) - just attach to it
  }

  const js = nc.jetstream();
  const consumer = await js.consumers.get(stream, name);
  const messages = await consumer.consume();
  for await (const m of messages) {
    // On escalation onCast acks (awaiting server confirmation) before it crashes, so the poison
    // cast is not redelivered; the happy path returns here and we ack in arrival order.
    await onCast(decode(m.data), async () => { await m.ackAck(); }); // process (serialized, FIFO) ...
    m.ack(); //                                                         ... and only then ack (at-least-once)
  }
}

export function okReply(req: Envelope, payload: unknown): Envelope {
  return { v: 1, id: req.id, kind: "reply", status: "ok", payload };
}

export function errReply(req: Envelope, type: string, message: string): Envelope {
  return {
    v: 1,
    id: req.id,
    kind: "reply",
    status: "error",
    error: { type, message, retryable: false },
  };
}

export function startHeartbeat(nc: NatsConnection, name: string, snapshot: () => unknown): void {
  const tick = () => {
    const hb: Envelope = { v: 1, kind: "hb", to: name, payload: snapshot(), ts: Date.now() };
    nc.publish(subjects.hb(name), encode(hb));
  };
  tick();
  const timer = setInterval(tick, heartbeatIntervalMs()); // lord-configured interval (default 2s)
  timer.unref?.();
}
