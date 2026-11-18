import { AckPolicy, DeliverPolicy, ErrorCode, nanos, type NatsConnection, type NatsError, type Subscription } from "nats";
import { decode, encode, ErrorType, type Envelope, type HeartbeatMetrics, type ThrallDescribe } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv, type Env } from "./connection";
import { useConnection, startChild, stopChild, call, cast, castConfirmed, orNewTrace, type SpawnSpec, type CallOpts, type CastOpts, type CastConfirmedOpts } from "./client";
import { newLogger, type Logger } from "./log";
import { appendEvent, type AppendOpts } from "./rebuild";
import { heartbeatIntervalMs } from "./heartbeat";
import { startFencingIfSingleton, startLordLivenessFencing, fenceConfigFromEnv } from "./fencing";
import { DedupCache, dedupKey } from "./dedup";
import { deadlineSkewMs, expiredReply, expiresInMs, overdueMs } from "./deadline";

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

// UNSPECIFIED_REFUSAL is the code of a refusal made without one (mirrors thrall.UnspecifiedRefusal).
export const UNSPECIFIED_REFUSAL = "refused";

// RefuseError is the typed "no" of a handler: the request is understood and the thrall is healthy,
// but a rule of its domain forbids what was asked (not enough funds, the day is already closed).
// A handler throws it via refuse(). It is the third outcome beside a plain error ("this request
// failed") and escalate() ("crash me"), and the only one of the three that is not a failure:
//
//   - the caller gets an error reply of type "refused" carrying code and reason, which its SDK
//     surfaces as a typed error (in TS a CallError with refused === true);
//   - the thrall's state is left as it was, exactly as for a plain error;
//   - nothing is recorded as the thrall's last error, and the refusal counts as a handled message.
//
// code is a stable, machine-readable identifier that callers branch on, so it is part of the
// thrall's contract: name it for the rule ("insufficient_funds"), never reword it. reason is for
// people and may change freely. Mirrors the Go SDK's RefuseError.
export class RefuseError extends Error {
  readonly code: string;
  readonly reason: string;
  constructor(code: string, reason: string) {
    // A refusal always carries a code - that is what tells a caller "the thrall said no" from "the
    // call failed" (refusalCode returns "" only for the latter) - so an empty one is replaced.
    const named = code || UNSPECIFIED_REFUSAL;
    super(`refused (${named}): ${reason}`);
    this.name = "RefuseError";
    this.code = named;
    this.reason = reason;
  }
}


// refuse declines the request being handled. code should name the rule; an empty code becomes
// UNSPECIFIED_REFUSAL.
//
// A refusal is never forwarded implicitly: a handler that calls another thrall and lets that
// call's CallError propagate gets a plain handler failure even when the other thrall refused,
// because a code from someone else's contract must not pose as this thrall's answer. To pass a
// downstream refusal on, say so: const r = asRefusal(err); if (r) refuse(r.code, r.reason).
export function refuse(code: string, reason: string): never {
  throw new RefuseError(code, reason);
}

// asRefuse returns the refusal if err is one, else null.
export function asRefuse(err: unknown): RefuseError | null {
  return err instanceof RefuseError ? err : null;
}

// refusedReply is the error reply that carries a refusal to the caller.
export function refusedReply(req: Envelope, refusal: RefuseError): Envelope {
  return {
    v: 1,
    id: req.id,
    kind: "reply",
    status: "error",
    error: { type: ErrorType.refused, code: refusal.code, message: refusal.reason, retryable: false },
  };
}

// exitProcess terminates the thrall process on escalation. A module-local seam (mirrors the Go
// SDK's exitProcess var) so it stays swappable; production exits for real.
let exitProcess: (code: number) => void = (code) => process.exit(code);

// trace and msgId describe the message being handled, so a Ctx is only valid for the handler
// invocation it was passed to: do not keep it for later. A serialized thrall reuses one Ctx for
// every message; a concurrent web thrall (WebDef.serve) hands each request its own.
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
  cast: (target: string, op: string, payload?: unknown, opts?: Omit<CastOpts, "trace">) => void;
  // castConfirmed resolves only once the cast is durably stored in the target's mailbox; see
  // castConfirmed in client.ts. Mirrors the Go SDK ctx.CastConfirmed.
  castConfirmed: (target: string, op: string, payload?: unknown, opts?: Omit<CastConfirmedOpts, "trace">) => Promise<void>;
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
  // version is the thrall's self-declared build (e.g. "1.4.0"), reported to the lord in the
  // heartbeat's self-description and shown by `aether ps` / `aether describe`. It lives with the
  // code, so it answers "what actually runs here". Optional; omitted means unversioned.
  version?: string;
}

// defThrall is just a typed definition - the actual run is started by start().
export function defThrall<S>(def: ThrallDef<S>): ThrallDef<S> {
  return def;
}

// SERIALIZED is the concurrency of a GenServer: one message at a time.
export const SERIALIZED = 1;

// Dispatch is how a thrall's calls are run. The default - SERIALIZED, no failureReply - is the
// GenServer every thrall is; the web helper is the only one that asks for anything else.
export interface Dispatch {
  // concurrency is how many call handlers may run at once (see start).
  concurrency: number;
  // failureReply, when set, is the payload a caller is answered with - as a successful reply -
  // when a call handler throws anything but a refusal or an escalation. Unset is the GenServer
  // rule: a thrown error is a handler_error reply.
  failureReply?: () => unknown;
}

// The dispatch of a definition travels with it under a key this module keeps to itself, so it is
// not a field a thrall author can set: the only way to get a definition that runs its calls
// concurrently is defWeb with a serve handler (http.ts), whose shape has no state to thread.
const dispatchKey = Symbol("aether.dispatch");

// withDispatch marks how def's calls are run. For the web helper only - it is not exported from
// the package.
export function withDispatch<S>(def: ThrallDef<S>, dispatch: Dispatch): ThrallDef<S> {
  return Object.defineProperty(def, dispatchKey, { value: dispatch, enumerable: false });
}

function dispatchOf<S>(def: ThrallDef<S>): Dispatch {
  return (def as { [dispatchKey]?: Dispatch })[dispatchKey] ?? { concurrency: SERIALIZED };
}

// concurrencyOf is how many call handlers def runs at once: SERIALIZED unless defWeb said otherwise.
export function concurrencyOf<S>(def: ThrallDef<S>): number {
  return dispatchOf(def).concurrency;
}

// start connects the thrall to the ether and runs its lifecycle.
//
// How many call handlers may run at once is a property of the definition. Normally it is
// SERIALIZED - the GenServer every thrall is: one mailbox, one handler at a time, each handler's
// returned state replacing the previous one. A web thrall defined with a serve handler runs above
// that: its calls leave the mailbox for a CallPool, and the state init returned becomes a value
// shared by every handler that the runtime never replaces (a handler's returned state is ignored,
// which is why the only way in is a handler shape that has none).
export async function start<S>(def: ThrallDef<S>): Promise<void> {
  const { concurrency, failureReply } = dispatchOf(def);
  const env: Env = readEnv();
  const name = def.name || env.name;
  if (typeof def.init !== "function") throw new Error(`thrall ${name}: init is required`);
  const durable = process.env.AETHER_DURABLE === "1";
  const nc = await open(env);
  useConnection(nc); // so this thrall can call()/cast() other thralls
  const log = newLogger({ component: "thrall", app: env.app, name });
  const singletonEpoch = fenceConfigFromEnv()?.epoch ?? 0;
  // newCtx builds a Ctx whose call/cast carry its own trace. The mailbox has one and rewrites its
  // trace and msgId per message; a concurrent web thrall builds one per request.
  const newCtx = (trace: string, msgId: string): Ctx => {
    const c: Ctx = {
      nats: nc,
      name,
      app: env.app,
      log,
      trace,
      msgId,
      singletonEpoch,
      call: (target, op, payload = {}, opts = {}) => call(target, op, payload, { ...opts, trace: opts.trace || c.trace }),
      cast: (target, op, payload = {}, opts = {}) => cast(target, op, payload, { ...opts, trace: c.trace }),
      castConfirmed: (target, op, payload = {}, opts = {}) => castConfirmed(target, op, payload, { ...opts, trace: c.trace }),
      append: (event, opts) => appendEvent(nc, env.app, name, event, opts),
      startChild: (spec, opts) => startChild(nc, spec, opts),
      stopChild: (childName, opts) => stopChild(nc, childName, opts),
    };
    return c;
  };
  const ctx = newCtx("", "");

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

  // Self-metrics reported on each heartbeat; see MailboxStats.
  const stats = new MailboxStats();
  const snapshot = () => stats.snapshot();
  // Turns away calls whose caller has stopped waiting, before their handler runs.
  const gate = new DeadlineGate(stats, log);

  // The last handler failure, reported in the self-description. Set from a handler, read by the
  // heartbeat - JS is single-threaded, so a plain object needs no lock.
  const lastErr = { msg: "", ms: 0 };
  const setLastError = (msg: string) => {
    lastErr.msg = msg;
    lastErr.ms = Date.now();
  };

  // The self-description attached to every heartbeat: the ops (derived from the handler maps, so
  // the developer declares nothing), the self-declared version and the current last error. Empty
  // fields are omitted to match the Go omitempty wire form.
  const callOps = Object.keys(def.handleCall ?? {}).sort();
  const castOps = Object.keys(def.handleCast ?? {}).sort();
  const describe = (): ThrallDescribe => {
    const d: ThrallDescribe = {};
    if (callOps.length) d.call_ops = callOps;
    if (castOps.length) d.cast_ops = castOps;
    if (def.version) d.version = def.version;
    if (lastErr.msg) {
      d.last_error = lastErr.msg;
      d.last_error_ms = lastErr.ms;
    }
    if (concurrency > SERIALIZED) d.concurrency = concurrency;
    return d;
  };
  // dyingBreath publishes one final heartbeat carrying the last error and flushes it, so the lord
  // learns WHY a thrall escalated before it sees the process exit. Used only on the crash path.
  const dyingBreath = () => {
    publishHeartbeat(nc, name, snapshot, describe);
    return nc.flush();
  };

  // handleCall/handleCast over the serialized mailbox (shared by the core and durable branches).
  // beginJob runs at enqueue time (before the serialized job), so mailbox_depth counts messages
  // waiting in the promise chain - matching the Go/Python SDKs, where begin() runs before the
  // mailbox lock.
  // replier builds the function that answers call e. A reply larger than one bus message cannot be
  // sent at all. Dropping it would leave the caller waiting out its timeout with nothing to go on,
  // so it is told what happened instead.
  const replier = (e: Envelope, respond: (data: Uint8Array) => void) => (reply: Envelope): void => {
    const data = encode(reply);
    try {
      respond(data);
    } catch (err) {
      if ((err as NatsError)?.code !== ErrorCode.MaxPayloadExceeded) throw err;
      const maxPayload = nc.info?.max_payload ?? 0;
      const tooLarge = tooLargeReply(e, data.length, maxPayload);
      setLastError(`${ErrorType.replyTooLarge}: ${tooLarge.error!.message}`);
      log.error("reply too large for the bus", { op: e.op, trace: e.trace, bytes: data.length, max_payload: maxPayload });
      respond(encode(tooLarge));
    }
  };

  // answerFailedCall answers a call whose handler threw. Shared by the mailbox and the concurrent
  // pool, so a failure means the same thing on both.
  const answerFailedCall = async (e: Envelope, trace: string, err: unknown, send: (reply: Envelope) => void): Promise<void> => {
    const esc = asEscalate(err);
    if (esc) {
      // Reply the caller before we crash, so it learns of the restart instead of
      // hanging until timeout; the dying breath then flushes both the reply and the final
      // heartbeat (carrying the reason) before the process exits.
      send(errReply(e, ErrorType.escalated, esc.reason));
      setLastError(`escalated: ${esc.reason}`);
      await dyingBreath();
      log.error("handler escalated - self-terminating for restart", { op: e.op, reason: esc.reason });
      exitProcess(1);
      return;
    }
    const refusal = asRefuse(err);
    if (refusal) {
      // A refusal is the domain saying no, not the thrall failing: the caller gets the
      // code, the state stays as it was, and nothing is recorded as the last error.
      log.debug("call refused", { op: e.op, code: refusal.code, trace });
      send(refusedReply(e, refusal));
      return;
    }
    setLastError(`handler_error: ${String(err)}`);
    if (failureReply) {
      // A web thrall answers its own failures as HTTP. An exception is the one way a handler here
      // can fail, a bug and a deliberate throw alike, so it is contained, counted and logged with
      // the request's trace and the stack - and the thrall carries on.
      stats.panicked();
      log.error("handler failed - the request is answered with a failure, the thrall carries on", {
        op: e.op,
        trace,
        err: String(err),
        stack: err instanceof Error ? err.stack : undefined,
      });
      send(okReply(e, failureReply()));
      return;
    }
    send(errReply(e, ErrorType.handler, String(err)));
  };

  const mailboxCall = (e: Envelope, respond: (data: Uint8Array) => void): Promise<void> => {
    const start = stats.begin();
    const send = replier(e, respond);
    return serialize(async () => {
      if (gate.drop(e, send)) return;
      stats.started();
      ctx.trace = orNewTrace(e.trace);
      ctx.msgId = e.id ?? "";
      log.debug("handling call", { op: e.op, trace: ctx.trace });
      try {
        const handler = def.handleCall?.[e.op ?? ""];
        if (!handler) {
          send(errReply(e, ErrorType.unknownOp, `unknown call op: ${e.op}`));
          return;
        }
        const dkey = dedup ? dedupKey(e) : ""; // empty (no idem and no id) -> not deduped
        if (dkey) {
          const [cached, seen] = dedup!.get(dkey);
          if (seen) {
            // A duplicate call: return the first reply instead of re-running the handler.
            send(okReply(e, cached));
            return;
          }
        }
        // Only the handler is guarded: a reply that cannot be sent is not the handler failing, and
        // must not be answered (or counted) as if it were.
        let reply: unknown;
        try {
          const [result, next] = await handler(e.payload, state, ctx);
          reply = result;
          state = next;
        } catch (err) {
          await answerFailedCall(e, ctx.trace, err, send);
          return;
        }
        if (dkey) dedup!.put(dkey, reply); // cache only a successful reply
        send(okReply(e, reply));
      } finally {
        stats.end(start);
      }
    });
  };

  // onCall is what a received call goes to: the mailbox above, or - for a concurrent web thrall -
  // a pool that runs up to concurrency handlers at once over the state init returned. That state
  // is captured here and never reassigned, so the handlers share it; each gets its own Ctx, since
  // trace and msgId differ per request. Idempotency dedup belongs to the mailbox and is not
  // consulted here (startWeb never asks for it).
  let onCall = mailboxCall;
  let pool: CallPool | undefined;
  if (concurrency > SERIALIZED) {
    const shared = state;
    const calls = (pool = new CallPool(concurrency, gate, stats));
    onCall = (e, respond) => {
      const send = replier(e, respond);
      const served = calls.submit(e, send, async () => {
        const reqCtx = newCtx(orNewTrace(e.trace), e.id ?? "");
        log.debug("handling call", { op: e.op, trace: reqCtx.trace });
        const handler = def.handleCall?.[e.op ?? ""];
        if (!handler) {
          send(errReply(e, ErrorType.unknownOp, `unknown call op: ${e.op}`));
          return;
        }
        let reply: unknown;
        try {
          [reply] = await handler(e.payload, shared, reqCtx);
        } catch (err) {
          await answerFailedCall(e, reqCtx.trace, err, send);
          return;
        }
        send(okReply(e, reply)); // outside the guard: a send that fails is not a handler failure
      });
      // The handler answers its own failures, so a rejection here happened while answering (the
      // bus went away mid-reply). It is logged; nobody awaits this promise, and an unhandled
      // rejection would end the process for one lost reply.
      return served.catch((err) => log.error("serving a call failed outside its handler", { op: e.op, trace: e.trace, err: String(err) }));
    };
  }

  // ackDurable acknowledges the source JetStream message (durable cast); undefined for a
  // non-durable core cast, which needs no ack. On escalation the poison cast is acked before
  // the crash, so it is not redelivered into a loop after the restart.
  const onCast = (e: Envelope, ackDurable?: () => Promise<void>): Promise<void> => {
    const start = stats.begin();
    return serialize(async () => {
      stats.started();
      ctx.trace = orNewTrace(e.trace);
      ctx.msgId = e.id ?? "";
      log.debug("handling cast", { op: e.op, trace: ctx.trace });
      try {
        const handler = def.handleCast?.[e.op ?? ""];
        if (!handler) return;
        const dkey = dedup ? dedupKey(e) : ""; // empty (no idem and no id) -> not deduped
        if (dkey) {
          const [, seen] = dedup!.get(dkey);
          if (seen) return; // already processed; a durable cast is acked by the consume loop
        }
        try {
          state = await handler(e.payload, state, ctx);
          if (dkey) dedup!.put(dkey, undefined); // mark processed only after success
        } catch (err) {
          const esc = asEscalate(err);
          if (esc) {
            // Ack the poison cast before crashing so JetStream does not redeliver it into a
            // crash loop; a non-durable cast (no ackDurable) is simply dropped, as it is today.
            if (ackDurable) await ackDurable();
            setLastError(`escalated: ${esc.reason}`);
            await dyingBreath(); // let the lord record the reason before the process dies
            log.error("cast handler escalated - self-terminating for restart", { op: e.op, reason: esc.reason });
            exitProcess(1);
            return;
          }
          const refusal = asRefuse(err);
          if (refusal) {
            // Nobody waits for a cast, so a refused one is only noted: a normal outcome, not a
            // failure, and the state stays as it was.
            log.info("cast refused", { op: e.op, code: refusal.code, reason: refusal.reason });
            return;
          }
          setLastError(`handler_error: ${String(err)}`);
          log.error("cast handler failed", { op: e.op, err: String(err) });
        }
      } finally {
        stats.end(start);
      }
    });
  };

  // The core subscriptions this thrall reads its messages from; a drain closes them first.
  const inbound: Subscription[] = [];
  if (durable) {
    // Durable thrall: call/info over core (synchronous), cast via JetStream (survives a crash).
    inbound.push(subscribeVerb(nc, subjects.call(env.app, name), (e, msg) => onCall(e, (d) => msg.respond(d))));
    inbound.push(subscribeVerb(nc, subjects.info(env.app, name), () => {})); // TODO handleInfo
    void consumeDurableCast(nc, env.app, name, onCast);
  } else {
    // Non-durable thrall: a single wildcard subscription over call/cast/info (FIFO).
    inbound.push(subscribeData(nc, env.app, name, onCall, onCast));
  }

  // ctl: controlled shutdown from the lord (drain / shutdown)
  const ctlSub = nc.subscribe(subjects.ctl(name));
  void (async () => {
    for await (const msg of ctlSub) {
      const e = decode(msg.data);
      if (e.op === "drain" || e.op === "shutdown") {
        // Stop reading first, as the Go SDK does: with the subscriptions closed nothing new is
        // taken (a caller is told at once that nobody is listening, instead of waiting for a
        // reply that will not come). Then finish what was already taken: the pooled calls that
        // are running or waiting for a slot, then the mailbox.
        for (const sub of inbound) sub.unsubscribe();
        await pool?.drain();
        await tail;
        await def.terminate?.(e.op, state);
        await nc.drain();
        process.exit(0);
      }
    }
  })();

  startHeartbeat(nc, name, snapshot, describe);
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
): Subscription {
  const sub = nc.subscribe(subjects.data(app, name));
  void (async () => {
    for await (const msg of sub) {
      const verb = msg.subject.slice(msg.subject.lastIndexOf(".") + 1);
      const e = decode(msg.data);
      if (verb === "call") onCall(e, (d) => msg.respond(d));
      else if (verb === "cast") onCast(e);
    }
  })();
  return sub;
}

// subscribeVerb: a core subscription on one specific subject.
export function subscribeVerb(
  nc: NatsConnection,
  subject: string,
  handle: (e: Envelope, msg: { respond: (d: Uint8Array) => void }) => void,
): Subscription {
  const sub = nc.subscribe(subject);
  void (async () => {
    for await (const msg of sub) {
      handle(decode(msg.data), msg);
    }
  })();
  return sub;
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

// MailboxStats collects the thrall's self-metrics reported on each heartbeat (mirrors the Go SDK
// mailboxStats): depth = messages currently held, lastMs = duration of the most recent handler,
// processed = cumulative count of handled messages, expired = cumulative count of calls dropped
// unexecuted because their deadline had passed. begin/end bracket every handled message; a dropped
// call ends with drop instead, so it is neither a processed message nor a latency sample.
//
// A message is accounted in three steps: begin when it is received, started when its turn comes
// and its handler is about to run, end when the handler has returned (a dropped call ends with
// drop instead of started/end). So depth - running is what is still waiting.
export class MailboxStats {
  private depth = 0;
  private running = 0;
  private processed = 0;
  private expired = 0;
  private panics = 0; // handler crashes contained by a web thrall
  private lastMs = 0;

  begin(): number {
    this.depth++;
    return performance.now();
  }

  // started marks the message's turn: it stops waiting and its handler runs. Paired with end.
  started(): void {
    this.running++;
  }

  end(start: number): void {
    this.lastMs = performance.now() - start;
    this.processed++;
    this.running--;
    this.depth--;
  }

  drop(): void {
    this.expired++;
    this.depth--;
  }

  // panicked counts a handler crash the thrall contained (the message itself still ends with end).
  panicked(): void {
    this.panics++;
  }

  // snapshot is the heartbeat payload. expired_total and in_flight are omitted while zero, matching
  // the Go omitempty tags, so an idle thrall that never dropped a call reports the shape it always did.
  snapshot(): HeartbeatMetrics {
    const m: HeartbeatMetrics = { mailbox_depth: this.depth, mailbox_latency_ms: this.lastMs, processed_total: this.processed };
    if (this.expired > 0) m.expired_total = this.expired;
    if (this.running > 0) m.in_flight = this.running;
    if (this.panics > 0) m.panics_total = this.panics;
    return m;
  }
}

// DeadlineGate stands in front of a thrall's call handlers and turns away calls whose caller has
// already stopped waiting (see deadline.ts). It is consulted when the call's turn in the serialized
// mailbox comes, i.e. at the last moment before the handler would run.
export class DeadlineGate {
  private readonly skewMs = deadlineSkewMs();

  constructor(
    private readonly stats: MailboxStats,
    private readonly log: Logger,
  ) {}

  // drop reports whether the call has expired. If it has, the call is accounted as dropped (not
  // as processed), logged, and answered with "deadline_exceeded".
  drop(e: Envelope, respond: (reply: Envelope) => void): boolean {
    const overdue = overdueMs(e, Date.now(), this.skewMs);
    if (overdue === undefined) return false;
    this.expire(e, overdue, respond);
    return true;
  }

  // expire turns away a call whose deadline has passed, overdue ms ago.
  expire(e: Envelope, overdue: number, respond: (reply: Envelope) => void): void {
    this.stats.drop();
    this.log.warn("dropping expired call - its caller stopped waiting", { op: e.op, trace: e.trace, overdue_ms: overdue });
    respond(expiredReply(e, overdue));
  }

  // expiresInMs is how long from now this gate would start dropping the call; undefined for a
  // call without a deadline.
  expiresInMs(e: Envelope): number | undefined {
    return expiresInMs(e, Date.now(), this.skewMs);
  }
}

// CallPool is what a concurrent web thrall has in place of the serialized mailbox for its calls:
// it runs up to limit handlers at once and makes the rest wait their turn. "At once" means
// interleaved at their awaits on the one event loop - a handler that never awaits still runs to
// completion alone.
//
// Waiting is bounded by the caller, not by a queue length. A call that finds every slot taken
// waits for one; if its deadline passes first it is dropped right then - unexecuted, accounted and
// answered exactly as the deadline gate drops a call - so calls nobody waits for any more do not
// pile up behind slow handlers. A call that carries no deadline waits as long as it takes. Slots
// are handed out in arrival order, but a concurrent thrall makes no ordering promise.
export class CallPool {
  private active = 0;
  private readonly waiting: Array<{ grant: (taken: boolean) => void; timer?: ReturnType<typeof setTimeout> }> = [];
  private readonly pending = new Set<Promise<void>>();

  constructor(
    private readonly limit: number,
    private readonly gate: DeadlineGate,
    private readonly stats: MailboxStats,
  ) {}

  // submit takes one received call. handle runs when a slot is free and the call's caller is still
  // waiting; otherwise the call is dropped and respond carries the deadline_exceeded reply. The
  // returned promise settles when the call is finished or dropped; it rejects only if handle does.
  submit(e: Envelope, respond: (reply: Envelope) => void, handle: () => Promise<void>): Promise<void> {
    const done = this.serve(e, respond, handle).finally(() => this.pending.delete(done));
    this.pending.add(done);
    return done;
  }

  private async serve(e: Envelope, respond: (reply: Envelope) => void, handle: () => Promise<void>): Promise<void> {
    const received = this.stats.begin();
    if (!(await this.acquire(e))) {
      this.gate.expire(e, Date.now() - (e.deadline ?? 0), respond);
      return;
    }
    try {
      // The slot may have come free at the very moment the deadline passed; the gate decides, as
      // it does for the mailbox, at the last moment before the handler would run.
      if (this.gate.drop(e, respond)) return;
      this.stats.started();
      try {
        await handle();
      } finally {
        this.stats.end(received);
      }
    } finally {
      this.release();
    }
  }

  // acquire waits for a free slot; false means the call's deadline (plus the skew tolerance)
  // passed first, and no slot was taken.
  private acquire(e: Envelope): Promise<boolean> {
    if (this.active < this.limit) {
      this.active++;
      return Promise.resolve(true);
    }
    return new Promise<boolean>((grant) => {
      const waiter: { grant: (taken: boolean) => void; timer?: ReturnType<typeof setTimeout> } = { grant };
      const wait = this.gate.expiresInMs(e);
      if (wait !== undefined) {
        // One past the tolerance, so a call dropped here is expired by the gate's own rule.
        waiter.timer = setTimeout(() => {
          this.waiting.splice(this.waiting.indexOf(waiter), 1);
          grant(false);
        }, Math.max(0, wait) + 1);
      }
      this.waiting.push(waiter);
    });
  }

  // release hands the slot to the longest-waiting call, or frees it.
  private release(): void {
    const next = this.waiting.shift();
    if (!next) {
      this.active--;
      return;
    }
    clearTimeout(next.timer);
    next.grant(true);
  }

  // drain resolves once every submitted call has finished or been dropped. It does not turn
  // calls away: the thrall stops reading before it drains, and a call that was already taken is
  // served like any other.
  async drain(): Promise<void> {
    while (this.pending.size > 0) await Promise.allSettled([...this.pending]);
  }
}

export function okReply(req: Envelope, payload: unknown): Envelope {
  return { v: 1, id: req.id, kind: "reply", status: "ok", payload };
}

// tooLargeReply builds the reply sent in place of one that did not fit a bus message. The text is
// part of the wire contract's golden fixtures, so every SDK words it the same way.
export function tooLargeReply(req: Envelope, size: number, maxPayload: number): Envelope {
  return errReply(req, ErrorType.replyTooLarge, `reply of ${size} bytes does not fit one bus message (at most ${maxPayload} bytes)`);
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

// publishHeartbeat sends one liveness beat carrying the thrall's metrics and, when a describe is
// supplied, its self-description. Shared by the periodic loop and the crash-path dying breath.
export function publishHeartbeat(
  nc: NatsConnection,
  name: string,
  snapshot: () => unknown,
  describe?: () => ThrallDescribe,
): void {
  const payload: Record<string, unknown> = { ...(snapshot() as Record<string, unknown>) };
  if (describe) payload.describe = describe();
  const hb: Envelope = { v: 1, kind: "hb", to: name, payload, ts: Date.now() };
  nc.publish(subjects.hb(name), encode(hb));
}

export function startHeartbeat(
  nc: NatsConnection,
  name: string,
  snapshot: () => unknown,
  describe?: () => ThrallDescribe,
): void {
  const tick = () => publishHeartbeat(nc, name, snapshot, describe);
  tick();
  const timer = setInterval(tick, heartbeatIntervalMs()); // lord-configured interval (default 2s)
  timer.unref?.();
}
