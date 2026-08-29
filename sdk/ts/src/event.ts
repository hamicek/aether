// EventManager is the third thrall behaviour (alongside the GenServer thrall and the FSM),
// mirroring the Go SDK's StartEvent and OTP's gen_event: an event manager holding an ordered
// set of handlers. One async event (a cast to the manager's name) is dispatched to EVERY handler
// in registration order on the same serialized mailbox, and each handler keeps its own state.
// This is what raw NATS fan-out (N independent subscribers) does NOT give: co-located, ordered
// handlers. A handler that throws is logged and skipped; the others still run.
//
// v1 scope: handlers are declared statically; events are async (cast). A call to an event manager
// is answered with an error rather than a silent timeout.

import type { NatsConnection } from "nats";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv } from "./connection";
import { useConnection, call, cast, startChild, stopChild, orNewTrace } from "./client";
import { newLogger, type Logger } from "./log";
import { appendEvent } from "./rebuild";
import { startFencingIfSingleton, startLordLivenessFencing, fenceConfigFromEnv } from "./fencing";
import { subscribeData, subscribeVerb, consumeDurableCast, startHeartbeat, errReply, type Ctx } from "./thrall";

// EventMsg is one input to the manager: an op and its payload. (Events are async casts; there is
// no per-event reply in v1.)
export interface EventMsg {
  op: string;
  payload: unknown;
}

// EventHandler is one reaction registered in the manager. It keeps its own state, seeded by init
// and threaded through handleEvent, which returns the handler's new state.
export interface EventHandler<S = any> {
  init?: (ctx: Ctx) => Promise<S> | S;
  handleEvent: (ev: EventMsg, state: S, ctx: Ctx) => Promise<S> | S;
}

// EventManagerDef defines an event-manager thrall. `handlers` is keyed by handler name; the object
// insertion order IS the registration (dispatch) order.
export interface EventManagerDef {
  name: string;
  handlers: Record<string, EventHandler>;
  terminate?: (reason: string) => void | Promise<void>;
  // version is the manager's self-declared build, reported in the heartbeat's self-description
  // (see ThrallDef.version). Optional; omitted means unversioned.
  version?: string;
}

// EventBus is the serialized fan-out core, independent of NATS (so it is unit-testable).
export interface EventBus {
  send(ev: EventMsg, trace?: string): Promise<void>;
  stateOf(name: string): unknown;
  snapshot(): { mailbox_depth: number; mailbox_latency_ms: number; processed_total: number };
  drain(): Promise<void>; // resolves when the enqueued mailbox jobs have completed
}

// createEventBus builds the serialized fan-out. All handler state mutation happens on one promise
// chain, so events never interleave and each handler keeps its state to itself.
export function createEventBus(
  def: EventManagerDef,
  ctx: Ctx,
  initialStates: Record<string, unknown>,
  log: Logger,
): EventBus {
  const names = Object.keys(def.handlers);
  const states: Record<string, unknown> = { ...initialStates };

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

  let tail: Promise<void> = Promise.resolve();
  const serialize = (job: () => Promise<void>): Promise<void> => {
    tail = tail.then(job, job);
    return tail;
  };

  const dispatch = async (ev: EventMsg, trace?: string): Promise<void> => {
    ctx.trace = orNewTrace(trace);
    log.debug("event", { op: ev.op, handlers: names.length, trace: ctx.trace });
    for (const name of names) {
      try {
        // On success the handler's state advances; a throw leaves it unchanged (the assignment
        // never runs) and is isolated so the remaining handlers still process the event.
        states[name] = await def.handlers[name].handleEvent(ev, states[name], ctx);
      } catch (err) {
        log.error("event handler failed", { handler: name, op: ev.op, err: String(err) });
      }
    }
  };

  function send(ev: EventMsg, trace?: string): Promise<void> {
    const start = beginJob();
    return serialize(async () => {
      try {
        await dispatch(ev, trace);
      } finally {
        endJob(start);
      }
    });
  }

  return {
    send,
    stateOf: (name) => states[name],
    snapshot: () => ({ mailbox_depth: stats.depth, mailbox_latency_ms: stats.lastMs, processed_total: stats.processed }),
    drain: () => tail,
  };
}

// defEvent is a typed identity helper (mirrors defThrall / defFSM).
export function defEvent(def: EventManagerDef): EventManagerDef {
  return def;
}

export async function startEvent(def: EventManagerDef): Promise<void> {
  const env = readEnv();
  const name = def.name || env.name;
  const handlerNames = Object.keys(def.handlers ?? {});
  if (handlerNames.length === 0) throw new Error(`event manager ${name}: at least one handler is required`);
  for (const hn of handlerNames) {
    if (typeof def.handlers[hn].handleEvent !== "function") {
      throw new Error(`event manager ${name}: handler ${hn} has no handleEvent`);
    }
  }
  const durable = process.env.AETHER_DURABLE === "1";
  const nc: NatsConnection = await open(env);
  useConnection(nc);
  const log = newLogger({ component: "thrall", app: env.app, name });
  const ctx: Ctx = {
    nats: nc,
    name,
    app: env.app,
    log,
    trace: "",
    msgId: "", // gen_event is fan-out notification, not command handling; no per-event id is threaded
    singletonEpoch: fenceConfigFromEnv()?.epoch ?? 0,
    call: (target, op, payload = {}, opts = {}) => call(target, op, payload, { ...opts, trace: ctx.trace }),
    cast: (target, op, payload = {}) => cast(target, op, payload, { trace: ctx.trace }),
    append: (event, opts) => appendEvent(nc, env.app, name, event, opts),
    startChild: (spec, opts) => startChild(nc, spec, opts),
    stopChild: (childName, opts) => stopChild(nc, childName, opts),
  };

  const initialStates: Record<string, unknown> = {};
  for (const hn of handlerNames) {
    const h = def.handlers[hn];
    initialStates[hn] = h.init ? await h.init(ctx) : undefined;
  }
  const bus = createEventBus(def, ctx, initialStates, log);

  const onCast = (e: Envelope): Promise<void> => bus.send({ op: e.op ?? "", payload: e.payload }, e.trace);
  // A call to an event manager is answered with an error - v1 events are async (a clear reply
  // beats a silent timeout on the caller).
  const onCall = (e: Envelope, respond: (d: Uint8Array) => void): void => {
    respond(encode(errReply(e, "events_async", "event manager is async: use cast, not call")));
  };

  if (durable) {
    subscribeVerb(nc, subjects.call(env.app, name), (e, msg) => onCall(e, (d) => msg.respond(d)));
    subscribeVerb(nc, subjects.info(env.app, name), () => {}); // info is out-of-band; not an event yet
    void consumeDurableCast(nc, env.app, name, onCast); // awaits onCast -> processes before ack
  } else {
    subscribeData(nc, env.app, name, onCall, onCast);
  }

  const ctlSub = nc.subscribe(subjects.ctl(name));
  void (async () => {
    for await (const msg of ctlSub) {
      const e = decode(msg.data);
      if (e.op === "drain" || e.op === "shutdown") {
        await bus.drain(); // finish the in-flight mailbox before terminating
        await def.terminate?.(e.op);
        await nc.drain();
        process.exit(0);
      }
    }
  })();

  // An event manager fans every event out to all handlers, so it declares no per-op contract; its
  // self-description carries only the version.
  startHeartbeat(nc, name, () => bus.snapshot(), () => (def.version ? { version: def.version } : {}));
  await startFencingIfSingleton(nc, name, log);
  await startLordLivenessFencing(nc, name, log);
}
