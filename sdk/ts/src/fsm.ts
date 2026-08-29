// FSM is the second thrall behaviour (alongside the GenServer thrall), mirroring the Go SDK's
// StartFSM and OTP's gen_statem: a finite state machine that is always in exactly one named
// state and dispatches an incoming message to the current state's reaction for that op.
// Processing is serialized on a single promise chain (like the GenServer thrall), so a state
// timeout event never interleaves an in-flight handler. Events on the wire are ordinary
// call/cast - the envelope is unchanged.

import type { NatsConnection } from "nats";
import { decode, encode, type Envelope, type ThrallDescribe } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv } from "./connection";
import { useConnection, call, cast, startChild, stopChild, orNewTrace } from "./client";
import { newLogger, type Logger } from "./log";
import { appendEvent } from "./rebuild";
import { startFencingIfSingleton, startLordLivenessFencing, fenceConfigFromEnv } from "./fencing";
import {
  subscribeData,
  subscribeVerb,
  consumeDurableCast,
  startHeartbeat,
  okReply,
  errReply,
  type Ctx,
} from "./thrall";

// FSM_STATE_OP is a reserved call op the machine answers itself with the current state, so it
// is observable from outside without any application code.
const FSM_STATE_OP = "_state";

export interface Event {
  op: string;
  payload: unknown;
  kind: "call" | "cast" | "timeout";
}

// StateTimeout arms a timeout on a state: if no transition out of it happens within `after`
// milliseconds, the machine delivers a timeout event with op `op` to the current state.
export interface StateTimeout {
  after: number; // milliseconds
  op: string;
}

export interface Outcome<D> {
  next?: string; // undefined/"" = stay in the current state
  data: D;
  reply?: unknown; // for call events
  timeout?: StateTimeout; // (re-)arm a state timeout, even while staying
}

export interface Reaction<D> {
  guard?: (data: D, ev: Event) => boolean; // optional; absent = always
  fn: (ev: Event, data: D, ctx: Ctx) => Promise<Outcome<D>> | Outcome<D>;
}

export interface State<D> {
  on: Record<string, Reaction<D>>;
  timeout?: StateTimeout;
}

export interface FSMDef<D> {
  name: string;
  initial: string;
  init: (ctx: Ctx) => Promise<D> | D;
  states: Record<string, State<D>>;
  terminate?: (reason: string, state: string, data: D) => void | Promise<void>;
  // version is the machine's self-declared build, reported in the heartbeat's self-description
  // (see ThrallDef.version). Optional; omitted means unversioned.
  version?: string;
}

// fsmDescribe builds an FSM's self-description: the union of every state's reaction ops (each is
// dispatchable as a call or a cast, so it appears in both sets), plus the reserved _state call op.
// The developer declares no operations - they are the reaction keys already present in the states.
export function fsmDescribe<D>(def: FSMDef<D>): ThrallDescribe {
  const events = [...new Set(Object.values(def.states).flatMap((s) => Object.keys(s.on)))].sort();
  const d: ThrallDescribe = { call_ops: [...events, FSM_STATE_OP].sort() };
  if (events.length) d.cast_ops = events;
  if (def.version) d.version = def.version;
  return d;
}

// Machine is the serialized state-machine core, independent of NATS (so it is unit-testable).
export interface Machine<D> {
  send(ev: Event, req: Envelope | undefined, respond?: (reply: Envelope) => void): Promise<void>;
  state(): string;
  data(): D;
  snapshot(): { mailbox_depth: number; mailbox_latency_ms: number; processed_total: number };
  drain(): Promise<void>; // resolves when the currently-enqueued mailbox jobs have completed
  stop(): void;
}

// createMachine builds the serialized dispatch + timeout machinery for an FSM. All state
// mutation happens on one promise chain, so events never interleave.
export function createMachine<D>(def: FSMDef<D>, ctx: Ctx, initialData: D, log: Logger): Machine<D> {
  let current = def.initial;
  let data = initialData;

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

  // State-timeout machinery: a generation token so a timer that fires after being superseded
  // (a transition or a re-arm) is recognized as stale and ignored.
  let timeoutGen = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const armTimeout = (to?: StateTimeout): void => {
    timeoutGen++;
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (!to) return;
    const gen = timeoutGen;
    const op = to.op;
    timer = setTimeout(() => {
      void send({ op, payload: {}, kind: "timeout" }, undefined, undefined, gen);
    }, to.after);
    timer.unref?.();
  };

  const enter = (next: string, override?: StateTimeout): void => {
    const from = current;
    if (!def.states[next]) {
      log.warn("fsm transition to unknown state", { from, to: next });
    }
    current = next;
    log.info("fsm transition", { from, to: next });
    armTimeout(override ?? def.states[next]?.timeout);
  };

  const unhandled = (
    ev: Event,
    req: Envelope | undefined,
    respond: ((reply: Envelope) => void) | undefined,
    type: string,
    message: string,
  ): void => {
    log.warn("fsm unhandled event", { state: current, op: ev.op, kind: ev.kind, reason: type });
    if (respond && req) respond(errReply(req, type, message));
  };

  const dispatch = async (
    ev: Event,
    req: Envelope | undefined,
    respond: ((reply: Envelope) => void) | undefined,
  ): Promise<void> => {
    ctx.trace = orNewTrace(ev.kind === "timeout" ? undefined : req?.trace);
    ctx.msgId = req?.id ?? "";
    log.debug("fsm event", { state: current, op: ev.op, kind: ev.kind, trace: ctx.trace });

    if (ev.kind === "call" && ev.op === FSM_STATE_OP) {
      if (respond && req) respond(okReply(req, { state: current }));
      return;
    }

    const r = def.states[current]?.on[ev.op];
    if (!r) {
      unhandled(ev, req, respond, "no_transition", `no transition for op ${ev.op} in state ${current}`);
      return;
    }
    if (r.guard && !r.guard(data, ev)) {
      unhandled(ev, req, respond, "guard_rejected", `guard rejected op ${ev.op} in state ${current}`);
      return;
    }

    let out: Outcome<D>;
    try {
      out = await r.fn(ev, data, ctx);
    } catch (err) {
      log.error("fsm handler failed", { state: current, op: ev.op, err: String(err) });
      if (respond && req) respond(errReply(req, "handler_error", String(err)));
      return;
    }
    data = out.data;
    if (respond && req) respond(okReply(req, out.reply));
    if (out.next && out.next !== current) enter(out.next, out.timeout);
    else if (out.timeout) armTimeout(out.timeout);
  };

  // send enqueues an event onto the serialized chain. `gen` (set only by the timer) drops a
  // stale timeout that a newer arm/transition has superseded.
  function send(
    ev: Event,
    req: Envelope | undefined,
    respond?: (reply: Envelope) => void,
    gen?: number,
  ): Promise<void> {
    const start = beginJob();
    return serialize(async () => {
      try {
        if (gen !== undefined && gen !== timeoutGen) return; // superseded timeout
        await dispatch(ev, req, respond);
      } finally {
        endJob(start);
      }
    });
  }

  armTimeout(def.states[current]?.timeout); // arm the initial state's timeout

  return {
    send: (ev, req, respond) => send(ev, req, respond),
    state: () => current,
    data: () => data,
    snapshot: () => ({ mailbox_depth: stats.depth, mailbox_latency_ms: stats.lastMs, processed_total: stats.processed }),
    drain: () => tail,
    stop: () => {
      timeoutGen++;
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
    },
  };
}

// defFSM is a typed identity helper (mirrors defThrall).
export function defFSM<D>(def: FSMDef<D>): FSMDef<D> {
  return def;
}

export async function startFSM<D>(def: FSMDef<D>): Promise<void> {
  const env = readEnv();
  const name = def.name || env.name;
  if (!def.initial) throw new Error(`fsm ${name}: initial state is required`);
  if (typeof def.init !== "function") throw new Error(`fsm ${name}: init is required`);
  if (!def.states[def.initial]) throw new Error(`fsm ${name}: initial state ${def.initial} is not in states`);
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
    msgId: "",
    singletonEpoch: fenceConfigFromEnv()?.epoch ?? 0,
    call: (target, op, payload = {}, opts = {}) => call(target, op, payload, { ...opts, trace: ctx.trace }),
    cast: (target, op, payload = {}) => cast(target, op, payload, { trace: ctx.trace }),
    append: (event, opts) => appendEvent(nc, env.app, name, event, opts),
    startChild: (spec, opts) => startChild(nc, spec, opts),
    stopChild: (childName, opts) => stopChild(nc, childName, opts),
  };

  const machine = createMachine(def, ctx, await def.init(ctx), log);

  // onCall/onCast return the send promise so the durable consumer can await processing before
  // it acks (process-then-ack, at-least-once) - matching the Go/Python SDKs and the GenServer.
  const onCall = (e: Envelope, respond: (d: Uint8Array) => void): Promise<void> =>
    machine.send({ op: e.op ?? "", payload: e.payload, kind: "call" }, e, (reply) => respond(encode(reply)));
  const onCast = (e: Envelope): Promise<void> =>
    machine.send({ op: e.op ?? "", payload: e.payload, kind: "cast" }, e);

  if (durable) {
    subscribeVerb(nc, subjects.call(env.app, name), (e, msg) => void onCall(e, (d) => msg.respond(d)));
    subscribeVerb(nc, subjects.info(env.app, name), () => {}); // info is out-of-band; not an FSM event yet
    void consumeDurableCast(nc, env.app, name, onCast); // awaits onCast -> processes before ack
  } else {
    subscribeData(nc, env.app, name, onCall, onCast);
  }

  const ctlSub = nc.subscribe(subjects.ctl(name));
  void (async () => {
    for await (const msg of ctlSub) {
      const e = decode(msg.data);
      if (e.op === "drain" || e.op === "shutdown") {
        await machine.drain(); // finish the in-flight mailbox before terminating
        machine.stop();
        await def.terminate?.(e.op, machine.state(), machine.data());
        await nc.drain();
        process.exit(0);
      }
    }
  })();

  startHeartbeat(nc, name, () => machine.snapshot(), () => fsmDescribe(def));
  await startFencingIfSingleton(nc, name, log);
  await startLordLivenessFencing(nc, name, log);
}
