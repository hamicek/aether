// Edge is the fourth thrall shape in the SDK (alongside the GenServer thrall, the FSM and the
// EventManager), mirroring the Go SDK's StartEdge - but unlike them it is NOT a behaviour: it has no
// mailbox. An edge owns a socket/connection whose input arrives from OUTSIDE the ether (a push - HTTP,
// a driver), which is concurrent and does not fit a serialized mailbox. The user supplies a run-loop
// (owning the socket) and an optional graceful-stop hook instead of call/cast handlers; the rest -
// connect, heartbeat, drain, fencing - is the same machinery every thrall gets.
//
// It is the "model B" edge (you write the code): the counterpart to the declarative built-in HTTP
// ingress (model A, [[edge.http]], lord-side). State lives in ordinary thralls behind it, via ctx.call/cast.

import type { NatsConnection } from "nats";
import { decode } from "./envelope";
import { subjects } from "./subjects";
import { open, readEnv } from "./connection";
import { useConnection, call, cast, startChild, stopChild } from "./client";
import { newLogger } from "./log";
import { appendEvent } from "./rebuild";
import { startFencingIfSingleton, startLordLivenessFencing } from "./fencing";
import { startHeartbeat, type Ctx } from "./thrall";

// EdgeDef defines an edge thrall: a run-loop that owns the socket and an optional graceful-stop hook.
export interface EdgeDef {
  name?: string; // taken from AETHER_NAME when omitted
  // init runs once before run, for setup that may fail (open a listener, dial a device).
  init?: (ctx: Ctx) => Promise<void> | void;
  // run is the socket-owning loop. It runs until `stop` resolves (a drain from the lord) and must honor
  // it. Resolving is a clean stop; throwing ends the edge ABNORMALLY (the process exits non-zero, so the
  // lord restarts it per its restart policy). Clean up resources opened in init at the end of run - `stop`
  // (the hook) is only a drain-time unblocker, not general cleanup.
  run: (ctx: Ctx, stop: Promise<void>) => Promise<void>;
  // stop is an optional hook invoked ONLY on a drain, to unblock run (e.g. server.close()). It is not
  // called when run resolves/throws on its own.
  stop?: () => void | Promise<void>;
}

// defEdge is a typed identity helper.
export function defEdge(def: EdgeDef): EdgeDef {
  return def;
}

// zeroMetrics is the heartbeat snapshot for an edge: it has no mailbox, so it reports zeros (the shape
// the lord's reaper expects).
const zeroMetrics = () => ({ mailbox_depth: 0, mailbox_latency_ms: 0, processed_total: 0 });

// startEdge connects an edge thrall to the ether and runs its lifecycle. It mirrors start / startFSM /
// startEvent - reusing the shared connect, heartbeat, ctl-drain and fencing plumbing - but runs the
// user's run-loop in place of a serialized mailbox. Unlike the others (which return once wiring is set up
// and let the process live on its subscriptions), startEdge AWAITS the run-loop: it resolves on a clean
// stop and REJECTS if the run-loop throws, so the process exits non-zero and the lord restarts it.
export async function startEdge(def: EdgeDef): Promise<void> {
  const env = readEnv();
  const name = def.name || env.name;
  if (typeof def.run !== "function") throw new Error(`edge ${name}: run is required`);

  const nc: NatsConnection = await open(env);
  useConnection(nc);
  const log = newLogger({ component: "thrall", app: env.app, name });
  const ctx: Ctx = {
    nats: nc,
    name,
    app: env.app,
    log,
    trace: "",
    call: (target, op, payload = {}, opts = {}) => call(target, op, payload, { ...opts, trace: ctx.trace }),
    cast: (target, op, payload = {}) => cast(target, op, payload, { trace: ctx.trace }),
    append: (event) => appendEvent(nc, env.app, name, event),
    startChild: (spec, opts) => startChild(nc, spec, opts),
    stopChild: (childName, opts) => stopChild(nc, childName, opts),
  };

  if (def.init) await def.init(ctx);

  // stop is resolved once, from either the ctl drain or a self-terminating run-loop (the guard makes the
  // two racing sources safe) - the analogue of Go's stop channel + sync.Once.
  let stopped = false;
  let stopResolve!: () => void;
  const stop = new Promise<void>((r) => {
    stopResolve = r;
  });
  const closeStop = () => {
    if (!stopped) {
      stopped = true;
      stopResolve();
    }
  };

  // Run the run-loop as a tracked promise; a throw is captured (not left unhandled) and surfaced below.
  let runErr: unknown;
  const runDone = Promise.resolve()
    .then(() => def.run(ctx, stop))
    .catch((e) => {
      runErr = e ?? new Error("edge run-loop failed");
      log.error("edge run-loop failed", { err: String(e) });
    });

  // fail tears down after the run-loop is already running, so an error setting up ctl/fencing does not
  // leave the run-loop holding its socket with no way to be told to stop (mirrors Go's fail helper).
  const fail = async (err: unknown): Promise<never> => {
    closeStop();
    await runDone;
    await nc.drain();
    throw err;
  };

  // ctl: controlled shutdown from the lord. Unlike the other behaviours (which process.exit(0) here), the
  // edge only signals stop and drains gracefully below.
  const ctlSub = nc.subscribe(subjects.ctl(name));
  void (async () => {
    for await (const msg of ctlSub) {
      const e = decode(msg.data);
      if (e.op === "drain" || e.op === "shutdown") {
        closeStop();
        break;
      }
    }
  })();

  startHeartbeat(nc, name, zeroMetrics);
  try {
    await startFencingIfSingleton(nc, name, log);
    await startLordLivenessFencing(nc, name, log);
  } catch (e) {
    return fail(e);
  }

  // Await whichever settles first (Go's select { <-stop / <-runDone }).
  const winner = await Promise.race([
    stop.then(() => "drain" as const),
    runDone.then(() => "run" as const),
  ]);

  if (winner === "drain") {
    // Drain from the lord: run the graceful hook (unblocks run), then wait for it to finish.
    await def.stop?.();
    await runDone;
    await nc.drain();
    return;
  }
  // The run-loop ended on its own: wind down and drain. A run-loop error is rethrown so the process exits
  // non-zero (abnormal), letting the lord restart per policy; a clean resolve is a normal stop.
  closeStop();
  await nc.drain();
  if (runErr !== undefined) throw runErr;
}
