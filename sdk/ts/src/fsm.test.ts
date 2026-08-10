import { test, expect } from "bun:test";
import { createMachine, type FSMDef, type Machine } from "./fsm";
import { newLogger } from "./log";
import type { Ctx } from "./thrall";
import type { Envelope } from "./envelope";

// A minimal ctx: the FSM handlers in these tests do not touch NATS.
function testCtx(): Ctx {
  return {
    nats: {} as unknown as Ctx["nats"],
    name: "t",
    app: "test",
    log: newLogger({ component: "test" }, { level: "error", format: "json", write: () => {} }),
    trace: "",
    call: (() => Promise.resolve(undefined)) as Ctx["call"],
    cast: () => {},
    startChild: async () => "",
    stopChild: async () => {},
  };
}

// turnstile mirrors the Go test machine: locked <-> unlocked, data counts pushes; "unlocked"
// has a coin reaction whose guard always rejects, and a "boom" that throws.
function turnstile(): FSMDef<number> {
  return {
    name: "turnstile",
    initial: "locked",
    init: () => 0,
    states: {
      locked: {
        on: { coin: { fn: (_e, d) => ({ next: "unlocked", data: d }) } },
      },
      unlocked: {
        on: {
          push: { fn: (_e, d) => ({ next: "locked", data: d + 1, reply: d + 1 }) },
          coin: { guard: () => false, fn: (_e, d) => ({ data: d }) },
          boom: {
            fn: () => {
              throw new Error("boom");
            },
          },
        },
      },
    },
  };
}

async function machineFor(def: FSMDef<number>): Promise<Machine<number>> {
  const ctx = testCtx();
  return createMachine(def, ctx, await Promise.resolve(def.init(ctx)), ctx.log);
}

async function callOp(m: Machine<number>, op: string): Promise<Envelope> {
  let reply: Envelope = { v: 1, kind: "reply" };
  await m.send({ op, payload: {}, kind: "call" }, { v: 1, kind: "call", op }, (r) => {
    reply = r;
  });
  return reply;
}

async function castOp(m: Machine<number>, op: string): Promise<void> {
  await m.send({ op, payload: {}, kind: "cast" }, { v: 1, kind: "cast", op });
}

async function waitState(m: Machine<number>, want: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (m.state() === want) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error(`timeout waiting for state ${want}, still in ${m.state()}`);
}

test("starts in the initial state", async () => {
  const m = await machineFor(turnstile());
  expect(m.state()).toBe("locked");
});

test("transitions and replies on a call", async () => {
  const m = await machineFor(turnstile());
  const coin = await callOp(m, "coin");
  expect(coin.status).toBe("ok");
  expect(m.state()).toBe("unlocked");

  const push = await callOp(m, "push");
  expect(push.status).toBe("ok");
  expect(push.payload).toBe(1);
  expect(m.state()).toBe("locked");
  expect(m.data()).toBe(1);
});

test("unhandled event errors with no_transition and keeps state", async () => {
  const m = await machineFor(turnstile());
  const reply = await callOp(m, "push"); // no push in locked
  expect(reply.status).toBe("error");
  expect(reply.error?.type).toBe("no_transition");
  expect(m.state()).toBe("locked");
});

test("guard rejects the transition", async () => {
  const m = await machineFor(turnstile());
  await callOp(m, "coin"); // -> unlocked
  const reply = await callOp(m, "coin"); // guard false
  expect(reply.status).toBe("error");
  expect(reply.error?.type).toBe("guard_rejected");
  expect(m.state()).toBe("unlocked");
});

test("handler error replies with handler_error", async () => {
  const m = await machineFor(turnstile());
  await callOp(m, "coin");
  const reply = await callOp(m, "boom");
  expect(reply.status).toBe("error");
  expect(reply.error?.type).toBe("handler_error");
});

test("reserved _state op returns the current state", async () => {
  const m = await machineFor(turnstile());
  await callOp(m, "coin");
  const reply = await callOp(m, "_state");
  expect(reply.status).toBe("ok");
  expect((reply.payload as { state: string }).state).toBe("unlocked");
});

test("cast transitions without a reply", async () => {
  const m = await machineFor(turnstile());
  await castOp(m, "coin");
  expect(m.state()).toBe("unlocked");
});

test("send resolves only after the event is processed (durable ack-after-process contract)", async () => {
  // The durable consumer awaits onCast (= machine.send) before acking; that promise must not
  // resolve until the event has actually been handled, or a crash after ack loses the message.
  const m = await machineFor(turnstile());
  await m.send({ op: "coin", payload: {}, kind: "cast" }, { v: 1, kind: "cast", op: "coin" });
  expect(m.state()).toBe("unlocked");
});

test("drain resolves after in-flight events complete", async () => {
  const m = await machineFor(turnstile());
  void m.send({ op: "coin", payload: {}, kind: "cast" }, { v: 1, kind: "cast", op: "coin" });
  await m.drain();
  expect(m.state()).toBe("unlocked");
});

// timeoutMachine: "waiting" auto-transitions to "expired" after a state timeout, unless "ping"
// moves it to "active" (no timeout) first.
function timeoutMachine(afterMs: number): FSMDef<number> {
  return {
    name: "tmo",
    initial: "waiting",
    init: () => 0,
    states: {
      waiting: {
        on: {
          ping: { fn: (_e, d) => ({ next: "active", data: d }) },
          tick: { fn: (_e, d) => ({ next: "expired", data: d + 1 }) },
        },
        timeout: { after: afterMs, op: "tick" },
      },
      active: { on: {} },
      expired: { on: {} },
    },
  };
}

test("state timeout fires a transition", async () => {
  const m = await machineFor(timeoutMachine(30));
  await waitState(m, "expired", 1000);
  expect(m.data()).toBe(1);
});

test("an event cancels the state timeout", async () => {
  const m = await machineFor(timeoutMachine(30));
  await castOp(m, "ping"); // -> active before 30ms
  expect(m.state()).toBe("active");
  await new Promise((r) => setTimeout(r, 80));
  expect(m.state()).toBe("active"); // timeout did not fire after leaving the state
});

test("Outcome.timeout re-arms while staying", async () => {
  const def: FSMDef<number> = {
    name: "rearm",
    initial: "idle",
    init: () => 0,
    states: {
      idle: {
        on: {
          poke: { fn: (_e, d) => ({ data: d + 1, timeout: { after: 30, op: "done" } }) },
          done: { fn: (_e, d) => ({ next: "finished", data: d }) },
        },
      },
      finished: { on: {} },
    },
  };
  const m = await machineFor(def);
  await castOp(m, "poke");
  expect(m.state()).toBe("idle");
  await waitState(m, "finished", 1000);
  expect(m.data()).toBe(1);
});
