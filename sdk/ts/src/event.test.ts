import { test, expect } from "bun:test";
import { createEventBus, startEvent, type EventManagerDef, type EventBus } from "./event";
import { newLogger } from "./log";
import { type Ctx } from "./thrall";

// A minimal ctx: the handlers in these tests do not touch NATS.
function testCtx(): Ctx {
  return {
    nats: {} as unknown as Ctx["nats"],
    name: "bus",
    app: "test",
    log: newLogger({ component: "test" }, { level: "error", format: "json", write: () => {} }),
    trace: "",
    msgId: "",
    singletonEpoch: 0,
    call: (() => Promise.resolve(undefined)) as Ctx["call"],
    cast: () => {},
    append: async () => {},
    startChild: async () => "",
    stopChild: async () => {},
  };
}

// busFor builds an EventBus from a def, seeding each handler's initial state (as startEvent does).
async function busFor(def: EventManagerDef): Promise<EventBus> {
  const ctx = testCtx();
  const initial: Record<string, unknown> = {};
  for (const name of Object.keys(def.handlers)) {
    const h = def.handlers[name];
    initial[name] = h.init ? await h.init(ctx) : undefined;
  }
  return createEventBus(def, ctx, initial, ctx.log);
}

test("dispatches to handlers in registration order", async () => {
  const seq: string[] = [];
  const rec = (name: string) => ({ handleEvent: (_e: unknown, s: unknown) => (seq.push(name), s) });
  const bus = await busFor({ name: "bus", handlers: { a: rec("a"), b: rec("b"), c: rec("c") } });
  await bus.send({ op: "ping", payload: {} });
  expect(seq).toEqual(["a", "b", "c"]);
});

test("an event reaches every handler", async () => {
  const seq: string[] = [];
  const rec = (name: string) => ({ handleEvent: (_e: unknown, s: unknown) => (seq.push(name), s) });
  const handlers = { a: rec("a"), b: rec("b"), c: rec("c") };
  const bus = await busFor({ name: "bus", handlers });
  await bus.send({ op: "ping", payload: {} });
  expect(seq.length).toBe(Object.keys(handlers).length);
});

test("handler state is isolated", async () => {
  const counter = (step: number) => ({
    init: () => 0,
    handleEvent: (_e: unknown, s: number) => s + step,
  });
  const bus = await busFor({ name: "bus", handlers: { a: counter(1), b: counter(10) } });
  await bus.send({ op: "tick", payload: {} });
  await bus.send({ op: "tick", payload: {} });
  expect(bus.stateOf("a")).toBe(2);
  expect(bus.stateOf("b")).toBe(20);
});

test("a failing handler is isolated - the others still run", async () => {
  let healthy = 0;
  const bus = await busFor({
    name: "bus",
    handlers: {
      bad: {
        handleEvent: () => {
          throw new Error("boom");
        },
      },
      good: {
        init: () => 0,
        handleEvent: (_e: unknown, s: number) => {
          healthy++;
          return s + 1;
        },
      },
    },
  });
  await bus.send({ op: "e", payload: {} });
  await bus.send({ op: "e", payload: {} });
  expect(healthy).toBe(2); // a failing sibling must not skip the healthy handler
  expect(bus.stateOf("good")).toBe(2);
});

test("startEvent rejects a manager with no handlers", async () => {
  process.env.AETHER_NATS_URL = "nats://127.0.0.1:1"; // never dialled: the guard fires first
  process.env.AETHER_APP = "test";
  process.env.AETHER_NAME = "empty";
  await expect(startEvent({ name: "empty", handlers: {} })).rejects.toThrow("at least one handler");
});
