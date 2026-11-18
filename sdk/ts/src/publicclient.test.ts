import { test, expect } from "bun:test";
import type { NatsConnection } from "nats";
import { Client, CallError, asRefusal, refusalCode, call, useConnection } from "./client";
import { decode, encode, type Envelope, type WireError } from "./envelope";

// fakeBus is a NatsConnection that records what a client sends and answers calls through `reply`,
// so the client side is tested without a server (the real bus is covered by dispatch.test.ts).
function fakeBus(reply: (req: Envelope) => Envelope = (req) => ({ v: 1, id: req.id, kind: "reply", status: "ok", payload: null })) {
  const sent: { subject: string; env: Envelope; timeout?: number }[] = [];
  const nc = {
    request: async (subject: string, data: Uint8Array, opts?: { timeout?: number }) => {
      const env = decode(data);
      sent.push({ subject, env, timeout: opts?.timeout });
      return { data: encode(reply(env)) };
    },
    publish: (subject: string, data: Uint8Array) => {
      sent.push({ subject, env: decode(data) });
    },
  } as unknown as NatsConnection;
  return { nc, sent, last: () => sent[sent.length - 1]! };
}

// failure awaits a call that must reject and returns what it rejected with.
async function failure(call: Promise<unknown>): Promise<CallError> {
  try {
    await call;
  } catch (err) {
    return err as CallError;
  }
  throw new Error("the call did not fail");
}

const errorReply = (error: WireError) => (req: Envelope): Envelope => ({ v: 1, id: req.id, kind: "reply", status: "error", error });

test("a Client needs only a connection and stamps when it stops waiting", async () => {
  const bus = fakeBus((req) => ({ v: 1, id: req.id, kind: "reply", status: "ok", payload: { amount: 7 } }));
  const before = Date.now();

  const got = await new Client(bus.nc, "shop").call<{ amount: number }>("account", "balance", { owner: "ada" }, { timeoutMs: 2000 });

  expect(got).toEqual({ amount: 7 });
  const { subject, env, timeout } = bus.last();
  expect(subject).toBe("aether.shop.account.call");
  expect(timeout).toBe(2000);
  expect(env.kind).toBe("call");
  expect(env.to).toBe("account");
  expect(env.op).toBe("balance");
  expect(env.payload).toEqual({ owner: "ada" });
  expect((env.id ?? "").length).toBeGreaterThan(0);
  expect((env.trace ?? "").length).toBeGreaterThan(0);
  expect(env.idem).toBeUndefined();
  // The deadline is the send time plus the timeout: when this caller stops waiting.
  expect(env.deadline).toBe(env.ts! + 2000);
  expect(env.deadline!).toBeGreaterThanOrEqual(before + 2000);
});

test("a call without an explicit timeout carries the default one as its deadline", async () => {
  const bus = fakeBus();
  await new Client(bus.nc, "shop").call("account", "balance");
  expect(bus.last().env.deadline).toBe(bus.last().env.ts! + 5000);
});

test("the app a message goes to is explicit: the client's own, another per send, or a derived client", async () => {
  const bus = fakeBus();
  const own = new Client(bus.nc, "ujito");

  await own.call("store", "get");
  expect(bus.last().subject).toBe("aether.ujito.store.call");

  await own.call("store", "get", {}, { app: "ae" });
  expect(bus.last().subject).toBe("aether.ae.store.call");

  await own.with({ app: "ae" }).call("store", "get");
  expect(bus.last().subject).toBe("aether.ae.store.call");

  own.cast("audit", "note", { n: 1 }, { app: "ae" });
  expect(bus.last().subject).toBe("aether.ae.audit.cast");
  expect(bus.last().env.kind).toBe("cast");
  expect(bus.last().env.deadline).toBeUndefined(); // nobody waits for a cast

  expect(own.app).toBe("ujito"); // with() leaves the client it derives from untouched
});

test("the caller's trace and idempotency key reach the target", async () => {
  const bus = fakeBus();
  const client = new Client(bus.nc, "shop");

  await client.call("account", "withdraw", {}, { trace: "t-req-1", idempotencyKey: "withdraw-42" });
  expect(bus.last().env.trace).toBe("t-req-1");
  expect(bus.last().env.idem).toBe("withdraw-42");

  const perRequest = client.with({ trace: "t-req-2" });
  await perRequest.call("account", "balance");
  expect(bus.last().env.trace).toBe("t-req-2");
  await perRequest.call("account", "balance", {}, { trace: "t-override" });
  expect(bus.last().env.trace).toBe("t-override");
  perRequest.cast("audit", "note");
  expect(bus.last().env.trace).toBe("t-req-2");
});

test("a refusal rejects with a CallError exposing type, code and reason", async () => {
  const bus = fakeBus(errorReply({ type: "refused", code: "insufficient_funds", message: "the balance is 3, the withdrawal 5", retryable: false }));

  const err: unknown = await failure(new Client(bus.nc, "shop").call("account", "withdraw"));

  expect(err).toBeInstanceOf(CallError);
  expect(err).toBeInstanceOf(Error); // it is still a plain error to code that does not care
  const refusal = err as CallError;
  expect(refusal.type).toBe("refused");
  expect(refusal.code).toBe("insufficient_funds");
  expect(refusal.reason).toBe("the balance is 3, the withdrawal 5");
  expect(refusal.retryable).toBe(false);
  expect(refusal.refused).toBe(true);
  expect(refusal.message).toBe("refused: the balance is 3, the withdrawal 5");
  expect(asRefusal(err)).toBe(refusal);
  expect(refusalCode(err)).toBe("insufficient_funds");
});

test("a failure and a dropped call are CallErrors but not refusals", async () => {
  const failedBus = fakeBus(errorReply({ type: "handler_error", message: "Error: disk on fire", retryable: false }));
  const failed = await failure(new Client(failedBus.nc, "shop").call("account", "broken"));
  expect(failed).toBeInstanceOf(CallError);
  expect(failed.type).toBe("handler_error");
  expect(failed.code).toBe("");
  expect(failed.refused).toBe(false);
  expect(asRefusal(failed)).toBeNull();
  expect(refusalCode(failed)).toBe("");
  expect(failed.message).toBe("handler_error: Error: disk on fire");

  const droppedBus = fakeBus(errorReply({ type: "deadline_exceeded", message: "call expired 9 ms before it could be handled", retryable: true }));
  const dropped = await failure(new Client(droppedBus.nc, "shop").call("account", "write"));
  expect(dropped.type).toBe("deadline_exceeded");
  expect(dropped.retryable).toBe(true);
  expect(dropped.refused).toBe(false);
});

test("an old thrall's replies read as the generic error type, and a malformed one still rejects", async () => {
  // What every SDK sent for a failed handler before refusals existed: no "code".
  const oldError = JSON.parse('{"type":"handler_error","message":"the balance is 3, the withdrawal 5","retryable":false}') as WireError;
  const old = await failure(new Client(fakeBus(errorReply(oldError)).nc, "legacy").call("account", "rule"));
  expect(old).toBeInstanceOf(CallError);
  expect(old.type).toBe("handler_error");
  expect(old.code).toBe("");
  expect(refusalCode(old)).toBe("");
  expect(old.message).toBe("handler_error: the balance is 3, the withdrawal 5");

  const noDetail = fakeBus((req) => ({ v: 1, id: req.id, kind: "reply", status: "error" }));
  const malformed = await new Client(noDetail.nc, "legacy").call("account", "x").catch((e: unknown) => e);
  expect(malformed).toBeInstanceOf(CallError);
});

test("errors that are not error replies are neither CallErrors nor refusals", async () => {
  const nc = {
    request: async () => {
      throw new Error("TIMEOUT");
    },
  } as unknown as NatsConnection;
  const err = await new Client(nc, "shop").call("mute", "x", {}, { timeoutMs: 10 }).catch((e: unknown) => e);
  expect(err).not.toBeInstanceOf(CallError);
  expect(asRefusal(err)).toBeNull();
  expect(refusalCode(err)).toBe("");
  expect(refusalCode(undefined)).toBe("");
});

test("the module-level call goes through the same client: deadline, app option, typed error", async () => {
  const bus = fakeBus(errorReply({ type: "refused", code: "day_closed", message: "the day is closed", retryable: false }));
  useConnection(bus.nc);
  const previous = process.env.AETHER_APP;
  process.env.AETHER_APP = "ujito";
  try {
    const err = await call("denik", "zapis", { km: 3 }, { timeoutMs: 3000, app: "ae" }).catch((e: unknown) => e);
    expect(bus.last().subject).toBe("aether.ae.denik.call");
    expect(bus.last().env.deadline).toBe(bus.last().env.ts! + 3000);
    expect(refusalCode(err)).toBe("day_closed");

    await call("denik", "stav").catch(() => {});
    expect(bus.last().subject).toBe("aether.ujito.denik.call");
  } finally {
    if (previous === undefined) delete process.env.AETHER_APP;
    else process.env.AETHER_APP = previous;
  }
});
