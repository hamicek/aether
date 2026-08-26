import { test, expect } from "bun:test";
import type { NatsConnection } from "nats";
import { DedupCache, dedupKey, DEFAULT_IDEMPOTENCY_MAX } from "./dedup";
import { call, cast, useConnection } from "./client";
import { decode, encode, type Envelope } from "./envelope";

// The dedup key prefers a caller-supplied idem and falls back to the per-send envelope id.
test("dedupKey prefers idem over id", () => {
  expect(dedupKey({ v: 1, kind: "call", id: "id-1", idem: "key-1" })).toBe("key-1");
  expect(dedupKey({ v: 1, kind: "call", id: "id-1" })).toBe("id-1");
});

// A cast records presence (undefined reply); a call records its reply value.
test("DedupCache stores and returns cached replies", () => {
  const c = new DedupCache(8);
  c.put("cast-key", undefined);
  expect(c.get("cast-key")).toEqual([undefined, true]);
  c.put("call-key", { value: 42 });
  expect(c.get("call-key")).toEqual([{ value: 42 }, true]);
  expect(c.get("never")).toEqual([undefined, false]);
});

// Bounded: with max=2 the two generations hold at most ~2*max keys, so the oldest keys evict.
test("DedupCache evicts the oldest keys", () => {
  const c = new DedupCache(2);
  for (const k of ["k0", "k1", "k2"]) c.put(k, undefined);
  expect(c.get("k0")[1]).toBe(true); // still in the previous generation
  for (const k of ["k3", "k4"]) c.put(k, undefined);
  expect(c.get("k0")[1]).toBe(false); // evicted after two rotations
  expect(c.get("k4")[1]).toBe(true);
});

test("DedupCache falls back to the default bound for a non-positive max", () => {
  expect(new DedupCache(0)["max" as never]).toBe(DEFAULT_IDEMPOTENCY_MAX as never);
});

// The caller-side idempotencyKey option stamps idem onto the outgoing call/cast envelope.
test("call and cast stamp idem from opts.idempotencyKey", async () => {
  let sent: Envelope = { v: 1, kind: "call" };
  const nc = {
    request: async (_subject: string, data: Uint8Array) => {
      sent = decode(data);
      return { data: encode({ v: 1, id: sent.id, kind: "reply", status: "ok", payload: 1 }) };
    },
    publish: (_subject: string, data: Uint8Array) => {
      sent = decode(data);
    },
  } as unknown as NatsConnection;
  useConnection(nc);

  await call("account", "withdraw", { amt: 5 }, { idempotencyKey: "w-1" });
  expect(sent.idem).toBe("w-1");

  cast("account", "touch", {}, { idempotencyKey: "t-1" });
  expect(sent.idem).toBe("t-1");
});
