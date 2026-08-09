import { test, expect } from "bun:test";
import type { NatsConnection } from "nats";
import { startChild, stopChild } from "./client";
import { decode, encode, type Envelope } from "./envelope";

// fakeLord returns a NatsConnection whose request() decodes the ctl envelope, hands it
// to `reply`, and returns the reply envelope - so the client side is tested without a
// real NATS server. `seen` captures the subject and request for assertions.
function fakeLord(
  reply: (req: Envelope) => Envelope,
  seen?: (subject: string, req: Envelope) => void,
): NatsConnection {
  return {
    request: async (subject: string, data: Uint8Array) => {
      const req = decode(data);
      seen?.(subject, req);
      return { data: encode(reply(req)) };
    },
  } as unknown as NatsConnection;
}

test("startChild sends a spawn ctl on aether._lord.ctl and returns the name", async () => {
  let subject = "";
  let req: Envelope = { v: 1, kind: "ctl" };
  const nc = fakeLord(
    (r) => ({ v: 1, id: r.id, kind: "reply", status: "ok", payload: { name: (r.payload as { name: string }).name } }),
    (s, r) => {
      subject = s;
      req = r;
    },
  );

  const name = await startChild(nc, { name: "worker-1", cmd: "./w", restart: "transient", durable: true });

  expect(name).toBe("worker-1");
  expect(subject).toBe("aether._lord.ctl");
  expect(req.kind).toBe("ctl");
  expect(req.op).toBe("spawn");
  const spec = req.payload as { cmd: string; restart: string; durable: boolean };
  expect(spec.cmd).toBe("./w");
  expect(spec.restart).toBe("transient");
  expect(spec.durable).toBe(true);
});

test("stopChild sends a stop ctl with the name", async () => {
  let op = "";
  let name = "";
  const nc = fakeLord((r) => {
    op = r.op ?? "";
    name = (r.payload as { name: string }).name;
    return { v: 1, id: r.id, kind: "reply", status: "ok" };
  });

  await stopChild(nc, "worker-1");

  expect(op).toBe("stop");
  expect(name).toBe("worker-1");
});

test("startChild throws on the lord's error reply", async () => {
  const nc = fakeLord((r) => ({
    v: 1,
    id: r.id,
    kind: "reply",
    status: "error",
    error: { type: "spawn_failed", message: "a child named \"dup\" already exists", retryable: false },
  }));

  await expect(startChild(nc, { name: "dup", cmd: "./w" })).rejects.toThrow(/already exists/);
});

test("startChild propagates a request timeout", async () => {
  const nc = {
    request: async () => {
      throw new Error("TIMEOUT");
    },
  } as unknown as NatsConnection;

  await expect(startChild(nc, { name: "x", cmd: "./x" }, { timeoutMs: 100 })).rejects.toThrow();
});
