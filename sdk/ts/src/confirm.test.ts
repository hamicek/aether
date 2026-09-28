import { test, expect, beforeAll, afterAll } from "bun:test";
import { connect, DiscardPolicy, RetentionPolicy, StorageType, type NatsConnection, type StreamConfig } from "nats";
import { subjects } from "./subjects";
import { decode } from "./envelope";
import { sendConfirmedCast, NotDurableError } from "./client";

type Proc = ReturnType<typeof Bun.spawn>;

// These tests need a real JetStream server; they spawn `nats-server` if present and skip
// otherwise (so a machine without the binary stays green). CI installs it.
const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;

const app = "cf";
const timeoutMs = 2000;

let proc: Proc | undefined;
let nc: NatsConnection | undefined;

async function startServer(): Promise<{ proc: Proc; url: string }> {
  const port = 17000 + Math.floor(Math.random() * 1000);
  const p = Bun.spawn([serverBin!, "-js", "-a", "127.0.0.1", "-p", String(port), "-sd", `/tmp/aether-js-cf-${port}`], {
    stdout: "ignore",
    stderr: "ignore",
  });
  const url = `nats://127.0.0.1:${port}`;
  for (let i = 0; i < 50; i++) {
    try {
      const c = await connect({ servers: url });
      await c.close();
      return { proc: p, url };
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("nats-server did not start");
}

// provisionMailbox creates the durable mailbox the lord would (a WorkQueue stream over the
// target's cast subject); extra adjusts it for tests that need a stream to refuse writes.
async function provisionMailbox(conn: NatsConnection, name: string, extra: Partial<StreamConfig> = {}): Promise<void> {
  const jsm = await conn.jetstreamManager();
  await jsm.streams.add({
    name: subjects.stream(app, name),
    subjects: [subjects.cast(app, name)],
    retention: RetentionPolicy.Workqueue,
    storage: StorageType.Memory,
    ...extra,
  });
}

async function storedCount(name: string): Promise<number> {
  const jsm = await nc!.jetstreamManager();
  return (await jsm.streams.info(subjects.stream(app, name))).state.messages;
}

beforeAll(async () => {
  if (!hasServer) return;
  const started = await startServer();
  proc = started.proc;
  nc = await connect({ servers: started.url });
});

afterAll(async () => {
  if (nc) await nc.close();
  if (proc) proc.kill();
});

test("confirmed cast resolves once the message is in the mailbox", async () => {
  if (!hasServer) return;
  await provisionMailbox(nc!, "stored");

  await sendConfirmedCast(nc!, app, "stored", "sample", { temp: 71.4 }, { trace: "t-1", timeoutMs });

  expect(await storedCount("stored")).toBe(1);
  const jsm = await nc!.jetstreamManager();
  const e = decode((await jsm.streams.getMessage(subjects.stream(app, "stored"), { seq: 1 })).data);
  expect(e).toMatchObject({ kind: "cast", to: "stored", op: "sample", trace: "t-1", payload: { temp: 71.4 } });
});

test("confirmed cast to a non-durable target rejects and sends nothing", async () => {
  if (!hasServer) return;
  let received = 0;
  const sub = nc!.subscribe(subjects.cast(app, "plain"), { callback: () => received++ });

  const err = await sendConfirmedCast(nc!, app, "plain", "sample", {}, { timeoutMs }).catch((e: unknown) => e);

  expect(err).toBeInstanceOf(NotDurableError);
  await nc!.flush();
  await Bun.sleep(200);
  expect(received).toBe(0);
  sub.unsubscribe();
});

test("a retry with the same idempotency key lands once", async () => {
  if (!hasServer) return;
  await provisionMailbox(nc!, "dedup");

  for (const key of ["sample-1", "sample-1", "sample-2"]) {
    await sendConfirmedCast(nc!, app, "dedup", "sample", {}, { idempotencyKey: key, timeoutMs });
  }

  expect(await storedCount("dedup")).toBe(2);
});

test("a write the mailbox refuses rejects instead of resolving", async () => {
  if (!hasServer) return;
  await provisionMailbox(nc!, "full", { max_msgs: 1, discard: DiscardPolicy.New });

  await sendConfirmedCast(nc!, app, "full", "sample", {}, { timeoutMs });
  const err = await sendConfirmedCast(nc!, app, "full", "sample", {}, { timeoutMs }).catch((e: unknown) => e);

  expect(err).toBeInstanceOf(Error);
  expect(await storedCount("full")).toBe(1);
});

test("with the bus down a confirmed cast rejects, even to a cached mailbox", async () => {
  if (!hasServer) return;
  // Its own server, so stopping it does not disturb the other tests.
  const own = await startServer();
  const conn = await connect({ servers: own.url, reconnect: false });
  try {
    await provisionMailbox(conn, "outage");
    await sendConfirmedCast(conn, app, "outage", "sample", {}, { timeoutMs });

    own.proc.kill();
    await own.proc.exited;

    const err = await sendConfirmedCast(conn, app, "outage", "sample", {}, { timeoutMs }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
  } finally {
    await conn.close().catch(() => {});
    own.proc.kill();
  }
});
