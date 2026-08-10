import { test, expect, beforeAll, afterAll } from "bun:test";
import { connect, type NatsConnection, RetentionPolicy, StorageType } from "nats";
import { subjects } from "./subjects";
import { rebuild, appendEvent } from "./rebuild";
import type { Ctx } from "./thrall";

type Proc = ReturnType<typeof Bun.spawn>;

// These tests need a real JetStream server; they spawn `nats-server` if present and skip
// otherwise (so CI without the binary stays green).
const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;

let proc: Proc | undefined;
let nc: NatsConnection | undefined;
const app = "es";

async function startServer(): Promise<{ proc: Proc; url: string }> {
  const port = 14000 + Math.floor(Math.random() * 1000);
  const dir = `/tmp/aether-js-test-${port}`;
  const p = Bun.spawn([serverBin!, "-js", "-a", "127.0.0.1", "-p", String(port), "-sd", dir], {
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

// provisions the retention event log stream for `name` (as the lord would).
async function provision(name: string): Promise<void> {
  const jsm = await nc!.jetstreamManager();
  await jsm.streams.add({
    name: subjects.eventLogStream(app, name),
    subjects: [subjects.eventLog(app, name)],
    retention: RetentionPolicy.Limits,
    storage: StorageType.Memory,
  });
}

function ctxFor(name: string): Ctx {
  return { nats: nc, app, name } as unknown as Ctx;
}

beforeAll(async () => {
  if (!hasServer) return;
  const started = await startServer();
  proc = started.proc;
  nc = await connect({ servers: started.url });
});

afterAll(async () => {
  await nc?.close();
  proc?.kill();
});

test.skipIf(!hasServer)("rebuild of an empty log returns initial", async () => {
  await provision("empty");
  const got = await rebuild(ctxFor("empty"), 7, () => 0);
  expect(got).toBe(7);
});

test.skipIf(!hasServer)("append then rebuild reconstructs state", async () => {
  await provision("acct");
  for (const delta of [10, 5, 3]) {
    await appendEvent(nc!, app, "acct", { delta });
  }
  const got = await rebuild(ctxFor("acct"), 0, (event, balance: number) => balance + (event as { delta: number }).delta);
  expect(got).toBe(18);
});

test.skipIf(!hasServer)("rebuild preserves append order", async () => {
  await provision("seq");
  for (let i = 0; i < 5; i++) {
    await appendEvent(nc!, app, "seq", { n: i });
  }
  const got = await rebuild(ctxFor("seq"), [] as number[], (event, acc: number[]) => [...acc, (event as { n: number }).n]);
  expect(got).toEqual([0, 1, 2, 3, 4]);
});
