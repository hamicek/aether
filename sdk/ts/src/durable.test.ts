import { test, expect, beforeAll, afterAll } from "bun:test";
import { connect, type NatsConnection } from "nats";
import { subjects } from "./subjects";
import { encode, type Envelope } from "./envelope";
import { consumeDurableCast } from "./thrall";

type Proc = ReturnType<typeof Bun.spawn>;

// These tests need a real JetStream server; they spawn `nats-server` if present and skip
// otherwise (so CI without the binary stays green).
const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;

let proc: Proc | undefined;
let nc: NatsConnection | undefined;
let url = "";
const app = "dur";

async function startServer(): Promise<{ proc: Proc; url: string }> {
  const port = 16000 + Math.floor(Math.random() * 1000);
  const dir = `/tmp/aether-js-dur-${port}`;
  const p = Bun.spawn([serverBin!, "-js", "-a", "127.0.0.1", "-p", String(port), "-sd", dir], {
    stdout: "ignore",
    stderr: "ignore",
  });
  const u = `nats://127.0.0.1:${port}`;
  for (let i = 0; i < 50; i++) {
    try {
      const c = await connect({ servers: u });
      await c.close();
      return { proc: p, url: u };
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("nats-server did not start");
}

// provisions the durable cast stream for `name` (as the lord would).
async function provisionCast(name: string): Promise<void> {
  const jsm = await nc!.jetstreamManager();
  await jsm.streams.add({
    name: subjects.stream(app, name),
    subjects: [subjects.cast(app, name)],
  });
}

beforeAll(async () => {
  if (!hasServer) return;
  const started = await startServer();
  proc = started.proc;
  url = started.url;
  nc = await connect({ servers: url });
});

afterAll(async () => {
  if (nc) await nc.close();
  if (proc) proc.kill();
});

test("durable cast drains in batches and preserves FIFO", async () => {
  if (!hasServer) return;
  const name = "q";
  await provisionCast(name);

  // consume() pulls in batches internally, so more casts than one network batch exercises FIFO
  // across batch boundaries just as the Go/Python fetch(N) loops do.
  const total = 300;
  const js = nc!.jetstream();
  for (let i = 0; i < total; i++) {
    await js.publish(subjects.cast(app, name), encode({ v: 1, kind: "cast", op: "inc", payload: { n: i } }));
  }

  const got: number[] = [];
  let resolveDone: () => void = () => {};
  const done = new Promise<void>((r) => (resolveDone = r));
  const onCast = async (e: Envelope): Promise<void> => {
    got.push((e.payload as { n: number }).n);
    if (got.length === total) resolveDone();
  };

  // The consume loop has no stop hook - closing its connection ends it. Give it a dedicated
  // connection so the test can stop it deterministically without touching the shared one.
  const consumerNc = await connect({ servers: url });
  const loop = consumeDurableCast(consumerNc, app, name, onCast);
  loop.catch(() => {}); // ends when consumerNc closes below

  await done;
  await consumerNc.close();

  expect(got.length).toBe(total); // no-loss
  expect(got).toEqual(Array.from({ length: total }, (_, i) => i)); // FIFO
});

test("durable cast processes nothing on an empty stream", async () => {
  if (!hasServer) return;
  const name = "idle";
  await provisionCast(name);

  const processed: Envelope[] = [];
  const onCast = async (e: Envelope): Promise<void> => {
    processed.push(e);
  };

  // TS has no stop hook - the consume loop runs until its connection closes. Give it a
  // dedicated connection, let it settle on the empty stream, then close to tear it down.
  const consumerNc = await connect({ servers: url });
  const loop = consumeDurableCast(consumerNc, app, name, onCast);
  loop.catch(() => {});

  await Bun.sleep(500);
  expect(processed).toEqual([]); // an empty stream yields no casts

  await consumerNc.close();
});
