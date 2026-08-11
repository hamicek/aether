import { test, expect, beforeAll, afterAll } from "bun:test";
import { connect, type NatsConnection } from "nats";
import { startFencing, fenceConfigFromEnv, type FenceConfig } from "./fencing";
import { type Logger } from "./log";

type Proc = ReturnType<typeof Bun.spawn>;

// These tests need a real JetStream server; they spawn `nats-server` if present and skip
// otherwise (so CI without the binary stays green).
const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;

let proc: Proc | undefined;
let nc: NatsConnection | undefined;
let url = "";
const bucket = "aether_singletons";

// fast lease/interval so the tests do not wait the real 3s TTL.
const opts = { leaseMs: 500, intervalMs: 100 };

const silentLog: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  with() {
    return silentLog;
  },
};

const encoder = new TextEncoder();

async function startServer(): Promise<{ proc: Proc; url: string }> {
  const port = 15000 + Math.floor(Math.random() * 1000);
  const dir = `/tmp/aether-fence-test-${port}`;
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

// putRecord writes a lock record (as internal/singleton does) with the given epoch.
async function putRecord(conn: NatsConnection, key: string, epoch: number): Promise<void> {
  const kv = await conn.jetstream().views.kv(bucket, { history: 1 });
  await kv.put(key, encoder.encode(JSON.stringify({ holder: "lord-a", ts: Date.now(), epoch })));
}

function waitLost(): { promise: Promise<string>; onLost: (r: string) => void } {
  let resolve!: (r: string) => void;
  const promise = new Promise<string>((r) => (resolve = r));
  return { promise, onLost: resolve };
}

beforeAll(async () => {
  if (!hasServer) return;
  const started = await startServer();
  proc = started.proc;
  url = started.url;
  nc = await connect({ servers: url });
});

afterAll(async () => {
  await nc?.close();
  proc?.kill();
});

test("fenceConfigFromEnv reads the injected token", () => {
  process.env.AETHER_SINGLETON_BUCKET = "aether_singletons";
  process.env.AETHER_SINGLETON_KEY = "svc";
  process.env.AETHER_SINGLETON_EPOCH = "7";
  expect(fenceConfigFromEnv()).toEqual({ bucket: "aether_singletons", key: "svc", epoch: 7 });

  delete process.env.AETHER_SINGLETON_EPOCH;
  expect(fenceConfigFromEnv()).toBeNull();
  delete process.env.AETHER_SINGLETON_BUCKET;
  delete process.env.AETHER_SINGLETON_KEY;
});

test.skipIf(!hasServer)("fencing stays while the epoch holds", async () => {
  const key = "hold";
  await putRecord(nc!, key, 1);
  const cfg: FenceConfig = { bucket, key, epoch: 1 };
  let fired = "";
  const { stop } = await startFencing(nc!, cfg, silentLog, (r) => (fired = r), opts);
  await Bun.sleep(opts.leaseMs + 3 * opts.intervalMs);
  stop();
  expect(fired).toBe("");
});

test.skipIf(!hasServer)("fencing fires on an epoch takeover", async () => {
  const key = "takeover";
  await putRecord(nc!, key, 1);
  const cfg: FenceConfig = { bucket, key, epoch: 1 };
  const { promise, onLost } = waitLost();
  await startFencing(nc!, cfg, silentLog, onLost, opts);
  await putRecord(nc!, key, 2); // a successor stamps a new epoch
  const reason = await Promise.race([promise, Bun.sleep(2000).then(() => "TIMEOUT")]);
  expect(reason).not.toBe("TIMEOUT");
});

test.skipIf(!hasServer)("fencing fires when the key is gone", async () => {
  const key = "purge";
  await putRecord(nc!, key, 1);
  const cfg: FenceConfig = { bucket, key, epoch: 1 };
  const { promise, onLost } = waitLost();
  await startFencing(nc!, cfg, silentLog, onLost, opts);
  const kv = await nc!.jetstream().views.kv(bucket, { history: 1 });
  await kv.purge(key);
  const reason = await Promise.race([promise, Bun.sleep(2000).then(() => "TIMEOUT")]);
  expect(reason).not.toBe("TIMEOUT");
});

test.skipIf(!hasServer)("fencing fires only after the lease when the bus is unreachable", async () => {
  const key = "unreachable";
  // A dedicated connection we can close without disturbing the shared one.
  const own = await connect({ servers: url });
  await putRecord(own, key, 1);
  const cfg: FenceConfig = { bucket, key, epoch: 1 };
  let firedAt = 0;
  const start = Date.now();
  await startFencing(own, cfg, silentLog, () => (firedAt = Date.now() - start), opts);

  await own.close(); // KV can no longer be verified

  // Must not fire before the lease elapses.
  await Bun.sleep(opts.leaseMs - opts.intervalMs);
  expect(firedAt).toBe(0);

  // Must fire once the lease has elapsed.
  await Bun.sleep(opts.leaseMs + 3 * opts.intervalMs);
  expect(firedAt).toBeGreaterThanOrEqual(opts.leaseMs);
});
