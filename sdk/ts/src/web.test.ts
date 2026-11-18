import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "path";
import { connect, type NatsConnection } from "nats";
import { Client } from "./client";
import { decode, encode, type Envelope, type HeartbeatMetrics, type ThrallDescribe } from "./envelope";
import { OP_HTTP_REQUEST, bodyText, type HTTPRequest, type HTTPResponse } from "./http";
import { subjects } from "./subjects";

// End-to-end tests of a web thrall's two dispatch modes in the TS SDK: a real web thrall process
// (testdata/web/peer.ts, built on startWeb) on a real nats-server, called by the real Client the
// way the ingress calls it. They spawn `nats-server` if present and skip otherwise; CI installs it.

type Proc = ReturnType<typeof Bun.spawn>;

const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;
const peerScript = join(import.meta.dir, "../../../testdata/web/peer.ts");
const app = "ae";

let server: Proc | undefined;
let nc: NatsConnection | undefined;
const peers: Proc[] = [];
let url = "";

async function startServer(): Promise<void> {
  const port = 19000 + Math.floor(Math.random() * 1000);
  server = Bun.spawn([serverBin!, "-a", "127.0.0.1", "-p", String(port)], { stdout: "ignore", stderr: "ignore" });
  url = `nats://127.0.0.1:${port}`;
  for (let i = 0; i < 50; i++) {
    try {
      nc = await connect({ servers: url });
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error("nats-server did not start");
}

// startWebPeer runs the web peer the way the lord would and resolves once it answers. shape and
// concurrency pick what is under test; the deadline is judged exactly (one host, one clock).
async function startWebPeer(name: string, shape: "serve" | "handle", concurrency?: number): Promise<Proc> {
  const env: Record<string, string> = {
    ...(process.env as Record<string, string>),
    AETHER_NATS_URL: url,
    AETHER_APP: app,
    AETHER_NAME: name,
    AETHER_LOG_LEVEL: "error",
    AETHER_HEARTBEAT_INTERVAL_MS: "100",
    AETHER_DEADLINE_SKEW_MS: "0",
    WEB_SHAPE: shape,
  };
  if (concurrency !== undefined) env.WEB_CONCURRENCY = String(concurrency);
  const proc = Bun.spawn(["bun", peerScript], { env, stdout: "ignore", stderr: "inherit" });
  peers.push(proc);
  const probe = new Client(nc!, app);
  for (let i = 0; i < 100; i++) {
    try {
      await probe.call(name, OP_HTTP_REQUEST, { method: "GET", path: "/fast" }, { timeoutMs: 200 });
      return proc;
    } catch {
      await Bun.sleep(50);
    }
  }
  throw new Error(`web peer ${name} did not become ready`);
}

// get sends one request the way the ingress does and reports the response with how long it took.
async function get(name: string, path: string, query = "", opts: { trace?: string } = {}): Promise<{ resp: HTTPResponse; ms: number }> {
  const started = performance.now();
  const req: HTTPRequest = { method: "GET", path, query };
  const resp = await new Client(nc!, app).call<HTTPResponse>(name, OP_HTTP_REQUEST, req, { timeoutMs: 5000, ...opts });
  return { resp, ms: performance.now() - started };
}

type Beat = HeartbeatMetrics & { describe?: ThrallDescribe };

// heartbeat waits for a heartbeat of the thrall that satisfies want and returns its payload.
async function heartbeat(name: string, want: (m: Beat) => boolean): Promise<Beat> {
  const sub = nc!.subscribe(subjects.hb(name), { timeout: 5000 });
  try {
    for await (const msg of sub) {
      const metrics = decode(msg.data).payload as Beat;
      if (want(metrics)) return metrics;
    }
  } finally {
    sub.unsubscribe();
  }
  throw new Error(`no matching heartbeat from ${name}`);
}

beforeAll(async () => {
  if (!hasServer) return;
  await startServer();
}, 30000);

afterAll(async () => {
  for (const p of peers) p.kill();
  if (nc) await nc.close();
  if (server) server.kill();
});

test("serve: a slow request does not hold up a fast one", async () => {
  if (!hasServer) return;
  await startWebPeer("web-serve", "serve", 4);
  const slow = get("web-serve", "/slow", "ms=800");
  await Bun.sleep(100); // the slow request is in its handler
  const fast = await get("web-serve", "/fast");
  expect(bodyText(fast.resp)).toBe("fast");
  expect(fast.ms).toBeLessThan(400); // answered while the slow one still had ~700 ms to go
  const finished = await slow;
  expect(bodyText(finished.resp)).toBe("slow");
  expect(finished.ms).toBeGreaterThanOrEqual(700);
}, 20000);

test("serve: each request gets its own ctx", async () => {
  if (!hasServer) return;
  await startWebPeer("web-ctx", "serve", 4);
  // Both requests are inside their handlers together (each sleeps 50 ms before reading ctx).
  const [a, b] = await Promise.all([
    get("web-ctx", "/trace", "", { trace: "trace-a" }),
    get("web-ctx", "/trace", "", { trace: "trace-b" }),
  ]);
  expect(bodyText(a.resp)).toBe("trace-a");
  expect(bodyText(b.resp)).toBe("trace-b");
}, 20000);

test("serve: the heartbeat tells running requests from waiting ones and declares the concurrency", async () => {
  if (!hasServer) return;
  await startWebPeer("web-beat", "serve", 2);
  const idle = await heartbeat("web-beat", (m) => m.mailbox_depth === 0);
  expect(idle.in_flight).toBeUndefined();
  expect(idle.describe?.concurrency).toBe(2);
  expect(idle.describe?.call_ops).toEqual([OP_HTTP_REQUEST]);

  const requests = [1, 2, 3].map(() => get("web-beat", "/slow", "ms=700"));
  const busy = await heartbeat("web-beat", (m) => m.mailbox_depth === 3);
  expect(busy.in_flight).toBe(2); // two in their handlers, the third waiting for a slot
  await Promise.all(requests);
  await heartbeat("web-beat", (m) => m.mailbox_depth === 0 && m.in_flight === undefined);
}, 20000);

test("serve: a request waiting for a slot is dropped when its caller's deadline passes", async () => {
  if (!hasServer) return;
  await startWebPeer("web-queue", "serve", 2);
  const counted = Number(bodyText((await get("web-queue", "/hits")).resp));
  const busy = Promise.all([get("web-queue", "/slow", "ms=900"), get("web-queue", "/slow", "ms=900")]);
  await Bun.sleep(100); // both slots are taken

  // Sent by hand, so that this test - unlike a real caller - still listens after the deadline.
  const sent = Date.now();
  const envelope: Envelope = {
    v: 1, id: "queued-1", trace: "t-queued", kind: "call", to: "web-queue", op: OP_HTTP_REQUEST,
    payload: { method: "GET", path: "/hits" }, ts: sent, deadline: sent + 200,
  };
  const reply = decode((await nc!.request(subjects.call(app, "web-queue"), encode(envelope), { timeout: 5000 })).data);
  const waited = Date.now() - sent;

  expect(reply.status).toBe("error");
  expect(reply.error?.type).toBe("deadline_exceeded");
  expect(reply.error?.retryable).toBe(true);
  expect(waited).toBeGreaterThanOrEqual(150);
  expect(waited).toBeLessThan(700); // at its deadline, not when a slot came free at ~900 ms

  await busy;
  const beat = await heartbeat("web-queue", (m) => m.mailbox_depth === 0);
  expect(beat.expired_total).toBe(1);
  // The dropped /hits request never ran: since the count was read, the thrall has counted the
  // two slow requests and now this one.
  expect(Number(bodyText((await get("web-queue", "/hits")).resp))).toBe(counted + 3);
}, 20000);

for (const [label, shape, concurrency] of [
  ["serve at concurrency 1", "serve", 1],
  ["handle", "handle", undefined],
] as const) {
  test(`${label}: requests are served one at a time, even with an async handler`, async () => {
    if (!hasServer) return;
    const name = `web-serial-${shape}`;
    await startWebPeer(name, shape, concurrency);
    const slow = get(name, "/slow", "ms=600");
    await Bun.sleep(100);
    const fast = await get(name, "/fast");
    expect(fast.ms).toBeGreaterThanOrEqual(400); // it waited for the slow request to return
    await slow;
    const beat = await heartbeat(name, (m) => m.mailbox_depth === 0);
    expect(beat.describe?.concurrency).toBeUndefined(); // a serialized thrall declares none
  }, 20000);
}

test("handle threads its state from one request to the next", async () => {
  if (!hasServer) return;
  await startWebPeer("web-state", "handle");
  const first = Number(bodyText((await get("web-state", "/hits")).resp));
  const second = Number(bodyText((await get("web-state", "/hits")).resp));
  expect(second).toBe(first + 1);
}, 20000);

test("drain lets a request that is in its handler finish", async () => {
  if (!hasServer) return;
  const proc = await startWebPeer("web-drain", "serve", 4);
  const inFlight = get("web-drain", "/slow", "ms=600");
  await Bun.sleep(100);
  nc!.publish(subjects.ctl("web-drain"), encode({ v: 1, kind: "ctl", op: "drain" }));
  const finished = await inFlight;
  expect(bodyText(finished.resp)).toBe("slow");
  expect(await proc.exited).toBe(0);
}, 20000);
