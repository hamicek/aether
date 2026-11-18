import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "path";
import { connect, ErrorCode, type NatsConnection, type NatsError } from "nats";
import { Client, CallError, asRefusal, refusalCode } from "./client";
import { decode, encode, type Envelope } from "./envelope";
import { subjects } from "./subjects";

// End-to-end tests of typed refusals and deadline drops in the TS SDK: a real thrall process
// (the cross-language peer, built on start()) on a real nats-server, called by the real Client.
// They spawn `nats-server` if present and skip otherwise (so a machine without the binary stays
// green). CI installs it.

type Proc = ReturnType<typeof Bun.spawn>;

const serverBin = Bun.which("nats-server");
const hasServer = serverBin !== null;
const peerScript = join(import.meta.dir, "../../../testdata/crosslang/peer.ts");

let server: Proc | undefined;
let nc: NatsConnection | undefined;
const peers: Proc[] = [];
let url = "";

async function startServer(): Promise<void> {
  const port = 18000 + Math.floor(Math.random() * 1000);
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

// startPeer runs the peer thrall the way the lord would - identity through the environment - and
// resolves once it answers on its call subject. skewMs sets its clock-skew tolerance.
async function startPeer(app: string, name: string, skewMs?: number): Promise<void> {
  const env: Record<string, string> = { ...(process.env as Record<string, string>), AETHER_NATS_URL: url, AETHER_APP: app, AETHER_NAME: name, AETHER_LOG_LEVEL: "error" };
  if (skewMs !== undefined) env.AETHER_DEADLINE_SKEW_MS = String(skewMs);
  peers.push(Bun.spawn(["bun", peerScript], { env, stdout: "ignore", stderr: "inherit" }));
  const probe = new Client(nc!, app);
  for (let i = 0; i < 100; i++) {
    try {
      await probe.call(name, "balance", {}, { timeoutMs: 200 });
      return;
    } catch {
      await Bun.sleep(50);
    }
  }
  throw new Error(`peer ${app}/${name} did not become ready`);
}

async function failure(call: Promise<unknown>): Promise<unknown> {
  try {
    await call;
  } catch (err) {
    return err;
  }
  throw new Error("the call did not fail");
}

beforeAll(async () => {
  if (!hasServer) return;
  await startServer();
  await startPeer("ae", "account"); // default skew tolerance
  await startPeer("ae", "ledger", 0); // exact deadline: caller and thrall share this host's clock
}, 30000);

afterAll(async () => {
  for (const p of peers) p.kill();
  if (nc) await nc.close();
  if (server) server.kill();
});

test("a refusal crosses the bus as a typed error, from a thrall in another app, and changes nothing", async () => {
  if (!hasServer) return;
  // The caller lives in app "ujito"; the shared thrall in app "ae".
  const caller = new Client(nc!, "ujito");
  const unreachable = (await failure(caller.call("account", "balance", {}, { timeoutMs: 500 }))) as NatsError;
  expect(unreachable).not.toBeInstanceOf(CallError);
  expect(unreachable.code).toBe(ErrorCode.NoResponders);

  const account = caller.with({ app: "ae" });
  const err = await failure(account.call("account", "withdraw", { amount: 5 }));
  const refusal = asRefusal(err);
  expect(refusal).not.toBeNull();
  expect(refusal!.type).toBe("refused");
  expect(refusal!.code).toBe("insufficient_funds");
  expect(refusal!.reason).toBe("the balance is 3, the withdrawal 5");
  expect(refusal!.retryable).toBe(false);

  expect(await account.call<number>("account", "balance")).toBe(3); // the refusal changed nothing
  expect(await account.call<number>("account", "withdraw", { amount: 2 })).toBe(1); // an allowed request goes through

  // The control: an ordinary handler error is a failure, not a refusal.
  const failed = (await failure(account.call("account", "broken"))) as CallError;
  expect(failed).toBeInstanceOf(CallError);
  expect(failed.type).toBe("handler_error");
  expect(failed.refused).toBe(false);
  expect(refusalCode(failed)).toBe("");
});

// heartbeat waits for a heartbeat of the thrall that satisfies want and returns its payload.
async function heartbeat(name: string, want: (m: Record<string, unknown>) => boolean): Promise<Record<string, unknown>> {
  const sub = nc!.subscribe(subjects.hb(name), { timeout: 5000 });
  try {
    for await (const msg of sub) {
      const metrics = decode(msg.data).payload as Record<string, unknown>;
      if (want(metrics)) return metrics;
    }
  } finally {
    sub.unsubscribe();
  }
  throw new Error(`no matching heartbeat from ${name}`);
}

test("a refusal is not the thrall's last error, a handler failure is", async () => {
  if (!hasServer) return;
  const account = new Client(nc!, "ae");
  await failure(account.call("account", "withdraw", { amount: 99 }));
  const afterRefusal = await heartbeat("account", () => true);
  const lastError = ((afterRefusal.describe as Record<string, unknown>).last_error as string | undefined) ?? "";
  expect(lastError).not.toContain("insufficient_funds");
  expect(lastError).not.toContain("refused");

  await failure(account.call("account", "broken"));
  const afterFailure = await heartbeat("account", (m) => ((m.describe as Record<string, unknown>).last_error as string | undefined)?.includes("disk on fire") ?? false);
  expect((afterFailure.describe as Record<string, unknown>).last_error).toContain("handler_error");
}, 15000);

test("a call that timed out while queued is not executed later", async () => {
  if (!hasServer) return;
  const ledger = new Client(nc!, "ae");

  // Hold the mailbox; calls sent meanwhile queue behind the slow handler.
  const held = ledger.call("ledger", "hold", { ms: 400 }, { timeoutMs: 5000 });
  await Bun.sleep(50);

  // The caller gives up after 100 ms while its call is still queued...
  const timedOut = (await failure(ledger.call("ledger", "write", {}, { timeoutMs: 100 }))) as NatsError;
  expect(timedOut.code).toBe(ErrorCode.Timeout);

  // ...and when the thrall gets to it, it must not run: the first write that executes is a fresh one.
  await held;
  expect(await ledger.call<number>("ledger", "write")).toBe(1);

  const beat = await heartbeat("ledger", (m) => m.expired_total === 1);
  expect(beat.mailbox_depth).toBe(0);
}, 15000);

// rawCall sends a hand-built call envelope and returns the decoded reply, for deadlines the Client
// would never stamp (already past, or none at all - the shape an old caller sends).
async function rawCall(app: string, e: Envelope): Promise<Envelope> {
  const msg = await nc!.request(subjects.call(app, e.to!), encode(e), { timeout: 2000 });
  return decode(msg.data);
}

test("the skew tolerance decides a drop, and an envelope without a deadline always runs", async () => {
  if (!hasServer) return;
  const write = (deadline?: number): Promise<Envelope> =>
    rawCall("ae", { v: 1, id: `raw-${Math.random()}`, kind: "call", to: "account", op: "write", payload: {}, ts: Date.now() - 3600_000, deadline });
  const account = new Client(nc!, "ae");
  const before = await account.call<number>("account", "writes");

  expect((await write(undefined)).status).toBe("ok"); // an old caller: no deadline, however old
  expect((await write(Date.now() + 5000)).status).toBe("ok");
  expect((await write(Date.now() - 50)).status).toBe("ok"); // overdue by less than the 250 ms tolerance

  const dropped = await write(Date.now() - 2000);
  expect(dropped.status).toBe("error");
  expect(dropped.error?.type).toBe("deadline_exceeded");
  expect(dropped.error?.retryable).toBe(true);
  expect(dropped.error?.code).toBeUndefined();

  expect(await account.call<number>("account", "writes")).toBe(before + 3); // the expired call did not run
});

test("an old caller reads a refusal as the error string it always got", async () => {
  if (!hasServer) return;
  // The envelope and the error handling of an SDK built before refusals: no deadline sent, and
  // the reply's error reduced to `${type}: ${message}`.
  const reply = await rawCall("ae", { v: 1, id: "old-1", trace: "t-old", kind: "call", to: "account", op: "withdraw", payload: { amount: 50 }, ts: Date.now() });
  expect(reply.status).toBe("error");
  const text = `${reply.error?.type}: ${reply.error?.message}`;
  expect(text).toMatch(/^refused: the balance is \d+, the withdrawal 50$/);
});
