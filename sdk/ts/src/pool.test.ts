import { test, expect } from "bun:test";
import { CallPool, DeadlineGate, MailboxStats } from "./thrall";
import type { Envelope } from "./envelope";
import type { Logger } from "./log";

// Unit tests of the CallPool - what a concurrent web thrall has in place of the serialized mailbox.
// No bus: calls are submitted by hand, and each handler is a promise the test settles when it wants.

const quiet: Logger = { debug() {}, info() {}, warn() {}, error() {}, with: () => quiet };

// held is a handler the test controls: it reports when it started and finishes on release().
function held() {
  let release!: () => void;
  let started = false;
  const done = new Promise<void>((resolve) => (release = resolve));
  return {
    handle: async () => {
      started = true;
      await done;
    },
    release,
    started: () => started,
  };
}

const call = (id: string, deadline?: number): Envelope => ({ v: 1, id, kind: "call", op: "http_request", deadline });

function newPool(limit: number) {
  process.env.AETHER_DEADLINE_SKEW_MS = "0"; // the test and the pool share one clock
  const stats = new MailboxStats();
  const pool = new CallPool(limit, new DeadlineGate(stats, quiet), stats);
  delete process.env.AETHER_DEADLINE_SKEW_MS;
  return { pool, stats };
}

test("a slow call does not hold up a fast one, up to the limit", async () => {
  const { pool, stats } = newPool(2);
  const slow = held();
  const fast = held();
  const replies: Envelope[] = [];
  const slowDone = pool.submit(call("slow"), (r) => replies.push(r), slow.handle);
  const fastDone = pool.submit(call("fast"), (r) => replies.push(r), fast.handle);
  await Bun.sleep(0);

  expect(slow.started()).toBe(true);
  expect(fast.started()).toBe(true);
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 2, in_flight: 2 });

  fast.release();
  await fastDone;
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 1, in_flight: 1, processed_total: 1 });
  slow.release();
  await slowDone;
  expect(stats.snapshot().in_flight).toBeUndefined(); // omitted while zero
  expect(replies).toEqual([]); // the pool itself answers only a call it drops
});

test("calls beyond the limit wait their turn, in arrival order", async () => {
  const { pool, stats } = newPool(1);
  const first = held();
  const second = held();
  const third = held();
  const done = [first, second, third].map((h, i) => pool.submit(call(`c-${i}`), () => {}, h.handle));
  await Bun.sleep(0);

  expect([first.started(), second.started(), third.started()]).toEqual([true, false, false]);
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 3, in_flight: 1 }); // two are waiting

  first.release();
  await done[0];
  await Bun.sleep(0);
  expect([second.started(), third.started()]).toEqual([true, false]);
  second.release();
  third.release();
  await Promise.all(done);
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 0, processed_total: 3 });
});

test("a waiting call is dropped when its deadline passes, not when a slot comes free", async () => {
  const { pool, stats } = newPool(1);
  const busy = held();
  const late = held();
  const replies: Envelope[] = [];
  const busyDone = pool.submit(call("busy"), () => {}, busy.handle);
  const sent = Date.now();
  await pool.submit(call("late", sent + 80), (r) => replies.push(r), late.handle);
  const waited = Date.now() - sent;

  expect(busy.started()).toBe(true); // the slot was still taken when the call was dropped
  expect(late.started()).toBe(false);
  expect(waited).toBeGreaterThanOrEqual(70);
  expect(waited).toBeLessThan(1000);
  expect(replies).toHaveLength(1);
  expect(replies[0]).toMatchObject({ id: "late", status: "error", error: { type: "deadline_exceeded", retryable: true } });
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 1, in_flight: 1, expired_total: 1, processed_total: 0 });

  // The dropped call left the queue: the next one gets the slot as soon as it is free.
  const next = held();
  const nextDone = pool.submit(call("next"), () => {}, next.handle);
  busy.release();
  await busyDone;
  await Bun.sleep(0);
  expect(next.started()).toBe(true);
  next.release();
  await nextDone;
});

test("a call that already expired while waiting for its slot is not run", async () => {
  const { pool } = newPool(1);
  const replies: Envelope[] = [];
  const ran = held();
  await pool.submit(call("stale", Date.now() - 1000), (r) => replies.push(r), ran.handle);
  expect(ran.started()).toBe(false);
  expect(replies[0]?.error?.type).toBe("deadline_exceeded");
});

test("drain waits for running and waiting calls and takes no new ones", async () => {
  const { pool } = newPool(1);
  const running = held();
  const waiting = held();
  void pool.submit(call("running"), () => {}, running.handle);
  void pool.submit(call("waiting"), () => {}, waiting.handle);
  await Bun.sleep(0);

  let drained = false;
  const drain = pool.drain().then(() => (drained = true));
  const refused = held();
  await pool.submit(call("after-drain"), () => {}, refused.handle);
  expect(refused.started()).toBe(false);

  await Bun.sleep(20);
  expect(drained).toBe(false);
  running.release();
  await Bun.sleep(0);
  expect(waiting.started()).toBe(true); // a call that was already waiting still gets its turn
  expect(drained).toBe(false);
  waiting.release();
  await drain;
  expect(drained).toBe(true);
});

test("a handler that throws frees its slot", async () => {
  const { pool, stats } = newPool(1);
  await pool.submit(call("boom"), () => {}, async () => {
    throw new Error("boom");
  }).catch(() => {});
  const next = held();
  const nextDone = pool.submit(call("next"), () => {}, next.handle);
  await Bun.sleep(0);
  expect(next.started()).toBe(true);
  next.release();
  await nextDone;
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 0, processed_total: 2 });
});

test("a far-future deadline waits for a slot instead of reading as expired", async () => {
  const { pool, stats } = newPool(1);
  const busy = held();
  const busyDone = pool.submit(call("busy"), () => {}, busy.handle);
  // A month away, past what a timer can hold (2^31 ms), and the largest safe integer.
  const far = [Date.now() + 30 * 24 * 3600 * 1000, Date.now() + 2 ** 40, Number.MAX_SAFE_INTEGER];
  const waiting = far.map(() => held());
  const replies: Envelope[] = [];
  const done = far.map((deadline, i) => pool.submit(call(`far-${i}`, deadline), (r) => replies.push(r), waiting[i]!.handle));
  await Bun.sleep(50);

  expect(replies).toEqual([]); // none was dropped
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 4, in_flight: 1 });
  expect(stats.snapshot().expired_total).toBeUndefined();

  busy.release();
  await busyDone;
  for (const w of waiting) {
    await Bun.sleep(0);
    expect(w.started()).toBe(true);
    w.release();
    await Bun.sleep(0);
  }
  await Promise.all(done);
  expect(stats.snapshot()).toMatchObject({ mailbox_depth: 0, processed_total: 4 });
});
