import { test, expect } from "bun:test";
import { createMachine, type FSMDef } from "./fsm";
import { newLogger } from "./log";
import { asRefuse, DeadlineGate, MailboxStats, refuse, RefuseError, refusedReply, type Ctx } from "./thrall";
import { DEFAULT_DEADLINE_SKEW_MS, deadlineAfter, deadlineSkewMs, overdueMs } from "./deadline";
import type { Envelope } from "./envelope";

// refuse() throws the typed RefuseError carrying the code and the reason - the "no" a handler
// gives when a rule of its domain forbids the request. The dispatch -> reply -> caller path over a
// real bus is covered in dispatch.test.ts.
test("refuse throws a RefuseError with its code and reason", () => {
  try {
    refuse("insufficient_funds", "the balance is 3, the withdrawal 5");
    throw new Error("refuse did not throw");
  } catch (err) {
    expect(err).toBeInstanceOf(RefuseError);
    expect((err as RefuseError).code).toBe("insufficient_funds");
    expect((err as RefuseError).reason).toBe("the balance is 3, the withdrawal 5");
    expect((err as RefuseError).message).toBe("refused (insufficient_funds): the balance is 3, the withdrawal 5");
  }
});

test("asRefuse distinguishes a refusal from any other error", () => {
  expect(asRefuse(new RefuseError("day_closed", "the day is closed"))?.code).toBe("day_closed");
  expect(asRefuse(new Error("just a handler error"))).toBeNull();
  expect(asRefuse("not even an error")).toBeNull();
  expect(asRefuse(null)).toBeNull();
});

test("the refusal reply carries the code in its own error type", () => {
  const reply = refusedReply({ v: 1, id: "c-1", kind: "call", op: "withdraw" }, new RefuseError("day_closed", "the day is closed"));
  expect(reply).toEqual({
    v: 1,
    id: "c-1",
    kind: "reply",
    status: "error",
    error: { type: "refused", code: "day_closed", message: "the day is closed", retryable: false },
  });
});

test("deadlineAfter is the send time plus the timeout, and none for a non-positive timeout", () => {
  expect(deadlineAfter(1700000000000, 5000)).toBe(1700000005000);
  expect(deadlineAfter(1700000000000, 0)).toBeUndefined();
  expect(deadlineAfter(1700000000000, -1)).toBeUndefined();
});

test("a call expires only once it is past its deadline by more than the skew tolerance", () => {
  const deadline = 1700000005000;
  const e: Envelope = { v: 1, kind: "call", deadline };
  expect(overdueMs(e, deadline - 1, 250)).toBeUndefined();
  expect(overdueMs(e, deadline, 250)).toBeUndefined();
  expect(overdueMs(e, deadline + 250, 250)).toBeUndefined();
  expect(overdueMs(e, deadline + 251, 250)).toBe(251);
  expect(overdueMs(e, deadline, 0)).toBeUndefined();
  expect(overdueMs(e, deadline + 1, 0)).toBe(1);
  // An envelope without a deadline (an old caller) never expires.
  expect(overdueMs({ v: 1, kind: "call" }, 1900000000000, 0)).toBeUndefined();
});

test("the skew tolerance comes from AETHER_DEADLINE_SKEW_MS when it is a non-negative integer", () => {
  const previous = process.env.AETHER_DEADLINE_SKEW_MS;
  try {
    const cases: [string | undefined, number][] = [
      [undefined, DEFAULT_DEADLINE_SKEW_MS],
      ["", DEFAULT_DEADLINE_SKEW_MS],
      ["0", 0],
      ["1500", 1500],
      ["-5", DEFAULT_DEADLINE_SKEW_MS],
      ["soon", DEFAULT_DEADLINE_SKEW_MS],
    ];
    for (const [value, want] of cases) {
      if (value === undefined) delete process.env.AETHER_DEADLINE_SKEW_MS;
      else process.env.AETHER_DEADLINE_SKEW_MS = value;
      expect(deadlineSkewMs()).toBe(want);
    }
    expect(DEFAULT_DEADLINE_SKEW_MS).toBe(250); // mirrors wire.DefaultDeadlineSkewMs
  } finally {
    if (previous === undefined) delete process.env.AETHER_DEADLINE_SKEW_MS;
    else process.env.AETHER_DEADLINE_SKEW_MS = previous;
  }
});

function capturingLogger() {
  const lines: string[] = [];
  return { lines, log: newLogger({ component: "test" }, { level: "debug", format: "json", write: (l) => lines.push(l) }) };
}

test("the deadline gate drops an expired call, says so in the log, and counts it apart from processed", () => {
  const { lines, log } = capturingLogger();
  const stats = new MailboxStats();
  const gate = new DeadlineGate(stats, log);
  const replies: Envelope[] = [];

  stats.begin();
  expect(gate.drop({ v: 1, id: "c-1", kind: "call", op: "write", deadline: Date.now() + 1000 }, (r) => replies.push(r))).toBe(false);
  expect(lines).toEqual([]);
  expect(replies).toEqual([]);

  expect(gate.drop({ v: 1, id: "c-2", trace: "t-77", kind: "call", op: "write", deadline: Date.now() - 1000 }, (r) => replies.push(r))).toBe(true);
  expect(lines).toHaveLength(1);
  const logged = JSON.parse(lines[0]!);
  expect(logged.msg).toContain("dropping expired call");
  expect(logged.op).toBe("write");
  expect(logged.trace).toBe("t-77");
  expect(logged.overdue_ms).toBeGreaterThanOrEqual(1000);
  expect(replies).toHaveLength(1);
  expect(replies[0]!.id).toBe("c-2");
  expect(replies[0]!.error?.type).toBe("deadline_exceeded");
  expect(replies[0]!.error?.retryable).toBe(true);
  expect(stats.snapshot()).toEqual({ mailbox_depth: 0, mailbox_latency_ms: 0, processed_total: 0, expired_total: 1 });
});

// A minimal ctx: these machines do not touch NATS.
function testCtx(log = capturingLogger().log): Ctx {
  return {
    nats: {} as unknown as Ctx["nats"],
    name: "door",
    app: "test",
    log,
    trace: "",
    msgId: "",
    singletonEpoch: 0,
    call: (() => Promise.resolve(undefined)) as Ctx["call"],
    cast: () => {},
    castConfirmed: async () => {},
    append: async () => {},
    startChild: async () => "",
    stopChild: async () => {},
  };
}

// door refuses a wrong code; the outcome it would have produced must be discarded.
const door: FSMDef<number> = {
  name: "door",
  initial: "locked",
  init: () => 0,
  states: {
    locked: {
      on: {
        open: {
          fn: (ev, attempts) => {
            if ((ev.payload as { code: string }).code !== "1234") refuse("wrong_code", "that is not the code");
            return { next: "open", data: attempts, reply: "opened" };
          },
        },
      },
    },
    open: { on: {} },
  },
};

async function callDoor(m: ReturnType<typeof createMachine<number>>, req: Envelope): Promise<Envelope | undefined> {
  let reply: Envelope | undefined;
  await m.send({ op: req.op ?? "", payload: req.payload, kind: "call" }, req, (r) => {
    reply = r;
  });
  return reply;
}

test("an FSM reaction can refuse: no transition, no failure", async () => {
  const { lines, log } = capturingLogger();
  const ctx = testCtx(log);
  const m = createMachine(door, ctx, 0, log);

  const refused = await callDoor(m, { v: 1, id: "c-1", kind: "call", op: "open", payload: { code: "0000" } });
  expect(refused?.error).toEqual({ type: "refused", code: "wrong_code", message: "that is not the code", retryable: false });
  expect(m.state()).toBe("locked");
  expect(lines.some((l) => JSON.parse(l).level === "ERROR")).toBe(false); // a refusal is not logged as an error

  const opened = await callDoor(m, { v: 1, id: "c-2", kind: "call", op: "open", payload: { code: "1234" } });
  expect(opened?.payload).toBe("opened");
  expect(m.state()).toBe("open");
  m.stop();
});

test("an expired call never becomes an FSM event", async () => {
  const { log } = capturingLogger();
  const m = createMachine(door, testCtx(log), 0, log);

  const late = await callDoor(m, { v: 1, id: "c-1", kind: "call", op: "open", payload: { code: "1234" }, deadline: Date.now() - 5000 });
  expect(late?.error?.type).toBe("deadline_exceeded");
  expect(m.state()).toBe("locked"); // the right code, sent too late, opened nothing
  expect(m.snapshot().expired_total).toBe(1);
  expect(m.snapshot().processed_total).toBe(0);
  expect(m.snapshot().mailbox_depth).toBe(0);
  m.stop();
});
