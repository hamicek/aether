import { test, expect } from "bun:test";
import { escalate, EscalateError, asEscalate } from "./thrall";

// escalate() throws the typed EscalateError carrying its reason - the "let it crash" signal a
// handler raises instead of a plain error. The full dispatch -> reply -> exit -> restart
// behavior is proven end-to-end in the Go lord integration tests (a real re-exec'd thrall).
test("escalate throws an EscalateError with its reason", () => {
  try {
    escalate("mailbox poisoned");
    throw new Error("escalate did not throw");
  } catch (err) {
    expect(err).toBeInstanceOf(EscalateError);
    expect((err as EscalateError).reason).toBe("mailbox poisoned");
    expect((err as EscalateError).message).toBe("escalate: mailbox poisoned");
  }
});

// asEscalate recognizes an escalation and returns null for anything else, so dispatch tells a
// crash request apart from an ordinary handler error.
test("asEscalate distinguishes an escalation from a plain error", () => {
  expect(asEscalate(new EscalateError("boom"))?.reason).toBe("boom");
  expect(asEscalate(new Error("just a handler error"))).toBeNull();
  expect(asEscalate("not even an error")).toBeNull();
  expect(asEscalate(null)).toBeNull();
});
