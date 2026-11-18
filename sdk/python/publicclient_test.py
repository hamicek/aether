"""Public client tests for the Python SDK: Client, the typed CallError, refusals and the deadline
primitives. They use a fake connection (no broker); the real bus is covered by dispatch_test.

Run: uv run --with nats-py -m unittest publicclient_test   (from sdk/python)
"""

import contextlib
import json
import os
import time
import unittest

import aether


class _Msg:
    def __init__(self, data: bytes) -> None:
        self.data = data


class FakeBus:
    """A connection that records what a client sends and answers calls through `reply`."""

    def __init__(self, reply=None) -> None:
        self.sent = []  # (subject, envelope, timeout)
        self._reply = reply or (lambda req: {"v": 1, "id": req.get("id"), "kind": "reply", "status": "ok",
                                             "payload": None})

    async def request(self, subject, data, timeout=None):
        env = json.loads(data)
        self.sent.append((subject, env, timeout))
        return _Msg(json.dumps(self._reply(env)).encode())

    async def publish(self, subject, data, **_kwargs):
        self.sent.append((subject, json.loads(data), None))

    @property
    def last(self):
        return self.sent[-1]


def error_reply(error):
    return lambda req: {"v": 1, "id": req.get("id"), "kind": "reply", "status": "error", "error": error}


class ClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_needs_only_a_connection_and_stamps_when_it_stops_waiting(self):
        bus = FakeBus(lambda req: {"v": 1, "id": req["id"], "kind": "reply", "status": "ok", "payload": {"amount": 7}})
        before = int(time.time() * 1000)

        got = await aether.Client(bus, "shop").call("account", "balance", {"owner": "ada"}, timeout=2.0)

        self.assertEqual(got, {"amount": 7})
        subject, env, timeout = bus.last
        self.assertEqual(subject, "aether.shop.account.call")
        self.assertEqual(timeout, 2.0)
        self.assertEqual((env["kind"], env["to"], env["op"], env["payload"]), ("call", "account", "balance", {"owner": "ada"}))
        self.assertTrue(env["id"] and env["trace"])
        self.assertNotIn("idem", env)
        # The deadline is the send time plus the timeout: when this caller stops waiting.
        self.assertEqual(env["deadline"], env["ts"] + 2000)
        self.assertGreaterEqual(env["deadline"], before + 2000)

    async def test_default_timeout_is_the_deadline_of_a_plain_call(self):
        bus = FakeBus()
        await aether.Client(bus, "shop").call("account", "balance")
        self.assertEqual(bus.last[1]["deadline"], bus.last[1]["ts"] + 5000)

    async def test_the_app_is_explicit(self):
        bus = FakeBus()
        own = aether.Client(bus, "ujito")

        await own.call("store", "get")
        self.assertEqual(bus.last[0], "aether.ujito.store.call")
        await own.call("store", "get", app="ae")
        self.assertEqual(bus.last[0], "aether.ae.store.call")
        await own.with_(app="ae").call("store", "get")
        self.assertEqual(bus.last[0], "aether.ae.store.call")

        await own.cast("audit", "note", {"n": 1}, app="ae")
        subject, env, _ = bus.last
        self.assertEqual(subject, "aether.ae.audit.cast")
        self.assertEqual(env["kind"], "cast")
        self.assertNotIn("deadline", env)  # nobody waits for a cast

        self.assertEqual(own.app, "ujito")  # with_ leaves the client it derives from untouched

    async def test_trace_and_idempotency_key_reach_the_target(self):
        bus = FakeBus()
        client = aether.Client(bus, "shop")

        await client.call("account", "withdraw", trace="t-req-1", idempotency_key="withdraw-42")
        self.assertEqual((bus.last[1]["trace"], bus.last[1]["idem"]), ("t-req-1", "withdraw-42"))

        per_request = client.with_(trace="t-req-2")
        await per_request.call("account", "balance")
        self.assertEqual(bus.last[1]["trace"], "t-req-2")
        await per_request.call("account", "balance", trace="t-override")
        self.assertEqual(bus.last[1]["trace"], "t-override")
        await per_request.cast("audit", "note")
        self.assertEqual(bus.last[1]["trace"], "t-req-2")

    async def test_ctx_call_goes_through_the_client(self):
        # A handler's ctx.call propagates the handled message's trace, can address another app, and
        # raises the same typed error.
        bus = FakeBus(error_reply({"type": "refused", "code": "day_closed", "message": "the day is closed",
                                   "retryable": False}))
        ctx = aether.Ctx(nats=bus, name="web", app="ujito", trace="t-handled")

        with self.assertRaises(aether.CallError) as raised:
            await ctx.call("denik", "zapis", {"km": 3}, timeout=3.0, app="ae")

        subject, env, _ = bus.last
        self.assertEqual(subject, "aether.ae.denik.call")
        self.assertEqual(env["trace"], "t-handled")
        self.assertEqual(env["deadline"], env["ts"] + 3000)
        self.assertEqual(aether.refusal_code(raised.exception), "day_closed")

    async def test_refusal_raises_a_typed_error(self):
        bus = FakeBus(error_reply({"type": "refused", "code": "insufficient_funds",
                                   "message": "the balance is 3, the withdrawal 5", "retryable": False}))

        with self.assertRaises(aether.CallError) as raised:
            await aether.Client(bus, "shop").call("account", "withdraw")

        err = raised.exception
        self.assertIsInstance(err, RuntimeError)  # still the plain error existing code catches
        self.assertEqual((err.type, err.code, err.message, err.retryable),
                         ("refused", "insufficient_funds", "the balance is 3, the withdrawal 5", False))
        self.assertTrue(err.refused)
        self.assertEqual(str(err), "refused: the balance is 3, the withdrawal 5")
        self.assertIs(aether.as_refusal(err), err)
        self.assertEqual(aether.refusal_code(err), "insufficient_funds")

    async def test_a_failure_and_a_dropped_call_are_not_refusals(self):
        with self.assertRaises(aether.CallError) as raised:
            await aether.Client(FakeBus(error_reply({"type": "handler_error", "message": "disk on fire",
                                                     "retryable": False})), "shop").call("account", "broken")
        failed = raised.exception
        self.assertEqual((failed.type, failed.code, failed.refused), ("handler_error", "", False))
        self.assertIsNone(aether.as_refusal(failed))
        self.assertEqual(aether.refusal_code(failed), "")
        self.assertEqual(str(failed), "handler_error: disk on fire")

        with self.assertRaises(aether.CallError) as raised:
            await aether.Client(FakeBus(error_reply({
                "type": "deadline_exceeded", "message": "call expired 9 ms before it could be handled",
                "retryable": True})), "shop").call("account", "write")
        dropped = raised.exception
        self.assertEqual((dropped.type, dropped.retryable, dropped.refused), ("deadline_exceeded", True, False))

    async def test_old_thrall_replies_read_as_the_generic_type(self):
        # What every SDK sent for a failed handler before refusals existed: no "code".
        old = json.loads('{"type":"handler_error","message":"the balance is 3, the withdrawal 5","retryable":false}')
        with self.assertRaises(aether.CallError) as raised:
            await aether.Client(FakeBus(error_reply(old)), "legacy").call("account", "rule")
        self.assertEqual((raised.exception.type, raised.exception.code), ("handler_error", ""))
        self.assertEqual(aether.refusal_code(raised.exception), "")
        self.assertEqual(str(raised.exception), "handler_error: the balance is 3, the withdrawal 5")

        # A malformed error reply (no error object) still raises rather than crashing.
        malformed = FakeBus(lambda req: {"v": 1, "id": req["id"], "kind": "reply", "status": "error"})
        with self.assertRaises(aether.CallError):
            await aether.Client(malformed, "legacy").call("account", "x")

    async def test_an_empty_message_stays_empty(self):
        # A thrall may refuse with a code and no sentence; the caller must not invent one.
        bus = FakeBus(error_reply({"type": "refused", "code": "day_closed", "message": "", "retryable": False}))
        with self.assertRaises(aether.CallError) as raised:
            await aether.Client(bus, "shop").call("denik", "zapis")
        self.assertEqual((raised.exception.code, raised.exception.message), ("day_closed", ""))
        self.assertEqual(str(raised.exception), "refused: ")

    async def test_fractional_timeout_stamps_a_whole_millisecond_deadline(self):
        bus = FakeBus()
        await aether.Client(bus, "shop").call("account", "balance", timeout=1 / 3)
        self.assertIsInstance(bus.last[1]["deadline"], int)

    async def test_ctx_call_can_carry_another_trace(self):
        bus = FakeBus()
        ctx = aether.Ctx(nats=bus, name="web", app="ujito", trace="t-handled")
        await ctx.call("denik", "stav", trace="t-explicit")
        self.assertEqual(bus.last[1]["trace"], "t-explicit")
        await ctx.cast("audit", "note")
        self.assertEqual(bus.last[1]["trace"], "t-handled")

    def test_other_errors_are_not_refusals(self):
        self.assertIsNone(aether.as_refusal(TimeoutError("no reply")))
        self.assertEqual(aether.refusal_code(RuntimeError("handler_error: boom")), "")
        self.assertEqual(aether.refusal_code(None), "")


class RefuseTest(unittest.TestCase):
    def test_carries_code_and_message(self):
        refusal = aether.Refuse("insufficient_funds", "the balance is 3, the withdrawal 5")
        self.assertIsInstance(refusal, Exception)
        self.assertEqual((refusal.code, refusal.message), ("insufficient_funds", "the balance is 3, the withdrawal 5"))
        self.assertEqual(str(refusal), "refused (insufficient_funds): the balance is 3, the withdrawal 5")

    def test_reply_carries_the_code_in_its_own_error_type(self):
        reply = aether._refused_reply({"v": 1, "id": "c-1", "kind": "call"}, aether.Refuse("day_closed", "the day is closed"))
        self.assertEqual(reply, {"v": 1, "id": "c-1", "kind": "reply", "status": "error",
                                 "error": {"type": "refused", "code": "day_closed", "message": "the day is closed",
                                           "retryable": False}})

    def test_a_refusal_always_carries_a_code(self):
        # A refusal without a code would read, through refusal_code, as "not a refusal".
        unnamed = aether.Refuse("", "no")
        self.assertEqual(unnamed.code, aether.UNSPECIFIED_REFUSAL)
        self.assertEqual(aether._refused_reply({"v": 1, "id": "c-1"}, unnamed)["error"]["code"], "refused")

    def test_a_refusal_is_not_an_escalation(self):
        self.assertNotIsInstance(aether.Refuse("x", "y"), aether.Escalate)


@contextlib.contextmanager
def _env(key, value):
    saved = os.environ.get(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved


class DeadlineTest(unittest.TestCase):
    def test_deadline_after(self):
        self.assertEqual(aether._deadline_after(1700000000000, 5.0), 1700000005000)
        self.assertEqual(aether._deadline_after(1700000000000, 0.1), 1700000000100)
        self.assertIsNone(aether._deadline_after(1700000000000, 0))
        self.assertIsNone(aether._deadline_after(1700000000000, -1))

    def test_expires_only_past_the_skew_tolerance(self):
        deadline = 1700000005000
        e = {"v": 1, "kind": "call", "deadline": deadline}
        self.assertIsNone(aether._overdue_ms(e, deadline - 1, 250))
        self.assertIsNone(aether._overdue_ms(e, deadline, 250))
        self.assertIsNone(aether._overdue_ms(e, deadline + 250, 250))
        self.assertEqual(aether._overdue_ms(e, deadline + 251, 250), 251)
        self.assertIsNone(aether._overdue_ms(e, deadline, 0))
        self.assertEqual(aether._overdue_ms(e, deadline + 1, 0), 1)
        # An envelope without a deadline (an old caller) never expires.
        self.assertIsNone(aether._overdue_ms({"v": 1, "kind": "call"}, 1900000000000, 0))

    def test_skew_from_env(self):
        self.assertEqual(aether.DEFAULT_DEADLINE_SKEW_MS, 250)  # mirrors wire.DefaultDeadlineSkewMs
        for value, want in ((None, 250), ("", 250), ("0", 0), ("1500", 1500), ("-5", 250), ("soon", 250)):
            with self.subTest(value=value), _env("AETHER_DEADLINE_SKEW_MS", value):
                self.assertEqual(aether._deadline_skew_ms(), want)

    def test_gate_drops_logs_and_counts_apart_from_processed(self):
        lines = []
        log = aether.Logger(base={"component": "test"}, level=10, fmt="json", write=lines.append)
        stats = aether._MailboxStats()
        with _env("AETHER_DEADLINE_SKEW_MS", None):
            gate = aether._DeadlineGate(stats, log)
        now = int(time.time() * 1000)

        stats.begin()
        self.assertIsNone(gate.drop({"v": 1, "id": "c-1", "kind": "call", "op": "write", "deadline": now + 1000}))
        self.assertEqual(lines, [])

        reply = gate.drop({"v": 1, "id": "c-2", "trace": "t-77", "kind": "call", "op": "write", "deadline": now - 1000})
        self.assertEqual((reply["id"], reply["error"]["type"], reply["error"]["retryable"]), ("c-2", "deadline_exceeded", True))
        self.assertEqual(len(lines), 1)
        logged = json.loads(lines[0])
        self.assertIn("dropping expired call", logged["msg"])
        self.assertEqual((logged["op"], logged["trace"]), ("write", "t-77"))
        self.assertGreaterEqual(logged["overdue_ms"], 1000)
        self.assertEqual(stats.snapshot(), {"mailbox_depth": 0, "mailbox_latency_ms": 0.0, "processed_total": 0,
                                            "expired_total": 1})


def _open_door(ev, attempts, _ctx):
    if ev.payload.get("code") != "1234":
        raise aether.Refuse("wrong_code", "that is not the code")
    return aether.Outcome(next="open", data=attempts, reply="opened")


def _door():
    return aether.FSM(name="door", initial="locked", init=lambda _ctx: 0, states={
        "locked": aether.State(on={"open": aether.Reaction(fn=_open_door)}),
        "open": aether.State(on={}),
    })


class FSMRefusalAndDeadlineTest(unittest.IsolatedAsyncioTestCase):
    def _machine(self):
        self.lines = []
        log = aether.Logger(base={"component": "test"}, level=10, fmt="json", write=self.lines.append)
        return aether._Machine(_door(), aether.Ctx(nats=None, name="door", app="test", log=log), 0, log)

    async def _call(self, m, req):
        box = {}
        await m.send(aether.Event(op=req["op"], payload=req.get("payload"), kind="call"), req,
                     lambda reply: box.__setitem__("r", reply))
        return box.get("r")

    async def test_a_reaction_can_refuse(self):
        m = self._machine()
        refused = await self._call(m, {"v": 1, "id": "c-1", "kind": "call", "op": "open", "payload": {"code": "0000"}})
        self.assertEqual(refused["error"], {"type": "refused", "code": "wrong_code", "message": "that is not the code",
                                            "retryable": False})
        self.assertEqual(m.state(), "locked")
        self.assertFalse(any(json.loads(line)["level"] == "ERROR" for line in self.lines))  # not logged as an error

        opened = await self._call(m, {"v": 1, "id": "c-2", "kind": "call", "op": "open", "payload": {"code": "1234"}})
        self.assertEqual(opened["payload"], "opened")
        self.assertEqual(m.state(), "open")
        m.stop()

    async def test_an_expired_call_never_becomes_an_event(self):
        m = self._machine()
        late = await self._call(m, {"v": 1, "id": "c-1", "kind": "call", "op": "open", "payload": {"code": "1234"},
                                    "deadline": int(time.time() * 1000) - 5000})
        self.assertEqual(late["error"]["type"], "deadline_exceeded")
        self.assertEqual(m.state(), "locked")  # the right code, sent too late, opened nothing
        snap = m.snapshot()
        self.assertEqual((snap["expired_total"], snap["processed_total"], snap["mailbox_depth"]), (1, 0, 0))
        m.stop()


if __name__ == "__main__":
    unittest.main()
