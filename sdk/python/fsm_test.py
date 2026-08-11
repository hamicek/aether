"""FSM behaviour tests for the Python SDK - exercise the serialized machine core directly
(no running NATS), mirroring the Go and TS tests.

Run: uv run --with nats-py -m unittest fsm_test   (from sdk/python)
"""

import asyncio
import contextlib
import os
import unittest

from aether import (
    Ctx,
    Event,
    FSM,
    Logger,
    Outcome,
    Reaction,
    State,
    StateTimeout,
    ThrallDef,
    _Machine,
    start,
    start_fsm,
)


@contextlib.contextmanager
def _fake_env():
    """Set the env a thrall needs so the startup guards run, then restore. The URL is never
    dialled: every guard exercised here fires before nats.connect."""
    saved = {k: os.environ.get(k) for k in ("AETHER_NATS_URL", "AETHER_APP")}
    os.environ["AETHER_NATS_URL"] = "nats://127.0.0.1:1"
    os.environ["AETHER_APP"] = "test"
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _boom(ev, d, ctx):
    raise RuntimeError("boom")


def turnstile() -> FSM:
    return FSM(
        name="turnstile",
        initial="locked",
        init=lambda ctx: 0,
        states={
            "locked": State(on={"coin": Reaction(fn=lambda ev, d, ctx: Outcome(next="unlocked", data=d))}),
            "unlocked": State(on={
                "push": Reaction(fn=lambda ev, d, ctx: Outcome(next="locked", data=d + 1, reply=d + 1)),
                "coin": Reaction(guard=lambda d, ev: False, fn=lambda ev, d, ctx: Outcome(data=d)),
                "boom": Reaction(fn=_boom),
            }),
        },
    )


def make(defn: FSM) -> _Machine:
    # Quiet logger so transition/event logs do not clutter test output.
    log = Logger({"component": "test"}, level=40, fmt="json", write=lambda line: None)
    ctx = Ctx(nats=None, name="t", app="test", log=log)
    return _Machine(defn, ctx, defn.init(ctx), ctx.log)


async def call_op(m: _Machine, op: str) -> dict:
    box: dict = {}
    await m.send(Event(op=op, payload={}, kind="call"), {"op": op}, lambda reply: box.__setitem__("r", reply))
    return box.get("r")


async def cast_op(m: _Machine, op: str) -> None:
    await m.send(Event(op=op, payload={}, kind="cast"), {"op": op}, None)


async def wait_state(m: _Machine, want: str, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if m.state() == want:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timeout waiting for state {want}, still in {m.state()}")


class FSMTest(unittest.IsolatedAsyncioTestCase):
    async def test_starts_in_initial_state(self):
        m = make(turnstile())
        m.arm_initial()
        self.assertEqual(m.state(), "locked")

    async def test_transition_and_reply_on_call(self):
        m = make(turnstile())
        m.arm_initial()
        coin = await call_op(m, "coin")
        self.assertEqual(coin["status"], "ok")
        self.assertEqual(m.state(), "unlocked")

        push = await call_op(m, "push")
        self.assertEqual(push["status"], "ok")
        self.assertEqual(push["payload"], 1)
        self.assertEqual(m.state(), "locked")
        self.assertEqual(m.data, 1)

    async def test_unhandled_call_errors(self):
        m = make(turnstile())
        m.arm_initial()
        reply = await call_op(m, "push")  # no push in locked
        self.assertEqual(reply["status"], "error")
        self.assertEqual(reply["error"]["type"], "no_transition")
        self.assertEqual(m.state(), "locked")

    async def test_guard_rejects_transition(self):
        m = make(turnstile())
        m.arm_initial()
        await call_op(m, "coin")  # -> unlocked
        reply = await call_op(m, "coin")  # guard false
        self.assertEqual(reply["status"], "error")
        self.assertEqual(reply["error"]["type"], "guard_rejected")
        self.assertEqual(m.state(), "unlocked")

    async def test_handler_error_replies(self):
        m = make(turnstile())
        m.arm_initial()
        await call_op(m, "coin")
        reply = await call_op(m, "boom")
        self.assertEqual(reply["status"], "error")
        self.assertEqual(reply["error"]["type"], "handler_error")

    async def test_reserved_state_op(self):
        m = make(turnstile())
        m.arm_initial()
        await call_op(m, "coin")
        reply = await call_op(m, "_state")
        self.assertEqual(reply["status"], "ok")
        self.assertEqual(reply["payload"]["state"], "unlocked")

    async def test_cast_transitions_without_reply(self):
        m = make(turnstile())
        m.arm_initial()
        await cast_op(m, "coin")
        self.assertEqual(m.state(), "unlocked")


def timeout_machine(after: float) -> FSM:
    return FSM(
        name="tmo",
        initial="waiting",
        init=lambda ctx: 0,
        states={
            "waiting": State(
                on={
                    "ping": Reaction(fn=lambda ev, d, ctx: Outcome(next="active", data=d)),
                    "tick": Reaction(fn=lambda ev, d, ctx: Outcome(next="expired", data=d + 1)),
                },
                timeout=StateTimeout(after=after, op="tick"),
            ),
            "active": State(on={}),
            "expired": State(on={}),
        },
    )


class FSMTimeoutTest(unittest.IsolatedAsyncioTestCase):
    async def test_state_timeout_fires_transition(self):
        m = make(timeout_machine(0.03))
        m.arm_initial()
        await wait_state(m, "expired", 1.0)
        self.assertEqual(m.data, 1)

    async def test_event_cancels_state_timeout(self):
        m = make(timeout_machine(0.03))
        m.arm_initial()
        await cast_op(m, "ping")  # -> active before 30ms
        self.assertEqual(m.state(), "active")
        await asyncio.sleep(0.08)
        self.assertEqual(m.state(), "active")  # timeout did not fire after leaving

    async def test_outcome_rearms_timeout_while_staying(self):
        defn = FSM(
            name="rearm",
            initial="idle",
            init=lambda ctx: 0,
            states={
                "idle": State(on={
                    "poke": Reaction(fn=lambda ev, d, ctx: Outcome(data=d + 1, timeout=StateTimeout(after=0.03, op="done"))),
                    "done": Reaction(fn=lambda ev, d, ctx: Outcome(next="finished", data=d)),
                }),
                "finished": State(on={}),
            },
        )
        m = make(defn)
        m.arm_initial()
        await cast_op(m, "poke")
        self.assertEqual(m.state(), "idle")
        await wait_state(m, "finished", 1.0)
        self.assertEqual(m.data, 1)


class FSMHardeningTest(unittest.IsolatedAsyncioTestCase):
    async def test_enter_unknown_state_warns(self):
        lines: list = []
        log = Logger({"component": "test"}, level=30, fmt="json", write=lambda line: lines.append(line))
        ctx = Ctx(nats=None, name="t", app="test", log=log)
        defn = FSM(
            name="ghosted",
            initial="here",
            init=lambda ctx: 0,
            # "ghost" is not a defined state
            states={"here": State(on={"leave": Reaction(fn=lambda ev, d, ctx: Outcome(next="ghost", data=d))})},
        )
        m = _Machine(defn, ctx, defn.init(ctx), ctx.log)
        m.arm_initial()
        await cast_op(m, "leave")
        self.assertTrue(any("fsm transition to unknown state" in line for line in lines), lines)

    async def test_start_fsm_rejects_unknown_initial(self):
        defn = FSM(name="typo", initial="startt", init=lambda ctx: 0, states={"start": State(on={})})
        with _fake_env(), self.assertRaises(RuntimeError) as cm:
            await start_fsm(defn)
        self.assertIn("not in states", str(cm.exception))

    async def test_start_fsm_rejects_missing_init(self):
        defn = FSM(name="no-init", initial="a", init=None, states={"a": State(on={})})
        with _fake_env(), self.assertRaises(RuntimeError) as cm:
            await start_fsm(defn)
        self.assertIn("init is required", str(cm.exception))

    async def test_start_rejects_missing_init(self):
        defn = ThrallDef(name="no-init", init=None)
        with _fake_env(), self.assertRaises(RuntimeError) as cm:
            await start(defn)
        self.assertIn("init is required", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
