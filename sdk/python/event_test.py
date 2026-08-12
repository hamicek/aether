"""Event-manager (gen_event) behaviour tests for the Python SDK - exercise the serialized
fan-out core directly (no running NATS), mirroring the Go and TS tests.

Run: uv run --with nats-py -m unittest event_test   (from sdk/python)
"""

import contextlib
import os
import unittest

from aether import (
    Ctx,
    Event,
    EventManager,
    Handler,
    Logger,
    _EventBus,
    start_event,
)


@contextlib.contextmanager
def _fake_env():
    """Set the env a thrall needs so the startup guards run, then restore. The URL is never
    dialled: the guards exercised here fire before nats.connect."""
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


def _bus(handlers: dict) -> _EventBus:
    # Quiet logger so event logs do not clutter test output.
    log = Logger({"component": "test"}, level=40, fmt="json", write=lambda line: None)
    ctx = Ctx(nats=None, name="bus", app="test", log=log)
    states = {name: (h.init(ctx) if h.init is not None else None) for name, h in handlers.items()}
    return _EventBus(EventManager(name="bus", handlers=handlers), ctx, states, log)


def _emit(bus: _EventBus, op: str):
    return bus.send(Event(op=op, payload={}, kind="cast"))


class EventManagerTest(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_order(self):
        seq = []

        def rec(name):
            def handle(ev, state, ctx):
                seq.append(name)
                return state
            return Handler(handle_event=handle)

        bus = _bus({"a": rec("a"), "b": rec("b"), "c": rec("c")})
        await _emit(bus, "ping")
        self.assertEqual(seq, ["a", "b", "c"])

    async def test_all_handlers_see_event(self):
        seq = []

        def rec(name):
            def handle(ev, state, ctx):
                seq.append(name)
                return state
            return Handler(handle_event=handle)

        handlers = {"a": rec("a"), "b": rec("b"), "c": rec("c")}
        bus = _bus(handlers)
        await _emit(bus, "ping")
        self.assertEqual(len(seq), len(handlers))

    async def test_state_isolation(self):
        def counter(step):
            return Handler(init=lambda ctx: 0, handle_event=lambda ev, s, ctx: s + step)

        bus = _bus({"a": counter(1), "b": counter(10)})
        await _emit(bus, "tick")
        await _emit(bus, "tick")
        self.assertEqual(bus.states["a"], 2)
        self.assertEqual(bus.states["b"], 20)

    async def test_handler_error_isolated(self):
        seen = {"healthy": 0}

        def bad(ev, s, ctx):
            raise RuntimeError("boom")

        def good(ev, s, ctx):
            seen["healthy"] += 1
            return s + 1

        bus = _bus({
            "bad": Handler(handle_event=bad),
            "good": Handler(init=lambda ctx: 0, handle_event=good),
        })
        await _emit(bus, "e")
        await _emit(bus, "e")
        self.assertEqual(seen["healthy"], 2)  # a failing sibling must not skip the healthy handler
        self.assertEqual(bus.states["good"], 2)

    async def test_start_event_rejects_no_handlers(self):
        with _fake_env():
            with self.assertRaises(RuntimeError) as cm:
                await start_event(EventManager(name="empty", handlers={}))
            self.assertIn("at least one handler", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
