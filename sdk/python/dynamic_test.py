"""Dynamic supervisor client tests for the Python SDK.

They exercise ctx.start_child / ctx.stop_child against a fake lord (a mock nats
connection), so no running NATS is needed. The real lord<->thrall path is covered by
the Go integration tests from AE-013; the contract is identical.

Run: uv run --with nats-py -m unittest dynamic_test   (from sdk/python)
"""

import unittest

import aether


class _Msg:
    def __init__(self, data: bytes):
        self.data = data


class _FakeLord:
    """A mock nats connection: request() decodes the ctl envelope, hands it to reply_fn,
    and returns the reply envelope. `seen` captures the last (subject, request)."""

    def __init__(self, reply_fn):
        self.reply_fn = reply_fn
        self.seen = None

    async def request(self, subject, payload, timeout=None):
        req = aether._decode(payload)
        self.seen = (subject, req)
        return _Msg(aether._encode(self.reply_fn(req)))


def _ok(req, payload=None):
    r = {"v": 1, "id": req.get("id"), "kind": "reply", "status": "ok"}
    if payload is not None:
        r["payload"] = payload
    return r


class DynamicSupervisor(unittest.IsolatedAsyncioTestCase):
    async def test_start_child_sends_spawn_and_returns_name(self):
        lord = _FakeLord(lambda req: _ok(req, {"name": req["payload"]["name"]}))
        ctx = aether.Ctx(nats=lord, name="mgr", app="demo")

        name = await ctx.start_child(
            aether.SpawnSpec(name="worker-1", cmd="./w", restart="transient", durable=True)
        )

        self.assertEqual(name, "worker-1")
        subject, req = lord.seen
        self.assertEqual(subject, "aether._lord.ctl")
        self.assertEqual(req["kind"], "ctl")
        self.assertEqual(req["op"], "spawn")
        self.assertEqual(req["payload"]["cmd"], "./w")
        self.assertEqual(req["payload"]["restart"], "transient")
        self.assertTrue(req["payload"]["durable"])

    async def test_start_child_minimal_omits_optional_fields(self):
        lord = _FakeLord(lambda req: _ok(req, {"name": req["payload"]["name"]}))
        ctx = aether.Ctx(nats=lord, name="mgr", app="demo")

        await ctx.start_child(aether.SpawnSpec(name="w", cmd="./w"))

        _, req = lord.seen
        self.assertEqual(req["payload"], {"name": "w", "cmd": "./w"})

    async def test_stop_child_sends_stop_with_name(self):
        lord = _FakeLord(lambda req: _ok(req))
        ctx = aether.Ctx(nats=lord, name="mgr", app="demo")

        await ctx.stop_child("worker-1")

        _, req = lord.seen
        self.assertEqual(req["op"], "stop")
        self.assertEqual(req["payload"]["name"], "worker-1")

    async def test_error_reply_raises(self):
        lord = _FakeLord(lambda req: {
            "v": 1, "id": req.get("id"), "kind": "reply", "status": "error",
            "error": {"type": "spawn_failed", "message": 'a child named "dup" already exists',
                      "retryable": False},
        })
        ctx = aether.Ctx(nats=lord, name="mgr", app="demo")

        with self.assertRaises(RuntimeError):
            await ctx.start_child(aether.SpawnSpec(name="dup", cmd="./w"))

    async def test_timeout_propagates(self):
        class _Timeout:
            async def request(self, *a, **k):
                raise TimeoutError("no lord")

        ctx = aether.Ctx(nats=_Timeout(), name="mgr", app="demo")
        with self.assertRaises(Exception):
            await ctx.start_child(aether.SpawnSpec(name="x", cmd="./x"), timeout=0.1)


if __name__ == "__main__":
    unittest.main()
