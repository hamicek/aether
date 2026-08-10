"""Trace propagation tests for the Python SDK (no running NATS - a fake connection captures
the outgoing envelopes).

Run: uv run --with nats-py -m unittest trace_test   (from sdk/python)
"""

import unittest

import aether


class _Msg:
    def __init__(self, data: bytes):
        self.data = data


class FakeNats:
    def __init__(self):
        self.published = []
        self.last_request = None

    async def publish(self, subject, data):
        self.published.append((subject, aether._decode(data)))

    async def request(self, subject, data, timeout=None):
        self.last_request = (subject, aether._decode(data))
        return _Msg(aether._encode({"v": 1, "kind": "reply", "status": "ok", "payload": {}}))


class TraceTest(unittest.IsolatedAsyncioTestCase):
    async def test_cast_propagates_trace(self):
        nc = FakeNats()
        ctx = aether.Ctx(nats=nc, name="a", app="app", trace="T-1")
        await ctx.cast("b", "op", {})
        subject, env = nc.published[0]
        self.assertEqual(subject, "aether.app.b.cast")
        self.assertEqual(env["trace"], "T-1")

    async def test_cast_mints_trace_when_absent(self):
        nc = FakeNats()
        ctx = aether.Ctx(nats=nc, name="a", app="app")  # trace defaults to ""
        await ctx.cast("b", "op", {})
        self.assertTrue(nc.published[0][1]["trace"])

    async def test_call_propagates_trace(self):
        nc = FakeNats()
        ctx = aether.Ctx(nats=nc, name="a", app="app", trace="T-2")
        await ctx.call("b", "op", {})
        _subject, env = nc.last_request
        self.assertEqual(env["trace"], "T-2")


class TraceHelpersTest(unittest.TestCase):
    def test_or_new_trace(self):
        self.assertEqual(aether._or_new_trace("keep"), "keep")
        self.assertTrue(aether._or_new_trace(""))  # mints a non-empty id


if __name__ == "__main__":
    unittest.main()
