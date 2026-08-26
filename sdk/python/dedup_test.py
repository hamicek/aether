"""Idempotence (call/cast dedup) tests for the Python SDK.

The dispatch -> skip/cached-reply behavior is proven end-to-end in the Go lord integration tests
(a real re-exec'd thrall); here we cover the SDK's own primitives: the dedup key, the bounded
generational cache, and that the caller-side idempotency_key stamps idem onto the envelope.

Run: uv run --with nats-py -m unittest dedup_test   (from sdk/python)
"""

import asyncio
import unittest

import aether


class DedupKey(unittest.TestCase):
    def test_prefers_idem_over_id(self):
        self.assertEqual(aether._dedup_key({"id": "id-1", "idem": "key-1"}), "key-1")
        self.assertEqual(aether._dedup_key({"id": "id-1"}), "id-1")


class DedupCache(unittest.TestCase):
    def test_stores_reply(self):
        c = aether._DedupCache(8)
        c.put("cast-key", None)
        self.assertEqual(c.get("cast-key"), (None, True))
        c.put("call-key", {"value": 42})
        self.assertEqual(c.get("call-key"), ({"value": 42}, True))
        self.assertEqual(c.get("never"), (None, False))

    def test_evicts_oldest(self):
        c = aether._DedupCache(2)
        for k in ("k0", "k1", "k2"):
            c.put(k, None)
        self.assertTrue(c.get("k0")[1])  # still in the previous generation
        for k in ("k3", "k4"):
            c.put(k, None)
        self.assertFalse(c.get("k0")[1])  # evicted after two rotations
        self.assertTrue(c.get("k4")[1])

    def test_default_max(self):
        self.assertEqual(aether._DedupCache(0).max, aether._DEFAULT_IDEMPOTENCY_MAX)


class _CapturingNats:
    """A minimal fake connection that captures the last sent envelope for call/cast."""

    def __init__(self):
        self.sent = None

    async def request(self, subject, data, timeout=5.0):
        self.sent = aether._decode(data)
        return _Msg(aether._encode({"v": 1, "id": self.sent.get("id"), "kind": "reply",
                                    "status": "ok", "payload": 1}))

    async def publish(self, subject, data):
        self.sent = aether._decode(data)


class _Msg:
    def __init__(self, data):
        self.data = data


class SenderStampsIdem(unittest.IsolatedAsyncioTestCase):
    async def test_call_and_cast_stamp_idem(self):
        nc = _CapturingNats()
        ctx = aether.Ctx(nats=nc, name="c", app="demo")

        await ctx.call("account", "withdraw", {"amt": 5}, idempotency_key="w-1")
        self.assertEqual(nc.sent.get("idem"), "w-1")

        await ctx.cast("account", "touch", {}, idempotency_key="t-1")
        self.assertEqual(nc.sent.get("idem"), "t-1")

        # No key -> no idem field on the wire (parity with the Go omitempty).
        await ctx.cast("account", "touch")
        self.assertNotIn("idem", nc.sent)


if __name__ == "__main__":
    unittest.main()
