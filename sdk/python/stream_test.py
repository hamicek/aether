"""SSEStream tests for the Python SDK - cover the pure authorization guards, which need no NATS,
mirroring the Go and TS tests. Live delivery / scope isolation is proven by a real run in the Python
live-dashboard example.

Run: uv run --with nats-py --with aiohttp -m unittest stream_test   (from sdk/python)
"""

import unittest

from aether import Ctx, SSEStream


def _stub_stream() -> SSEStream:
    # nats is never touched: the guards under test fire before any subscribe.
    return SSEStream(Ctx(nats=None, name="edge", app="test"))


class SSEStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_empty_scope_is_403(self):
        resp = await _stub_stream().serve_client(None)
        self.assertEqual(resp.status, 403)

    async def test_wildcard_scope_is_400(self):
        for subj in ("test.>", "test.*.evt"):
            resp = await _stub_stream().serve_client(None, subj)
            self.assertEqual(resp.status, 400)


if __name__ == "__main__":
    unittest.main()
