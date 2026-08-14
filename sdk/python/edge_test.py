"""Edge behaviour tests for the Python SDK - cover the validation guard that fires before any dial,
mirroring the Go and TS tests. The full lifecycle (init -> run -> drain -> stop) is proven by a real run
in the Python live-dashboard / webserver-custom examples (there is no embedded-NATS harness here).

Run: uv run --with nats-py -m unittest edge_test   (from sdk/python)
"""

import contextlib
import os
import unittest

from aether import EdgeDef, def_edge, start_edge


@contextlib.contextmanager
def _fake_env():
    """Set the env a thrall needs so the startup guards run, then restore. The URL is never dialled:
    the guard exercised here fires before nats.connect."""
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


class EdgeTest(unittest.IsolatedAsyncioTestCase):
    async def test_start_edge_rejects_missing_run(self):
        with _fake_env():
            with self.assertRaises(RuntimeError) as cm:
                await start_edge(EdgeDef(name="gw"))
            self.assertIn("run is required", str(cm.exception))

    def test_def_edge_identity(self):
        async def run(ctx, stop):
            await stop.wait()

        d = def_edge(name="gw", run=run)
        self.assertIs(d.run, run)
        self.assertEqual(d.name, "gw")


if __name__ == "__main__":
    unittest.main()
