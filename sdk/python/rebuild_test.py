"""Event-sourced rebuild tests for the Python SDK. These need a real JetStream server; they
spawn `nats-server` if present and skip otherwise (so CI without the binary stays green).

Run: uv run --with nats-py -m unittest rebuild_test   (from sdk/python)
"""

import asyncio
import random
import shutil
import socket
import subprocess
import unittest

import nats
from nats.js.api import RetentionPolicy, StorageType

import aether

NATS_SERVER = shutil.which("nats-server")


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH")
class RebuildTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = random.randint(14500, 15500)
        self.dir = f"/tmp/aether-py-js-{self.port}"
        self.proc = subprocess.Popen(
            [NATS_SERVER, "-js", "-a", "127.0.0.1", "-p", str(self.port), "-sd", self.dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for the port to accept connections before connecting (avoids noisy retries).
        for _ in range(100):
            with socket.socket() as s:
                s.settimeout(0.2)
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    break
            await asyncio.sleep(0.05)
        self.nc = await nats.connect(f"nats://127.0.0.1:{self.port}")

    async def asyncTearDown(self):
        await self.nc.close()
        self.proc.terminate()

    async def _provision(self, name):
        js = self.nc.jetstream()
        await js.add_stream(
            name=aether._stream_evt("es", name),
            subjects=[aether._sub_evt("es", name)],
            retention=RetentionPolicy.LIMITS,
            storage=StorageType.MEMORY,
        )

    def _ctx(self, name):
        return aether.Ctx(nats=self.nc, name=name, app="es")

    async def test_empty_log_returns_initial(self):
        await self._provision("empty")
        got = await aether.rebuild(self._ctx("empty"), 7, lambda ev, s: 0)
        self.assertEqual(got, 7)

    async def test_append_then_rebuild(self):
        await self._provision("acct")
        ctx = self._ctx("acct")
        for delta in (10, 5, 3):
            await ctx.append({"delta": delta})
        got = await aether.rebuild(self._ctx("acct"), 0, lambda ev, bal: bal + ev["delta"])
        self.assertEqual(got, 18)

    async def test_rebuild_preserves_order(self):
        await self._provision("seq")
        ctx = self._ctx("seq")
        for i in range(5):
            await ctx.append({"n": i})
        got = await aether.rebuild(self._ctx("seq"), [], lambda ev, acc: acc + [ev["n"]])
        self.assertEqual(got, [0, 1, 2, 3, 4])


if __name__ == "__main__":
    unittest.main()
