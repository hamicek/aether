"""Durable cast consumer tests for the Python SDK. These need a real JetStream server; they
spawn `nats-server` if present and skip otherwise (so CI without the binary stays green).

Run: uv run --with nats-py -m unittest durable_test   (from sdk/python)
"""

import asyncio
import random
import shutil
import socket
import subprocess
import unittest

import nats

import aether

NATS_SERVER = shutil.which("nats-server")

APP = "dur"


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH")
class DurableCastTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = random.randint(15600, 16600)
        self.dir = f"/tmp/aether-py-dur-{self.port}"
        self.proc = subprocess.Popen(
            [NATS_SERVER, "-js", "-a", "127.0.0.1", "-p", str(self.port), "-sd", self.dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
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

    async def _provision_cast_stream(self, name):
        """Provision the durable cast stream, as the lord would."""
        js = self.nc.jetstream()
        await js.add_stream(
            name=aether._stream(APP, name),
            subjects=[aether._sub_cast(APP, name)],
        )

    async def test_batched_drain_preserves_fifo(self):
        name = "q"
        await self._provision_cast_stream(name)

        # Preload more casts than a single batch holds, so FIFO is exercised across batches.
        total = 300
        js = self.nc.jetstream()
        for i in range(total):
            await js.publish(
                aether._sub_cast(APP, name),
                aether._encode({"v": 1, "kind": "cast", "op": "inc", "payload": {"n": i}}),
            )

        got = []
        done = asyncio.Event()

        async def on_cast(e, ack_durable=None):
            got.append(e["payload"]["n"])
            if len(got) == total:
                done.set()

        stop = asyncio.Event()
        task = asyncio.create_task(aether._cast_pull_loop(self.nc, APP, name, stop, on_cast))
        try:
            await asyncio.wait_for(done.wait(), timeout=15)
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=5)  # clean exit on the next loop iteration

        self.assertEqual(len(got), total, "no-loss: every cast must be drained")
        self.assertEqual(got, list(range(total)), "FIFO: casts must be processed in arrival order")

    async def test_stop_on_empty_stream(self):
        name = "idle"
        await self._provision_cast_stream(name)

        processed = []

        async def on_cast(e, ack_durable=None):
            processed.append(e)

        stop = asyncio.Event()
        task = asyncio.create_task(aether._cast_pull_loop(self.nc, APP, name, stop, on_cast))
        # Let the loop spin through at least one empty fetch, then ask it to stop.
        await asyncio.sleep(1.5)
        stop.set()
        await asyncio.wait_for(task, timeout=5)

        self.assertEqual(processed, [], "no cast should be processed on an empty stream")


if __name__ == "__main__":
    unittest.main()
