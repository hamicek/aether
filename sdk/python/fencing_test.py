"""Thrall-side singleton fencing tests for the Python SDK. These need a real JetStream server;
they spawn `nats-server` if present and skip otherwise (so CI without the binary stays green).

Run: uv run --with nats-py -m unittest fencing_test   (from sdk/python)
"""

import asyncio
import json
import os
import random
import shutil
import socket
import subprocess
import time
import unittest

import nats

import aether

NATS_SERVER = shutil.which("nats-server")
BUCKET = "aether_singletons"

# fast lease/interval so the tests do not wait the real 3s TTL.
LEASE_MS = 500
INTERVAL_MS = 100


class EnvConfigTest(unittest.TestCase):
    def test_reads_injected_token(self):
        os.environ["AETHER_SINGLETON_BUCKET"] = BUCKET
        os.environ["AETHER_SINGLETON_KEY"] = "svc"
        os.environ["AETHER_SINGLETON_EPOCH"] = "7"
        self.assertEqual(
            aether._fence_config_from_env(),
            {"bucket": BUCKET, "key": "svc", "epoch": 7},
        )
        del os.environ["AETHER_SINGLETON_EPOCH"]
        self.assertIsNone(aether._fence_config_from_env())
        del os.environ["AETHER_SINGLETON_BUCKET"]
        del os.environ["AETHER_SINGLETON_KEY"]


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH")
class FencingTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = random.randint(15500, 16500)
        self.dir = f"/tmp/aether-py-fence-{self.port}"
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
        self.url = f"nats://127.0.0.1:{self.port}"
        self.nc = await nats.connect(self.url)
        self._tasks: list[asyncio.Task] = []

    async def asyncTearDown(self):
        for t in self._tasks:
            t.cancel()
        if not self.nc.is_closed:
            await self.nc.close()
        self.proc.terminate()

    async def _put_record(self, conn, key, epoch):
        js = conn.jetstream()
        kv = await js.create_key_value(bucket=BUCKET)
        await kv.put(key, json.dumps({"holder": "lord-a", "ts": 0, "epoch": epoch}).encode())

    async def _run_fencing(self, conn, key, epoch, on_lost):
        cfg = {"bucket": BUCKET, "key": key, "epoch": epoch}
        kv = await conn.jetstream().key_value(BUCKET)
        task = asyncio.create_task(
            aether._fencing(kv, cfg, aether.new_logger(), asyncio.Event(), on_lost, LEASE_MS, INTERVAL_MS)
        )
        self._tasks.append(task)  # keep a reference so the task is not GC'd mid-run
        return task

    async def test_stays_while_epoch_holds(self):
        await self._put_record(self.nc, "hold", 1)
        fired = []
        task = await self._run_fencing(self.nc, "hold", 1, lambda r: fired.append(r))
        await asyncio.sleep((LEASE_MS + 3 * INTERVAL_MS) / 1000.0)
        task.cancel()
        self.assertEqual(fired, [])

    async def test_fires_on_epoch_takeover(self):
        await self._put_record(self.nc, "takeover", 1)
        lost = asyncio.get_event_loop().create_future()
        await self._run_fencing(self.nc, "takeover", 1, lambda r: lost.set_result(r))
        await self._put_record(self.nc, "takeover", 2)  # a successor stamps a new epoch
        await asyncio.wait_for(lost, timeout=2.0)

    async def test_fires_when_key_gone(self):
        await self._put_record(self.nc, "purge", 1)
        lost = asyncio.get_event_loop().create_future()
        await self._run_fencing(self.nc, "purge", 1, lambda r: lost.set_result(r))
        js = self.nc.jetstream()
        kv = await js.key_value(BUCKET)
        await kv.purge("purge")
        await asyncio.wait_for(lost, timeout=2.0)

    async def test_fires_only_after_lease_when_unreachable(self):
        # A dedicated connection we can close without disturbing setUp/tearDown.
        own = await nats.connect(self.url)
        await self._put_record(own, "unreachable", 1)
        start = time.monotonic()
        fired_at = []
        await self._run_fencing(own, "unreachable", 1, lambda r: fired_at.append(time.monotonic() - start))

        # Let the loop confirm the lock once, then make the KV unverifiable.
        await asyncio.sleep(INTERVAL_MS / 1000.0)
        await own.close()

        # Must not fire before the lease elapses.
        await asyncio.sleep((LEASE_MS - 2 * INTERVAL_MS) / 1000.0)
        self.assertEqual(fired_at, [])

        # Must fire once the lease has elapsed.
        await asyncio.sleep((LEASE_MS + 3 * INTERVAL_MS) / 1000.0)
        self.assertTrue(fired_at, "fencing did not fire after the lease elapsed")
        self.assertGreaterEqual(fired_at[0], LEASE_MS / 1000.0)


if __name__ == "__main__":
    unittest.main()
