"""Confirmed durable cast tests for the Python SDK. These need a real JetStream server; they
spawn `nats-server` if present and skip otherwise (so a machine without the binary stays green).

Run: uv run --with nats-py -m unittest confirm_test   (from sdk/python)
"""

import asyncio
import random
import shutil
import socket
import subprocess
import unittest

import nats
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig

import aether

NATS_SERVER = shutil.which("nats-server")

APP = "cf"
TIMEOUT = 2.0


async def _start_server():
    """Start a JetStream-enabled nats-server on a random port and wait until it accepts."""
    port = random.randint(17000, 18000)
    proc = subprocess.Popen(
        [NATS_SERVER, "-js", "-a", "127.0.0.1", "-p", str(port), "-sd", f"/tmp/aether-py-cf-{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(100):
        with socket.socket() as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        await asyncio.sleep(0.05)
    return proc, f"nats://127.0.0.1:{port}"


async def _provision_mailbox(nc, name, **extra):
    """Create the durable mailbox the lord would (a WorkQueue stream over the target's cast
    subject); extra adjusts it for tests that need a stream to refuse writes."""
    await nc.jetstream().add_stream(StreamConfig(
        name=aether._stream(APP, name),
        subjects=[aether._sub_cast(APP, name)],
        retention=RetentionPolicy.WORK_QUEUE,
        storage=StorageType.MEMORY,
        **extra,
    ))


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH")
class ConfirmedCastTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.proc, url = await _start_server()
        self.nc = await nats.connect(url)

    async def asyncTearDown(self):
        await self.nc.close()
        self.proc.terminate()
        self.proc.wait()

    async def _stored_count(self, name):
        info = await self.nc.jetstream().stream_info(aether._stream(APP, name))
        return info.state.messages

    async def test_returns_once_the_message_is_in_the_mailbox(self):
        await _provision_mailbox(self.nc, "stored")

        await aether._send_confirmed_cast(self.nc, APP, "t-1", "stored", "sample", {"temp": 71.4},
                                          TIMEOUT, None)

        self.assertEqual(await self._stored_count("stored"), 1)
        msg = await self.nc.jetstream().get_msg(aether._stream(APP, "stored"), 1)
        e = aether._decode(msg.data)
        self.assertEqual((e["kind"], e["to"], e["op"], e["trace"]), ("cast", "stored", "sample", "t-1"))
        self.assertEqual(e["payload"], {"temp": 71.4})

    async def test_non_durable_target_raises_and_nothing_is_sent(self):
        received = []

        async def on_msg(msg):
            received.append(msg)

        await self.nc.subscribe(aether._sub_cast(APP, "plain"), cb=on_msg)

        with self.assertRaises(aether.NotDurableError):
            await aether._send_confirmed_cast(self.nc, APP, "t-1", "plain", "sample", None,
                                              TIMEOUT, None)

        await self.nc.flush()
        await asyncio.sleep(0.2)
        self.assertEqual(received, [])

    async def test_retry_with_same_idempotency_key_lands_once(self):
        await _provision_mailbox(self.nc, "dedup")

        for key in ("sample-1", "sample-1", "sample-2"):
            await aether._send_confirmed_cast(self.nc, APP, "t-1", "dedup", "sample", None,
                                              TIMEOUT, key)

        self.assertEqual(await self._stored_count("dedup"), 2)

    async def test_write_the_mailbox_refuses_raises(self):
        await _provision_mailbox(self.nc, "full", max_msgs=1, discard=DiscardPolicy.NEW)

        await aether._send_confirmed_cast(self.nc, APP, "t-1", "full", "sample", None, TIMEOUT, None)
        with self.assertRaises(Exception):
            await aether._send_confirmed_cast(self.nc, APP, "t-1", "full", "sample", None,
                                              TIMEOUT, None)

        self.assertEqual(await self._stored_count("full"), 1)

    async def test_bus_down_raises_even_for_a_cached_mailbox(self):
        # Its own connection without reconnect, so the outage surfaces as an error at once.
        nc = await nats.connect(self.nc.connected_url.geturl(), allow_reconnect=False)
        try:
            await _provision_mailbox(nc, "outage")
            await aether._send_confirmed_cast(nc, APP, "t-1", "outage", "sample", None, TIMEOUT, None)

            self.proc.terminate()
            self.proc.wait()

            with self.assertRaises(Exception):
                await aether._send_confirmed_cast(nc, APP, "t-1", "outage", "sample", None,
                                                  TIMEOUT, None)
        finally:
            await nc.close()

    async def test_ctx_cast_confirmed_propagates_trace_and_key(self):
        await _provision_mailbox(self.nc, "sink")
        ctx = aether.Ctx(nats=self.nc, name="driver", app=APP, trace="t-handler")

        await ctx.cast_confirmed("sink", "sample", timeout=TIMEOUT, idempotency_key="k-1")

        msg = await self.nc.jetstream().get_msg(aether._stream(APP, "sink"), 1)
        e = aether._decode(msg.data)
        self.assertEqual((e["trace"], e["idem"]), ("t-handler", "k-1"))


if __name__ == "__main__":
    unittest.main()
