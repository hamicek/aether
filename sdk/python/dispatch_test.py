"""End-to-end tests of typed refusals and deadline drops in the Python SDK: a real thrall process
(the cross-language peer, built on aether.run) on a real nats-server, called by the real Client.
They spawn `nats-server` if present and skip otherwise (so a machine without the binary stays
green). CI installs it.

Run: uv run --with nats-py -m unittest dispatch_test   (from sdk/python)
"""

import asyncio
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time
import unittest

import nats
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError

import aether

NATS_SERVER = shutil.which("nats-server")
PEER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "testdata", "crosslang", "peer.py")


async def _start_server():
    port = random.randint(19000, 20000)
    proc = subprocess.Popen([NATS_SERVER, "-a", "127.0.0.1", "-p", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        with socket.socket() as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        await asyncio.sleep(0.05)
    return proc, f"nats://127.0.0.1:{port}"


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH")
class DispatchTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server, self.url = await _start_server()
        self.nc = await nats.connect(self.url)
        self.peers = []

    async def asyncTearDown(self):
        for p in self.peers:
            p.terminate()
        for p in self.peers:
            p.wait()
        await self.nc.close()
        self.server.terminate()
        self.server.wait()

    async def _start_peer(self, app, name, skew_ms=None):
        """Run the peer thrall the way the lord would - identity through the environment - and
        return once it answers on its call subject. skew_ms sets its clock-skew tolerance."""
        env = {**os.environ, "AETHER_NATS_URL": self.url, "AETHER_APP": app, "AETHER_NAME": name,
               "AETHER_LOG_LEVEL": "error", "AETHER_HEARTBEAT_INTERVAL_MS": "100"}
        env.pop("AETHER_DEADLINE_SKEW_MS", None)
        if skew_ms is not None:
            env["AETHER_DEADLINE_SKEW_MS"] = str(skew_ms)
        self.peers.append(subprocess.Popen([sys.executable, PEER], env=env, stdout=subprocess.DEVNULL))
        probe = aether.Client(self.nc, app)
        for _ in range(100):
            try:
                await probe.call(name, "balance", timeout=0.2)
                return
            except (NoRespondersError, NatsTimeoutError):
                await asyncio.sleep(0.05)
        self.fail(f"peer {app}/{name} did not become ready")

    async def _heartbeat(self, name, want):
        """Wait for a heartbeat of the thrall that satisfies want and return its payload."""
        sub = await self.nc.subscribe(aether._sub_hb(name))
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    msg = await sub.next_msg(timeout=1)
                except NatsTimeoutError:
                    continue
                metrics = json.loads(msg.data).get("payload") or {}
                if want(metrics):
                    return metrics
        finally:
            await sub.unsubscribe()
        self.fail(f"no matching heartbeat from {name}")

    async def _raw_call(self, app, e):
        """Send a hand-built call envelope and return the decoded reply, for deadlines the Client
        would never stamp (already past, or none at all - the shape an old caller sends)."""
        msg = await self.nc.request(aether._sub_call(app, e["to"]), json.dumps(e).encode(), timeout=2)
        return json.loads(msg.data)

    async def test_refusal_round_trip_across_apps(self):
        await self._start_peer("ae", "account")
        # The caller lives in app "ujito"; the shared thrall in app "ae".
        caller = aether.Client(self.nc, "ujito")
        with self.assertRaises(NoRespondersError):
            await caller.call("account", "balance", timeout=0.5)
        account = caller.with_(app="ae")

        with self.assertRaises(aether.CallError) as raised:
            await account.call("account", "withdraw", {"amount": 5})
        refusal = aether.as_refusal(raised.exception)
        self.assertIsNotNone(refusal)
        self.assertEqual((refusal.type, refusal.code, refusal.message, refusal.retryable),
                         ("refused", "insufficient_funds", "the balance is 3, the withdrawal 5", False))

        self.assertEqual(await account.call("account", "balance"), 3)  # the refusal changed nothing
        self.assertEqual(await account.call("account", "withdraw", {"amount": 2}), 1)  # an allowed one goes through

        # A refusal is not the thrall's last error...
        beat = await self._heartbeat("account", lambda m: m.get("processed_total", 0) >= 4)
        self.assertNotIn("last_error", beat.get("describe", {}))

        # ...while an ordinary handler error is, and reads as a failure, not a refusal.
        with self.assertRaises(aether.CallError) as raised:
            await account.call("account", "broken")
        self.assertEqual((raised.exception.type, raised.exception.refused), ("handler_error", False))
        self.assertEqual(aether.refusal_code(raised.exception), "")
        await self._heartbeat("account", lambda m: m.get("describe", {}).get("last_error") == "handler_error: disk on fire")

    async def test_timed_out_call_is_not_executed_later(self):
        await self._start_peer("ae", "ledger", skew_ms=0)  # caller and thrall share this host's clock
        ledger = aether.Client(self.nc, "ae")

        # Hold the mailbox; calls sent meanwhile queue behind the slow handler.
        held = asyncio.create_task(ledger.call("ledger", "hold", {"ms": 400}, timeout=5))
        await asyncio.sleep(0.05)

        # The caller gives up after 100 ms while its call is still queued...
        with self.assertRaises(NatsTimeoutError):
            await ledger.call("ledger", "write", timeout=0.1)

        # ...and when the thrall gets to it, it must not run: the first write that executes is a fresh one.
        await held
        self.assertEqual(await ledger.call("ledger", "write"), 1)

        beat = await self._heartbeat("ledger", lambda m: m.get("expired_total") == 1)
        self.assertEqual(beat["mailbox_depth"], 0)

    async def test_skew_tolerance_and_old_shapes(self):
        await self._start_peer("ae", "account")  # default tolerance
        async def write(deadline_offset_ms=None):
            # The deadline is set relative to the moment of sending, so the margin to the tolerance
            # does not depend on how long the earlier calls took.
            now = int(time.time() * 1000)
            e = {"v": 1, "id": f"raw-{random.random()}", "kind": "call", "to": "account", "op": "write",
                 "payload": {}, "ts": now - 3600_000}
            if deadline_offset_ms is not None:
                e["deadline"] = now + deadline_offset_ms
            return await self._raw_call("ae", e)

        self.assertEqual((await write())["status"], "ok")  # an old caller: no deadline, however old
        self.assertEqual((await write(5000))["status"], "ok")
        self.assertEqual((await write(-50))["status"], "ok")  # overdue by less than the 250 ms tolerance

        dropped = await write(-2000)
        self.assertEqual(dropped["status"], "error")
        self.assertEqual(dropped["error"], {"type": "deadline_exceeded", "message": dropped["error"]["message"],
                                            "retryable": True})
        self.assertIn("expired", dropped["error"]["message"])

        account = aether.Client(self.nc, "ae")
        self.assertEqual(await account.call("account", "writes"), 3)  # the expired call did not run

        # An old caller reads a refusal as the error string it always got: "<type>: <message>".
        reply = await self._raw_call("ae", {"v": 1, "id": "old-1", "trace": "t-old", "kind": "call", "to": "account",
                                           "op": "withdraw", "payload": {"amount": 50},
                                           "ts": int(time.time() * 1000)})
        self.assertEqual(f"{reply['error']['type']}: {reply['error']['message']}",
                         "refused: the balance is 3, the withdrawal 50")


if __name__ == "__main__":
    unittest.main()
