"""End-to-end and unit tests of a web thrall's two dispatch modes in the Python SDK.

The end-to-end part runs a real web thrall process (testdata/web/peer.py, built on run_web) on a
real nats-server and calls it with the real Client, the way the ingress does. It spawns
`nats-server` if present and skips otherwise; CI installs it. The unit part drives the call pool
by hand, with no bus.

Run: uv run --with nats-py -m unittest web_test   (from sdk/python)
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
PEER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "testdata", "web", "peer.py")
APP = "ae"


class _QuietLog:
    def debug(self, *_a, **_k): pass
    def info(self, *_a, **_k): pass
    def warn(self, *_a, **_k): pass
    def error(self, *_a, **_k): pass


class _Held:
    """A handler the test controls: it reports when it started and finishes on release()."""

    def __init__(self):
        self.started = False
        self._release = asyncio.Event()

    async def handle(self):
        self.started = True
        await self._release.wait()

    def release(self):
        self._release.set()


def _call(call_id, deadline=None):
    e = {"v": 1, "id": call_id, "kind": "call", "op": "http_request"}
    if deadline is not None:
        e["deadline"] = deadline
    return e


def _now_ms():
    return int(time.time() * 1000)


class CallPoolTest(unittest.IsolatedAsyncioTestCase):
    """The call pool: what a concurrent web thrall has in place of the serialized mailbox."""

    def _pool(self, limit):
        os.environ["AETHER_DEADLINE_SKEW_MS"] = "0"  # the test and the pool share one clock
        try:
            stats = aether._MailboxStats()
            log = _QuietLog()
            return aether._CallPool(limit, aether._DeadlineGate(stats, log), stats, log), stats
        finally:
            del os.environ["AETHER_DEADLINE_SKEW_MS"]

    async def _settle(self):
        for _ in range(5):
            await asyncio.sleep(0)

    async def test_slow_call_does_not_hold_up_a_fast_one(self):
        pool, stats = self._pool(2)
        slow, fast = _Held(), _Held()
        replies = []

        async def respond(reply):
            replies.append(reply)

        pool.submit(_call("slow"), respond, slow.handle)
        pool.submit(_call("fast"), respond, fast.handle)
        await self._settle()
        self.assertTrue(slow.started and fast.started)
        self.assertEqual((stats.snapshot()["mailbox_depth"], stats.snapshot()["in_flight"]), (2, 2))

        fast.release()
        await self._settle()
        snap = stats.snapshot()
        self.assertEqual((snap["mailbox_depth"], snap["in_flight"], snap["processed_total"]), (1, 1, 1))
        slow.release()
        await pool.drain()
        self.assertNotIn("in_flight", stats.snapshot())  # omitted while zero
        self.assertEqual(replies, [])  # the pool itself answers only a call it drops

    async def test_calls_beyond_the_limit_wait_their_turn(self):
        pool, stats = self._pool(1)
        held = [_Held(), _Held(), _Held()]
        for i, h in enumerate(held):
            pool.submit(_call(f"c-{i}"), None, h.handle)
        await self._settle()
        self.assertEqual([h.started for h in held], [True, False, False])
        self.assertEqual((stats.snapshot()["mailbox_depth"], stats.snapshot()["in_flight"]), (3, 1))

        held[0].release()
        await self._settle()
        self.assertEqual([h.started for h in held], [True, True, False])  # arrival order
        held[1].release()
        held[2].release()
        await pool.drain()
        self.assertEqual((stats.snapshot()["mailbox_depth"], stats.snapshot()["processed_total"]), (0, 3))

    async def test_waiting_call_is_dropped_at_its_deadline(self):
        pool, stats = self._pool(1)
        busy, late = _Held(), _Held()
        replies = []
        dropped = asyncio.Event()

        async def respond(reply):
            replies.append(reply)
            dropped.set()

        pool.submit(_call("busy"), respond, busy.handle)
        sent = time.monotonic()
        pool.submit(_call("late", _now_ms() + 80), respond, late.handle)
        await asyncio.wait_for(dropped.wait(), timeout=2)
        waited = time.monotonic() - sent

        self.assertTrue(busy.started)  # the slot was still taken when the call was dropped
        self.assertFalse(late.started)
        self.assertGreaterEqual(waited, 0.07)
        self.assertEqual(len(replies), 1)
        self.assertEqual((replies[0]["id"], replies[0]["error"]["type"], replies[0]["error"]["retryable"]),
                         ("late", "deadline_exceeded", True))
        snap = stats.snapshot()
        self.assertEqual((snap["mailbox_depth"], snap["in_flight"], snap["expired_total"], snap["processed_total"]),
                         (1, 1, 1, 0))

        # The dropped call left the queue: the next one gets the slot as soon as it is free.
        nxt = _Held()
        pool.submit(_call("next"), respond, nxt.handle)
        busy.release()
        await self._settle()
        self.assertTrue(nxt.started)
        nxt.release()
        await pool.drain()

    async def test_drain_waits_and_takes_no_new_calls(self):
        pool, _ = self._pool(1)
        running, waiting, refused = _Held(), _Held(), _Held()
        pool.submit(_call("running"), None, running.handle)
        pool.submit(_call("waiting"), None, waiting.handle)
        await self._settle()

        drain = asyncio.ensure_future(pool.drain())
        await self._settle()
        pool.submit(_call("after-drain"), None, refused.handle)
        await self._settle()
        self.assertFalse(refused.started)
        self.assertFalse(drain.done())

        running.release()
        await self._settle()
        self.assertTrue(waiting.started)  # a call that was already waiting still gets its turn
        self.assertFalse(drain.done())
        waiting.release()
        await asyncio.wait_for(drain, timeout=2)

    async def test_failing_handler_frees_its_slot(self):
        pool, stats = self._pool(1)

        async def boom():
            raise RuntimeError("boom")

        pool.submit(_call("boom"), None, boom)
        nxt = _Held()
        pool.submit(_call("next"), None, nxt.handle)
        await self._settle()
        self.assertTrue(nxt.started)
        nxt.release()
        await pool.drain()
        self.assertEqual((stats.snapshot()["mailbox_depth"], stats.snapshot()["processed_total"]), (0, 2))


class WebDefTest(unittest.TestCase):
    """A web definition is one of two shapes, and only serve runs concurrently."""

    def test_shapes_and_their_concurrency(self):
        def handle(req, state, ctx):
            return aether.text_response(200, "ok"), state

        def serve(req, shared, ctx):
            return aether.text_response(200, "ok")

        self.assertEqual(aether.def_web(handle=handle)._concurrency, 1)
        self.assertEqual(aether.def_web(handle=handle, concurrency=1)._concurrency, 1)
        self.assertEqual(aether.def_web(serve=serve)._concurrency, aether.DEFAULT_WEB_CONCURRENCY)
        self.assertEqual(aether.def_web(serve=serve, concurrency=1)._concurrency, 1)
        self.assertEqual(aether.def_web(serve=serve, concurrency=8)._concurrency, 8)
        # An ordinary thrall is serialized, and def_thrall offers no way to say otherwise.
        self.assertEqual(aether.def_thrall("plain", lambda ctx: 0)._concurrency, 1)

        for bad in (dict(), dict(handle=handle, serve=serve), dict(handle=handle, concurrency=4),
                    dict(serve=serve, concurrency=-1), dict(serve=serve, concurrency=1.5),
                    dict(serve=serve, concurrency=True)):
            with self.subTest(bad=sorted(bad)), self.assertRaises(ValueError):
                aether.def_web(name="web", **bad)

    def test_serve_handler_has_no_next_state(self):
        shared = {"hits": 0}

        def serve(req, s, ctx):
            s["hits"] += 1
            return aether.text_response(200, f"path={req.path} hits={s['hits']}")

        defn = aether.def_web(init=lambda ctx: shared, serve=serve)
        wire_resp, next_state = asyncio.run(
            defn.handle_call[aether.OP_HTTP_REQUEST]({"method": "GET", "path": "/x"}, shared, None))
        self.assertIs(next_state, shared)
        self.assertEqual(aether.HTTPResponse.from_wire(wire_resp).body_text(), "path=/x hits=1")


async def _start_server():
    port = random.randint(20001, 21000)
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
class WebDispatchTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server, self.url = await _start_server()
        self.nc = await nats.connect(self.url)
        self.client = aether.Client(self.nc, APP)
        self.peers = []

    async def asyncTearDown(self):
        for p in self.peers:
            p.terminate()
        for p in self.peers:
            p.wait()
        await self.nc.close()
        self.server.terminate()
        self.server.wait()

    async def _start_web_peer(self, name, shape, concurrency=None):
        """Run the web peer the way the lord would and return once it answers. shape and
        concurrency pick what is under test; the deadline is judged exactly (one host, one clock)."""
        env = {**os.environ, "AETHER_NATS_URL": self.url, "AETHER_APP": APP, "AETHER_NAME": name,
               "AETHER_LOG_LEVEL": "error", "AETHER_HEARTBEAT_INTERVAL_MS": "100",
               "AETHER_DEADLINE_SKEW_MS": "0", "WEB_SHAPE": shape}
        env.pop("WEB_CONCURRENCY", None)
        if concurrency is not None:
            env["WEB_CONCURRENCY"] = str(concurrency)
        proc = subprocess.Popen([sys.executable, PEER], env=env, stdout=subprocess.DEVNULL)
        self.peers.append(proc)
        for _ in range(100):
            try:
                await self.client.call(name, aether.OP_HTTP_REQUEST, {"method": "GET", "path": "/fast"}, timeout=0.2)
                return proc
            except (NoRespondersError, NatsTimeoutError):
                await asyncio.sleep(0.05)
        self.fail(f"web peer {name} did not become ready")

    async def _get(self, name, path, query="", trace=None):
        """Send one request the way the ingress does; return (body text, seconds it took)."""
        started = time.monotonic()
        payload = aether.HTTPRequest(method="GET", path=path, query=query).to_wire()
        reply = await self.client.call(name, aether.OP_HTTP_REQUEST, payload, timeout=5, trace=trace)
        return aether.HTTPResponse.from_wire(reply).body_text(), time.monotonic() - started

    async def _heartbeat(self, name, want):
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

    async def test_serve_slow_request_does_not_hold_up_a_fast_one(self):
        await self._start_web_peer("web", "serve", 4)
        slow = asyncio.ensure_future(self._get("web", "/slow", "ms=800"))
        await asyncio.sleep(0.1)  # the slow request is in its handler
        body, took = await self._get("web", "/fast")
        self.assertEqual(body, "fast")
        self.assertLess(took, 0.4)  # answered while the slow one still had ~700 ms to go
        body, took = await slow
        self.assertEqual(body, "slow")
        self.assertGreaterEqual(took, 0.7)

    async def test_serve_gives_each_request_its_own_ctx(self):
        await self._start_web_peer("web", "serve", 4)
        # Both requests are inside their handlers together (each sleeps 50 ms before reading ctx).
        (a, _), (b, _) = await asyncio.gather(self._get("web", "/trace", trace="trace-a"),
                                             self._get("web", "/trace", trace="trace-b"))
        self.assertEqual((a, b), ("trace-a", "trace-b"))

    async def test_serve_heartbeat_tells_running_from_waiting(self):
        await self._start_web_peer("web", "serve", 2)
        idle = await self._heartbeat("web", lambda m: m.get("mailbox_depth") == 0)
        self.assertNotIn("in_flight", idle)
        self.assertEqual(idle["describe"]["concurrency"], 2)
        self.assertEqual(idle["describe"]["call_ops"], [aether.OP_HTTP_REQUEST])

        requests = [asyncio.ensure_future(self._get("web", "/slow", "ms=700")) for _ in range(3)]
        busy = await self._heartbeat("web", lambda m: m.get("mailbox_depth") == 3)
        self.assertEqual(busy["in_flight"], 2)  # two in their handlers, the third waiting for a slot
        await asyncio.gather(*requests)
        await self._heartbeat("web", lambda m: m.get("mailbox_depth") == 0 and "in_flight" not in m)

    async def test_serve_waiting_request_is_dropped_at_its_callers_deadline(self):
        await self._start_web_peer("web", "serve", 2)
        counted = int((await self._get("web", "/hits"))[0])
        busy = [asyncio.ensure_future(self._get("web", "/slow", "ms=900")) for _ in range(2)]
        await asyncio.sleep(0.1)  # both slots are taken

        # Sent by hand, so that this test - unlike a real caller - still listens after the deadline.
        sent = _now_ms()
        envelope = {"v": 1, "id": "queued-1", "trace": "t-queued", "kind": "call", "to": "web",
                    "op": aether.OP_HTTP_REQUEST, "payload": {"method": "GET", "path": "/hits"},
                    "ts": sent, "deadline": sent + 200}
        msg = await self.nc.request(aether._sub_call(APP, "web"), json.dumps(envelope).encode(), timeout=5)
        waited = _now_ms() - sent
        reply = json.loads(msg.data)

        self.assertEqual((reply["status"], reply["error"]["type"], reply["error"]["retryable"]),
                         ("error", "deadline_exceeded", True))
        self.assertGreaterEqual(waited, 150)
        self.assertLess(waited, 700)  # at its deadline, not when a slot came free at ~900 ms

        await asyncio.gather(*busy)
        beat = await self._heartbeat("web", lambda m: m.get("mailbox_depth") == 0)
        self.assertEqual(beat["expired_total"], 1)
        # The dropped /hits request never ran: since the count was read, the thrall has counted
        # the two slow requests and now this one.
        self.assertEqual(int((await self._get("web", "/hits"))[0]), counted + 3)

    async def test_serialized_shapes_serve_one_request_at_a_time(self):
        for label, shape, concurrency in (("serve-1", "serve", 1), ("handle", "handle", None)):
            with self.subTest(shape=label):
                await self._start_web_peer(label, shape, concurrency)
                slow = asyncio.ensure_future(self._get(label, "/slow", "ms=600"))
                await asyncio.sleep(0.1)
                _, took = await self._get(label, "/fast")
                self.assertGreaterEqual(took, 0.4)  # it waited for the slow request to return
                await slow
                beat = await self._heartbeat(label, lambda m: m.get("mailbox_depth") == 0)
                self.assertNotIn("concurrency", beat.get("describe", {}))  # a serialized thrall declares none

    async def test_failing_handler_costs_one_request_not_the_process(self):
        for label, shape, concurrency in (("fail-serve", "serve", 4), ("fail-handle", "handle", None)):
            with self.subTest(shape=label):
                proc = await self._start_web_peer(label, shape, concurrency)
                before = int((await self._get(label, "/hits"))[0])

                # A raise, a raise after an await, and a handler that returned nothing.
                for path in ("/throw", "/reject", "/nothing"):
                    reply = await self.client.call(label, aether.OP_HTTP_REQUEST,
                                                   {"method": "GET", "path": path}, timeout=5)
                    resp = aether.HTTPResponse.from_wire(reply)
                    self.assertEqual((resp.status, resp.body_text()), (500, "Internal Server Error"))
                    self.assertEqual((await self._get(label, "/fast"))[0], "fast")  # the next one is served

                beat = await self._heartbeat(label, lambda m: m.get("panics_total") == 3)
                self.assertEqual(beat["describe"]["last_error"],
                                 "handler_error: the web handler did not return an HTTPResponse")
                self.assertEqual(beat["mailbox_depth"], 0)
                self.assertIsNone(proc.poll())  # still running

                if shape == "handle":
                    # The failing requests left the threaded state alone: only the three /fast counted.
                    self.assertEqual(int((await self._get(label, "/hits"))[0]), before + 4)

    async def test_refusal_from_a_web_handler_stays_a_typed_error(self):
        await self._start_web_peer("web", "serve", 4)
        with self.assertRaises(aether.CallError) as raised:
            await self._get("web", "/refuse")
        self.assertEqual(aether.refusal_code(raised.exception), "not_for_you")
        beat = await self._heartbeat("web", lambda m: m.get("mailbox_depth") == 0)
        self.assertNotIn("panics_total", beat)  # a refusal is not a failure

    async def test_handle_threads_its_state(self):
        await self._start_web_peer("web", "handle")
        first = int((await self._get("web", "/hits"))[0])
        second = int((await self._get("web", "/hits"))[0])
        self.assertEqual(second, first + 1)

    async def test_drain_lets_an_in_flight_request_finish(self):
        for label, shape, concurrency in (("drain-serve", "serve", 4), ("drain-handle", "handle", None)):
            with self.subTest(shape=label):
                proc = await self._start_web_peer(label, shape, concurrency)
                in_flight = asyncio.ensure_future(self._get(label, "/slow", "ms=600"))
                await asyncio.sleep(0.1)
                await self.nc.publish(aether._sub_ctl(label), json.dumps({"v": 1, "kind": "ctl", "op": "drain"}).encode())
                body, _ = await in_flight
                self.assertEqual(body, "slow")
                self.assertEqual(await asyncio.to_thread(proc.wait, 5), 0)


if __name__ == "__main__":
    unittest.main()
