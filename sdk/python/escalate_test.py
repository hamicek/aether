"""Escalate (typed let-it-crash) tests for the Python SDK.

The full dispatch -> reply -> exit -> restart behavior is proven end-to-end in the Go lord
integration tests (a real re-exec'd thrall process); here we cover the SDK's own primitives:
the signal carries its reason, the wire error reply is the distinguishable "escalated" shape,
and the exit seam is the immediate os._exit (not a swallowable sys.exit).

Run: uv run --with nats-py -m unittest escalate_test   (from sdk/python)
"""

import os
import unittest

import aether


class EscalateSignal(unittest.TestCase):
    def test_carries_reason(self):
        esc = aether.Escalate("mailbox poisoned")
        self.assertEqual(esc.reason, "mailbox poisoned")
        self.assertIsInstance(esc, Exception)
        self.assertEqual(str(esc), "mailbox poisoned")

    def test_error_reply_is_distinguishable(self):
        # The reply a call handler's escalation sends the caller: an error reply typed
        # "escalated", distinct from the "handler_error" a plain exception produces.
        req = {"v": 1, "id": "abc", "kind": "call", "op": "boom"}
        reply = aether._err_reply(req, "escalated", "call asked to crash")
        self.assertEqual(reply["status"], "error")
        self.assertEqual(reply["error"]["type"], "escalated")
        self.assertEqual(reply["error"]["message"], "call asked to crash")

    def test_exit_seam_is_immediate(self):
        # Production must terminate with os._exit so the crash is immediate and cannot be
        # swallowed by an enclosing except, mirroring the Go/TS abnormal exit.
        self.assertIs(aether._exit_process, os._exit)


if __name__ == "__main__":
    unittest.main()
