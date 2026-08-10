"""Heartbeat interval resolution tests for the Python SDK.

Run: uv run --with nats-py -m unittest heartbeat_test   (from sdk/python)
"""

import os
import unittest

import aether


class HeartbeatIntervalTest(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("AETHER_HEARTBEAT_INTERVAL_MS", None)

    def test_default_when_unset_or_invalid(self):
        os.environ.pop("AETHER_HEARTBEAT_INTERVAL_MS", None)
        self.assertEqual(aether._heartbeat_interval(), 2.0)
        os.environ["AETHER_HEARTBEAT_INTERVAL_MS"] = "nonsense"
        self.assertEqual(aether._heartbeat_interval(), 2.0)
        os.environ["AETHER_HEARTBEAT_INTERVAL_MS"] = "0"
        self.assertEqual(aether._heartbeat_interval(), 2.0)

    def test_configured_value(self):
        os.environ["AETHER_HEARTBEAT_INTERVAL_MS"] = "500"
        self.assertEqual(aether._heartbeat_interval(), 0.5)

    def test_clamped_to_floor(self):
        os.environ["AETHER_HEARTBEAT_INTERVAL_MS"] = "10"
        self.assertEqual(aether._heartbeat_interval(), 0.1)


if __name__ == "__main__":
    unittest.main()
