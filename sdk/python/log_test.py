"""Structured logging tests for the Python SDK.

No running NATS is needed - the logger writes through an injected sink.

Run: uv run --with nats-py -m unittest log_test   (from sdk/python)
"""

import json
import unittest

from aether import Logger, new_logger


def capture():
    lines: list[str] = []
    return lines, lines.append


class LogTest(unittest.TestCase):
    def test_json_carries_base_and_call_fields(self):
        lines, write = capture()
        log = Logger({"component": "thrall", "app": "counter", "name": "worker"}, level=20, fmt="json", write=write)
        log.info("thrall ready", pid=42)

        self.assertEqual(len(lines), 1)
        rec = json.loads(lines[0])
        self.assertEqual(rec["level"], "INFO")
        self.assertEqual(rec["msg"], "thrall ready")
        self.assertEqual(rec["component"], "thrall")
        self.assertEqual(rec["app"], "counter")
        self.assertEqual(rec["name"], "worker")
        self.assertEqual(rec["pid"], 42)
        self.assertIsInstance(rec["time"], str)

    def test_level_below_threshold_dropped(self):
        lines, write = capture()
        log = Logger(level=30, fmt="json", write=write)  # warn
        log.info("dropped")
        log.warn("kept")

        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["msg"], "kept")

    def test_text_is_default_rendering(self):
        lines, write = capture()
        log = Logger({"name": "w"}, level=20, fmt="text", write=write)
        log.info("hello")

        self.assertFalse(lines[0].startswith("{"))
        self.assertIn("INFO", lines[0])
        self.assertIn("hello", lines[0])
        self.assertIn("name=w", lines[0])

    def test_with_derives_child_logger(self):
        lines, write = capture()
        base = Logger({"app": "counter"}, level=20, fmt="json", write=write)
        base.with_(name="worker").error("boom", err="nope")

        rec = json.loads(lines[0])
        self.assertEqual(rec["app"], "counter")
        self.assertEqual(rec["name"], "worker")
        self.assertEqual(rec["err"], "nope")
        self.assertEqual(rec["level"], "ERROR")

    def test_new_logger_factory_defaults_from_env(self):
        # new_logger() reads env; with nothing set it defaults to info/text and does not raise.
        log = new_logger(component="thrall")
        self.assertIsInstance(log, Logger)


if __name__ == "__main__":
    unittest.main()
