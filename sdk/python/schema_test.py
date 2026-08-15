"""Payload-contract schema helper tests for the Python SDK.

Mirrors sdk/go/schema and sdk/ts/src/schema.test.ts.

Run: uv run --with fastjsonschema --with nats-py -m unittest schema_test   (from sdk/python)
"""

import unittest

from aether import ValidationError, decode, validate

MEASUREMENT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["siteId", "metric", "value", "ts"],
    "properties": {
        "siteId": {"type": "string", "minLength": 1},
        "metric": {"type": "string", "enum": ["voltage", "current", "temperature"]},
        "value": {"type": "number"},
        "unit": {"type": "string"},
        "ts": {"type": "integer"},
    },
}


class SchemaHelperTest(unittest.TestCase):
    def test_validate_accepts_valid_payload(self):
        validate(MEASUREMENT_SCHEMA, {"siteId": "s-1", "metric": "voltage", "value": 231.4, "ts": 1700000000000})

    def test_validate_reports_field_path(self):
        cases = [
            ("wrong type", {"siteId": "s-1", "metric": "voltage", "value": "hot", "ts": 1}, "/value"),
            ("out of enum", {"siteId": "s-1", "metric": "pressure", "value": 1, "ts": 1}, "/metric"),
        ]
        for name, payload, want_path in cases:
            with self.subTest(name):
                with self.assertRaises(ValidationError) as cm:
                    validate(MEASUREMENT_SCHEMA, payload)
                self.assertTrue(
                    any(want_path in p.path for p in cm.exception.problems),
                    f"no problem path contains {want_path!r}: {cm.exception.problems}",
                )

    def test_validate_rejects_missing_required(self):
        with self.assertRaises(ValidationError):
            validate(MEASUREMENT_SCHEMA, {"siteId": "s-1", "metric": "voltage", "value": 1})

    def test_decode_returns_validated_value(self):
        m = decode(MEASUREMENT_SCHEMA, {"siteId": "s-1", "metric": "current", "value": 12.5, "unit": "A", "ts": 1})
        self.assertEqual(m["siteId"], "s-1")
        self.assertEqual(m["metric"], "current")
        self.assertEqual(m["value"], 12.5)

    def test_decode_rejects_invalid(self):
        with self.assertRaises(ValidationError):
            decode(MEASUREMENT_SCHEMA, {"siteId": "", "metric": "voltage", "value": 1, "ts": 1})

    def test_accepts_json_bytes(self):
        validate(MEASUREMENT_SCHEMA, b'{"siteId":"s-1","metric":"voltage","value":1,"ts":1}')


if __name__ == "__main__":
    unittest.main()
