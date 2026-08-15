import { test, expect } from "bun:test";
import { decode, ValidationError } from "@hamicek/aether";
import measurementSchema from "./schemas/measurement.schema.json";

// Exercises the payload contract through the shared schema *file* (not an inline schema):
// the same measurement.schema.json the driver produces against and the BFF validates with.

test("the boundary accepts a valid measurement", () => {
  const m = decode<{ metric: string; value: number }>(measurementSchema, {
    siteId: "s-1",
    metric: "voltage",
    value: 231.4,
    unit: "V",
    ts: 1700000000000,
  });
  expect(m.metric).toBe("voltage");
  expect(m.value).toBe(231.4);
});

test("the boundary rejects a measurement whose metric is out of the schema enum", () => {
  expect(() =>
    decode(measurementSchema, { siteId: "s-3", metric: "pressure", value: 9.9, ts: 1700000000000 }),
  ).toThrow(ValidationError);
});

test("the boundary rejects a measurement missing a required field", () => {
  expect(() => decode(measurementSchema, { siteId: "s-1", metric: "voltage", value: 1 })).toThrow(
    ValidationError,
  );
});
