import { test, expect } from "bun:test";
import { validate, decode, ValidationError } from "./schema";

const measurementSchema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  type: "object",
  additionalProperties: false,
  required: ["siteId", "metric", "value", "ts"],
  properties: {
    siteId: { type: "string", minLength: 1 },
    metric: { type: "string", enum: ["voltage", "current", "temperature"] },
    value: { type: "number" },
    unit: { type: "string" },
    ts: { type: "integer" },
  },
} as const;

interface Measurement {
  siteId: string;
  metric: "voltage" | "current" | "temperature";
  value: number;
  unit?: string;
  ts: number;
}

test("validate accepts a valid payload", () => {
  expect(() =>
    validate(measurementSchema, { siteId: "s-1", metric: "voltage", value: 231.4, ts: 1700000000000 }),
  ).not.toThrow();
});

test("validate reports the offending field path", () => {
  const cases: Array<[string, unknown, string]> = [
    ["wrong type", { siteId: "s-1", metric: "voltage", value: "hot", ts: 1 }, "/value"],
    ["out of enum", { siteId: "s-1", metric: "pressure", value: 1, ts: 1 }, "/metric"],
  ];
  for (const [name, payload, wantPath] of cases) {
    try {
      validate(measurementSchema, payload);
      throw new Error(`${name}: invalid payload accepted`);
    } catch (e) {
      expect(e).toBeInstanceOf(ValidationError);
      const ve = e as ValidationError;
      expect(ve.problems.some((p) => p.path.includes(wantPath))).toBe(true);
    }
  }
});

test("validate rejects a payload missing a required field", () => {
  expect(() => validate(measurementSchema, { siteId: "s-1", metric: "voltage", value: 1 })).toThrow(
    ValidationError,
  );
});

test("decode returns a typed value", () => {
  const m = decode<Measurement>(measurementSchema, {
    siteId: "s-1",
    metric: "current",
    value: 12.5,
    unit: "A",
    ts: 1700000000000,
  });
  expect(m.siteId).toBe("s-1");
  expect(m.metric).toBe("current");
  expect(m.value).toBe(12.5);
});

test("decode rejects an invalid payload before returning", () => {
  expect(() => decode<Measurement>(measurementSchema, { siteId: "", metric: "voltage", value: 1, ts: 1 })).toThrow(
    ValidationError,
  );
});

test("two distinct schema objects sharing an $id do not collide", () => {
  // ajv keys schemas by $id; without addUsedSchema:false the second compile would throw
  // "schema with key or id already exists".
  const mk = () => ({ $id: "dup.json", type: "object", required: ["a"], properties: { a: { type: "number" } } });
  expect(() => validate(mk(), { a: 1 })).not.toThrow();
  expect(() => validate(mk(), { a: 2 })).not.toThrow();
});
