// Opt-in payload-contract helper for the TS SDK: validate a payload against a JSON Schema
// at an application boundary, and decode it into a typed value in one step.
//
// It is deliberately outside the transport: the runtime stays untyped, the payload stays
// `unknown` on the wire, and nothing here is wired into call/cast. The application calls
// validate/decode explicitly, at the boundary it owns. See PAYLOAD-CONTRACT.md (AE-042).

import Ajv2020, { type ValidateFunction, type ErrorObject } from "ajv/dist/2020";
import addFormats from "ajv-formats";

const ajv = new Ajv2020({ allErrors: true, strict: false });
addFormats(ajv);

// Compiled validators are cached by schema identity, so revalidating with the same schema
// object (e.g. an imported schemas/*.json module) does not recompile per message.
const cache = new WeakMap<object, ValidateFunction>();

function compiled(schema: object): ValidateFunction {
  let fn = cache.get(schema);
  if (!fn) {
    fn = ajv.compile(schema);
    cache.set(schema, fn);
  }
  return fn;
}

/** One schema violation: the JSON Pointer path to the offending value and a message. */
export interface Problem {
  path: string;
  message: string;
}

/** Thrown by validate/decode when a payload does not match the schema. */
export class ValidationError extends Error {
  readonly problems: Problem[];
  constructor(problems: Problem[]) {
    const detail = problems
      .map((p) => `${p.path || "(root)"}: ${p.message}`)
      .join("; ");
    super(`payload does not match schema: ${detail}`);
    this.name = "ValidationError";
    this.problems = problems;
  }
}

/** Validate payload against schema; throws ValidationError listing each offending path. */
export function validate(schema: object, payload: unknown): void {
  const fn = compiled(schema);
  if (!fn(payload)) {
    throw new ValidationError((fn.errors ?? []).map(toProblem));
  }
}

/**
 * Validate payload against schema and return it typed as T. The ergonomic boundary call:
 * one step yields a trusted, typed value (the cast is sound because validation just passed).
 */
export function decode<T>(schema: object, payload: unknown): T {
  validate(schema, payload);
  return payload as T;
}

function toProblem(e: ErrorObject): Problem {
  return { path: e.instancePath, message: e.message ?? e.keyword };
}
