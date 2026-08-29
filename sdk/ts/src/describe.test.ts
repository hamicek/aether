import { test, expect } from "bun:test";
import { fsmDescribe, type FSMDef } from "./fsm";

// Mirrors the Go TestFSMDescribe: an FSM reports the union of its states' reaction ops - each
// dispatchable as a call or a cast - plus the reserved _state call op, all sorted, and the version.
test("fsmDescribe reports the union of reaction ops as call and cast, plus _state and version", () => {
  const def: FSMDef<number> = {
    name: "gate",
    initial: "idle",
    init: () => 0,
    version: "2.0.0",
    states: {
      idle: { on: { start: { fn: () => ({ data: 0 }) } } },
      running: { on: { stop: { fn: () => ({ data: 0 }) }, start: { fn: () => ({ data: 0 }) } } },
    },
  };
  const d = fsmDescribe(def);
  expect(d.call_ops).toEqual(["_state", "start", "stop"]);
  expect(d.cast_ops).toEqual(["start", "stop"]);
  expect(d.version).toBe("2.0.0");
});

// A machine with no reactions still exposes the reserved _state call op, and omits the empty cast set
// and the version - matching the Go omitempty wire form.
test("fsmDescribe on an empty machine has only _state and omits empties", () => {
  const def: FSMDef<number> = {
    name: "bare",
    initial: "s",
    init: () => 0,
    states: { s: { on: {} } },
  };
  const d = fsmDescribe(def);
  expect(d.call_ops).toEqual(["_state"]);
  expect(d.cast_ops).toBeUndefined();
  expect(d.version).toBeUndefined();
});
