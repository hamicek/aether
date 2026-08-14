import { test, expect } from "bun:test";
import { startEdge, defEdge, type EdgeDef } from "./edge";

// The lifecycle of startEdge (init -> run -> drain -> stop) is proven by a real run in the TS
// live-dashboard / webserver-custom examples; here we cover the validation guard that fires before any
// dial, the way the FSM/event behaviours are unit-tested.

test("startEdge rejects a missing run", async () => {
  process.env.AETHER_NATS_URL = "nats://127.0.0.1:1"; // never dialled: the guard fires first
  process.env.AETHER_APP = "test";
  const def = { name: "gw" } as unknown as EdgeDef;
  await expect(startEdge(def)).rejects.toThrow("run is required");
});

test("defEdge is an identity helper", () => {
  const run = async () => {};
  const def = defEdge({ name: "gw", run });
  expect(def.run).toBe(run);
  expect(def.name).toBe("gw");
});
