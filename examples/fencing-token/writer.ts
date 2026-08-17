// Write-side fencing token demo in TypeScript - functionally identical to main.go and writer.py.
//
// A singleton thrall stamps its fencing epoch (ctx.singletonEpoch) on every write to a resource,
// and the resource rejects a write carrying a lower epoch than it has seen. Singleton fencing
// alone only bounds liveness overlap (DESIGN 14); this is strict single-writer against a resource.
// The "resource" is a NATS KV bucket holding {value, epoch}; a real one would be a PLC/driver/DB.
//
//	aether call writer write       '{"value": "A"}'   # accepted, stored with the live epoch
//	aether call writer write-stale  '{"value": "B"}'  # a simulated old instance -> fenced
//	aether call writer read                            # -> {"value":"A","epoch":N}
import { defThrall, start, type Ctx } from "@hamicek/aether";
// Type-only import (erased at runtime, so the example needs no `nats` dependency of its own).
import type { KV } from "nats";

const resourceBucket = "resource";
const resourceKey = "reading";

type Stored = { value: string; epoch: number };

const enc = (s: Stored): Uint8Array => new TextEncoder().encode(JSON.stringify(s));
const dec = (b: Uint8Array): Stored => JSON.parse(new TextDecoder().decode(b)) as Stored;

// writeFenced is the resource-side guard: accept only if epoch >= the epoch already stored
// (monotonic), so a stale writer (a lower epoch) is rejected. Returns whether it stored.
async function writeFenced(kv: KV, value: string, epoch: number): Promise<boolean> {
  const cur = await kv.get(resourceKey);
  if (cur) {
    const prev = dec(cur.value);
    if (epoch < prev.epoch) return false; // fenced: a newer epoch has already written here
  }
  await kv.put(resourceKey, enc({ value, epoch }));
  return true;
}

let kv: KV;

const writer = defThrall<Record<string, never>>({
  name: "writer",

  init: async (ctx) => {
    // The resource stub: a KV bucket standing in for an external resource.
    kv = await ctx.nats.jetstream().views.kv(resourceBucket);
    ctx.log.info("writer ready", { singleton_epoch: ctx.singletonEpoch });
    return {};
  },

  handleCall: {
    // write stamps the live epoch (the honest, correct write).
    write: async (payload, state, ctx: Ctx) => {
      const value = (payload as { value: string }).value;
      const stored = await writeFenced(kv, value, ctx.singletonEpoch);
      return [{ stored, epoch: ctx.singletonEpoch }, state];
    },
    // write-stale simulates a fenced-out old instance writing with an older epoch; rejected.
    "write-stale": async (payload, state, ctx: Ctx) => {
      const value = (payload as { value: string }).value;
      const stale = ctx.singletonEpoch - 1;
      const stored = await writeFenced(kv, value, stale);
      return [{ stored, epoch: stale }, state];
    },
    read: async (_payload, state) => {
      const cur = await kv.get(resourceKey);
      return [cur ? dec(cur.value) : { value: null }, state];
    },
  },
});

await start(writer);
