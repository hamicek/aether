import { defThrall, start, call, cast } from "@hamicek/aether";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Waits until the target thrall is available (answers `get`). Core NATS is
// ephemeral - casts sent before the thrall subscribes would be lost.
async function ready(name: string): Promise<void> {
  for (let i = 0; i < 100; i++) {
    try {
      await call(name, "get", {}, { timeoutMs: 500 });
      return;
    } catch {
      await sleep(50);
    }
  }
  throw new Error(`${name} did not come up in time`);
}

// The gateway probes the thralls from AETHER_TARGETS (or the default) and prints them side by side.
// To the gateway they are indistinguishable: the same call/cast, the same contract.
const TARGETS = (process.env.AETHER_TARGETS ?? "counter,counter-py,counter-go")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const gateway = defThrall<null>({
  name: "gateway",

  init: async () => {
    for (const t of TARGETS) await ready(t);

    let tick = 0;
    void (async () => {
      for (;;) {
        tick++;
        const cols: string[] = [];
        for (const name of TARGETS) {
          try {
            cast(name, "inc");
            const v = await call<number>(name, "get", {}, { timeoutMs: 800 });
            cols.push(`${name}=${v}`);
          } catch {
            cols.push(`${name}=unavailable`);
          }
        }
        console.log(`probe #${tick}: ${cols.join("  ")}`);
        await sleep(700);
      }
    })();

    return null;
  },
});

await start(gateway);
