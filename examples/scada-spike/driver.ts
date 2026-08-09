import { defThrall, start, cast } from "@hamicek/aether";

// SCADA spike - driver (AE-014). A stateless polyglot driver: it generates telemetry
// and casts it to the `site` thrall. Real drivers translate a PLC protocol; here we
// synthesize values so the vertical runs end-to-end via `aether up`. Throwaway code.
//
// This driver demonstrates ergonomics and "runs through aether up"; the precise
// throughput/latency numbers come from the Go bench harness (step 3), where load and
// site share one clock and Bun's send rate is not the bottleneck.

const TAGS = Number(process.env.SCADA_TAGS ?? "10"); // one site is ~10 values today
const HZ = Number(process.env.SCADA_HZ ?? "100"); // total samples per second
const THRESHOLD = Number(process.env.SCADA_THRESHOLD ?? "100");
const TARGET = process.env.SCADA_TARGET ?? "site";

const driver = defThrall<null>({
  name: process.env.AETHER_NAME ?? "driver",

  init: () => {
    const seq = new Array<number>(TAGS).fill(0);
    const intervalMs = Math.max(1, Math.floor(1000 / HZ));
    let i = 0;

    setInterval(() => {
      const tag = `tag${i % TAGS}`;
      const k = i % TAGS;
      seq[k] += 1;
      // Mostly below threshold; every ~50th sample spikes above it to exercise alarms.
      const value = i % 50 === 0 ? THRESHOLD + 25 : 40 + (i % 20);
      cast(TARGET, "tele", {
        tag,
        value,
        seq: seq[k],
        ts_ns: Date.now() * 1_000_000, // ms precision is enough for the demo path
      });
      i += 1;
    }, intervalMs);

    return null;
  },
});

await start(driver);
