# SCADA spike on aether - report (AE-014)

**Question:** is aether viable as an Elixir replacement for a SCADA core, if Elixir
is rejected? This spike built the thinnest working vertical (1 TS driver → 1
coarse-grained Go site thrall holding the whole site's process image in memory → 1
threshold alarm → snapshot over `call`) and measured its runtime viability.

## Verdict: **viable, with caveats**

On the runtime dimension the answer is a clear yes: aether carries the SCADA telemetry
profile with a very large margin, and the coarse-grained mapping (one thrall = one
site's image as a Go map, NOT one thrall per tag) is ergonomic to write. The caveats
are about what the spike deliberately did **not** cover, not about what it found.

## Measured numbers

Bench harness: embedded ether, site thrall in-process (the real `siteDef` code), Go
load generator (so Bun is not the bottleneck). Reproduce:

```
mise exec go@latest -- go test -tags scadaspike -run TestSpike -v -timeout 5m ./examples/scada-spike/
```

| Metric | Measured | Target (SCADA design) | Margin |
|---|---|---|---|
| Image throughput, no loss | **≥ 50 000 values/s** (ceiling not reached in sweep) | 1 000 /s | ~50× over target; ~5000× over a real site (~10 values today) |
| Snapshot call latency | p50 **0.33 ms**, p99 **1.7 ms**, max 4.1 ms | p99 < 50 ms | ~30× |
| Alarm latency (breach → event) | p50 **0.30 ms**, p99 **0.98 ms**, max 1.0 ms | < 250 ms | ~250× |

Throughput swept 1k → 50k values/s with **zero sequence gaps** at every step; the
ceiling is above 50k/s and was not found. Processing latency does rise under pressure
(avg 215 µs at 1k/s → 1.4 ms at 50k/s; max 1 ms → 10.8 ms) - the mailbox fills and
drains but keeps up. For the real profile (tens of values/s per site) this is all deep
in the noise.

## Ergonomics (judgment, from writing the spike)

- **The coarse-grained model fits naturally.** A site thrall holding the image as a Go
  `map[string]float64` and mutating it in `HandleCast` reads cleanly (`site.go`). This
  is the right shape for aether - a few rich thralls, not a thrall per tag.
- **The serialized mailbox removes a whole class of bugs.** Handlers never run
  concurrently, so the image needs no locks. The SCADA design's "serialized processing
  of messages addressed to me" maps 1:1 onto the thrall.
- **Direct NATS access pays off.** The alarm is published straight through
  `ctx.NATS.Publish` - no facade to fight. Same for reaching KV/JetStream later.
- **Polyglot is frictionless.** TS driver + Go site under one `aether up`, one JSON
  contract, no glue.
- **The rough edge is per-message JSON.** Every telemetry sample is a JSON envelope
  marshalled/unmarshalled end to end; that is what grows processing latency at extreme
  rates. Irrelevant at the real profile, but it is the first thing that would bite a
  genuinely high-rate site - batching would be the answer there.
- Minor: the bench harness had to hand-build wire envelopes because it is not a thrall
  (no SDK client helpers). Applications never hit this - real thralls use the SDK.

## What the spike did NOT cover (the caveats)

- **State survival across restart.** Not measured. The right answer is event-sourced
  rebuild (replay the image from a JetStream stream in `init`), consistent with the
  SCADA design's "log is truth" - NOT the in-memory snapshot that cancelled AE-012.
  Needs its own task before this is production-shaped.
- **Dynamic thralls.** Drivers-per-connection and Petri-net instances arrive at
  runtime; aether only spawns from the manifest today. **AE-013 (dynamic supervisor)
  is still required**, regardless of this spike.
- **Scale of many sites/thralls per cell**, memory footprint of a large image, and
  behaviour over real OS processes at full rate (the in-process harness isolates the
  runtime, not the deployment).
- **Application core** - Petri-net engine, rules/AST evaluator, scheduler, BFF - is
  work *on top of* aether, untouched here.

## Recommendation

1. **Aether is a real fallback.** If Elixir is rejected, the runtime holds; the
   application core becomes Go work on top of aether, not a rewrite of aether.
2. **Build AE-013 (dynamic supervisor)** - needed for drivers-per-connection and
   instances in either branch.
3. **Open an event-sourced rebuild task** when image durability across restart is
   needed - the correct successor to the cancelled AE-012.
4. If a genuinely high-rate site appears, revisit **telemetry batching** to push the
   per-message JSON ceiling further.
