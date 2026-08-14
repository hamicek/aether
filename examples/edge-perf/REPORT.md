# Edge perf report

A snapshot of the edge category's performance, measured by the `edgeperf` harness (`bench_test.go`).
**These numbers are indicative and machine-dependent** (measured on macOS / Apple Silicon, embedded NATS
over loopback, everything in one process). The point is the *shape*: absolute figures, the cost of the
aether layer, and where the ceiling sits - **not** a contest with a web framework.

## What this is (and is not)

An aether edge is an **HTTP→ether→genserver bridge**. It will always be slower than a bare `net/http`
handler because it does more: a NATS round-trip, a serialized mailbox, a JSON envelope, per-request
translation. The harness measures the **edge data path** (router match → envelope → NATS request/reply →
status) against a bare-handler baseline. It does **not** measure process spawn, fencing or heartbeat
overhead (one-off, off the hot path), nor cross-machine network latency (loopback here).

## HTTP ingress (16 clients, closed-loop)

| Scenario | req/s | p50 | p99 | Note |
|---|---|---|---|---|
| **baseline** (bare net/http) | ~113 000 | ~125 µs | ~445 µs | no ether at all |
| **ingress call** (waits for reply) | ~50 000 | ~298 µs | ~815 µs | full round-trip to the thrall |
| **ingress cast** (fire-and-forget) | ~64 000 | ~228 µs | ~655 µs | no reply wait |

**Cost of the aether layer:** an ingress `call` sustains ~44% of the baseline throughput and adds
~+170 µs to p50 latency. That delta *is* the layer: NATS round-trip + serialized mailbox + envelope. A
`cast` is cheaper (no reply wait) but still pays the publish + envelope cost.

## Serialized-backend ceiling (concurrency sweep vs ONE thrall)

The mailbox is serialized, so a single thrall's ceiling is `1 / handler-time`. The sweep shows both faces:

**Trivial handler (`get`, near-zero work):** the mailbox is *not* the bottleneck - throughput scales with
concurrency (≈10k → 110k req/s from 1 → 128 clients), only latency grows with the queue.

**~500 µs handler (`slow`, simulating real work):** the mailbox *is* the ceiling.

| clients | req/s | p50 | p99 |
|---|---|---|---|
| 1 | ~1 100 | ~0.86 ms | ~1.7 ms |
| 2 | ~1 720 | ~1.1 ms | ~1.7 ms |
| 8 | ~1 740 | ~4.6 ms | ~5.3 ms |
| 32 | ~1 750 | ~18 ms | ~21 ms |
| 128 | ~1 800 | ~73 ms | ~76 ms |

Throughput **plateaus near 1/handler-time (~1750/s ≈ 1/500µs)** regardless of concurrency; adding clients
does not add throughput, it only **grows latency linearly** as the queue. This is the hard number behind
the design's verbal rule "a slow serialized backend is the throughput ceiling": to scale past it you make
the handler faster or split the load across more thralls - edge concurrency does not remove the limit.

## SSE push fan-out (2000 events/s published, per-client bounded buffer 16)

| clients | delivered/s (fan-out) | p50 | p99 | drops |
|---|---|---|---|---|
| 1 | ~2 000 | ~100 µs | ~590 µs | 0 |
| 10 | ~19 900 | ~130 µs | ~570 µs | 0 |
| 50 | ~100 000 | ~180 µs | ~340 µs | 0 |
| 200 | ~390 000 | ~1-3 ms | ~10 ms | ~1 100 (≈0.1%) |

Fan-out scales cleanly to ~100k deliveries/s at 50 clients with no drops and sub-ms latency. At 200 clients
the edge pushes ~390k deliveries/s but the bounded per-client buffer starts shedding (~0.1% drop) and
latency climbs - the deliberate backpressure trade-off (a live view drops a stale event rather than
stalling). To go further: fewer/leaner events, or scale edges behind a reverse proxy.

## Verdict

For **desktop-class hardware over loopback**: HTTP ingress ~50k call/s (~64k cast/s) at sub-ms p50, and SSE
fan-out ~100k deliveries/s across 50 clients with no drops. The aether layer costs ~2.3× the throughput and
~+170 µs latency vs a bare handler - the price of process isolation, supervision and a polyglot wire. The
real limit is the **serialized backend**: a slow handler caps a single thrall at `1/handler-time`, which is
a design property, not a bug. More than enough for aether's target band (dozens of processes, real work per
message); reach for a reverse proxy / more thralls only past those numbers.

## Run it

```bash
scripts/edge-perf.sh
# or: go test -tags edgeperf -run 'TestEdgePerf|TestEdgeSSEPerf|TestEdgeBackendCeiling' -v -timeout 5m ./examples/edge-perf/
```

Numbers vary run to run and machine to machine; re-run and read the *shape*, not the last digit.
