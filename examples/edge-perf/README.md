# edge-perf

A build-tagged (`edgeperf`) performance/load harness for the edge category. It **measures reality and
logs numbers** (it does not gate CI), patterned on `examples/scada-spike`. It runs an embedded NATS with
an in-process backend thrall and drives load through the real edge data path via `httptest`.

## Run

```bash
scripts/edge-perf.sh            # all scenarios
scripts/edge-perf.sh ingress    # HTTP ingress call/cast + baseline
scripts/edge-perf.sh ceiling    # serialized-backend ceiling sweep
scripts/edge-perf.sh sse        # SSE push fan-out
```

Or directly: `go test -tags edgeperf -run TestEdgePerf -v ./examples/edge-perf/` (`GOTOOLCHAIN=local`,
or via `mise exec go@latest -- go`).

## What it measures

- **HTTP ingress** (`call`/`cast`): req/s and p50/p99, vs a bare `net/http` **baseline** - the delta is
  the aether-layer cost.
- **Serialized-backend ceiling**: a concurrency sweep against one thrall, with a trivial handler (mailbox
  is not the bottleneck) and a ~500 µs handler (mailbox *is* the ceiling).
- **SSE push fan-out**: N clients on one stream, event→client throughput and latency plus drops.

Numbers are indicative and machine-dependent. See [REPORT.md](./REPORT.md) for a measured snapshot and its
interpretation. It is **not** a contest with a web framework - see the framing there.

The harness is excluded from normal builds/tests by the `edgeperf` build tag, so `go test ./...` never runs it.
