# durable-perf

Throughput harness for the **durable cast** path: how fast a single durable thrall drains a
JetStream backlog of casts, in casts/s. Build-tagged (`durableperf`), out of normal CI.

It preloads N casts into the stream *before* the thrall attaches, so the timed window is pure
consumer drain (fetch + serialized handler + ack), not publish. That is the path AE-065 tuned
(batched `Fetch` + `AckWait`/`MaxAckPending`), so this harness is its before/after evidence.

## Run

```bash
scripts/durable-perf.sh            # backlog of 20000 casts
scripts/durable-perf.sh 200000     # larger backlog (recommended for a stable rate)

# or directly:
go test -tags durableperf -run TestDurableThroughput ./examples/durable-perf/
```

Backlog size is `AETHER_PERF_CASTS` (default 20000). Use a large N (e.g. 200000) for a
representative figure - the harness polls the drained count every 20 ms, so small backlogs
drain faster than they can be observed and the rate reads as a floor.

## Before/after

The harness measures the current (tuned) build. To see the pre-AE-065 shape, set
`durableBatchSize = 1` in `sdk/go/thrall/thrall.go`, run again, then revert. See
[REPORT.md](REPORT.md) for a measured snapshot.
