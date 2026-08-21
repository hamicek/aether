// Package durableperf is a build-tagged (durableperf) throughput harness for the durable cast
// path. It measures the one number the durable consumer is about: how fast a single durable
// thrall drains a JetStream backlog of casts, in casts/s, on this machine.
//
// The scenario is deliberately drain-bound: N casts are preloaded into the stream before the
// thrall attaches, so the timed window is pure consumer drain (fetch + serialized handler +
// ack), not publish. That is the path AE-065 tuned (batched Fetch + AckWait/MaxAckPending),
// so the harness is the before/after evidence for the batching change - run it on the tuned
// build, then again with durableBatchSize=1 (the pre-AE-065 shape) to see the ceiling move.
//
// Run it with scripts/durable-perf.sh (or `go test -tags durableperf -run TestDurableThroughput
// ./examples/durable-perf/`). Backlog size is AETHER_PERF_CASTS (default 20000). See REPORT.md
// for a measured snapshot.
package durableperf
