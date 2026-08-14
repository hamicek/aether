// Package edgeperf is a build-tagged (edgeperf) performance/load harness for the edge category. It
// measures reality - HTTP ingress and SSE-push throughput and latency, the serialized-backend ceiling,
// and the cost of the aether layer against a bare net/http baseline - and is excluded from normal CI.
//
// It is NOT a contest with a web framework: an aether edge is an HTTP->ether->genserver bridge, so it
// will always be slower than a bare handler because it does more (a NATS round-trip + a serialized
// mailbox + envelope). The numbers here are the absolute figures, the layer cost, and where the ceiling
// sits, on this machine.
//
// Run it with scripts/edge-perf.sh (or `go test -tags edgeperf -run TestEdgePerf ./examples/edge-perf/`).
// See REPORT.md for a measured snapshot.
package edgeperf
