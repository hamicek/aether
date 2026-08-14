#!/usr/bin/env bash
# Edge perf/load harness (build tag `edgeperf`, out of normal CI). Measures reality and logs numbers -
# HTTP ingress + SSE-push throughput and latency, the serialized-backend ceiling, and the aether-layer
# cost vs a bare net/http baseline. See examples/edge-perf/REPORT.md for a measured snapshot.
#
# Usage: scripts/edge-perf.sh [scenario]
#   scenario: all (default) | ingress | ceiling | sse
set -euo pipefail

scenario="${1:-all}"
case "$scenario" in
  all)     run='TestEdgePerf|TestEdgeSSEPerf|TestEdgeBackendCeiling' ;;
  ingress) run='TestEdgePerf$' ;;
  ceiling) run='TestEdgeBackendCeiling' ;;
  sse)     run='TestEdgeSSEPerf' ;;
  *) echo "unknown scenario: $scenario (use: all | ingress | ceiling | sse)" >&2; exit 2 ;;
esac

# Go may not be on PATH on this machine; fall back to mise.
if command -v go >/dev/null 2>&1; then
  GO=(go)
else
  GO=(mise exec go@latest -- go)
fi
export GOTOOLCHAIN=local

cd "$(dirname "$0")/.."
exec "${GO[@]}" test -tags edgeperf -run "$run" -timeout 10m -v ./examples/edge-perf/
