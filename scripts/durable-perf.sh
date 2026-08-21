#!/usr/bin/env bash
# Durable cast throughput harness (build tag `durableperf`, out of normal CI). Preloads a
# JetStream backlog of casts and measures how fast one durable thrall drains it, in casts/s.
# It is the before/after evidence for AE-065's batched fetch: run it on the tuned build, then
# again with sdk/go/thrall durableBatchSize=1 to see the pre-AE-065 ceiling. See
# examples/durable-perf/REPORT.md for a measured snapshot.
#
# Usage: scripts/durable-perf.sh [casts]
#   casts: backlog size (default 20000)
set -euo pipefail

casts="${1:-20000}"

# Go may not be on PATH on this machine; fall back to mise.
if command -v go >/dev/null 2>&1; then
  GO=(go)
else
  GO=(mise exec go@latest -- go)
fi
export GOTOOLCHAIN=local
export AETHER_PERF_CASTS="$casts"

cd "$(dirname "$0")/.."
exec "${GO[@]}" test -tags durableperf -run TestDurableThroughput -timeout 10m -v ./examples/durable-perf/
