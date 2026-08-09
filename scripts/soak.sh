#!/usr/bin/env bash
#
# Run the aether soak/chaos suite. This is deliberately out of CI: it drives the
# runtime under load and checks stability and resilience against concrete bars
# (call p99, durable no-loss, resource growth, chaos recovery, singleton failover,
# drain no-loss). Run it explicitly.
#
# Usage:
#   scripts/soak.sh [profile] [seed] [scenario]
#
#   profile    smoke (~2m, default) | default (~45m) | overnight (~8h)
#   seed       optional PRNG seed for a reproducible run
#   scenario   all (default) | load | chaos | drain | singleton
#
# Each timed scenario runs for the profile duration, so `all` at a long profile is
# long: pick a single scenario for default/overnight runs.
#
# Env overrides (read by the test itself):
#   AETHER_SOAK_DURATION   override the profile run length, e.g. 30s
#   AETHER_SOAK_REPORT     also write the report to this path
#
set -euo pipefail

profile="${1:-smoke}"
seed="${2:-}"
scenario="${3:-all}"

case "$profile" in
  smoke)     timeout="20m" ;;
  default)   timeout="4h" ;;
  overnight) timeout="30h" ;;
  *)
    echo "unknown profile: $profile (want smoke|default|overnight)" >&2
    exit 2
    ;;
esac

case "$scenario" in
  all)       run="TestSoak" ;;
  load)      run="TestSoak$" ;;
  chaos)     run="TestSoakChaos" ;;
  drain)     run="TestSoakDrain" ;;
  singleton) run="TestSoakSingletonFailover" ;;
  *)
    echo "unknown scenario: $scenario (want all|load|chaos|drain|singleton)" >&2
    exit 2
    ;;
esac

# Go is not always on PATH on dev machines; fall back to mise when it is missing.
if command -v go >/dev/null 2>&1; then
  go_cmd=(go)
else
  go_cmd=(mise exec go@latest -- go)
fi

test_args=(-soak.profile "$profile")
[ -n "$seed" ] && test_args+=(-soak.seed "$seed")
[ -n "${AETHER_SOAK_REPORT:-}" ] && test_args+=(-soak.report "$AETHER_SOAK_REPORT")

cd "$(dirname "$0")/.."
export GOTOOLCHAIN=local

set -x
"${go_cmd[@]}" test -tags soak -run "$run" -timeout "$timeout" -v ./internal/lord/ -args "${test_args[@]}"
