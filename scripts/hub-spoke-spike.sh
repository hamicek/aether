#!/usr/bin/env bash
#
# Hub-spoke multi-node spike (AE-051). Builds and runs a hub + 2 sites, each
# as a standalone node (its own NATS + its own lord), the sites connected to the
# hub as NATS leaf nodes with account isolation, and verifies three assertions:
#
#   1. DISTRIBUTION - the gateway on the hub reads the real state of both sites cross-node.
#   2. ISOLATION(-) - site A cannot reach site B's thrall (no responders).
#   3. ISOLATION(+) - a probe on site A sees no traffic from site B,
#                     while a control probe on site B does see it.
#
# Deliberately out of CI: it runs 3 NATS servers + 3 lords and asserts a live
# multi-node topology. Exit 0 = all assertions passed.
#
# Usage: scripts/hub-spoke-spike.sh
#
set -euo pipefail

# Go is not always on PATH on dev machines; fall back to mise. nats-server is required.
if command -v go >/dev/null 2>&1; then
  go_cmd=(go)
else
  go_cmd=(mise exec go@latest -- go)
fi
if ! command -v nats-server >/dev/null 2>&1; then
  echo "nats-server not on PATH - install it (https://docs.nats.io/running-a-nats-service/introduction/installation)" >&2
  exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
sp="$root/examples/hub-spoke-spike"
run="$sp/.run"
export GOTOOLCHAIN=local

rm -rf "$run"
mkdir -p "$run" "$sp/bin"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  # give the lords time to gracefully drain the thralls (drain ceiling ~5s), then finish off the rest
  sleep 6
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# --- build -------------------------------------------------------------------
echo "==> build (aether, counter, gateway, probe)"
"${go_cmd[@]}" build -o "$sp/bin/aether"  "$root/cmd/aether"
"${go_cmd[@]}" build -o "$sp/bin/counter" "$root/examples/counter"
"${go_cmd[@]}" build -o "$sp/bin/gateway" "$sp"
"${go_cmd[@]}" build -o "$sp/bin/probe"   "$sp/cmd/probe"
aether="$sp/bin/aether"

# --- helpers -----------------------------------------------------------------
# wait_for waits for a pattern to appear in the log. The patterns below are the
# wording of the nats-server log (pinned dev binary); recheck them
# on a nats-server upgrade.
wait_for() { # <file> <pattern> <timeout_s>
  local file="$1" pat="$2" limit="${3:-10}" waited=0
  while ! grep -q "$pat" "$file" 2>/dev/null; do
    sleep 0.2
    waited=$((waited + 1))
    if [ "$waited" -ge $((limit * 5)) ]; then
      echo "timeout waiting for '$pat' in $file" >&2
      return 1
    fi
  done
}

failures=0
check() { # <description> <expected> <got>
  if [ "$2" = "$3" ]; then
    echo "  PASS  $1 (=$3)"
  else
    echo "  FAIL  $1: expected '$2', got '$3'"
    failures=$((failures + 1))
  fi
}

hub="nats://127.0.0.1:7390"
spa="nats://127.0.0.1:7392"
spb="nats://127.0.0.1:7393"

# --- start NATS nodes --------------------------------------------------------
echo "==> start NATS nodes (hub + 2 leaf)"
nats-server -c "$sp/nats/hub.conf"     -sd "$run/hub" -l "$run/hub.log" >/dev/null 2>&1 & pids+=($!); disown
wait_for "$run/hub.log" "Listening for leafnode" 10
nats-server -c "$sp/nats/spoke-a.conf" -sd "$run/sa"  -l "$run/sa.log"  >/dev/null 2>&1 & pids+=($!); disown
nats-server -c "$sp/nats/spoke-b.conf" -sd "$run/sb"  -l "$run/sb.log"  >/dev/null 2>&1 & pids+=($!); disown
# the hub log confirms both leaf connections via their remote JetStream domains (sa/sb)
wait_for "$run/hub.log" 'remote "sa"' 10
wait_for "$run/hub.log" 'remote "sb"' 10
echo "  leaf connections established"

# --- start lords -------------------------------------------------------------
# Runs from the sp dir so the relative ./bin/... paths in the manifests match. IMPORTANT:
# aether up is started DIRECTLY in the background (not in a subshell), so $! is the lord's PID -
# otherwise the cleanup kill would hit only the subshell and the lord (incl. its thralls) would
# survive, reconnect on the next run and accumulate state.
echo "==> start lords (hub, spoke-a, spoke-b)"
cd "$sp"
"$aether" up -f aether-hub.toml     >"$run/up-hub.log" 2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-a.toml >"$run/up-sa.log"  2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-b.toml >"$run/up-sb.log"  2>&1 & pids+=($!); disown
wait_for "$run/up-hub.log" "thrall ready" 15
wait_for "$run/up-sa.log"  "thrall ready" 15
wait_for "$run/up-sb.log"  "thrall ready" 15
echo "  all thralls on the bus"

# --- seed (locally on the sites; the --url flag must be BEFORE positional arguments) --
echo "==> seed: counterA += 3 (site A), counterB += 5 (site B)"
for _ in 1 2 3;     do "$aether" cast --url "$spa" --app demo counterA inc >/dev/null; done
for _ in 1 2 3 4 5; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null; done

# Casts are async; wait locally for the seed to take effect before the assertions start
# (otherwise a cross-node read could catch state that has not been processed yet).
poll_value() { # <url> <name> <expected> [timeout_s]
  local url="$1" name="$2" want="$3" limit="${4:-5}" waited=0 got=""
  while :; do
    got="$("$aether" call --url "$url" --app demo --timeout 2s "$name" get 2>/dev/null || true)"
    [ "$got" = "$want" ] && return 0
    sleep 0.2
    waited=$((waited + 1))
    [ "$waited" -ge $((limit * 5)) ] && return 1
  done
}
poll_value "$spa" counterA 3 || echo "  warning: counterA did not settle at 3"
poll_value "$spb" counterB 5 || echo "  warning: counterB did not settle at 5"

# --- 1) DISTRIBUTION ---------------------------------------------------------
echo "==> 1) DISTRIBUTION: the center reads both sites cross-node"
# gateway.check does two sequential cross-node calls (2s each) -> outer timeout 5s
check "gateway.check sees both sites" \
  '{"counterA":3,"counterB":5}' \
  "$("$aether" call --url "$hub" --app demo --timeout 5s gateway check)"
check "direct call hub -> counterA" 3 "$("$aether" call --url "$hub" --app demo --timeout 3s counterA get)"
check "direct call hub -> counterB" 5 "$("$aether" call --url "$hub" --app demo --timeout 3s counterB get)"

# --- 2) ISOLATION negative ---------------------------------------------------
# We distinguish three outcomes: success = isolation broken; "no responders" = correct
# isolation; any other error (timeout/connection) = inconclusive -> FAIL, so a masked
# error does not lead us to a false PASS.
echo "==> 2) ISOLATION(-): site A cannot reach counterB"
if iso_out="$("$aether" call --url "$spa" --app demo --timeout 2s counterB get 2>&1)"; then
  echo "  FAIL  site A REACHED counterB (isolation broken): $iso_out"
  failures=$((failures + 1))
elif printf '%s' "$iso_out" | grep -qi "no responders"; then
  echo "  PASS  site A -> counterB rejected (no responders)"
else
  echo "  FAIL  inconclusive: expected 'no responders', got: $iso_out"
  failures=$((failures + 1))
fi

# --- 3) ISOLATION positive (probe) -------------------------------------------
echo "==> 3) ISOLATION(+): a probe on site A sees no traffic from site B"
"$sp/bin/probe" --url "$spa" --subject "aether.demo.counterB.>" --secs 2 >"$run/probe-a.out" 2>&1 & pa=$!
"$sp/bin/probe" --url "$spb" --subject "aether.demo.counterB.>" --secs 2 >"$run/probe-b.out" 2>&1 & pb=$!
sleep 0.4
for _ in 1 2 3 4 5 6; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null; done
wait "$pa" "$pb"
check "probe on site A (foreign traffic)" "received=0" "$(cat "$run/probe-a.out")"
if [ "$(cat "$run/probe-b.out")" = "received=0" ]; then
  echo "  FAIL  control probe on site B saw nothing (the test is broken)"
  failures=$((failures + 1))
else
  echo "  PASS  control probe on site B saw traffic ($(cat "$run/probe-b.out"))"
fi

# --- verdict -----------------------------------------------------------------
echo
if [ "$failures" -eq 0 ]; then
  echo "ALL ASSERTIONS PASSED - hub-spoke topology works, the sites are isolated."
  exit 0
fi
echo "$failures assertions FAILED - see $run/*.log"
exit 1
