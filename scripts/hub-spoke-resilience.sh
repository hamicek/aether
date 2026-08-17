#!/usr/bin/env bash
#
# Hub-spoke resilience (AE-053). Extends the AE-051 topology (examples/hub-spoke-spike)
# with failure injection and asserts that the model survives failure, not just the
# happy path:
#
#   1. CENTER OUTAGE - a site keeps serving locally while the hub is down, and the
#      leaf link + cross-node reach recover on their own once the hub returns.
#   2. SITE LORD DEATH - when a site's lord is killed, its thralls self-exit (AE-031
#      lord-liveness fencing) while the center and the other site keep running.
#
# The cross-node fencing question is answered in RESILIENCE.md (fencing is node-local
# by construction; a hub-spoke site is single-node, so cross-node failover is out of
# the model) - it is not a runtime assertion here.
#
# Deliberately out of CI: it drives live multi-node failure. Exit 0 = all assertions
# passed. Usage: scripts/hub-spoke-resilience.sh
#
set -euo pipefail

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
run="$sp/.run-resilience"
export GOTOOLCHAIN=local

rm -rf "$run"
mkdir -p "$run" "$sp/bin"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  sleep 3
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
  done
  # defensive sweep: orphan thralls after a lord SIGKILL (scenario 2) are not in pids.
  # The lord starts them relatively (`sh -c ./bin/counter`, cwd = $sp), so the cmdline
  # does NOT contain the absolute $sp/bin/ - we match the relative binary names.
  pkill -9 -f 'bin/counter' 2>/dev/null || true
  pkill -9 -f 'bin/gateway' 2>/dev/null || true
}
trap cleanup EXIT

# --- build -------------------------------------------------------------------
echo "==> build (aether, counter, gateway)"
"${go_cmd[@]}" build -o "$sp/bin/aether"  "$root/cmd/aether"
"${go_cmd[@]}" build -o "$sp/bin/counter" "$root/examples/counter"
"${go_cmd[@]}" build -o "$sp/bin/gateway" "$sp"
aether="$sp/bin/aether"

# --- helpers -----------------------------------------------------------------
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

# aeq = aether call value (empty string on error; flags BEFORE positional arguments)
aeq() { # <url> <name>
  "$aether" call --url "$1" --app demo --timeout 2s "$2" get 2>/dev/null || true
}

# poll_reach waits until a call on <url>/<name> returns <want> (or times out) - for reconnect.
# Note: the timeout is `if ... then return`, NOT `[ ] && return` - the latter would crash
# under `set -e` on the very first iteration (when the condition is false, `&&` returns 1 -> exit).
poll_reach() { # <url> <name> <want> [timeout_s]
  local url="$1" name="$2" want="$3" limit="${4:-15}" waited=0
  while [ "$(aeq "$url" "$name")" != "$want" ]; do
    waited=$((waited + 1))
    if [ "$waited" -ge $((limit * 3)) ]; then
      return 1
    fi
    sleep 0.3
  done
}

# poll_gone waits until a call on <url>/<name> STARTS failing (the thrall self-exited).
poll_gone() { # <url> <name> [timeout_s]
  local url="$1" name="$2" limit="${3:-10}" waited=0
  while "$aether" call --url "$url" --app demo --timeout 2s "$name" get >/dev/null 2>&1; do
    waited=$((waited + 1))
    if [ "$waited" -ge "$limit" ]; then
      return 1
    fi
    sleep 1
  done
}

hub="nats://127.0.0.1:7390"
spa="nats://127.0.0.1:7392"
spb="nats://127.0.0.1:7393"

hub_starts=0
start_hub_nats() { # (re)start hub NATS with a FRESH log (so wait_for does not read an old run)
  hub_starts=$((hub_starts + 1))
  hub_log="$run/hub-$hub_starts.log"
  nats-server -c "$sp/nats/hub.conf" -sd "$run/hub" -l "$hub_log" >/dev/null 2>&1 &
  hub_nats=$!
  pids+=("$hub_nats")
  disown
}

# --- start topology ----------------------------------------------------------
echo "==> start NATS nodes (hub + 2 leaf)"
start_hub_nats
wait_for "$hub_log" "Listening for leafnode" 10
nats-server -c "$sp/nats/spoke-a.conf" -sd "$run/sa" -l "$run/sa.log" >/dev/null 2>&1 & pids+=($!); disown
nats-server -c "$sp/nats/spoke-b.conf" -sd "$run/sb" -l "$run/sb.log" >/dev/null 2>&1 & pids+=($!); disown
wait_for "$hub_log" 'remote "sa"' 10
wait_for "$hub_log" 'remote "sb"' 10
echo "  leaf connections established"

echo "==> start lords (hub, spoke-a, spoke-b)"
cd "$sp"
"$aether" up -f aether-hub.toml     >"$run/up-hub.log" 2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-a.toml >"$run/up-sa.log"  2>&1 & spa_lord=$!; pids+=("$spa_lord"); disown
"$aether" up -f aether-spoke-b.toml >"$run/up-sb.log"  2>&1 & pids+=($!); disown
wait_for "$run/up-hub.log" "thrall ready" 15
wait_for "$run/up-sa.log"  "thrall ready" 15
wait_for "$run/up-sb.log"  "thrall ready" 15
echo "  all thralls on the bus"

echo "==> seed: counterA += 3, counterB += 5"
for _ in 1 2 3;     do "$aether" cast --url "$spa" --app demo counterA inc >/dev/null || true; done
for _ in 1 2 3 4 5; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null || true; done
poll_reach "$spa" counterA 3 || echo "  warning: counterA did not settle"
poll_reach "$spb" counterB 5 || echo "  warning: counterB did not settle"

# --- 1) CENTER OUTAGE + RECOVERY --------------------------------------------
echo "==> 1) CENTER OUTAGE: the site survives and recovers cleanly"
check "baseline: center -> counterA" 3 "$(aeq "$hub" counterA)"

echo "  -- killing hub NATS --"
kill -9 "$hub_nats" 2>/dev/null || true
sleep 2

check "site A serves locally even when cut off" 3 "$(aeq "$spa" counterA)"
check "site B serves locally even when cut off" 5 "$(aeq "$spb" counterB)"

echo "  -- restarting hub NATS --"
start_hub_nats
wait_for "$hub_log" "Listening for leafnode" 10

SECONDS=0
if poll_reach "$hub" counterA 3 45; then
  echo "  PASS  after hub recovery leaf reconnect + center -> counterA (=3, recovery ~${SECONDS}s)"
else
  echo "  FAIL  center did not recover (counterA unreachable after hub restart): $(aeq "$hub" counterA)"
  failures=$((failures + 1))
fi

# --- 2) SITE LORD DEATH IS FENCED -------------------------------------------
echo "==> 2) FENCING: site A's lord death is fenced (AE-031 lord-liveness)"
check "before: site A serves counterA" 3 "$(aeq "$spa" counterA)"

echo "  -- SIGKILL site A's lord (site A NATS keeps running) --"
kill -9 "$spa_lord" 2>/dev/null || true

if poll_gone "$spa" counterA 10; then
  echo "  PASS  counterA self-exited after its lord's death (lease self-exit)"
else
  echo "  FAIL  counterA still responds after the lord's death (fencing failed): $(aeq "$spa" counterA)"
  failures=$((failures + 1))
fi

check "center reaches site B (untouched)" 5 "$(aeq "$hub" counterB)"
check "site B serves locally (untouched)"  5 "$(aeq "$spb" counterB)"

# --- verdict -----------------------------------------------------------------
echo
if [ "$failures" -eq 0 ]; then
  echo "ALL ASSERTIONS PASSED - hub-spoke survives failure."
  exit 0
fi
echo "$failures assertions FAILED - see $run/*.log"
exit 1
