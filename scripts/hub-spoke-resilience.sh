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
  echo "nats-server neni na PATH - nainstaluj (viz CLAUDE.md 'External NATS pro dev')" >&2
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
  # defenzivni sweep: orphan thrally po zabitem lordu (scenar 2) nejsou v pids
  pkill -9 -f "$sp/bin/" 2>/dev/null || true
}
trap cleanup EXIT

# --- build -------------------------------------------------------------------
echo "==> build (aether, counter, gateway)"
"${go_cmd[@]}" build -o "$sp/bin/aether"  "$root/cmd/aether"
"${go_cmd[@]}" build -o "$sp/bin/counter" "$root/examples/counter"
"${go_cmd[@]}" build -o "$sp/bin/gateway" "$sp"
aether="$sp/bin/aether"

# --- helpers -----------------------------------------------------------------
wait_for() { # <soubor> <vzor> <timeout_s>
  local file="$1" pat="$2" limit="${3:-10}" waited=0
  while ! grep -q "$pat" "$file" 2>/dev/null; do
    sleep 0.2
    waited=$((waited + 1))
    if [ "$waited" -ge $((limit * 5)) ]; then
      echo "timeout cekani na '$pat' v $file" >&2
      return 1
    fi
  done
}

failures=0
check() { # <popis> <ocekavano> <ziskano>
  if [ "$2" = "$3" ]; then
    echo "  PASS  $1 (=$3)"
  else
    echo "  FAIL  $1: ocekavano '$2', ziskano '$3'"
    failures=$((failures + 1))
  fi
}

# aeq = aether call value (prazdny retezec pri chybe; flagy PRED pozicnimi argumenty)
aeq() { # <url> <name>
  "$aether" call --url "$1" --app demo --timeout 2s "$2" get 2>/dev/null || true
}

# poll_reach ceka, az call na <url>/<name> vrati <want> (nebo timeout) - pro reconnect.
# Pozn.: timeout je `if ... then return`, NE `[ ] && return` - to by pod `set -e`
# spadlo hned v prvni iteraci (kdyz je podminka false, `&&` vrati 1 -> exit).
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

# poll_gone ceka, az call na <url>/<name> ZACNE selhavat (thrall se sam ukoncil).
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
start_hub_nats() { # (re)start hub NATS s CERSTVYM logem (aby wait_for necetl stary beh)
  hub_starts=$((hub_starts + 1))
  hub_log="$run/hub-$hub_starts.log"
  nats-server -c "$sp/nats/hub.conf" -sd "$run/hub" -l "$hub_log" >/dev/null 2>&1 &
  hub_nats=$!
  pids+=("$hub_nats")
  disown
}

# --- start topologie ---------------------------------------------------------
echo "==> start NATS uzlu (hub + 2 leaf)"
start_hub_nats
wait_for "$hub_log" "Listening for leafnode" 10
nats-server -c "$sp/nats/spoke-a.conf" -sd "$run/sa" -l "$run/sa.log" >/dev/null 2>&1 & pids+=($!); disown
nats-server -c "$sp/nats/spoke-b.conf" -sd "$run/sb" -l "$run/sb.log" >/dev/null 2>&1 & pids+=($!); disown
wait_for "$hub_log" 'remote "sa"' 10
wait_for "$hub_log" 'remote "sb"' 10
echo "  leaf spojeni navazana"

echo "==> start lordu (hub, spoke-a, spoke-b)"
cd "$sp"
"$aether" up -f aether-hub.toml     >"$run/up-hub.log" 2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-a.toml >"$run/up-sa.log"  2>&1 & spa_lord=$!; pids+=("$spa_lord"); disown
"$aether" up -f aether-spoke-b.toml >"$run/up-sb.log"  2>&1 & pids+=($!); disown
wait_for "$run/up-hub.log" "thrall ready" 15
wait_for "$run/up-sa.log"  "thrall ready" 15
wait_for "$run/up-sb.log"  "thrall ready" 15
echo "  vsechny thrally on the bus"

echo "==> seed: counterA += 3, counterB += 5"
for _ in 1 2 3;     do "$aether" cast --url "$spa" --app demo counterA inc >/dev/null; done
for _ in 1 2 3 4 5; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null; done
poll_reach "$spa" counterA 3 || echo "  varovani: counterA se neustalil"
poll_reach "$spb" counterB 5 || echo "  varovani: counterB se neustalil"

# --- 1) VYPADEK CENTRALY + ZOTAVENI -----------------------------------------
echo "==> 1) VYPADEK CENTRALY: sajta prezije a cerstve se zotavi"
check "baseline: centrala -> counterA" 3 "$(aeq "$hub" counterA)"

echo "  -- zabijim hub NATS --"
kill -9 "$hub_nats" 2>/dev/null || true
sleep 2

check "sajta A obsluhuje lokalne i odriznuta" 3 "$(aeq "$spa" counterA)"
check "sajta B obsluhuje lokalne i odriznuta" 5 "$(aeq "$spb" counterB)"

echo "  -- restartuji hub NATS --"
start_hub_nats
wait_for "$hub_log" "Listening for leafnode" 10

SECONDS=0
if poll_reach "$hub" counterA 3 45; then
  echo "  PASS  po obnove hubu leaf reconnect + centrala -> counterA (=3, zotaveni ~${SECONDS}s)"
else
  echo "  FAIL  centrala se nezotavila (counterA nedosazitelna po restartu hubu): $(aeq "$hub" counterA)"
  failures=$((failures + 1))
fi

# --- 2) PAD LORDA SAJTY JE OHRANICENY ---------------------------------------
echo "==> 2) FENCING: pad lorda sajty A je ohraniceny (AE-031 lord-liveness)"
check "pred: sajta A obsluhuje counterA" 3 "$(aeq "$spa" counterA)"

echo "  -- SIGKILL lorda sajty A (NATS sajty A bezi dal) --"
kill -9 "$spa_lord" 2>/dev/null || true

if poll_gone "$spa" counterA 10; then
  echo "  PASS  counterA se sam ukoncil po smrti sveho lorda (lease self-exit)"
else
  echo "  FAIL  counterA odpovida i po smrti lorda (fencing selhal): $(aeq "$spa" counterA)"
  failures=$((failures + 1))
fi

check "centrala dosahne na sajtu B (nedotcena)" 5 "$(aeq "$hub" counterB)"
check "sajta B obsluhuje lokalne (nedotcena)"    5 "$(aeq "$spb" counterB)"

# --- verdikt -----------------------------------------------------------------
echo
if [ "$failures" -eq 0 ]; then
  echo "VSECHNA TVRZENI PROSLA - hub-spoke prezije selhani."
  exit 0
fi
echo "$failures tvrzeni SELHALO - viz $run/*.log"
exit 1
