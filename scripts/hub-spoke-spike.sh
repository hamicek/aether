#!/usr/bin/env bash
#
# Hub-spoke viceuzlovy spike (AE-051). Postavi a spusti hub + 2 sajty, kazdou
# jako samostatny uzel (vlastni NATS + vlastni lord), sajty pripojene k hubu
# jako NATS leaf nodes s account izolaci, a overi tri tvrzeni:
#
#   1. DISTRIBUCE   - gateway na hubu precte realny stav obou sajt cross-node.
#   2. IZOLACE(-)   - sajta A nedosahne na thrall sajty B (no responders).
#   3. IZOLACE(+)   - odposlech na sajte A neuvidi zadny provoz sajty B,
#                     zatimco kontrolni odposlech na sajte B ho vidi.
#
# Deliberately out of CI: spousti 3 NATS servery + 3 lordy a asertuje zivou
# viceuzlovou topologii. Exit 0 = vsechna tvrzeni prosla.
#
# Usage: scripts/hub-spoke-spike.sh
#
set -euo pipefail

# Go neni vzdy na PATH na dev strojich; fallback na mise. nats-server je nutny.
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
run="$sp/.run"
export GOTOOLCHAIN=local

rm -rf "$run"
mkdir -p "$run" "$sp/bin"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  # dej lordum cas na graceful drain thrallu (drain ceiling ~5s), pak dorazi zbytek
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
# wait_for ceka na vyskyt vzoru v logu. Vzory nize jsou znenim logu nats-serveru
# (pinnuty dev binar, viz CLAUDE.md); pri upgrade nats-serveru je overit.
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

hub="nats://127.0.0.1:7390"
spa="nats://127.0.0.1:7392"
spb="nats://127.0.0.1:7393"

# --- start NATS uzly ---------------------------------------------------------
echo "==> start NATS uzlu (hub + 2 leaf)"
nats-server -c "$sp/nats/hub.conf"     -sd "$run/hub" -l "$run/hub.log" >/dev/null 2>&1 & pids+=($!); disown
wait_for "$run/hub.log" "Listening for leafnode" 10
nats-server -c "$sp/nats/spoke-a.conf" -sd "$run/sa"  -l "$run/sa.log"  >/dev/null 2>&1 & pids+=($!); disown
nats-server -c "$sp/nats/spoke-b.conf" -sd "$run/sb"  -l "$run/sb.log"  >/dev/null 2>&1 & pids+=($!); disown
# hub log potvrdi obe leaf spojeni pres jejich vzdalene JetStream domeny (sa/sb)
wait_for "$run/hub.log" 'remote "sa"' 10
wait_for "$run/hub.log" 'remote "sb"' 10
echo "  leaf spojeni navazana"

# --- start lordy -------------------------------------------------------------
# Bezi z sp dir, aby relativni ./bin/... cesty v manifestech sedely. DULEZITE:
# aether up se spousti PRIMO na pozadi (ne v subshellu), aby $! byl PID lorda -
# jinak by kill v uklidu trefil jen subshell a lord (vc. jeho thrallu) by prezil,
# reconnectnul se na dalsi beh a akumuloval stav.
echo "==> start lordu (hub, spoke-a, spoke-b)"
cd "$sp"
"$aether" up -f aether-hub.toml     >"$run/up-hub.log" 2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-a.toml >"$run/up-sa.log"  2>&1 & pids+=($!); disown
"$aether" up -f aether-spoke-b.toml >"$run/up-sb.log"  2>&1 & pids+=($!); disown
wait_for "$run/up-hub.log" "thrall ready" 15
wait_for "$run/up-sa.log"  "thrall ready" 15
wait_for "$run/up-sb.log"  "thrall ready" 15
echo "  vsechny thrally on the bus"

# --- seed (lokalne na sajtach; --url flag musi byt PRED pozicnimi argumenty) --
echo "==> seed: counterA += 3 (sajta A), counterB += 5 (sajta B)"
for _ in 1 2 3;     do "$aether" cast --url "$spa" --app demo counterA inc >/dev/null; done
for _ in 1 2 3 4 5; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null; done

# Casty jsou async; pockej lokalne, az se seed projevi, nez zacnou tvrzeni
# (jinak by cross-node cteni mohlo zachytit jeste nezpracovany stav).
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
poll_value "$spa" counterA 3 || echo "  varovani: counterA se neustalil na 3"
poll_value "$spb" counterB 5 || echo "  varovani: counterB se neustalil na 5"

# --- 1) DISTRIBUCE -----------------------------------------------------------
echo "==> 1) DISTRIBUCE: centrala cte obe sajty cross-node"
# gateway.check dela dva sekvencni cross-node cally (2s kazdy) -> outer timeout 5s
check "gateway.check vidi obe sajty" \
  '{"counterA":3,"counterB":5}' \
  "$("$aether" call --url "$hub" --app demo --timeout 5s gateway check)"
check "primy call hub -> counterA" 3 "$("$aether" call --url "$hub" --app demo --timeout 3s counterA get)"
check "primy call hub -> counterB" 5 "$("$aether" call --url "$hub" --app demo --timeout 3s counterB get)"

# --- 2) IZOLACE negativni ----------------------------------------------------
# Rozlisujeme tri vysledky: uspech = izolace porusena; "no responders" = spravna
# izolace; jina chyba (timeout/spojeni) = neprukazne -> FAIL, at nas maskovana
# chyba nesvede k falesnemu PASS.
echo "==> 2) IZOLACE(-): sajta A nedosahne na counterB"
if iso_out="$("$aether" call --url "$spa" --app demo --timeout 2s counterB get 2>&1)"; then
  echo "  FAIL  sajta A DOSAHLA na counterB (izolace porusena): $iso_out"
  failures=$((failures + 1))
elif printf '%s' "$iso_out" | grep -qi "no responders"; then
  echo "  PASS  sajta A -> counterB odmitnuto (no responders)"
else
  echo "  FAIL  neprukazne: ocekavano 'no responders', ziskano: $iso_out"
  failures=$((failures + 1))
fi

# --- 3) IZOLACE pozitivni (odposlech) ----------------------------------------
echo "==> 3) IZOLACE(+): odposlech na sajte A neuvidi provoz sajty B"
"$sp/bin/probe" --url "$spa" --subject "aether.demo.counterB.>" --secs 2 >"$run/probe-a.out" 2>&1 & pa=$!
"$sp/bin/probe" --url "$spb" --subject "aether.demo.counterB.>" --secs 2 >"$run/probe-b.out" 2>&1 & pb=$!
sleep 0.4
for _ in 1 2 3 4 5 6; do "$aether" cast --url "$spb" --app demo counterB inc >/dev/null; done
wait "$pa" "$pb"
check "odposlech na sajte A (cizi provoz)" "received=0" "$(cat "$run/probe-a.out")"
if [ "$(cat "$run/probe-b.out")" = "received=0" ]; then
  echo "  FAIL  kontrolni odposlech na sajte B nevidel nic (test je vadny)"
  failures=$((failures + 1))
else
  echo "  PASS  kontrolni odposlech na sajte B videl provoz ($(cat "$run/probe-b.out"))"
fi

# --- verdikt -----------------------------------------------------------------
echo
if [ "$failures" -eq 0 ]; then
  echo "VSECHNA TVRZENI PROSLA - hub-spoke topologie funguje, sajty jsou izolovane."
  exit 0
fi
echo "$failures tvrzeni SELHALO - viz $run/*.log"
exit 1
