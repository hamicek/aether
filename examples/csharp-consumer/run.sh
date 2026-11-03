#!/usr/bin/env bash
# End-to-end demo of the thin C# client: a Go -> C# -> Go roundtrip over the ether.
# Builds the aether binary and the Go thralls, publishes the C# transformer, runs the lord, and
# asserts that a transformed reading (100 C -> 212 F) reaches the Go printer.
set -euo pipefail
cd "$(dirname "$0")"
export GOTOOLCHAIN=local

echo "building aether + Go thralls..."
mise exec go@latest -- go build -o ../../bin/aether ../../cmd/aether
mise exec go@latest -- go build -o bin/source ./source
mise exec go@latest -- go build -o bin/printer ./printer

echo "publishing C# transformer..."
mise exec dotnet@latest -- dotnet publish Transformer/Transformer.csproj -c Release -o Transformer/publish --nologo -v quiet

LOG="$(mktemp)"
echo "running lord (log: $LOG)..."
mise exec dotnet@latest -- ../../bin/aether up -f aether.toml >"$LOG" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  if grep -q "100.0C = 212.0F" "$LOG"; then
    echo "PASS: cross-language roundtrip observed"
    grep "reading:" "$LOG" | sort -u | head
    exit 0
  fi
  sleep 1
done

echo "FAIL: expected transformed reading (100.0C = 212.0F) not seen in time"
echo "--- lord output ---"
tail -60 "$LOG"
exit 1
