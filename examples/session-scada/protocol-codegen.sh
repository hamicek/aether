#!/usr/bin/env bash
# Regenerate the per-role automata from the *.protocol.yaml sources. The protocols are the single
# source of truth; gen/automaton is a derived, committed artifact. Run this after changing a protocol,
# then commit gen/. The CI protocol-drift check regenerates into a temp dir and diffs against the
# committed gen/, so a protocol change without a regeneration fails the build. The generator also
# exits non-zero on an ill-formed or non-dual protocol, so the same job is the well-formedness gate.
#
# Usage: ./protocol-codegen.sh [OUT_DIR]   (OUT_DIR defaults to this dir; the drift check passes a temp)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-$HERE}"

# Deterministic output is what makes the drift diff meaningful: the generator sorts states and ops,
# so the JSON depends only on the protocol. Run from the repo root so the Go module resolves.
cd "$HERE/../.."
GOTOOLCHAIN=local go run ./tools/protocol-gen "$HERE" "$OUT"
