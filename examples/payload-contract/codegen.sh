#!/usr/bin/env bash
# Regenerate native types from the JSON schemas. The schemas are the single source of truth;
# gen/ is a derived, committed artifact. Run this after changing a schema, then commit gen/.
# The CI drift check regenerates into a temp dir and diffs against the committed gen/, so a
# schema change without a regeneration fails the build. See PAYLOAD-CONTRACT.md (AE-042 §5).
#
# Usage: ./codegen.sh [OUT_DIR]   (OUT_DIR defaults to "gen"; the drift check passes a temp dir)
set -euo pipefail
cd "$(dirname "$0")"

OUT="${1:-gen}"

# Pinned tool versions keep the generated output stable, so the drift diff is meaningful.
GO_JSONSCHEMA="github.com/atombender/go-jsonschema@v0.24.1"
JSON2TS="json-schema-to-typescript@15.0.4"

mkdir -p "$OUT/go" "$OUT/ts"
for schema in schemas/*.schema.json; do
  base="$(basename "$schema" .schema.json)"
  go run "$GO_JSONSCHEMA" \
    --package contract --only-models --struct-name-from-title --tags json \
    "$schema" >"$OUT/go/${base}.go"
  bunx --bun "$JSON2TS" "$schema" >"$OUT/ts/${base}.ts"
done
