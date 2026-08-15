# payload-contract - shared schema across languages (PoC)

Proof of ergonomics for the payload contract (AE-042 §8, bundle 1). One shared JSON Schema,
`schemas/measurement.schema.json`, used on both sides of a boundary in two languages:

- **driver (Go)** produces typed `Measurement` values and casts them to the BFF.
- **bff (TS)** validates every incoming measurement against the same schema with `decode()`
  at its boundary, before it counts. A valid measurement passes as a typed value; a malformed
  one is rejected with a clear reason and never pollutes state.

The runtime is untouched: the payload stays untyped on the wire, and validation is an opt-in
call the BFF makes at the boundary it owns - not something the transport enforces.

## Run

```bash
export GOTOOLCHAIN=local
# from the repo root
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/contract-driver ./examples/payload-contract
bun install

cd examples/payload-contract
../../bin/aether up -f aether.toml
```

Expected output (the driver sends two valid measurements and one malformed one):

```
bff: accepted voltage=231.4V from s-1
bff: accepted current=12.5A from s-2
bff: rejected a malformed measurement - /metric: must be equal to one of the allowed values
```

## Test

```bash
cd examples/payload-contract
bun test           # decode() accepts a valid measurement and rejects malformed ones, via the schema file
```

## What the codegen follow-up changes

The `Measurement` types here are hand-written on both sides. Bundle 2 (AE-042 §8) generates
them from `schemas/measurement.schema.json`, so the producer and consumer types cannot drift
from the schema.
