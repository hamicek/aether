# Contributing to aether

Thanks for your interest. aether is an early-stage polyglot actor/OTP runtime over NATS; see
[DESIGN.md](DESIGN.md) for the architecture and [ROADMAP.md](ROADMAP.md) for what is intentionally
still open.

## Prerequisites

- **Go** 1.23+ (the runtime, lord, CLI, embedded NATS server)
- **Bun** (the TypeScript SDK and example thralls)
- **Python** 3.10+ with `nats-py` (the Python SDK)

## Build

```bash
# Go runtime and CLI
go build ./...
go build -o bin/aether ./cmd/aether
go build -o bin/counter-go ./examples/counter

# TypeScript workspace (from the repo root)
bun install
```

If your Go setup tries to download a different toolchain and you want to pin to the local one,
prefix the build with `GOTOOLCHAIN=local`.

## Run the tests

Each language has one command and none of them need a running NATS server (the Go integration
tests start an embedded server in-process):

```bash
# Go: unit + integration (embedded NATS)
go test ./...

# TypeScript SDK
cd sdk/ts && bun test

# Python SDK (nats-py is the SDK's own dependency; no broker is contacted)
cd sdk/python && python -m unittest wire_parity_test
```

## The wire contract is the one thing that must stay in sync

The wire contract (JSON envelope and subject conventions) has three hand-mirrored
implementations: Go (`internal/wire`, the source of truth), TypeScript
(`sdk/ts/src/{envelope,subjects}.ts`) and Python (`sdk/python/aether.py`).

**Go is authoritative.** When you change the envelope or a subject:

1. Update `internal/wire` first.
2. Regenerate the golden fixtures shared by all three parity suites:
   ```bash
   go test ./internal/wire -run TestEnvelopeGolden -update
   ```
3. Mirror the change in the TypeScript and Python SDKs.
4. Run all three parity suites (above); they will fail if any side drifts.

## Commit conventions

- Write commit messages in plain, human style. Keep them focused on one logical change.
- Use a plain hyphen `-`; do not use en or em dashes (a git hook rejects them).
- The source code is English-only (comments, docstrings, log and error messages).

## Running the demo

```bash
cd examples/counter
../../bin/aether up -f aether.toml
# the gateway prints: counter=N counter-py=N counter-go=N
```

`aether.toml` (polyglot) and the sibling manifests (`aether-durable*.toml`,
`aether-external*.toml`, `aether-singleton.toml`, `aether-one-for-all.toml`,
`aether-rest-for-one.toml`) each demonstrate one scenario.
