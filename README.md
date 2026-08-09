# aether

[![CI](https://github.com/hamicek/aether/actions/workflows/ci.yml/badge.svg)](https://github.com/hamicek/aether/actions/workflows/ci.yml)

A polyglot distributed actor/OTP runtime over NATS. A **lord** (supervisor) spawns
**thralls** (genservers) as OS processes and lets them communicate in the **ether** (NATS).

The goal: an SDK that makes it very easy to write thralls and run a lord. Not BEAM-scale
(millions of processes), but tens of processes that communicate reliably - in any language
and with real OS-process isolation.

Full design: [DESIGN.md](./DESIGN.md). License: [MIT](./LICENSE). Contributing:
[CONTRIBUTING.md](./CONTRIBUTING.md). Roadmap and deliberately deferred work: [ROADMAP.md](./ROADMAP.md).

## Glossary

| term | meaning | OTP counterpart |
|---|---|---|
| **ether** | the bus - embedded NATS or an external cluster | message transport |
| **lord** | the supervisor - spawns and watches thralls, restarts them per strategy | Supervisor |
| **thrall** | the genserver - state + serialized message processing | GenServer |

## Features

Everything below is implemented and exercised for real (see the manifests in `examples/counter/`).

| Area | Status | Detail |
|---|---|---|
| **Spawn** | ✅ | The lord starts thralls as OS processes, injects the `AETHER_*` env, watches their exit code |
| **Polyglot SDK** | ✅ | **TS/Bun**, **Python**, **Go** - the same wire contract, indistinguishable to the lord |
| **Messaging** | ✅ | `call` (sync request/reply), `cast` (fire-and-forget), a serialized mailbox |
| **Supervision** | ✅ | `one_for_one`, `one_for_all`, `rest_for_one` + a restart-intensity window + backoff |
| **Graceful drain** | ✅ | `ctl:drain` -> the thrall finishes its mailbox -> `terminate` -> escalation to SIGTERM/SIGKILL |
| **Observability** | ✅ | KV registry (`name -> pid/status`), a lifecycle stream, the CLI `ps` / `events` |
| **Durable mailbox** | ✅ | `durable=true` -> casts survive a thrall crash (JetStream). TS + Python + Go. What survives a *restart*: see [Durability](#durability) |
| **External NATS** | ✅ | `mode="external"` is purely a config switch - the same stack against a real cluster |
| **Singleton** | ✅ | `scope="singleton"` -> a distributed KV-CAS lock, one instance per cluster + failover |

Restart policy per thrall: `permanent` / `transient` / `temporary`.

## Layout

```
cmd/aether/           CLI: up | ps | events | cast | call
internal/
  ether/              embedded NATS / external connection (mode switch)
  lord/               supervisor: manifest, supervisor loop, restart strategies,
                      graceful drain, durable stream provisioning, singleton lock
  registry/           JetStream KV registry (name -> pid/status)
  singleton/          distributed lock over KV (Create/CAS + TTL failover)
  soak/               bounded latency/leak metric primitives for the soak suite
  wire/               envelope + subject/stream conventions (Go side, shared with the SDKs)
sdk/ts/               @hamicek/aether (Bun/TS): defThrall, start, call, cast
sdk/python/           aether.py: def_thrall, start, run
sdk/go/thrall/        thrall.Def[S], thrall.Start, thrall.Call/Cast
examples/counter/     counter (TS/Py/Go) + gateway + a manifest per scenario
scripts/soak.sh       run the soak/chaos suite (out of CI)
```

## Subject convention

```
aether.<app>.<name>.call     # request/reply (call)
aether.<app>.<name>.cast     # fire-and-forget (cast); for durable thralls a JetStream stream captures it
aether.<app>.<name>.info     # out-of-band (timers, notifications)
aether._lord.<name>.ctl      # lord -> thrall (drain / shutdown / ping)
aether._lord.<name>.hb       # thrall -> lord (heartbeat)
aether._lord.events          # lifecycle stream (spawned/ready/down/restarting/...)
aether_<app>_<name>          # JetStream stream for the durable mailbox (dots -> underscores)
```

## CLI

```bash
aether up -f <manifest>          # bring up the ether + the lord per the manifest
aether ps [--url <nats>]         # a table of thrall status from the KV registry
aether events [--url <nats>]     # the live lifecycle stream
aether cast <name> <op> [json]   # send a cast to a thrall
aether call <name> <op> [json]   # send a call and print the reply
```

In embedded mode `ps`/`events`/`cast`/`call` connect via `.aether-endpoint`
(written by `aether up`); against an external cluster via `--url`.

## A thrall in TS (example)

```ts
import { defThrall, start } from "@hamicek/aether";

const counter = defThrall<number>({
  name: process.env.AETHER_NAME ?? "counter",
  init: () => 0,
  handleCall: { get: (_p, s) => [s, s] },        // [reply, newState]
  handleCast: { inc: (_p, s) => s + 1, dec: (_p, s) => s - 1 },
  terminate: (reason, s) => console.log(`counter exiting (${reason}), last value = ${s}`),
});

await start(counter);
```

The same thrall also exists in `counter_py.py` (Python) and `counter_go.go` (Go) - functionally
identical. Durability is purely a manifest concern (`durable = true`), not thrall code.

## Manifest (example)

```toml
app = "demo"
strategy = "one_for_one"                 # | one_for_all | rest_for_one
restart_intensity = { max = 3, within_ms = 5000 }

[nats]
mode = "embedded"                        # | external (+ url = "nats://...")
# store_dir = "./.aether-store"          # embedded only: persist the durable mailbox across restarts

[[thrall]]
name = "counter"
cmd  = "bun run ./counter.ts"
restart = "permanent"                    # | transient | temporary
scope   = "local"                        # | singleton
durable = false                          # true -> cast over JetStream
```

## Durability

`durable = true` means a cast is **not lost** if the thrall crashes before handling it - it does
**not** mean the thrall's state is remembered (a restart runs a clean `init`, like OTP). How long the
durable mailbox survives depends on where JetStream stores it:

| Mode | Config | Survives a thrall crash | Survives a lord/process restart |
|---|---|---|---|
| embedded, ephemeral (default) | `mode="embedded"` | ✅ | ❌ (temp dir wiped on Stop) |
| embedded, persistent | `mode="embedded"` + `store_dir` | ✅ | ✅ (same host, same dir) |
| external, persistent | `mode="external"` | ✅ | ✅ (independent of the app host) |

Use `store_dir` for a single-host deployment that must keep the mailbox across restarts (see
`examples/counter/aether-durable-persistent.toml`); use external NATS when durability must be
independent of the application host. Full model: [DESIGN.md §13](./DESIGN.md).

## Quickstart

The default demo (`aether.toml`) is polyglot, so it needs Go, Bun and Python. Build the runtime
and the Go thrall, then set up the Bun and Python sides:

```bash
# 1) build the runtime and the Go counter thrall
go build -o bin/aether ./cmd/aether
go build -o bin/counter-go ./examples/counter

# 2) TypeScript workspace
bun install

# 3) Python thrall dependency
cd examples/counter
python -m venv .venv
.venv/bin/pip install nats-py
cd ../..

# 4) run it
cd examples/counter
../../bin/aether up -f aether.toml
# the gateway prints, once per second: counter=N counter-py=N counter-go=N
```

If `go` tries to download a different toolchain, prefix the build with `GOTOOLCHAIN=local`.

Sample manifests in `examples/counter/`: `aether.toml` (polyglot TS/Py/Go),
`aether-durable.toml`, `aether-durable-persistent.toml`, `aether-external.toml`,
`aether-singleton.toml`, `aether-one-for-all.toml`, `aether-rest-for-one.toml`,
`aether-secure-external.toml`.

## Security

By default the embedded server binds `127.0.0.1` and is meant for a trusted single host. To run
against a **networked / shared NATS**, connect over TLS with nkey authentication: add a `[nats.tls]`
CA and a `[nats.auth]` nkey seed to the manifest. The lord authenticates with them and injects the
**paths** (not the secrets) into every thrall, so all three SDKs connect the same secured way.

```toml
[nats]
mode = "external"
url  = "tls://nats.internal:4222"

[nats.tls]
ca = "/etc/aether/ca.pem"          # verify the server (server TLS)

[nats.auth]
nkey_seed = "/etc/aether/user.nk"  # authenticate (nkey)
```

Secrets are passed by **file path**, never as an env value - the nkey seed never appears in `ps` or
the process environment. A full example is `examples/counter/aether-secure-external.toml`.

Python thralls using nkey auth need the optional `nkeys` package (plain `nats-py` omits it):
`pip install 'nats-py[nkeys]'`.

Scope: this secures the client side against an already-secured external NATS (server TLS + nkeys).
Securing the embedded server itself (for a networked `0.0.0.0` bind), mutual TLS, JWT/account
isolation, token auth and the operator CLI against a secured cluster are follow-ups tracked in
[ROADMAP.md](./ROADMAP.md).

## Reliability testing (soak)

For high-reliability use (e.g. SCADA) the functional integration tests are not enough - they run for
seconds and cannot surface leaks, message loss under pressure or latency drift. The soak suite drives
the runtime under an hours-long load and checks it against concrete bars. It lives behind the `soak`
build tag, so normal CI (`go test ./...`) never runs it; you invoke it explicitly:

```bash
scripts/soak.sh smoke                 # ~2 min  - all scenarios, a quick end-to-end check
scripts/soak.sh default chaos         # ~45 min - one scenario (all|load|chaos|drain|singleton)
scripts/soak.sh overnight singleton   # ~8 h
scripts/soak.sh smoke 12345           # a fixed seed for a reproducible run

# ad-hoc: override the length, write the report to a file
AETHER_SOAK_DURATION=30s AETHER_SOAK_REPORT=soak.txt scripts/soak.sh smoke
```

Each timed scenario runs for the profile duration (so `all` at a long profile is long - pick a single
scenario for `default`/`overnight`). The suite measures:

- **Sustained load** - a concurrent `call` stream; bar: p99 < 50ms and no upward latency trend.
- **Durable no-loss** - a durable cast stream over JetStream; bar: every stored cast delivered
  (zero loss); duplicates are counted and tolerated.
- **Leak detection** - the lord's goroutines and heap plus a sample of thrall RSS; bar: < 10% growth
  after warm-up. (On a run too short to warm up, the deltas are reported but not enforced.)
- **Chaos** - random thralls are `SIGKILL`ed under load; bar: recovery per strategy < 3s, with durable
  delivery lossless through the kills (checked server-side by the stream draining to zero).
- **Singleton failover** - two lord nodes (OS processes) race for a singleton; the holder's node is
  killed repeatedly; bar: failover < 5s and never more than one live instance (fencing).
- **Graceful drain** - a durable thrall is drained mid-stream and restarted; bar: no work lost.

It ends with a structured report and a **non-zero exit on any bar breach**.

## Deliberately deferred

The runtime has conscious gaps, tracked in [ROADMAP.md](./ROADMAP.md): liveness beyond heartbeats
(`$SYS` events), full thrall-level fencing for orphaned singletons, `temporary` semantics inside
group strategies, thrall state persistence (today durability covers the mailbox, not the state - see
[Durability](#durability)), and chaos/failover coverage on top of the soak suite for high-reliability use.
