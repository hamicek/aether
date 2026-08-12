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
| **Behaviours** | ✅ | GenServer thrall (`Def` / `defThrall`) and a state-machine thrall (`FSM` / `defFSM`, a gen_statem analogue) - see [State machine](#state-machine-fsm-behaviour) |
| **Supervision** | ✅ | `one_for_one`, `one_for_all`, `rest_for_one` + a restart-intensity window + backoff |
| **Graceful drain** | ✅ | `ctl:drain` -> the thrall finishes its mailbox -> `terminate` -> escalation to SIGTERM/SIGKILL |
| **Observability** | ✅ | Structured logs (lord + all SDKs), a Prometheus `/metrics` endpoint, heartbeat miss detection, cross-process tracing - see [Observability](#observability) |
| **Durable mailbox** | ✅ | `durable=true` -> casts survive a thrall crash (JetStream). TS + Python + Go. What survives a *restart*: see [Durability](#durability) |
| **Event-sourced rebuild** | ✅ | `event_log=true` -> `Append` events to a retention log, `Rebuild` state from it in init - **state survives a restart** by replaying the log, not a snapshot. See [Event-sourced rebuild](#event-sourced-rebuild) |
| **External NATS** | ✅ | `mode="external"` is purely a config switch - the same stack against a real cluster |
| **Singleton** | ✅ | `scope="singleton"` -> a distributed KV-CAS lock, one instance per cluster + failover |
| **Dynamic supervisor** | ✅ | `ctx.StartChild(spec)` / `ctx.StopChild(name)` -> spawn/stop thralls at runtime, supervised one_for_one, outside manifest groups; idempotent on name |

Restart policy per thrall: `permanent` / `transient` / `temporary`.

Dynamic children live only in the running lord and do **not** survive a lord restart by
design - re-establishing them is the owner's job, not the lord's. Because `StartChild` is
idempotent on name, a supervising thrall can re-spawn its children blindly from its own
`init` (and re-apply on demand) with no duplicates. Runnable demo:
`examples/dynamic-topology/` (Go/TS/Python); rationale in `DESIGN.md` section 12.

## Layout

```
cmd/aether/           CLI: up | ps | events | cast | call
internal/
  ether/              embedded NATS / external connection (mode switch)
  lord/               supervisor: manifest, supervisor loop, restart strategies,
                      graceful drain, durable stream provisioning, singleton lock
  registry/           JetStream KV registry (name -> pid/status)
  singleton/          distributed lock over KV (Create/CAS + TTL failover)
  obs/                structured logging + the metric registry (Prometheus exposition)
  soak/               bounded latency/leak metric primitives for the soak suite
  wire/               envelope + subject/stream conventions (Go side, shared with the SDKs)
sdk/ts/               @hamicek/aether (Bun/TS): defThrall/start + defFSM/startFSM, call, cast
sdk/python/           aether.py: def_thrall/start/run + FSM/start_fsm/run_fsm
sdk/go/thrall/        thrall.Def[S]/Start (GenServer) + thrall.FSM[D]/StartFSM (state machine)
examples/counter/     counter (TS/Py/Go) + gateway + a manifest per scenario
examples/fsm/         state-machine (FSM) behaviour demo - a turnstile
examples/eventsourced/ event-sourced rebuild demo - state that survives a restart
examples/dynamic-topology/ dynamic children re-established by their owner after a lord restart
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
(written by `aether up`); against an external cluster via `--url`. For a secured
external bus (server TLS + nkeys) pass `--ca <file>` / `--nkey <seed>` (or the
`AETHER_NATS_CA` / `AETHER_NATS_NKEY_SEED` env); each layers flag > `.aether-endpoint`
> env. Against a secured cluster `aether up` writes the credential paths into
`.aether-endpoint`, so the other tools pick them up automatically.

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

## State machine (FSM) behaviour

Alongside the GenServer thrall, a thrall can be a **finite state machine** - aether's analogue
of OTP's `gen_statem`. Instead of a state field and a `switch` in a cast handler, you declare
named states and per-op reactions; the SDK dispatches an event to the current state's reaction,
applies the transition, and serializes everything through the same mailbox (so the machine's
data needs no locks). Reactions may carry a **guard**, and a state may arm a **state timeout**.
Events are ordinary call/cast, so an FSM thrall interoperates with any caller; a reserved
`_state` call op reports the current state.

```go
fsm := thrall.FSM[int]{
  Name: "turnstile", Initial: "locked",
  Init: func(*thrall.Ctx) (int, error) { return 0, nil },
  States: map[string]thrall.State[int]{
    "locked": {On: map[string]thrall.Reaction[int]{
      "coin": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
        return thrall.Outcome[int]{Next: "unlocked", Data: pushes}, nil
      }},
    }},
    "unlocked": {
      On: map[string]thrall.Reaction[int]{
        "push": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
          return thrall.Outcome[int]{Next: "locked", Data: pushes + 1, Reply: pushes + 1}, nil
        }},
        "autolock": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
          return thrall.Outcome[int]{Next: "locked", Data: pushes}, nil
        }},
      },
      Timeout: &thrall.StateTimeout[int]{After: 5 * time.Second, Op: "autolock"}, // idle -> autolock
    },
  },
}
thrall.StartFSM(fsm)
```

An event with no reaction in the current state is rejected (`no_transition`), not silently lost.
The FSM is deliberately domain-neutral - a lifecycle/alarm automaton is built *on top* of it, not
inside it. Mirrored in all three SDKs (`startFSM`/`defFSM` in TS, `start_fsm`/`FSM` in Python).
Runnable demo: `examples/fsm/`.

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

## Event-sourced rebuild

The durable mailbox keeps *messages*, not *state* - a restarted thrall runs a clean `init`. To
make **state** survive a restart, opt a thrall into an **event log** and rebuild from it: "the
log is truth, state is a projection". The event log is a separate **retention** stream (kept, so
it can be replayed), independent of the WorkQueue mailbox; the lord provisions it when the
manifest sets `event_log = true`.

```toml
[[thrall]]
name = "account"
cmd  = "../../bin/account"
event_log = true
# event_log_max_msgs = 100000   # optional bounds on retention
# event_log_max_age_ms = 604800000
```

A thrall `Append`s domain events as it handles messages, and `Rebuild`s its state from the log
in `init` (works for both the GenServer and the FSM behaviour, since both have an `init`):

```go
Init: func(ctx *thrall.Ctx) (account, error) {
  // replay the log into the balance; empty log -> the initial value
  return thrall.Rebuild(ctx, account{}, apply)
},
HandleCast: map[string]thrall.CastFn[account]{
  "deposit": func(payload json.RawMessage, acc account, ctx *thrall.Ctx) (account, error) {
    ctx.Append(delta{Delta: amount}) // persist first (the log is the truth)
    acc.Balance += amount
    return acc, nil
  },
},
```

Replay is ordered and complete (single-writer = append order). Because the mailbox is
at-least-once, **the fold and handlers must be idempotent** (an event may be replayed). With a
persistent JetStream (`store_dir` or external), the rebuilt state survives stopping and starting
`aether up`. Mirrored in all three SDKs (`ctx.append` + `rebuild` in TS/Python). Snapshots and
compaction are future work - the event log is bounded only by its configured retention. Runnable
demo: `examples/eventsourced/` (a bank account whose balance survives a restart).

## Observability

The runtime reports on itself so a remote deployment can answer "why isn't it working" without
guessing. Three layers, all configured through the manifest and environment:

**Structured logs.** The lord and all three SDKs log as machine-parseable records with levels,
tagged with `component` / `app` / `name` so lord and thrall lines are tellable apart on a shared
stream. Configured by environment, and the lord injects the same config into every thrall so the
whole tree logs consistently:

```bash
AETHER_LOG_LEVEL=debug   # debug | info | warn | error  (default info)
AETHER_LOG_FORMAT=json   # json | text                  (default text, for dev)
```

**Metrics.** Enable the Prometheus endpoint in the manifest (off by default):

```toml
[observability]
metrics_addr = "127.0.0.1:7391"   # empty / omitted = disabled
```

Then scrape `http://127.0.0.1:7391/metrics`. The endpoint is the lord's own HTTP server, so it
works the same embedded or external. Exposed series:

| Metric | Type | Meaning |
|---|---|---|
| `aether_up` | gauge | 1 while the lord is running |
| `aether_thralls{status}` | gauge | thralls by current status (`starting`/`ready`/`down`/`stale`) |
| `aether_restarts_total{name}` | counter | restarts per thrall |
| `aether_gave_up_total{name}` | counter | thralls the lord stopped restarting (intensity exceeded) |
| `aether_heartbeat_misses_total{name}` | counter | detected heartbeat outages |
| `aether_mailbox_depth{name}` | gauge | messages a thrall currently holds (self-reported) |
| `aether_mailbox_latency_ms{name}` | gauge | most recent handler duration (self-reported) |
| `aether_processed_total{name}` | counter | messages a thrall has processed (self-reported) |
| `aether_durable_backlog{name}` | gauge | pending casts in a durable thrall's stream (JetStream `num_pending`) |

Thralls report their own mailbox metrics on the heartbeat they already send every 2s; the lord
aggregates them plus its own supervision counters. A thrall that stops heart-beating is marked
`stale` and counted (`aether_heartbeat_misses_total`) even though its process is still alive.

**Tuning liveness detection.** The heartbeat interval and how many misses count as `stale` are
configurable, so a latency-sensitive deployment can detect a hung thrall faster than the default
~6s. The lord injects the interval into the thralls and derives its own reaper threshold from the
same value, so the two never drift:

```toml
[liveness]
heartbeat_interval_ms = 500   # default 2000
stale_after_misses    = 2     # default 3   -> stale after 1s instead of 6s
```

(Process death is already detected instantly by the OS watcher; this tunes detection of the
hung-but-alive case. Faster connection-loss detection via NATS `$SYS` events is future work,
relevant to a networked topology where thralls connect over a network rather than to a local
embedded NATS.)

**Tracing.** The envelope carries a `trace` correlation id propagated across `call`/`cast` hops:
an edge (the CLI, or the first message) mints it, `ctx.Call` / `ctx.Cast` pass it downstream, and
it appears in the logs - so one logical operation can be followed across processes. See
`examples/tracing/` for a runnable two-thrall demo.

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
.venv/bin/pip install -r requirements.txt
cd ../..

# 4) run it
cd examples/counter
../../bin/aether up -f aether.toml
# the gateway prints, once per second: counter=N counter-py=N counter-go=N
```

If `go` tries to download a different toolchain, prefix the build with `GOTOOLCHAIN=local`.

Sample manifests in `examples/counter/` (each runs with any of the three languages - the
thrall name comes from the manifest):

- `aether.toml` - the default polyglot demo (TS + Python + Go + gateway)
- `aether-durable.toml` - durable mailbox (embedded), survives a thrall restart
- `aether-durable-persistent.toml` - durable mailbox with a persistent embedded JetStream (`store_dir`)
- `aether-durable-poly.toml` - durable mailbox in both Python and Go
- `aether-external.toml` - against a standalone NATS cluster (7390)
- `aether-external-durable.toml` - durable mailbox on the external cluster (stream + KV off-runtime)
- `aether-observability.toml` - polyglot demo with the `/metrics` endpoint enabled
- `aether-singleton.toml` - singleton scope with distributed-lock failover (external cluster)
- `aether-one-for-all.toml` - the one_for_all supervision strategy
- `aether-rest-for-one.toml` - the rest_for_one supervision strategy
- `aether-secure-external.toml` - external cluster over TLS with nkey auth

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

Scope: this secures the client side against an already-secured external NATS (server TLS + nkeys),
including the operator CLI (`--ca`/`--nkey`). Securing the embedded server itself (for a networked
`0.0.0.0` bind), mutual TLS, JWT/account isolation and token auth are follow-ups tracked in
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
