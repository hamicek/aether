# aether

[![CI](https://github.com/hamicek/aether/actions/workflows/ci.yml/badge.svg)](https://github.com/hamicek/aether/actions/workflows/ci.yml)

A polyglot distributed actor/OTP runtime over NATS. A **lord** (supervisor) spawns
**thralls** (genservers) as OS processes and lets them communicate in the **ether** (NATS).

## What it's for

You have a handful of long-running programs - a device driver, a worker pool, an API gateway, a
background service - that need to **talk to each other reliably and stay alive**. Wiring that up by
hand usually means gluing together a process manager, a message broker with hand-written reconnect
logic, and your own restart/health/shutdown handling. And when one piece leaks memory or hangs, it
often drags the rest down with it.

aether gives you that plumbing as **one small binary**. It starts your programs as **isolated OS
processes**, watches them, **restarts** them when they crash (per a policy you choose), **drains**
them gracefully on shutdown, and lets them talk over NATS with simple `call` / `cast` messages. A
crash or a leak in one process does not take the others down. And it works the same whether your
programs are **Go, TypeScript, or Python**.

In short: **OTP-style supervision and messaging, over real OS processes, across languages** - without
adopting Erlang, a sidecar, or Kubernetes.

**Reach for aether when you have:**
- **tens** of long-running processes (not millions of tiny ones),
- that must be **supervised** (started, watched, restarted, drained) and **isolated** (one crash does
  not sink the rest),
- possibly written in **different languages**, and
- need **reliable messaging** (sync request/reply, fire-and-forget, optional durable delivery).

Typical shapes: protocol/device drivers feeding a gateway (IoT / edge / SCADA-style), a pool of
workers behind an ingress, or edge sites that must keep running while disconnected from a central hub.
**Concrete shapes and runnable examples: [USE-CASES.md](./USE-CASES.md).**

**It is not a BEAM replacement:** if you need millions of cheap in-VM actors in one language, reach
for Erlang/Elixir. aether is **OTP-inspired, not OTP** - it trades that scale for real OS-process
isolation and polyglot freedom.

## For OTP folks

If you know OTP, that is all you need: the **lord** is a **supervisor**, a **thrall** is a
**GenServer** (or a `gen_statem` / `gen_event` behaviour), and the **ether** is the message
transport. The names are flavor; the model is plain supervision-and-genservers, here over NATS and
across languages. The goal is an SDK that makes it very easy to write thralls and run a lord.

Full design: [DESIGN.md](./DESIGN.md). License: [MIT](./LICENSE). Contributing:
[CONTRIBUTING.md](./CONTRIBUTING.md). Roadmap and deliberately deferred work: [ROADMAP.md](./ROADMAP.md).

This README tracks `main`; for tagged, versioned changes see [Releases](https://github.com/hamicek/aether/releases) (latest **v0.9.0**).

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
| **Behaviours** | ✅ | GenServer thrall (`Def` / `defThrall`), a state-machine thrall (`FSM` / `defFSM`, a gen_statem analogue - see [State machine](#state-machine-fsm-behaviour)) and an event manager (`EventManager` / `defEvent`, a gen_event analogue - see [Event manager](#event-manager-gen_event-behaviour)) |
| **Supervision** | ✅ | `one_for_one`, `one_for_all`, `rest_for_one` + a restart-intensity window + backoff |
| **Graceful drain** | ✅ | `ctl:drain` -> the thrall finishes its mailbox -> `terminate` -> escalation to SIGTERM/SIGKILL |
| **Lord-liveness fencing** | ✅ | no thrall survives its lord: every thrall verifies a KV lease and self-terminates when its lord dies, even on an external SIGKILL where the process-group kill never runs |
| **Observability** | ✅ | Structured logs (lord + all SDKs), a Prometheus `/metrics` endpoint, heartbeat miss detection, cross-process tracing - see [Observability](#observability) |
| **Fleet health** | ✅ | `[observability] fleet_health` -> each lord publishes a curated health summary on `aether._fleet.>`; `aether fleet` aggregates a view of every lord across the bus, cluster, or leaf sites (opt-in, mechanism not domain; supervision stays node-local) - see [Observability](#observability) |
| **Durable mailbox** | ✅ | `durable=true` -> casts survive a thrall crash (JetStream). TS + Python + Go. What survives a *restart*: see [Durability](#durability) |
| **Event-sourced rebuild** | ✅ | `event_log=true` -> `Append` events to a retention log, `Rebuild` state from it in init - **state survives a restart** by replaying the log, not a snapshot. See [Event-sourced rebuild](#event-sourced-rebuild) |
| **External NATS** | ✅ | `mode="external"` is purely a config switch - the same stack against a real cluster |
| **Embedded leaf spoke** | ✅ | `[nats.leaf]` -> the embedded bus joins a hub as a leaf node, bound into its site's account with its own JetStream domain; a single-binary site, no spoke NATS config - see [Multi-node](#multi-node-and-isolation) |
| **Secured bus** | ✅ | `[nats.security]` -> a networked embedded bind with server TLS + mandatory nkey; one shared identity or three least-privilege roles (lord/thrall/operator) whose permissions keep `aether._lord.>` node-local; credential rotation without downtime via `SIGHUP` - see [Security](#security) |
| **Singleton** | ✅ | `scope="singleton"` -> a distributed KV-CAS lock: **at most one _live_ instance within the app** (overlap bounded by the lock TTL), not a strict single-writer - see [Singleton fencing](#singleton-fencing-liveness-not-write-exclusivity) |
| **Dynamic supervisor** | ✅ | `ctx.StartChild(spec)` / `ctx.StopChild(name)` -> spawn/stop thralls at runtime, supervised one_for_one, outside manifest groups; idempotent on name |
| **HTTP edge (ingress)** | ✅ | `[[edge.http]]` -> a built-in HTTP server maps routes to a thrall op (call/cast) with no code, supervised as a singleton thrall - see [HTTP edge](#http-edge-ingress) |
| **Let it crash** | ✅ | `Escalate(reason)` from a handler -> typed OTP crash: the thrall exits abnormally and the lord restarts it through `init` per policy, no manual `panic`/`os.Exit`. Go + TS + Python - see [A thrall in TS](#a-thrall-in-ts-example) |
| **Idempotence** | ✅ | opt-in per thrall -> dedup a call/cast by an idempotency key: a duplicate cast is skipped, a duplicate call returns the first reply. In-memory (the thrall's lifetime), complements the event-log command-key. Go + TS + Python - see [Durability](#durability) |

Restart policy per thrall: `permanent` / `transient` / `temporary`.

Dynamic children live only in the running lord and do **not** survive a lord restart by
design - re-establishing them is the owner's job, not the lord's. Because `StartChild` is
idempotent on name, a supervising thrall can re-spawn its children blindly from its own
`init` (and re-apply on demand) with no duplicates. Runnable demo:
`examples/dynamic-topology/` (Go/TS/Python); rationale in `DESIGN.md` section 12.

## Layout

```
cmd/aether/           CLI: up | ps | events | fleet | cast | call
internal/
  ether/              embedded NATS / external connection (mode switch, incl. [nats.leaf], [nats.security])
  lord/               supervisor: manifest, supervisor loop, restart strategies,
                      graceful drain, durable stream provisioning, singleton lock
  registry/           JetStream KV registry (name -> pid/status)
  singleton/          distributed lock over KV (Create/CAS) - one instance within the app
  fleet/              fleet health payload + aggregator (network-wide runtime view across lords)
  obs/                structured logging + the metric registry (Prometheus exposition)
  soak/               bounded latency/leak metric primitives for the soak suite
  wire/               envelope + subject/stream conventions (Go side, shared with the SDKs)
sdk/ts/               @hamicek/aether (Bun/TS): defThrall/start + defFSM/startFSM + defEvent/startEvent, call, cast
sdk/python/           aether.py: def_thrall/start/run + FSM/start_fsm/run_fsm + def_event/start_event/run_event
sdk/go/thrall/        thrall.Def[S]/Start (GenServer) + thrall.FSM[D]/StartFSM (state machine) + thrall.EventManager/StartEvent (event manager)
examples/counter/     counter (TS/Py/Go) + gateway + a manifest per scenario
examples/fsm/         state-machine (FSM) behaviour demo - a turnstile
examples/eventbus/    event-manager (gen_event) behaviour demo - one event, many handlers
examples/eventsourced/ event-sourced rebuild demo - state that survives a restart
examples/dynamic-topology/ dynamic children re-established by their owner after a lord restart
examples/webserver/   built-in HTTP edge (ingress) - routes mapped to a thrall op, no code
examples/webserver-custom/ custom edge via the SDK helper (StartEdge), alongside the ingress
examples/live-dashboard/ live push to the browser over SSE, subject-scoped per client
examples/tracing/     trace propagation across call/cast hops
examples/fencing-token/ write-side fencing token (ctx.SingletonEpoch) against a resource
examples/hub-spoke-spike/ multi-node hub + isolated sites (leaf nodes) - see Multi-node below
examples/nats-leaf/    embedded spoke joined to a hub via [nats.leaf] - single-binary site
scripts/soak.sh       run the soak/chaos suite (out of CI)
scripts/durable-perf.sh durable cast drain throughput harness (out of CI) - examples/durable-perf/
```

## Subject convention

```
aether.<app>.<name>.call     # request/reply (call)
aether.<app>.<name>.cast     # fire-and-forget (cast); for durable thralls a JetStream stream captures it
aether.<app>.<name>.info     # out-of-band (timers, notifications)
aether._lord.<name>.ctl      # lord -> thrall (drain / shutdown / ping)
aether._lord.<name>.hb       # thrall -> lord (heartbeat)
aether._lord.events          # lifecycle stream (spawned/ready/down/restarting/...)
aether._fleet.<app>.<lord>   # curated fleet health summary a lord publishes about itself (opt-in)
aether_<app>_<name>          # JetStream stream for the durable mailbox (dots -> underscores)
```

## CLI

```bash
aether up -f <manifest>          # bring up the ether + the lord per the manifest
aether ps [--url <nats>]         # a table of thrall status from the KV registry
aether events [--url <nats>]     # the live lifecycle stream
aether fleet [--url <nats>] [--watch]  # network-wide fleet health across lords (opt-in)
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

**Let it crash.** A returned/thrown error means "this request failed, but I'm fine" (the caller gets
an error reply, the thrall keeps its state). To ask for the OTP *let it crash* instead, escalate -
the thrall exits abnormally and the lord restarts it through `init` per its restart policy, no manual
`panic`/`os.Exit` in your handler. A call caller gets a distinguishable `"escalated"` error reply
before the crash rather than waiting out its timeout.

```ts
import { escalate } from "@hamicek/aether";
handleCast: { withdraw: (amt, s) => { if (amt > s) escalate("balance underflow"); return s - amt; } }
```
```go
return s, thrall.Escalate("balance underflow")     // Go
```
```python
raise aether.Escalate("balance underflow")         # Python
```

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

## Event manager (gen_event) behaviour

The third behaviour is an **event manager** - aether's analogue of OTP's `gen_event`. Instead of
one state and one handler, you register several named **handlers**; one incoming event (an async
`cast` to the manager's name) is dispatched to **every** handler, in registration order, on the
same serialized mailbox - so handlers see events in a stable order and each keeps its own state.
That is what raw NATS fan-out (N independent subscribers) does not give: co-located, ordered
handlers. A handler that throws is logged and skipped, so the others still run.

```ts
import { defEvent, startEvent } from "@hamicek/aether";

const alarms = defEvent({
  name: "alarms",
  handlers: {
    audit: {                                     // counts every alarm in its own state
      init: () => ({ count: 0 }),
      handleEvent: (ev, s: { count: number }, ctx) => {
        const count = s.count + 1;
        ctx.log.info("alarm audited", { op: ev.op, total: count });
        return { count };
      },
    },
    pager: {                                     // reacts only to a hot temperature
      init: () => ({}),
      handleEvent: (ev, s, ctx) => {
        const p = (ev.payload ?? {}) as { celsius?: number };
        if (ev.op === "temp_high" && (p.celsius ?? 0) >= 80) ctx.log.warn("would page on-call");
        return s;
      },
    },
  },
});

await startEvent(alarms);
```

Events are async: a `call` to an event manager is answered with an error rather than a silent
timeout (synchronous events and runtime add/remove of handlers are deliberate follow-ups). Mirrored
in all three SDKs (`startEvent`/`defEvent` in TS, `start_event`/`def_event` in Python,
`StartEvent`/`EventManager` in Go). Runnable demo: `examples/eventbus/`.

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
# fencing = false                        # opt out of lord-liveness fencing (see below)
```

By default every thrall self-terminates when it can no longer verify its lord's lease, so no
thrall outlives its lord. On a shared external bus a KV hiccup then reaps the whole tree. Set
`fencing = false` on a thrall whose orphan is harmless (a stateless / read-only poller) so a bus
blip does not take it down - it then **may** outlive its lord, so use it only for thralls that own
nothing. Singleton fencing (one instance within the app) is separate and stays on.

**One lord per app** is enforced: a second lord starting for an app another lord is already
running refuses to start (rather than stomp its lease and crash-loop the tree). Run one lord per
app; for many isolated sites, give each its own node and app (leaf-node isolation), not two lords
racing on one bus.

## HTTP edge (ingress)

An **edge** is a process whose input arrives from *outside* the ether (a push - HTTP) rather than
from a mailbox (a pull). A `[[edge.http]]` section maps HTTP routes to a thrall operation with **no
code** - the lord runs a built-in `aether _edge` server for it, supervised as a singleton thrall
(heartbeat, restart, drain, fencing):

```toml
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /value"      = { thrall = "counter", op = "get", kind = "call" }   # waits for the reply -> body
route."POST /increment" = { thrall = "counter", op = "inc", kind = "cast" }   # fire-and-forget -> 202

[[edge.http]]                            # multiple servers, each on its own port, in one manifest
name = "admin"
addr = ":8081"
route."POST /decrement" = { thrall = "counter", op = "dec", kind = "cast" }
```

A `call` route returns the thrall's reply as the body (200); a `cast` route returns 202. Errors map
deterministically: application error -> 502, no responders -> 503, reply timeout -> 504, unknown
route -> 404. A real port is held by one active instance (singleton); aether does no load balancing -
put a reverse proxy in front to scale a single port. Runnable demo: `examples/webserver/`.

**Custom edge (you write the code).** When a route cannot be expressed as configuration - custom auth,
transformation, a non-HTTP protocol (a SCADA driver, cron, tail) - write the edge yourself via
`StartEdge` / `startEdge` / `start_edge` (**Go, TS and Python SDK**): you supply a run-loop that owns the
socket and a graceful-stop hook, and get heartbeat/restart/drain/fencing for free. A custom edge is an
ordinary `[[thrall]]` with a `cmd`, so it coexists with the built-in ingress in one manifest - and a TS or
Python edge is indistinguishable from a Go edge under the same lord. Demos: `examples/webserver-custom/`
(`main.go`, `edge.ts`, `edge.py`). `SSEStream` (live push) has the same full Go+TS+Python parity - edge is
polyglot across all three SDKs. Design: [DESIGN.md §12b](./DESIGN.md).

```go
thrall.StartEdge(thrall.EdgeDef{
    Init: func(ctx *thrall.Ctx) error { /* build the http.Server */ return nil },
    Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            return err // a real error -> abnormal exit -> the lord restarts; ErrServerClosed is a clean drain
        }
        return nil
    },
    Stop: func() { srv.Shutdown(context.Background()) }, // graceful hook on drain
})
```

**Live push to the browser (SSE).** The reverse flow - ether -> browser. `thrall.SSEStream` lets an edge
push events out over Server-Sent Events, scoped per client: your handler authorizes the request and
passes the authorized subject scope to `ServeClient`, which gives that connection its own NATS
subscription - so NATS never delivers a client an out-of-scope event. Backpressure drops for a slow
client; `Close` drains live connections. Read-only (control goes through ingress); reconnect is the
browser's `EventSource` retry with no server-side replay. Demo: `examples/live-dashboard/` (two clients,
each streaming only its own site).

```go
stream := thrall.NewSSEStream(ctx)
mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
    scope := authorize(r)                 // your code: verify a token -> subject scope
    if scope == nil { http.Error(w, "unauthorized", 401); return }
    stream.ServeClient(w, r, scope...)    // holds the SSE connection, forwards only that scope
})
// on drain: stream.Close(); srv.Shutdown(ctx)
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
event_log_use = "rebuild"       # or "audit"; declares what the log is for - see the caveat below
# event_log_max_msgs = 100000   # optional retention bounds - rejected with "rebuild"
# event_log_max_age_ms = 604800000
```

> **Retention vs rebuild.** `Rebuild` folds the *whole* log, and there is no snapshot or
> compaction (yet). So a retention bound silently truncates the rebuilt state once it purges old
> events - safe for an audit-only log, wrong as a rebuild source. Declare the intent with
> `event_log_use`: `"rebuild"` + a bound is a **config error** (`aether up` fails, telling you to
> drop the bound or switch to `"audit"`); `"audit"` + a bound is fine and silent; leaving it unset
> keeps the old behaviour - a bound just logs a startup warning. Leave the log unbounded if you
> rebuild from it.

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

Replay is ordered, and complete as long as retention keeps the whole log (single-writer = append
order). Because the mailbox is at-least-once, **the fold must be idempotent** (an event may be
replayed). A *non-idempotent* event (a signed delta, not "set to X") needs more: pass a dedup key
to `Append` - typically `ctx.MsgID`, the id of the message being handled - so a redelivered
command does not write the event twice (within the stream's duplicate window). See the
**command-key pattern** in [DESIGN §13c](./DESIGN.md). With a persistent JetStream (`store_dir` or
external), the rebuilt state survives stopping and starting `aether up`. Mirrored in all three
SDKs (`ctx.append` + `rebuild` in TS/Python). Snapshots and compaction are future work. Runnable
demo: `examples/eventsourced/` (a bank account whose balance survives a restart).

The command-key dedups the *event-log write*; the handler still runs on every delivery. To
deduplicate the **processing** too, mark the thrall `Idempotent` (`idempotent` / `idempotent=True`):
a call/cast is deduplicated by an idempotency key - a stable key the caller supplies
(`WithIdempotencyKey` / `opts.idempotencyKey` / `idempotency_key=`), else the envelope id - so a
duplicate cast is skipped and a duplicate call returns the first reply from a cache. It is opt-in,
bounded, and **in-memory** (it holds for the thrall's lifetime, not across a restart), which makes it
complementary to the command-key: handler dedup stops double-processing while the thrall is alive,
the event log keeps the write single even across a restart. All three SDKs; see [DESIGN §13c](./DESIGN.md).

## Singleton fencing (liveness, not write-exclusivity)

A `scope="singleton"` thrall holds a KV-CAS lock, and if it loses that lock (a new holder took it,
or it cannot reach the KV past the lease) it **self-terminates**. That bounds the window in which
two instances are *alive* to the lock TTL - which is what the soak suite measures, and what
lord/orphan failover needs.

It is **not** a strict single-writer guarantee. A lease plus self-termination can never be, per
[Kleppmann's fencing argument](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
between a lease expiring (a new holder starting) and the old, e.g. GC-paused, instance *noticing*
and exiting, the old instance can still issue a write. The guarantee is "**at most one live
instance, overlap ≤ lock TTL**", not "exactly one writer".

If you need strict single-writer against a resource (a PLC, a driver, a DB), enforce a **fencing
token** at that resource: the lord stamps a monotonic epoch into the lock, and a handler reads it
as `ctx.SingletonEpoch` (`ctx.singletonEpoch` / `ctx.singleton_epoch`, 0 for a non-singleton).
Send it with every write and have the resource reject a lower epoch, so a stale writer is fenced
out even if it has not yet self-terminated. Runnable demo in all three languages:
[`examples/fencing-token/`](./examples/fencing-token/).

## Multi-node and isolation

One lord runs one app on one NATS node - that boundary is deliberate ([one lord per
app](#manifest-example) is enforced at startup). To run
across many machines or sites, you do **not** point many lords at one bus; you give each node its
own lord, its own app, and its own NATS **leaf node with its own account**. A central hub connects
the leaves and imports only the **data plane** (`aether.<app>.<name>.*`); each site's supervision
and KV fencing (`aether._lord.*`) stay **node-local** and invisible to the others. So sites that
must not see each other simply do not - isolation is a property of the NATS topology, not something
the lord enforces per message.

This keeps the model coherent: the lord stays a single-node supervisor, cross-node reach is a
NATS-layer concern, and a "global" single instance is the one node that owns that responsibility -
not a lock several lords contend for (which does not work; see the fencing section). Full design:
[DESIGN.md §11b](./DESIGN.md). Runnable spike: [`examples/hub-spoke-spike/`](./examples/hub-spoke-spike/)
(a hub + two isolated sites, asserting cross-node reach and negative isolation).

**Embedded spoke (`[nats.leaf]`).** For the common spoke - one node, one site, one hub link - a
site deploys as a single binary: `mode = "embedded"` plus a `[nats.leaf]` section makes the embedded
bus a leaf of the hub, bound into the site's account with its own JetStream domain, with no
hand-written spoke NATS config. Only the hub stays operator-authored (bring-your-own). Runnable demo:
[`examples/nats-leaf/`](./examples/nats-leaf/); asserted in `internal/ether/leaf_e2e_test.go`.

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
dashboard    = true               # also serve the read-only observer dashboard at / (off by default)
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
| `aether_thrall_rss_bytes{name}` | gauge | thrall resident memory, summed over its process group |
| `aether_thrall_cpu_percent{name}` | gauge | thrall CPU usage %, a delta over the sample interval, summed over its process group |

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

**Observer dashboard.** A read-only web view of the supervision tree, aether's analogue of
Erlang's `observer` / Phoenix LiveDashboard. Enable `dashboard = true` (it shares the `/metrics`
server, so it needs `metrics_addr`) and open `http://127.0.0.1:7391/`. It shows the live tree -
each thrall's status (`starting`/`ready`/`down`/`stale`), scope, restart policy, `durable`/
`event_log` flags and self-metrics (memory/CPU, mailbox depth/latency, processed, durable backlog, restarts) -
and a live event feed pushed over SSE from the lifecycle stream, so the tree updates within ~1s of
a spawn/crash/restart without a page refresh. It is a consumer of signals the lord already holds
(no SDK change) and works the same embedded or external. It deliberately does **not** chart metrics
over time - that stays the domain of Prometheus/Grafana, which `/metrics` feeds - and it is
read-only (no control actions). The page is self-contained (no external assets), so it works
offline. Like `/metrics`, it binds wherever `metrics_addr` says (default a loopback address);
exposing it on a network needs authentication (a follow-up).

**Fleet health (across lords).** The observer dashboard is per-lord; for a view of **every lord on
the bus**, opt in with `fleet_health = true` and each lord publishes a curated summary of itself on
`aether._fleet.<app>.<lord_id>`. `aether fleet [--watch]` (a reusable aggregator) assembles the whole
fleet from those summaries - which lords are live or stale, their thralls and states - from one
place. It is deliberately a separate subject from supervision: `_fleet` is a curated summary meant to
be seen across lords, whereas the raw `aether._lord.>` channels are node-local. The payload is a JSON
contract, so an application dashboard can consume it in any language; a *domain* dashboard
(tags/alarms) is an application thrall that reads this feed as one input, not part of aether.

In a single account (one bus, or an external cluster's single account) the aggregator sees every
lord directly. **Across a leaf boundary** (a hub seeing its isolated spokes) the summary crosses as a
**stream export**: a `[nats.leaf]` spoke exports `aether._fleet.<app>.>` automatically, and the hub's
center account imports it - operator-authored, exactly like the data-plane import - so `aether fleet`
run against the hub shows every site. Supervision (`aether._lord.>`) is still never exported, so
isolation holds: only the curated summary crosses, and only where the hub imports it.

```
# hub NATS config (operator-authored): the center account imports each site's fleet health
HUB    { imports: [ { stream: { account: SITE_A, subject: "aether._fleet.sitea.>" } } ] }
SITE_A { exports: [ { stream: "aether._fleet.sitea.>" } ] }   # matches the spoke's own export
```

```toml
[observability]
fleet_health             = true    # publish this lord's health summary (off by default)
fleet_health_interval_ms = 5000    # publish cadence (default 5000)
```

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
- `aether-singleton.toml` - singleton scope: one instance within the app, via a KV-CAS lock (external NATS)
- `aether-one-for-all.toml` - the one_for_all supervision strategy
- `aether-rest-for-one.toml` - the rest_for_one supervision strategy
- `aether-secure-external.toml` - external cluster over TLS with nkey auth
- `aether-docker.toml` - Go-only, embedded NATS; the manifest the Docker image runs by default
- `aether-docker-external.toml` - Go-only against an external NATS (the `docker-compose.yml` demo)

## Deployment (Docker)

aether runs in a container: the runtime is a single Go binary, and thralls are just `cmd`. The
repo ships a **Go image** (the runtime plus the Go counter thrall) and, below, a recipe for a
polyglot image.

```bash
# build and run the canonical Go image (runs aether-docker.toml on an embedded NATS)
docker build -t aether .
docker run --rm --name aether aether

# from another shell: drive the demo thrall
docker exec aether /app/bin/aether ps
docker exec aether /app/bin/aether call counter-go get     # -> 0
docker exec aether /app/bin/aether cast counter-go inc
docker exec aether /app/bin/aether call counter-go get     # -> 1

docker stop aether     # SIGTERM -> the lord drains, the container exits 0 within Docker's window
```

Things the image already handles, and the knobs you may want:

- **PID 1 reaping.** A thrall runs as `sh -c <cmd>` in its own process group; the lord only waits
  on its direct children, so an orphaned grandchild reparents to PID 1. The image bakes
  [`tini`](https://github.com/krallin/tini) as the entrypoint to reap it. If you build your own
  image without tini, run it with `docker run --init`.
- **Graceful shutdown.** The lord handles SIGINT/SIGTERM, so `docker stop` triggers a drain and the
  container exits cleanly well inside Docker's default 10 s grace.
- **Persistence.** The embedded JetStream is in-container by default. To keep durable mailboxes /
  event logs across a container restart, set `store_dir` in the manifest and mount a volume there
  (e.g. `-v aether-data:/app/data` with `store_dir = "/app/data"`).
- **Metrics.** The RSS/CPU dashboard (`/metrics`) shells out to `ps`, which the image provides via
  `procps`. On a slimmed-down base (distroless/scratch) those metrics degrade gracefully - a skipped
  sample, not a crash - so drop `procps` if you want a smaller image and do not need per-thrall RSS/CPU.

### Multi-node

A lord does not spawn processes into other containers, so distribution is **one lord per
container/node over a shared external NATS**, not many lords on one bus. `examples/counter/docker-compose.yml`
shows the shape - a `nats` service (JetStream on a named volume) plus an `aether` service pointed at
it:

```bash
cd examples/counter
docker compose up --build      # nats + one aether lord (counter-go) over nats://nats:4222
docker compose down            # keeps the js-data volume; add -v to wipe it
```

Scale by adding more `aether` services, **each with its own `app`** (its own slice of thralls).
Note: `docker compose up --scale aether=N` does **not** work - it would start N lords for the *same*
app, and one-lord-per-app (§14) makes all but the first refuse to start. That is correct behavior,
just not what `--scale` implies: give each replica a distinct `app` (and manifest) instead.

### Polyglot image (recipe)

Because thralls are `cmd`, a TS/Python deployment builds its own image on top of the same idea:
start from a base that carries the interpreters your thralls need (Go for the runtime, plus Bun
and/or a Python venv), install the SDK dependencies, and point the entrypoint at
`aether up -f <your-manifest>`. A ready-made polyglot Dockerfile is deliberately deferred until a
real deployment needs it (tracked in the backlog); the Go image above is the supported default.

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

**Securing the embedded server (`[nats.security]`).** To expose the *embedded* bus on the network
(instead of running against an external NATS), add a `[nats.security]` block: it binds the given
address with server-side TLS and requires nkey authentication. Absent, the server stays on
`127.0.0.1` with no auth, exactly as before - existing manifests are unaffected.

```toml
[nats]
mode = "embedded"

[nats.security]
listen    = "0.0.0.0:4222"                # network bind (absent = loopback, no auth)
tls_cert  = "/etc/aether/server.pem"      # server certificate + key (server-side TLS)
tls_key   = "/etc/aether/server-key.pem"
ca        = "/etc/aether/ca.pem"          # clients verify the server against it
nkey_seed = "/etc/aether/node.nk"         # the identity authorized to connect
```

The lord authenticates its own connection and injects the CA and seed into every thrall and the
operator endpoint, so the whole node speaks the same secured way. `nkey_seed` above is the **simple
tier**: one shared identity with full rights - encrypted and authenticated, but no role separation.

**Least-privilege tier (per-role identities).** For real least privilege, replace `nkey_seed` with
one seed per role - all three together, mutually exclusive with `nkey_seed`:

```toml
[nats.security]
listen        = "0.0.0.0:4222"
tls_cert      = "/etc/aether/server.pem"
tls_key       = "/etc/aether/server-key.pem"
ca            = "/etc/aether/ca.pem"
lord_nkey     = "/etc/aether/lord.nk"      # the supervisor: full rights
thrall_nkey   = "/etc/aether/thrall.nk"    # workers: data plane, but not supervision control
operator_nkey = "/etc/aether/operator.nk"  # CLI / dashboard: call/cast and observe, not control
```

The server then enforces a role-scoped permission set: a thrall cannot command sibling thralls,
forge lifecycle events, or write the fencing leases it only reads; an operator cannot drive
supervision; only the lord may use `aether._lord.>`. This is what makes `aether._lord.>` node-local
**by permission** on a networked bus, not merely by non-export. The lord connects as the lord,
injects the thrall identity into thralls, and the operator endpoint carries the operator identity. A
reference manifest is [`examples/counter/aether-secure-embedded.toml`](./examples/counter/aether-secure-embedded.toml).

Two honest limits: the permissions are **deny-based** (allow all, subtract the dangerous subjects),
chosen so JetStream and KV keep working - it is not a strict allow-list. And a single shared thrall
identity does **not** isolate one thrall from another (name-scoped channels stay open across
thralls); the roles isolate the lord / thrall / operator boundaries, not thrall-vs-thrall.

**Rotating credentials without downtime.** To renew a certificate or a key on a running node, replace
the file in place (the same path the manifest points at) and send the process **`SIGHUP`**; the
embedded server reloads the new credentials without a restart. A **TLS cert/key** rotation keeps live
connections up (they keep their negotiated session; new connections get the new cert - clients must
then trust the new CA). An **nkey** rotation applies the new authorization: the new key connects and
the old one is rejected; a connection still using the removed key is closed and reconnects with the
new key from its seed file - a brief drop. For that reason, rotating the **lord's own** key disrupts
the lord's system connection for that window, so a lord-key change is better done with a restart.
**Structural changes** (a different `listen`, adding or removing a role) are not a reload - they need
a node restart.

Scope: `[nats.tls]` / `[nats.auth]` secure the **client** side against an already-secured external
NATS (including the operator CLI, `--ca`/`--nkey`); `[nats.security]` secures the **embedded server**
itself for a networked bind, with either one shared identity or three least-privilege roles, and its
certificates and keys rotate without downtime (`SIGHUP`). Still open: mutual TLS - tracked in
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
- **Orphan reaping** - a lord is `SIGKILL`ed, orphaning its thralls; bar: each orphan self-reaps
  within the lease once it can no longer verify the lord (fencing - a *liveness* bound, not
  write-exclusivity; see [Singleton fencing](#singleton-fencing-liveness-not-write-exclusivity)).
  (The former two-lords-race-a-singleton bar is retired: one lord per app is now enforced, and that
  topology never provided failover - the loser's instance mutually reaps.)
- **Graceful drain** - a durable thrall is drained mid-stream and restarted; bar: no work lost.

It ends with a structured report and a **non-zero exit on any bar breach**.

## Deliberately deferred

The runtime has conscious gaps, tracked in [ROADMAP.md](./ROADMAP.md): liveness beyond heartbeats
(`$SYS` events), `temporary` semantics inside group strategies, and thrall state persistence
(today durability covers the mailbox, not the state - see [Durability](#durability); event-sourced
rebuild covers the rest). Thrall-level fencing for orphaned singletons - once listed here - is **done** (both singleton
and lord-liveness fencing; see [Singleton fencing](#singleton-fencing-liveness-not-write-exclusivity)).
