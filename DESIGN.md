# aether - architecture sketch

> A polyglot distributed actor/OTP runtime where the substrate is **NATS** and "processes" are
> real **OS processes**. The goal: an SDK that makes it very easy to write **thralls** (genservers)
> and run a **lord** (supervisor). Not BEAM-scale (millions of processes), but **tens** of
> processes that communicate reliably.

Status: implemented core, actively evolving. Last updated 2026-08-08.

## Glossary

The project has its own vocabulary (the "ether" metaphor from the Necroscope universe), under which
sits entirely sober OTP semantics:

| term | meaning | OTP counterpart |
|---|---|---|
| **aether** | the project / runtime name | - |
| **ether** | the bus - embedded NATS or an external cluster | message transport |
| **lord** | the supervisor - spawns and watches thralls, restarts them per policy | Supervisor |
| **thrall** | the genserver - state + serialized message processing | GenServer |

Throughout this document "lord/thrall/ether" refer to our components and "GenServer/Supervisor" only
where OTP is referenced as the model.

---

## 1. The idea in one sentence

Take OTP semantics (GenServer / Supervisor / Registry), but instead of an in-process runtime put it
on a **message broker (NATS)** and let the "processes" be **language-independent OS processes**. The
lord sits above them as an OS-process manager - a little different from Elixir, but with OTP restart
and supervision-tree semantics.

It is not a competitor to the BEAM. It is **OTP semantics laid on NATS**, deliberately bounded to
tens of processes.

---

## 2. Mapping OTP to NATS

A large part of the OTP concepts have a clean counterpart in NATS - that is the core reason the
design makes sense:

| OTP concept | NATS primitive |
|---|---|
| `call` (sync request/reply) | NATS **request/reply** - built-in timeout, ephemeral `_INBOX` |
| `cast` (fire-and-forget) | core **publish** (ephemeral) |
| Durable mailbox / at-least-once | **JetStream** stream + durable consumer |
| Registry (named thralls) | subject convention + a **KV bucket** name -> node/pid/status |
| Worker pool (poolboy) | **queue group** - load balancing for free |
| Liveness / heartbeat | KV with TTL + a heartbeat subject + `$SYS` connection events |
| State persistence | JetStream **KV / Object store** |
| Observer / dashboard | subject tapping + `$SYS` events |

---

## 3. Runtime: Go + embedded NATS

The runtime = **a single Go binary** that is **both broker and lord**. NATS can be used as a Go
library (`server.NewServer(...)`, run in-process).

```
aether up  ==  ether (embedded NATS)  +  lord (reads aether.toml)  +  KV registry
```

**Why Go:** a static binary, cross-compilation, first-class process management (`os/exec`, signals,
process groups), and an embedded NATS with no external dependency.

**What this buys you (elegance of the mechanism):**
1. The lord sits *on* the bus, not beside it -> a "smart lord" (see §5) is cheap.
2. Observability almost for free (every message flows through a broker that is your own code).
3. One artifact: `aether up` = ether + lord + registry. That is the differentiation against a
   sidecar model (no sidecar, no mandatory k8s).

**What Go + embedded NATS does NOT solve automatically** (these are semantic decisions, not language ones):
- **Dual-liveness** - the broker knows "the PID is alive" + "the subscription is active" (2 of 3
  signals for free), but "the handler is actually responsive" still needs an application heartbeat.
- **Mailbox durability** - core vs JetStream, and what survives a restart (see §13).
- **Idempotence / state persistence** - language-neutral contracts.

The elegance is real, but it is elegance of the *mechanism*, not the *semantics*.

---

## 4. Topology

```
+-------------------------------------------------------------+
|  aether (Go binary) = "runtime"                             |
|                                                             |
|   +--------------+         +---------------------------+    |
|   |  Lord        |<------->|  Ether (embedded NATS)    |    |
|   |  (reads      | in-proc |  (subjects, req/reply,    |    |
|   |  aether.toml)|         |   KV registry, JetStream) |    |
|   +------+-------+         +-------------^-------------+    |
|          | fork+exec                     | NATS (JSON)      |
+----------+-------------------------------+------------------+
           |                               |
     +-----v-----+   +-----------+   +------v-----+
     | counter   |   | gateway   |   | ingest x3  |
     | (Bun/TS)  |   | (Bun/TS)  |   | (queue grp)|
     | thrall    |   | thrall    |   | thrall     |
     +-----------+   +-----------+   +------------+
```

The lord spawns thralls as OS processes and injects the env `AETHER_NATS_URL`, `AETHER_APP`,
`AETHER_NAME`. Through the SDK a thrall connects, subscribes to its subjects, and starts beating a
heartbeat.

**The key principle for future scaling: the SDK never talks to the lord directly - only to the ether
(NATS).** The lord and the runtime are invisible to a thrall. That is what makes the embedded ->
external transition (§10) a configuration change, not a rewrite.

---

## 5. Lord: variant A vs B

The question is how closely the lord talks to the thralls:

- **A) Dumb (a process manager only).** It starts a process, watches the PID/exit code, restarts per
  policy. It knows nothing more. The thrall connects, registers and beats a heartbeat on its own.
  Analogy: `systemd`.
- **B) Smart (coordinates the run over the ether).** In addition it actively speaks over NATS:
  *graceful drain* ("finish and stop"), *health beyond "alive"*, *coordinated restart* for
  `one_for_all`. Analogy: a conductor.

**Decision:** start with **A**, but leave a hole in the protocol for **B**. Concretely, keep the
heartbeat and graceful-drain message in the contract from the start (they are cheap and essential to
reliability). A purely dumb lord with no drain would throw away in-flight work on restart - which is
exactly the pain that drives people to OTP.

Restart policy: `permanent` (always) / `transient` (only on an abnormal exit) / `temporary` (never).
Strategies: `one_for_one` / `one_for_all` / `rest_for_one`.
Restart intensity: `max` restarts `within_ms` -> exceeding it escalates per the strategy.

---

## 6. Subject convention

```
aether.<app>.<name>.call     # target for a call (request/reply)
aether.<app>.<name>.cast     # target for a cast (fire-and-forget)
aether.<app>.<name>.info     # out-of-band messages (timers, notifications)
aether._lord.<name>.ctl      # lord -> thrall (drain / shutdown)
aether._lord.<name>.hb       # thrall -> lord (heartbeat)
aether._lord.events          # lifecycle events (spawned/ready/down/restarting) for a dashboard
aether_<app>_<name>          # JetStream stream for the durable mailbox (dots become underscores)
```

A worker pool = several thralls with the same `name` in a **queue group** on `...call`/`...cast` ->
NATS load-balances. The reply subject is not handled by hand - NATS request/reply keeps an ephemeral
`_INBOX.*` itself.

---

## 7. Wire envelope (JSON)

One envelope for everything; `kind` distinguishes the type, `op` is "which handler function". The
authoritative shape lives in `internal/wire/envelope.go`; the TS and Python SDKs mirror it.

**Call (request):**
```json
{
  "v": 1,
  "id": "abc-123",
  "kind": "call",
  "from": "gateway",
  "to": "counter",
  "op": "get",
  "payload": {},
  "ts": 1730000000000
}
```

**Reply (on `_INBOX`, correlated via `id`):**
```json
{ "v": 1, "id": "abc-123", "kind": "reply", "status": "ok", "payload": 2 }
```

**Error reply:**
```json
{
  "v": 1, "id": "abc-123", "kind": "reply", "status": "error",
  "error": { "type": "handler_error", "message": "key not found", "retryable": false }
}
```

**Cast (no reply):**
```json
{ "v": 1, "id": "abc-124", "kind": "cast", "to": "counter", "op": "inc", "payload": {} }
```

**Heartbeat (thrall -> lord):**
```json
{ "v": 1, "kind": "hb", "to": "counter", "ts": 1730000000000 }
```

**Control (lord -> thrall):**
```json
{ "v": 1, "kind": "ctl", "op": "drain" }
```
`op` is `drain` (finish and stop) or `shutdown` (stop now). A `ping`/health op is a future addition
(see the ROADMAP liveness item).

---

## 8. Counter thrall - TS/Bun SDK (home base)

The handler shapes hold GenServer semantics: `handleCall` returns `[reply, newState]`, `handleCast`
returns `newState`. Dispatch is through an op-keyed map.

```ts
// counter.ts
import { defThrall, start } from "@hamicek/aether";

const counter = defThrall<number>({
  name: "counter",

  init: () => 0,

  handleCall: {
    get: (_payload, state) => [state, state],          // [reply, newState]
  },

  handleCast: {
    inc: (_payload, state) => state + 1,               // newState
    dec: (_payload, state) => state - 1,
  },

  terminate: (reason, state) => console.log(`counter exiting (${reason}), last value = ${state}`),
});

await start(counter);
// The SDK reads AETHER_NATS_URL from the env, connects, subscribes to
// aether.<app>.counter.{call,cast,info} + aether._lord.counter.ctl,
// starts a heartbeat on aether._lord.counter.hb,
// and CRUCIALLY: serializes processing internally (one message at a time) -> GenServer semantics.
```

A client (`gateway.ts`) that calls the counter:

```ts
import { call, cast } from "@hamicek/aether";

await cast("counter", "inc");
await cast("counter", "inc");
const value = await call<number>("counter", "get", {}, { timeoutMs: 5000 }); // -> 2
```

---

## 9. Manifest - aether.toml

The separation: **thrall behavior = code** (in the SDK), **tree topology = a declarative manifest**.

```toml
app = "demo"
strategy = "one_for_one"                 # one_for_one | one_for_all | rest_for_one
restart_intensity = { max = 3, within_ms = 5000 }

[nats]
mode = "embedded"                        # embedded | external
# url = "nats://node-a:4222,nats://node-b:4222"   # for mode = "external"
# [nats.tls]  ca = "/etc/aether/ca.pem"           # server TLS (verify the server) - external
# [nats.auth] nkey_seed = "/etc/aether/user.nk"   # nkey authentication - external

[[thrall]]
name = "counter"
cmd  = "bun run ./counter.ts"
restart = "permanent"                    # permanent | transient | temporary
scope   = "local"                        # local | singleton  (see §12)

[[thrall]]
name = "gateway"
cmd  = "bun run ./gateway.ts"
restart = "permanent"

[[thrall]]
name = "ingest"
cmd  = "bun run ./ingest.ts"
restart = "transient"
replicas = 3                             # -> queue group, a pool of 3 workers
```

A programmatic API on top (a DynamicSupervisor, §12) for "add a thrall at runtime" is planned; the
default path is the manifest.

---

## 9b. Lord lifecycle (step by step)

1. `aether up` -> brings up embedded NATS (or connects to an external one), creates the KV bucket
   `aether_registry`.
2. Reads `aether.toml`. For each thrall, `fork+exec` with the injected env. Writes
   `name -> {pid, node, status:starting}` to KV.
3. The thrall (SDK) connects, subscribes, starts a heartbeat. The lord sees the heartbeat -> status
   `ready`, emits `ready` on `aether._lord.events`.
4. **The watch loop** watches the signals: the PID exit code and the heartbeat. An abnormal crash ->
   restart per policy + backoff, bounded by `restart_intensity` (exceeding it -> escalation per the
   `strategy`).
5. **Graceful shutdown** (on Ctrl-C / restart): `ctl:drain` -> grace period -> `SIGTERM` -> fallback
   `SIGKILL`. After a drain the thrall finishes the in-flight message, calls `terminate`, and
   disconnects.

---

## 9c. One `call` end to end

```
gateway.call("counter","get")
   |  publish -> aether.demo.counter.call   (reply-to: _INBOX.abc)
   v
[ether / embedded NATS] -- delivers --> counter (thrall)
                                          |  enqueue into the internal mailbox (serialization)
                                          |  handleCall.get(payload, state=2) -> [2, 2]
                                          v
                                 publish reply -> _INBOX.abc  { status:"ok", payload:2 }
   ^
gateway <-- the NATS request returns 2 (or a TimeoutError after timeoutMs)
```

---

## 10. Transition embedded -> connected node (external cluster)

Beyond running embedded, the runtime also runs against a connected NATS cluster. The design allows
it as a **config switch**, because the SDK talks only to the ether:

```toml
[nats]
mode = "external"
url  = "nats://node-a:4222,nats://node-b:4222,nats://node-c:4222"
```

The thralls do not change by a single line. Two things do shift - accounted for from the start:

- **The lord loses its "broker seat".** In external mode it is just another NATS client. ->
  Observability should not be built on the embedded privilege but on the NATS `$SYS` / system account
  (connect/disconnect/subscription events). Built on `$SYS` it works the same embedded and external.
  (This `$SYS`-based liveness is on the ROADMAP; today liveness is derived from heartbeats.)
- **A lord runs on every host** and starts its *local* thralls (holding to the rule "lord = a local
  process manager"). That raises the singleton question - see §12.

So the runtime has two modes: `embedded` (the default, "download and run" convenience) and `external`
(production, JetStream HA, scale).

---

## 11. Principle: do NOT hide NATS behind the thrall

The thrall layer (envelope, call/cast, mailbox serialization) is **convenience on top, not a prison.**
The SDK hands the thrall a live NATS connection in its context so it can reach anything - JetStream,
KV, the Object store, its own subjects:

```ts
const ingest = defThrall<State>({
  name: "ingest",
  init: async (ctx) => {
    const kv = await ctx.nats.jetstream().views.kv("cache");
    const js = ctx.nats.jetstream();
    return { kv, js, seen: 0 };
  },
  handleCast: {
    event: async (payload, state) => {
      await state.kv.put(payload.key, JSON.stringify(payload.value)); // durable
      await state.js.publish("audit.events", encode(payload));        // JetStream
      return { ...state, seen: state.seen + 1 };
    },
  },
});
```

Rule of thumb: **a thrall handles "state + serialized processing of the messages addressed to me".
Everything else in NATS is freely available to the thrall via `ctx.nats`.** A facade that locked NATS
away would be a mistake - it would take exactly the reliable things we go to NATS for.

---

## 12. Catalog of building blocks (besides the thrall)

**Almost for free (a thin wrapper over NATS), implemented today:**
- **Registry** -> NATS **KV** `name -> {node, pid, status}`. Cluster-wide with no extra work.
- **Lifecycle stream** -> `aether._lord.events` (spawned/ready/down/restarting/...), surfaced by the
  CLI `events`.
- **Singleton / global thrall** (the equivalent of Erlang `:global`). In a single node it is trivial;
  in a cluster the lord runs on several hosts and two could start the same `counter` -> two instances
  of the same state = a silent catastrophe. The solution: a **distributed lock over NATS KV with CAS**
  (compare-and-swap on the revision) - whoever acquires the key `singleton/<name>` may start; the
  others wait and take over on failure (failover). That is why the manifest has
  `scope = "local" | "singleton"`.
- **DynamicSupervisor** - starting and stopping thralls at runtime, not only from the manifest, via
  `ctx.StartChild(spec)` / `ctx.StopChild(name)` (Go SDK). Needed for "a driver per connection", a
  worker per request, or a workflow instance. The SDK sends a `ctl` request to the lord's inbound
  control channel (`aether._lord.ctl`); the lord spawns the OS process and supervises it exactly like
  a manifest child - restart per policy, registry status, lifecycle events. **Model:** a dynamic child
  is **local scope, supervised one_for_one, and outside any manifest group strategy** - its crash does
  not pull a `one_for_all` group and vice versa. `Stop` drains dynamic children alongside static ones.

  **Boundary (deliberate):** a dynamic child's topology lives only in the lord's memory, and the
  manifest is the source of truth on a lord restart, so dynamic children do **not** survive a lord
  restart. This is a decision, not a missing feature. The lord is an OS process supervisor: when it
  dies, the whole process group dies with it - including the thrall that spawned the child. Re-establishing
  the topology is the **owner's** responsibility, not the lord's:
  - The supervising thrall re-spawns its children from its own `init` (or `Rebuild`, §13c) when it comes
    back. `StartChild` is **idempotent on name** - a repeat spawn of a name already under supervision is
    a no-op, not an error - so the owner may call it blindly from `init` without risking a duplicate. (A
    name that collides with a manifest child is still refused: that is a genuine misconfiguration.)
  - A child with no such owner is not really parentless: if it must be permanent, it belongs in the
    manifest; if it is genuinely ephemeral (a driver per connection re-establishes itself anyway), losing
    it on restart is correct; if an external control plane owns it, that plane re-issues the spawn when it
    notices the child gone (via the registry / heartbeats).

  Making the lord a *second* source of truth (persisting spawn specs to KV and rehydrating them on start)
  was considered and rejected: it creates two owners of the same child - duplicates and stale records
  after a crash - the same inconsistency for which the in-memory state snapshot was rejected in favour of
  event-sourcing (§13c, "the log is truth, state is a projection"). KV topology persistence and lord
  rehydration are therefore out of scope.

**Own blocks, planned (not yet implemented), in priority order:**
1. **Task / work queue** -> cast + a **JetStream work-queue stream** + a queue group. A durable task
   queue, at-least-once, a worker pool, retry.
2. **gen_statem (state machine)** - a thrall variant with explicit states and transitions
   (`new -> paid -> shipped`).
3. **RateLimiter / Circuit breaker** - shared state in KV.

---

## 13. Durability model (what survives what)

Durability is easy to over-read. Two things must be kept apart:

- **Mailbox durability** - a durable thrall's *casts* are captured in a JetStream
  stream (`durable = true`), so a message survives even if the thrall crashes before
  handling it. This is a property of the **queue**.
- **State durability** - the thrall's in-memory state. aether does **not** snapshot it:
  like OTP, a restart runs a clean `init`. State is instead made durable by
  **event-sourcing** (§13c): "the log is truth, state is a projection".

So "durable" (the mailbox) always means *the message is not lost*, never *the state is
remembered*; remembering state is the separate job of the event log.

How long the mailbox survives then depends on **where JetStream stores it**, which is
a deployment choice, not a thrall concern:

| Mode | Storage | Thrall crash | Lord/process restart | Machine restart |
|---|---|---|---|---|
| **embedded, ephemeral** (default) | temp dir, removed on Stop | survives | **lost** | lost |
| **embedded, persistent** (`store_dir`) | fixed dir, file storage | survives | **survives** | survives (same host, same dir) |
| **external, persistent** | external NATS, file storage | survives | survives | survives (independent of the app host) |

```toml
# embedded, persistent: keep the durable mailbox across restarts, no external NATS
[nats]
mode = "embedded"
store_dir = "./.aether-store"
```

The default stays ephemeral: with no `store_dir`, the embedded server uses a temp dir
wiped on Stop - convenient for "download and run", but the durable mailbox does not
outlive the process. Set `store_dir` when a single-host deployment must keep the
mailbox across restarts. Choose **external NATS** when durability must be independent
of the application host (HA, storage managed separately, several lords sharing one
bus). `store_dir` is an embedded-only setting; in external mode the storage lives on
the cluster and the field is ignored.

No-loss claims about restarts are measured **server-side** via the stream backlog
(`StreamInfo().State.Msgs`), not via an in-process counter - the latter resets on
restart, and a WorkQueue stream drops messages once they are delivered and acked.

---

## 13c. Event-sourced state (rebuild from the log)

State that must survive a restart is made durable not by snapshotting the in-memory image
(that was deliberately rejected - a snapshot fights the model), but by **event-sourcing**: the
thrall appends domain events to a log and rebuilds its state by replaying that log in `init`.
"The log is truth, state is a projection." An in-memory snapshot would have to be kept in sync
with the events and could drift; replaying the log cannot.

The event log is a **separate retention stream** (`event_log = true`), NOT the mailbox. The
distinction is essential:

| | Mailbox (`durable`) | Event log (`event_log`) |
|---|---|---|
| Stream policy | WorkQueue (consumed on ack) | Limits (retained, replayable) |
| Holds | commands to process | the record of what happened |
| Replayable | no (acked messages are gone) | yes (from the beginning, in order) |
| Purpose | at-least-once delivery | rebuild state / audit trail |

The two are independent: a thrall may have a mailbox, an event log, both, or neither. The lord
provisions each opt-in stream. The SDK gives `Append` (a JetStream publish that waits for the
stream ack) and `Rebuild(ctx, initial, fold)` (an ordered `DeliverAll` replay into a fold),
callable from either behaviour's `init`. Because the mailbox is at-least-once and rebuild
replays, **the fold and handlers must be idempotent** - the same discipline the SCADA design
calls "idempotence from structure".

Bounded memory: the event log is bounded only by its configured retention (`event_log_max_msgs`
/ `event_log_max_age_ms`); snapshots and compaction (a state snapshot + replay-since) are
deliberately future work. Whether the rebuilt state survives a *machine* restart depends on the
same JetStream persistence as the mailbox (`store_dir` or external). This replaces the cancelled
in-memory state-snapshot approach.

---

## 13b. Observability (telemetry about the runtime itself)

The runtime reports on itself so a remote operator can diagnose "why isn't it working" without
being at the machine. Three layers, deliberately built to stay thin across the polyglot SDKs.

**Structured logs.** The lord and all three SDKs log through a small structured logger
(Go `log/slog`; minimal JSON/text loggers in TS and Python) configured from `AETHER_LOG_LEVEL`
and `AETHER_LOG_FORMAT`. The lord injects those into every thrall's environment, so the tree logs
consistently; records carry `component` / `app` / `name` to separate lord and thrall lines on a
shared stream.

**Metrics via a lord-side Prometheus endpoint.** The lord is the single aggregation point (it
already holds the registry, the lifecycle stream, restart windows and JetStream), so it exposes a
Prometheus `/metrics` HTTP endpoint (opt-in, `[observability] metrics_addr`). Crucially the SDKs do
**not** each carry a metrics client - that would fight the thin-SDK principle. Instead a thrall
attaches its self-metrics (mailbox depth, last handler latency, processed count) to the heartbeat
it already sends every 2s, and the lord folds them in alongside its own supervision counters
(thrall count by status, restarts, gave-ups, heartbeat misses) and the durable backlog it reads
from JetStream (`num_pending`). The endpoint is the lord's own HTTP server, independent of the
NATS mode, so it works the same embedded or external - satisfying "not built on the embedded
privilege" (see §9/§10) without depending on `$SYS`. The metric model is exporter-agnostic, leaving
room for an OTLP bridge later.

**Heartbeat miss detection.** Heartbeats previously only flipped a thrall to `ready`. A reaper now
tracks last-seen per thrall and marks one `stale` (with an event and a counter) when it stops
heart-beating - catching a hung process the OS-level exit watcher cannot see. A resumed heartbeat
flips it back to `ready`. The interval and the miss threshold are configurable (`[liveness]`): the
lord injects the interval into the thralls and derives its reaper threshold from the same value,
so a deployment can tighten detection without the two drifting. (Faster connection-loss detection
via NATS `$SYS` events is deferred - it earns its keep only in a networked topology, and requires
system-account access; the per-site model, where thralls connect to a local embedded NATS, is
already well covered by the exit watcher plus this reaper.)

**Tracing.** The envelope gained a `trace` correlation id, distinct from `id` (which pairs a
request with its reply). An edge mints it; `ctx.Call` / `ctx.Cast` propagate the current message's
trace to downstream messages; it is logged, so a log line and a trace can be joined and one logical
operation followed across process boundaries. Full OTLP tracing is deferred - this is log-based
correlation, which the exporter-agnostic model can later bridge.

---

## 14. Deliberately deferred (gaps in the design)

See [ROADMAP.md](./ROADMAP.md) for the maintained list. In short:

- **Liveness beyond heartbeats** - `$SYS` connection events as a supplement, so liveness works even
  outside the embedded-server case.
- ~~**Full thrall-level fencing for singletons**~~ - *implemented.* A singleton thrall now
  verifies its lock ownership itself, not only through its lord. The lord stamps a fencing epoch
  into the KV lock record at acquisition (the create revision, preserved across renewals) and
  injects it into the thrall (`AETHER_SINGLETON_*`). The thrall reads the lock key every TTL/3 and
  self-terminates the moment its epoch is superseded or the key is gone; if it cannot reach the KV
  at all (a partition), it self-terminates once the lock TTL (the lease) elapses without a
  confirmation. This bounds the window in which two instances could run to the lock TTL, even when
  the lord is dead or partitioned and cannot kill the orphan itself. Wired into both the GenServer
  and FSM start paths in all three SDKs. Proven by `TestSoakSingletonOrphanFencing` (kills only the
  lord process, leaving the probe orphaned, and asserts it reaps itself).
- ~~**Lord-liveness fencing for all thralls**~~ - *implemented (AE-031).* AE-013's "no thrall
  survives its lord" invariant held only for a graceful shutdown, where the lord actively kills its
  children's process groups (`cmd.Cancel` -> group SIGKILL). An external SIGKILL/crash of the lord
  skips that path, and because each thrall runs in its own process group it survives as an orphan -
  covered for singletons by the fencing above, but not for plain `local` thralls, which after a
  lord restart collide with the freshly spawned ones. So the lord now also establishes a per-app
  **liveness lease** in KV (bucket `aether_lords`, carrying a monotonic epoch = the write revision)
  and renews it on a fixed interval, independent of the (configurable, possibly disabled) heartbeat
  reaper. **Every** thrall - not just singletons - is injected with the epoch (`AETHER_LORD_*`) and
  verifies it every TTL/3. Two triggers end an orphan: (a) the **epoch is superseded** - a
  replacement lord stamped a higher one - which fires at once; (b) the lease is **gone or
  unreachable** - a `NotFound` (the key TTL-expired: the lord stopped renewing) also fires at once,
  while a *read error* (the KV cannot be reached at all) is tolerated until the lease window elapses,
  so a brief hiccup does not reap a thrall whose lord is fine. This covers both deployment modes: in
  **external** mode a dead lord stops renewing, the key expires, and a replacement (if any) supersedes
  the epoch; in **embedded** mode the bus lives *inside* the lord, so a lord crash takes the KV with
  it and the thrall reaps via the unreachable-past-lease path. Wired into all three SDKs and both the
  GenServer and FSM paths. Proven by `TestSoakLordDeathReapsOrphanThrall` (kills only the lord
  process and asserts the orphaned `local` thrall reaps itself, leaving one instance - the
  external-bus/epoch-superseded path) and, for the unreachable-bus path, by the SDK fencing unit
  tests (e.g. Go `TestFencingFiresAfterLeaseWhenUnreachable`).

  **Trade-offs, spelled out.** (1) *Wider blast radius:* generalizing from singletons to every thrall
  means a KV/bus outage self-terminates the *whole* app, not just singletons. The TTL bounds the
  *detection* latency, not the recovery: while the outage lasts, `permanent` thralls reap, restart,
  re-fence and reap again - a crash-loop for the duration - so the KV must be as available as the app
  needs to be. The grace is deliberately asymmetric: a genuinely-expired key (lord truly gone) reaps
  immediately, an unreachable KV is tolerated for the lease. (2) *One lord per app* is assumed, not
  enforced: the lease key is the app. Running two lords for one app is a misconfiguration, and under
  AE-031 it is a *worse* one than before - both write the same key with different epochs, so each
  lord's thralls see the other's epoch and mutually reap into a crash-loop, where previously they
  would merely have coexisted as harmless duplicates. A single writer must be ensured operationally
  (distinct apps per lord, or singletons for cross-node single-instance).
- **`temporary` semantics inside group strategies** - its interaction with `one_for_all` /
  `rest_for_one` is not fully specified.
- **Thrall state persistence** - durability today covers the *mailbox* (casts survive a crash via
  JetStream), not the *state*. Like OTP, a restart runs a clean `init` and loses in-memory state.
- **Monitoring / observability and long-running soak testing** - for high-reliability use.
- **Stronger and server-side security** - the client side authenticates to an *external* bus with
  nkeys over server TLS (manifest `[nats.tls]` / `[nats.auth]`; the lord injects the credential
  paths into thralls, and the operator CLI takes `--ca`/`--nkey`). Still open: securing the embedded
  server itself for a networked bind, mutual TLS, JWT/account isolation, token auth, and key rotation.

Note: JetStream durable mailboxes, cluster-wide singletons, and client-side nkey auth over server TLS,
listed as future work in earlier drafts, are now implemented (see §6, §12 and the manifest `durable` /
`scope` / `[nats.tls]` / `[nats.auth]` fields).

---

## 15. Summary of decisions

| Area | Decision |
|---|---|
| Runtime | a Go binary, embedded NATS (default) or an external cluster (config switch) |
| Wire format | a JSON envelope, `kind` + `op` dispatch |
| SDK home base | TS/Bun (`@hamicek/aether`); plus Python and Go |
| SDK behaviours | GenServer thrall (`Def`/`Start`) and a state-machine thrall (`FSM`/`StartFSM`, a `gen_statem` analogue: states, guards, state timeouts) - both on the same serialized mailbox; the FSM stays domain-neutral, application automata build on top |
| Topology | a declarative `aether.toml`; behavior in code |
| Lord | variant A (dumb) + a heartbeat/drain contract ready for B |
| Mailbox | core NATS ephemeral, with an optional JetStream durable mailbox (`durable = true`) |
| Observability | structured logs + a Prometheus `/metrics` endpoint + heartbeat miss detection + `trace` propagation (§14); `$SYS` liveness still planned |
| NATS features | do NOT hide them from thralls - `ctx.nats` is freely available |
| Multi-node | a local lord; singletons via a KV CAS lock |
