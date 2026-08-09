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
- **Mailbox durability** - core vs JetStream (see §11).
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

**Own blocks, planned (not yet implemented), in priority order:**
1. **DynamicSupervisor** - starting thralls at runtime (`ctx.startChild({...})`), not only from the
   manifest. Needed for "spawn a worker per request".
2. **Task / work queue** -> cast + a **JetStream work-queue stream** + a queue group. A durable task
   queue, at-least-once, a worker pool, retry.
3. **gen_statem (state machine)** - a thrall variant with explicit states and transitions
   (`new -> paid -> shipped`).
4. **RateLimiter / Circuit breaker** - shared state in KV.

---

## 13. Deliberately deferred (gaps in the design)

See [ROADMAP.md](./ROADMAP.md) for the maintained list. In short:

- **Liveness beyond heartbeats** - `$SYS` connection events as a supplement, so liveness works even
  outside the embedded-server case.
- **Full thrall-level fencing for singletons** - the window where a lord dies and its thrall is
  briefly orphaned before the KV lock expires.
- **`temporary` semantics inside group strategies** - its interaction with `one_for_all` /
  `rest_for_one` is not fully specified.
- **Thrall state persistence** - durability today covers the *mailbox* (casts survive a crash via
  JetStream), not the *state*. Like OTP, a restart runs a clean `init` and loses in-memory state.
- **Monitoring / observability and long-running soak testing** - for high-reliability use.
- **Stronger and server-side security** - the client side authenticates to an *external* bus with
  nkeys over server TLS (manifest `[nats.tls]` / `[nats.auth]`; the lord injects the credential
  paths into thralls). Still open: securing the embedded server itself for a networked bind, mutual
  TLS, JWT/account isolation, token auth, the operator CLI against a secured cluster, and key rotation.

Note: JetStream durable mailboxes, cluster-wide singletons, and client-side nkey auth over server TLS,
listed as future work in earlier drafts, are now implemented (see §6, §12 and the manifest `durable` /
`scope` / `[nats.tls]` / `[nats.auth]` fields).

---

## 14. Summary of decisions

| Area | Decision |
|---|---|
| Runtime | a Go binary, embedded NATS (default) or an external cluster (config switch) |
| Wire format | a JSON envelope, `kind` + `op` dispatch |
| SDK home base | TS/Bun (`@hamicek/aether`); plus Python and Go |
| Topology | a declarative `aether.toml`; behavior in code |
| Lord | variant A (dumb) + a heartbeat/drain contract ready for B |
| Mailbox | core NATS ephemeral, with an optional JetStream durable mailbox (`durable = true`) |
| Observability | the KV registry + the `aether._lord.events` lifecycle stream; `$SYS` events planned |
| NATS features | do NOT hide them from thralls - `ctx.nats` is freely available |
| Multi-node | a local lord; singletons via a KV CAS lock |
