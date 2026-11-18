# aether - architecture sketch

> A polyglot distributed actor/OTP runtime where the substrate is **NATS** and "processes" are
> real **OS processes**. The goal: an SDK that makes it very easy to write **thralls** (genservers)
> and run a **lord** (supervisor). Not BEAM-scale (millions of processes), but **tens** of
> processes that communicate reliably.

Status: implemented core, actively evolving. Last updated 2026-08-29 (through v0.10.0).

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

**Containerizing it.** The single-binary runtime drops cleanly into a container: build the `aether`
binary plus the Go thrall binaries, run `aether up -f <manifest>`. Two process-model details carry
over from above. The lord waits only on its *direct* children, but a thrall runs as `sh -c '<cmd>'`
in its own process group (§12b, §13b), so an orphaned grandchild reparents to PID 1 - the image
needs a real init (`tini`, or `docker run --init`) to reap it. And because the lord handles
SIGINT/SIGTERM, `docker stop` triggers the same graceful drain as Ctrl-C. Distribution stays one
lord per container/node over a shared external NATS (a lord does not spawn into other containers);
the embedded broker is single-node. See the README "Deployment (Docker)" section for the image and
a compose example.

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
aether._fleet.<app>.<lord>   # curated health summary a lord publishes about itself (opt-in, fleet view)
aether_<app>_<name>          # JetStream stream for the durable mailbox (dots become underscores)
```

A worker pool = several thralls with the same `name` in a **queue group** on `...call`/`...cast` ->
NATS load-balances. The reply subject is not handled by hand - NATS request/reply keeps an ephemeral
`_INBOX.*` itself.

---

## 7. Wire envelope (JSON)

One envelope for everything; `kind` distinguishes the type, `op` is "which handler function". The
authoritative shape lives in `internal/wire/envelope.go`; the TS and Python SDKs mirror it. A thin
**C# client** (`sdk/csharp`) mirrors the same wire shape (envelope, subjects, heartbeat) for a
consume + cast role - a data-processing thrall that reads its (optionally durable) mailbox and casts
results on - deliberately not a full SDK (no behaviours); parity is held by the shared golden fixtures.

**Call (request):**
```json
{
  "v": 1,
  "id": "abc-123",
  "trace": "t-9f2",
  "kind": "call",
  "from": "gateway",
  "to": "counter",
  "op": "get",
  "payload": {},
  "ts": 1730000000000,
  "deadline": 1730000005000
}
```
`deadline` is the instant (Unix ms) at which the caller stops waiting: its send time plus its
timeout. A thrall drops a call whose deadline has passed before it starts handling it (§9c). It is
only ever set on a `call`; `idem` (an optional idempotency key, §13c) and `trace` (the correlation
id of §13b) are the other optional fields of a request.

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
`error.type` says what kind of "no" this is, so a caller never has to read the message to decide:

| `type` | Meaning | The thrall afterwards |
|---|---|---|
| `refused` | the thrall declined the request by a rule of its domain; `error.code` is its stable machine code for the rule (§8) | unchanged, healthy - a refusal is a normal answer |
| `handler_error` | the handler failed at this request | alive, state unchanged, failure recorded as its last error |
| `unknown_op` | no call handler for the `op` | unaffected |
| `escalated` | the handler asked to be crashed (§8) | restarts through `init` |
| `deadline_exceeded` | the call's deadline had passed before the thrall got to it; nothing ran, hence `retryable: true` (§9c) | unaffected |
| `reply_too_large` | the handler ran, but its reply does not fit one bus message (`max_payload`) and could not be sent; the caller is told instead of waiting out its timeout | alive, the handler's state change stands, recorded as its last error |

An FSM thrall additionally answers `no_transition` and `guard_rejected` for an event its current
state does not act on.

**Refusal** (the one error reply that carries a `code`):
```json
{
  "v": 1, "id": "abc-123", "kind": "reply", "status": "error",
  "error": { "type": "refused", "code": "insufficient_funds",
             "message": "the balance is 3, the withdrawal 5", "retryable": false }
}
```

**The envelope grows only additively.** Every field added after the first version - `trace`,
`idem`, `deadline`, `error.code`, the heartbeat's `describe`, `expired_total` and `in_flight` - is optional and
omitted when empty, so peers of different ages interoperate in both directions: an old caller sends
no `deadline` (its calls are handled whenever they are dequeued, as before) and reads a refusal as
the `"<type>: <message>"` error string it has always got; a new caller talking to an old thrall gets
`handler_error` with no `code`, i.e. the generic failure. The golden fixtures in `testdata/wire`
pin the shape for all SDKs, and the cross-language test (`sdk/go/thrall/crosslang_test.go`) runs
real Go, TS and Python thralls on one bus: a refusal made by each SDK is read as the typed error by
each of the other SDKs, each SDK's caller stamps the deadline the same way, and each SDK's thrall
drops a call that arrives past its deadline.

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

### The client - calling from anywhere

Calling a thrall needs nothing but a NATS connection. The SDKs expose that as a **client**: no
lord, no injected environment, no running thrall. A gateway, a CLI, a test, or a library that other
thralls import all produce the same envelope a handler's own `ctx.call` does, so nothing outside
the runtime has to mirror the envelope by hand.

```go
c := thrall.NewClient(nc, "ujito")                       // the app it addresses by default
raw, err := c.Call("denik", "stav", req, 3*time.Second)  // json.RawMessage
stav, err := thrall.CallInto[StavReply](c, "denik", "stav", req, 3*time.Second)   // typed
doc, err := thrall.CallInto[GetReply](c, "store", "get", get, 2*time.Second,
    thrall.InApp("ae"),                                  // a thrall in another app
    thrall.WithTrace(trace),                             // carry the caller's trace
    thrall.WithIdempotencyKey(key))                      // dedup a retry (§13c)
perRequest := c.With(thrall.WithTrace(trace))            // a derived client, defaults bound
```
```ts
const c = new Client(nc, "ujito");
const stav = await c.call<StavReply>("denik", "stav", req, { timeoutMs: 3000 });
const doc = await c.call<GetReply>("store", "get", get, { app: "ae", trace, idempotencyKey: key });
const perRequest = c.with({ trace });
```
```python
c = aether.Client(nc, "ujito")
stav = await c.call("denik", "stav", req, timeout=3.0)
doc = await c.call("store", "get", get, app="ae", trace=trace, idempotency_key=key)
per_request = c.with_(trace=trace)
```

There is one implementation. A handler's `ctx.call` is the client bound to the thrall's connection
and app, carrying the trace of the message being handled; the package-level `call` is the client of
the thrall running in the process. In Go both satisfy the `thrall.Caller` interface, which is what
a library should accept: handed a `Ctx` it propagates the trace, handed a `Client` it works
anywhere. A call resolves to the reply payload or fails in one of two distinguishable ways: a
**typed call error** when the target answered with an error reply (`*thrall.CallError` /
`CallError` / `aether.CallError`, exposing the reply's `type`, `code`, message and `retryable` -
the human sentence is `Message` in Go, `message` in Python and `reason` in TS, where `message` is
the standard error text `"<type>: <reason>"`), or the **NATS error** when no reply arrived
(timeout, no responders). From a handler the trace of the message being handled is carried by
default; an explicit one (`WithTrace` / `opts.trace` / `trace=`) takes its place.

For the rare code that works at the envelope level - a test double standing in for a thrall, a
bridge to another transport - the Go SDK re-exports the wire contract from `internal/wire`
(`thrall.Envelope`, `thrall.WireError`, `thrall.CallSubject`, `thrall.OKReply`,
`thrall.ErrorReply`, the kind / status / error-type constants). They are aliases of the wire types,
so there is still exactly one definition.

### Refusing a request

A handler has three ways to say that a request did not go through, and they mean different things:

- **A refusal** = "I understood, I am fine, and the answer is no." A rule of the thrall's domain
  forbids what was asked: not enough funds, the day is already closed. This is a normal outcome of
  a healthy thrall, not a failure.
- **A returned/thrown error** = "this request failed, but I'm fine."
- **An escalation** = "I can't safely continue - crash me" (next section).

```go
return nil, s, thrall.Refuse("insufficient_funds", "the balance is 3, the withdrawal 5")   // Go
```
```ts
refuse("insufficient_funds", "the balance is 3, the withdrawal 5");                         // TS
```
```python
raise aether.Refuse("insufficient_funds", "the balance is 3, the withdrawal 5")             # Python
```

The caller gets an error reply of type `refused` carrying the **code** and the **message**, which
its SDK surfaces as the typed call error:

```go
if code := thrall.RefusalCode(err); code != "" { /* a rule said no: switch on code */ }
var ce *thrall.CallError
if errors.As(err, &ce) && ce.Refused() { /* ce.Code, ce.Message */ }
```
```ts
if (err instanceof CallError && err.refused) { /* err.code, err.reason */ }
```
```python
except aether.CallError as err:
    if err.refused: ...  # err.code, err.message
```

The **code is contract, the message is prose.** A code is what callers branch on, so it is part of
the thrall's published contract: name it after the rule and never reword it. The message is for
people and may change freely. Before refusals existed a domain "no" travelled as `handler_error`
and callers had to recognise it by the text of the message, which broke the moment a sentence was
rephrased and made every refusal look like a fault.

A refusal is deliberately *not* a failure of the thrall, in any SDK and in the lord's view of it:
the thrall's state is left as it was (as for any error), it is **not** recorded as the thrall's
`last_error`, it counts as a processed message, it is logged at debug rather than as an error, and
it is not stored in the idempotency cache (only successful replies are). A refused **cast** has no
caller to tell and is only noted in the log. A refusal always carries a code: refusing with an
empty one yields the code `refused`, so "the code is empty" reliably means "this was not a refusal".

What a *caller* makes of a refusal is the caller's business. The two callers built into the
runtime are generic and cannot know a thrall's codes: `aether call` prints the refusal with its
code and exits non-zero like for any error reply, and the built-in HTTP edge (§12b) answers 502 as
for any error reply - mapping a thrall's refusal codes to client-facing statuses belongs in a
gateway that knows the contract, which is what a web thrall is for.

**A refusal is never forwarded implicitly.** If a handler calls another thrall, that thrall
refuses, and the handler returns the call's error as it is, the caller of the *handler* gets a plain
`handler_error` - the typed call error (what a caller receives) and the refusal a handler returns
are two different types on purpose. A code belongs to the contract of the thrall that issued it; if
it were passed up automatically, a store's `revision_conflict` would pose as the answer of whatever
thrall happened to sit in front of the store, and that thrall's genuine failure would not show up
as its last error. Passing a refusal on is one explicit line:
`if r, ok := thrall.AsRefusal(err); ok { return nil, s, thrall.Refuse(r.Code, r.Message) }`.

### Let it crash - escalation

Beside a refusal, a handler has two ways to signal *failure*, and they mean different things:

- **A returned/thrown error** = "this request failed, but I'm fine." The caller gets an error reply
  (`type: "handler_error"`), a failed cast is logged, and the thrall keeps running with its state.
- **An escalation** = "I can't safely continue - crash me." This is the OTP *let it crash*: the SDK
  terminates the thrall with an **abnormal exit**, so the lord restarts it through `init` per its
  restart policy and restart-intensity budget (§9). State is discarded; the restart starts
  clean. Prefer this over reaching for a raw `panic`/`os.Exit`/`process.exit` in a handler - those
  bypass the SDK contract, and a call caller then only learns of the crash by timing out.

```ts
import { escalate } from "@hamicek/aether";        // TS: throw the signal
handleCast: { withdraw: (amt, s) => { if (amt > s) escalate("balance underflow"); return s - amt; } }
```
```go
return s, thrall.Escalate("balance underflow")     // Go: return the signal
```
```python
raise aether.Escalate("balance underflow")         # Python: raise the signal
```

On a **call**, the caller is not left hanging until timeout: it first gets a distinguishable error
reply (`type: "escalated"`, carrying the reason) and only then does the thrall exit. Escalation is not
a back door around supervision - a `temporary` thrall that escalates stays down, and repeated
escalation trips the same restart-intensity cap as any crash loop.

When a **durable** cast handler escalates, the SDK acks the source JetStream message before the
crash. The poison cast is consumed once and the restart is clean - it is not redelivered into a loop
that would crash every restart until restart-intensity gives up (which is what an unacked exit would
cause, since durable delivery is at-least-once with redelivery on crash).

### A panic is not an outcome - what it costs depends on the thrall

A Go handler can also blow up without choosing to: a nil map, an index out of range. (TS and Python
have no such fourth outcome - a bug there surfaces as a thrown error, the first bullet above.) The
SDK used to do nothing about it, so the process died with a raw stack on stderr and the caller of
that call timed out. What a panic *should* cost is decided by what the thrall is:

- **An ordinary thrall (GenServer, FSM) still crashes - deliberately.** Its state is whatever its
  handlers made of it, and a handler that panicked half way may have left it half changed (a map
  mutated in place, a call to another thrall already sent). Continuing on that state is exactly what
  *let it crash* exists to avoid, so the semantics stay: abnormal exit (status 2, as an unrecovered
  panic always had), restart through `init` under the restart policy and intensity budget. What
  changed is that it now dies properly, the way an escalation does: the caller of the panicking
  call gets an **`escalated`** error reply at once (message `panic: <value>`) instead of waiting
  out its timeout, the panic is logged through the thrall's logger with the op, the request's
  trace and the stack, and the reason travels to the lord as `last_error` on the dying breath.
  One difference from `Escalate` is intentional: a **durable cast that panics is not acked**. An
  escalation is the handler's decision about that message; a panic is a bug that says nothing
  about it, so delivery stays at-least-once and the cast is redelivered after the restart.
- **A web thrall survives - one request pays.** Requests are independent of each other, and a
  gateway that dies takes every user's request with the one that hit the bug (four such requests
  in five seconds and the lord gave up on the gateway for good, §9). So `StartWeb` contains it: the
  request is answered with a bare **500** by the web thrall itself, the panic is logged with trace
  and stack, counted in `panics_total` (`aether_panics_total{name}`), recorded as `last_error`,
  and the thrall keeps serving. With `Handle` the state the request would have returned is
  discarded, so the next request gets the one from before it (anything the handler changed in place
  through a pointer or a map stays changed); with `Serve` the shared value is whatever the handler
  had made of it (release locks with `defer`).
- **An event manager** already isolated a panicking handler from the others (§12); unchanged.

The web rule is the same in all three SDKs: a web handler that fails - a Go panic or returned
error, a thrown error or rejected promise in TS, a raised exception in Python - is answered as HTTP
500 by the thrall, logged with trace and stack, and the process keeps serving; TS and Python count
it in `panics_total` too, since there a crash and a failure are the same exception. This is a
change for the returned/thrown error: it used to be a `handler_error` reply, which the ingress
could only render as a 502. A web thrall is HTTP-native - it answers its own "no" as a response,
and now its own failures too - while `Refuse` and `Escalate` keep their meaning. The 500 comes
from the SDK, not the ingress, so the ingress stays a dumb forwarder and its own statuses (502
for an error reply, 504, 503) mean what they meant. Only the handler's own invocation is covered:
a goroutine, promise or task the handler started and did not wait for fails outside the request
and still ends the process.

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
# [nats.leaf]                                     # embedded spoke: make this node a leaf of a hub (§11b)
#   remote = "nats-leaf://hub.internal:7422"      #   the hub's leafnode listener
#   site   = "SITE_A"                             #   the account this node binds to (its isolation unit)
#   domain = "sa"                                 #   JetStream domain, unique per node
#   user = "leafA"  password = "leafA"            #   or: nkey = "/etc/aether/siteA.nk"
# [nats.security]                                 # expose the embedded server on the network (§11b, §14)
#   listen = "0.0.0.0:4222"                       #   networked bind (absent = loopback, no auth)
#   tls_cert = "/etc/aether/server.pem"           #   server cert + key (server-side TLS)
#   tls_key  = "/etc/aether/server-key.pem"
#   ca = "/etc/aether/ca.pem"                     #   clients verify the server against it
#   nkey_seed = "/etc/aether/node.nk"             #   one shared identity (or the three per-role seeds below)
#   # lord_nkey / thrall_nkey / operator_nkey     #   least-privilege roles (exclusive with nkey_seed)

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

A programmatic API on top (a DynamicSupervisor, §12) adds a thrall at runtime (`ctx.StartChild` /
`ctx.StopChild`); the default path is still the manifest.

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

### The deadline: a call nobody waits for is not executed

The mailbox is serialized, so a call can sit behind a slow handler for longer than its caller is
willing to wait. Without anything else, the caller times out, the thrall later reaches the call and
executes it anyway - and when the user retries, the write happens twice.

So every call says when its caller stops waiting. The caller stamps `deadline = now + timeout` on
the envelope; the thrall, at the last moment before the handler would run (once the mailbox lock is
held), checks it and **drops** an expired call: the handler does not run, the drop is logged at
warn with the op, the trace and how late the call was, it is counted in the thrall's
`expired_total` heartbeat metric (`aether_expired_total{name}` on the lord, and not in
`processed_total`), and the reply is `deadline_exceeded` with `retryable: true`. An envelope
without a deadline behaves exactly as before. The built-in HTTP edge and `aether call` stamp the
deadline too, since they are callers like any other.

**What it does not do.** A deadline narrows the window, it does not close it: a handler that
*starts* just before the deadline still finishes after the caller gave up. It turns "every
timed-out call is executed later" into "only a call that was already running is". A caller that
must not write twice sends an **idempotency key** as well (§13c); the two are complementary.

**Clocks.** The deadline is stamped by the caller's clock and judged by the callee's, so the callee
allows a skew tolerance: a call is dropped only when `now > deadline + tolerance`. The default is
**250 ms**, overridable per thrall process with `AETHER_DEADLINE_SKEW_MS` (a thrall inherits the
lord's environment, so setting it on the lord covers the node; `0` judges the deadline exactly).

- On **one host** caller and callee read the same clock, so the true skew is zero and the tolerance
  only costs something: a call that expired less than 250 ms ago is still executed.
- **Across nodes** kept by NTP the clocks differ by milliseconds to a few tens of milliseconds;
  250 ms absorbs that with a wide margin, so a healthy deployment never drops a live call.

The cost of the margin is small against the window a deadline cannot close anyway (the handler that
started just in time), which is why one default serves both cases instead of a per-topology knob.
The failure modes of a clock that is wrong by *more* than the tolerance are asymmetric and safe. A
callee running ahead refuses live calls immediately with `deadline_exceeded` - loud, nothing
executed, retryable - which shows up as a climbing `aether_expired_total` and points straight at
the clock. A callee running behind executes an expired call, which is simply the behavior without a
deadline. Neither direction can make a thrall execute something twice on its own.

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
  process manager"). "One instance across hosts" is then a deployment concern - solved by leaf-node
  isolation (one lord per node, its own app; §11b), not by lords racing a shared lock. The
  `scope = "singleton"` lock is single-instance *within* an app, not across a cluster - see §12.

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

## 11b. Multi-node topology, isolation, and the auth boundary

aether runs across many nodes by keeping the model simple: **one lord per NATS node.** A node is a
lord plus its own NATS (embedded or a local server); distribution comes from linking the NATS servers
at the bus layer (**leaf nodes**, clustering, gateways), *not* from running several lords over one
bus. Two lords never share responsibility for the same app.

**Isolated sites (hub-and-spoke).** A common shape is one central node plus sites that must not see
one another (e.g. SCADA: a center plus individual sites). This is achieved entirely at the NATS
layer: each site is a **leaf node bound to its own NATS account**, the hub **imports only each site's
data plane** (`aether.<app>.<name>.*`), and the sites import nothing from each other. Isolation is
then **structural** - a site cannot reach or observe another site because accounts are a hard
boundary, not because application code is careful. A runnable, asserted example is
`examples/hub-spoke-spike/` (distribution + isolation proven).

**Embedded spoke ergonomics (`[nats.leaf]`).** The spoke side of that topology - one node, one site,
one hub link - is common and small enough that aether ergonomizes it: an `[nats.leaf]` section under
`[nats]` makes the *embedded* server a leaf of the hub, bound into the site's account with its own
JetStream domain. aether renders the proven spoke-side NATS config from that intent (account, service
export of `aether.<app>.>`, leaf remote, per-node domain), so a site deploys as a single binary
instead of "aether + a hand-written NATS config". Supervision (`aether._lord.>`) is simply never
exported, so it stays node-local by construction - the export list *is* the isolation policy, no
allow/deny rules needed. The section carries only aether-shaped intent (which hub, which site, which
domain, which credential); anything richer drops to `mode = "external"` against an operator-run bus.
Deliberately asymmetric: aether ergonomizes only the **spoke** side; the **hub** side - the
multi-account import matrix - stays operator-authored NATS config (bring-your-own). `examples/nats-leaf/`
shows a spoke manifest (`[nats.leaf]`) against a hub node (`external`). Cross-node *fencing* is not
introduced (each site is single-node and owns its singletons); a singleton that could fail over
*between* sites would need a shared KV crossing the account boundary, which is out of scope here.

**What crosses the node boundary.** Only the data plane (`aether.<app>.<name>.*`) needs to be exported
across accounts. **Supervision** (`aether._lord.*`) and the KV fencing buckets are provisioned by each
lord in its own NATS and stay **node-local** - they are never exported, so no supervision traffic
leaks between sites. This is what makes one-lord-per-node clean. This is a *different* mechanism from
the one that keeps `aether._lord.>` node-local on a single shared secured bus (§14): **non-export**
across leaf accounts is the boundary here, whereas **role permissions** are the boundary on a
standalone networked embedded server. (Two consequences: a cross-node
`ctx.Call` works transparently only within a *shared app namespace* - it is bound to the caller's app;
and durable delivery *across* nodes is deliberately deferred - see §14 - because JetStream over leaf
nodes needs a domain per node and cross-node streams mean mirror/source.)

**The auth boundary.** "A site cannot see another site" and "a client is authorized only for its own
subjects" are the same statement - authorization lives at the NATS layer, and aether does not
reimplement it. Auth splits into three layers with a clear boundary:

- **Infra (node ↔ bus)** - who may connect and to which subjects. aether provides the client-side
  plumbing (nkey/CA from the manifest, injected into thralls; see §9). For a **secured embedded
  server**, aether also *generates* the server-side policy: `[nats.security]`'s least-privilege tier
  renders per-role nkey users (lord/thrall/operator) whose subject permissions keep `aether._lord.>`
  node-local by construction (§14), so no manual rule is needed. For an **external** bus (or the hub
  side of a leaf topology) the bus policy - accounts, exports/imports, subject permissions - stays
  operator-authored NATS configuration; there the supervision subjects `aether._lord.>` should be
  permission-restricted to the lord/thrall users under network exposure (ordinary clients deny them).
- **Operator (control of aether)** - the dashboard, `/metrics`, and the control channels. This is
  aether's job (kept small - an auth gate and a permission recipe), not an application concern.
- **Application users and roles** - login, RBAC, "who may acknowledge this". This is the **domain's**
  job, above aether. aether offers only the edge plumbing to project an application's own auth onto a
  subject scope (see §12b), never a user or role model of its own.

In one line: **aether owns the client-side infra plumbing and the control of itself; it does not own
application identity or roles.**

---

## 12. Catalog of building blocks (besides the thrall)

**Almost for free (a thin wrapper over NATS), implemented today:**
- **Registry** -> NATS **KV** `name -> {node, pid, status}`. Cluster-wide with no extra work.
- **Lifecycle stream** -> `aether._lord.events` (spawned/ready/down/restarting/...), surfaced by the
  CLI `events`.
- **Fleet health** (opt-in, `[observability] fleet_health`) -> each lord publishes a **curated
  summary of its own state** (its thralls, their status and metrics) on `aether._fleet.<app>.<lord>`,
  and the CLI `fleet` (a `fleet.Aggregator`) assembles a view of **every lord on the bus** - the
  fleet-wide sibling of the per-node observer dashboard. It is a mechanism, not a domain concern: it
  describes the runtime, never application data. Deliberately a separate subject from supervision -
  the raw `aether._lord.>` channels are node-local, whereas `_fleet` is a curated summary meant to be
  seen across lords. The payload is a JSON contract, so a dashboard can consume it in any language; a
  *domain* dashboard (tags/alarms/layout) is an application thrall that consumes this feed as one
  input. **Across a leaf boundary:** in a single account (one bus, or an external cluster's single
  account) the aggregator sees every lord directly. In a hub-spoke topology the summary crosses the
  leaf as a **stream export** - the `[nats.leaf]` spoke exports `aether._fleet.<app>.>`, and the hub's
  center account imports it (operator-authored, like the data-plane import), so an aggregator on the
  hub sees every isolated spoke. Supervision (`aether._lord.>`) is still never exported, so isolation
  holds: only the curated summary crosses, and only where the hub imports it.
- **Singleton thrall.** `scope = "singleton"` runs the thrall as **at most one live instance within
  the app** - no replicas - guarded by a **distributed lock over NATS KV with CAS** (the lord acquires
  the key `singleton/<name>` before spawning). This is deliberately *not* Erlang's `:global`, which
  registers a name cluster-wide across a BEAM cluster: aether does not cluster its supervision (§11b),
  so this is single-instance within one app on one bus, not a globally registered name. Its original job
  - arbitrating between lords on several hosts racing to start the same instance - is gone: one lord
  per app is now enforced (§14), and lord-liveness fencing already reaps an orphan within the lease.
  What the lock still adds over `scope = "local"` is a **tighter handoff across a lord restart**: a
  replacement lord waits for the old, named lock to release before it spawns, whereas a `local`
  thrall re-spawns immediately and would overlap the dying orphan for up to the lease. That value is
  narrow (the lock is per-name and held across a lord change) and largely subsumed by lord-liveness
  fencing - kept, not relied on for clustering. This is single-instance *within an app*, not a pool
  of lords racing a shared lock across a cluster; cross-node single-instance is a deployment concern
  solved by leaf-node isolation (§11b). The manifest has `scope = "local" | "singleton"`.
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

**Own blocks, implemented as SDK behaviours (alongside the GenServer thrall):**
- **gen_statem (state machine)** - a thrall variant with explicit states and transitions
  (`new -> paid -> shipped`): `FSM`/`StartFSM` (see §8's counterpart and the manifest FSM demo).
- **gen_event (event manager)** - a thrall holding an ordered set of handlers; one async event is
  dispatched to every handler, in registration order, on the same serialized mailbox, each keeping
  its own state (`EventManager`/`StartEvent`). A failing handler is logged and skipped. v1 is
  async-only (a call is answered with an error); runtime add/remove of handlers and synchronous
  events are deferred. Demo: `examples/eventbus/`.

**Own blocks, planned (not yet implemented), in priority order:**
1. **Task / work queue** -> cast + a **JetStream work-queue stream** + a queue group. A durable task
   queue, at-least-once, a worker pool, retry.
2. **RateLimiter / Circuit breaker** - shared state in KV.

---

## 12b. Edge: input from outside the ether (HTTP ingress)

Every block so far is a *pull* from the ether: a thrall sits on a serialized mailbox and reacts to
call/cast. Real systems also need *push from outside* - an HTTP request, a SCADA driver reading a
device, a timer. That is the opposite shape: input arrives on an OS socket the process owns, it is
concurrent and stateless, and it does not fit a serialized mailbox (a thousand parallel requests
would queue behind one goroutine - head-of-line blocking). So an **edge** is not a behaviour; it is
a socket-owning process that bridges the outside world to the ether, holding no state itself - the
state lives in ordinary thralls behind it.

**HTTP ingress (implemented, model "built-in, by configuration").** A `[[edge.http]]` section maps
routes to a thrall operation, with **no code**:

```toml
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /value"      = { thrall = "counter", op = "value", kind = "call" }
route."POST /increment" = { thrall = "counter", op = "increment", kind = "cast" }
```

The lord synthesizes a supervised child per section running the internal `aether _edge` subcommand;
it is a normal singleton thrall to the lord, so it inherits spawn, heartbeat, drain and fencing
unchanged. Each request is translated to a wire envelope: a `call` route waits for the reply and
returns it as the body (200), a `cast` route hands the message to the ether and returns 202. Errors
map deterministically - an application error reply -> 502 (a refusal included: the edge does not
know a thrall's refusal codes, §8), no responders -> 503, a reply timeout -> 504, a call the thrall
dropped past its deadline -> 504 as well (it is the same timeout seen from the other side, §9c), an
unknown route -> 404. The edge connects with the same client path as the CLI (`wire` +
`nats.Conn`), not the thrall runtime, so it carries no mailbox. Multiple `[[edge.http]]` servers
coexist in one manifest, each on its own port. Demo: `examples/webserver/`.

**Custom edge (implemented, model "you write the code").** When the ingress cannot be expressed by
configuration - custom auth, transformation, a non-HTTP protocol (a Modbus/OPC-UA/MQTT driver, cron,
tail) - you write the edge yourself over the same machinery, via `thrall.StartEdge` (Go SDK). It is a
fourth thrall shape alongside `Start`/`StartFSM`/`StartEvent`, but **not a behaviour** - it has no
mailbox. You supply a run-loop that owns the socket and an optional graceful-stop hook; connect,
heartbeat, drain and fencing are reused. A custom edge is an ordinary `[[thrall]]` with a `cmd` in the
manifest, so model A and model B **coexist in one manifest** and the lord supervises both identically.
State still lives in ordinary thralls behind it, reached via `ctx.Call`/`ctx.Cast`. Demo:
`examples/webserver-custom/` (a custom edge doing an Authorization check beside the built-in ingress).

Where the A/B line falls: what maps cleanly as route -> operation is model A (declarative, no code);
custom logic around the call is model B. Input validation follows the same line - model A validates
declaratively (a JSON schema on a route: `route."..." = { ..., schema = "..." }`, rejecting a bad body
with a 400 before it reaches the ether), model B validates in code.

**Web thrall: "a thrall IS the web server" (http wire contract, implemented AE-091).** Models A and B
both keep HTTP *at the edge* - the edge owns the socket and the thrall behind it never sees HTTP, it
answers a domain op (`value`, `increment`). The inverse model pushes HTTP *to the thrall*: a single
`http_request` contract carries the whole request/response over NATS, so a web thrall is an ordinary
genserver whose one operation is `http_request -> http_response` and that does its own path routing. It
never owns a socket or a port; a **dumb, generic ingress** maps the `Host` header to a subject and
forwards the whole request, so adding a site is a route-table entry, not a new edge. The contract lives
in `internal/wire` (Go = source of truth; `HTTPRequest`/`HTTPResponse` with a base64 body for binary
safety and `http.Header`-shaped headers), mirrored in the TS and Python SDKs with golden-fixture parity.
Each SDK ships a helper (`StartWeb` / `startWeb` / `start_web`) so the author writes only the handler,
plus ergonomic response constructors (HTML/text/JSON). It does not replace models A/B: use
`[[edge.http]]` to put HTTP in front of an existing thrall's operations, and the web thrall + ingress
when the thrall itself is the web app.

**A web thrall serves requests concurrently - if its handler says it can.** A web thrall that
inherited the GenServer mailbox served one request at a time: one slow request (a stalled thrall
behind it, a chain of calls) held every other request of every user, the health check included. A
gateway is the one kind of thrall for which that is the wrong default, so the web helper has two
handler shapes, and **the shape, not a flag, decides how requests run**:

| | `Handle` / `handle` | `Serve` / `serve` |
|---|---|---|
| signature | `(req, state, ctx) -> (resp, next state)` | `(req, shared, ctx) -> resp` |
| requests at once | one - the serialized mailbox, as every thrall | up to `Concurrency` (default **32**) |
| the value from `Init` | threaded: each request gets what the previous one returned | shared: built once, handed to every request, never replaced by the runtime |
| ordering | arrival order | none between requests |

The rule a `Serve` handler lives by: **the mailbox no longer stands between two requests.** What
the shared value holds after `Init` is what every request sees. Treat it as read-only (clients,
configuration, parsed templates, embedded files need nothing), or make what changes safe yourself -
in Go a mutex, an atomic or a type that already is; in TS and Python, where handlers interleave only
at an `await`, by keeping a change within one stretch without an `await` (a read, an `await`, then a
write based on that read is a lost update). State that must change one request at a time belongs in
an ordinary thrall the handler calls: its mailbox is the lock, which is what a GenServer is for.
`ctx` is per request in a `Serve` thrall (its trace and message id are that request's) and must not
be kept.

It is hard to misuse on purpose. A handler written against the mailbox has the `Handle` shape, and
there is no way to run that shape concurrently: `Handle` with `Concurrency > 1`, or both shapes at
once, is refused at start instead of ignored, and an ordinary thrall (`Start`, FSM, event manager)
has no concurrency setting at all - **GenServer semantics are unchanged**. `Concurrency = 1` with
`Serve` runs through the mailbox, exactly as `Handle` does. Nothing switches over by itself: a web
thrall written before this keeps its `Handle` shape and its one-at-a-time behaviour until its
author moves it to `Serve`.

What stays true in the concurrent mode:

- **The deadline (§9c).** A request that finds every slot taken waits for one. The gate still
  decides at the last moment before the handler runs - and a request whose deadline passes *while
  it waits* is dropped right then (same log line, same `expired_total`, same `deadline_exceeded`
  reply), not when a slot eventually frees. So the queue is bounded by how long callers are willing
  to wait (the ingress stamps 5 s on every request), not by a count; a call without a deadline
  waits as long as it takes, as before. A hard cap on queued work is a separate concern (overload
  protection, deliberately not part of this).
- **Drain.** A draining thrall stops reading, finishes every request it had already received -
  those in their handlers and those still waiting (which run or expire) - flushes their replies,
  and only then terminates. The serialized path now waits for its in-flight handler too, whether or
  not the thrall has a `Terminate` hook.
- **Metrics.** `mailbox_depth` counts requests received and not finished; the new `in_flight` is how
  many are in their handlers right now, so `mailbox_depth - in_flight` is the queue. `describe`
  carries `concurrency` for a concurrent web thrall (absent = serialized). On the lord:
  `aether_in_flight{name}`, and `aether describe` prints the concurrency.
- **Idempotency keys.** The in-memory dedup (§13c) belongs to the serialized mailbox and a web
  thrall never enabled it (`WebDef` has no such option, and the ingress sends no key), so nothing
  changes: a request's idempotency is the business of the thrall that does the write.
- **Escalation** still crashes the process - with the other requests that were in flight.

The three SDKs behave the same. Before this change all three serialized web requests (in TS and
Python an `async` handler did *not* interleave with the next request: the mailbox awaited it), and
all three now offer the same two shapes with the same default. The one honest difference is what
"at once" means: goroutines in Go, interleaving at `await` on a single event loop in TS and Python,
where a handler that blocks the loop still blocks every other request.

**The ingress (`cmd/http-ingress`, core in `internal/ingress`; AE-092/093, a component since AE-095).**
The generic host->subject forwarder that puts web thralls on the network: an edge thrall
(`thrall.StartEdge`) that owns the socket, resolves `Host` to a thrall, forwards the request as one
call and writes the reply back. It started as an example; once a production site ran on it, it became
a program of the repository with the properties a public front door needs. It is a separate binary
rather than an `aether` subcommand because it is an ordinary thrall the lord runs through `cmd` - the
lord should not carry an HTTP server it does not use - and its contract is the binary and its flags,
which is why the Go code is internal. The design decisions:

- **Dumb forwarder, default-deny.** No path routing, no middleware, no domain logic; a host reaches
  the ether only if a `RouteProvider` admits it. TLS, certificates and public rate limiting belong to
  the reverse proxy in front.
- **A route lookup has three outcomes**, not two: found, authoritatively not found (404), and *could
  not tell* (503). The site-registry provider keeps the registry's answers so that an outage does not
  take the sites down: the last known answer of a host (routable or denied) is served for a bounded
  time past its TTL, the registry is asked again only once a second while it is down, and routable
  and denied hosts are cached apart, each under a cap, so a scan of random hosts can neither grow the
  cache without limit nor evict real sites. The bound on staleness is the price: a site retired right
  before an outage stays reachable for at most that long.
- **Sizes follow the bus.** A request is one message and so is the reply, so the body limit is
  *derived* from the connection's `max_payload` (minus a reserve for headers and envelope, times 3/4
  for base64) instead of being an independent number that can disagree with it. Over the limit is a
  413 before anything is sent, never a truncation; the header reserve is an estimate, so a request
  that still does not fit is caught when the client library refuses the publish, with the same 413.
  The reply side cannot be checked by the ingress - an oversized reply never leaves the thrall - so
  the SDKs answer such a call with `reply_too_large` (§7) and the ingress turns that into a 502.
  A thrall on an SDK that predates it drops the reply and the request ends as a 504.
- **Every phase of a connection is bounded**: reading headers, reading the whole request, waiting
  for the thrall, writing the response, idling between requests, and the number of connections served
  at once (further ones wait in the accept queue - backpressure, not unbounded goroutines). The write
  limit is counted from the moment the ingress has a response, so it is independent of the time a
  thrall may take. The limits have safe defaults and can be tuned but not disabled.
- **The call is an SDK call.** It carries the deadline (= the ingress timeout, so thrall and ingress
  give up at the same moment, §9c) and a trace minted per request, which is the handler's `ctx.Trace`,
  is in every log line and goes back to the browser in `X-Aether-Trace`. A `deadline_exceeded` reply
  is the ingress's own timeout seen from the other side, hence the same 504.
- **The ingress vouches for the request's origin.** `HTTPRequest` carries `host`, `scheme` and
  `client_ip`, which only the holder of the connection can state. They come from the connection
  itself; forwarding headers are consulted only when the peer is a configured trusted proxy (default
  loopback), walking `X-Forwarded-For` from the right to the first address that is not a trusted
  proxy. Client-sent `X-Forwarded-*` / `Forwarded` / `X-Real-IP` are dropped and the canonical three
  rewritten from the established facts, so a thrall reading headers and one reading the fields get
  the same answer. The fields were added later and are omitted when empty: an old thrall ignores them,
  a new thrall behind an old ingress sees them empty, which means "not stated", never "trusted".
- **It explains itself.** One access line per request (no query string, headers or bodies - that is
  where secrets travel) and, before every response it produces itself, a failure line with a stable
  cause and the error.

Out of scope by design: WebSocket and streaming, replies split over several messages, path-prefix
routing, mapping a thrall's refusal codes to statuses (a web thrall answers its own "no" as HTTP).

**Live push to the browser (implemented, SSE).** The reverse flow: ether -> browser. A frontend opens a
connection and the edge pushes events out to it as they cross the ether. `thrall.SSEStream` (Go SDK) is
the plumbing an edge runs inside `StartEdge`: it holds browser SSE connections, forwards ether events,
handles backpressure (a bounded buffer that drops for a slow client rather than stalling) and drain
(`Close` ends live connections so the HTTP server can shut down). Demo: `examples/live-dashboard/`.

The hard part is **authorization**: it all flows over one ether, but client A may see only site 1,
client B only site 2. The design makes that the application's job, not the runtime's - `ServeClient`
takes the subject scope the application already authorized, and gives each connection its OWN NATS
subscription on that scope, so NATS never delivers a client an out-of-scope event (scope-safe by
construction, not by a filter that a bug could leak past). Where the A/B line falls: verifying the token
and mapping it to a scope is model B code (a JWT's `site` claim -> `site.<x>.>`); a built-in declarative
`[[edge.stream]]` with `auth = "jwt"` would be the model A path, deferred. The channel is **read-only** -
control actions from the frontend go through an ingress route (call/cast), not this one. Reconnect is the
browser's built-in `EventSource` retry; the edge keeps no history, so a client resumes with the live
stream (durable replay is the event log's job, §13c), not a backfill.

**Boundaries (deliberate):** a real OS port is held by one active instance (**singleton fit**; HA via
the standby + fencing of §14), and aether does no SO_REUSEPORT or load balancing - scale a single
port with a reverse proxy in front. HTTP routing/middleware and business logic stay in application
code; the built-in model stays intentionally small (route -> operation). A slow serialized backend is
the throughput ceiling, by design.

The **TS and Python SDKs have parity**: `startEdge`/`start_edge` + `SSEStream` mirror the Go shape (a
run-loop + graceful-stop hook, scoped per-connection SSE), so a TS or Python edge is indistinguishable
from a Go edge under the same lord (demos: `examples/webserver-custom/edge.{ts,py}`,
`examples/live-dashboard/edge.{ts,py}`). Both non-Go run-loops await `stop` rather than blocking (node
`http` / Python asyncio do not block like Go's `ListenAndServe`), so they need no separate stop hook -
otherwise the semantics match, including rethrowing a run-loop error for a non-zero exit. The Python
edge is asyncio + aiohttp (`SSEStream` pushes over aiohttp's `StreamResponse`, with the same bounded
16-event drop-on-full backpressure as Go). Edge is thus **fully polyglot** (Go + TS + Python).

**Planned (not yet implemented):** WebSocket (for the bidirectional case; push is one-way, so SSE covers
it), and the built-in declarative `[[edge.http]]` egress / `[[edge.stream]]` with configured auth.

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

**How the mailbox drains.** Each SDK's durable consumer pulls casts from the stream in
**batches** (Go/Python `Fetch(128)`, TS `consume()` batches internally), processes them
through the serialized mailbox in arrival order, and acks each only after its handler
returns. Batching amortizes the per-message server round-trip a one-at-a-time fetch pays,
which is the difference between a few hundred and a few hundred thousand casts/s on the
drain path (`examples/durable-perf`). Two consumer knobs bound the batch: `AckWait` (how
long a delivered-but-unacked cast may sit before the server redelivers it) and
`MaxAckPending` (in-flight unacked ceiling). **Ordering:** FIFO holds in the happy path -
a single consumer receives casts in stream sequence and acks them in order. It is *not*
guaranteed under redelivery: an `AckWait` timeout or a crash re-queues a cast, which can
then arrive after later ones. That is the honest reading of at-least-once - **idempotent
handlers, not ordering, are the correctness contract** (§13c).

**Where the guarantee starts: the confirmed cast.** A plain `cast` is a core publish, so
mailbox durability begins only once the stream has stored the message. A cast sent while the
bus is unreachable (or after the client's reconnect buffer overflows) is lost, and the sender is
not told. When the sender must know, it uses a **confirmed cast** (Go `CastConfirmed`, TS
`castConfirmed`, Python `cast_confirmed`): a JetStream publish that returns only after the
target's mailbox stream acknowledged the write.

- **Success means stored; an error means *maybe not stored*.** The caller decides whether to
  retry; the SDK does not retry on its own.
- **Retry with the same idempotency key.** The key is sent as `Nats-Msg-Id`, so a message that
  did land before an unclear failure (a lost ack, a timeout) is stored once. This holds within the
  mailbox's duplicate window, the JetStream default of 2 minutes (the lord does not set it for the
  mailbox); a retry after that can store a duplicate.
- **A target without a mailbox fails with `ErrNotDurable` / `NotDurableError`, and nothing is
  sent.** The SDK looks the stream up first (cached per connection once found), because a
  JetStream publish to a non-durable thrall would reach its core subscription, get processed and
  never be acknowledged - and the retry would process it a second time.
- **Only the target's own stream may acknowledge.** The publish carries the expected stream name,
  so another stream capturing the subject cannot confirm it.

Confirmation covers the hop *into* the stream. Delivery *out of* it stays at-least-once, so
handlers still have to be idempotent. Like durable delivery itself, the confirmed cast is
node-local: the mailbox is looked up in the sender's JetStream domain, and durable delivery across
leaf nodes is deliberately deferred (§11b, §14).

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

**Retention vs rebuild - a declared intent, not a warning nobody reads.** A retention bound on a
log you *rebuild* from is silent corruption: `Rebuild` folds the *whole* log, but once retention
purges the oldest events the projection is incomplete with no error (there is no snapshot). The
runtime cannot infer whether a bounded log is a rebuild source or an audit trail, so the manifest
lets the operator say so with `event_log_use`:

- `event_log_use = "rebuild"` + a retention bound = a **config error**, rejected at manifest load
  (`aether up` fails, names the field, tells you to drop the bound or switch to `"audit"`). The
  footgun becomes an invariant.
- `event_log_use = "audit"` + a bound = fine, and the AE-055 startup warning is **suppressed** (the
  operator has declared rebuild is not in play).
- unset = **today's behaviour**: a bound still logs the startup warning, nothing fails. Backward
  compatible - no existing manifest changes meaning.

**Command-key: dedup on the event log.** "The fold must be idempotent" covers replay, but it does
*not* make a **non-idempotent event** safe against duplicate *delivery*. A signed delta ("add
+10") is the canonical case: if the mailbox delivers the same cast twice (it is at-least-once) or
a handler retries, a plain `Append` writes the event twice and the balance is wrong - the fold
cannot tell a replayed event from a genuine second one. `Append` therefore takes an optional
**dedup key** (`DedupKey` / `{ dedupKey }` / `dedup_key`) that it sends as the JetStream
`Nats-Msg-Id` header; two appends with the same key collapse to one record. The SDK exposes the
handled message's id as `ctx.MsgID` (`ctx.msgId` / `ctx.msg_id`), so the **command-key pattern**
is to key the append on it:

```go
ctx.Append(event, thrall.DedupKey(ctx.MsgID))
```

Two honest limits, both inherent to JetStream dedup rather than worked around:

- **The window, not forever.** Dedup holds within the stream's *duplicate window*
  (`event_log_dedup_window_ms`, 2 min by default, set explicitly by the lord). It guards
  real-time retry and redelivery, not a replay of the same id days later. Raise the window to
  cover slower retries, at the cost of a larger dedup index. The window is fixed at stream
  creation, like the retention bounds; the lord does not update an existing stream, so changing it
  in the manifest against an already-provisioned stream is **rejected at startup** (a config drift
  the lord names and fails on) rather than silently ignored - recreate the stream to apply it.
- **The id, not the intent.** Keying on `ctx.MsgID` deduplicates *the same message*. Two
  logically identical commands a client sends as two separate casts have two ids and are two
  events - correctly, since the runtime cannot know they were meant to be one. For client-driven
  idempotence, derive the key from a domain idempotency key the client supplies instead.

### Handler-level idempotence (dedup the processing, not just the log)

The command-key above deduplicates the **event-log write**, but the handler still runs on every
delivery - a duplicate `withdraw` debits twice even though it is logged once. An opt-in flag on the
thrall (`Idempotent` / `idempotent` / `idempotent=True`) closes that: the runtime deduplicates the
**call/cast itself**, before the handler runs.

- The **key** is a stable idempotency key the caller attaches (`WithIdempotencyKey(k)` in Go,
  `opts.idempotencyKey` in TS, `idempotency_key=` in Python), else the per-send envelope id (which
  never conflates two distinct sends but still catches an exact redelivery of the same message).
- A **duplicate cast** is skipped; a **duplicate call** returns the first reply from a cache, so a
  retrying caller gets the same answer instead of a re-execution. A durable duplicate is still
  acked. Only **successful** outcomes are recorded - a handler that errored or escalated is not
  marked, so a retry after a failure runs again (at-least-once for failures).
- The cache is **bounded** (a generational FIFO) and **in-memory**: it holds for the thrall's
  lifetime and does **not survive a restart**. That is the deliberate boundary - for idempotence
  that outlives a crash, combine it with the event log (the command-key dedup above), whose
  JetStream window persists. The two are complementary: handler dedup stops the double-processing
  while the thrall is alive; the event log keeps the *write* single even across a restart.

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
attaches its self-metrics (mailbox depth, last handler latency, processed count, and the count of
calls dropped past their deadline, §9c) to the heartbeat
it already sends every 2s, and the lord folds them in alongside its own supervision counters
(thrall count by status, restarts, gave-ups, heartbeat misses) and the durable backlog it reads
from JetStream (`num_pending`). The endpoint is the lord's own HTTP server, independent of the
NATS mode, so it works the same embedded or external - satisfying "not built on the embedded
privilege" (see §9/§10) without depending on `$SYS`. The metric model is exporter-agnostic, leaving
room for an OTLP bridge later.

The lord also samples each thrall's **resident memory and CPU** (`aether_thrall_rss_bytes` /
`aether_thrall_cpu_percent`) on a poll loop, via one `ps` per tick aggregated by process group.
This matters because a thrall runs as `sh -c '<cmd>'` in its own process group, so its real
footprint is the sum over the group (the `sh` leader plus the interpreter grandchild), not the
leader PID; CPU is a delta over the sample interval, not a lifetime average. Like the durable
backlog these are lord-observed (the SDK carries nothing extra), so they work uniformly across
languages and the two NATS modes.

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

**Observer dashboard.** A read-only web view of the supervision tree, the aether analogue of
Erlang's `observer` (the "Observer / dashboard" row of §2). Opt-in via `[observability] dashboard`,
it is served on the lord's existing HTTP server (the one serving `/metrics`, hence it requires
`metrics_addr`; a fail-fast guards the misconfiguration). It is a **consumer of signals the lord
already holds** - the children and their status/self-metrics for a `/api/tree` snapshot, and the
`aether._lord.events` lifecycle stream forwarded to browsers over SSE so the tree updates live -
so it needs no SDK change and works the same embedded or external. Two deliberate boundaries keep
it small and safe: it does **not** chart metrics over time (that is Prometheus/Grafana, which
`/metrics` feeds; a dashboard reimplementation would fight that split), and it is **read-only** -
control actions (restart/drain from the UI) and authentication for network exposure are follow-ups,
not v1. The page is self-contained (embedded, no external assets) so it serves in a closed
environment. Because aether is bounded to tens of processes, the whole tree renders on one screen -
unlike a BEAM-scale process list. The dashboard shows a **single lord's** local subtree, consistent
with "lord = a local process manager"; the network-wide view across several lords is a separate
mechanism - **fleet health** (§12, opt-in `[observability] fleet_health` + the `aether fleet` CLI),
which aggregates the curated health summaries lords publish rather than reaching into each dashboard.

**Self-description (what a thrall is).** A thrall answers "what operations do you support, what
build are you, what last failed" without an ssh. Like the self-metrics, the description rides the
heartbeat the thrall already sends: an optional `describe` block (`wire.ThrallDescribe`) carrying
its ops, self-declared version and last error. The lord stores the last one it saw and folds it into
the registry entry, so `aether ps` gains a version column, `aether describe <name>` prints the whole
record, and `aether fleet` carries it network-wide. It compares to NATS Micro's endpoint/version/
metadata INFO, but arrives without a services layer or a mandatory contract - four choices keep it
aligned with the rest of the design:

- **Ops are derived, not declared.** The SDK reports the keys of a thrall's handler maps, so a
  handler stays "just a map of functions" (§11's spirit, and faithful to OTP - a genserver does not
  declare its `handle_call` clauses either). This is the opposite of Micro, where the endpoint *is*
  the contract; here declaration would cost the map-of-functions ergonomics for little gain. The
  call and cast ops are kept apart (`call_ops` / `cast_ops`) so an `[[edge.http]]` route - itself a
  call or a cast - is validated against the right set. That validation is necessarily asynchronous:
  the manifest load can check a route's *target thrall* exists (§12b), but not its *op*, because
  thralls start after the load. So when a thrall first describes itself the lord checks the routes
  aimed at it and logs a warning (once) for an op it does not answer - a discovery, not a fatal
  error.
- **Push on the heartbeat, not a one-shot register or a pull.** The static fields (ops, version)
  repeat on every beat. At this runtime's scale (tens of processes) the bytes are negligible, and it
  buys self-healing: the lord always holds the current description, with no one-shot to miss on a
  late subscribe or a lord restart, and no request/reply round-trip to add to `ctl`. The mutable
  `last_error` needs a push channel anyway.
- **Version lives with the code; metadata lives in the manifest.** Two different facts with two
  different owners. The **version** is a code fact, declared on the thrall definition, so it answers
  "what actually runs here" - a manifest-declared version would lie exactly when a wrong binary is
  deployed against a right manifest, which is the rollout bug you most want to catch. **Metadata**
  (site, PLC address, protocol, criticality) is a deployment fact the operator declares in
  `[[thrall]] metadata`; the lord attaches it from the manifest and never round-trips it through the
  thrall, which keeps it off the SDK surface entirely. The same split gives the manifest a companion
  `expected_version`: the version the operator *intends* to run. The lord compares it against the
  reported version and raises a **rollout mismatch** (a warning logged once per reported version,
  plus a marker in `ps` / `describe` / `fleet`) when they differ - catching the wrong-binary case the
  self-declared version alone only exposes if you read both numbers by eye. The mismatch is *derived*
  at render time (reported vs expected), never a stored flag, so it self-heals the moment the two
  line up; like the edge-route op check it is asynchronous, since a thrall reports only after startup.
- **Metadata is shown, and mapped onto metric labels only by an allowlist.** It appears in `ps` /
  `describe` / `fleet` and could feed an application dashboard. It is *not* turned into Prometheus
  labels by default - a high-cardinality value (a PLC address, an id) would explode the series count.
  A per-thrall metric therefore carries only the `name` label until the operator opts in with
  `[observability] metadata_labels = ["site", "criticality"]`, which projects just those
  (low-cardinality) keys onto every per-thrall series - each series carries all allowlisted keys,
  empty for a thrall that lacks one, so the label set stays consistent and a `sum by (site)` works.
  Because the same enriched label set writes and deletes a series, a stopped dynamic thrall still
  leaves no orphan. The allowlist is a global observability *policy* (which keys are label-safe),
  which is why it sits in `[observability]` rather than being scattered per thrall.

A **refusal** (§8) never becomes `last_error`: it is the domain declining a request, not the thrall
failing at one, and a thrall that answers "no" all day is healthy. Likewise a call dropped past its
deadline (§9c) is not an error of the thrall; it has its own counter, `expired_total`.

`last_error` resets on restart, like `processed_total` - it is process-local. On an *escalation* the
thrall would die before its next scheduled beat, so it publishes one final "dying breath" heartbeat
carrying the reason and flushes it before exiting, giving the lord the *why* of the crash rather
than only the abnormal exit. It is best-effort (a hard `SIGKILL` skips it), and the reason is the
same one the caller already receives on the reply.

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

  **Honest bar: liveness overlap, not write-exclusivity.** This bounds the window in which two
  instances are *alive* to the lock TTL - and that is exactly what the soak measures (a count of
  live processes). It is **not** a strict single-writer guarantee, and a lease plus
  self-termination can never be (Kleppmann's fencing argument): between the lease expiring - a new
  holder starting - and the old, e.g. GC-paused, instance *noticing* and self-exiting, the old
  instance can still issue a write. The guarantee is "at most one live instance, overlap <= TTL",
  not "exactly one writer". Strict single-writer against a resource requires a **fencing token
  enforced at that resource**: aether already issues one - the epoch, available to a handler as
  `ctx.SingletonEpoch` (`ctx.singletonEpoch` / `ctx.singleton_epoch`), 0 for a non-singleton.
  Send it with every write and have the resource reject a lower epoch, so a stale writer is fenced
  even before it self-terminates. Runnable demo in all three languages: `examples/fencing-token/`.
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
  immediately, an unreachable KV is tolerated for the lease. A per-thrall escape hatch,
  `fencing = false`, opts a thrall out of lord-liveness fencing for exactly the case where the blast
  radius is not worth it - a stateless / read-only thrall (e.g. a poller that only publishes) whose
  orphan is harmless. It then does *not* self-terminate on a lost lease, at the cost that it **may
  outlive its lord**, so it is only for thralls that carry no ownership. It really applies to
  `local` thralls: singleton fencing is a separate mechanism and stays on, and because the lord
  (not the thrall) renews the singleton lock, a `fencing = false` singleton is still reaped when
  its lord dies - the lock stops being renewed, expires, and the singleton's own fencing acts on
  it. (2) *One lord per app*, now **enforced at startup**: the lease key is the app, so two lords
  for one app would both write it with different epochs, each lord's thralls would see the other's
  epoch and reap themselves into a crash-loop (this is not HA failover - the singleton instance
  self-terminates on the superseded epoch). So a starting lord first checks the app's lease: if
  another lord is *actively renewing* it, the new lord refuses to start with a clear error rather
  than stomp it. A live competitor is told apart from a dead predecessor (a lingering lease after a
  crash) by whether the lease is being renewed, not by holder id - a legitimate restart takes the
  lease over. Cross-node single-instance is achieved by leaf-node isolation (§11b: one lord per
  node, its own app), not by lords racing for a shared lock on one bus.
- **`temporary` semantics inside group strategies** - its interaction with `one_for_all` /
  `rest_for_one` is not fully specified.
- **Thrall state persistence** - durability today covers the *mailbox* (casts survive a crash via
  JetStream), not the *state*. Like OTP, a restart runs a clean `init` and loses in-memory state.
- **Stronger and server-side security** - the client side authenticates to an *external* bus with
  nkeys over server TLS (manifest `[nats.tls]` / `[nats.auth]`; the lord injects the credential
  paths into thralls, and the operator CLI takes `--ca`/`--nkey`). The *embedded* server can now be
  exposed on the network with server-side TLS and mandatory nkey auth (`[nats.security]`); absent, it
  stays loopback with no auth. Two tiers: one shared identity (`nkey_seed`), or three least-privilege
  roles (`lord_nkey` / `thrall_nkey` / `operator_nkey`, mutually exclusive with the shared seed) with
  a deny-based, role-scoped permission set - which is what makes `aether._lord.>` node-local *by
  permission* on a networked bus, not only by non-export (a thrall cannot drive supervision, an
  operator cannot forge control). The role boundary is lord / thrall / operator; a shared thrall
  identity does not isolate one thrall from another (that would need per-thrall identities).
  Credentials rotate without downtime: replacing a cert/key (or nkey) file in place and sending
  `SIGHUP` makes the embedded server reload it via `ReloadOptions` - a TLS rotation keeps live
  connections up, an nkey rotation closes a connection using the removed key (it reconnects with the
  new one), and structural changes (listen, role set) still need a restart. Still open: mutual TLS -
  the last increment of the same arc.
- **Cross-node placement and scheduling - deliberately outside aether.** Which node runs what,
  re-placement on node failure, capacity-aware scheduling and scaling an instance count across nodes
  are **not** aether's job: they are scheduling *policy*, the deployer / orchestrator's domain, not a
  thin mechanism over NATS. aether stays one lord per node supervising local OS processes (§11b), and
  it does not distribute code - `ctx.StartChild` runs a `cmd` that must already exist on the node
  (§12), so placement without code distribution would be half a feature anyway. Making the lord hold
  fleet topology would also make it a second source of truth, which §12/AE-030 rejected even for a
  single node. An external placement authority composes only aether's **existing** public surfaces:
  the fleet view (`aether._fleet.>` + `aether fleet`, §12) as its world-view, the per-node control
  channel (`aether._lord.ctl`, §12) as actuation, and the registry + liveness (KV `name -> {node,
  pid, status}`, heartbeats, the lord-liveness lease) for detection - exactly the external control
  plane §12 already anticipates. The isolation boundary (§11b) applies: supervision (`aether._lord.>`,
  including `ctl`) is never exported across leaf accounts, so actuation reaches only a node the
  authority connects to directly, whereas the fleet view *is* exported and crosses the boundary - the
  authority sees the whole fleet centrally but cannot drive another site's supervision through the hub.
  **Guardrail:** aether must not grow a fleet-wide desired-state store,
  a scheduler, cross-node state migration, or a cross-node authority over lords; if a deployer needs
  the runtime to carry new placement semantics (in `internal/wire`, the lord, or the SDK run-loop),
  that is a red flag to evaluate as a core change, not add quietly. Decision record: bundle AE-085.
- **Session types (typed protocols) beyond the CI slice** - a session type is a contract for a
  *conversation* between two thralls (the order, direction and mandatory replies), layered over the
  payload contract (which types one message's shape). The Tier 1 **CI slice is implemented** (AE-090):
  a binary `*.protocol.yaml` is projected onto a per-role automaton and checked at build time -
  well-formedness (every call has a mandatory reply, the conversation can terminate), duality of the
  two role projections (well-formedness of the *contract*, not of a running system), drift against the
  committed automata, and a name-existence conformance lint against a thrall's `ThrallDescribe`.
  Runnable example: `examples/session-scada/` (the pharos control-command and driver-control loops).
  **Deferred:** the runtime **monitor** (Tier 2) that would interpret the same automaton to enforce
  ordering live and catch an out-of-order or forgotten message at its first occurrence - the CI slice
  is a design-time contract only, and the transport stays untyped (`json.RawMessage`). Also deferred:
  typestate stubs, a `Sess` correlation header for multi-step cast sessions, and multiparty (3+ role)
  protocols, which decompose into binary links per hop. The pi-calculus is only the semantic
  justification (a subject is a channel, a request/reply inbox a one-shot channel), not an API.

Note: several items listed as future work in earlier drafts are now implemented - JetStream durable
mailboxes (`durable`), within-app singletons with thrall-level fencing (`scope`, §12; these are
single-instance *within an app*, not cluster-wide), client-side nkey auth over server TLS
(`[nats.tls]` / `[nats.auth]`), structured logs + a Prometheus `/metrics` endpoint + the observer
dashboard (§13b), fleet health across lords and leaf sites (§12, `[observability] fleet_health`), the
embedded leaf spoke (`[nats.leaf]`, §11b), the secured embedded server (`[nats.security]`: a networked
bind with server TLS + nkey, least-privilege lord/thrall/operator roles making `aether._lord.>`
node-local by permission, and SIGHUP credential rotation - §11b/§14), and a Docker image and recipe
(see the README). The chaos and soak suite (`scripts/soak.sh`, out of CI) exercises these under failure.

---

## 15. Summary of decisions

| Area | Decision |
|---|---|
| Runtime | a Go binary, embedded NATS (default) or an external cluster (config switch) |
| Wire format | a JSON envelope, `kind` + `op` dispatch; it grows only additively, so peers of different ages interoperate (§7) |
| Call outcomes | a reply, a typed **refusal** (`refused` + a stable `code`: the domain said no, the thrall is healthy), a failure (`handler_error`), or a crash (`escalated`) - told apart by `error.type`, never by message text (§7, §8) |
| Call deadline | the caller stamps when it stops waiting; a thrall drops an expired call before handling it (250 ms skew tolerance, `AETHER_DEADLINE_SKEW_MS`); complements, does not replace, an idempotency key (§9c) |
| Client | calling needs only a NATS connection: one client implementation behind `ctx.call`, the package-level call and standalone code, in all three SDKs; the Go SDK re-exports the wire contract so nothing outside mirrors it (§8) |
| SDK home base | TS/Bun (`@hamicek/aether`); plus Python and Go |
| SDK behaviours | GenServer thrall (`Def`/`Start`), a state-machine thrall (`FSM`/`StartFSM`, a `gen_statem` analogue: states, guards, state timeouts) and an event manager (`EventManager`/`StartEvent`, a `gen_event` analogue: an ordered set of handlers, one event fanned out to all) - all on the same serialized mailbox; each behaviour stays domain-neutral, applications build on top |
| Topology | a declarative `aether.toml`; behavior in code |
| Lord | variant A (dumb) + a heartbeat/drain contract ready for B |
| Mailbox | core NATS ephemeral, with an optional JetStream durable mailbox (`durable = true`) |
| Observability | structured logs + a Prometheus `/metrics` endpoint + heartbeat miss detection + `trace` propagation + the per-lord observer dashboard + fleet health across lords (§13b); `$SYS` liveness still planned |
| NATS features | do NOT hide them from thralls - `ctx.nats` is freely available |
| Multi-node | one lord per NATS node; distribution at the bus (leaf nodes + per-site accounts, §11b), not several lords over one bus; a within-app singleton via a KV-CAS lock + liveness fencing; fleet health across lords/sites |
| Security | client-side auth to an external bus (`[nats.tls]` / `[nats.auth]`); the embedded server secured for a networked bind (`[nats.security]`: server TLS + nkey, one shared identity or three least-privilege roles making `aether._lord.>` node-local by permission, SIGHUP credential rotation, §11b/§14); mutual TLS still open |
