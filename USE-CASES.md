# Use cases

> These are the shapes aether is built for. Each links to a **runnable example** in the repo - this
> is "here is how you would structure it", not "imagine you could". Where the runtime is proven but
> the domain is your application's job, that is called out honestly.

## 1. A polyglot group of services

**You have** a gateway and a handful of workers, possibly in different languages, that must talk to
each other and be supervised together.

**You structure it** as one lord plus thralls (Go / TS / Python side by side), talking over `call`
(sync request/reply) and `cast` (fire-and-forget). Want a pool? `replicas = N` puts them in a NATS
queue group that load-balances for free.

**Runs:** [`examples/counter`](./examples/counter) (a polyglot demo - the gateway sums a counter
across three languages at once).

## 2. An edge / IoT / SCADA site

**You have** devices behind a protocol (Modbus / OPC-UA / MQTT), telemetry flowing from them, and you
want to keep a current picture of it, watch threshold alarms, and show it live in a browser.

**You structure it** as: a driver = a *custom edge* (it owns the socket, pushes in from outside) ->
a *site thrall* (a coarse-grained process image on a serialized mailbox, so no locks) -> a threshold
alarm -> an *SSE edge* (live push to the UI). The site holds the state, the drivers feed it.

**Runs:** [`examples/scada-spike`](./examples/scada-spike) (measured: >=50,000 values/s with no loss,
alarm p99 < 1 ms) + [`examples/live-dashboard`](./examples/live-dashboard) (SSE) +
[`examples/webserver-custom`](./examples/webserver-custom) (a custom edge).

**Honestly:** the runtime is proven by the spike; the **domain** (tags, alarms as a concept, protocol
drivers, the HMI) is built *on top of* aether, not inside it.

## 3. A hub-and-spoke fleet

**You have** a center and several sites that must **not** see one another, and yet you want a
fleet-wide view of their health from the center.

**You structure it** at the NATS layer: each site is a *leaf node* bound to its own account
(`[nats.leaf]`, embedded, a single binary per site), and the hub imports only the data plane. The
isolation is *structural* (an account is a hard boundary), not "careful code". `aether fleet`
assembles a health view across the sites, even across the leaf boundary.

**Runs:** [`examples/hub-spoke-spike`](./examples/hub-spoke-spike) (distribution + isolation, proven)
+ [`examples/nats-leaf`](./examples/nats-leaf) (a single-binary site).

**Bonus:** partition-tolerant - a site keeps running when the center is unreachable and reconnects on
its own when the center returns.

## 4. A durable task / worker pipeline

**You have** a stream of tasks that must not be lost, want to process them in parallel, and survive a
restart without losing state.

**You structure it** as `durable = true` thralls (casts survive a process crash via JetStream) +
`replicas` (a queue group, a pool of workers) + `event_log` (state is rebuilt after a restart by
replaying the log, not from a snapshot - "the log is truth").

**Runs:** [`examples/counter/aether-durable.toml`](./examples/counter) (durable mailbox) +
[`examples/eventsourced`](./examples/eventsourced) (state survives a restart by replay).

**Two mechanisms sharpen it** for at-least-once delivery: mark the worker `Idempotent` so a
redelivered task is deduplicated (a duplicate cast is skipped, a duplicate call returns the first
reply - [DESIGN.md §13c](./DESIGN.md)), and return `Escalate(reason)` from the handler to let a
worker crash on a task it cannot process, so the lord restarts it clean instead of the code reaching
for `os.Exit` ([DESIGN.md §8](./DESIGN.md)).

**One caveat on `replicas`:** a queue group spreads each task to *one of* the workers, so it fits
**stateless** work (or work partitioned by an external key). A *stateful* actor whose in-memory state
must be authoritative - one whose identity matters, like a per-entity aggregate - must be a **single
instance** (`scope = "singleton"`, or one thrall per key), not a replica pool: pooling would split
that entity's messages across replicas that each hold a divergent copy. `replicas` scales the
stateless tier, singletons own the stateful one.

## 5. An HTTP / SSE front over a supervised backend

**You have** a backend of several supervised processes and want a REST/HTTP front plus live push to a
browser.

**You structure it** as an `[[edge.http]]` ingress (maps a route to a thrall operation - no code) +
an SSE edge for live push. The edges are ordinary thralls (the lord supervises them the same way),
and the state lives in the thralls behind them.

**Runs:** [`examples/webserver`](./examples/webserver) (HTTP ingress) +
[`examples/live-dashboard`](./examples/live-dashboard) (live push, scope-safe per client).

---

## When NOT aether

- You need **millions** of cheap in-VM actors in one language -> reach for BEAM (Erlang/Elixir).
- A single stateless web service -> just run it. aether earns its keep once you have **several**
  processes that must be supervised and talk to each other reliably.
