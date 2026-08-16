# Hub-spoke multi-node spike on aether - report (AE-051)

**Question:** can aether host a "one center + several sites" topology, where each
site is a separate node (its own lord + its own NATS) linked to the center, yet the
sites cannot see one another? This is the shape SCADA needs: a central node plus
individual sites that must not "hear" each other. This spike built that topology from
hand-written NATS configuration (aether as a client, no product code changes) and
asserted distribution and isolation.

## Verdict: **works, with a clean division of labour**

Yes. A hub with two spokes connected as **NATS leaf nodes**, each spoke bound to its
own **NATS account**, gives exactly the wanted behaviour:

- the center reaches both sites (cross-node `call`/`cast`),
- the sites are isolated from each other **by construction** (different accounts, no
  cross-import), not by application discipline.

The key finding is *where the boundary lives*: distribution and isolation are entirely
a **NATS-layer** concern (leaf nodes + accounts + service exports/imports). aether does
not - and should not - reimplement any of it. For the SDK, a cross-node call is an
ordinary call; it never learns that a leaf or an account exists.

## Topology

Three NATS servers, one lord per server, sites attached to the hub as leaf nodes.
App name is `demo` on every node - the isolation unit is the **account**, not the app
name.

```
                       HUB server (client :7390, leaf :7391)
             account HUB    : gateway thrall; imports counterA.> (SITE_A), counterB.> (SITE_B)
             account SITE_A : leaf bind + export aether.demo.counterA.>
             account SITE_B : leaf bind + export aether.demo.counterB.>
                       ▲ leaf (SITE_A)              ▲ leaf (SITE_B)
        SPOKE-A server (:7392)               SPOKE-B server (:7393)
        account SITE_A, JS domain "sa"        account SITE_B, JS domain "sb"
        lord + thrall counterA                lord + thrall counterB
```

The hub imports each site's data plane, so the gateway on the hub reaches both. SITE_A
and SITE_B import nothing from each other, so a site cannot reach or observe a foreign
site.

## What was asserted (see `scripts/hub-spoke-spike.sh`, reproducible, exits non-zero on failure)

Run 3× back to back, deterministic, no state accumulation:

1. **Distribution** - after seeding `counterA=3` (locally on spoke-A) and `counterB=5`
   (locally on spoke-B), the gateway thrall on the hub returns
   `{"counterA":3,"counterB":5}` by calling both sites cross-node, and a direct call
   from the hub to each site returns the same. The center sees the real, live state of
   both sites across the node boundary.
2. **Isolation (negative)** - a call from spoke-A to `counterB` fails with *no
   responders*: the site cannot reach a foreign site.
3. **Isolation (positive)** - a subscriber on spoke-A watching `aether.demo.counterB.>`
   receives **0** messages while spoke-B has live traffic, whereas a control subscriber
   on spoke-B sees that traffic. This is positive proof of account isolation, not merely
   the absence of a reply.

## Findings

### 1. Only the data plane crosses the boundary; supervision stays node-local

aether's subjects split cleanly (`internal/wire/subjects.go`):

- **data plane** `aether.<app>.<name>.call|cast|info` - the only subjects that need to
  cross between accounts (exported by the site, imported by the hub);
- **supervision** `aether._lord.*` (ctl/heartbeat/events) and the JetStream **KV
  buckets** `aether_registry`, `aether_singletons`, `aether_lords` - all provisioned by
  each lord in its own local NATS and **never exported**.

This is what makes the 1-lord-per-node model tidy: each lord supervises only its own
node, and no supervision traffic leaks between sites. Only the deliberately exported
data-plane subjects are visible across the boundary.

### 2. `ctx.Call` works cross-node transparently - within a shared app namespace

Because all nodes share the app namespace `demo` and differ only by thrall name, the
gateway calls a remote thrall with a plain `ctx.Call("counterA", "get", ...)`; the SDK
builds `aether.demo.counterA.call` and NATS routes it over the import + leaf. The SDK is
oblivious to the topology.

The flip side is a real limitation: `ctx.Call` is **bound to the caller's own app**
(`doCall` uses `wire.Call(app, target)` with the caller's `AETHER_APP`). There is no way
for a thrall in app X to call a thrall in app Y. So this transparent cross-node pattern
requires the shared-namespace design; a per-site *app name* scheme would need a
raw-NATS/edge caller or a future SDK affordance (a cross-app or "remote" call). This is
the concrete ergonomics question for any future hub-spoke feature.

### 3. Embedded NATS cannot be a leaf node → external NATS per node

aether's embedded server is not configured as a leaf, so a real hub-spoke topology uses
an **external** `nats-server` per node (`[nats] mode = "external"`), with aether
connecting as a client. This is a fine model (and matches the existing
`aether-external*.toml` examples), but it means embedded mode is single-node only. A
future option would be to let the embedded server take leaf-node configuration.

### 4. Auth: `no_auth_user` for the local lord, leaf authorization for the account bind

aether's external auth supports **nkey only** (`internal/ether/ether.go`). The spike
sidesteps client auth entirely: each server maps unauthenticated local connections to
its single local account via `no_auth_user`, so the lord connects with no credentials
and lands in the right account. The **leaf** connections authenticate (username/password
in `leafnodes`) and that is what binds each spoke to its hub account. A production
deployment would use nkeys/TLS per the existing security work (AE-009/029); the spike
keeps it minimal.

### 5. Durable across the node boundary (phase 2, findings only - not exercised)

The durable mailbox is a JetStream stream provisioned by the lord in its **local** NATS
(`aether_<app>_<name>`, WorkQueue). Over leaf nodes JetStream requires a **domain per
node** (verified against NATS docs; the spike gives each server a distinct domain
`hub`/`sa`/`sb`, and without distinct domains the leaf's and hub's JetStream collide).
Consequences for a hub-spoke deployment, to decide when the need is real:

- **Durable is naturally per-site and local.** A site's durable mailbox and its KV
  fencing (singleton lock, lord-liveness lease) live in that site's JetStream domain and
  are account-local. Singleton/lord fencing therefore works **within** a node, not
  across nodes - which is consistent, since each app is single-node.
- **Guaranteed cross-node delivery hub↔site is not free.** Domains are an *addressing*
  tool, not isolation; to move stream data across the leaf you use JetStream
  **mirror/source** (a hub stream sourcing a site's stream, or vice versa) with
  domain-aware addressing. That is real work and a real design decision.

Recommended framing (open question for a follow-up): keep durable **per-site and local**
(the site processes its own backlog and pushes results up over core NATS), and only
introduce mirror/source if a use-case genuinely needs the center to durably own every
site value across an outage. Don't build it speculatively.

## Scope (deliberately not covered)

- Cross-node durable delivery (mirror/source) - documented above, not built.
- Failover of a site's standby across nodes, partition behaviour under leaf loss - these
  belong to the multi-node testing follow-up (inbox `ae-viceuzlove-testovani-vice-lordu`).
- Production auth/TLS on the leaf and client connections (AE-009/029 covers the pieces).
- More than two spokes / scale - the pattern generalises but was not measured.
