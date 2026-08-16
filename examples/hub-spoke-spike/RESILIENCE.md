# Hub-spoke resilience - report (AE-053)

**Question:** the AE-051 spike proved the hub-spoke happy path (distribution + isolation). Does the
model also **survive failure** - a site cut off from the center, and a site's lord dying - the way
SCADA needs (a site must keep running when the WAN drops, and a crash must not spread)?

## Verdict: **it survives, and recovers on its own**

A site keeps serving locally while the center is unreachable and re-joins the moment the center
returns; a site lord's death is bounded to that site by the existing lord-liveness fencing. Nothing in
the model needed changing - this is a validation, with no product code touched.

## What was asserted (`scripts/hub-spoke-resilience.sh`, self-asserting, exit 0; run repeatedly clean)

1. **Center outage + recovery.** The center is cut off by killing the hub NATS outright; with it down,
   site A still answers a local call (`counterA == 3`) and site B likewise - each site keeps running
   independently of the center. After the hub NATS is restarted, the leaf links reconnect **on their
   own** and the center reaches the sites again - recovery completed within ~1 second in every run
   (the script times it to whole-second resolution, always reporting 0-1 s, well inside a 45 s budget).
2. **Site lord death is bounded.** Killing site A's lord (SIGKILL; the site's NATS stays up) makes
   `counterA` **self-exit** within the lease window (AE-031 lord-liveness fencing) - a call to it then
   fails (the thrall is gone) - while the **center and site B keep running untouched**.

## Findings

### 1. A site is partition-tolerant and self-healing

Because each site's lord connects to its **own local NATS** (not the hub), losing the hub only drops
the leaf link; the site's lord and thralls keep serving local traffic. When the hub returns, the
spokes' leaf remotes reconnect automatically (aether/NATS reconnect, no manual step) and cross-node
reach is restored within ~1 second (whole-second timing). This is exactly the SCADA property wanted:
a site runs through a center/WAN outage and rejoins cleanly.

### 2. A lord's death is fenced to its own node

The AE-031 lord-liveness fencing (generalized to all thralls) works unchanged in the multi-node
topology: when a site's lord dies, its thralls self-exit via the KV lease in that site's **local**
JetStream domain, and the failure does not touch the center or other sites. The blast radius of a
lord failure is one node.

### 3. Cross-node fencing: answered - it is node-local by construction, and that is correct

The open question from AE-052 (§7): does cross-node fencing make sense? **No - and it is not needed.**
The fencing/lock KV buckets (`aether_lords`, `aether_singletons`) are provisioned by each lord in its
**own local NATS**, inside that node's JetStream domain (`sa`/`sb`/`hub`), and are never exported
across accounts (AE-051/AE-052). Fencing is therefore **per-node by construction** - which is exactly
right for the hub-spoke model, where **a site is single-node and owns its own singletons**. A
cross-node singleton (one that could run on either of two sites) is not part of the model; making it
work would require a shared KV across accounts/domains, deliberately out of scope. The site-lord-death
test above confirms the node-local lease is what fences a thrall, with no cross-node coordination
involved.

## Reproduce

```
scripts/hub-spoke-resilience.sh
```

Builds the binaries, starts the AE-051 topology (hub + 2 leaf-node sites), injects the two failures
above, and asserts recovery/fencing. Exit 0 means every assertion passed. Requires `nats-server` on
PATH. Out of CI (it drives live multi-node failure).

## Scope (deliberately not covered)

- A "leaf link drops while the hub stays up" test via network manipulation - the hub kill/restart is
  a realistic proxy and is what the script uses.
- Building cross-node fencing (answered above: out of the model, not built).
- Scaling to more than two sites (the pattern generalizes; not measured here).
- Durable delivery across the node boundary (phase 2, per the AE-051 REPORT).
