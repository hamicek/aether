# hub-spoke-spike

A multi-node spike (AE-051): one **hub** and two **spokes**, each a separate node with
its own NATS server and its own lord, the spokes attached to the hub as **NATS leaf
nodes** with **account** isolation. It proves that the center can reach both sites
cross-node while the sites cannot see each other.

This is a topology/verification spike, not product code - it lives entirely in this
directory plus `scripts/hub-spoke-spike.sh` (happy path) and `scripts/hub-spoke-resilience.sh`
(failure), and it is out of CI (it runs live servers). Read **REPORT.md** for the
distribution/isolation verdict and **RESILIENCE.md** for the behaviour under failure.

## Run

```
scripts/hub-spoke-spike.sh        # distribution + isolation (AE-051)
scripts/hub-spoke-resilience.sh   # center outage + site lord death (AE-053)
```

`hub-spoke-spike.sh` builds the binaries, starts three `nats-server` instances and three
lords, seeds the site counters, asserts distribution and isolation, and tears everything
down. `hub-spoke-resilience.sh` reuses the same topology and injects failure: it asserts
that a site keeps serving while the hub is down and recovers on reconnect, and that a
site lord's death is fenced to its own node. Exit 0 means every assertion passed. Both
require `nats-server` on PATH - install any recent build (https://docs.nats.io/running-a-nats-service/introduction/installation).

## Layout

- `nats/hub.conf`, `nats/spoke-a.conf`, `nats/spoke-b.conf` - the three NATS servers:
  accounts, leaf nodes, service exports/imports, JetStream domains.
- `aether-hub.toml`, `aether-spoke-a.toml`, `aether-spoke-b.toml` - one manifest per
  node (`[nats] mode = "external"`), each with its thrall.
- `gateway.go` - the thrall on the hub; its `check` op calls both sites cross-node.
- `cmd/probe/` - a tiny subscriber used for the positive isolation proof (eavesdrop).
- counters reuse `../counter` under the names `counterA` / `counterB` (via `AETHER_NAME`).

## Ports

Hub client `7390`, hub leaf `7391`, spoke-A `7392`, spoke-B `7393` (aether dev block
7390-7394).

## Manual checks

While the servers are up you can inspect the topology directly (paths are
repo-root-relative; the script builds its own `aether` into the spike's `bin/`):

```
# leaf connections established (hub side)
grep -i leafnode examples/hub-spoke-spike/.run/hub.log

# center reaches a site; flags come BEFORE the positional args
examples/hub-spoke-spike/bin/aether call --url nats://127.0.0.1:7390 --app demo counterA get

# a site cannot reach a foreign site (no responders)
examples/hub-spoke-spike/bin/aether call --url nats://127.0.0.1:7392 --app demo counterB get
```
