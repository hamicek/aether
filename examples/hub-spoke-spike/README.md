# hub-spoke-spike

A multi-node spike (AE-051): one **hub** and two **spokes**, each a separate node with
its own NATS server and its own lord, the spokes attached to the hub as **NATS leaf
nodes** with **account** isolation. It proves that the center can reach both sites
cross-node while the sites cannot see each other.

This is a topology/verification spike, not product code - it lives entirely in this
directory plus `scripts/hub-spoke-spike.sh`, and it is out of CI (it runs live servers).
Read **REPORT.md** for the verdict and findings.

## Run

```
scripts/hub-spoke-spike.sh
```

It builds the binaries, starts three `nats-server` instances and three lords, seeds the
site counters, asserts distribution and isolation, and tears everything down. Exit 0
means every assertion passed. Requires `nats-server` on PATH (see the repo CLAUDE.md,
"External NATS pro dev").

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

While the servers are up you can inspect the topology directly:

```
# leaf connections established (hub side)
grep -i leafnode examples/hub-spoke-spike/.run/hub.log

# center reaches a site; flags come BEFORE the positional args
bin/aether call --url nats://127.0.0.1:7390 --app demo counterA get

# a site cannot reach a foreign site (no responders)
bin/aether call --url nats://127.0.0.1:7392 --app demo counterB get
```
