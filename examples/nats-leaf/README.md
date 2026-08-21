# nats-leaf

An **embedded spoke** joined to a hub with `[nats.leaf]` (AE-067). Each site runs its own
aether with an *embedded* bus, and the `[nats.leaf]` section makes that bus a leaf node of a
central hub - bound into the site's own NATS account, with its own JetStream domain.

The point of this example is the **ergonomics**: compare it with `examples/hub-spoke-spike/`,
where every spoke needs a hand-written `nats/spoke-*.conf`. Here the spoke side is just a manifest:

```toml
[nats]
mode = "embedded"

[nats.leaf]
remote   = "nats-leaf://127.0.0.1:7391"   # the hub's leafnode listener
site     = "SITE_A"                        # the account this node binds to
domain   = "sa"                            # JetStream domain, unique per node
user     = "leafA"                         # dev credential; prod: nkey = "/path/to/seed"
password = "leafA"
```

aether renders the proven spoke-side NATS config from that intent (a per-site account, a service
export of the app's data plane `aether.<app>.>`, the leaf link, the per-node JetStream domain).
The supervision subjects `aether._lord.>` are simply never exported, so they stay node-local by
construction - no allow/deny rules.

**Deliberately asymmetric.** aether ergonomizes only the **spoke** side. The **hub** side - the
multi-account import matrix in `nats/hub.conf` - stays operator-authored NATS config
(bring-your-own). aether connects to the hub as an ordinary client; it never generates the hub.

## Run

Needs `nats-server` on PATH (any recent build) and the aether + counter binaries. From the repo root:

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/counter-go ./examples/counter

cd examples/nats-leaf
mkdir -p .run/hub-js
nats-server -c nats/hub.conf -sd "$PWD/.run/hub-js" &   # the operator-run hub
../../bin/aether up -f aether-spoke-a.toml &             # site A: embedded bus, leaf of the hub
```

The spoke logs `ether running ... mode=embedded` and the hub logs `Leafnode connection created`.
Now reach site A's counter **from the hub**, cross-node - the request routes over the leaf into the
site's account:

```bash
../../bin/aether cast --url nats://127.0.0.1:7390 --app sitea counter inc
../../bin/aether cast --url nats://127.0.0.1:7390 --app sitea counter inc
../../bin/aether call --url nats://127.0.0.1:7390 --app sitea counter get   # -> 2
```

Bring up `aether-spoke-b.toml` the same way for a second, isolated site (account SITE_B, domain
`sb`, app `siteb`): the hub reaches both, but the sites cannot see each other.

Tear down with `kill %1 %2` (or kill the two background PIDs) and `rm -rf .run`.

## Layout

- `aether-spoke-a.toml`, `aether-spoke-b.toml` - the spokes: `mode = "embedded"` + `[nats.leaf]`.
  **No spoke NATS config** - aether generates it.
- `nats/hub.conf` - the only hand-written NATS config: the operator-run hub (accounts, leafnode
  listener, per-site service imports, JetStream domain).
- The spokes run the Go counter thrall (`../../bin/counter-go`, built from `examples/counter`).

## Where the proof is

This directory demonstrates the manifest form and is runnable, but it is not an automated test.
The end-to-end guarantees - data plane crosses the leaf, sites are isolated, supervision stays
node-local, JetStream runs under the per-node domain - are asserted in
`internal/ether/leaf_e2e_test.go`, and the underlying topology is proven in
`examples/hub-spoke-spike/` (see its `REPORT.md`).
