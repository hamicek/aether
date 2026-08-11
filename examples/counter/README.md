# Counter demo - polyglot GenServer thrall

The counter is the simplest thrall: `number` state with `get` / `inc` / `dec`. The same
GenServer behaviour is written three times - `counter.ts`, `counter_py.py`, `counter_go.go` -
running side by side under one lord, on the same ether, with the same JSON contract. To the
`gateway.ts` orchestrator they differ only in name.

Each thrall takes its name from `AETHER_NAME` (injected by the lord from the manifest), so the
same code runs as `counter`, `counter-single`, or any name a manifest gives it - which is why
every manifest below works with any of the three languages.

## Run

```bash
export GOTOOLCHAIN=local
# 1) runtime + Go counter thrall
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/counter-go ./examples/counter

# 2) TypeScript workspace
bun install

# 3) Python thrall dependency
cd examples/counter
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cd ../..

# 4) run the default polyglot demo
cd examples/counter
../../bin/aether up -f aether.toml
```

Other manifests in this directory (durability, singleton failover, supervision strategies,
observability, secured external cluster) are listed with a one-line description in the root
`README.md`. They all run with any of the three languages; the external ones need a standalone
NATS on port 7390 (see the root README).

## What to look for

`gateway.ts` probes all three counters once per second and prints them side by side, so you can
watch three languages answer the same call identically:

```
probe #3: counter=3  counter-py=3  counter-go=3
```

Bring the demo down (Ctrl-C) and the lord drains each thrall; every counter prints its
`terminate` line with the last state.
