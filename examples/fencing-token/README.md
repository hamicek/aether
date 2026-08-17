# Write-side fencing token demo

Singleton fencing (`scope="singleton"`) only bounds **liveness** overlap: it guarantees at most
one *live* instance, with any overlap bounded by the lock TTL. It is **not** a strict
single-writer guarantee - a GC-paused old instance can still issue a write between losing its lock
and noticing (see DESIGN "Singleton fencing"). To get strict single-writer against a resource, use
the epoch as a **write-side fencing token**: stamp it on every write, and have the resource reject
a lower epoch.

This demo shows exactly that. A singleton `writer` thrall reads its fencing epoch from
`ctx.SingletonEpoch` / `ctx.singletonEpoch` / `ctx.singleton_epoch` (injected by the lord) and
sends it with each write to a **resource** - here a NATS KV bucket holding `{value, epoch}` that
rejects a write carrying a lower epoch than it has already seen. A real resource would be a PLC, a
driver, or a DB enforcing the same monotonic-epoch check.

The same writer is written in all three languages (`main.go`, `writer.ts`, `writer.py`) with
identical behaviour. Pick one manifest; the interaction is the same.

## Run

Build the runtime once, then run **one** of the three variants.

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
```

**Go** (`aether.toml`):

```bash
mise exec go@latest -- go build -o bin/writer ./examples/fencing-token
cd examples/fencing-token
../../bin/aether up -f aether.toml
```

**TypeScript** (`aether-ts.toml`): `bun install` once at the repo root, then
`cd examples/fencing-token && ../../bin/aether up -f aether-ts.toml`.

**Python** (`aether-py.toml`):

```bash
cd examples/fencing-token
python -m venv .venv && .venv/bin/pip install -r requirements.txt
../../bin/aether up -f aether-py.toml
```

In another shell (identical for every variant):

```bash
cd examples/fencing-token
../../bin/aether call writer write       '{"value": "A"}'   # -> {"stored":true,"epoch":N}
../../bin/aether call writer read                            # -> {"value":"A","epoch":N}
../../bin/aether call writer write-stale  '{"value": "B"}'   # -> {"stored":false,...}  (fenced)
../../bin/aether call writer read                            # -> {"value":"A","epoch":N}  (unchanged)
```

## What to look for

`write` stamps the live epoch (`N`) and the resource accepts it. `write-stale` simulates what a
fenced-out *old* instance would do - it writes with an older epoch (`N-1`) - and the resource
**rejects** it (`stored:false`), so the value stays what the current writer wrote. That rejection
is the guarantee singleton fencing alone does not give: it does not depend on the old instance
having noticed it lost the lock.

**Honesty:** aether only *issues* the epoch. Exclusivity is enforced by the resource's guard (the
`writeFenced` check in this example), not by the runtime. Two `write`s with the same live epoch
both succeed - the epoch fences *older* writers, it is not a mutex.
