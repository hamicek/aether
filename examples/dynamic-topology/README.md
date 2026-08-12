# Dynamic topology demo

Shows how a supervising thrall **owns its dynamic topology**. A static `manager` spawns a
set of worker children (`worker-1..3`) from its own `init`, and re-applies that set on a
`reconcile` cast.

Dynamic children (`ctx.StartChild`) do **not** survive a lord restart by design: the lord
is an OS process supervisor, so a restart takes the whole process group - including the
manager - with it (see `DESIGN.md`, section 12). Re-establishing the topology is the
**owner's** job, not the lord's. The manager does it by re-spawning its workers from
`init`, which runs again every time the lord comes back.

This is safe because `StartChild` is **idempotent on name**: spawning a worker that is
already under supervision is a no-op, not an error and not a duplicate. So the manager can
call it blindly from `init` and re-apply on `reconcile` as often as it likes.

The same manager/worker behaviour is written in all three languages - `main.go`,
`manager.ts`, `manager.py` - with identical behaviour. Pick one manifest below.

## Run

Build the runtime once, then run **one** of the three variants.

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
```

**Go** (`aether.toml`):

```bash
mise exec go@latest -- go build -o bin/dyntopo-demo ./examples/dynamic-topology
cd examples/dynamic-topology
../../bin/aether up -f aether.toml
```

**TypeScript** (`aether-ts.toml`):

```bash
bun install
cd examples/dynamic-topology
../../bin/aether up -f aether-ts.toml
```

## What to look for

In another shell, list the supervised thralls - the manager plus the three workers it
spawned from its init:

```bash
cd examples/dynamic-topology
../../bin/aether ps
```

Each worker answers a `ping` call:

```bash
../../bin/aether call worker-1 ping '{}'
# -> "pong from worker-1"
```

Ask the manager to reconcile. The workers already run, so this is a no-op - **no
duplicates appear** and the worker PIDs do not change:

```bash
../../bin/aether cast manager reconcile '{}'
../../bin/aether ps        # still exactly manager + worker-1..3
```

**Restart survival.** Stop `aether up` (Ctrl-C) and start it again. The manager's `init`
runs again and re-spawns `worker-1..3`, so `aether ps` shows the same topology as before -
without any lord-side persistence. The owner rebuilt it.
