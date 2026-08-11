# Event-sourced rebuild demo - account

A domain-neutral demonstration of **event-sourced rebuild**: a bank account whose balance
survives a restart by replaying its event log, with no in-memory snapshot. In `init` the
thrall rebuilds the balance from its event log; each `deposit` / `withdraw` appends an event
and updates the balance.

The event log is an opt-in retention JetStream stream (`event_log = true` in the manifest),
separate from the durable mailbox. With a persistent embedded JetStream (`store_dir`), the log
- and therefore the rebuilt state - outlives stopping and starting `aether up`.

The same account is written in all three languages - `main.go`, `account.ts`, `account.py` -
with identical behaviour. Pick one manifest below; the interaction afterwards is the same.

## Run

Build the runtime once, then run **one** of the three variants.

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
```

**Go** (`aether.toml`):

```bash
mise exec go@latest -- go build -o bin/account ./examples/eventsourced
cd examples/eventsourced
../../bin/aether up -f aether.toml
```

**TypeScript** (`aether-ts.toml`):

```bash
bun install
cd examples/eventsourced
../../bin/aether up -f aether-ts.toml
```

**Python** (`aether-py.toml`):

```bash
cd examples/eventsourced
python -m venv .venv
.venv/bin/pip install -r requirements.txt
../../bin/aether up -f aether-py.toml
```

In another shell (identical for every variant):

```bash
cd examples/eventsourced
../../bin/aether cast account deposit  '{"delta": 100}'
../../bin/aether cast account withdraw '{"delta": 30}'
../../bin/aether call account balance                     # -> 70
```

## What to look for

Stop `aether up` (Ctrl-C) and start it again, then read the balance:

```bash
../../bin/aether call account balance                     # -> 70, rebuilt from the event log
```

The balance is the same after the restart even though nothing snapshotted the in-memory state -
it was reconstructed by replaying the appended events. (Deleting `./.aether-store` wipes the
persisted log, so the next start rebuilds from an empty log back to 0.)
