# Event manager demo - alarms

A demonstration of the **event-manager thrall behaviour** (`startEvent` / `start_event` /
`StartEvent`), aether's third behaviour alongside the GenServer thrall and the FSM. It is
aether's analogue of OTP's `gen_event`.

The `alarms` thrall holds two handlers, and **one event reaches both**, in registration order,
on a single serialized mailbox:

- **audit** - counts every alarm and logs the running total (its own state)
- **pager** - reacts only to a high-severity temperature, logging a would-page line

This is what raw NATS fan-out (N independent subscribers) does not give you: co-located,
ordered handlers that each keep their own state. Events are ordinary casts, so anything can
raise one - and a handler that throws is logged and skipped, leaving the others to run.

## Run

Build the runtime once, install the workspace deps, then bring the manifest up:

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
bun install

cd examples/eventbus
AETHER_LOG_FORMAT=json AETHER_LOG_LEVEL=info ../../bin/aether up -f aether.toml
```

In a second shell, raise a few events and watch **both** handlers react:

```bash
cd examples/eventbus
../../bin/aether cast alarms door_open '{"room":"lab"}'
../../bin/aether cast alarms temp_high  '{"celsius":91}'
```

The first cast logs a single `alarm audited` line (total 1); `pager` ignores it. The second
logs `alarm audited` (total 2) **and** a `would page on-call` warning from `pager` - one event,
two ordered reactions, each with its own state.

## Try it

Send `temp_high` a few more times: the audit total keeps climbing while the pager fires each
time - the two handlers advance their own state independently.
