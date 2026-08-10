# FSM behaviour demo - turnstile

A domain-neutral demonstration of the **FSM thrall behaviour** (`StartFSM`), aether's second
behaviour alongside the GenServer thrall. A classic turnstile:

- **locked** - a `coin` unlocks it
- **unlocked** - a `push` locks it again (counting pushes), or a 5s idle **state timeout**
  (`autolock`) locks it on its own

Events are ordinary casts/calls - the wire is unchanged, so any GenServer caller reaches it.

## Run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/turnstile ./examples/fsm

cd examples/fsm
AETHER_LOG_FORMAT=json AETHER_LOG_LEVEL=debug ../../bin/aether up -f aether.toml
```

In another shell:

```bash
cd examples/fsm
../../bin/aether call turnstile _state          # {"state":"locked"}
../../bin/aether cast turnstile coin            # -> unlocked (see the transition log)
../../bin/aether call turnstile _state          # {"state":"unlocked"}
../../bin/aether call turnstile push            # -> locked, replies the push count
# unlock again and wait 5s without pushing:
../../bin/aether cast turnstile coin
sleep 6
../../bin/aether call turnstile _state          # {"state":"locked"}  (auto-locked by the state timeout)
```

## What to look for

The `aether up` log shows each transition and the reserved `_state` op answering the current
state without any handler code:

```json
{"level":"INFO","msg":"fsm transition","name":"turnstile","from":"locked","to":"unlocked"}
{"level":"INFO","msg":"fsm transition","name":"turnstile","from":"unlocked","to":"locked"}
```

An unknown event in a state is rejected (`no_transition`) rather than crashing:

```bash
../../bin/aether call turnstile push            # while locked -> error: no_transition
```
