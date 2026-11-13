# session-scada - typed protocols across thralls (CI slice)

A session type is a contract for a *conversation*: the order, direction and mandatory replies
between two thralls, not just the shape of one message. This example carries two real SCADA
conversations (from the pharos project) as session types and checks a thrall against them.

- **control-command** - the safety-critical command lifecycle: `command` (a call, so a reply is
  mandatory) answered by `authorized | rejected`, then `execute` answered by `confirmed | failed`.
  "confirmed" cannot precede "authorized": it only exists in the sub-flow after authorization.
- **driver-control** - the command/ack scan loop: each round the collector chooses `write` or
  `health` (internal choice) and the device answers (external choice); `write` may `applied` (loop)
  or `fault` (stop). The device's reading/telemetry stream is deliberately *not* here - a broadcast
  with no counterpart is a payload-contract shape, not a conversation (DESIGN, session types §15).

## How it fits the existing flow

Each protocol is one `*.protocol.yaml`, the single source of truth. Its ops reference the same
payload schemas the payload contract already uses (`payload: command.schema.json`), so the two
contracts compose: the payload contract types the nouns, the protocol orders them in time.

The generator projects each protocol onto a per-role automaton and emits it as JSON under
`gen/automaton/`. That JSON is the one language-neutral artifact: a Go, TS or Python thrall checks
the same contract against it, which is how a custom driver and the core stay in protocol lockstep.

```
control-command.protocol.yaml  (source of truth)
  -> gen/automaton/<protocol>.<role>.json   (committed, derived; read by every SDK)
```

## What this slice checks (and what it does not)

This is the **Tier 1 CI slice** - a design-time contract, not a runtime monitor. In CI:

- **well-formedness** - every call has a mandatory reply, every state is reachable, the
  conversation can terminate. A structurally forgotten reply or a dead branch fails the build.
- **duality** - the two role projections are compatible (what one sends the other receives). For a
  single protocol they are dual by construction, so this guards the projection and any
  separately authored role pair - it is well-formedness of the *contract*, not of a running system.
- **drift** - `protocol-drift` regenerates `gen/automaton/` into a temp dir and diffs it against the
  committed copy, so a protocol change without a regeneration cannot merge (mirrors `codegen-drift`).
- **conformance** - `conformance_test.go` loads the committed automaton and checks that the device
  thrall declares a handler for every op its role receives (against its `ThrallDescribe`). This is a
  name-existence lint: it catches "thrall missing an op for its role", not ordering or direction.

What is **not** here: runtime enforcement. "confirm before authorize at run time", "forgotten reply
from a live thrall" and "illegal op in a phase at run time" are caught by the runtime **monitor**,
which is Tier 2 and intentionally omitted. The transport stays untyped (`json.RawMessage`); nothing
on the wire changes.

## Regenerate

```bash
export GOTOOLCHAIN=local
./protocol-codegen.sh        # rewrites gen/automaton from the *.protocol.yaml sources, then commit it
```

## Test

```bash
mise exec go@latest -- go test ./examples/session-scada/...
```
