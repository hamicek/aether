# Wire contract fixtures

Canonical JSON envelopes for the wire contract, shared by the SDK parity test suites
(Go, TypeScript, Python, C#). They pin the `Envelope` / `WireError` shape and cover every `Kind`,
the `omitempty` fields (`minimal`), an error reply (`reply_error`), a call with a deadline
(`call_deadline`), a refusal (`reply_refused`), the reply to a call dropped past its deadline
(`reply_deadline_exceeded`), the reply sent in place of one too large for a bus message
(`reply_too_large`), a heartbeat reporting dropped calls (`hb_expired`) and the heartbeat of a
concurrent web thrall (`hb_concurrent`: handlers in flight and the declared concurrency) and of a
web thrall that contained handler crashes (`hb_panics`).

**Go is the source of truth.** Regenerate after any intentional contract change:

```
go test ./internal/wire -run TestEnvelopeGolden -update
```

The parity tests compare parsed values (not bytes), so a language emitting fields in a
different order or with different whitespace still passes - only a real contract drift
(renamed field, changed type, altered subject) turns a test red.

Run the three suites:

```
go test ./internal/wire
cd sdk/ts && bun test
cd sdk/python && uv run --with nats-py -m unittest wire_parity_test
```
