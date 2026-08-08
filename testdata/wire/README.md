# Wire contract fixtures

Canonical JSON envelopes for the wire contract, shared by all three SDK parity test suites
(Go, TypeScript, Python). They pin the `Envelope` / `WireError` shape and cover every `Kind`,
the `omitempty` fields (`minimal`) and an error reply (`reply_error`).

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
