# Tracing demo

Follows one logical operation across two OS-process thralls. An `api` thrall receives a
request and relays it to a `db` thrall with `ctx.Cast`, which **propagates the correlation
trace**. Both thralls log the same `trace`, so the path can be reconstructed from the logs.

## Run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/trace-demo ./examples/tracing

cd examples/tracing
AETHER_LOG_FORMAT=json AETHER_LOG_LEVEL=debug ../../bin/aether up -f aether.toml
```

In another shell, send a request to the edge (the CLI mints a fresh trace):

```bash
cd examples/tracing
../../bin/aether cast api request '{"item":"widget"}'
```

## What to look for

The `api` and `db` log lines carry the **same** `trace`, e.g.:

```json
{"time":"...","level":"INFO","component":"thrall","app":"trace","name":"api","msg":"api received request, relaying to db","trace":"_INBOX.abc123"}
{"time":"...","level":"INFO","component":"thrall","app":"trace","name":"db","msg":"db stored value","trace":"_INBOX.abc123","payload":"{\"item\":\"widget\"}"}
```

Filter one operation out of the interleaved stream by its trace:

```bash
# from the `aether up` output
... | grep '_INBOX.abc123'
```

The SDK also emits a `handling call`/`handling cast` debug line per message with the same
`trace`, so a log line and a trace can always be joined.

## Metrics

While it runs, scrape the runtime's own telemetry:

```bash
curl -s 127.0.0.1:7391/metrics
```
