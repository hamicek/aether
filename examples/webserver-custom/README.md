# webserver-custom - a hand-written edge (model B) next to the built-in ingress (model A)

Shows the two edge models side by side in one manifest, both fronting the same `counter` thrall:

- **`api` (model A):** the built-in `[[edge.http]]` ingress (AE-035) - a route→operation mapping, no code.
- **`secure-gw` (model B):** a custom edge thrall written via `thrall.StartEdge` (`main.go`). It is an
  ordinary `[[thrall]]` with a `cmd`, supervised like any other. It does a per-request **Authorization
  check** - something configuration cannot express - and only then calls the counter.

That is the split: use **model A** when a route maps cleanly to an operation; reach for **model B** when
you need custom logic (auth, transformation, a non-HTTP protocol, a SCADA driver) around the call. The
edge owns the socket and is concurrent/stateless; the state lives in the `counter` behind it.

`StartEdge` gives the custom edge the full thrall machinery for free (heartbeat, restart, drain,
fencing) - you write only the run-loop (`ListenAndServe`) and a graceful-stop hook (`Shutdown`).

## Run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/counter-go ./examples/counter
mise exec go@latest -- go build -o bin/edge-custom ./examples/webserver-custom

cd examples/webserver-custom
../../bin/aether up -f aether.toml
```

## Try it

```bash
curl -s localhost:7392/value                                   # model A: no auth -> the value
curl -s localhost:7393/value                                   # model B: no auth header -> 401
curl -s -H 'Authorization: Bearer secret' localhost:7393/value # model B: authorized -> the value
```

## Boundary: head-of-line blocking

Both edges are concurrent, but the `counter` behind them is a single serialized mailbox - its
throughput is the ceiling. A slow operation on the hot path blocks other requests waiting on that one
thrall. Keep the stateful backend fast, or split the load across more thralls; the edge concurrency
does not remove that limit.
