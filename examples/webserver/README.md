# webserver - HTTP edge ingress

Exposes a stateful `counter` thrall over HTTP **without writing a web server**: the
`[[edge.http]]` sections in `aether.toml` map routes to thrall operations, and the runtime
spawns a built-in edge process (`aether _edge`) for each one, supervised like any thrall.

It shows the edge split: a concurrent, stateless HTTP front door (`api`, `admin`) in front of a
single serialized, stateful thrall (`counter`). Two independent edge servers run on their own
ports from one manifest.

## Run

Build the binaries, then start the manifest:

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/counter-go ./examples/counter

cd examples/webserver
../../bin/aether up -f aether.toml
```

## Try it

```bash
curl -s localhost:7392/value            # -> the counter value (call: waits for the reply)
curl -s -X POST localhost:7392/increment  # -> 202 Accepted (cast: fire-and-forget)
curl -s localhost:7392/value            # -> value + 1
curl -s -X POST localhost:7393/decrement  # -> 202, on the second edge server
```

- A `call` route (`GET /value`) waits for the thrall's reply and returns it as the body.
- A `cast` route (`POST /increment`) hands the message to the ether and returns `202` at once.
- An unknown route returns `404`; if the target thrall is down, the edge returns `503`, and a
  reply timeout returns `504`.

## Boundaries

Each edge binds a real OS port, so it runs as a **singleton** (one active instance; failover via
the KV lock + fencing). aether does no load balancing - scale a single port with a reverse proxy in
front. HTTP routing beyond `route -> operation` and any business logic belong in application code.
