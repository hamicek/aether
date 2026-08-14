# live-dashboard - live push to the browser (SSE), scoped per client

The reverse of ingress: **ether → browser**. Two site sensors publish readings to their own event
subjects; a live edge holds browser SSE connections and pushes **each client only the site it is
authorized for**. It shows the hard part of live push - *authorization is per client* - solved by
`thrall.SSEStream`: each connection gets its own scoped subscription, so NATS never delivers a client
an out-of-scope event.

- **`site-1` / `site-2`** - sensor thralls, each publishing `{"site","temp","seq"}` once a second to
  `aether.demo.site-N.evt`.
- **`dashboard`** - a model B edge (`StartEdge`) serving SSE on `:7392`. Its handler does the
  authorization (a demo token → a site) and hands the connection to `SSEStream`; the plumbing (SSE
  format, scoped subscribe, backpressure, drain) is the helper's.

## Run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/site-sensor ./examples/live-dashboard/sensor
mise exec go@latest -- go build -o bin/live-edge ./examples/live-dashboard/edge

cd examples/live-dashboard
../../bin/aether up -f aether.toml
```

## Try it

Open two streams side by side - each sees only its own site:

```bash
curl -N 'localhost:7392/events?token=tok-site-1'   # a live stream of site-1 readings
curl -N 'localhost:7392/events?token=tok-site-2'   # a live stream of site-2 readings
curl -sN localhost:7392/events                      # no token -> 401
```

`-N` disables curl buffering so you see events as they arrive (`data: {"site":"site-1",...}`). From a
browser it is `new EventSource("http://localhost:7392/events?token=tok-site-1")`.

## Polyglot

The sensors and the live edge exist in all three SDKs - `sensor/main.go`+`edge/main.go`, `sensor.ts`+
`edge.ts`, and `sensor.py`+`edge.py`. `aether.toml` runs the Go set, `aether-ts.toml` the TypeScript,
`aether-py.toml` the Python one (asyncio + aiohttp; `SSEStream` over aiohttp's `StreamResponse`):

```bash
cd examples/live-dashboard
uv venv && uv pip install --python .venv/bin/python -r requirements.txt
../../bin/aether up -f aether-py.toml   # then the same curls on :7392
```

## Boundaries

- **Authorization is your code.** The helper never authorizes - here a demo token maps to a site; in
  production you would verify a JWT and read its `site` claim. The pattern is identical: request →
  authorized subject scope.
- **Read-only.** This channel only pushes events out. Control actions from the frontend go through an
  ingress route (call/cast), not here.
- **Reconnect, no replay.** SSE reconnects automatically (`EventSource`), but the edge keeps no history -
  a client that was disconnected resumes with the live stream; events missed while away are gone. Durable
  history is the event log's job, not this channel's.
- **Backpressure = drop.** A client that cannot keep up loses events (bounded buffer), rather than
  stalling the edge. A real OS port → singleton (one active instance; failover via fencing).
