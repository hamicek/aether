# http-ingress - expose a web thrall over HTTP

A generic HTTP front door for the **"a thrall IS the web server"** model (AE-091/092). The web thrall
(`blog`) is a plain genserver that answers `http_request -> http_response` and **owns no port**; the
ingress maps the request's `Host` header to a thrall, forwards the whole request over NATS, and writes
the reply back. Adding a site is a line in `routes.json`, not a new server.

This is the inverse of the built-in `[[edge.http]]` ingress (`examples/webserver`): there the edge
maps a *route* to a thrall *operation* and the thrall never sees HTTP. Here the whole HTTP
request/response is the contract and the thrall itself is the web server. The ingress is a dumb
host->subject forwarder - it does no path routing (that is the web thrall's job).

## Run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/blog ./examples/http-ingress/blog
mise exec go@latest -- go build -o bin/http-ingress ./examples/http-ingress

cd examples/http-ingress
../../bin/aether up -f aether.toml
```

## Try it

```bash
curl -s localhost:7391/                      # -> the blog's home page (Host: localhost -> blog)
curl -s localhost:7391/about                 # -> the blog's /about page (the thrall routes the path)
curl -s -o /dev/null -w '%{http_code}\n' localhost:7391/nope          # -> 404 from the thrall
curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: evil.test' localhost:7391/   # -> 404, default-deny
```

- The `Host` header picks the thrall (`routes.json` maps `localhost` and `blog.localhost` -> `blog`).
  The port is stripped and the host lowercased before the lookup.
- **Default-deny:** a host with no entry in `routes.json` never reaches the ether - it gets a 404.
- The web thrall does its own path routing (`/`, `/about`, else 404); the ingress just forwards.

## How it fits together

```
curl --Host--> [http-ingress]  --aether.web.blog.call (http_request)-->  [blog web thrall]
                 (owns :7391)   <--------- http_response ---------------   (owns no port)
```

- The ingress is an ordinary **edge thrall** (`thrall.StartEdge`): it owns the HTTP socket and is
  supervised like any thrall (heartbeat, drain, fencing). It binds a real port, so it is a
  **singleton** (one active instance; failover via the KV lock + fencing).
- The `blog` web thrall is a plain genserver started with `thrall.StartWeb` - it implements only an
  HTTP handler and never binds a port.

## Boundaries (deliberate)

- **Route table is pluggable.** The ingress takes a `RouteProvider` (host -> thrall). This example
  ships the static-file provider (`LoadStaticRoutes`); a dynamic `site-registry` provider that lets
  sites be published at runtime is a later, separate concern (it implements the same interface).
- **Dumb forwarder.** No path routing, no middleware, no business logic in the ingress - all of that
  lives in the web thrall. TLS, certificates and rate limiting belong in a reverse proxy in front.
- Request bodies are size-capped before they enter the ether; a reply with no status is treated as an
  upstream fault (502), not forwarded as an invalid HTTP status.
