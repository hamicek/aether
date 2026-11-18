# http-ingress - expose a web thrall over HTTP

> The ingress itself is a component of aether now: the program is `cmd/http-ingress`, its core
> `internal/ingress`, and its flags, limits and status codes are documented in the top-level
> `README.md` ("HTTP ingress for web thralls"). This directory is the **demo** - a web thrall behind
> it. `main.go` here is only a wrapper kept so that `go build ./examples/http-ingress` still yields
> the same binary for tooling that has not moved to `./cmd/http-ingress` yet.

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
mise exec go@latest -- go build -o bin/http-ingress ./cmd/http-ingress

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

## Dynamic routing (site-registry)

The static `routes.json` is one of two route providers. The other resolves each `Host` dynamically
against the commons **`site-registry`** thrall over NATS, so sites can be published and retired at
runtime without restarting the ingress:

```bash
../../bin/http-ingress -registry                       # resolve via site-registry instead of -routes
#   -registry-subject aether.ae.site-registry.call      # the resolve call subject (default shown)
#   -registry-scope   public                            # this ingress's reach: public | vpn
#   -registry-ttl     5s                                # cache a resolved route this long
```

- The provider calls the registry's `resolve` op (`host` + the ingress's `scope`) and routes to the
  returned thrall. Reuse is over the **wire contract**, not a code import - the registry lives in the
  private `ae-commons` repo and is reached purely by its NATS envelope.
- **Caching:** resolve is a call per request, so results are cached for `-registry-ttl`.
- **Outage:** when the registry cannot be reached, the last known answer of a host keeps being served
  for `-registry-stale` (default 5m) past its TTL; a host with no such answer gets **503**, not 404.
- **Scope:** a `public` ingress sees only public sites; a `vpn` ingress sees both. The scope travels
  in the resolve request and the public/vpn filter stays in the registry, so the ingress keeps its
  default-deny behaviour unchanged.

## Boundaries (deliberate)

- **Route table is pluggable.** The ingress takes a `RouteProvider` (host -> thrall). It ships two:
  the static-file provider (`-routes`, `LoadStaticRoutes`) and the dynamic `site-registry` provider
  (`-registry`, `NewRegistryRoutes`) above. Both sit behind the same interface and keep the same
  default-deny behaviour.
- **Dumb forwarder.** No path routing, no middleware, no business logic in the ingress - all of that
  lives in the web thrall. TLS, certificates and rate limiting belong in a reverse proxy in front.
- A request is one bus message: a body over what the bus carries (about 714 KiB by default) is answered
  413, never truncated; a reply with no status, or too large for the bus, is an upstream fault (502).
