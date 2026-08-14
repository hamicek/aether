# A live-dashboard edge in Python - the parity of examples/live-dashboard/edge/main.go and edge.ts. It
# holds browser SSE connections and pushes each client only the events of the site it is authorized for.
# The plumbing (SSE, per-connection scoped subscribe, backpressure, drain) is SSEStream; this file's job
# is only authorization - map the request to a site, then hand the connection to the stream.
import os

from aiohttp import web

from aether import SSEStream, _sub_evt, def_edge, run_edge

addr = 7392


def site_from_token(request: web.Request):
    # The demo authorization: a bearer token (query ?token= or Authorization header) maps to a site. A
    # real edge would verify a JWT and take the site from its claims.
    tok = request.query.get("token", "")
    if not tok:
        auth = request.headers.get("Authorization", "")
        tok = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
    return {"tok-site-1": "site-1", "tok-site-2": "site-2"}.get(tok)


async def run(ctx, stop):
    stream = SSEStream(ctx)

    async def handle_events(request: web.Request):
        site = site_from_token(request)
        if not site:
            return web.Response(status=401, text="unauthorized")
        # The client sees ONLY its site's event subject - NATS never delivers anything else.
        return await stream.serve_client(request, _sub_evt(ctx.app, site))  # aether.<app>.<site>.evt

    app = web.Application()
    app.router.add_get("/events", handle_events)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", addr)
    await site.start()
    ctx.log.info("live-dashboard edge (py) listening", addr=addr)
    try:
        await stop.wait()
    finally:
        # On drain: end the live SSE connections first (stream.close), then close the server.
        stream.close()
        await runner.cleanup()


if __name__ == "__main__":
    run_edge(def_edge(name=os.environ.get("AETHER_NAME") or "dashboard", run=run))
