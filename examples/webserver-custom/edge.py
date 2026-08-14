# A custom edge (model B) in Python - the parity of examples/webserver-custom/main.go and edge.ts. It
# holds an aiohttp server and does a per-request Authorization check (which the declarative ingress
# cannot express) before calling the counter thrall over the ether. Runs via start_edge, so it is
# supervised like any thrall (heartbeat, restart, drain, fencing) - and coexists with a Go counter under
# one lord (a Python edge fronting a Go backend, proving the wire contract is language-neutral).
import os

from aiohttp import web

from aether import def_edge, run_edge

addr = 7393


def authorized(request: web.Request) -> bool:
    # The demo authorization: a fixed bearer token. A real edge would verify a JWT.
    return request.headers.get("Authorization") == "Bearer secret"


async def run(ctx, stop):
    # run owns the socket: it serves until drain (stop is set), then shuts the server down itself. As in
    # the TS edge, run awaits `stop` rather than blocking, so no separate stop hook is needed.
    async def handle_value(request: web.Request) -> web.Response:
        # This is the reason it is model B, not A: a per-request auth check.
        if not authorized(request):
            return web.Response(status=401, text="unauthorized")
        try:
            value = await ctx.call("counter", "get", {})
        except Exception as ex:  # noqa: BLE001
            ctx.log.error("call counter failed", err=str(ex))
            return web.Response(status=502, text="bad gateway")
        return web.json_response(value)

    app = web.Application()
    app.router.add_get("/value", handle_value)

    runner = web.AppRunner(app)
    await runner.setup()
    server = web.TCPSite(runner, "0.0.0.0", addr)
    await server.start()  # an error here (e.g. address in use) ends run abnormally -> the lord restarts it
    ctx.log.info("custom edge (py) listening", addr=addr)
    try:
        await stop.wait()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    run_edge(def_edge(name=os.environ.get("AETHER_NAME") or "secure-gw", run=run))
