"""Tracing demo in Python - functionally identical to main.go and trace.ts.

Two thralls: an "api" thrall relays an incoming request to a "db" thrall via ctx.cast, which
propagates the correlation trace. Both log the same trace for one logical operation, so the
path can be reconstructed from the logs. One file plays both roles, selected by AETHER_NAME
(injected by the lord). Run with AETHER_LOG_LEVEL=debug to see the shared trace.
"""

import os

from aether import def_thrall, run


# runApi receives a "request" cast (the edge, from `aether cast`) and relays it to "db".
# ctx.cast carries the trace of the incoming message, so the whole path shares one id.
def run_api():
    async def request(payload, state, ctx):
        ctx.log.info("api received request, relaying to db", trace=ctx.trace)
        await ctx.cast("db", "store", payload)
        return state

    run(def_thrall(name="api", init=lambda ctx: 0, handle_cast={"request": request}))


# run_db is the downstream thrall; it logs the trace it received, which must match the one the
# api thrall logged for the same request.
def run_db():
    def store(payload, state, ctx):
        ctx.log.info("db stored value", trace=ctx.trace, payload=payload)
        return state

    run(def_thrall(name="db", init=lambda ctx: 0, handle_cast={"store": store}))


if __name__ == "__main__":
    role = os.environ.get("AETHER_NAME")
    if role == "api":
        run_api()
    elif role == "db":
        run_db()
    else:
        raise RuntimeError(f"unknown thrall {role}")
