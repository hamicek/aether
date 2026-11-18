"""The web thrall test peer, Python flavour. See README.md in this directory."""

import asyncio
import os
import sys
from urllib.parse import parse_qs

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sdk", "python"))

import aether  # noqa: E402


async def respond(req, ctx, count):
    """The routing both shapes share; count is what /hits reports for this request."""
    if req.path == "/fast":
        return aether.text_response(200, "fast")
    if req.path == "/slow":
        await asyncio.sleep(int(parse_qs(req.query).get("ms", ["0"])[0]) / 1000.0)
        return aether.text_response(200, "slow")
    if req.path == "/trace":
        await asyncio.sleep(0.05)
        return aether.text_response(200, ctx.trace)
    if req.path == "/hits":
        return aether.text_response(200, str(count))
    return aether.text_response(404, "not found")


async def handle(req, state, ctx):
    """The handle shape: the count is the state, threaded from one request to the next."""
    hits = state["hits"] + 1
    return await respond(req, ctx, hits), {"hits": hits}


async def serve(req, shared, ctx):
    """The serve shape: the count lives in the shared value and is changed in one step, before
    the first await - which is what keeps shared state consistent on the single event loop."""
    shared["hits"] += 1
    return await respond(req, ctx, shared["hits"])


def init(_ctx):
    return {"hits": 0}


if os.environ.get("WEB_SHAPE") == "handle":
    aether.run_web(init=init, handle=handle)
else:
    aether.run_web(init=init, serve=serve, concurrency=int(os.environ.get("WEB_CONCURRENCY", "0")))
