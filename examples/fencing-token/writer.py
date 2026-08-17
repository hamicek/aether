"""Write-side fencing token demo in Python - functionally identical to main.go and writer.ts.

A singleton thrall stamps its fencing epoch (ctx.singleton_epoch) on every write to a resource,
and the resource rejects a write carrying a lower epoch than it has seen. Singleton fencing alone
only bounds liveness overlap (DESIGN 14); this is strict single-writer against a resource. The
"resource" is a NATS KV bucket holding {value, epoch}; a real one would be a PLC/driver/DB.

    aether call writer write       '{"value": "A"}'   # accepted, stored with the live epoch
    aether call writer write-stale  '{"value": "B"}'  # a simulated old instance -> fenced
    aether call writer read                            # -> {"value":"A","epoch":N}
"""

import json

from nats.js.errors import KeyNotFoundError

from aether import def_thrall, run

RESOURCE_BUCKET = "resource"
RESOURCE_KEY = "reading"


async def _write_fenced(kv, value, epoch):
    """Resource-side guard: accept only if epoch >= the epoch already stored (monotonic), so a
    stale writer (a lower epoch) is rejected. Returns whether it stored."""
    try:
        cur = await kv.get(RESOURCE_KEY)
        prev = json.loads(bytes(cur.value))
        if epoch < prev["epoch"]:
            return False  # fenced: a newer epoch has already written here
    except KeyNotFoundError:
        pass
    await kv.put(RESOURCE_KEY, json.dumps({"value": value, "epoch": epoch}).encode())
    return True


def make_writer():
    state = {"kv": None}

    async def _init(ctx):
        # The resource stub: a KV bucket standing in for an external resource.
        state["kv"] = await ctx.nats.jetstream().create_key_value(bucket=RESOURCE_BUCKET)
        ctx.log.info("writer ready", singleton_epoch=ctx.singleton_epoch)
        return {}

    async def _write(payload, s, ctx):
        stored = await _write_fenced(state["kv"], payload["value"], ctx.singleton_epoch)
        return ({"stored": stored, "epoch": ctx.singleton_epoch}, s)

    async def _write_stale(payload, s, ctx):
        # Simulate a fenced-out old instance writing with an older epoch; rejected.
        stale = ctx.singleton_epoch - 1
        stored = await _write_fenced(state["kv"], payload["value"], stale)
        return ({"stored": stored, "epoch": stale}, s)

    async def _read(payload, s, ctx):
        try:
            cur = await state["kv"].get(RESOURCE_KEY)
            return (json.loads(bytes(cur.value)), s)
        except KeyNotFoundError:
            return ({"value": None}, s)

    return def_thrall(
        name="writer",
        init=_init,
        handle_call={"write": _write, "write-stale": _write_stale, "read": _read},
    )


if __name__ == "__main__":
    run(make_writer())
