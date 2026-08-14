# A site sensor thrall in Python - the parity of examples/live-dashboard/sensor/main.go and sensor.ts.
# In init it starts a ticker publishing a reading to its own event subject on the ether. The live edge
# subscribes to these per-site subjects and pushes them to browsers. Two instances run under names
# site-1 and site-2, each publishing to a distinct subject (aether.<app>.site-N.evt).
import asyncio
import json
import os

from aether import _sub_evt, def_thrall, run

name = os.environ.get("AETHER_NAME") or "site-1"


def init(ctx):
    subject = _sub_evt(ctx.app, ctx.name)  # aether.<app>.<name>.evt

    async def tick():
        seq = 0
        while True:
            await asyncio.sleep(1)
            seq += 1
            temp = 18 + (seq % 6)  # a deterministic wobble between 18 and 23 C
            await ctx.nats.publish(subject, json.dumps({"site": ctx.name, "temp": temp, "seq": seq}).encode())

    asyncio.create_task(tick())
    return 0


if __name__ == "__main__":
    run(def_thrall(name=name, init=init))
