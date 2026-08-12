"""Dynamic topology demo in Python - functionally identical to main.go and manager.ts.

A "manager" thrall owns its dynamic topology: it spawns worker-1..3 from its own init (so
they come back after a lord restart, when init runs again) and re-applies them on a
"reconcile" cast. Because start_child is idempotent on name, both are safe to call blindly -
a worker already running is left untouched, so there are never duplicates.

Dynamic children do not survive a lord restart by design (see DESIGN.md, section 12);
re-establishing the topology is the owner's job, demonstrated here. One file plays every
role, selected by AETHER_NAME (injected by the lord).
"""

import os

from aether import def_thrall, run, SpawnSpec

# The manager's target topology: the children it wants running at all times. It
# re-establishes exactly this set from init and on every reconcile.
DESIRED_WORKERS = ["worker-1", "worker-2", "worker-3"]

# The command the lord runs for each dynamic worker - this same file, dispatched by
# AETHER_NAME. Relative to the manifest's directory.
WORKER_CMD = "PYTHONPATH=../../sdk/python .venv/bin/python manager.py"


# reconcile brings the running topology up to the desired set. A spawn of a worker already
# under supervision is an idempotent no-op, so this never creates a duplicate.
async def reconcile(ctx):
    for name in DESIRED_WORKERS:
        try:
            await ctx.start_child(SpawnSpec(name=name, cmd=WORKER_CMD, restart="permanent"))
            ctx.log.info("reconcile: worker ensured", worker=name)
        except Exception as err:  # noqa: BLE001 - report and keep reconciling the rest
            ctx.log.error("reconcile: spawn worker failed", worker=name, err=str(err))


# run_manager owns the topology: it spawns the workers from init and re-applies them on a
# reconcile cast.
def run_manager():
    async def init(ctx):
        await reconcile(ctx)
        return DESIRED_WORKERS

    async def do_reconcile(_payload, state, ctx):
        await reconcile(ctx)
        return state

    run(def_thrall(name="manager", init=init, handle_cast={"reconcile": do_reconcile}))


# run_worker is a trivial dynamically-spawned child: it answers a "ping" call so you can see
# it on the ether. It carries no state that must survive a restart.
def run_worker():
    def ping(_payload, state, _ctx):
        return f"pong from {os.environ.get('AETHER_NAME')}", state

    run(def_thrall(name=os.environ.get("AETHER_NAME"), init=lambda ctx: 0,
                   handle_call={"ping": ping}))


if __name__ == "__main__":
    role = os.environ.get("AETHER_NAME", "")
    if role == "manager":
        run_manager()
    elif role.startswith("worker-"):
        run_worker()
    else:
        raise RuntimeError(f"unknown thrall {role}")
