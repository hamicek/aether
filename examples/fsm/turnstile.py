"""Turnstile FSM thrall in Python - functionally identical to main.go and turnstile.ts.

A classic two-state turnstile: `locked` accepts a coin to become `unlocked`, `unlocked`
accepts a push to lock again (counting pushes), and auto-locks after 5s idle via a state
timeout. Events are ordinary casts/calls - the wire is unchanged, so any GenServer caller
reaches it. Name from env (AETHER_NAME) so the manifest sets it.
"""

import os

from aether import Outcome, Reaction, State, StateTimeout, def_fsm, run_fsm


def _coin(ev, pushes, ctx):
    ctx.log.info("coin accepted, unlocking")
    return Outcome(next="unlocked", data=pushes)


def _push(ev, pushes, ctx):
    ctx.log.info("push, locking", total_pushes=pushes + 1)
    return Outcome(next="locked", data=pushes + 1, reply=pushes + 1)


def _autolock(ev, pushes, ctx):
    ctx.log.info("idle timeout, auto-locking")
    return Outcome(next="locked", data=pushes)


turnstile = def_fsm(
    name=os.environ.get("AETHER_NAME") or "turnstile",
    initial="locked",
    init=lambda ctx: 0,  # data = number of completed pushes
    states={
        "locked": State(on={"coin": Reaction(fn=_coin)}),
        "unlocked": State(
            on={
                "push": Reaction(fn=_push),
                "autolock": Reaction(fn=_autolock),
            },
            # If nobody pushes within 5s of unlocking, fire "autolock" back to locked.
            timeout=StateTimeout(after=5.0, op="autolock"),
        ),
    },
)

if __name__ == "__main__":
    run_fsm(turnstile)
