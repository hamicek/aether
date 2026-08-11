"""Event-sourced account thrall in Python - functionally identical to main.go and account.ts.

A bank account whose balance survives a restart by replaying its event log, with no in-memory
snapshot. init rebuilds the balance from the log; each deposit/withdraw appends a signed delta
(the log is the truth) and updates the balance. With a persistent JetStream (store_dir in the
manifest), the balance is reconstructed after `aether up` is stopped and started again.

	aether cast account deposit  '{"delta": 100}'
	aether cast account withdraw '{"delta": 30}'
	aether call account balance                     # -> 70   (and still 70 after a restart)
"""

import os

from aether import def_thrall, rebuild, run


def _fold(event, balance):
    """Fold one event (a signed delta) into the balance. Used to rebuild in init; the log is
    at-least-once so this must be idempotent (a pure sum is)."""
    return balance + event["delta"]


async def _init(ctx):
    # Rebuild the balance by replaying the event log ("log is truth, state is a projection").
    balance = await rebuild(ctx, 0, _fold)
    ctx.log.info("rebuilt from event log", balance=balance)
    return balance


def _move(sign):
    """Build a cast handler that appends a signed event and updates the balance. sign is +1 for
    deposit, -1 for withdraw."""

    async def handler(payload, balance, ctx):
        delta = payload["delta"] * sign
        await ctx.append({"delta": delta})  # persist first - the log is the truth
        nxt = balance + delta
        ctx.log.info("balance changed", delta=delta, balance=nxt)
        return nxt

    return handler


account = def_thrall(
    name=os.environ.get("AETHER_NAME") or "account",
    init=_init,
    handle_cast={"deposit": _move(+1), "withdraw": _move(-1)},
    handle_call={"balance": lambda payload, balance, ctx: (balance, balance)},  # (reply, new_state)
)

if __name__ == "__main__":
    run(account)
