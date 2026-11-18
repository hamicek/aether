"""The cross-language test peer, Python flavour. See README.md in this directory."""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sdk", "python"))

import aether  # noqa: E402


def init(_ctx):
    return {"balance": 3, "writes": 0}


def balance(_payload, state, _ctx):
    return state["balance"], state


def withdraw(payload, state, _ctx):
    amount = payload["amount"]
    if amount > state["balance"]:
        raise aether.Refuse("insufficient_funds", f"the balance is {state['balance']}, the withdrawal {amount}")
    nxt = {**state, "balance": state["balance"] - amount}
    return nxt["balance"], nxt


def broken(_payload, _state, _ctx):
    raise RuntimeError("disk on fire")


async def hold(payload, state, _ctx):
    await asyncio.sleep(payload["ms"] / 1000)
    return "released", state


def write(_payload, state, _ctx):
    nxt = {**state, "writes": state["writes"] + 1}
    return nxt["writes"], nxt


def writes(_payload, state, _ctx):
    return state["writes"], state


async def relay(payload, state, ctx):
    try:
        reply = await ctx.call(payload["target"], payload["op"], payload.get("payload"),
                               timeout=payload.get("timeout_ms", 2000) / 1000, app=payload["app"])
        return {"ok": True, "payload": reply}, state
    except aether.CallError as err:
        return {"ok": False, "typed": True, "type": err.type, "code": err.code, "message": err.message,
                "retryable": err.retryable, "text": str(err)}, state
    except Exception as err:  # noqa: BLE001
        return {"ok": False, "typed": False, "text": str(err)}, state


if __name__ == "__main__":
    aether.run(aether.def_thrall(
        name="",  # taken from AETHER_NAME
        init=init,
        handle_call={"balance": balance, "withdraw": withdraw, "broken": broken, "hold": hold,
                     "write": write, "writes": writes, "relay": relay},
    ))
