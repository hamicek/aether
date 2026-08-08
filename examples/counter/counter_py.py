"""Counter thrall in Python - functionally identical to the TS counter.ts.

Under the same lord, on the same ether, with the same JSON contract. To the gateway
it is indistinguishable from the TS version (differing only in name: counter-py vs counter).
"""

from aether import def_thrall, run

counter = def_thrall(
    name="counter-py",
    init=lambda ctx: 0,
    handle_call={
        "get": lambda payload, state, ctx: (state, state),  # (reply, new_state)
    },
    handle_cast={
        "inc": lambda payload, state, ctx: state + 1,        # new_state
        "dec": lambda payload, state, ctx: state - 1,
    },
    terminate=lambda reason, state: print(f"counter-py exiting ({reason}), last = {state}"),
)

if __name__ == "__main__":
    run(counter)
