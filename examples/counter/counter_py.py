"""Counter thrall in Python - functionally identical to counter.ts and counter_go.go.

Under the same lord, on the same ether, with the same JSON contract; to the gateway the three
differ only in name. Name from env (AETHER_NAME) -> the same code runs under whatever name the
manifest gives it (counter-py, counter-single, ...), falling back to counter-py without a lord.
"""

import os

from aether import def_thrall, run

name = os.environ.get("AETHER_NAME") or "counter-py"

counter = def_thrall(
    name=name,
    version="1.0.0",
    init=lambda ctx: 0,
    handle_call={
        "get": lambda payload, state, ctx: (state, state),  # (reply, new_state)
    },
    handle_cast={
        "inc": lambda payload, state, ctx: state + 1,        # new_state
        "dec": lambda payload, state, ctx: state - 1,
    },
    terminate=lambda reason, state: print(f"{name} exiting ({reason}), last = {state}"),
)

if __name__ == "__main__":
    run(counter)
