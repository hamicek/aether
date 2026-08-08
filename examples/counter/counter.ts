import { defThrall, start } from "@hamicek/aether";

// Counter = a thrall with `number` state. The handlers hold the GenServer semantics.
// Name from env (AETHER_NAME) -> the same code runs as both `counter` and `counter-dur`.
// Durability is purely a manifest concern (durable = true), not thrall code.
const counter = defThrall<number>({
  name: process.env.AETHER_NAME ?? "counter",

  init: () => 0,

  handleCall: {
    get: (_payload, state) => [state, state], // [reply, newState]
  },

  handleCast: {
    inc: (_payload, state) => state + 1, // newState
    dec: (_payload, state) => state - 1,
  },

  terminate: (reason, state) => {
    console.log(`counter exiting (${reason}), last value = ${state}`);
  },
});

await start(counter);
