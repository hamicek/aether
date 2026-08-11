// Turnstile FSM thrall in TypeScript - functionally identical to main.go and turnstile.py.
//
// A classic two-state turnstile: `locked` accepts a coin to become `unlocked`, `unlocked`
// accepts a push to lock again (counting pushes), and auto-locks after 5s idle via a state
// timeout. Events are ordinary casts/calls - the wire is unchanged, so any GenServer caller
// reaches it. Name from env (AETHER_NAME) so the manifest sets it.
import { defFSM, startFSM } from "@hamicek/aether";

const turnstile = defFSM<number>({
  name: process.env.AETHER_NAME ?? "turnstile",
  initial: "locked",
  init: () => 0, // data = number of completed pushes

  states: {
    locked: {
      on: {
        coin: {
          fn: (_ev, pushes, ctx) => {
            ctx.log.info("coin accepted, unlocking");
            return { next: "unlocked", data: pushes };
          },
        },
      },
    },

    unlocked: {
      on: {
        push: {
          fn: (_ev, pushes, ctx) => {
            ctx.log.info("push, locking", { total_pushes: pushes + 1 });
            return { next: "locked", data: pushes + 1, reply: pushes + 1 };
          },
        },
        autolock: {
          fn: (_ev, pushes, ctx) => {
            ctx.log.info("idle timeout, auto-locking");
            return { next: "locked", data: pushes };
          },
        },
      },
      // If nobody pushes within 5s of unlocking, fire "autolock" back to locked.
      timeout: { after: 5000, op: "autolock" },
    },
  },
});

await startFSM(turnstile);
