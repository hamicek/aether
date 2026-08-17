// Event-sourced account thrall in TypeScript - functionally identical to main.go and account.py.
//
// A bank account whose balance survives a restart by replaying its event log, with no in-memory
// snapshot. init rebuilds the balance from the log; each deposit/withdraw appends a signed delta
// (the log is the truth) and updates the balance. With a persistent JetStream (store_dir in the
// manifest), the balance is reconstructed after `aether up` is stopped and started again.
//
//	aether cast account deposit  '{"delta": 100}'
//	aether cast account withdraw '{"delta": 30}'
//	aether call account balance                     # -> 70   (and still 70 after a restart)
import { defThrall, start, rebuild, type Ctx } from "@hamicek/aether";

type Delta = { delta: number };

// move builds a cast handler that appends a signed event and updates the balance. sign is +1
// for deposit, -1 for withdraw.
const move =
  (sign: number) =>
  async (payload: unknown, balance: number, ctx: Ctx): Promise<number> => {
    const delta = (payload as Delta).delta * sign;
    // Command-key: key the append on the message id so a redelivered cast (same envelope) does
    // not double-count - a signed delta is not idempotent, so the fold alone cannot tell a
    // replayed event from a genuine second one.
    await ctx.append({ delta }, { dedupKey: ctx.msgId }); // persist first - the log is the truth
    const next = balance + delta;
    ctx.log.info("balance changed", { delta, balance: next });
    return next;
  };

const account = defThrall<number>({
  name: process.env.AETHER_NAME ?? "account",

  init: async (ctx) => {
    // Rebuild the balance by replaying the event log ("log is truth, state is a projection").
    const balance = await rebuild<number>(ctx, 0, (event, bal) => bal + (event as Delta).delta);
    ctx.log.info("rebuilt from event log", { balance });
    return balance;
  },

  handleCast: {
    deposit: move(+1),
    withdraw: move(-1),
  },

  handleCall: {
    balance: (_payload, balance) => [balance, balance], // [reply, newState]
  },
});

await start(account);
