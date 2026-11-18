// The cross-language test peer, TypeScript flavour. See README.md in this directory.
import { CallError, defThrall, refuse, start } from "../../sdk/ts/src/index";

interface State {
  balance: number;
  writes: number;
}

interface Relay {
  app: string;
  target: string;
  op: string;
  payload?: unknown;
  timeout_ms?: number;
}

await start(
  defThrall<State>({
    name: "", // taken from AETHER_NAME
    init: () => ({ balance: 3, writes: 0 }),
    handleCall: {
      balance: (_p, s) => [s.balance, s],
      withdraw: (p, s) => {
        const { amount } = p as { amount: number };
        if (amount > s.balance) refuse("insufficient_funds", `the balance is ${s.balance}, the withdrawal ${amount}`);
        const next = { ...s, balance: s.balance - amount };
        return [next.balance, next];
      },
      broken: () => {
        throw new Error("disk on fire");
      },
      hold: async (p, s) => {
        await Bun.sleep((p as { ms: number }).ms);
        return ["released", s];
      },
      write: (_p, s) => {
        const next = { ...s, writes: s.writes + 1 };
        return [next.writes, next];
      },
      writes: (_p, s) => [s.writes, s],
      bulk: (p, s) => ["x".repeat((p as { bytes: number }).bytes), s],
      relay: async (p, s, ctx) => {
        const r = p as Relay;
        try {
          const payload = await ctx.call(r.target, r.op, r.payload ?? {}, { app: r.app, timeoutMs: r.timeout_ms ?? 2000 });
          return [{ ok: true, payload }, s];
        } catch (err) {
          if (err instanceof CallError) {
            return [
              { ok: false, typed: true, type: err.type, code: err.code, message: err.reason, retryable: err.retryable, text: err.message },
              s,
            ];
          }
          return [{ ok: false, typed: false, text: String(err) }, s];
        }
      },
    },
  }),
);
