// Dynamic topology demo in TypeScript - functionally identical to main.go and manager.py.
//
// A "manager" thrall owns its dynamic topology: it spawns worker-1..3 from its own init
// (so they come back after a lord restart, when init runs again) and re-applies them on a
// "reconcile" cast. Because startChild is idempotent on name, both are safe to call
// blindly - a worker already running is left untouched, so there are never duplicates.
//
// Dynamic children do not survive a lord restart by design (see DESIGN.md, section 12);
// re-establishing the topology is the owner's job, demonstrated here. One file plays every
// role, selected by AETHER_NAME (injected by the lord).
import { defThrall, start, type Ctx } from "@hamicek/aether";

// desiredWorkers is the manager's target topology: the children it wants running at all
// times. It re-establishes exactly this set from init and on every reconcile.
const desiredWorkers = ["worker-1", "worker-2", "worker-3"];

// workerCmd is the command the lord runs for each dynamic worker - this same file, which
// dispatches on AETHER_NAME. The path is relative to the manifest's directory.
const workerCmd = "bun run ./manager.ts";

// reconcile brings the running topology up to the desired set. A spawn of a worker already
// under supervision is an idempotent no-op, so this never creates a duplicate.
async function reconcile(ctx: Ctx): Promise<void> {
  for (const name of desiredWorkers) {
    try {
      await ctx.startChild({ name, cmd: workerCmd, restart: "permanent" });
      ctx.log.info("reconcile: worker ensured", { worker: name });
    } catch (err) {
      ctx.log.error("reconcile: spawn worker failed", { worker: name, err: String(err) });
    }
  }
}

// runManager owns the topology: it spawns the workers from init and re-applies them on a
// reconcile cast.
function runManager(): Promise<void> {
  return start(
    defThrall<string[]>({
      name: "manager",
      init: async (ctx) => {
        await reconcile(ctx);
        return desiredWorkers;
      },
      handleCast: {
        reconcile: async (_payload, state, ctx: Ctx) => {
          await reconcile(ctx);
          return state;
        },
      },
    }),
  );
}

// runWorker is a trivial dynamically-spawned child: it answers a "ping" call so you can see
// it on the ether. It carries no state that must survive a restart.
function runWorker(): Promise<void> {
  return start(
    defThrall<number>({
      name: process.env.AETHER_NAME ?? "",
      init: () => 0,
      handleCall: {
        ping: (_payload, state) => [`pong from ${process.env.AETHER_NAME}`, state],
      },
    }),
  );
}

const role = process.env.AETHER_NAME ?? "";
if (role === "manager") await runManager();
else if (role.startsWith("worker-")) await runWorker();
else throw new Error(`unknown thrall ${role}`);
