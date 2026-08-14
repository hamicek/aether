// A site sensor thrall in TypeScript - the parity of examples/live-dashboard/sensor/main.go. In init it
// starts a ticker publishing a reading to its own event subject on the ether. The live edge subscribes
// to these per-site subjects and pushes them to browsers. Two instances run under names site-1 and
// site-2, each publishing to a distinct subject (aether.<app>.site-N.evt).
import { defThrall, start, subjects } from "@hamicek/aether";

const name = process.env.AETHER_NAME ?? "site-1";

await start(
  defThrall<number>({
    name,
    init: (ctx) => {
      const subject = subjects.eventLog(ctx.app, ctx.name); // aether.<app>.<name>.evt
      const enc = new TextEncoder();
      let seq = 0;
      const timer = setInterval(() => {
        seq++;
        const temp = 18 + (seq % 6); // a deterministic wobble between 18 and 23 C
        ctx.nats.publish(subject, enc.encode(JSON.stringify({ site: ctx.name, temp, seq })));
      }, 1000);
      (timer as { unref?: () => void }).unref?.();
      return 0;
    },
  }),
);
