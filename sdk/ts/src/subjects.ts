// Subject conventions - mirrors internal/wire/subjects.go.
export const subjects = {
  call: (app: string, name: string) => `aether.${app}.${name}.call`,
  cast: (app: string, name: string) => `aether.${app}.${name}.cast`,
  info: (app: string, name: string) => `aether.${app}.${name}.info`,
  // data = a single wildcard over call/cast/info -> one subscription, FIFO order.
  data: (app: string, name: string) => `aether.${app}.${name}.*`,
  ctl: (name: string) => `aether._lord.${name}.ctl`,
  hb: (name: string) => `aether._lord.${name}.hb`,
  // lordCtl = the lord's inbound control channel (thrall -> lord), request/reply, for
  // runtime spawn/stop. Unlike ctl, which is lord -> thrall.
  lordCtl: () => "aether._lord.ctl",
  events: "aether._lord.events",
  // JetStream stream for the durable mailbox (dots are not allowed in stream names).
  stream: (app: string, name: string) => `aether_${app}_${name}`,
} as const;
