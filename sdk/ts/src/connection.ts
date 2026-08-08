import { connect, type NatsConnection } from "nats";

// Env = the environment injected by the lord when spawning a thrall.
export interface Env {
  natsUrl: string;
  app: string;
  name: string;
}

export function readEnv(): Env {
  const natsUrl = process.env.AETHER_NATS_URL;
  const app = process.env.AETHER_APP;
  const name = process.env.AETHER_NAME;
  if (!natsUrl || !app) {
    throw new Error(
      "missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`",
    );
  }
  return { natsUrl, app, name: name ?? "" };
}

export function open(env: Env): Promise<NatsConnection> {
  // name = the thrall's name -> carried in $SYS connection events (correlation) and in debugging.
  return connect({ servers: env.natsUrl, name: env.name });
}
