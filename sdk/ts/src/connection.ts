import { readFileSync } from "fs";
import {
  connect,
  nkeyAuthenticator,
  type ConnectionOptions,
  type NatsConnection,
} from "nats";

// Env = the environment injected by the lord when spawning a thrall. caFile and
// nkeySeed are present only when the bus is secured.
export interface Env {
  natsUrl: string;
  app: string;
  name: string;
  caFile?: string;
  nkeySeed?: string;
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
  return {
    natsUrl,
    app,
    name: name ?? "",
    caFile: process.env.AETHER_NATS_CA || undefined,
    nkeySeed: process.env.AETHER_NATS_NKEY_SEED || undefined,
  };
}

// connectOptions builds the NATS connect options: the server and name plus, when the
// bus is secured, the TLS CA and an nkey authenticator. The seed is read lazily (at
// connect time) so this stays a pure mapping with no file I/O. Absent fields leave
// the options unsecured, exactly as before.
export function connectOptions(env: Env): ConnectionOptions {
  const opts: ConnectionOptions = { servers: env.natsUrl, name: env.name };
  if (env.caFile) {
    opts.tls = { caFile: env.caFile };
  }
  if (env.nkeySeed) {
    const seedPath = env.nkeySeed;
    opts.authenticator = nkeyAuthenticator(
      () => new Uint8Array(readFileSync(seedPath)),
    );
  }
  return opts;
}

export function open(env: Env): Promise<NatsConnection> {
  // name = the thrall's name -> carried in $SYS connection events (correlation) and in debugging.
  return connect(connectOptions(env));
}
