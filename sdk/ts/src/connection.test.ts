import { test, expect } from "bun:test";
import { connectOptions, readEnv, type Env } from "./connection";

test("connectOptions carries the TLS CA and an nkey authenticator when the bus is secured", () => {
  const env: Env = {
    natsUrl: "tls://bus:4222",
    app: "a",
    name: "n",
    caFile: "/etc/aether/ca.pem",
    nkeySeed: "/etc/aether/user.nk",
  };
  const opts = connectOptions(env);
  expect(opts.servers).toBe("tls://bus:4222");
  expect(opts.tls?.caFile).toBe("/etc/aether/ca.pem");
  expect(typeof opts.authenticator).toBe("function");
});

test("connectOptions stays unsecured without a security block", () => {
  const env: Env = { natsUrl: "nats://bus:4222", app: "a", name: "n" };
  const opts = connectOptions(env);
  expect(opts.tls).toBeUndefined();
  expect(opts.authenticator).toBeUndefined();
});

test("readEnv picks up the injected CA and nkey seed paths", () => {
  process.env.AETHER_NATS_URL = "tls://bus:4222";
  process.env.AETHER_APP = "a";
  process.env.AETHER_NAME = "n";
  process.env.AETHER_NATS_CA = "/etc/aether/ca.pem";
  process.env.AETHER_NATS_NKEY_SEED = "/etc/aether/user.nk";
  try {
    const env = readEnv();
    expect(env.caFile).toBe("/etc/aether/ca.pem");
    expect(env.nkeySeed).toBe("/etc/aether/user.nk");
  } finally {
    delete process.env.AETHER_NATS_CA;
    delete process.env.AETHER_NATS_NKEY_SEED;
  }
});
