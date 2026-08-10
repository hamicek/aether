import { test, expect } from "bun:test";
import { newLogger } from "./log";

// Capture output through the injectable writer so tests never touch stderr.
function capture() {
  const lines: string[] = [];
  return { lines, write: (line: string) => lines.push(line) };
}

test("json format carries base and per-call fields", () => {
  const { lines, write } = capture();
  const log = newLogger({ component: "thrall", app: "counter", name: "worker" }, { level: "info", format: "json", write });
  log.info("thrall ready", { pid: 42 });

  expect(lines.length).toBe(1);
  const rec = JSON.parse(lines[0]);
  expect(rec.level).toBe("INFO");
  expect(rec.msg).toBe("thrall ready");
  expect(rec.component).toBe("thrall");
  expect(rec.app).toBe("counter");
  expect(rec.name).toBe("worker");
  expect(rec.pid).toBe(42);
  expect(typeof rec.time).toBe("string");
});

test("level below threshold is dropped", () => {
  const { lines, write } = capture();
  const log = newLogger({}, { level: "warn", format: "json", write });
  log.info("dropped");
  log.warn("kept");

  expect(lines.length).toBe(1);
  expect(JSON.parse(lines[0]).msg).toBe("kept");
});

test("text format is the default rendering", () => {
  const { lines, write } = capture();
  const log = newLogger({ name: "w" }, { level: "info", format: "text", write });
  log.info("hello");

  expect(lines[0]).not.toStartWith("{");
  expect(lines[0]).toContain("INFO");
  expect(lines[0]).toContain("hello");
  expect(lines[0]).toContain("name=w");
});

test("with() derives a child logger merging fields", () => {
  const { lines, write } = capture();
  const base = newLogger({ app: "counter" }, { level: "info", format: "json", write });
  base.with({ name: "worker" }).error("boom", { err: "nope" });

  const rec = JSON.parse(lines[0]);
  expect(rec.app).toBe("counter");
  expect(rec.name).toBe("worker");
  expect(rec.err).toBe("nope");
  expect(rec.level).toBe("ERROR");
});
