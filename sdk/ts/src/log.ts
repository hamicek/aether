// Structured logger for the TS SDK, mirroring the Go SDK (internal/obs): the level and
// format come from the same AETHER_LOG_LEVEL / AETHER_LOG_FORMAT env the lord injects,
// so a thrall logs consistently with the rest of the tree. No external dependency.

export type LogLevel = "debug" | "info" | "warn" | "error";

const ORDER: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

export type Fields = Record<string, unknown>;

export interface Logger {
  debug(msg: string, fields?: Fields): void;
  info(msg: string, fields?: Fields): void;
  warn(msg: string, fields?: Fields): void;
  error(msg: string, fields?: Fields): void;
  with(fields: Fields): Logger;
}

export interface LoggerOptions {
  level?: LogLevel;
  format?: "json" | "text";
  write?: (line: string) => void;
}

// levelFromEnv resolves AETHER_LOG_LEVEL; an empty or unknown value falls back to info,
// so a typo never silences the runtime.
export function levelFromEnv(): LogLevel {
  switch ((process.env.AETHER_LOG_LEVEL ?? "").trim().toLowerCase()) {
    case "debug":
      return "debug";
    case "warn":
    case "warning":
      return "warn";
    case "error":
      return "error";
    default:
      return "info";
  }
}

function formatFromEnv(): "json" | "text" {
  return (process.env.AETHER_LOG_FORMAT ?? "").trim().toLowerCase() === "json" ? "json" : "text";
}

function render(format: "json" | "text", level: LogLevel, msg: string, fields: Fields): string {
  const time = new Date().toISOString();
  if (format === "json") {
    return JSON.stringify({ time, level: level.toUpperCase(), msg, ...fields });
  }
  const pairs = Object.entries(fields)
    .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
    .join(" ");
  return `${time} ${level.toUpperCase()} ${msg}${pairs ? " " + pairs : ""}`;
}

// newLogger builds a logger. Defaults come from the environment; options override them
// (used by tests to capture output and pin level/format).
export function newLogger(base: Fields = {}, opts: LoggerOptions = {}): Logger {
  const level = opts.level ?? levelFromEnv();
  const format = opts.format ?? formatFromEnv();
  const write = opts.write ?? ((line: string) => process.stderr.write(line + "\n"));
  const threshold = ORDER[level];

  const emit = (at: LogLevel, msg: string, fields: Fields = {}): void => {
    if (ORDER[at] < threshold) return;
    write(render(format, at, msg, { ...base, ...fields }));
  };

  return {
    debug: (msg, fields) => emit("debug", msg, fields),
    info: (msg, fields) => emit("info", msg, fields),
    warn: (msg, fields) => emit("warn", msg, fields),
    error: (msg, fields) => emit("error", msg, fields),
    with: (fields) => newLogger({ ...base, ...fields }, { level, format, write }),
  };
}
