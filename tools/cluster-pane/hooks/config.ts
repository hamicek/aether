import type { PluginOptions } from 'claude-code'

export type ClusterConfig = {
  ports: number[]
  dashboards: string[]
  refreshMs: number
  processMatch: string
}

const DEFAULT_PORTS = '4222'
const DEFAULT_PROCESS_MATCH = 'aether up'
const DEFAULT_REFRESH_SECONDS = 5
const MIN_REFRESH_SECONDS = 1

const asString = (value: unknown, fallback: string): string =>
  typeof value === 'string' ? value : fallback

const splitList = (value: string): string[] =>
  value
    .split(/[\s,]+/)
    .map(item => item.trim())
    .filter(item => item.length > 0)

const parsePorts = (value: string): number[] => {
  const ports: number[] = []
  const seen = new Set<number>()
  for (const token of splitList(value)) {
    const port = Number.parseInt(token, 10)
    if (!Number.isInteger(port) || port <= 0 || port > 65535) continue
    if (seen.has(port)) continue
    seen.add(port)
    ports.push(port)
  }
  return ports
}

/**
 * Turns the plugin's user config into a validated shape the hooks work with.
 * Ports and dashboards are forgiving comma/space separated lists so they edit
 * cleanly in the config menu; nothing about any particular project is assumed.
 */
export const readConfig = (options: PluginOptions): ClusterConfig => {
  const refreshSeconds =
    typeof options.refreshSeconds === 'number'
      ? options.refreshSeconds
      : DEFAULT_REFRESH_SECONDS
  const refreshMs =
    Math.max(MIN_REFRESH_SECONDS, Math.round(refreshSeconds)) * 1000

  const processMatch =
    asString(options.processMatch, DEFAULT_PROCESS_MATCH).trim() ||
    DEFAULT_PROCESS_MATCH

  return {
    ports: parsePorts(asString(options.ports, DEFAULT_PORTS)),
    dashboards: splitList(asString(options.dashboards, '')),
    refreshMs,
    processMatch,
  }
}
