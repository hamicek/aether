import type { AetherProcess, PortHolder, PortStatus } from '../types'

/**
 * The slice of `$.process.run` the probes need. Keeping it narrow lets the
 * parsing be unit tested with a stub and no engine.
 */
export type CommandRunner = (
  argv: readonly string[],
) => Promise<{ exitCode: number; stdout: string; stderr: string }>

export type ProcessProbe = {
  processes: AetherProcess[]
  error: string | null
}

const describeError = (err: unknown): string =>
  err instanceof Error ? err.message : String(err)

const extractManifest = (command: string): string | null => {
  const tokens = command.split(/\s+/)
  const flag = tokens.findIndex(token => token === '-f' || token === '--file')
  if (flag < 0) return null
  const path = tokens[flag + 1]
  if (path === undefined || path.length === 0) return null
  const base = path.split('/').pop()
  return base && base.length > 0 ? base : path
}

/**
 * Parses `ps -axo pid=,command=` output into the aether processes whose command
 * line contains `match`. The `ps` line that produced this output never carries
 * the match itself, so no self-filtering is needed.
 */
export const parseProcessList = (
  stdout: string,
  match: string,
): AetherProcess[] => {
  const processes: AetherProcess[] = []
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim()
    if (trimmed.length === 0) continue
    const space = trimmed.indexOf(' ')
    if (space < 0) continue
    const pid = Number.parseInt(trimmed.slice(0, space), 10)
    if (!Number.isInteger(pid)) continue
    const command = trimmed.slice(space + 1).trim()
    if (!command.includes(match)) continue
    processes.push({ pid, command, manifest: extractManifest(command) })
  }
  return processes
}

/**
 * Parses `lsof` output into the distinct processes listening on a port. The
 * header row is skipped and IPv4/IPv6 duplicates are collapsed.
 */
export const parsePortHolders = (stdout: string): PortHolder[] => {
  const holders: PortHolder[] = []
  const seen = new Set<string>()
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim()
    if (trimmed.length === 0 || trimmed.startsWith('COMMAND')) continue
    const fields = trimmed.split(/\s+/)
    const command = fields[0]
    const pidField = fields[1]
    if (command === undefined || pidField === undefined) continue
    const pid = Number.parseInt(pidField, 10)
    if (!Number.isInteger(pid)) continue
    const key = `${command}:${pid}`
    if (seen.has(key)) continue
    seen.add(key)
    holders.push({ command, pid })
  }
  return holders
}

export const probeProcesses = async (
  run: CommandRunner,
  match: string,
): Promise<ProcessProbe> => {
  try {
    const { stdout } = await run(['ps', '-axo', 'pid=,command='])
    return { processes: parseProcessList(stdout, match), error: null }
  } catch (err) {
    return { processes: [], error: describeError(err) }
  }
}

export const probePort = async (
  run: CommandRunner,
  port: number,
): Promise<PortStatus> => {
  try {
    const { stdout } = await run([
      'lsof',
      '+c0',
      '-nP',
      `-iTCP:${port}`,
      '-sTCP:LISTEN',
    ])
    return { port, holders: parsePortHolders(stdout), error: null }
  } catch (err) {
    return { port, holders: [], error: describeError(err) }
  }
}
