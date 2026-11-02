import { atom, read, update } from 'claude-code'
import type { EngineInterface, Register } from 'claude-code'

import { readConfig } from './config'
import type { ClusterConfig } from './config'
import { probePort, probeProcesses } from './probes'
import type { ClusterSnapshot, DashboardStatus } from '../types'

const PANE = 'aether-cluster'
const TITLE = 'Aether cluster'

const EMPTY: ClusterSnapshot = {
  refreshedAt: null,
  isRefreshing: false,
  processError: null,
  processes: [],
  ports: [],
  dashboards: [],
}

// Bump the shape tag whenever a snapshot field changes: a value an older
// version stored is then declined and read as EMPTY rather than mis-drawn.
const snapshot = atom(
  { plugin: 'aether-cluster-pane', key: 'snapshot' } as const,
  EMPTY,
  { shape: 'snapshot-1' },
)

const DASHBOARD_TIMEOUT_MS = 4000

const probeDashboard = (
  $: EngineInterface,
  url: string,
): Promise<DashboardStatus> => {
  const fetched: Promise<DashboardStatus> = $.http
    .fetch(url)
    .then(res => ({
      url,
      state: (res.ok ? 'ok' : 'http') as DashboardStatus['state'],
      status: res.status,
      detail: null,
    }))
    .catch(err => ({
      url,
      state: 'down' as const,
      status: null,
      detail: err instanceof Error ? err.message : String(err),
    }))
  // A hung endpoint must not freeze the whole refresh: race the fetch against a
  // bound. The losing fetch is left to settle on its own and ignored.
  const timedOut: Promise<DashboardStatus> = $.clock
    .sleep(DASHBOARD_TIMEOUT_MS)
    .then(() => ({ url, state: 'down' as const, status: null, detail: 'timed out' }))

  return Promise.race([fetched, timedOut])
}

const agoLabel = (refreshedAt: number | null, now: number): string => {
  if (refreshedAt === null) return 'never'
  const seconds = Math.max(0, Math.round((now - refreshedAt) / 1000))
  if (seconds < 1) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  return `${Math.round(seconds / 60)}m ago`
}

// Guards against two probes overlapping (a manual refresh landing on the
// timer's). Module scope so it survives the closures but not a reload.
let isProbing = false

const refresh = async (
  $: EngineInterface,
  config: ClusterConfig,
): Promise<void> => {
  if (isProbing) return
  isProbing = true
  await update($, snapshot, current => ({ ...current, isRefreshing: true }))
  try {
    const run: Parameters<typeof probeProcesses>[0] = argv => $.process.run(argv)
    const [processResult, ports, dashboards] = await Promise.all([
      probeProcesses(run, config.processMatch),
      Promise.all(config.ports.map(port => probePort(run, port))),
      Promise.all(config.dashboards.map(url => probeDashboard($, url))),
    ])
    const refreshedAt = await $.clock.now()
    await update($, snapshot, () => ({
      refreshedAt,
      isRefreshing: false,
      processError: processResult.error,
      processes: processResult.processes,
      ports,
      dashboards,
    }))
  } catch {
    await update($, snapshot, current => ({ ...current, isRefreshing: false }))
  } finally {
    isProbing = false
  }
}

export const register: Register = (on, options) => {
  const config = readConfig(options)

  on('session.start', async ($, e, next) => {
    const started = await next(e)
    await $.command.register({
      name: PANE,
      description: 'Open the aether cluster pane (what is running on the bus)',
    })
    void $.ui.open({ id: PANE, title: TITLE })
    void refresh($, config)
    $.clock.every(config.refreshMs, () => {
      void refresh($, config)
    })

    return started
  })

  on('command.run', { command: PANE }, async $ => {
    await $.ui.open({ id: PANE, title: TITLE })
    void refresh($, config)

    return { text: `${TITLE} pane opened.` }
  })

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text, Button } = $.ui.resolve(e)
    const snap = await read($, snapshot)
    const now = await $.clock.now()

    return (
      <Box flexDirection="column" gap={1}>
        <Box justifyContent="space-between">
          <Text dimColor>
            updated {agoLabel(snap.refreshedAt, now)}
            {snap.isRefreshing ? ' (refreshing)' : ''}
          </Text>
          <Button
            key="refresh"
            label="Refresh"
            onPress={() => {
              void refresh($, config)
            }}
          />
        </Box>

        <Box flexDirection="column">
          <Text bold>Processes</Text>
          {snap.processError !== null && (
            <Text color="red">ps failed: {snap.processError}</Text>
          )}
          {snap.processError === null && snap.processes.length === 0 && (
            <Text dimColor>No aether processes running.</Text>
          )}
          {snap.processes.map(proc => (
            <Text>
              <Text color="green">{String(proc.pid)}</Text>{' '}
              {proc.manifest ?? proc.command}
            </Text>
          ))}
        </Box>

        <Box flexDirection="column">
          <Text bold>Ports</Text>
          {config.ports.length === 0 && (
            <Text dimColor>No ports watched.</Text>
          )}
          {snap.ports.map(status => {
            if (status.error !== null) {
              return (
                <Text>
                  {String(status.port)}{' '}
                  <Text color="red">lsof failed: {status.error}</Text>
                </Text>
              )
            }
            if (status.holders.length === 0) {
              return (
                <Text>
                  {String(status.port)} <Text dimColor>free</Text>
                </Text>
              )
            }
            return (
              <Text>
                <Text color="green">{String(status.port)}</Text>{' '}
                {status.holders
                  .map(holder => `${holder.command} (${holder.pid})`)
                  .join(', ')}
              </Text>
            )
          })}
        </Box>

        <Box flexDirection="column">
          <Text bold>Dashboard</Text>
          {config.dashboards.length === 0 && (
            <Text dimColor>No dashboards watched.</Text>
          )}
          {snap.dashboards.map(dash => {
            if (dash.state === 'ok') {
              return (
                <Text>
                  <Text color="green">ok</Text> {dash.url}
                </Text>
              )
            }
            if (dash.state === 'http') {
              return (
                <Text>
                  <Text color="yellow">HTTP {String(dash.status)}</Text>{' '}
                  {dash.url}
                </Text>
              )
            }
            return (
              <Text>
                <Text color="red">down</Text> {dash.url}
              </Text>
            )
          })}
        </Box>
      </Box>
    )
  })
}
