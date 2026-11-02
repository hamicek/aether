export type AetherProcess = {
  pid: number
  command: string
  manifest: string | null
}

export type PortHolder = {
  command: string
  pid: number
}

export type PortStatus = {
  port: number
  holders: PortHolder[]
  error: string | null
}

export type DashboardState = 'ok' | 'http' | 'down'

export type DashboardStatus = {
  url: string
  state: DashboardState
  status: number | null
  detail: string | null
}

export type ClusterSnapshot = {
  refreshedAt: number | null
  isRefreshing: boolean
  processError: string | null
  processes: AetherProcess[]
  ports: PortStatus[]
  dashboards: DashboardStatus[]
}

declare module 'claude-code' {
  interface PluginState {
    'aether-cluster-pane': {
      snapshot: Shaped<ClusterSnapshot>
    }
  }
}
