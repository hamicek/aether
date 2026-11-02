import { expect, test } from 'claude-code/testing'

import { readConfig } from './config'
import { parsePortHolders, parseProcessList } from './probes'

test('parseProcessList keeps matching lines and pulls the manifest', () => {
  const stdout = [
    '  501 /usr/sbin/cupsd -l',
    '25199 aether up -f /tmp/some/macro-verify.toml',
    '25926 bun run ./counter.ts',
    '25930 aether up -f examples/counter/aether.toml --verbose',
  ].join('\n')

  const processes = parseProcessList(stdout, 'aether up')

  expect(processes.length).toBe(2)
  expect(processes[0]?.pid).toBe(25199)
  expect(processes[0]?.manifest).toBe('macro-verify.toml')
  expect(processes[1]?.pid).toBe(25930)
  expect(processes[1]?.manifest).toBe('aether.toml')
})

test('parseProcessList leaves manifest null when there is no -f flag', () => {
  const processes = parseProcessList('42 aether up', 'aether up')

  expect(processes.length).toBe(1)
  expect(processes[0]?.manifest).toBe(null)
  expect(processes[0]?.command).toBe('aether up')
})

test('parsePortHolders skips the header and collapses v4/v6 duplicates', () => {
  const stdout = [
    'COMMAND   PID  USER   FD   TYPE DEVICE SIZE/OFF NODE NAME',
    'nats-server 25200 user   7u  IPv4 0x1234      0t0  TCP 127.0.0.1:4222 (LISTEN)',
    'nats-server 25200 user   8u  IPv6 0x5678      0t0  TCP [::1]:4222 (LISTEN)',
  ].join('\n')

  const holders = parsePortHolders(stdout)

  expect(holders.length).toBe(1)
  expect(holders[0]?.command).toBe('nats-server')
  expect(holders[0]?.pid).toBe(25200)
})

test('parsePortHolders returns nothing for an empty listing', () => {
  expect(parsePortHolders('').length).toBe(0)
})

test('readConfig applies defaults when nothing is configured', () => {
  const config = readConfig({})

  expect(config.ports).toEqual([4222])
  expect(config.dashboards).toEqual([])
  expect(config.refreshMs).toBe(5000)
  expect(config.processMatch).toBe('aether up')
})

test('readConfig parses, dedupes and bounds the watched ports', () => {
  const config = readConfig({ ports: '4222, 7447 7447 99999 abc 0' })

  expect(config.ports).toEqual([4222, 7447])
})

test('readConfig clamps the refresh interval to at least one second', () => {
  const config = readConfig({ refreshSeconds: 0 })

  expect(config.refreshMs).toBe(1000)
})

test('readConfig splits dashboards on commas and whitespace', () => {
  const config = readConfig({
    dashboards: 'http://localhost:7447 , https://example.test/health',
  })

  expect(config.dashboards).toEqual([
    'http://localhost:7447',
    'https://example.test/health',
  ])
})
