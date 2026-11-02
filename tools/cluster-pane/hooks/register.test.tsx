import { expect, test } from 'claude-code/testing'

const PANE = {
  plugin: 'aether-cluster-pane',
  component: 'Pane',
  requestId: 'aether-cluster',
  props: {
    title: 'Aether cluster',
    isFocused: false,
    bodyColumns: 48,
    placement: 'dock',
    scroll: { offset: 0, bodyRows: 24 },
    view: {},
  },
} as const

const PS_FIXTURE = '25199 aether up -f /tmp/macro-verify.toml'

const ran = (exitCode: number, stdout: string) => ({
  value: {
    exitCode,
    stdout,
    stderr: '',
    isStdoutTruncated: false,
    isStderrTruncated: false,
  },
})

test('an idle pane states each section is empty, on every surface', async ($, on) => {
  on('ui.open', () => ({ value: { isPlaced: true } }))
  on('clock.now', () => ({ value: 1_000_000 }))

  for (const surface of ['terminal', 'desktop', 'vscode', 'mobile'] as const) {
    const ui = await $.ui.mount({ ...PANE, surface })

    expect(
      (await ui.find({ type: 'Text', text: /No aether processes running/ }))
        ?.text,
    ).toBeDefined()
    expect(
      (await ui.find({ type: 'Text', text: /No dashboards watched/ }))?.text,
    ).toBeDefined()
    expect((await ui.find({ type: 'Text', text: /updated never/ }))?.text).toBeDefined()

    await ui.unmount()
  }
})

test('pressing Refresh probes the bus and lists the running process', async ($, on) => {
  on('ui.open', () => ({ value: { isPlaced: true } }))
  on('clock.now', () => ({ value: 1_000_000 }))
  on('process.run', (_, e) =>
    // lsof exits 1 with no output when nothing listens on the port.
    e.argv[0] === 'ps' ? ran(0, PS_FIXTURE) : ran(1, ''),
  )

  for (const surface of ['terminal', 'desktop'] as const) {
    const ui = await $.ui.mount({ ...PANE, surface })

    await ui.press({ key: 'refresh' })

    expect(
      (await ui.find({ type: 'Text', text: /25199/ }))?.text,
    ).toBeDefined()
    expect(
      (await ui.find({ type: 'Text', text: /macro-verify\.toml/ }))?.text,
    ).toBeDefined()
    // The default watched port 4222 has no listener in the stub.
    expect((await ui.find({ type: 'Text', text: /free/ }))?.text).toBeDefined()

    await ui.unmount()
  }
})
