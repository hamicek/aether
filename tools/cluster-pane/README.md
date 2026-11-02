# aether cluster pane

A developer mod that puts a live pane beside the chat showing what is currently
running on the aether bus. It answers the question that keeps coming up during
development: how many runtime processes are up, who holds the ports, and is the
dashboard alive - so duplicate instances do not pile up unnoticed.

It is aether-general: it serves any project built on aether and hardcodes
nothing project-specific. The watched ports and dashboard URLs are configuration.

## What the pane shows

- **Processes** - running `aether up` processes, each with its PID and the
  manifest name pulled from the command line (`aether up -f <manifest>`). When
  none are running it says so.
- **Ports** - for each watched TCP port, whether something is listening and, if
  so, which process (command and PID). An unheld port shows as `free`.
- **Dashboard** - for each watched URL, reachability from a short HTTP GET:
  `ok`, `HTTP <status>`, or `down`. With no URLs configured the section says so.

A header line shows how long ago the data was refreshed and a `Refresh` button
forces an immediate poll; otherwise the pane refreshes on its own interval.

All signals are OS-level only (`ps`, `lsof`, HTTP GET). The mod requires no
change to aether. Once aether exposes its own status/ps operation the pane can
be enriched with the thrall list and restart state.

## Configuration

Set these in the plugin's `/config` entry (or `pluginConfigs` in settings):

| Field            | Default      | Meaning                                                         |
| ---------------- | ------------ | --------------------------------------------------------------- |
| `ports`          | `4222`       | TCP ports to watch, comma or space separated (e.g. `4222 7447`) |
| `dashboards`     | *(empty)*    | Dashboard URLs to ping, comma or space separated                |
| `refreshSeconds` | `5`          | Refresh interval in seconds (minimum 1)                         |
| `processMatch`   | `aether up`  | Substring that marks an aether process in its command line      |

For an embedded-NATS stack the port is often ephemeral (printed in
`.aether-endpoint`); point `ports` at the external NATS port or the dashboard
port you actually want to watch.

## Running it during development

Load the mod straight from this folder for one session:

```bash
claude --plugin-dir tools/cluster-pane
```

Then open the pane with the `/aether-cluster` command (it also opens on its own
when the session starts). To install it for good, add the repo as a marketplace
and `/plugin install aether-cluster-pane`.

## Tests

```bash
claude plugin validate tools/cluster-pane
claude plugin test tools/cluster-pane
```

The parsing of `ps` and `lsof` output and the config handling are unit tested;
the pane rendering and the manual refresh are covered by UI tests across
surfaces.
