// Command aether starts the runtime (up) and provides tools: ps, events, fleet, cast, call.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/lord"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
)

// endpointFile holds the URL of the running bus + app, so that ps/events/cast/call
// can connect to it even in embedded mode (where the port is random).
const endpointFile = ".aether-endpoint"

type endpoint struct {
	URL string `json:"url"`
	App string `json:"app"`
	// CA and NkeySeed are the paths to the client credentials for a secured external bus.
	// Written by `up` (from the manifest) and reused by ps/events/cast/call; omitted (and so
	// absent from the file) for an unsecured/embedded bus.
	CA       string `json:"ca,omitempty"`
	NkeySeed string `json:"nkey_seed,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "up":
		up(os.Args[2:])
	case "ps":
		psCmd(os.Args[2:])
	case "describe":
		describeCmd(os.Args[2:])
	case "events":
		eventsCmd(os.Args[2:])
	case "fleet":
		fleetCmd(os.Args[2:])
	case "cast":
		castCmd(os.Args[2:])
	case "call":
		callCmd(os.Args[2:])
	case "_edge":
		// internal: a built-in HTTP ingress server, spawned by the lord from [[edge.http]].
		edgeCmd(os.Args[2:])
	case "down":
		fmt.Println("use Ctrl-C on a running `aether up` for a graceful shutdown")
	default:
		usage()
	}
}

func up(argv []string) {
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	manifestPath := fs.String("f", "aether.toml", "path to the manifest")
	_ = fs.Parse(argv)

	m, err := lord.LoadManifest(*manifestPath)
	if err != nil {
		log.Fatalf("manifest: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := obs.NewLogger().With(slog.String("component", "aether"), slog.String("app", m.App))

	eth, err := ether.Start(ctx, m.Nats, ether.WithApp(m.App))
	if err != nil {
		log.Fatalf("ether: %v", err)
	}
	defer eth.Stop()
	logger.Info("ether running", slog.String("url", eth.URL()), slog.String("mode", m.Nats.Mode))

	// SIGHUP rotates the secured bus's credentials in place: the operator replaces the cert/key (or
	// nkey) files and signals the process, and the embedded server reloads them without dropping live
	// connections. On a bus that cannot reload (no [nats.security]), it logs the reason and does nothing.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				signal.Stop(hup)
				return
			case <-hup:
				if err := eth.Reload(m.Nats); err != nil {
					logger.Warn("credential reload failed", slog.String("error", err.Error()))
				} else {
					logger.Info("credentials reloaded")
				}
			}
		}
	}()

	// The operator CLI inherits the operator role (call/cast and observe, not control).
	epCA, epNkey := m.Nats.ClientCredentials(ether.RoleOperator)
	writeEndpoint(endpoint{URL: eth.URL(), App: m.App, CA: epCA, NkeySeed: epNkey})
	defer os.Remove(endpointFile)

	root, err := lord.New(m, eth)
	if err != nil {
		log.Fatalf("lord: %v", err)
	}
	if err := root.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	<-ctx.Done()
	logger.Info("shutdown: graceful drain of thralls")
	root.Stop()
}

// psCmd prints the current state of thralls from the KV registry.
func psCmd(argv []string) {
	fs := flag.NewFlagSet("ps", flag.ExitOnError)
	url := fs.String("url", "", "bus address (default: ."+endpointFile+")")
	ca, nkey := credFlags(fs)
	_ = fs.Parse(argv)

	ep := resolveEndpoint(*url, "", *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	reg, err := registry.Open(nc)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	list, err := reg.List()
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	now := time.Now().UnixMilli()
	fmt.Printf("%-14s %-8s %-10s %-10s %s\n", "NAME", "PID", "STATUS", "VERSION", "AGE")
	for _, e := range list {
		age := time.Duration(now-e.UpdatedMs) * time.Millisecond
		pid := "-"
		if e.PID > 0 {
			pid = fmt.Sprintf("%d", e.PID)
		}
		row := fmt.Sprintf("%-14s %-8s %-10s %-10s %s", e.Name, pid, e.Status, orDash(e.Version), age.Round(time.Second))
		if e.LastError != "" {
			row += "  ! " + truncate(e.LastError, 40)
		}
		fmt.Println(row)
	}
	if len(list) == 0 {
		fmt.Println("(registry empty - is `aether up` running?)")
	}
}

// describeCmd prints one thrall's full self-description from the registry: the operations it
// answers (call vs cast), its self-declared version, its deployment metadata and the reason of its
// most recent failure. It answers "what is this thrall, and what build runs here" without an ssh.
func describeCmd(argv []string) {
	fs := flag.NewFlagSet("describe", flag.ExitOnError)
	url := fs.String("url", "", "bus address (default: ."+endpointFile+")")
	asJSON := fs.Bool("json", false, "print the raw registry entry as JSON")
	ca, nkey := credFlags(fs)
	// Parse the leading flags, take the thrall name, then parse any flags that trail it - so both
	// `describe --json <name>` and `describe <name> --json` work (Go's flag stops at the first
	// positional argument).
	_ = fs.Parse(argv)
	if fs.NArg() < 1 {
		log.Fatal("usage: aether describe <thrall> [--url ...] [--json]")
	}
	name := fs.Arg(0)
	_ = fs.Parse(fs.Args()[1:])

	ep := resolveEndpoint(*url, "", *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	reg, err := registry.Open(nc)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	entry, ok, err := reg.Get(name)
	if err != nil {
		log.Fatalf("get %q: %v", name, err)
	}
	if !ok {
		log.Fatalf("no thrall named %q in the registry (is `aether up` running?)", name)
	}

	if *asJSON {
		out, err := json.MarshalIndent(struct {
			Name string `json:"name"`
			registry.Entry
		}{Name: name, Entry: entry}, "", "  ")
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}
		fmt.Println(string(out))
		return
	}

	fmt.Printf("%-12s %s\n", "name", name)
	fmt.Printf("%-12s %s\n", "status", entry.Status)
	if entry.PID > 0 {
		fmt.Printf("%-12s %d\n", "pid", entry.PID)
	}
	fmt.Printf("%-12s %s\n", "node", entry.Node)
	fmt.Printf("%-12s %s\n", "version", orDash(entry.Version))
	fmt.Printf("%-12s %s\n", "call ops", orDash(strings.Join(entry.CallOps, ", ")))
	fmt.Printf("%-12s %s\n", "cast ops", orDash(strings.Join(entry.CastOps, ", ")))
	if len(entry.Metadata) > 0 {
		keys := make([]string, 0, len(entry.Metadata))
		for k := range entry.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			label := ""
			if i == 0 {
				label = "metadata"
			}
			fmt.Printf("%-12s %s = %s\n", label, k, entry.Metadata[k])
		}
	}
	if entry.LastError != "" {
		when := time.UnixMilli(entry.LastErrorMs).Format("2006-01-02 15:04:05")
		fmt.Printf("%-12s %s (%s)\n", "last error", entry.LastError, when)
	}
}

// orDash renders an empty string as a dash, so a blank field reads as "none" rather than a gap.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// eventsCmd prints the live lifecycle stream from aether._lord.events (until Ctrl-C).
func eventsCmd(argv []string) {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	ca, nkey := credFlags(fs)
	_ = fs.Parse(argv)

	ep := resolveEndpoint(*url, "", *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Println("listening for lifecycle events (Ctrl-C to stop)...")
	_, err := nc.Subscribe(wire.Events, func(m *nats.Msg) {
		var ev struct {
			Event string `json:"event"`
			Name  string `json:"name"`
			PID   int    `json:"pid"`
			TS    int64  `json:"ts"`
		}
		if json.Unmarshal(m.Data, &ev) != nil {
			return
		}
		ts := time.UnixMilli(ev.TS).Format("15:04:05")
		fmt.Printf("%s  %-11s %-12s pid=%d\n", ts, ev.Event, ev.Name, ev.PID)
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	<-ctx.Done()
}

// fleetCmd shows the fleet: every lord publishing a health summary on aether._fleet.>. Without
// --watch it collects for a window and prints once; with --watch it redraws until Ctrl-C. Fleet
// health is fire-and-forget, so a one-shot only sees a lord that publishes while it is listening -
// the default window therefore spans a full default publish interval (5s) so every publishing lord
// appears at least once; --watch avoids the wait for continuous monitoring.
func fleetCmd(argv []string) {
	fs := flag.NewFlagSet("fleet", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	watch := fs.Bool("watch", false, "keep redrawing the fleet until Ctrl-C")
	collect := fs.Duration("for", 6*time.Second, "how long to collect before the one-shot print (span a publish interval)")
	ca, nkey := credFlags(fs)
	_ = fs.Parse(argv)

	ep := resolveEndpoint(*url, "", *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	agg := fleet.NewAggregator()
	if _, err := agg.Subscribe(nc); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	if !*watch {
		time.Sleep(*collect)
		printFleet(agg.Snapshot())
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fmt.Printf("\n=== fleet @ %s ===\n", time.Now().Format("15:04:05"))
			printFleet(agg.Snapshot())
		}
	}
}

// printFleet renders one row per node: app, lord id, live/stale, thrall count (ready of total),
// and how long ago its last summary arrived.
func printFleet(nodes []fleet.NodeView) {
	if len(nodes) == 0 {
		fmt.Println("no lords publishing fleet health (is [observability] fleet_health = true?)")
		return
	}
	fmt.Printf("%-16s %-24s %-6s %-14s %s\n", "APP", "LORD", "STATE", "THRALLS", "LAST")
	now := time.Now()
	for _, n := range nodes {
		ready := 0
		for _, th := range n.Thralls {
			if th.Status == "ready" {
				ready++
			}
		}
		state := "live"
		if n.Stale {
			state = "STALE"
		}
		age := now.Sub(time.UnixMilli(n.LastSeen)).Truncate(time.Second)
		fmt.Printf("%-16s %-24s %-6s %-14s %s ago\n",
			n.App, n.LordID, state, fmt.Sprintf("%d (%d ready)", len(n.Thralls), ready), age)
		// Per-thrall detail: the version and status that node reports, and a compact last-error
		// marker when one is set - so a fleet-wide rollout or failure is visible without an ssh.
		sorted := append([]fleet.ThrallHealth(nil), n.Thralls...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
		for _, th := range sorted {
			line := fmt.Sprintf("    %-14s %-10s %s", th.Name, th.Status, orDash(th.Version))
			if th.LastError != "" {
				line += "  ! " + truncate(th.LastError, 40)
			}
			fmt.Println(line)
		}
	}
}

// truncate shortens a string to at most n runes, appending an ellipsis when it had to cut, so a
// long last-error message stays on one line in a table.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-3]) + "..."
}

// castCmd sends a fire-and-forget cast: `aether cast <name> <op> [json-payload]`.
func castCmd(argv []string) {
	fs := flag.NewFlagSet("cast", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	app := fs.String("app", "", "app namespace (default from endpoint)")
	ca, nkey := credFlags(fs)
	_ = fs.Parse(argv)
	rest := fs.Args()
	if len(rest) < 2 {
		log.Fatal("usage: aether cast <name> <op> [json-payload]")
	}
	name, op := rest[0], rest[1]
	payload := json.RawMessage("{}")
	if len(rest) >= 3 {
		payload = json.RawMessage(rest[2])
	}

	ep := resolveEndpoint(*url, *app, *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	env := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: nats.NewInbox(), Kind: wire.KindCast, To: name, Op: op, Payload: payload, TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(env)
	if err := nc.Publish(wire.Cast(ep.App, name), data); err != nil {
		log.Fatalf("publish: %v", err)
	}
	_ = nc.Flush()
	fmt.Printf("cast %s %s sent\n", name, op)
}

// callCmd sends a sync call and prints the reply payload: `aether call <name> <op> [json-payload]`.
func callCmd(argv []string) {
	fs := flag.NewFlagSet("call", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	app := fs.String("app", "", "app namespace (default from endpoint)")
	timeout := fs.Duration("timeout", 2*time.Second, "timeout")
	ca, nkey := credFlags(fs)
	_ = fs.Parse(argv)
	rest := fs.Args()
	if len(rest) < 2 {
		log.Fatal("usage: aether call <name> <op> [json-payload]")
	}
	name, op := rest[0], rest[1]
	payload := json.RawMessage("{}")
	if len(rest) >= 3 {
		payload = json.RawMessage(rest[2])
	}

	ep := resolveEndpoint(*url, *app, *ca, *nkey)
	nc := connect(ep)
	defer nc.Close()

	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: op, Payload: payload, TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(wire.Call(ep.App, name), data, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "call %s %s: %v\n", name, op, err)
		os.Exit(1)
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		log.Fatalf("reply: %v", err)
	}
	if reply.Status == "error" {
		fmt.Fprintf(os.Stderr, "error: %s: %s\n", reply.Error.Type, reply.Error.Message)
		os.Exit(1)
	}
	fmt.Println(string(reply.Payload))
}

// credFlags registers the shared --ca/--nkey credential flags on a subcommand's flag set.
func credFlags(fs *flag.FlagSet) (ca, nkey *string) {
	return fs.String("ca", "", "CA file for a secured bus (default: "+endpointFile+"/env)"),
		fs.String("nkey", "", "nkey seed file for a secured bus (default: "+endpointFile+"/env)")
}

// dial connects to the bus, applying the endpoint's credentials (CA/nkey) for a secured
// external cluster. It returns the error (no os.Exit) so it can be tested; connect wraps it.
func dial(ep endpoint) (*nats.Conn, error) {
	opts, err := ether.ClientOptions(ep.CA, ep.NkeySeed)
	if err != nil {
		return nil, err
	}
	return nats.Connect(ep.URL, opts...)
}

func connect(ep endpoint) *nats.Conn {
	nc, err := dial(ep)
	if err != nil {
		log.Fatalf("connection to %s failed: %v", ep.URL, err)
	}
	return nc
}

// resolveEndpoint layers each field flag > .aether-endpoint > env (AETHER_NATS_URL / AETHER_APP
// / AETHER_NATS_CA / AETHER_NATS_NKEY_SEED), the last two being the same names the lord injects
// into thralls, so operating a secured cluster needs no extra wiring.
func resolveEndpoint(flagURL, flagApp, flagCA, flagNkey string) endpoint {
	ep := endpoint{URL: flagURL, App: flagApp, CA: flagCA, NkeySeed: flagNkey}
	if data, err := os.ReadFile(endpointFile); err == nil {
		var fromFile endpoint
		if json.Unmarshal(data, &fromFile) == nil {
			if ep.URL == "" {
				ep.URL = fromFile.URL
			}
			if ep.App == "" {
				ep.App = fromFile.App
			}
			if ep.CA == "" {
				ep.CA = fromFile.CA
			}
			if ep.NkeySeed == "" {
				ep.NkeySeed = fromFile.NkeySeed
			}
		}
	}
	if ep.URL == "" {
		ep.URL = os.Getenv("AETHER_NATS_URL")
	}
	if ep.App == "" {
		ep.App = os.Getenv("AETHER_APP")
	}
	if ep.CA == "" {
		ep.CA = os.Getenv("AETHER_NATS_CA")
	}
	if ep.NkeySeed == "" {
		ep.NkeySeed = os.Getenv("AETHER_NATS_NKEY_SEED")
	}
	if ep.URL == "" {
		log.Fatalf("unknown bus address - run from a directory with `aether up`, or pass --url")
	}
	return ep
}

func writeEndpoint(ep endpoint) {
	data, _ := json.Marshal(ep)
	if err := os.WriteFile(endpointFile, data, 0o644); err != nil {
		log.Printf("failed to write %s: %v", endpointFile, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: aether <up|ps|describe|events|fleet|cast|call|down> ...")
	os.Exit(2)
}
