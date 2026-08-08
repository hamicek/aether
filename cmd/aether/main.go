// Command aether starts the runtime (up) and provides tools: ps, events, cast, call.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/lord"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
)

// endpointFile holds the URL of the running bus + app, so that ps/events/cast/call
// can connect to it even in embedded mode (where the port is random).
const endpointFile = ".aether-endpoint"

type endpoint struct {
	URL string `json:"url"`
	App string `json:"app"`
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
	case "events":
		eventsCmd(os.Args[2:])
	case "cast":
		castCmd(os.Args[2:])
	case "call":
		callCmd(os.Args[2:])
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

	eth, err := ether.Start(ctx, m.Nats)
	if err != nil {
		log.Fatalf("ether: %v", err)
	}
	defer eth.Stop()
	log.Printf("ether running at %s (mode=%s)", eth.URL(), m.Nats.Mode)

	writeEndpoint(endpoint{URL: eth.URL(), App: m.App})
	defer os.Remove(endpointFile)

	root, err := lord.New(m, eth)
	if err != nil {
		log.Fatalf("lord: %v", err)
	}
	if err := root.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	<-ctx.Done()
	log.Println("shutdown: graceful drain of thralls...")
	root.Stop()
}

// psCmd prints the current state of thralls from the KV registry.
func psCmd(argv []string) {
	fs := flag.NewFlagSet("ps", flag.ExitOnError)
	url := fs.String("url", "", "bus address (default: ."+endpointFile+")")
	_ = fs.Parse(argv)

	ep := resolveEndpoint(*url, "")
	nc := connect(ep.URL)
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
	fmt.Printf("%-14s %-8s %-10s %s\n", "NAME", "PID", "STATUS", "AGE")
	for _, e := range list {
		age := time.Duration(now-e.UpdatedMs) * time.Millisecond
		pid := "-"
		if e.PID > 0 {
			pid = fmt.Sprintf("%d", e.PID)
		}
		fmt.Printf("%-14s %-8s %-10s %s\n", e.Name, pid, e.Status, age.Round(time.Second))
	}
	if len(list) == 0 {
		fmt.Println("(registry empty - is `aether up` running?)")
	}
}

// eventsCmd prints the live lifecycle stream from aether._lord.events (until Ctrl-C).
func eventsCmd(argv []string) {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	_ = fs.Parse(argv)

	ep := resolveEndpoint(*url, "")
	nc := connect(ep.URL)
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

// castCmd sends a fire-and-forget cast: `aether cast <name> <op> [json-payload]`.
func castCmd(argv []string) {
	fs := flag.NewFlagSet("cast", flag.ExitOnError)
	url := fs.String("url", "", "bus address")
	app := fs.String("app", "", "app namespace (default from endpoint)")
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

	ep := resolveEndpoint(*url, *app)
	nc := connect(ep.URL)
	defer nc.Close()

	env := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCast, To: name, Op: op, Payload: payload, TS: time.Now().UnixMilli()}
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

	ep := resolveEndpoint(*url, *app)
	nc := connect(ep.URL)
	defer nc.Close()

	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: op, Payload: payload, TS: time.Now().UnixMilli()}
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

func connect(url string) *nats.Conn {
	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("connection to %s failed: %v", url, err)
	}
	return nc
}

// resolveEndpoint: --url/--app > .aether-endpoint > AETHER_NATS_URL/AETHER_APP.
func resolveEndpoint(flagURL, flagApp string) endpoint {
	ep := endpoint{URL: flagURL, App: flagApp}
	if data, err := os.ReadFile(endpointFile); err == nil {
		var fromFile endpoint
		if json.Unmarshal(data, &fromFile) == nil {
			if ep.URL == "" {
				ep.URL = fromFile.URL
			}
			if ep.App == "" {
				ep.App = fromFile.App
			}
		}
	}
	if ep.URL == "" {
		ep.URL = os.Getenv("AETHER_NATS_URL")
	}
	if ep.App == "" {
		ep.App = os.Getenv("AETHER_APP")
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
	fmt.Fprintln(os.Stderr, "usage: aether <up|ps|events|cast|call|down> ...")
	os.Exit(2)
}
