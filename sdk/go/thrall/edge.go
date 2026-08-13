package thrall

// Edge is the fourth thrall shape in the SDK, alongside the GenServer Def, the FSM and the
// EventManager - but unlike them it is NOT a behaviour: it has no mailbox to react to. An edge owns
// a socket or connection whose input arrives from OUTSIDE the ether (a push - HTTP, a Modbus/OPC-UA
// driver, cron, tail), which is concurrent and stateless and does not fit a serialized mailbox. The
// user supplies a run-loop (owning the socket) and a graceful-stop hook instead of call/cast
// handlers; the rest - spawn, heartbeat, drain and fencing - is the same machinery every thrall gets.
//
// It is the "model B" edge (you write the code): the counterpart to the declarative built-in HTTP
// ingress (model A, `[[edge.http]]`). Use it when the ingress cannot be expressed by configuration
// (custom auth, transformation, a non-HTTP protocol). State still lives in ordinary thralls behind
// it, reached via ctx.Call / ctx.Cast.

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// EdgeDef defines an edge thrall: a run-loop that owns the socket and an optional graceful-stop hook.
type EdgeDef struct {
	Name string // taken from AETHER_NAME when empty
	// Init runs once before Run, for setup that may fail (open a listener, dial a device).
	Init func(ctx *Ctx) error
	// Run is the socket-owning loop. It runs until stop is closed (a drain from the lord) and must
	// honor it. Returning (even nil) ends the edge; returning an error logs it - the lord then
	// restarts the process per its restart policy.
	Run func(ctx *Ctx, stop <-chan struct{}) error
	// Stop is an optional graceful-stop hook invoked on drain, before waiting for Run to finish -
	// e.g. http.Server.Shutdown, which unblocks a Run blocked in Serve.
	Stop func()
}

// StartEdge connects an edge thrall to the ether and runs its lifecycle. It mirrors Start / StartFSM
// / StartEvent - reusing the shared connect, heartbeat, ctl-drain and fencing plumbing - but runs the
// user's run-loop in place of a serialized mailbox. There is no durable mailbox: an edge pulls nothing
// from the ether, it pushes into it.
func StartEdge(def EdgeDef) error {
	url := os.Getenv("AETHER_NATS_URL")
	app := os.Getenv("AETHER_APP")
	envName := os.Getenv("AETHER_NAME")
	if url == "" || app == "" {
		return fmt.Errorf("missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`")
	}
	name := def.Name
	if name == "" {
		name = envName
	}
	if def.Run == nil {
		return fmt.Errorf("edge %q: Run is required", name)
	}

	opts, err := connectOptions(name)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return err
	}
	sharedConn = nc
	log := obs.NewLogger().With(slog.String("component", "thrall"), slog.String("app", app), slog.String("name", name))
	ctx := &Ctx{NATS: nc, Name: name, App: app, Log: log}

	if def.Init != nil {
		if err := def.Init(ctx); err != nil {
			return fmt.Errorf("edge %q: init: %w", name, err)
		}
	}

	// stop is closed once, from either the ctl drain or a self-terminating run-loop; sync.Once keeps
	// the two racing sources safe.
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := def.Run(ctx, stop); err != nil {
			log.Error("edge run-loop failed", slog.Any("err", err))
		}
	}()

	// ctl: controlled shutdown from the lord (drain / shutdown).
	if _, err := nc.Subscribe(wire.Ctl(name), func(m *nats.Msg) {
		var e wire.Envelope
		if json.Unmarshal(m.Data, &e) == nil && (e.Op == "drain" || e.Op == "shutdown") {
			closeStop()
		}
	}); err != nil {
		return err
	}

	// The edge has no mailbox, so it reports zero self-metrics - the shape the lord's reaper expects.
	go heartbeat(nc, name, &mailboxStats{}, stop)

	if err := startFencingIfSingleton(nc, name, log, stop); err != nil {
		return err
	}
	if err := startLordLivenessFencing(nc, name, log, stop); err != nil {
		return err
	}

	select {
	case <-stop:
		// Drain from the lord: run the graceful hook (unblocks Run), then wait for it to finish.
		if def.Stop != nil {
			def.Stop()
		}
		<-runDone
	case <-runDone:
		// The run-loop ended on its own (e.g. a socket error): close stop so heartbeat and fencing
		// wind down, then tear the connection down - the lord restarts the process per policy.
		closeStop()
	}

	return nc.Drain()
}
