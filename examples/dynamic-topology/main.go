// Command dyntopo-demo shows how a supervising thrall owns its dynamic topology. The
// "manager" spawns a set of worker children from its own init, so the topology
// re-establishes itself after a lord restart (init runs again), and it re-applies the set
// on a "reconcile" cast. Because StartChild is idempotent on name, both are safe to call
// blindly - a worker already running is left untouched, so there are never duplicates.
//
// Dynamic children do not survive a lord restart by design (see DESIGN.md, section 12):
// the lord is an OS process supervisor, so a restart takes the whole process group with
// it. Surviving the restart is the owner's job, demonstrated here.
//
// One binary plays every role, selected by AETHER_NAME (injected by the lord): the static
// "manager" from the manifest, and each dynamically spawned "worker-N".
package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// desiredWorkers is the manager's target topology: the children it wants running at all
// times. It re-establishes exactly this set from init and on every reconcile.
var desiredWorkers = []string{"worker-1", "worker-2", "worker-3"}

// workerCmd is the command the lord runs for each dynamic worker - this same binary, which
// dispatches on AETHER_NAME. The path is relative to the manifest's directory.
const workerCmd = "../../bin/dyntopo-demo"

func main() {
	name := os.Getenv("AETHER_NAME")
	switch {
	case name == "manager":
		runManager()
	case strings.HasPrefix(name, "worker-"):
		runWorker()
	default:
		log.Fatalf("unknown thrall %q", name)
	}
}

// runManager owns the dynamic topology. It spawns the desired workers from init - so they
// come back after a lord restart, when init runs again - and re-applies them on a
// "reconcile" cast.
func runManager() {
	def := thrall.Def[[]string]{
		Init: func(ctx *thrall.Ctx) ([]string, error) {
			reconcile(ctx)
			return desiredWorkers, nil
		},
		HandleCast: map[string]thrall.CastFn[[]string]{
			"reconcile": func(_ json.RawMessage, s []string, ctx *thrall.Ctx) ([]string, error) {
				reconcile(ctx)
				return s, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

// reconcile brings the running topology up to the desired set by asking the lord to spawn
// each worker. A spawn of a worker already under supervision is an idempotent no-op, so
// reconcile is safe to call as often as you like and never creates a duplicate.
func reconcile(ctx *thrall.Ctx) {
	for _, name := range desiredWorkers {
		if _, err := ctx.StartChild(wire.SpawnSpec{Name: name, Cmd: workerCmd, Restart: "permanent"}, 5*time.Second); err != nil {
			ctx.Log.Error("reconcile: spawn worker failed", "worker", name, "err", err)
			continue
		}
		ctx.Log.Info("reconcile: worker ensured", "worker", name)
	}
}

// runWorker is a trivial dynamically-spawned child: it answers a "ping" call so you can
// see it on the ether. It carries no state that must survive a restart; if it did, the
// owner would re-establish it, or the worker would rebuild from an event log (see
// examples/eventsourced).
func runWorker() {
	def := thrall.Def[int]{
		Init: func(*thrall.Ctx) (int, error) { return 0, nil },
		HandleCall: map[string]thrall.CallFn[int]{
			"ping": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) {
				return "pong from " + os.Getenv("AETHER_NAME"), s, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
