package lord

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

// What the lord does after it gives up on a thrall: carry on without it (the default), or end the
// node so that the supervisor above restarts it (on_give_up = "exit").

// crashLoop is a thrall that can never be kept running: it exits abnormally as soon as it starts.
func crashLoop(name string) ThrallSpec {
	return ThrallSpec{Name: name, Cmd: "sh -c 'exit 1'", Restart: "permanent", Scope: "local"}
}

// giveUpManifest is a node with one healthy thrall and one that crash-loops, with a restart
// intensity the crash loop exceeds on its second restart.
func giveUpManifest(t *testing.T, onGiveUp string) *Manifest {
	t.Helper()
	return &Manifest{
		App:              "giveup",
		Strategy:         "one_for_one",
		RestartIntensity: Intensity{Max: 1, WithinMs: 60000},
		OnGiveUp:         onGiveUp,
		Thralls: []ThrallSpec{
			{Name: "healthy", Cmd: probeCmd(t), Restart: "permanent", Scope: "local"},
			crashLoop("broken"),
		},
	}
}

// lifecycle subscribes to the lord's event stream before the lord starts and returns a function
// that reports whether the named event for the named thrall has been seen so far.
func lifecycle(t *testing.T, eth *ether.Ether) func(event, name string) bool {
	t.Helper()
	sub, err := eth.Conn().SubscribeSync(wire.Events)
	if err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	seen := map[string]bool{}
	return func(event, name string) bool {
		for {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			if errors.Is(err, nats.ErrTimeout) {
				break
			}
			if err != nil {
				t.Fatalf("events: %v", err)
			}
			var ev struct {
				Event string `json:"event"`
				Name  string `json:"name"`
			}
			if json.Unmarshal(msg.Data, &ev) == nil {
				seen[ev.Event+"/"+ev.Name] = true
			}
		}
		return seen[event+"/"+name]
	}
}

// TestGiveUpExitEndsTheNode: with on_give_up = "exit" a thrall the lord cannot keep running ends
// the node - the lord reports why, having first made the give-up visible the usual way.
func TestGiveUpExitEndsTheNode(t *testing.T) {
	eth := startEmbedded(t)
	saw := lifecycle(t, eth)
	l := startLord(t, eth, giveUpManifest(t, GiveUpExit))

	var failure error
	select {
	case failure = <-l.Failed():
	case <-time.After(15 * time.Second):
		t.Fatal("the lord did not fail after giving up on a thrall")
	}
	var gaveUp *GaveUpError
	if !errors.As(failure, &gaveUp) || gaveUp.Thrall != "broken" || gaveUp.Starts < 2 {
		t.Fatalf("failure = %v, want a GaveUpError naming the broken thrall", failure)
	}
	if !strings.Contains(failure.Error(), `"broken"`) {
		t.Errorf("the failure must name the thrall: %v", failure)
	}

	waitFor(t, 2*time.Second, "the gave_up and node_exit events", func() bool {
		return saw("gave_up", "broken") && saw("node_exit", "broken")
	})
	if got := l.metrics.snapshot()["broken"].GaveUp; got != 1 {
		t.Errorf("aether_gave_up_total for the broken thrall = %d, want 1", got)
	}
	if !l.stopping() {
		t.Error("a failed lord must stop supervising: it would otherwise keep restarting thralls while its node is being shut down")
	}
	select {
	case again := <-l.Failed():
		t.Errorf("Failed yielded a second time: %v", again)
	default:
	}
}

// TestGiveUpContinueKeepsTheNodeRunning: the default is today's behaviour - the thrall stays
// down, the give-up is visible as an event and a metric, and the lord carries on supervising the
// rest of the node without ever failing.
func TestGiveUpContinueKeepsTheNodeRunning(t *testing.T) {
	for name, onGiveUp := range map[string]string{"unset": "", "continue": GiveUpContinue} {
		t.Run(name, func(t *testing.T) {
			eth := startEmbedded(t)
			saw := lifecycle(t, eth)
			l := startLord(t, eth, giveUpManifest(t, onGiveUp))
			nc := eth.Conn()
			waitReady(t, eth, "healthy")

			waitFor(t, 15*time.Second, "the lord gives up on the broken thrall", func() bool {
				return saw("gave_up", "broken")
			})
			if got := l.metrics.snapshot()["broken"].GaveUp; got != 1 {
				t.Errorf("aether_gave_up_total for the broken thrall = %d, want 1", got)
			}

			select {
			case err := <-l.Failed():
				t.Fatalf("the lord failed with the default on_give_up: %v", err)
			case <-time.After(time.Second):
			}
			if saw("node_exit", "broken") {
				t.Error("node_exit was emitted although the node carries on")
			}
			if l.stopping() {
				t.Error("the lord stopped supervising")
			}

			// The rest of the node is still supervised: a crash of the healthy thrall is restarted.
			pid := callInt(t, nc, "giveup", "healthy", "pid")
			cast(t, nc, "giveup", "healthy", "crash")
			waitFor(t, 10*time.Second, "the healthy thrall is restarted after the give-up", func() bool {
				v, ok := tryCallInt(nc, "giveup", "healthy", "pid")
				return ok && v != pid
			})
		})
	}
}

// TestGiveUpExitDoesNotCoverDynamicChildren: a child spawned at runtime is not part of the node
// the manifest declares, so giving up on it retires it as before and the node lives on.
func TestGiveUpExitDoesNotCoverDynamicChildren(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "giveup", "one_for_one", spec("static", "permanent", "local"))
	m.RestartIntensity = Intensity{Max: 1, WithinMs: 60000}
	m.OnGiveUp = GiveUpExit
	l := startLord(t, eth, m)
	nc := eth.Conn()
	waitReady(t, eth, "static")

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: "sh -c 'exit 1'", Restart: "permanent"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitFor(t, 15*time.Second, "the dynamic child is given up on and retired", func() bool {
		return !childInSlice(l, "dyn")
	})

	select {
	case err := <-l.Failed():
		t.Fatalf("giving up on a dynamic child ended the node: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if _, ok := tryCallInt(nc, "giveup", "static", "pid"); !ok {
		t.Error("the static thrall stopped answering")
	}
}
