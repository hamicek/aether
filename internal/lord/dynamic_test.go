package lord

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/wire"
)

// control sends a spawn/stop request on the lord's control channel and returns the
// reply envelope.
func control(t *testing.T, nc *nats.Conn, op string, payload any) wire.Envelope {
	t.Helper()
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCtl, Op: op,
		Payload: mustJSON(payload), TS: time.Now().UnixMilli()}
	msg, err := nc.Request(wire.LordCtl(), mustJSON(req), 2*time.Second)
	if err != nil {
		t.Fatalf("control %s request: %v", op, err)
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("control %s reply: %v", op, err)
	}
	return reply
}

func spawnDynamic(t *testing.T, nc *nats.Conn, spec wire.SpawnSpec) wire.Envelope {
	t.Helper()
	return control(t, nc, wire.OpSpawn, spec)
}

// TestDynamicSpawnAndSupervise: a thrall started at runtime (not in the manifest) comes
// up, is on the ether, answers a call, and is recorded ready in the registry.
func TestDynamicSpawnAndSupervise(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	reply := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"})
	if reply.Status != "ok" {
		t.Fatalf("spawn reply status = %q, want ok (err=%+v)", reply.Status, reply.Error)
	}
	waitReady(t, eth, "dyn")
	if _, ok := tryCallInt(nc, "demo", "dyn", "pid"); !ok {
		t.Fatal("dynamic thrall does not answer call")
	}
}

// TestDynamicRestartAfterCrash: a permanent dynamic child is restarted on crash, just
// like a static one.
func TestDynamicRestartAfterCrash(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")
	pid := callInt(t, nc, "demo", "dyn", "pid")

	cast(t, nc, "demo", "dyn", "crash")
	waitFor(t, 5*time.Second, "dynamic child restarted", func() bool {
		p, ok := tryCallInt(nc, "demo", "dyn", "pid")
		return ok && p != pid
	})
}

// TestDynamicStop: a runtime stop drains the child and it is not restarted afterwards.
func TestDynamicStop(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")

	if r := control(t, nc, wire.OpStop, wire.StopSpec{Name: "dyn"}); r.Status != "ok" {
		t.Fatalf("stop reply: %+v", r.Error)
	}
	// It must go down and stay down (no restart).
	reg, _ := registry.Open(eth.Conn())
	waitFor(t, 5*time.Second, "dynamic child down after stop", func() bool {
		e, ok, err := reg.Get("dyn")
		return err == nil && ok && e.Status == "down"
	})
	time.Sleep(500 * time.Millisecond)
	if _, ok := tryCallInt(nc, "demo", "dyn", "pid"); ok {
		t.Fatal("stopped dynamic thrall still answers - it was restarted")
	}
}

// TestDynamicStopRejectsStatic: static children are owned by the manifest and cannot be
// stopped via the runtime API.
func TestDynamicStopRejectsStatic(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	if r := control(t, nc, wire.OpStop, wire.StopSpec{Name: "static"}); r.Status != "error" {
		t.Fatalf("stopping a static child should be refused, got status %q", r.Status)
	}
}

// TestDynamicOutsideGroupStrategy: under one_for_all, a static child's crash restarts the
// static group but leaves a dynamic child untouched - the dynamic child is supervised
// one_for_one, outside the manifest group.
func TestDynamicOutsideGroupStrategy(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_all",
		spec("s1", "permanent", "local"), spec("s2", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "s1")
	waitReady(t, eth, "s2")

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")
	dynPid := callInt(t, nc, "demo", "dyn", "pid")
	s2Pid := callInt(t, nc, "demo", "s2", "pid")

	// Crash s1 -> one_for_all restarts the static group (s2 gets a new pid)...
	cast(t, nc, "demo", "s1", "crash")
	waitFor(t, 5*time.Second, "static group restarted", func() bool {
		p, ok := tryCallInt(nc, "demo", "s2", "pid")
		return ok && p != s2Pid
	})
	// ...but the dynamic child, outside the group, keeps its pid.
	if p, ok := tryCallInt(nc, "demo", "dyn", "pid"); !ok || p != dynPid {
		t.Fatalf("dynamic child pid changed (%d -> %d, ok=%v) - it was pulled into the group", dynPid, p, ok)
	}
}

// TestDynamicSpawnIdempotent: a repeat spawn of a name already under supervision is an
// idempotent no-op, not an error, and does not start a second process. This is what lets
// an owner call StartChild blindly from its init to re-establish topology after a lord
// restart.
func TestDynamicSpawnIdempotent(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	sp := wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"}
	if r := spawnDynamic(t, nc, sp); r.Status != "ok" {
		t.Fatalf("first spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")
	pid := callInt(t, nc, "demo", "dyn", "pid")

	r := spawnDynamic(t, nc, sp)
	if r.Status != "ok" {
		t.Fatalf("second spawn should be an idempotent ok, got status %q (err=%+v)", r.Status, r.Error)
	}
	var reply wire.SpawnReply
	if err := json.Unmarshal(r.Payload, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply.Name != "dyn" {
		t.Fatalf("reply name = %q, want dyn", reply.Name)
	}
	// Same pid => no second process was spawned. Allow a moment for an (erroneous) restart.
	time.Sleep(300 * time.Millisecond)
	pid2, ok := tryCallInt(nc, "demo", "dyn", "pid")
	if !ok {
		t.Fatal("dynamic thrall stopped answering after the idempotent spawn")
	}
	if pid2 != pid {
		t.Fatalf("pid changed after the idempotent spawn (%d -> %d) - a second process was started", pid, pid2)
	}
}

// TestDynamicSpawnRevivesDeadChild: a re-spawn of a name whose dead dynamic entry is still in
// the slice must bring a fresh process back, not silently no-op on the corpse. The lord now
// retires a dead dynamic child on exit, so this dead-entry-still-present state is the race
// window between the exit's retirement and the re-spawn - the revive path covers it either
// way: if the entry is gone the spawn just appends fresh, if it lingers the spawn replaces it.
func TestDynamicSpawnRevivesDeadChild(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	// A temporary child is not restarted when it exits, so after a crash it is dead - either
	// already retired or briefly lingering; a re-spawn must bring a fresh process back regardless.
	sp := wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "temporary"}
	if r := spawnDynamic(t, nc, sp); r.Status != "ok" {
		t.Fatalf("first spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")
	pid1 := callInt(t, nc, "demo", "dyn", "pid")

	cast(t, nc, "demo", "dyn", "crash")
	waitFor(t, 5*time.Second, "dead temporary child stops answering", func() bool {
		_, ok := tryCallInt(nc, "demo", "dyn", "pid")
		return !ok
	})

	if r := spawnDynamic(t, nc, sp); r.Status != "ok" {
		t.Fatalf("re-spawn of a dead child: %+v", r.Error)
	}
	waitFor(t, 5*time.Second, "dead child revived with a new process", func() bool {
		pid2, ok := tryCallInt(nc, "demo", "dyn", "pid")
		return ok && pid2 != pid1
	})
}

// childInSlice reports whether a child of the given name is currently in the supervision
// slice (read under the same lock the supervisor uses).
func childInSlice(l *Lord, name string) bool {
	l.childrenMu.RLock()
	defer l.childrenMu.RUnlock()
	for _, c := range l.children {
		if c.spec.Name == name {
			return true
		}
	}
	return false
}

// TestDynamicTemporaryChildRemovedOnExit: a dynamic temporary child that exits is not
// restarted (DontRestart), so the lord must drop it from the supervision slice instead of
// leaving a dead entry to accumulate.
func TestDynamicTemporaryChildRemovedOnExit(t *testing.T) {
	eth := startEmbedded(t)
	l := startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "temporary"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")

	cast(t, nc, "demo", "dyn", "crash")
	waitFor(t, 5*time.Second, "dead temporary child removed from supervision slice", func() bool {
		return !childInSlice(l, "dyn")
	})
	// The static sibling is untouched.
	if !childInSlice(l, "static") {
		t.Fatal("static child disappeared from the supervision slice")
	}
}

// TestStaticChildNotRemovedOnDontRestart: a static (manifest) temporary child that exits also
// hits DontRestart, but it is owned by the manifest and drained on Stop, so it must stay in
// the supervision slice - only dynamic children are retired.
func TestStaticChildNotRemovedOnDontRestart(t *testing.T) {
	eth := startEmbedded(t)
	l := startLord(t, eth, manifest(t, "demo", "one_for_one",
		spec("keeper", "permanent", "local"),
		spec("victim", "temporary", "local"),
	))
	nc := eth.Conn()
	waitReady(t, eth, "victim")

	cast(t, nc, "demo", "victim", "crash")
	waitFor(t, 5*time.Second, "static temporary child stops answering", func() bool {
		_, ok := tryCallInt(nc, "demo", "victim", "pid")
		return !ok
	})
	if !childInSlice(l, "victim") {
		t.Fatal("static temporary child was removed from the supervision slice; only dynamic children should be retired")
	}
}

// TestDynamicChildRemovedAfterGiveUp: once the lord gives up restarting a dynamic child that
// keeps crashing past the restart-intensity cap, the dead child must be dropped from the
// supervision slice, not left to accumulate. The static sibling stays supervised throughout.
func TestDynamicChildRemovedAfterGiveUp(t *testing.T) {
	eth := startEmbedded(t)
	m := manifest(t, "demo", "one_for_one", spec("static", "permanent", "local"))
	m.RestartIntensity = Intensity{Max: 1, WithinMs: 60000} // give up after a couple of restarts
	l := startLord(t, eth, m)
	nc := eth.Conn()
	waitReady(t, eth, "static")

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "dyn", Cmd: probeCmd(t), Restart: "permanent"}); r.Status != "ok" {
		t.Fatalf("spawn: %+v", r.Error)
	}
	waitReady(t, eth, "dyn")

	// Crash it whenever it comes back up; the lord restarts it until the intensity cap trips,
	// then gives up - at which point the child is retired from the slice.
	waitFor(t, 10*time.Second, "dynamic child gives up and is removed from the slice", func() bool {
		if !childInSlice(l, "dyn") {
			return true
		}
		if _, ok := tryCallInt(nc, "demo", "dyn", "pid"); ok {
			cast(t, nc, "demo", "dyn", "crash")
		}
		return false
	})
	if !childInSlice(l, "static") {
		t.Fatal("static child disappeared from the supervision slice")
	}
}

// TestDynamicSpawnNameConflictsStatic: a dynamic spawn must not shadow a manifest name -
// that is a genuine misconfiguration the author must see, not an idempotent no-op.
func TestDynamicSpawnNameConflictsStatic(t *testing.T) {
	eth := startEmbedded(t)
	startLord(t, eth, manifest(t, "demo", "one_for_one", spec("static", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "static")

	if r := spawnDynamic(t, nc, wire.SpawnSpec{Name: "static", Cmd: probeCmd(t)}); r.Status != "error" {
		t.Fatalf("spawning over a manifest name should be refused, got %q", r.Status)
	}
	// The refused spawn must not have disturbed the existing static child.
	if _, ok := tryCallInt(nc, "demo", "static", "pid"); !ok {
		t.Fatal("static child stopped answering after a refused dynamic spawn over its name")
	}
}
