package main

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/session"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// describeOf derives a thrall's self-report from its definition the same way the SDK does on the
// wire: the handler-map keys are the ops it answers. This is what the lord sees in the heartbeat, so
// checking the committed automaton against it is the same check that runs against a live thrall.
func describeOf[S any](def thrall.Def[S]) wire.ThrallDescribe {
	return wire.ThrallDescribe{CallOps: keys(def.HandleCall), CastOps: keys(def.HandleCast), Version: def.Version}
}

func keys[V any](m map[string]V) []string {
	if len(m) == 0 {
		return nil
	}
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func loadAutomaton(t *testing.T, file string) *session.Automaton {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("gen", "automaton", file))
	if err != nil {
		t.Fatalf("read automaton %s: %v", file, err)
	}
	a, err := session.Load(data)
	if err != nil {
		t.Fatalf("load automaton %s: %v", file, err)
	}
	return a
}

// The device declares a handler for every op its role receives across both protocols. If a protocol
// gained a received op and the thrall was not updated, this would fail - the point of the slice.
func TestDeviceConformsToProtocols(t *testing.T) {
	d := describeOf(DeviceDef())
	for _, file := range []string{"control-command.device.json", "driver-control.device.json"} {
		a := loadAutomaton(t, file)
		if errs := a.ConformsTo(d); len(errs) != 0 {
			t.Fatalf("%s: device does not conform: %v", file, errs)
		}
	}
}

// Dropping a handler the role needs is exactly the "thrall deployed without an op for its role" bug
// the conformance lint is meant to catch.
func TestMissingHandlerIsCaught(t *testing.T) {
	def := DeviceDef()
	delete(def.HandleCall, "write")
	d := describeOf(def)

	a := loadAutomaton(t, "driver-control.device.json")
	errs := a.ConformsTo(d)
	if len(errs) != 1 {
		t.Fatalf("want exactly one missing-handler error, got %v", errs)
	}
}
