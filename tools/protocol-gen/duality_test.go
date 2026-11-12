package main

import (
	"strings"
	"testing"
)

func TestDualityFixtures(t *testing.T) {
	for _, f := range []string{"control-command.protocol.yaml", "driver-control.protocol.yaml"} {
		p := readFixture(t, f)
		if err := CheckDuality(p); err != nil {
			t.Fatalf("%s: expected dual projections, got %v", f, err)
		}
	}
}

// A hand-built pair that is not dual: both roles "send" the same op. The checker must reject it.
// (A single protocol.yaml can never produce this, which is exactly why the checker is the guard for
// separately authored role types.)
func TestDualityRejectsSameDirection(t *testing.T) {
	a := &Automaton{Protocol: "P", Role: "a", Start: "s", States: map[string]*AutoState{
		"s":   {On: map[string]Edge{"x": {Dir: dirSend, Kind: "cast", Next: "end"}}},
		"end": {Final: true, On: map[string]Edge{}},
	}}
	b := &Automaton{Protocol: "P", Role: "b", Start: "s", States: map[string]*AutoState{
		"s":   {On: map[string]Edge{"x": {Dir: dirSend, Kind: "cast", Next: "end"}}},
		"end": {Final: true, On: map[string]Edge{}},
	}}
	if err := dual("P", a, b); err == nil || !strings.Contains(err.Error(), "not dual") {
		t.Fatalf("want not-dual error, got %v", err)
	}
}

// A pair that disagrees on the offered op set must be rejected too.
func TestDualityRejectsDifferentOps(t *testing.T) {
	a := &Automaton{Protocol: "P", Role: "a", Start: "s", States: map[string]*AutoState{
		"s":   {On: map[string]Edge{"x": {Dir: dirSend, Kind: "cast", Next: "end"}}},
		"end": {Final: true, On: map[string]Edge{}},
	}}
	b := &Automaton{Protocol: "P", Role: "b", Start: "s", States: map[string]*AutoState{
		"s":   {On: map[string]Edge{"y": {Dir: dirRecv, Kind: "cast", Next: "end"}}},
		"end": {Final: true, On: map[string]Edge{}},
	}}
	if err := dual("P", a, b); err == nil {
		t.Fatalf("want op-set mismatch error, got nil")
	}
}
