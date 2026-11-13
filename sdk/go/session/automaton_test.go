package session

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/wire"
)

// a small automaton for role "device": it receives write and health (calls) and sends their replies.
func deviceAutomaton() *Automaton {
	return &Automaton{
		Protocol: "DriverControl", Role: "device", Start: "s1",
		States: map[string]*AutoState{
			"s1": {On: map[string]Edge{
				"write":  {Dir: DirRecv, Kind: "call", Next: "s2"},
				"health": {Dir: DirRecv, Kind: "call", Next: "s3"},
			}},
			"s2": {On: map[string]Edge{
				"applied": {Dir: DirSend, Kind: "reply", Next: "s1"},
				"fault":   {Dir: DirSend, Kind: "reply", Next: "end"},
			}},
			"s3":  {On: map[string]Edge{"status": {Dir: DirSend, Kind: "reply", Next: "s1"}}},
			"end": {Final: true, On: map[string]Edge{}},
		},
	}
}

func TestConformsToOK(t *testing.T) {
	a := deviceAutomaton()
	d := wire.ThrallDescribe{CallOps: []string{"write", "health"}}
	if errs := a.ConformsTo(d); len(errs) != 0 {
		t.Fatalf("expected conformance, got %v", errs)
	}
}

func TestConformsToReportsMissingHandler(t *testing.T) {
	a := deviceAutomaton()
	d := wire.ThrallDescribe{CallOps: []string{"write"}} // health handler missing
	errs := a.ConformsTo(d)
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "health") {
		t.Fatalf("want a single missing-health error, got %v", errs)
	}
}

func TestConformsToIgnoresSendsAndReplies(t *testing.T) {
	// A sender-only role (sends writes, receives replies) needs no handlers, so an empty describe
	// conforms. This pins the "received call/cast only" semantics.
	sender := &Automaton{
		Protocol: "P", Role: "collector", Start: "s1",
		States: map[string]*AutoState{
			"s1":  {On: map[string]Edge{"write": {Dir: DirSend, Kind: "call", Next: "s2"}}},
			"s2":  {On: map[string]Edge{"ok": {Dir: DirRecv, Kind: "reply", Next: "end"}}},
			"end": {Final: true, On: map[string]Edge{}},
		},
	}
	if errs := sender.ConformsTo(wire.ThrallDescribe{}); len(errs) != 0 {
		t.Fatalf("sender needs no handlers, got %v", errs)
	}
}

func TestLoadRoundTrip(t *testing.T) {
	blob := []byte(`{"protocol":"P","role":"device","start":"s1","states":{"s1":{"on":{"x":{"dir":"recv","kind":"cast","next":"end"}}},"end":{"final":true}}}`)
	a, err := Load(blob)
	if err != nil {
		t.Fatal(err)
	}
	if a.Role != "device" || a.States["s1"].On["x"].Dir != DirRecv || !a.States["end"].Final {
		t.Fatalf("round-trip lost data: %+v", a)
	}
}
