// Package session is the consumer side of aether's session types (AE-090, Tier 1). A session type
// describes a binary conversation - the order, direction and mandatory replies between two thralls -
// as a language-neutral automaton. The generator (tools/protocol-gen) projects a *.protocol.yaml
// onto this automaton and emits it as JSON; this package loads that JSON and offers the design-time
// conformance lint. It deliberately holds no YAML and no transport: the automaton JSON is the single
// shared artifact, which is what lets a Go, TS or Python thrall check the same contract.
//
// This is a design-time contract, not a runtime monitor. The monitor (Tier 2) would interpret the
// very same automaton to enforce ordering at runtime; this slice omits it on purpose (design.md §14).
package session

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hamicek/aether/internal/wire"
)

// Direction of an op from a role's point of view.
const (
	DirSend = "send" // ! : the role sends this op
	DirRecv = "recv" // ? : the role receives this op
)

// Automaton is the projection of a protocol onto one role: a data-driven state machine over ops.
// It is the single language-neutral artifact the generator emits and (later) the monitor interprets.
type Automaton struct {
	Protocol string                `json:"protocol"`
	Role     string                `json:"role"`
	Start    string                `json:"start"`
	States   map[string]*AutoState `json:"states"`
}

// AutoState is one phase of the conversation. Final marks the end (On is then empty); On maps each
// legal op in this phase to its edge.
type AutoState struct {
	Final bool            `json:"final,omitempty"`
	On    map[string]Edge `json:"on,omitempty"`
}

// Edge is a single legal transition: the direction and wire kind of the op, the payload schema it
// carries (the composition point with the payload contract - the op names which shape it carries),
// and the next state. Payload is empty when the protocol declares none for that op.
type Edge struct {
	Dir     string `json:"dir"`  // "send" (!) | "recv" (?)
	Kind    string `json:"kind"` // "call" | "cast" | "reply"
	Payload string `json:"payload,omitempty"`
	Next    string `json:"next"`
}

// Load decodes an automaton from the JSON the generator emits.
func Load(data []byte) (*Automaton, error) {
	var a Automaton
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("load automaton: %w", err)
	}
	return &a, nil
}

// ConformsTo is the name-existence conformance lint (design.md §5c, §14). A thrall that plays this
// role must have a handler for every op its role receives, so every received call/cast op must
// appear in the thrall's self-reported ThrallDescribe (call_ops or cast_ops, the handler-map keys).
//
// It is deliberately a *name* check, not a phase or direction check: ThrallDescribe is a flat set of
// ops with no states, and the FSM behaviour reports every op in both call_ops and cast_ops, so it
// can only answer "does a handler of that name exist", never "is the op legal now" or "is it a call
// or a cast". Ordering and direction are enforced only by the runtime monitor (Tier 2). Reply ops
// are excluded because a reply is the return value of a call, not a handler.
func (a *Automaton) ConformsTo(d wire.ThrallDescribe) []error {
	declared := make(map[string]struct{}, len(d.CallOps)+len(d.CastOps))
	for _, op := range d.CallOps {
		declared[op] = struct{}{}
	}
	for _, op := range d.CastOps {
		declared[op] = struct{}{}
	}

	missing := map[string]struct{}{}
	for _, st := range a.States {
		for op, e := range st.On {
			if e.Dir != DirRecv || e.Kind == "reply" {
				continue // the role only needs handlers for what it receives; replies are not handlers
			}
			if _, ok := declared[op]; !ok {
				missing[op] = struct{}{}
			}
		}
	}

	ops := make([]string, 0, len(missing))
	for op := range missing {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	errs := make([]error, 0, len(ops))
	for _, op := range ops {
		errs = append(errs, fmt.Errorf("role %s receives op %q but the thrall declares no handler for it", a.Role, op))
	}
	return errs
}
