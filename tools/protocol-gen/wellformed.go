package main

import (
	"fmt"

	"github.com/hamicek/aether/sdk/go/session"
)

// WellFormed checks a projected automaton is sound: every edge lands on a real state, every state is
// reachable from the start, and the conversation can terminate (a final state is reachable). These
// are the design-time guarantees of the CI slice - well-formedness of the contract, not of any
// running system (design.md §14).
func WellFormed(a *session.Automaton) error {
	// Closure: no edge dangles. This also catches an unresolved goto that slipped through.
	for name, st := range a.States {
		for op, e := range st.On {
			if _, ok := a.States[e.Next]; !ok {
				return fmt.Errorf("protocol %s role %s: op %q in state %s points to missing state %s",
					a.Protocol, a.Role, op, name, e.Next)
			}
		}
	}

	reachable := map[string]bool{}
	var walk func(string)
	walk = func(n string) {
		if reachable[n] {
			return
		}
		reachable[n] = true
		for _, e := range a.States[n].On {
			walk(e.Next)
		}
	}
	walk(a.Start)

	for name := range a.States {
		if reachable[name] {
			continue
		}
		if name == endState {
			return fmt.Errorf("protocol %s role %s: no reachable end - the conversation can never terminate",
				a.Protocol, a.Role)
		}
		return fmt.Errorf("protocol %s role %s: state %s is unreachable", a.Protocol, a.Role, name)
	}
	return nil
}
