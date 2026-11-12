package main

import "encoding/json"

// EmitAutomaton renders a projected automaton as deterministic JSON. encoding/json writes map keys
// (states and ops) in sorted order, so the output depends only on the protocol, not on traversal
// order - which is what makes the drift diff meaningful (same principle as the payload codegen).
func EmitAutomaton(a *Automaton) ([]byte, error) {
	b, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
