package main

import (
	"fmt"
	"sort"

	"github.com/hamicek/aether/internal/wire"
)

// ConformsTo is the name-existence conformance lint (design.md §5c, §14). A thrall that plays this
// role must have a handler for every op its role receives, so every received call/cast op must
// appear in the thrall's self-reported ThrallDescribe (call_ops or cast_ops, the handler-map keys).
//
// It is deliberately a *name* check, not a phase or direction check. ThrallDescribe is a flat set of
// ops with no states, and the FSM behaviour reports every op in both call_ops and cast_ops, so this
// can only answer "does a handler of that name exist", not "is the op legal now" or "is it a call or
// a cast". Ordering and direction are enforced only by the runtime monitor (Tier 2), which this CI
// slice omits. Reply ops are excluded because a reply is the return value of a call, not a handler.
func ConformsTo(a *Automaton, d wire.ThrallDescribe) []error {
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
			if e.Dir != dirRecv || e.Kind == "reply" {
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
