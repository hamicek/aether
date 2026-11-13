package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hamicek/aether/sdk/go/session"
)

// CheckDuality projects both roles and verifies the two local types are dual: at every paired phase
// they offer the same ops, each op flows the opposite way (one sends, the other receives) with the
// same kind, and they agree on termination. For a single global protocol.yaml the projections are
// dual by construction, so this is a structural self-check of the projection and the point where a
// separately authored pair of role types would be verified - not a proof that running thralls never
// deadlock (the monitor is opt-in and a thrall can always drop to a raw Call/Cast; see design.md §14).
func CheckDuality(p *Protocol) error {
	a, err := Project(p, p.Roles[0])
	if err != nil {
		return err
	}
	b, err := Project(p, p.Roles[1])
	if err != nil {
		return err
	}
	return dual(p.Protocol, a, b)
}

func dual(name string, a, b *session.Automaton) error {
	type pair struct{ x, y string }
	visited := map[pair]bool{}

	var walk func(x, y string) error
	walk = func(x, y string) error {
		if visited[pair{x, y}] {
			return nil
		}
		visited[pair{x, y}] = true

		sx, sy := a.States[x], b.States[y]
		if sx.Final != sy.Final {
			return fmt.Errorf("protocol %s: %s/%s disagree on termination", name, a.Role, b.Role)
		}
		if len(sx.On) != len(sy.On) {
			return fmt.Errorf("protocol %s: %s offers {%s} but %s offers {%s}",
				name, a.Role, opList(sx), b.Role, opList(sy))
		}
		for op, ex := range sx.On {
			ey, ok := sy.On[op]
			if !ok {
				return fmt.Errorf("protocol %s: op %q offered by %s but not by %s", name, op, a.Role, b.Role)
			}
			if !opposite(ex.Dir, ey.Dir) {
				return fmt.Errorf("protocol %s: op %q is not dual (%s %ss, %s %ss)",
					name, op, a.Role, ex.Dir, b.Role, ey.Dir)
			}
			if ex.Kind != ey.Kind {
				return fmt.Errorf("protocol %s: op %q kind mismatch (%s vs %s)", name, op, ex.Kind, ey.Kind)
			}
			if err := walk(ex.Next, ey.Next); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(a.Start, b.Start)
}

func opposite(x, y string) bool {
	return (x == session.DirSend && y == session.DirRecv) || (x == session.DirRecv && y == session.DirSend)
}

func opList(s *session.AutoState) string {
	ops := make([]string, 0, len(s.On))
	for op := range s.On {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return strings.Join(ops, ", ")
}
