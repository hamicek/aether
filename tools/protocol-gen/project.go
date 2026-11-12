package main

import "fmt"

// endState is the reserved name of the single terminal state every automaton carries.
const endState = "end"

// Project builds the local automaton for one role by walking the protocol tree. A message becomes a
// send or a receive (which way depends on the role); a call adds a following reply state whose
// branches flip direction; a choose (internal choice) and a reply (external choice) both fan several
// ops out of one state; a rec/goto pair becomes a back-edge. Gotos are emitted as sentinels and
// resolved once, at the end, so a label used before its rec body finishes projecting still binds.
func Project(p *Protocol, role string) (*Automaton, error) {
	pr := &projector{proto: p, role: role, states: map[string]*AutoState{}, labels: map[string]string{}}
	pr.states[endState] = &AutoState{Final: true, On: map[string]Edge{}}
	start := pr.seq(p.Flow, endState)
	if err := pr.resolveGotos(); err != nil {
		return nil, err
	}
	return &Automaton{Protocol: p.Protocol, Role: role, Start: start, States: pr.states}, nil
}

type projector struct {
	proto  *Protocol
	role   string
	states map[string]*AutoState
	labels map[string]string // rec label -> entry state name (filled as bodies are projected)
	n      int
}

func (pr *projector) fresh() string {
	pr.n++
	return fmt.Sprintf("s%d", pr.n)
}

func (pr *projector) state(name string) *AutoState {
	st, ok := pr.states[name]
	if !ok {
		st = &AutoState{On: map[string]Edge{}}
		pr.states[name] = st
	}
	return st
}

// seq projects a sequence of steps whose fall-through continuation is cont, returning the entry
// state. It builds the tail first so each step can point at the state that follows it.
func (pr *projector) seq(steps []Step, cont string) string {
	if len(steps) == 0 {
		return cont
	}
	rest := pr.seq(steps[1:], cont)
	return pr.step(steps[0], rest)
}

func (pr *projector) step(s Step, after string) string {
	switch {
	case s.isRec():
		entry := pr.seq(s.Body, after)
		pr.labels[s.Rec] = entry
		return entry
	case s.isChoose():
		entry := pr.fresh()
		st := pr.state(entry)
		for _, o := range s.Choose {
			pr.emit(st, o, after)
		}
		return entry
	default:
		entry := pr.fresh()
		pr.emit(pr.state(entry), s, after)
		return entry
	}
}

// emit adds one message's leading edge to state st, and for a call also the following reply state.
// The call/reply pair flips direction: the caller sends the call and receives the reply, the callee
// the other way around.
func (pr *projector) emit(st *AutoState, s Step, after string) {
	dir := pr.dir(s.From)
	if s.Kind == "cast" {
		st.On[s.Op] = Edge{Dir: dir, Kind: "cast", Next: after}
		return
	}
	reply := pr.fresh()
	st.On[s.Op] = Edge{Dir: dir, Kind: "call", Next: reply}

	replyDir := dirSend
	if s.From == pr.role {
		replyDir = dirRecv // the caller receives its reply
	}
	rst := pr.state(reply)
	for _, b := range s.Reply.Choice {
		rst.On[b.Op] = Edge{Dir: replyDir, Kind: "reply", Next: pr.thenTarget(b.Then, after)}
	}
}

// dir is send when this role is the sender of the message, receive otherwise.
func (pr *projector) dir(from string) string {
	if from == pr.role {
		return dirSend
	}
	return dirRecv
}

// thenTarget resolves a branch continuation to a state name. A goto becomes a sentinel ("@label")
// that resolveGotos rewrites once every rec body has been projected.
func (pr *projector) thenTarget(t Then, after string) string {
	switch {
	case t.End:
		return endState
	case t.Goto != "":
		return "@" + t.Goto
	default:
		return pr.seq(t.Flow, after)
	}
}

func (pr *projector) resolveGotos() error {
	for _, st := range pr.states {
		for op, e := range st.On {
			if len(e.Next) == 0 || e.Next[0] != '@' {
				continue
			}
			label := e.Next[1:]
			target, ok := pr.labels[label]
			if !ok {
				return fmt.Errorf("protocol %s: goto to undefined rec label %q", pr.proto.Protocol, label)
			}
			e.Next = target
			st.On[op] = e
		}
	}
	return nil
}
