package main

import (
	"fmt"

	"sigs.k8s.io/yaml"
)

// Parse decodes a *.protocol.yaml document and checks its local structure. It does not resolve
// goto targets or reachability (that is the projection's job, see project.go) - it only catches
// mistakes visible in a single step: unknown roles, a call without a reply, an empty choice, a
// step that is neither a message nor a rec block. sigs.k8s.io/yaml routes the YAML through JSON,
// so the json struct tags and the custom Then decoder apply uniformly.
func Parse(data []byte) (*Protocol, error) {
	var p Protocol
	if err := yaml.UnmarshalStrict(data, &p); err != nil {
		return nil, fmt.Errorf("decode protocol: %w", err)
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Protocol) validate() error {
	if p.Protocol == "" {
		return fmt.Errorf("protocol: name is required")
	}
	if len(p.Roles) != 2 {
		return fmt.Errorf("protocol %s: exactly two roles are required (binary session), got %d", p.Protocol, len(p.Roles))
	}
	if p.Roles[0] == "" || p.Roles[1] == "" {
		return fmt.Errorf("protocol %s: role names must be non-empty", p.Protocol)
	}
	if p.Roles[0] == p.Roles[1] {
		return fmt.Errorf("protocol %s: the two roles must be distinct", p.Protocol)
	}
	if !p.hasRole(p.Start) {
		return fmt.Errorf("protocol %s: start role %q is not one of the declared roles", p.Protocol, p.Start)
	}
	if len(p.Flow) == 0 {
		return fmt.Errorf("protocol %s: flow is empty", p.Protocol)
	}
	return p.validateSteps(p.Flow)
}

func (p *Protocol) hasRole(r string) bool {
	return r == p.Roles[0] || r == p.Roles[1]
}

func (p *Protocol) validateSteps(steps []Step) error {
	for i, s := range steps {
		shapes := 0
		for _, is := range []bool{s.isMessage(), s.isRec(), s.isChoose()} {
			if is {
				shapes++
			}
		}
		if shapes > 1 {
			return fmt.Errorf("protocol %s: step %d mixes message/rec/choose - a step is exactly one shape", p.Protocol, i)
		}
		switch {
		case s.isMessage():
			if err := p.validateMessage(s); err != nil {
				return err
			}
		case s.isRec():
			if len(s.Body) == 0 {
				return fmt.Errorf("protocol %s: rec %q has an empty body", p.Protocol, s.Rec)
			}
			if err := p.validateSteps(s.Body); err != nil {
				return err
			}
		case s.isChoose():
			if err := p.validateChoose(s.Choose); err != nil {
				return err
			}
		default:
			return fmt.Errorf("protocol %s: step %d is neither a message, a rec block nor a choose", p.Protocol, i)
		}
	}
	return nil
}

// validateChoose checks an internal choice: every option is a message, all options are sent by the
// same role (the one choosing), and their ops are distinct.
func (p *Protocol) validateChoose(options []Step) error {
	var by string
	seen := make(map[string]struct{}, len(options))
	for _, o := range options {
		if !o.isMessage() {
			return fmt.Errorf("protocol %s: a choose option must be a message, got a rec/choose block", p.Protocol)
		}
		if by == "" {
			by = o.From
		} else if o.From != by {
			return fmt.Errorf("protocol %s: choose options must all be sent by one role (%q vs %q)", p.Protocol, by, o.From)
		}
		if _, dup := seen[o.Op]; dup {
			return fmt.Errorf("protocol %s: choose repeats option op %q", p.Protocol, o.Op)
		}
		seen[o.Op] = struct{}{}
		if err := p.validateMessage(o); err != nil {
			return err
		}
	}
	return nil
}

func (p *Protocol) validateMessage(s Step) error {
	if !p.hasRole(s.From) {
		return fmt.Errorf("protocol %s: op %q has unknown from-role %q", p.Protocol, s.Op, s.From)
	}
	if !p.hasRole(s.To) {
		return fmt.Errorf("protocol %s: op %q has unknown to-role %q", p.Protocol, s.Op, s.To)
	}
	if s.From == s.To {
		return fmt.Errorf("protocol %s: op %q sends from a role to itself", p.Protocol, s.Op)
	}
	switch s.Kind {
	case "call":
		if s.Reply == nil || len(s.Reply.Choice) == 0 {
			return fmt.Errorf("protocol %s: call %q has no reply (a call demands a mandatory reply)", p.Protocol, s.Op)
		}
		return p.validateChoice(s.Op, s.Reply.Choice)
	case "cast":
		if s.Reply != nil {
			return fmt.Errorf("protocol %s: cast %q carries a reply (only a call has one)", p.Protocol, s.Op)
		}
	default:
		return fmt.Errorf("protocol %s: op %q has unknown kind %q (expected call|cast)", p.Protocol, s.Op, s.Kind)
	}
	return nil
}

func (p *Protocol) validateChoice(callOp string, branches []Branch) error {
	seen := make(map[string]struct{}, len(branches))
	for _, b := range branches {
		if b.Op == "" {
			return fmt.Errorf("protocol %s: reply to %q has a branch with no op", p.Protocol, callOp)
		}
		if _, dup := seen[b.Op]; dup {
			return fmt.Errorf("protocol %s: reply to %q repeats branch op %q", p.Protocol, callOp, b.Op)
		}
		seen[b.Op] = struct{}{}
		if b.Then.Flow != nil {
			if err := p.validateSteps(b.Then.Flow); err != nil {
				return err
			}
		}
	}
	return nil
}
