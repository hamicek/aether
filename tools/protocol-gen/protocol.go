// Command protocol-gen turns a binary session-type protocol (*.protocol.yaml) into a
// language-neutral per-role automaton and checks it: well-formedness, duality of the two
// role projections, and a name-existence conformance lint against a thrall's ThrallDescribe.
//
// This is the Tier 1 "CI slice" of AE-090 (see the bundle design.md, §14/§15): a design-time
// contract, not a runtime monitor. It enforces nothing on the live wire; it only proves that
// a protocol is well-formed and that two roles agree, and that a thrall declares the ops its
// role needs. Ordering and direction are only enforced at runtime by the monitor (Tier 2),
// which this slice deliberately omits.
package main

import (
	"encoding/json"
	"fmt"
)

// Protocol is the source-of-truth description of a binary (two-party) conversation. It composes
// with the payload contract: every message op references an existing payload schema by name, so
// the protocol is the grammar that orders payloads in time (design.md §3.1).
type Protocol struct {
	// Protocol is the type name (e.g. "ControlCommand"); it names the generated automaton files.
	Protocol string `json:"protocol"`
	// Roles are exactly the two parties. Binary only - multiparty is out of scope (design.md §8/§15).
	Roles []string `json:"roles"`
	// Start names the role that speaks first.
	Start string `json:"start"`
	// Flow is the top-level sequence of steps.
	Flow []Step `json:"flow"`
}

// Step is one node of the protocol tree. It is exactly one of two shapes:
//   - a message: From/To/Kind/Op set, optionally carrying a Reply (only when Kind == "call");
//   - a recursion point: Rec names a loop label and Body is its sub-sequence.
type Step struct {
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Kind    string `json:"kind,omitempty"` // "call" (demands a reply) | "cast" (fire-and-forget)
	Op      string `json:"op,omitempty"`
	Payload string `json:"payload,omitempty"`
	Reply   *Reply `json:"reply,omitempty"`

	Rec  string `json:"rec,omitempty"`  // names a recursion point a later `goto` can re-enter
	Body []Step `json:"body,omitempty"` // the rec point's sub-sequence

	// Choose is an internal choice (design.md §2, the +{} form): the active role picks one of
	// several messages to send. Each option is itself a message step (a call carrying its reply),
	// and all options share the same From role - the one making the choice. It is the dual of a
	// reply's external choice (&{}, where the callee picks the branch).
	Choose []Step `json:"choose,omitempty"`
}

// isMessage reports whether the step is a message (as opposed to a rec or choose block).
func (s Step) isMessage() bool { return s.Op != "" }

// isRec reports whether the step is a recursion point.
func (s Step) isRec() bool { return s.Rec != "" }

// isChoose reports whether the step is an internal choice.
func (s Step) isChoose() bool { return len(s.Choose) > 0 }

// Reply is the set of branches the callee may answer with. Exactly one branch is taken, so a
// reply is a choice: the caller offers the branches, the callee selects one (design.md §2/§4.1).
type Reply struct {
	Choice []Branch `json:"choice"`
}

// Branch is one option of a reply choice: an op (with its payload) and what the conversation does
// afterwards. Then is required so every branch has an explicit continuation.
type Branch struct {
	Op      string `json:"op"`
	Payload string `json:"payload,omitempty"`
	Then    Then   `json:"then"`
}

// Then is a branch continuation. It is exactly one of three forms, mirrored from the YAML:
//
//	then: end             -> End == true          (the conversation finishes)
//	then: { goto: Loop }  -> Goto == "Loop"       (jump back to a rec point)
//	then: { flow: [ ... ] } -> Flow == [ ... ]    (continue with a nested sub-sequence)
type Then struct {
	End  bool
	Goto string
	Flow []Step
}

// UnmarshalJSON accepts the bare string "end" or an object with a "goto" or "flow" key. sigs.k8s.io/yaml
// converts the YAML to JSON first, so this one decoder covers both the string and object spellings.
func (t *Then) UnmarshalJSON(data []byte) error {
	// Form 1: the bare string "end".
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s != "end" {
			return fmt.Errorf("then: unknown keyword %q (expected \"end\")", s)
		}
		*t = Then{End: true}
		return nil
	}
	// Form 2/3: an object carrying exactly one of goto/flow.
	var obj struct {
		Goto string `json:"goto"`
		Flow []Step `json:"flow"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("then: expected \"end\", {goto: LABEL} or {flow: [...]}")
	}
	switch {
	case obj.Goto != "" && obj.Flow != nil:
		return fmt.Errorf("then: set only one of goto/flow")
	case obj.Goto != "":
		*t = Then{Goto: obj.Goto}
	case obj.Flow != nil:
		*t = Then{Flow: obj.Flow}
	default:
		return fmt.Errorf("then: object needs a goto or flow key")
	}
	return nil
}
