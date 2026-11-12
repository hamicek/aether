package main

// Automaton is the projection of a protocol onto one role (design.md §2): a data-driven state
// machine over ops. It is the single language-neutral artifact - the generator emits it as JSON,
// the well-formedness and duality checks run on it, and the Tier 2 monitor would later interpret
// the very same shape. A local type is exactly a state machine over operations, which is why the
// projection maps cleanly onto the FSM behaviour the runtime already has.
type Automaton struct {
	Protocol string                `json:"protocol"`
	Role     string                `json:"role"`
	Start    string                `json:"start"`
	States   map[string]*AutoState `json:"states"`
}

// AutoState is one phase of the conversation from the role's point of view. Final marks the end of
// the conversation (On is then empty). On maps each legal op in this phase to its edge.
type AutoState struct {
	Final bool            `json:"final,omitempty"`
	On    map[string]Edge `json:"on,omitempty"`
}

// Edge is a single legal transition: the direction (send/recv) and wire kind of the op, and the
// state the conversation moves to once that op passes.
type Edge struct {
	Dir  string `json:"dir"`  // "send" (!) | "recv" (?)
	Kind string `json:"kind"` // "call" | "cast" | "reply"
	Next string `json:"next"`
}

const (
	dirSend = "send"
	dirRecv = "recv"
)
