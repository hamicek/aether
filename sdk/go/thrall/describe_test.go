package thrall

import (
	"reflect"
	"testing"
)

// TestOpNames proves the handler-map keys become a sorted op list, and an empty map yields nil so
// the describe field is omitted on the wire.
func TestOpNames(t *testing.T) {
	call := map[string]CallFn[int]{"value": nil, "get": nil}
	if got := opNames(call); !reflect.DeepEqual(got, []string{"get", "value"}) {
		t.Errorf("call ops = %v, want [get value] (sorted)", got)
	}
	cast := map[string]CastFn[int]{"reset": nil, "inc": nil}
	if got := opNames(cast); !reflect.DeepEqual(got, []string{"inc", "reset"}) {
		t.Errorf("cast ops = %v, want [inc reset] (sorted)", got)
	}
	if got := opNames(map[string]CastFn[int]{}); got != nil {
		t.Errorf("empty map ops = %v, want nil (omitted on the wire)", got)
	}
}

// TestFSMDescribe proves an FSM reports the union of its states' reaction ops - each dispatchable as
// a call or a cast - plus the reserved _state call op, all sorted, and passes the version through.
func TestFSMDescribe(t *testing.T) {
	def := FSM[int]{
		Version: "2.0.0",
		States: map[string]State[int]{
			"idle":    {On: map[string]Reaction[int]{"start": {}}},
			"running": {On: map[string]Reaction[int]{"stop": {}, "start": {}}}, // start repeats across states
		},
	}
	d := def.describe()
	if !reflect.DeepEqual(d.CallOps, []string{"_state", "start", "stop"}) {
		t.Errorf("call ops = %v, want [_state start stop]", d.CallOps)
	}
	if !reflect.DeepEqual(d.CastOps, []string{"start", "stop"}) {
		t.Errorf("cast ops = %v, want [start stop] (no reserved _state on the cast side)", d.CastOps)
	}
	if d.Version != "2.0.0" {
		t.Errorf("version = %q, want 2.0.0", d.Version)
	}
}
