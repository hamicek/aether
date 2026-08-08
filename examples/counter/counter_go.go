// Counter thrall in Go - functionally identical to counter.ts and counter_py.py.
// Under the same lord, on the same ether, with the same JSON contract.
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	def := thrall.Def[int]{
		Name: "counter-go",
		Init: func(_ *thrall.Ctx) (int, error) { return 0, nil },

		HandleCall: map[string]thrall.CallFn[int]{
			"get": func(_ json.RawMessage, state int, _ *thrall.Ctx) (any, int, error) {
				return state, state, nil // (reply, new_state, err)
			},
		},

		HandleCast: map[string]thrall.CastFn[int]{
			"inc": func(_ json.RawMessage, state int, _ *thrall.Ctx) (int, error) { return state + 1, nil },
			"dec": func(_ json.RawMessage, state int, _ *thrall.Ctx) (int, error) { return state - 1, nil },
		},

		Terminate: func(reason string, state int) {
			fmt.Printf("counter-go exiting (%s), last = %d\n", reason, state)
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
