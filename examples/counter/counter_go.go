// Counter thrall in Go - functionally identical to counter.ts and counter_py.py.
// Under the same lord, on the same ether, with the same JSON contract.
//
// Name from env (AETHER_NAME) -> the same code runs under whatever name the manifest gives it
// (counter-go, counter-single, ...), falling back to counter-go when run without a lord.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	name := os.Getenv("AETHER_NAME")
	if name == "" {
		name = "counter-go"
	}
	def := thrall.Def[int]{
		Name: name,
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
			fmt.Printf("%s exiting (%s), last = %d\n", name, reason, state)
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
