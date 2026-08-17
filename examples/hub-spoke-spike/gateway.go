// Gateway thrall on the center (HUB) - proof of cross-node communication in the
// hub-spoke topology (AE-051 spike). It runs under the lord on the hub (app "demo")
// and on `check` calls counterA (site A) and counterB (site B) via a plain ctx.Call.
//
// The key finding this spike proves: ctx.Call works cross-node transparently,
// because the nodes share the app namespace ("demo") and differ only by thrall
// name. The SDK knows nothing about leaf nodes or accounts - routing and
// isolation are handled entirely by the NATS layer (import HUB<-SITE_A/SITE_B).
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

const callTimeout = 2 * time.Second

func main() {
	def := thrall.Def[struct{}]{
		Name: "gateway",
		Init: func(_ *thrall.Ctx) (struct{}, error) { return struct{}{}, nil },

		HandleCall: map[string]thrall.CallFn[struct{}]{
			// check reads the current state of both sites cross-node and returns it
			// as {"counterA": N, "counterB": M}. The spike script builds the
			// distribution assertion on it (the center sees the real state of both sites).
			"check": func(_ json.RawMessage, state struct{}, ctx *thrall.Ctx) (any, struct{}, error) {
				a, err := readCounter(ctx, "counterA")
				if err != nil {
					return nil, state, err
				}
				b, err := readCounter(ctx, "counterB")
				if err != nil {
					return nil, state, err
				}
				return map[string]int{"counterA": a, "counterB": b}, state, nil
			},
		},

		Terminate: func(reason string, _ struct{}) {
			fmt.Printf("gateway exiting (%s)\n", reason)
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

// readCounter calls `get` on the given site and unpacks the integer state.
func readCounter(ctx *thrall.Ctx, name string) (int, error) {
	reply, err := ctx.Call(name, "get", nil, callTimeout)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	var value int
	if err := json.Unmarshal(reply, &value); err != nil {
		return 0, fmt.Errorf("%s: unmarshal reply: %w", name, err)
	}
	return value, nil
}
