// Command turnstile is a domain-neutral demo of the FSM thrall behaviour (StartFSM). It is a
// classic two-state turnstile: `locked` accepts a coin to become `unlocked`, `unlocked` accepts
// a push to lock again (counting pushes), and auto-locks after 5s idle via a state timeout.
//
// Deliberately generic - not an alarm automaton. Events are ordinary casts/calls:
//
//	aether cast turnstile coin        # -> unlocked
//	aether call turnstile push        # -> locked, replies with the push count
//	aether call turnstile _state      # -> {"state":"locked"} (reserved introspection op)
package main

import (
	"log"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	fsm := thrall.FSM[int]{
		Name:    "turnstile",
		Initial: "locked",
		Init:    func(*thrall.Ctx) (int, error) { return 0, nil },
		States: map[string]thrall.State[int]{
			"locked": {On: map[string]thrall.Reaction[int]{
				"coin": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
					ctx.Log.Info("coin accepted, unlocking")
					return thrall.Outcome[int]{Next: "unlocked", Data: pushes}, nil
				}},
			}},
			"unlocked": {
				On: map[string]thrall.Reaction[int]{
					"push": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
						ctx.Log.Info("push, locking", "total_pushes", pushes+1)
						return thrall.Outcome[int]{Next: "locked", Data: pushes + 1, Reply: pushes + 1}, nil
					}},
					"autolock": {Fn: func(_ thrall.Event, pushes int, ctx *thrall.Ctx) (thrall.Outcome[int], error) {
						ctx.Log.Info("idle timeout, auto-locking")
						return thrall.Outcome[int]{Next: "locked", Data: pushes}, nil
					}},
				},
				// If nobody pushes within 5s of unlocking, fire "autolock" back to locked.
				Timeout: &thrall.StateTimeout[int]{After: 5 * time.Second, Op: "autolock"},
			},
		},
	}
	if err := thrall.StartFSM(fsm); err != nil {
		log.Fatal(err)
	}
}
