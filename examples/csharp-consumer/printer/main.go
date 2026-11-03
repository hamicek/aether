// printer is the final hop: it consumes the transformed readings the C# thrall casts to it and
// prints them. Its durable mailbox makes the C# -> Go cast land reliably regardless of timing.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hamicek/aether/sdk/go/thrall"
)

type reading struct {
	Celsius    float64 `json:"celsius"`
	Fahrenheit float64 `json:"fahrenheit"`
}

func main() {
	name := os.Getenv("AETHER_NAME")
	if name == "" {
		name = "printer"
	}

	def := thrall.Def[int]{
		Name:    name,
		Version: "1.0.0",
		Init:    func(_ *thrall.Ctx) (int, error) { return 0, nil },

		HandleCast: map[string]thrall.CastFn[int]{
			"reading": func(payload json.RawMessage, state int, _ *thrall.Ctx) (int, error) {
				var r reading
				if err := json.Unmarshal(payload, &r); err != nil {
					return state, err
				}
				fmt.Printf("reading: %.1fC = %.1fF\n", r.Celsius, r.Fahrenheit)
				return state + 1, nil
			},
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
