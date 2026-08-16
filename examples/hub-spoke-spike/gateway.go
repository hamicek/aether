// Gateway thrall na centrale (HUB) - dukaz cross-node komunikace v hub-spoke
// topologii (AE-051 spike). Bezi pod lordem na hubu (app "demo") a na `check`
// zavola counterA (sajta A) a counterB (sajta B) pres bezny ctx.Call.
//
// Klicove zjisteni, ktere tim spike dokazuje: ctx.Call funguje cross-node
// transparentne, protoze uzly sdili app namespace ("demo") a lisi se jen
// jmenem thralla. SDK o leaf nodes ani accountech nic nevi - smerovani a
// izolaci resi vyhradne NATS vrstva (import HUB<-SITE_A/SITE_B).
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
			// check precte aktualni stav obou sajt cross-node a vrati ho jako
			// {"counterA": N, "counterB": M}. Skript spiku na tom stavi tvrzeni
			// o distribuci (centrala vidi realny stav obou sajt).
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

// readCounter zavola `get` na dane sajte a rozbali celociselny stav.
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
