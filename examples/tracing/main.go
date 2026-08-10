// Command trace-demo is a two-thrall tracing example: an "api" thrall relays an incoming
// request to a "db" thrall via ctx.Cast, which propagates the correlation trace. Run with
// AETHER_LOG_LEVEL=debug to see both thralls log the same trace for one logical operation.
//
// One binary plays both roles, selected by AETHER_NAME (injected by the lord).
package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	switch os.Getenv("AETHER_NAME") {
	case "api":
		runAPI()
	case "db":
		runDB()
	default:
		log.Fatalf("unknown thrall %q", os.Getenv("AETHER_NAME"))
	}
}

// runAPI receives a "request" cast (the edge, from `aether cast`) and relays it to "db".
// ctx.Cast carries the trace of the incoming message, so the whole path shares one id.
func runAPI() {
	def := thrall.Def[int]{
		Init: func(*thrall.Ctx) (int, error) { return 0, nil },
		HandleCast: map[string]thrall.CastFn[int]{
			"request": func(payload json.RawMessage, s int, ctx *thrall.Ctx) (int, error) {
				ctx.Log.Info("api received request, relaying to db", "trace", ctx.Trace)
				return s, ctx.Cast("db", "store", payload)
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

// runDB is the downstream thrall; it logs the trace it received, which must match the one the
// api thrall logged for the same request.
func runDB() {
	def := thrall.Def[int]{
		Init: func(*thrall.Ctx) (int, error) { return 0, nil },
		HandleCast: map[string]thrall.CastFn[int]{
			"store": func(payload json.RawMessage, s int, ctx *thrall.Ctx) (int, error) {
				ctx.Log.Info("db stored value", "trace", ctx.Trace, "payload", string(payload))
				return s, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
