// A SCADA field device thrall (Go). It plays the "device" role of two session protocols: the
// control-command lifecycle (it receives command then execute) and the driver-control loop (it
// receives write and health). It declares a handler for every op its role receives.
//
// The conformance test checks those handlers against the committed automata, so the thrall and the
// protocol cannot drift on which ops exist - the same "generated contract, checked in CI" idea as
// the payload contract, one level up: there it is the message shape, here it is the conversation.
//
// This is the Tier 1 CI slice: the contract is checked, the transport is untouched, and nothing is
// enforced at runtime (that is the monitor, Tier 2). The reply handlers here just acknowledge; the
// ordering the protocol describes is a design-time guarantee, not something this thrall enforces.
package main

import (
	"encoding/json"
	"log"

	"github.com/hamicek/aether/sdk/go/thrall"
)

type deviceState struct {
	applied int // commands applied, for a plausible health report
}

// DeviceDef is the thrall definition, exported so the conformance test can read the ops it declares
// without starting a broker.
func DeviceDef() thrall.Def[deviceState] {
	return thrall.Def[deviceState]{
		Name:    "device",
		Version: "0.1.0",
		Init: func(ctx *thrall.Ctx) (deviceState, error) {
			return deviceState{}, nil
		},
		HandleCall: map[string]thrall.CallFn[deviceState]{
			// ControlCommand: the operator requests, then executes.
			"command": func(_ json.RawMessage, s deviceState, _ *thrall.Ctx) (any, deviceState, error) {
				return map[string]any{"authorized": true}, s, nil
			},
			"execute": func(_ json.RawMessage, s deviceState, _ *thrall.Ctx) (any, deviceState, error) {
				s.applied++
				return map[string]any{"confirmed": true}, s, nil
			},
			// DriverControl: the collector writes or checks health.
			"write": func(_ json.RawMessage, s deviceState, _ *thrall.Ctx) (any, deviceState, error) {
				s.applied++
				return map[string]any{"applied": true}, s, nil
			},
			"health": func(_ json.RawMessage, s deviceState, _ *thrall.Ctx) (any, deviceState, error) {
				return map[string]any{"applied": s.applied}, s, nil
			},
		},
	}
}

func main() {
	if err := thrall.Start(DeviceDef()); err != nil {
		log.Fatal(err)
	}
}
