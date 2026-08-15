// Driver = a Go producer thrall. It is built against the same measurement.schema.json the BFF
// validates with: it constructs typed Measurement values and casts them to the BFF. To show
// the boundary at work it also sends one deliberately malformed payload, which the BFF rejects.
//
// The producer is trusted (its values are already typed), so it does not validate before
// sending - validation belongs at the consumer's boundary. See PAYLOAD-CONTRACT.md (AE-042).
package main

import (
	"log"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

// Hand-written for the PoC; the codegen follow-up (AE-042 §8, bundle 2) generates this.
type Measurement struct {
	SiteID string  `json:"siteId"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Unit   string  `json:"unit,omitempty"`
	TS     int64   `json:"ts"`
}

func main() {
	def := thrall.Def[int]{
		Name: "driver",
		Init: func(ctx *thrall.Ctx) (int, error) {
			go produce(ctx)
			return 0, nil
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

func produce(ctx *thrall.Ctx) {
	// Casts are ephemeral over core NATS, so wait until the BFF is subscribed (answers a call).
	for i := 0; i < 100; i++ {
		if _, err := ctx.Call("bff", "accepted", struct{}{}, 500*time.Millisecond); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	valid := []Measurement{
		{SiteID: "s-1", Metric: "voltage", Value: 231.4, Unit: "V", TS: time.Now().UnixMilli()},
		{SiteID: "s-2", Metric: "current", Value: 12.5, Unit: "A", TS: time.Now().UnixMilli()},
	}
	for _, m := range valid {
		if err := ctx.Cast("bff", "ingest", m); err != nil {
			log.Printf("driver: cast: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// A malformed measurement (metric out of the schema enum): the BFF boundary rejects it.
	malformed := map[string]any{"siteId": "s-3", "metric": "pressure", "value": 9.9, "ts": time.Now().UnixMilli()}
	if err := ctx.Cast("bff", "ingest", malformed); err != nil {
		log.Printf("driver: cast: %v", err)
	}
}
