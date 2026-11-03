// source produces raw measurements and casts them to the C# "transformer" thrall. The transformer
// has a durable mailbox, so the casts are captured in JetStream even if sent before it subscribes;
// the loop re-sends for a while so the demo is robust regardless of startup order.
package main

import (
	"log"
	"os"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	name := os.Getenv("AETHER_NAME")
	if name == "" {
		name = "source"
	}

	def := thrall.Def[int]{
		Name:    name,
		Version: "1.0.0",
		Init: func(ctx *thrall.Ctx) (int, error) {
			go func() {
				for i := 0; i < 20; i++ {
					for _, celsius := range []float64{100, 0, 37} {
						_ = ctx.Cast("transformer", "measure", map[string]float64{"celsius": celsius})
					}
					time.Sleep(time.Second)
				}
			}()
			return 0, nil
		},
	}

	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}
