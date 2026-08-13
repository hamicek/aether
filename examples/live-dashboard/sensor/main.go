// A site sensor thrall: a plain GenServer that, in init, starts a ticker publishing a domain event
// (a temperature reading) to its own event subject on the ether. The live edge subscribes to these
// per-site subjects and pushes them to browsers. Two instances run under names site-1 and site-2, so
// each publishes to a distinct subject (aether.<app>.site-N.evt) - the basis for per-client scope.
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	name := os.Getenv("AETHER_NAME")
	if name == "" {
		name = "site-1"
	}
	def := thrall.Def[int]{
		Name: name,
		Init: func(ctx *thrall.Ctx) (int, error) {
			go publishReadings(ctx)
			return 0, nil
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

// publishReadings emits a reading roughly once a second on this site's event subject. It runs for the
// life of the process (the lord's drain ends the process, which ends this goroutine).
func publishReadings(ctx *thrall.Ctx) {
	subject := wire.EventLog(ctx.App, ctx.Name) // aether.<app>.<name>.evt
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for seq := 1; ; seq++ {
		<-t.C
		temp := 18 + seq%6 // a deterministic wobble between 18 and 23 °C
		payload := fmt.Sprintf(`{"site":%q,"temp":%d,"seq":%d}`, ctx.Name, temp, seq)
		if err := ctx.NATS.Publish(subject, []byte(payload)); err != nil {
			ctx.Log.Warn("publish reading", "err", err)
		}
	}
}
