// Command http-ingress in this directory is the OLD build path of the HTTP ingress, kept so that
// tooling which builds `./examples/http-ingress` keeps getting the same binary. The ingress is a
// component of aether now: the program is cmd/http-ingress, its core internal/ingress, and this
// file is a wrapper over the same Run - same flags, same behaviour. Build ./cmd/http-ingress in new
// setups; this wrapper goes away once nothing builds it.
package main

import (
	"log"
	"os"

	"github.com/hamicek/aether/internal/ingress"
)

func main() {
	if err := ingress.Run(os.Args[1:]); err != nil {
		log.Fatalf("http-ingress: %v", err)
	}
}
