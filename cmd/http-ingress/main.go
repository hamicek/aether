// Command http-ingress is the HTTP front door of the "a thrall IS the web server" model: it owns
// an HTTP socket, resolves each request's Host to a web thrall, forwards the whole request as one
// http_request call over NATS and writes the http_response back. It holds no state and does no
// path routing - that is the web thrall's job. It is an ordinary edge thrall (thrall.StartEdge):
// run it under `aether up` like any thrall, behind a reverse proxy that terminates TLS.
//
// The program itself lives in internal/ingress; see `http-ingress -h` for the flags and README.md
// ("HTTP ingress for web thralls") for the behaviour.
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
