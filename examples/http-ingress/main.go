// Command http-ingress is the generic HTTP ingress for the "a thrall IS the web server" model
// (AE-091/092). It is an ordinary edge thrall (thrall.StartEdge) that owns an HTTP socket: it
// resolves each request's Host to a web thrall via a static route table, forwards the whole request
// as an http_request call over NATS, and writes the http_response back. It holds no state and does no
// path routing - that is the web thrall's job. Run it under `aether up` like any thrall.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/hamicek/aether/examples/http-ingress/ingress"
	"github.com/hamicek/aether/sdk/go/thrall"
)

func main() {
	addr := flag.String("addr", ":7391", "host:port to bind the ingress HTTP server")
	routesPath := flag.String("routes", "routes.json", "path to the JSON host -> thrall route table")
	flag.Parse()

	var srv *http.Server
	def := thrall.EdgeDef{
		// Init loads the route table and builds the server before Run serves it; a bad route file
		// fails startup here (the lord restarts per policy) rather than silently denying everything.
		Init: func(ctx *thrall.Ctx) error {
			routes, err := ingress.LoadStaticRoutes(*routesPath)
			if err != nil {
				return err
			}
			handler := ingress.NewHandler(ctx.NATS, routes, ingress.Options{App: ctx.App, Log: ctx.Log})
			srv = &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
			ctx.Log.Info("http ingress listening", "addr", *addr, "routes", *routesPath)
			return nil
		},
		// Run owns the socket until Stop (a drain) shuts the server down.
		Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},
		Stop: func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		},
		Version: "0.1.0",
	}
	if err := thrall.StartEdge(def); err != nil {
		log.Fatalf("ingress: %v", err)
	}
}
