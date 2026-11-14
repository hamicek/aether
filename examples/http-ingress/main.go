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
	routesPath := flag.String("routes", "routes.json", "path to the JSON host -> thrall route table (static provider)")
	useRegistry := flag.Bool("registry", false, "resolve routes dynamically via the site-registry thrall instead of the static -routes table")
	registrySubject := flag.String("registry-subject", ingress.DefaultRegistrySubject, "site-registry resolve call subject (with -registry)")
	registryScope := flag.String("registry-scope", "public", "this ingress's reach: public (only public sites) or vpn (public and vpn sites) (with -registry)")
	registryTTL := flag.Duration("registry-ttl", 5*time.Second, "how long a resolved route is cached before the registry is asked again (with -registry)")
	flag.Parse()

	var srv *http.Server
	def := thrall.EdgeDef{
		// Init builds the route provider and the server before Run serves it; a misconfigured provider
		// (a bad route file or an invalid scope) fails startup here (the lord restarts per policy)
		// rather than silently denying everything.
		Init: func(ctx *thrall.Ctx) error {
			routes, err := buildProvider(ctx, *useRegistry, *routesPath, *registrySubject, *registryScope, *registryTTL)
			if err != nil {
				return err
			}
			handler := ingress.NewHandler(ctx.NATS, routes, ingress.Options{App: ctx.App, Log: ctx.Log})
			srv = &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
			ctx.Log.Info("http ingress listening", "addr", *addr)
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

// buildProvider selects the ingress's RouteProvider: the dynamic site-registry client when -registry
// is set, otherwise the static routes.json table. Either way a misconfiguration (an invalid scope, a
// bad or missing route file) is returned as an error so Init fails startup rather than silently
// denying every request. The ingress's default-deny behaviour is identical for both providers - only
// the source of the table differs.
func buildProvider(ctx *thrall.Ctx, useRegistry bool, routesPath, subject, scope string, ttl time.Duration) (ingress.RouteProvider, error) {
	if useRegistry {
		ctx.Log.Info("route provider: site-registry", "subject", subject, "scope", scope, "ttl", ttl)
		return ingress.NewRegistryRoutes(ctx.NATS, ingress.RegistryOptions{
			Subject: subject,
			Scope:   scope,
			TTL:     ttl,
			Log:     ctx.Log,
		})
	}
	ctx.Log.Info("route provider: static routes", "routes", routesPath)
	return ingress.LoadStaticRoutes(routesPath)
}
