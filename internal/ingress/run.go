package ingress

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

// Version is the ingress's self-declared build, reported to the lord in the heartbeat.
const Version = "0.2.0"

// shutdownGrace bounds how long a drain waits for in-flight requests before the server is closed.
const shutdownGrace = 3 * time.Second

// Config is everything the ingress program takes from its command line. ParseFlags fills it; Run
// turns it into a route provider, a handler and a server.
type Config struct {
	Addr            string
	RoutesPath      string
	UseRegistry     bool
	RegistrySubject string
	RegistryScope   string
	RegistryTTL     time.Duration
}

// ParseFlags reads the ingress command line. Usage and parse errors are written to out; the
// returned error is flag.ErrHelp when the caller asked for help, so a program can tell "printed
// usage" from "bad command line".
func ParseFlags(args []string, out io.Writer) (Config, error) {
	var cfg Config
	fs := flag.NewFlagSet("http-ingress", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.Addr, "addr", ":7391", "host:port to bind the ingress HTTP server")
	fs.StringVar(&cfg.RoutesPath, "routes", "routes.json", "path to the JSON host -> thrall route table (static provider)")
	fs.BoolVar(&cfg.UseRegistry, "registry", false, "resolve routes dynamically via the site-registry thrall instead of the static -routes table")
	fs.StringVar(&cfg.RegistrySubject, "registry-subject", DefaultRegistrySubject, "site-registry resolve call subject (with -registry)")
	fs.StringVar(&cfg.RegistryScope, "registry-scope", "public", "this ingress's reach: public (only public sites) or vpn (public and vpn sites) (with -registry)")
	fs.DurationVar(&cfg.RegistryTTL, "registry-ttl", 5*time.Second, "how long a resolved route is cached before the registry is asked again (with -registry)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Run is the whole ingress program: parse the command line, then run as an edge thrall
// (thrall.StartEdge) that owns the HTTP socket until the lord drains it. Both entry points - the
// cmd/http-ingress binary and the examples/http-ingress wrapper kept for the old build path - call
// it, so they cannot drift apart. It returns nil after a clean stop or after printing usage.
func Run(args []string) error {
	cfg, err := ParseFlags(args, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	var srv *http.Server
	return thrall.StartEdge(thrall.EdgeDef{
		// Init builds the route provider and the server before Run serves it; a misconfigured provider
		// (a bad route file or an invalid scope) fails startup here (the lord restarts per policy)
		// rather than silently denying everything.
		Init: func(ctx *thrall.Ctx) error {
			routes, err := buildProvider(ctx, cfg)
			if err != nil {
				return err
			}
			handler := NewHandler(ctx.NATS, routes, Options{App: ctx.App, Log: ctx.Log})
			srv = &http.Server{Addr: cfg.Addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
			ctx.Log.Info("http ingress listening", "addr", cfg.Addr)
			return nil
		},
		// Run owns the socket until Stop (a drain) shuts the server down.
		Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
		Stop: func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		},
		Version: Version,
	})
}

// buildProvider selects the ingress's RouteProvider: the dynamic site-registry client when -registry
// is set, otherwise the static routes.json table. Either way a misconfiguration (an invalid scope, a
// bad or missing route file) is returned as an error so Init fails startup rather than silently
// denying every request. The ingress's default-deny behaviour is identical for both providers - only
// the source of the table differs.
func buildProvider(ctx *thrall.Ctx, cfg Config) (RouteProvider, error) {
	if cfg.UseRegistry {
		ctx.Log.Info("route provider: site-registry", "subject", cfg.RegistrySubject, "scope", cfg.RegistryScope, "ttl", cfg.RegistryTTL)
		return NewRegistryRoutes(ctx.NATS, RegistryOptions{
			Subject: cfg.RegistrySubject,
			Scope:   cfg.RegistryScope,
			TTL:     cfg.RegistryTTL,
			Log:     ctx.Log,
		})
	}
	ctx.Log.Info("route provider: static routes", "routes", cfg.RoutesPath)
	return LoadStaticRoutes(cfg.RoutesPath)
}
