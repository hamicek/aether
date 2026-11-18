package ingress

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	RegistryStale   time.Duration
	RegistryCache   int

	Timeout           time.Duration
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxConns          int
	MaxHeaderBytes    int
	MaxBodyBytes      int64

	TrustedProxies TrustedProxies
}

// ParseFlags reads the ingress command line. The flag package writes usage (and a malformed flag) to
// out; the returned error is flag.ErrHelp when the caller asked for help, so a program can tell
// "printed usage" from "bad command line". A limit that would switch a protection off (a zero timeout, no
// connection cap) is rejected: every limit has a safe default and can be tuned, not removed.
func ParseFlags(args []string, out io.Writer) (Config, error) {
	var cfg Config
	fs := flag.NewFlagSet("http-ingress", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&cfg.Addr, "addr", ":7391", "host:port to bind the ingress HTTP server")
	fs.StringVar(&cfg.RoutesPath, "routes", "routes.json", "path to the JSON host -> thrall route table (static provider)")
	fs.BoolVar(&cfg.UseRegistry, "registry", false, "resolve routes dynamically via the site-registry thrall instead of the static -routes table")
	fs.StringVar(&cfg.RegistrySubject, "registry-subject", DefaultRegistrySubject, "site-registry resolve call subject (with -registry)")
	fs.StringVar(&cfg.RegistryScope, "registry-scope", "public", "this ingress's reach: public (only public sites) or vpn (public and vpn sites) (with -registry)")
	fs.DurationVar(&cfg.RegistryTTL, "registry-ttl", DefaultRegistryTTL, "how long a resolved route is cached before the registry is asked again (with -registry)")
	fs.DurationVar(&cfg.RegistryStale, "registry-stale", DefaultRegistryStale, "how long past its TTL a route is still served while the registry is unreachable; 0 = not at all (with -registry)")
	fs.IntVar(&cfg.RegistryCache, "registry-cache-size", DefaultRegistryCacheSize, "most routable hosts and, separately, most denied hosts kept in the route cache (with -registry)")

	fs.DurationVar(&cfg.Timeout, "timeout", DefaultTimeout, "how long to wait for a web thrall's reply before answering 504; also the deadline the call carries")
	fs.DurationVar(&cfg.ReadHeaderTimeout, "read-header-timeout", DefaultReadHeaderTimeout, "how long a client may take to send the request line and headers")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", DefaultReadTimeout, "how long a client may take to send the whole request, body included (then 408)")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", DefaultWriteTimeout, "how long a client may take to read the response once the ingress has it")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", DefaultIdleTimeout, "how long a keep-alive connection may sit idle between requests")
	fs.IntVar(&cfg.MaxConns, "max-conns", DefaultMaxConns, "connections served at once; further ones wait to be accepted")
	fs.IntVar(&cfg.MaxHeaderBytes, "max-header-bytes", DefaultMaxHeaderBytes, "largest request line plus headers accepted (then 431)")
	fs.Int64Var(&cfg.MaxBodyBytes, "max-body-bytes", 0, "largest request body accepted (then 413); 0 = as much as one bus message carries, a higher value is lowered to that")
	trustedProxies := fs.String("trusted-proxies", DefaultTrustedProxies, "comma-separated IPs/CIDRs of the reverse proxies whose X-Forwarded-For and X-Forwarded-Proto are believed; empty = trust none")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	trusted, err := ParseTrustedProxies(*trustedProxies)
	if err != nil {
		return Config{}, err
	}
	cfg.TrustedProxies = trusted
	return cfg, nil
}

// validate rejects a command line that would run the ingress without one of its limits.
func (c Config) validate() error {
	for name, d := range map[string]time.Duration{
		"-timeout": c.Timeout, "-read-header-timeout": c.ReadHeaderTimeout, "-read-timeout": c.ReadTimeout,
		"-write-timeout": c.WriteTimeout, "-idle-timeout": c.IdleTimeout,
	} {
		if d <= 0 {
			return fmt.Errorf("ingress: %s must be positive, got %s", name, d)
		}
	}
	if c.MaxConns < 1 {
		return fmt.Errorf("ingress: -max-conns must be at least 1, got %d", c.MaxConns)
	}
	if c.MaxHeaderBytes < 1 {
		return fmt.Errorf("ingress: -max-header-bytes must be positive, got %d", c.MaxHeaderBytes)
	}
	if c.MaxBodyBytes < 0 {
		return fmt.Errorf("ingress: -max-body-bytes must not be negative, got %d", c.MaxBodyBytes)
	}
	if c.RegistryTTL <= 0 {
		return fmt.Errorf("ingress: -registry-ttl must be positive, got %s", c.RegistryTTL)
	}
	if c.RegistryStale < 0 {
		return fmt.Errorf("ingress: -registry-stale must not be negative, got %s", c.RegistryStale)
	}
	if c.RegistryCache < 1 {
		return fmt.Errorf("ingress: -registry-cache-size must be at least 1, got %d", c.RegistryCache)
	}
	return nil
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

	var (
		srv *http.Server
		ln  net.Listener
	)
	return thrall.StartEdge(thrall.EdgeDef{
		// Init builds the route provider, derives the limits from the live connection and binds the
		// socket before Run serves it. Anything wrong - a bad route file, an invalid scope, a bus too
		// small to carry a request, an address already in use - fails startup here (the lord restarts
		// per policy) rather than leaving an ingress that silently denies everything.
		Init: func(ctx *thrall.Ctx) error {
			routes, err := buildProvider(ctx, cfg)
			if err != nil {
				return err
			}
			maxBody, err := bodyLimitFor(ctx.NATS.MaxPayload(), cfg, ctx.Log)
			if err != nil {
				return err
			}
			handler := NewHandler(ctx.NATS, routes, Options{
				App: ctx.App, Timeout: cfg.Timeout, MaxBodyBytes: maxBody, WriteTimeout: cfg.WriteTimeout,
				TrustedProxies: cfg.TrustedProxies, Log: ctx.Log,
			})
			srv = NewServer(handler, ServerOptions{
				ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout, IdleTimeout: cfg.IdleTimeout,
				MaxHeaderBytes: cfg.MaxHeaderBytes, Log: ctx.Log,
			})
			if ln, err = Listen(cfg.Addr, cfg.MaxConns); err != nil {
				return err
			}
			ctx.Log.Info("http ingress listening", "addr", cfg.Addr, "version", Version,
				"timeout", cfg.Timeout, "max_body_bytes", maxBody, "max_conns", cfg.MaxConns,
				"read_timeout", cfg.ReadTimeout, "write_timeout", cfg.WriteTimeout, "idle_timeout", cfg.IdleTimeout,
				"trusted_proxies", fmt.Sprint(cfg.TrustedProxies))
			return nil
		},
		// Run owns the socket. It ends when the lord drains the ingress (stop is closed): requests in
		// flight get a moment to finish, and what is still open after that is cut. A server that
		// stops by itself is a failure, and the lord restarts the ingress.
		Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
			served := make(chan error, 1)
			go func() { served <- srv.Serve(ln) }()
			select {
			case err := <-served:
				return fmt.Errorf("ingress: http server stopped: %w", err)
			case <-stop:
				shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
				defer cancel()
				if err := srv.Shutdown(shutdownCtx); err != nil {
					_ = srv.Close()
				}
				return nil
			}
		},
		Version: Version,
	})
}

// bodyLimitFor settles the request body limit for a bus whose messages carry at most maxPayload
// bytes. What the bus carries is a hard ceiling: -max-body-bytes can only lower it, and a higher
// value is brought down to it with a warning rather than accepted and then failed per request.
func bodyLimitFor(maxPayload int64, cfg Config, log *slog.Logger) (int64, error) {
	carried := BodyLimit(maxPayload, cfg.MaxHeaderBytes)
	if carried <= 0 {
		return 0, fmt.Errorf("ingress: the bus carries messages of at most %d bytes, which leaves no room for a request body "+
			"beside %d bytes of headers; raise the server's max_payload or lower -max-header-bytes", maxPayload, cfg.MaxHeaderBytes)
	}
	if cfg.MaxBodyBytes == 0 {
		return carried, nil
	}
	if cfg.MaxBodyBytes > carried {
		log.Warn("ingress: -max-body-bytes is more than one bus message carries, using the lower limit",
			"configured", cfg.MaxBodyBytes, "limit", carried, "bus_max_payload", maxPayload)
		return carried, nil
	}
	return cfg.MaxBodyBytes, nil
}

// buildProvider selects the ingress's RouteProvider: the dynamic site-registry client when -registry
// is set, otherwise the static routes.json table. Either way a misconfiguration (an invalid scope, a
// bad or missing route file) is returned as an error so Init fails startup rather than silently
// denying every request. The ingress's default-deny behaviour is identical for both providers - only
// the source of the table differs.
func buildProvider(ctx *thrall.Ctx, cfg Config) (RouteProvider, error) {
	if cfg.UseRegistry {
		ctx.Log.Info("route provider: site-registry", "subject", cfg.RegistrySubject, "scope", cfg.RegistryScope,
			"ttl", cfg.RegistryTTL, "stale", cfg.RegistryStale, "cache_size", cfg.RegistryCache)
		return NewRegistryRoutes(ctx.NATS, RegistryOptions{
			Subject:   cfg.RegistrySubject,
			Scope:     cfg.RegistryScope,
			TTL:       cfg.RegistryTTL,
			StaleFor:  cfg.RegistryStale,
			CacheSize: cfg.RegistryCache,
			Log:       ctx.Log,
		})
	}
	ctx.Log.Info("route provider: static routes", "routes", cfg.RoutesPath)
	return LoadStaticRoutes(cfg.RoutesPath)
}
