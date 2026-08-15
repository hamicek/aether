package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/edge"
	"github.com/hamicek/aether/internal/fencing"
	"github.com/hamicek/aether/internal/lordlease"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/singleton"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/schema"
)

// edgeCallTimeout bounds how long a synchronous (call) route waits for the target thrall's reply
// before returning 504. Web requests tolerate more latency than the CLI's one-shot call, so this
// is deliberately more generous than `aether call`'s 2s default.
const edgeCallTimeout = 5 * time.Second

// edgeCmd runs a built-in HTTP ingress server (the `_edge` subcommand). It is spawned by the lord
// as an ordinary child: the edge spec arrives as JSON in AETHER_EDGE_SPEC and the bus endpoint and
// credentials via the same AETHER_* env the lord injects into every thrall. It bridges HTTP to the
// ether (call/cast), heartbeats to the lord, and drains on the lord's ctl signal.
func edgeCmd(argv []string) {
	logger := obs.NewLogger().With(slog.String("component", "edge"))

	spec, err := edge.SpecFromEnv()
	if err != nil {
		log.Fatalf("edge: %v", err)
	}
	logger = logger.With(slog.String("edge", spec.Name))

	ep := resolveEndpoint("", "", "", "")
	nc := connect(ep)
	defer nc.Close()

	// Bind the port synchronously so a bad address fails loudly (the lord sees the crash), mirroring
	// the lord's own metrics server.
	ln, err := net.Listen("tcp", spec.Addr)
	if err != nil {
		log.Fatalf("edge %q: listen %s: %v", spec.Name, spec.Addr, err)
	}

	router := edge.NewRouter(spec)
	srv := &http.Server{
		Handler:           edgeHandler(nc, ep.App, router, edgeCallTimeout, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// WriteTimeout must exceed edgeCallTimeout, so a legitimately slow thrall reply is not cut off.
		WriteTimeout: edgeCallTimeout + 10*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// The edge binds an OS port, so the lord runs it as a singleton and injects the fencing tokens.
	// Verify them like any thrall and self-terminate on loss, so an edge never outlives its lord and
	// never runs as a second instance behind a stale lock.
	if err := startEdgeFencing(nc, spec.Name, logger); err != nil {
		log.Fatalf("edge %q: fencing: %v", spec.Name, err)
	}

	stopHB := make(chan struct{})
	go heartbeat(nc, spec.Name, stopHB)

	// A drain arrives either as the lord's ctl message or as a SIGTERM/SIGINT escalation.
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	drained := watchDrain(nc, spec.Name, logger)

	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("edge server stopped", slog.String("err", err.Error()))
		}
	}()
	logger.Info("edge http listening", slog.String("addr", spec.Addr), slog.Int("routes", len(spec.Routes)))

	select {
	case <-drained:
		logger.Info("edge draining (lord ctl)")
	case <-ctx.Done():
		logger.Info("edge draining (signal)")
	}

	close(stopHB)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("edge shutdown", slog.String("err", err.Error()))
	}
}

// maxRequestBytes caps the request body an edge accepts - a baseline guard against a memory-exhaustion
// POST at the ingress boundary. Larger payloads are an application-specific concern for a reverse proxy.
const maxRequestBytes = 1 << 20 // 1 MiB

// edgeHandler translates each HTTP request into an ether call/cast and the outcome back to HTTP.
// Server-side failures are logged (the operator's only window into 5xx) and returned to the caller as
// a generic status text - the underlying NATS/thrall detail stays in the log, not on the wire.
func edgeHandler(nc *nats.Conn, app string, router *edge.Router, timeout time.Duration, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := router.Match(r.Method, r.URL.Path)
		if !ok {
			http.Error(w, "no route for "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			// Over the limit or a broken read: too large / unreadable, not a bad gateway.
			logger.Warn("edge: reading request body", slog.String("route", r.Method+" "+r.URL.Path), slog.String("err", err.Error()))
			http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		// The payload is forwarded verbatim as the envelope body. When the route carries a schema
		// the edge is a real boundary: reject a malformed body with a precise 400 naming the
		// offending field, before anything reaches the ether. Otherwise fall back to a
		// well-formed-JSON check so a marshal error does not leak downstream.
		if route.SchemaJSON != "" {
			if len(body) == 0 {
				http.Error(w, "request body is required and must match the route schema", http.StatusBadRequest)
				return
			}
			if err := schema.Validate([]byte(route.SchemaJSON), body); err != nil {
				var ve *schema.ValidationError
				if errors.As(err, &ve) {
					http.Error(w, ve.Error(), http.StatusBadRequest)
				} else {
					http.Error(w, "request body: "+err.Error(), http.StatusBadRequest)
				}
				return
			}
		} else if len(body) > 0 && !json.Valid(body) {
			http.Error(w, "request body must be valid JSON", http.StatusBadRequest)
			return
		}

		env := edge.BuildEnvelope(route, body, r.URL.Query())
		data, err := json.Marshal(env)
		if err != nil {
			logger.Error("edge: encoding envelope", slog.String("route", r.Method+" "+r.URL.Path), slog.String("err", err.Error()))
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		// cast: hand the message to the ether and acknowledge acceptance without waiting.
		if route.Kind == "cast" {
			if err := nc.Publish(wire.Cast(app, route.Thrall), data); err != nil {
				logger.Error("edge: publishing cast", slog.String("thrall", route.Thrall), slog.String("err", err.Error()))
				http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
				return
			}
			_ = nc.Flush()
			w.WriteHeader(http.StatusAccepted)
			return
		}

		// call: wait for the thrall's reply and return it.
		msg, err := nc.Request(wire.Call(app, route.Thrall), data, timeout)
		if err != nil {
			status := edge.StatusForError(err)
			logger.Warn("edge: call failed", slog.String("thrall", route.Thrall), slog.String("op", route.Op),
				slog.Int("status", status), slog.String("err", err.Error()))
			http.Error(w, http.StatusText(status), status)
			return
		}
		var reply wire.Envelope
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			logger.Error("edge: decoding reply", slog.String("thrall", route.Thrall), slog.String("err", err.Error()))
			http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			return
		}
		if reply.Status == "error" {
			// Application error from the thrall: log the detail, return a generic status to the caller.
			status := edge.StatusForReply(reply)
			var errType, errMsg string
			if reply.Error != nil {
				errType, errMsg = reply.Error.Type, reply.Error.Message
			}
			logger.Warn("edge: thrall error reply", slog.String("thrall", route.Thrall), slog.String("op", route.Op),
				slog.String("type", errType), slog.String("message", errMsg))
			http.Error(w, http.StatusText(status), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if len(reply.Payload) > 0 {
			_, _ = w.Write(reply.Payload)
		}
	})
}

// startEdgeFencing starts the self-fencing loops for the tokens the lord injected: the singleton
// lock (the edge holds a port) and the lord-liveness lease (every spawned process). Each is a no-op
// if its token is absent (e.g. an edge run outside a lord). A confirmed loss terminates the process.
func startEdgeFencing(nc *nats.Conn, name string, logger *slog.Logger) error {
	stop := make(chan struct{}) // fencing runs for the whole process lifetime
	if cfg, ok := fencing.ConfigFromEnv("AETHER_SINGLETON_EPOCH", "AETHER_SINGLETON_KEY"); ok {
		mgr, err := singleton.Open(nc)
		if err != nil {
			return fmt.Errorf("singleton fencing: open lock bucket: %w", err)
		}
		verify := func() (bool, error) { return mgr.Verify(cfg.Key, cfg.Epoch) }
		go fencing.Loop("singleton fencing", verify, singleton.TTL/3, singleton.TTL, logger, stop,
			fencing.ExitOnLost("singleton fencing", name, logger))
	}
	if cfg, ok := fencing.ConfigFromEnv("AETHER_LORD_EPOCH", "AETHER_LORD_KEY"); ok {
		mgr, err := lordlease.Open(nc)
		if err != nil {
			return fmt.Errorf("lord-liveness fencing: open lease bucket: %w", err)
		}
		verify := func() (bool, error) { return mgr.Verify(cfg.Key, cfg.Epoch) }
		go fencing.Loop("lord-liveness fencing", verify, lordlease.TTL/3, lordlease.TTL, logger, stop,
			fencing.ExitOnLost("lord-liveness fencing", name, logger))
	}
	return nil
}

// heartbeat publishes a heartbeat to the lord at the injected interval so the reaper sees the edge
// as alive. The edge has no mailbox, so it reports zero self-metrics (the shape the lord expects).
func heartbeat(nc *nats.Conn, name string, stop <-chan struct{}) {
	tick := func() {
		hb := wire.Envelope{V: 1, Kind: wire.KindHB, To: name, TS: time.Now().UnixMilli()}
		hb.Payload, _ = json.Marshal(wire.HeartbeatMetrics{})
		data, _ := json.Marshal(hb)
		_ = nc.Publish(wire.Heartbeat(name), data)
	}
	tick()
	t := time.NewTicker(obs.HeartbeatInterval())
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			tick()
		}
	}
}

// watchDrain subscribes to the edge's control channel and closes the returned channel on the first
// ctl:drain from the lord (the same graceful-stop contract the SDK thralls honor).
func watchDrain(nc *nats.Conn, name string, logger *slog.Logger) <-chan struct{} {
	drained := make(chan struct{})
	var once sync.Once
	_, err := nc.Subscribe(wire.Ctl(name), func(m *nats.Msg) {
		var env wire.Envelope
		if json.Unmarshal(m.Data, &env) != nil {
			return
		}
		if env.Kind == wire.KindCtl && env.Op == "drain" {
			once.Do(func() { close(drained) })
		}
	})
	if err != nil {
		logger.Warn("edge ctl subscribe", slog.String("err", err.Error()))
	}
	return drained
}
