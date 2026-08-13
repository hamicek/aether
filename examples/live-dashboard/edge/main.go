// A live-dashboard edge (model B): it holds browser SSE connections and pushes each client only the
// events of the site it is authorized for. The plumbing (SSE, per-connection scoped subscribe,
// backpressure, drain) is thrall.SSEStream; THIS file's job is only authorization - map the request to
// a site, then hand the connection to the stream. It runs via StartEdge, so it is supervised like any
// thrall (heartbeat, restart, drain, fencing).
package main

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

const addr = ":7392"

func main() {
	var srv *http.Server
	var stream *thrall.SSEStream

	def := thrall.EdgeDef{
		Init: func(ctx *thrall.Ctx) error {
			stream = thrall.NewSSEStream(ctx)

			mux := http.NewServeMux()
			mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
				// Authorization is application code. Here a demo token maps to a site; in production
				// this is where you would verify a JWT and read its `site` claim. The point is the same:
				// request -> authorized subject scope.
				site := siteFromToken(r)
				if site == "" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				// The client sees ONLY its site's event subject - NATS never delivers anything else.
				subject := wire.EventLog(ctx.App, site) // aether.<app>.<site>.evt
				_ = stream.ServeClient(w, r, subject)
			})

			// SSE needs long-lived connections: set only ReadHeaderTimeout (a WriteTimeout would cut
			// the stream off), unlike the request/response ingress server.
			srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			ctx.Log.Info("live-dashboard edge listening", "addr", addr)
			return nil
		},

		Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},

		// On drain: end the live SSE connections first, then shut the server down (Shutdown would
		// otherwise block waiting for the streaming handlers to return).
		Stop: func() {
			stream.Close()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		},
	}

	if err := thrall.StartEdge(def); err != nil {
		log.Fatalf("edge: %v", err)
	}
}

// siteFromToken is the demo authorization: a bearer token (query ?token= or Authorization header)
// maps to a site. A real edge would verify a JWT and take the site from its claims.
func siteFromToken(r *http.Request) string {
	tok := r.URL.Query().Get("token")
	if tok == "" {
		tok = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	switch tok {
	case "tok-site-1":
		return "site-1"
	case "tok-site-2":
		return "site-2"
	default:
		return ""
	}
}
