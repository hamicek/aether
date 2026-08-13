// Custom edge (model B): a webserver written by hand via thrall.StartEdge. Unlike the built-in
// [[edge.http]] ingress (model A), it does something configuration cannot express - here a per-request
// Authorization check - before calling the stateful counter thrall over the ether. State lives in the
// counter; the edge only owns the socket.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/hamicek/aether/sdk/go/thrall"
)

// The custom edge binds its own port. A real OS port -> the manifest runs this thrall as a singleton.
const addr = ":7393"

func main() {
	var srv *http.Server

	def := thrall.EdgeDef{
		// Init builds the server (with access to ctx for the handlers) before Run serves it.
		Init: func(ctx *thrall.Ctx) error {
			mux := http.NewServeMux()
			mux.HandleFunc("/value", func(w http.ResponseWriter, r *http.Request) {
				// This is the reason it is model B, not A: a per-request auth check, which the
				// declarative ingress cannot express.
				if r.Header.Get("Authorization") != "Bearer secret" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				reply, err := ctx.Call("counter", "get", map[string]any{}, 2*time.Second)
				if err != nil {
					ctx.Log.Error("call counter failed", "err", err)
					http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(reply)
			})
			srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			ctx.Log.Info("custom edge listening", "addr", addr)
			return nil
		},

		// Run owns the socket: it serves until Stop (a drain) shuts the server down.
		Run: func(ctx *thrall.Ctx, stop <-chan struct{}) error {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},

		// Stop is the graceful hook invoked on drain - it unblocks Run's ListenAndServe.
		Stop: func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		},
	}

	if err := thrall.StartEdge(def); err != nil {
		log.Fatalf("edge: %v", err)
	}
}
