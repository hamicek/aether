// Package obs holds the runtime's observability primitives shared by the lord and
// the Go SDK: structured logging and the in-process metric registry. Logs and metrics
// are configured from the environment so that a thrall (a child OS process) inherits
// the same setup the lord injects, without any per-SDK wiring.
package obs

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Environment variables that configure logging. The lord reads them for itself and
// injects them into every thrall it spawns, so the whole tree logs consistently.
const (
	EnvLogLevel  = "AETHER_LOG_LEVEL"  // debug | info | warn | error (default info)
	EnvLogFormat = "AETHER_LOG_FORMAT" // json | text (default text)
)

// LevelFromEnv resolves AETHER_LOG_LEVEL to a slog.Level. An empty or unknown value
// falls back to info, so a typo never silences the runtime.
func LevelFromEnv() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvLogLevel))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger builds the runtime logger from the environment, writing to stderr. Child
// processes inherit stderr from the lord, so lord and thrall logs land on one stream;
// the per-record attributes (component, app, name) keep them tellable apart.
func NewLogger() *slog.Logger { return NewWithWriter(os.Stderr) }

// NewWithWriter is NewLogger with an explicit sink - used by tests to capture output.
// The handler format follows AETHER_LOG_FORMAT (json for machines, text for dev).
func NewWithWriter(w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: LevelFromEnv()}
	var h slog.Handler
	if strings.EqualFold(strings.TrimSpace(os.Getenv(EnvLogFormat)), "json") {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
