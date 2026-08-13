// Package fencing holds the self-fencing loop shared by processes the lord spawns: a thrall or an
// edge server periodically verifies a KV token (a singleton lock or the lord's liveness lease) and
// terminates itself when the token is lost, so no process outlives the authority that owns it. The
// loop is deliberately transport-agnostic - it takes a verify closure - so both the singleton lock
// and the lord lease reuse it.
package fencing

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config is a fencing token injected by the lord (an epoch + the KV key it stamps).
type Config struct {
	Key   string
	Epoch uint64
}

// ConfigFromEnv reads a token from the given epoch/key environment variables. ok is false when the
// process was not handed this kind of token (e.g. a non-singleton thrall, or one started without a
// lord), in which case the corresponding fencing loop must not run.
func ConfigFromEnv(epochEnv, keyEnv string) (Config, bool) {
	epoch, err := strconv.ParseUint(os.Getenv(epochEnv), 10, 64)
	if err != nil || epoch == 0 {
		return Config{}, false
	}
	key := os.Getenv(keyEnv)
	if key == "" {
		return Config{}, false
	}
	return Config{Key: key, Epoch: epoch}, true
}

// Loop runs the self-fencing loop until stop is closed. On each tick it calls verify(): a confirmed
// loss (verify returns false - the epoch was superseded or the key is gone) calls onLost at once.
// When verify cannot conclude (an error, e.g. the KV is unreachable) it calls onLost only once the
// lease has fully elapsed with no confirmation, bounding the window in which the fenced condition
// may already have failed. onLost is expected to terminate the process; label prefixes the logs.
func Loop(label string, verify func() (bool, error), interval, lease time.Duration, log *slog.Logger, stop <-chan struct{}, onLost func(reason string)) {
	lastConfirmed := time.Now()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			ok, err := verify()
			switch {
			case err != nil:
				if time.Since(lastConfirmed) > lease {
					onLost(fmt.Sprintf("unverifiable for over %s: %v", lease, err))
					return
				}
				log.Warn(label+": verify failed, within lease", slog.Any("err", err))
			case !ok:
				onLost("epoch superseded or key gone")
				return
			default:
				lastConfirmed = time.Now()
			}
		}
	}
}

// ExitOnLost is the production onLost: it logs the loss and terminates the process. Tests inject a
// channel-based onLost instead, so Loop stays verifiable without exiting.
func ExitOnLost(label, name string, log *slog.Logger) func(reason string) {
	return func(reason string) {
		log.Error(label+": self-terminating", slog.String("name", name), slog.String("reason", reason))
		os.Exit(1)
	}
}
