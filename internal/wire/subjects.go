package wire

import "fmt"

// Thrall addressing: aether.<app>.<name>.<verb>
func Call(app, name string) string { return fmt.Sprintf("aether.%s.%s.call", app, name) }
func Cast(app, name string) string { return fmt.Sprintf("aether.%s.%s.cast", app, name) }
func Info(app, name string) string { return fmt.Sprintf("aether.%s.%s.info", app, name) }

// Data = wildcard across call/cast/info; a thrall puts a single subscription on it
// (one iterator = FIFO order, i.e. a serialized mailbox).
func Data(app, name string) string { return fmt.Sprintf("aether.%s.%s.*", app, name) }

// Stream = name of the JetStream stream for a thrall's durable mailbox. Dots are
// not allowed in stream names, hence underscores (app/name do not contain them).
func Stream(app, name string) string { return fmt.Sprintf("aether_%s_%s", app, name) }

// Supervision channels (lord <-> thrall): aether._lord.<name>.<verb>
func Ctl(name string) string       { return fmt.Sprintf("aether._lord.%s.ctl", name) }
func Heartbeat(name string) string { return fmt.Sprintf("aether._lord.%s.hb", name) }

// HeartbeatAll = wildcard for the lord to listen to heartbeats of all thralls.
func HeartbeatAll() string { return "aether._lord.*.hb" }

// LordCtl = the lord's inbound control channel (thrall -> lord), request/reply. Unlike
// Ctl (lord -> thrall), this is where a thrall asks the lord to spawn or stop a child
// at runtime (a ctl envelope with Op "spawn" | "stop").
func LordCtl() string { return "aether._lord.ctl" }

// Events = lifecycle stream (started/crashed/restarted) for the dashboard.
const Events = "aether._lord.events"
