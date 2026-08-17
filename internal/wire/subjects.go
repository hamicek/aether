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

// EventLog = subject a thrall appends event-sourcing events to (opt-in). Unlike the WorkQueue
// mailbox, these are captured by a separate RETENTION stream (EventLogStream) and are therefore
// replayable in init to rebuild state ("log is truth, state is a projection").
//
// Note: this subject is under the mailbox wildcard Data (`aether.<app>.<name>.*`), so a
// non-durable thrall receives its own appended events back on its data subscription; the mailbox
// dispatch ignores the "evt" verb (only call/cast are handled), so it is harmless - just a small
// self-echo. Durable thralls do not have the wildcard subscription and are unaffected.
func EventLog(app, name string) string { return fmt.Sprintf("aether.%s.%s.evt", app, name) }

// EventLogStream = name of the retention JetStream stream backing a thrall's event log.
func EventLogStream(app, name string) string { return fmt.Sprintf("aether_%s_%s_evt", app, name) }

// DedupHeader is the JetStream message header an Append carries to deduplicate an event within
// the event-log stream's duplicate window. Two Appends with the same value land as one message.
// This is the single source of truth for the dedup contract mirrored by the Go/TS/Python SDKs
// (Go nats.MsgId, TS msgID, Python Nats-Msg-Id header all set this same header).
const DedupHeader = "Nats-Msg-Id"

// DefaultEventLogDedupWindowMs is the event-log stream's duplicate window when the manifest does
// not set one. It matches the JetStream default (2 min) but is applied explicitly so the window
// is a deliberate, inspectable choice rather than an implicit server default.
const DefaultEventLogDedupWindowMs = 2 * 60 * 1000

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
