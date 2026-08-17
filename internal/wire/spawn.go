package wire

// Runtime supervision ops carried in a ctl envelope's Op field on the LordCtl channel
// (thrall -> lord). SpawnSpec is the payload for OpSpawn; StopSpec for OpStop.
const (
	OpSpawn = "spawn"
	OpStop  = "stop"
)

// SpawnSpec is the request payload to start a child at runtime - the subset of a
// manifest thrall relevant to a dynamic child. A dynamic child is always local scope
// and single instance; Replicas and Scope are not part of the runtime API.
type SpawnSpec struct {
	Name     string `json:"name"`
	Cmd      string `json:"cmd"`
	Restart  string `json:"restart,omitempty"`   // permanent | transient | temporary (default permanent)
	Durable  bool   `json:"durable,omitempty"`   // true -> casts go through JetStream
	EventLog bool   `json:"event_log,omitempty"` // true -> provision an event-sourcing log for Append/Rebuild
	// EventLogDedupWindowMs sets the event-log stream's duplicate window (0 = default). Within it,
	// two Appends carrying the same Nats-Msg-Id land as one message.
	EventLogDedupWindowMs int64 `json:"event_log_dedup_window_ms,omitempty"`
}

// StopSpec is the request payload to stop a child at runtime.
type StopSpec struct {
	Name string `json:"name"`
}

// SpawnReply is the reply payload for a successful spawn.
type SpawnReply struct {
	Name string `json:"name"`
}
