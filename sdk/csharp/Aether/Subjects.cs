namespace Aether;

// Subject and stream naming, mirroring internal/wire/subjects.go. Thrall addressing is
// aether.<app>.<name>.<verb>; supervision rides aether._lord.<name>.<verb>.
public static class Subjects
{
    // Thrall addressing: aether.<app>.<name>.<verb>
    public static string Call(string app, string name) => $"aether.{app}.{name}.call";
    public static string Cast(string app, string name) => $"aether.{app}.{name}.cast";
    public static string Info(string app, string name) => $"aether.{app}.{name}.info";

    // Data = wildcard across call/cast/info (a thrall's serialized mailbox subscription).
    public static string Data(string app, string name) => $"aether.{app}.{name}.*";

    // Stream = JetStream stream name for a thrall's durable mailbox (dots are not allowed, hence
    // underscores; app/name do not contain them).
    public static string Stream(string app, string name) => $"aether_{app}_{name}";

    // EventLog = subject a thrall appends event-sourcing events to (opt-in).
    public static string EventLog(string app, string name) => $"aether.{app}.{name}.evt";

    // EventLogStream = retention JetStream stream backing a thrall's event log.
    public static string EventLogStream(string app, string name) => $"aether_{app}_{name}_evt";

    // Supervision channels (lord <-> thrall): aether._lord.<name>.<verb>
    public static string Ctl(string name) => $"aether._lord.{name}.ctl";
    public static string Heartbeat(string name) => $"aether._lord.{name}.hb";
    public static string HeartbeatAll() => "aether._lord.*.hb";

    // LordCtl = the lord's inbound control channel (thrall -> lord).
    public static string LordCtl() => "aether._lord.ctl";

    // Events = lifecycle stream (started/crashed/restarted) for the dashboard.
    public const string Events = "aether._lord.events";

    // Fleet observability: a curated health summary each lord publishes about itself.
    public static string FleetHealth(string app, string lordId) => $"aether._fleet.{app}.{lordId}";
    public static string FleetHealthAll() => "aether._fleet.>";

    // DedupHeader is the JetStream header an Append carries to deduplicate an event within the
    // stream's duplicate window (all SDKs resolve to this same header name).
    public const string DedupHeader = "Nats-Msg-Id";
}
