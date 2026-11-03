using System.Text.Json;
using System.Text.Json.Serialization;

namespace Aether;

// Kind distinguishes the message type on the wire (mirrors internal/wire/envelope.go).
public static class Kind
{
    public const string Call = "call";
    public const string Cast = "cast";
    public const string Reply = "reply";
    public const string HB = "hb";
    public const string Ctl = "ctl";
}

// Envelope is the uniform JSON envelope for all communication in the ether. Its shape is defined by
// internal/wire/envelope.go (the single source of truth) and mirrored by the Go, TS and Python SDKs.
// Any deviation here is a parity bug, caught by the shared golden fixtures in testdata/wire.
//
// Fields Go marks `omitempty` are nullable here and dropped when null (Json.Options). `v` and `kind`
// carry no omitempty in Go, so they are non-nullable and always serialized. Payload stays a raw
// JsonElement - the runtime forwards it verbatim and never looks inside.
public sealed class Envelope
{
    [JsonPropertyName("v")]
    public int V { get; set; }

    [JsonPropertyName("id")]
    public string? Id { get; set; }

    // Trace is a correlation id propagated across hops (call/cast chains), distinct from Id which
    // correlates a single request with its reply.
    [JsonPropertyName("trace")]
    public string? Trace { get; set; }

    // Idem is an optional caller-supplied idempotency key. On an idempotent thrall a call/cast
    // carrying the same Idem is deduplicated by the receiver.
    [JsonPropertyName("idem")]
    public string? Idem { get; set; }

    [JsonPropertyName("kind")]
    public string Kind { get; set; } = "";

    [JsonPropertyName("from")]
    public string? From { get; set; }

    [JsonPropertyName("to")]
    public string? To { get; set; }

    [JsonPropertyName("op")]
    public string? Op { get; set; }

    [JsonPropertyName("payload")]
    public JsonElement? Payload { get; set; }

    // Reply status: "ok" | "error".
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("error")]
    public WireError? Error { get; set; }

    [JsonPropertyName("ts")]
    public long? Ts { get; set; }
}

// WireError carries an error inside a reply message. type/message/retryable have no omitempty in Go,
// so retryable is always serialized (a false is written, matching the reply_error golden).
public sealed class WireError
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    [JsonPropertyName("message")]
    public string Message { get; set; } = "";

    [JsonPropertyName("retryable")]
    public bool Retryable { get; set; }
}

// HeartbeatMetrics are a thrall's self-reported runtime samples, carried in the payload of a
// heartbeat envelope. Field order and names are part of the wire contract. The three metric fields
// carry no omitempty in Go, so they are always written (a zero included); describe is optional.
public sealed class HeartbeatMetrics
{
    [JsonPropertyName("mailbox_depth")]
    public int MailboxDepth { get; set; }

    [JsonPropertyName("mailbox_latency_ms")]
    public double MailboxLatencyMs { get; set; }

    [JsonPropertyName("processed_total")]
    public ulong ProcessedTotal { get; set; }

    [JsonPropertyName("describe")]
    public ThrallDescribe? Describe { get; set; }
}

// ThrallDescribe is a thrall's self-report of its contract and most recent failure. All fields are
// omitempty in Go, so each is nullable here and dropped when unset.
public sealed class ThrallDescribe
{
    [JsonPropertyName("call_ops")]
    public string[]? CallOps { get; set; }

    [JsonPropertyName("cast_ops")]
    public string[]? CastOps { get; set; }

    [JsonPropertyName("version")]
    public string? Version { get; set; }

    [JsonPropertyName("last_error")]
    public string? LastError { get; set; }

    [JsonPropertyName("last_error_ms")]
    public long? LastErrorMs { get; set; }
}
