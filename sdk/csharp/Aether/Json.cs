using System.Text.Json;
using System.Text.Json.Serialization;

namespace Aether;

// Shared serialization options for the wire envelope. The wire contract (internal/wire/envelope.go)
// drops empty fields via Go's `omitempty`; we mirror that with WhenWritingNull plus nullable value
// types on exactly the fields Go marks omitempty. Fields Go leaves without omitempty (v, kind, the
// heartbeat metric fields, WireError.retryable) are non-nullable here, so they are always written -
// including a zero or false, which is what the Go source of truth emits.
public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static byte[] Encode<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static T? Decode<T>(ReadOnlySpan<byte> data) => JsonSerializer.Deserialize<T>(data, Options);
}
