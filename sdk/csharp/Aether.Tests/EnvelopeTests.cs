using System.Text.Json;
using System.Text.Json.Nodes;
using Aether;
using Xunit;

namespace Aether.Tests;

public class EnvelopeTests
{
    [Fact]
    public void EncodeDecode_RoundTrip_PreservesFields()
    {
        var env = new Envelope
        {
            V = 1,
            Id = "c-1",
            Trace = "t-abc",
            Kind = Kind.Cast,
            To = "counter",
            Op = "inc",
            Payload = JsonSerializer.Deserialize<JsonElement>("{\"delta\":3}"),
            Ts = 1700000000000,
        };

        var decoded = Json.Decode<Envelope>(Json.Encode(env))!;

        Assert.Equal(1, decoded.V);
        Assert.Equal("c-1", decoded.Id);
        Assert.Equal("t-abc", decoded.Trace);
        Assert.Equal(Kind.Cast, decoded.Kind);
        Assert.Equal("counter", decoded.To);
        Assert.Equal("inc", decoded.Op);
        Assert.Equal(1700000000000, decoded.Ts);
        Assert.Equal(3, decoded.Payload!.Value.GetProperty("delta").GetInt32());
    }

    [Fact]
    public void Encode_DropsOmitemptyFields()
    {
        // A minimal envelope writes only the non-omitempty fields (v, kind); everything else is
        // null and dropped, matching the Go `omitempty` wire form.
        var node = JsonNode.Parse(Json.Encode(new Envelope { V = 1, Kind = Kind.Call }))!.AsObject();

        Assert.Equal(2, node.Count);
        Assert.True(node.ContainsKey("v"));
        Assert.True(node.ContainsKey("kind"));
    }

    [Fact]
    public void Describe_OmitsEmptyFields()
    {
        // Mirrors the Python test_describe_dict_omits_empty_fields: unset fields do not appear.
        Assert.Equal("{}", JsonSerializer.Serialize(new ThrallDescribe(), Json.Options));

        var node = JsonNode.Parse(
            Json.Encode(new ThrallDescribe { CallOps = ["get"], Version = "1.0.0" }))!.AsObject();
        Assert.Equal(new[] { "call_ops", "version" }, node.Select(kv => kv.Key).OrderBy(k => k).ToArray());
    }

    [Fact]
    public void WireError_AlwaysWritesRetryable()
    {
        // retryable has no omitempty in Go, so false must be serialized (see the reply_error golden).
        var node = JsonNode.Parse(
            Json.Encode(new WireError { Type = "unknown_op", Message = "nope", Retryable = false }))!.AsObject();

        Assert.True(node.ContainsKey("retryable"));
        Assert.False(node["retryable"]!.GetValue<bool>());
    }
}
