using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Aether;
using Xunit;

namespace Aether.Tests;

// Unit tests for the cast envelope the client produces - no broker. The live cross-language
// roundtrip is exercised by the polyglot example (plan step 5).
public class CastTests
{
    private static JsonElement EmptyObject => JsonSerializer.SerializeToElement(new { });

    private static byte[] Golden(string name, [CallerFilePath] string thisFile = "")
        => File.ReadAllBytes(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "..", "testdata", "wire", name + ".json")));

    [Fact]
    public void NewCast_MatchesGoldenCastShape()
    {
        // The cast golden is {v,id,kind:cast,to:counter,op:inc,payload:{},ts}. Pin id/ts to the
        // fixture's (they are generated at runtime) and the rest must be byte-for-value identical.
        var envelope = AetherClient.NewCast("counter", "inc", EmptyObject, trace: null, idem: null);
        envelope.Id = "c-2";
        envelope.Ts = 1700000000001;

        Assert.True(JsonNode.DeepEquals(
            JsonNode.Parse(Golden("cast")), JsonNode.Parse(Json.Encode(envelope))));
    }

    [Fact]
    public void NewCast_IncludesTrace_OmitsFrom()
    {
        var node = JsonNode.Parse(Json.Encode(
            AetherClient.NewCast("counter", "inc", EmptyObject, trace: "t-abc", idem: null)))!.AsObject();

        Assert.Equal("t-abc", node["trace"]!.GetValue<string>());
        Assert.Equal("cast", node["kind"]!.GetValue<string>());
        Assert.False(node.ContainsKey("from"));
        Assert.False(node.ContainsKey("status"));
        Assert.False(node.ContainsKey("idem")); // null idem is dropped
    }

    [Fact]
    public void NewCast_GeneratesIdAndTimestamp()
    {
        var envelope = AetherClient.NewCast("counter", "inc", EmptyObject, trace: null, idem: null);

        Assert.False(string.IsNullOrEmpty(envelope.Id));
        Assert.True(envelope.Ts > 0);
        Assert.Null(envelope.Trace); // NewCast does not mint a trace; CastAsync/OrNewTrace does
    }

    [Fact]
    public void OrNewTrace_KeepsGivenTrace_MintsWhenEmpty()
    {
        Assert.Equal("t-1", AetherClient.OrNewTrace("t-1"));
        Assert.False(string.IsNullOrEmpty(AetherClient.OrNewTrace(null)));
        Assert.False(string.IsNullOrEmpty(AetherClient.OrNewTrace("")));
    }
}
