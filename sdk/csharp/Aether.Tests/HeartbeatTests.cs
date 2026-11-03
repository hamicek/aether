using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Aether;
using Xunit;

namespace Aether.Tests;

public class HeartbeatTests
{
    private static byte[] Golden(string name, [CallerFilePath] string thisFile = "")
        => File.ReadAllBytes(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "..", "..", "testdata", "wire", name + ".json")));

    [Fact]
    public void New_MatchesGoldenMetrics()
    {
        var metrics = new HeartbeatMetrics { MailboxDepth = 2, MailboxLatencyMs = 1.5, ProcessedTotal = 10 };
        var envelope = Heartbeat.New("counter", metrics);
        envelope.Ts = 1700000000003;

        Assert.True(JsonNode.DeepEquals(
            JsonNode.Parse(Golden("hb_metrics")), JsonNode.Parse(Json.Encode(envelope))));
    }

    [Fact]
    public void New_MatchesGoldenWithDescribe()
    {
        var metrics = new HeartbeatMetrics
        {
            MailboxDepth = 0,
            MailboxLatencyMs = 0,
            ProcessedTotal = 3,
            Describe = new ThrallDescribe
            {
                CallOps = ["get", "value"],
                CastOps = ["inc", "reset"],
                Version = "1.2.0",
                LastError = "handler_error: boom",
                LastErrorMs = 1700000000005,
            },
        };
        var envelope = Heartbeat.New("counter", metrics);
        envelope.Ts = 1700000000006;

        Assert.True(JsonNode.DeepEquals(
            JsonNode.Parse(Golden("hb_describe")), JsonNode.Parse(Json.Encode(envelope))));
    }

    [Theory]
    [InlineData(0, Heartbeat.DefaultIntervalMs)]
    [InlineData(-5, Heartbeat.DefaultIntervalMs)]
    [InlineData(50, Heartbeat.MinIntervalMs)]
    [InlineData(5000, 5000)]
    public void ClampIntervalMs_Normalizes(int input, int expected)
    {
        Assert.Equal(expected, Heartbeat.ClampIntervalMs(input));
    }
}
