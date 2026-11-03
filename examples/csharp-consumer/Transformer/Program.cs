// A C# transformer thrall on aether. It consumes measurements from its durable mailbox (raw data
// another thrall casts to it, captured in JetStream), converts Celsius to Fahrenheit, and casts the
// result on to the "printer" thrall - a cross-language roundtrip: Go -> C# -> Go. The code uses no
// aether behaviour, only the thin C# client (connect, consume mailbox, cast, heartbeat).

using System.Runtime.InteropServices;
using Aether;

await using var client = AetherClient.ConnectFromEnv();
await client.ConnectAsync();

using var stop = new CancellationTokenSource();
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => stop.Cancel());
using var sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, _ => stop.Cancel());

long processed = 0;

// Liveness: a heartbeat so the lord marks this thrall ready and it shows up in `aether ps` / `fleet`.
var heartbeat = Heartbeat.RunAsync(
    client,
    () => new HeartbeatMetrics { ProcessedTotal = (ulong)Interlocked.Read(ref processed) },
    stop.Token);

try
{
    await client.ConsumeMailboxAsync(async (envelope, ct) =>
    {
        var celsius = envelope.Payload!.Value.GetProperty("celsius").GetDouble();
        var fahrenheit = (celsius * 9 / 5) + 32;
        Interlocked.Increment(ref processed);

        // Propagate the incoming trace so the whole Go -> C# -> Go operation stays correlated.
        await client.CastAsync("printer", "reading",
            new { celsius, fahrenheit }, trace: envelope.Trace, ct: ct);
    }, stop.Token);
}
catch (OperationCanceledException)
{
    // Normal shutdown: the lord asked the thrall to stop.
}

await heartbeat;
