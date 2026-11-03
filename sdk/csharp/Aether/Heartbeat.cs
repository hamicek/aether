using System.Text.Json;

namespace Aether;

// Heartbeat publishes the thrall's liveness beat to aether._lord.<name>.hb, so the lord marks this
// process live/ready and it shows up in `aether ps` / `aether fleet`. A thin consumer has no mailbox
// and no self-description, so it reports zero mailbox metrics and a running processed count, with no
// describe block. The lord still supervises the OS process it spawned regardless; the heartbeat adds
// the liveness and visibility a first-class thrall has.
//
// Deliberately out of scope for the thin client (full-SDK guarantees): the describe block (ops), the
// lord-liveness fencing loop (AETHER_LORD_*) and singleton fencing (AETHER_SINGLETON_*).
public static class Heartbeat
{
    // DefaultIntervalMs mirrors internal/obs (the historical 2s); MinIntervalMs floors it so a typo
    // cannot flood the bus.
    public const int DefaultIntervalMs = 2000;
    public const int MinIntervalMs = 100;

    // ClampIntervalMs normalizes a configured interval: non-positive falls back to the default, and
    // anything below the floor is raised to it (mirrors obs.ClampHeartbeatIntervalMs).
    public static int ClampIntervalMs(int ms)
    {
        if (ms <= 0)
        {
            return DefaultIntervalMs;
        }
        return ms < MinIntervalMs ? MinIntervalMs : ms;
    }

    // IntervalFromEnv reads AETHER_HEARTBEAT_INTERVAL_MS (the lord injects it), clamped.
    public static TimeSpan IntervalFromEnv()
    {
        var raw = Environment.GetEnvironmentVariable("AETHER_HEARTBEAT_INTERVAL_MS");
        var ms = int.TryParse(raw, out var parsed) ? ClampIntervalMs(parsed) : DefaultIntervalMs;
        return TimeSpan.FromMilliseconds(ms);
    }

    // New builds one heartbeat envelope (pure, broker-free so the wire shape is unit-testable).
    public static Envelope New(string name, HeartbeatMetrics metrics)
        => new()
        {
            V = 1,
            Kind = Kind.HB,
            To = name,
            Payload = JsonSerializer.SerializeToElement(metrics, Json.Options),
            Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

    // RunAsync publishes a heartbeat immediately (so the lord sees the thrall ready at once) and then
    // on every interval tick until cancellation. `snapshot` supplies the current metrics each beat.
    public static async Task RunAsync(AetherClient client, Func<HeartbeatMetrics> snapshot, CancellationToken ct)
    {
        var subject = Subjects.Heartbeat(client.Name);
        using var timer = new PeriodicTimer(IntervalFromEnv());
        do
        {
            var envelope = New(client.Name, snapshot());
            await client.Connection.PublishAsync(subject, Json.Encode(envelope), cancellationToken: ct);
        }
        while (await WaitQuietly(timer, ct));
    }

    // WaitForNextTickAsync throws on cancellation; a stopped heartbeat is a normal shutdown, not an
    // error, so we swallow the cancellation and end the loop.
    private static async Task<bool> WaitQuietly(PeriodicTimer timer, CancellationToken ct)
    {
        try
        {
            return await timer.WaitForNextTickAsync(ct);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
