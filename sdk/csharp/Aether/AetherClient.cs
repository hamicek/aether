using System.Runtime.CompilerServices;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Aether;

// AetherClient is the thin C# client for the ether: it connects with the AETHER_* environment the
// lord injects, publishes casts and consumes messages, decoding them into the shared Envelope. It is
// deliberately not a full SDK - no behaviours, mailbox, durable delivery or fencing (see the inbox
// item "Full C# SDK"). Durability is a consumer-side concern: a cast is a plain publish to
// aether.<app>.<target>.cast, and a durable target's stream captures it (mirrors Go doCast).
public sealed class AetherClient : IAsyncDisposable
{
    private readonly NatsConnection _nats;

    // App is the application namespace (AETHER_APP); Name is this process's thrall name (AETHER_NAME).
    public string App { get; }
    public string Name { get; }

    private AetherClient(NatsConnection nats, string app, string name)
    {
        _nats = nats;
        App = app;
        Name = name;
    }

    // The underlying NATS connection, for a consumer that needs JetStream (reading a durable stream)
    // or a raw subscription beyond Consume.
    public NatsConnection Connection => _nats;

    // ConnectFromEnv builds a client from the environment the lord injects when it spawns a thrall:
    // AETHER_NATS_URL and AETHER_APP are required, AETHER_NAME names this thrall, and the optional
    // AETHER_NATS_CA / AETHER_NATS_NKEY_SEED carry TLS and nkey credentials for a secured bus.
    public static AetherClient ConnectFromEnv()
    {
        var url = Environment.GetEnvironmentVariable("AETHER_NATS_URL");
        var app = Environment.GetEnvironmentVariable("AETHER_APP");
        if (string.IsNullOrEmpty(url) || string.IsNullOrEmpty(app))
        {
            throw new InvalidOperationException(
                "missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`");
        }
        var name = Environment.GetEnvironmentVariable("AETHER_NAME") ?? "";

        var opts = NatsOpts.Default with { Url = url, Name = name };

        var seed = Environment.GetEnvironmentVariable("AETHER_NATS_NKEY_SEED");
        if (!string.IsNullOrEmpty(seed))
        {
            opts = opts with { AuthOpts = NatsAuthOpts.Default with { Seed = seed } };
        }

        var ca = Environment.GetEnvironmentVariable("AETHER_NATS_CA");
        if (!string.IsNullOrEmpty(ca))
        {
            opts = opts with { TlsOpts = new NatsTlsOpts { CaFile = ca, Mode = TlsMode.Require } };
        }

        return new AetherClient(new NatsConnection(opts), app, name);
    }

    // ConnectAsync opens the connection eagerly. Publishing or subscribing also connects on demand,
    // so this is optional - useful to fail fast at startup.
    public ValueTask ConnectAsync() => _nats.ConnectAsync();

    // Cast is a fire-and-forget message to another thrall (GenServer.cast). A fresh trace is minted
    // when none is given; a consumer inside the chain passes the trace it received to keep the whole
    // operation correlated across hops.
    public ValueTask CastAsync(string target, string op, object? payload,
        string? trace = null, string? idem = null, CancellationToken ct = default)
    {
        var envelope = NewCast(target, op, ToElement(payload), OrNewTrace(trace), idem);
        return _nats.PublishAsync(Subjects.Cast(App, target), Json.Encode(envelope), cancellationToken: ct);
    }

    // IsDurable reports whether the lord started this thrall with a durable mailbox (AETHER_DURABLE=1),
    // in which case its casts are captured by a JetStream stream the lord provisions.
    public bool IsDurable => Environment.GetEnvironmentVariable("AETHER_DURABLE") == "1";

    // ConsumeMailbox reads this thrall's own cast mailbox - the natural entry point for a consumer
    // the lord spawned. A durable thrall reads its JetStream mailbox stream with explicit ack (casts
    // that arrived while it was down are drained on return); a non-durable one reads the live subject
    // over core NATS. The handler receives each decoded envelope; it runs before the ack, so a
    // durable message is not acked until the handler returns. At-least-once: the handler must be
    // idempotent (mirrors the Go/TS/Python durable contract).
    public Task ConsumeMailboxAsync(Func<Envelope, CancellationToken, Task> handler, CancellationToken ct = default)
        => IsDurable
            ? ConsumeDurableAsync(handler, ct)
            : ConsumeCoreAsync(Subjects.Cast(App, Name), handler, ct);

    // ConsumeAsync subscribes to a subject over core NATS and yields each decoded envelope until
    // cancellation - an ad-hoc tap for a live subject. For a thrall's own mailbox prefer
    // ConsumeMailbox, which also handles the durable case.
    public async IAsyncEnumerable<Envelope> ConsumeAsync(string subject,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var msg in _nats.SubscribeAsync<byte[]>(subject, cancellationToken: ct))
        {
            var envelope = Decode(msg.Data);
            if (envelope is not null)
            {
                yield return envelope;
            }
        }
    }

    private async Task ConsumeCoreAsync(string subject, Func<Envelope, CancellationToken, Task> handler, CancellationToken ct)
    {
        await foreach (var msg in _nats.SubscribeAsync<byte[]>(subject, cancellationToken: ct))
        {
            var envelope = Decode(msg.Data);
            if (envelope is not null)
            {
                await handler(envelope, ct);
            }
        }
    }

    // ConsumeDurableAsync binds a durable pull consumer to this thrall's mailbox stream and drains it
    // with explicit, in-order ack - the C# mirror of the Go SDK's consumeDurableCast. The lord
    // provisions the stream before it spawns the thrall; a brief retry covers the startup race.
    private async Task ConsumeDurableAsync(Func<Envelope, CancellationToken, Task> handler, CancellationToken ct)
    {
        var js = new NatsJSContext(_nats);
        var stream = Subjects.Stream(App, Name);
        var config = new ConsumerConfig(Name)
        {
            FilterSubject = Subjects.Cast(App, Name),
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
        };

        var consumer = await CreateConsumerWithRetryAsync(js, stream, config, ct);

        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
        {
            var envelope = Decode(msg.Data);
            if (envelope is not null)
            {
                await handler(envelope, ct);
            }
            await msg.AckAsync(cancellationToken: ct);
        }
    }

    private static async Task<INatsJSConsumer> CreateConsumerWithRetryAsync(
        NatsJSContext js, string stream, ConsumerConfig config, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await js.CreateOrUpdateConsumerAsync(stream, config, ct);
            }
            catch (NatsJSApiException) when (attempt < 20)
            {
                // The lord may not have provisioned the stream yet at startup; back off and retry.
                await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
            }
        }
    }

    private static Envelope? Decode(byte[]? data) => data is null ? null : Json.Decode<Envelope>(data);

    // NewCast builds the cast envelope exactly as the Go SDK's newCastEnvelope does: v=1, a fresh id,
    // the trace and optional idem, kind=cast, the target and op, the payload, and a millisecond
    // timestamp. From is deliberately left unset, matching the Go producer. Pure and broker-free so
    // the wire shape is unit-testable.
    public static Envelope NewCast(string target, string op, JsonElement? payload, string? trace, string? idem)
        => new()
        {
            V = 1,
            Id = NewId(),
            Trace = trace,
            Idem = idem,
            Kind = Kind.Cast,
            To = target,
            Op = op,
            Payload = payload,
            Ts = NowMs(),
        };

    // OrNewTrace returns the given trace, or a fresh one when it is empty (this process is the edge
    // that starts the operation).
    public static string OrNewTrace(string? trace) => string.IsNullOrEmpty(trace) ? NewId() : trace;

    private static JsonElement? ToElement(object? payload) =>
        payload is null ? null : JsonSerializer.SerializeToElement(payload, Json.Options);

    private static string NewId() => Guid.NewGuid().ToString("N");

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public ValueTask DisposeAsync() => _nats.DisposeAsync();
}
