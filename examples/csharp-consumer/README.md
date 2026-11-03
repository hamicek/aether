# csharp-consumer - a C# transformer on the ether

A polyglot roundtrip across three languages, two of them swapping roles around a C# data processor:

```
source (Go)        casts raw {celsius} to the transformer
transformer (C#)   consumes its durable mailbox, converts C -> F, casts {celsius, fahrenheit}
printer (Go)       consumes the transformed reading and prints it
```

It demonstrates the **thin C# client** (`sdk/csharp`): connect from the `AETHER_*` environment the
lord injects, consume a (durable) mailbox, cast results back into the ether, and heartbeat so the
lord sees the thrall live. The C# code uses no aether behaviour - it is a plain consumer/producer,
which is exactly the data-processing role a C# developer writes on top of aether (e.g. transforming
raw measurements another thrall wrote to JetStream).

`transformer` and `printer` are `durable = true`, so casts are captured in JetStream and the demo
does not depend on startup order.

## Run

```bash
./run.sh
```

The script builds the `aether` binary and the Go thralls, publishes the C# transformer, starts the
lord, and asserts that a transformed reading reaches the printer:

```
reading: 0.0C = 32.0F
reading: 100.0C = 212.0F
reading: 37.0C = 98.6F
```

Requires Go, .NET 10+ (`mise use dotnet@latest`) and a working `aether` build. The C# transformer
is spawned by the lord via `dotnet Transformer/publish/Transformer.dll`, like any other thrall `cmd`.
