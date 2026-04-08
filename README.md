# TraceStax .NET SDK

Official .NET SDK for [TraceStax](https://tracestax.com) — Worker Intelligence Platform.

TraceStax collects job and task lifecycle events from background job frameworks so
you can monitor worker health, queue depth, and job success/failure rates in one
place.

---

## Requirements

- .NET 6 or later
- For Hangfire integration: [Hangfire.Core](https://www.nuget.org/packages/Hangfire.Core) 1.8+

---

## Installation

```shell
dotnet add package TraceStax
```

---

## Quick start

```csharp
using TraceStax;

var client = new TraceStaxClient(apiKey: "ts_live_xxxxxxxxxxxxxxxx");
```

All HTTP calls are fire-and-forget. The SDK never throws, never blocks the
calling thread, and silently discards any network or serialisation errors so
instrumentation cannot affect your application.

---

## Hangfire integration

### 1. Register the filter

Add `TraceStaxJobFilter` once during application startup, before the Hangfire
server starts processing jobs.

```csharp
using Hangfire;
using TraceStax;
using TraceStax.Hangfire;

var client = new TraceStaxClient(apiKey: "ts_live_xxxxxxxxxxxxxxxx");

// Register globally — applies to every job processed by this server.
GlobalJobFilters.Filters.Add(new TraceStaxJobFilter(client));
```

With ASP.NET Core and the Hangfire dashboard:

```csharp
builder.Services.AddHangfire(config =>
{
    config.UseSqlServerStorage(connectionString);
});
builder.Services.AddHangfireServer();

// Add the filter after the service container is built.
var client = new TraceStaxClient(apiKey: Environment.GetEnvironmentVariable("TRACESTAX_API_KEY")!);
GlobalJobFilters.Filters.Add(new TraceStaxJobFilter(client));
```

### 2. What the filter records

| Hangfire event         | TraceStax event      |
|------------------------|--------------------|
| Job picked up          | `started`          |
| Job completes without exception | `succeeded` |
| Job throws an exception | `failed`          |
| Job exceeds retry limit | `failed`          |

The filter implements both `IServerFilter` (for in-process timing) and
`IElectStateFilter` (for terminal state transitions that bypass normal
execution, such as exceeded retry limits).

---

## Plain .NET — manual instrumentation

Use `TraceStaxClient` directly when you are not using Hangfire or want fine-
grained control.

```csharp
using TraceStax;
using System.Diagnostics;

var client = new TraceStaxClient(apiKey: "ts_live_xxxxxxxxxxxxxxxx");

async Task ProcessJobAsync(string jobId, string jobType)
{
    client.TrackStart(runId: jobId, jobType: jobType, queue: "default");

    var sw = Stopwatch.StartNew();
    try
    {
        await DoWorkAsync();
        client.TrackSuccess(runId: jobId, durationMs: sw.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        client.TrackFailure(runId: jobId, durationMs: sw.ElapsedMilliseconds, ex: ex);
        throw;
    }
}
```

### Worker heartbeats

Send periodic heartbeats to keep your workers visible on the TraceStax dashboard:

```csharp
using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
while (await timer.WaitForNextTickAsync())
{
    client.Heartbeat(
        workerId: $"{Environment.MachineName}:{Environment.ProcessId}",
        queues: ["default", "critical"],
        threadCount: Environment.ProcessorCount);
}
```

---

## Configuration options

| Parameter    | Type     | Default                        | Description                                      |
|--------------|----------|--------------------------------|--------------------------------------------------|
| `apiKey`     | `string` | _(required)_                   | TraceStax API key. Obtain from the dashboard.      |
| `endpoint`   | `string` | `https://ingest.tracestax.com`    | Override for self-hosted or staging deployments. |

The API key may also be supplied via the `TRACESTAX_API_KEY` environment variable
by reading it yourself before passing to the constructor:

```csharp
var client = new TraceStaxClient(
    apiKey: Environment.GetEnvironmentVariable("TRACESTAX_API_KEY")
            ?? throw new InvalidOperationException("TRACESTAX_API_KEY is not set"));
```

### Pointing at a custom endpoint

```csharp
var client = new TraceStaxClient(
    apiKey: "ts_live_xxxxxxxxxxxxxxxx",
    endpoint: "https://ingest.staging.tracestax.com");
```

---

## Authentication

The SDK sends the API key as a Bearer token:

```
Authorization: Bearer {api_key}
```

Both `Authorization: Bearer` and `X-Api-Key` headers are accepted by the
TraceStax ingest API. The SDK uses `Authorization: Bearer` by default.

---

## API endpoints

| Method | Path             | Description                   |
|--------|------------------|-------------------------------|
| POST   | `/v1/ingest`     | Task lifecycle events         |
| POST   | `/v1/heartbeat`  | Worker heartbeats             |
| POST   | `/v1/snapshot`   | Queue depth snapshots         |

---

## Thread safety

`TraceStaxClient` is fully thread-safe. A single instance can be shared across
all threads, background services, and job workers in a process. The internal
`HttpClient` is a static instance that is reused across all requests.

---

## License

MIT — see [LICENSE](LICENSE).
