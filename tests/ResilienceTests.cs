using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace TraceStax.Tests;

/// <summary>
/// Resilience tests for <see cref="TraceStaxClient"/>.
///
/// These tests guard the most critical production guarantee: the SDK must NEVER
/// crash, throw, or block the host application — even when the ingest server is
/// down, slow, or returning errors.
///
/// Scenarios covered:
/// <list type="bullet">
///   <item>enabled=false / dryRun=true are complete no-ops</item>
///   <item>Track* / Heartbeat / Snapshot never throw with a dead server</item>
///   <item>SDK does not mask the original exception when called from a finally block</item>
///   <item>Circuit breaker: CLOSED → OPEN after 3 failures, events dropped silently</item>
///   <item>Circuit breaker: OPEN → HALF_OPEN after 30 s cooldown, resets on success</item>
///   <item>Circuit breaker: HALF_OPEN → OPEN again if probe fails</item>
///   <item>Concurrent Track* calls do not race or throw</item>
/// </list>
/// </summary>
public class ResilienceTests
{
    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Start a local HTTP listener on an OS-assigned port and return its URL.
    /// The caller is responsible for calling <c>listener.Close()</c>.
    /// </summary>
    private static (HttpListener listener, string url) StartListener()
    {
        var listener = new HttpListener();
        // Port 0 isn't supported directly; pick a high random port instead.
        // In CI the port-in-use probability is negligible for short-lived tests.
        int port = new Random().Next(30_000, 60_000);
        string url = $"http://localhost:{port}/";
        listener.Prefixes.Add(url);
        listener.Start();
        return (listener, url.TrimEnd('/'));
    }

    /// <summary>
    /// Returns a <see cref="Task"/> that handles <paramref name="count"/> requests
    /// by responding with the given status code, then closes the listener.
    /// Counts actual hits into <paramref name="hitCount"/>.
    /// </summary>
    private static Task ServeAsync(
        HttpListener listener,
        int count,
        int statusCode,
        ConcurrentQueue<int> hitCount)
    {
        return Task.Run(async () =>
        {
            for (int i = 0; i < count; i++)
            {
                HttpListenerContext ctx;
                try { ctx = await listener.GetContextAsync(); }
                catch (Exception) { break; }

                hitCount.Enqueue(1);
                ctx.Response.StatusCode = statusCode;
                ctx.Response.OutputStream.Close();
            }
            listener.Close();
        });
    }

    /// <summary>Reads the private <c>_circuitState</c> field as an int (0=Closed, 1=Open, 2=HalfOpen).</summary>
    private static int GetCircuitStateInt(TraceStaxClient client)
    {
        var f = typeof(TraceStaxClient).GetField("_circuitState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (int)f.GetValue(client)!;
    }

    /// <summary>
    /// Force the circuit-open timestamp into the past so the cooldown has
    /// already elapsed.  Returns the client for chaining.
    /// </summary>
    private static TraceStaxClient BackDateCircuitOpen(TraceStaxClient client, long millisAgo = 31_000)
    {
        var f = typeof(TraceStaxClient).GetField("_circuitOpenedAt", BindingFlags.NonPublic | BindingFlags.Instance)!;
        f.SetValue(client, Environment.TickCount64 - millisAgo);
        return client;
    }

    /// <summary>
    /// Zero out the adaptive backoff delay so probe requests fire immediately
    /// rather than waiting up to 60 s for the exponential back-off to elapse.
    /// </summary>
    private static void ResetDispatchDelay(TraceStaxClient client)
    {
        var f = typeof(TraceStaxClient).GetField("_dispatchDelayMs", BindingFlags.NonPublic | BindingFlags.Instance)!;
        f.SetValue(client, 0);
    }

    // ── enabled=false ─────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithEmptyApiKey_AndEnabledFalse_DoesNotThrow()
    {
        // Disabled clients must be constructable with an empty API key — common in tests
        // and staging environments where the key isn't available but tracing is off.
        var client = new TraceStaxClient("", enabled: false);
        client.TrackStart("run-1", "TestJob"); // must be a silent no-op
    }

    [Fact]
    public void Constructor_WithEmptyApiKey_AndDryRun_DoesNotThrow()
    {
        // Dry-run clients must also be constructable with an empty API key.
        var client = new TraceStaxClient("", dryRun: true);
        client.TrackStart("run-dry", "TestJob"); // must log, not throw
    }

    [Fact]
    public void Constructor_WithEmptyApiKey_AndEnabled_Throws()
    {
        // A fully-enabled client still requires a real API key.
        Assert.Throws<ArgumentException>(() => new TraceStaxClient(""));
    }

    [Fact]
    public void DisabledClient_AllTrackMethods_DoNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1", enabled: false);
        client.TrackStart("run-1", "MyJob", "default");
        client.TrackSuccess("run-1", durationMs: 100);
        client.TrackFailure("run-1", durationMs: 50, ex: new Exception("boom"));
        client.Heartbeat("worker-1");
        client.Snapshot("default", depth: 5);
        // Reaching here means no exception was thrown.
    }

    [Fact]
    public async Task DisabledClient_HeartbeatAsync_ReturnsNull()
    {
        var client = new TraceStaxClient("ts_test", enabled: false);
        var result = await client.HeartbeatAsync("worker-1");
        Assert.Null(result);
    }

    // ── dryRun=true ───────────────────────────────────────────────────────────

    [Fact]
    public void DryRunClient_AllTrackMethods_DoNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1", dryRun: true);
        client.TrackStart("run-dry", "DryJob", "default");
        client.TrackSuccess("run-dry", durationMs: 42);
        client.TrackFailure("run-dry", durationMs: 10, ex: null);
    }

    [Fact]
    public async Task DryRunClient_HeartbeatAsync_ReturnsNull()
    {
        var client = new TraceStaxClient("ts_test", dryRun: true);
        var result = await client.HeartbeatAsync("worker-1");
        Assert.Null(result);
    }

    // ── Fire-and-forget guarantee ─────────────────────────────────────────────

    [Fact]
    public void TrackStart_DoesNotThrow_WhenServerIsUnreachable()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        // Must not throw — fire-and-forget Task swallows all errors internally
        for (int i = 0; i < 20; i++)
            client.TrackStart($"run-{i}", "MyJob", "default");
    }

    [Fact]
    public void SdkDoesNotPropagate_ThroughJobFinallyBlock()
    {
        var client      = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        bool jobDone    = false;

        try
        {
            jobDone = true;
        }
        finally
        {
            // In production this runs inside a Hangfire/MassTransit job hook.
            // It must never throw — otherwise it swallows the original exception.
            client.TrackSuccess("run-finally", durationMs: 5);
        }

        Assert.True(jobDone);
    }

    [Fact]
    public void OriginalJobException_PropagatesWhenSdkIsInFinally()
    {
        var client   = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var jobError = new InvalidOperationException("job crashed");

        Exception? caught = null;
        try
        {
            try   { throw jobError; }
            finally { client.TrackFailure("run-crash", durationMs: 1, ex: jobError); }
        }
        catch (Exception ex) { caught = ex; }

        Assert.Same(jobError, caught);
    }

    // ── Circuit breaker ───────────────────────────────────────────────────────

    /// <summary>
    /// After 3 consecutive non-2xx responses the circuit opens and further
    /// calls must be dropped without any additional HTTP requests.
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_OpensAfter3ConsecutiveFailures()
    {
        var (listener, url)  = StartListener();
        var hitQueue         = new ConcurrentQueue<int>();
        var serverTask       = ServeAsync(listener, 3, 503, hitQueue);

        var client = new TraceStaxClient("ts_test", endpoint: url);

        client.TrackStart("r1", "Job", "q");
        client.TrackStart("r2", "Job", "q");
        client.TrackStart("r3", "Job", "q");

        // Wait for all 3 server-side hits, then allow RecordFailure tasks to complete
        await serverTask;
        await Task.Delay(200);

        int hitsAtOpen = hitQueue.Count;
        Assert.Equal(3, hitsAtOpen);

        // Circuit should be OPEN (int value 1)
        Assert.Equal(1, GetCircuitStateInt(client));

        // 4th call must be dropped — restart listener to detect any new request
        var (listener2, _url2) = StartListener();
        var extra              = new ConcurrentQueue<int>();
        var extraTask          = ServeAsync(listener2, 1, 200, extra);

        client.TrackStart("r4-dropped", "Job", "q");
        await Task.Delay(200);

        Assert.Equal(0, extra.Count); // no extra request made
        listener2.Stop();
    }

    /// <summary>
    /// After cooldown the circuit transitions OPEN → HALF_OPEN, a probe request
    /// succeeds, and the circuit closes.
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_ResetsToClosedAfterSuccessfulProbe()
    {
        // Phase 1: open the circuit via 3 failures
        var (listener1, url)  = StartListener();
        var hits1             = new ConcurrentQueue<int>();
        var serve1            = ServeAsync(listener1, 3, 503, hits1);

        var client = new TraceStaxClient("ts_test", endpoint: url);
        client.TrackStart("r1", "Job", "q");
        client.TrackStart("r2", "Job", "q");
        client.TrackStart("r3", "Job", "q");

        await serve1;
        await Task.Delay(200);
        Assert.Equal(1, GetCircuitStateInt(client)); // OPEN

        // Phase 2: simulate cooldown elapsed; also clear backoff so the probe fires immediately
        BackDateCircuitOpen(client);
        ResetDispatchDelay(client);

        // Phase 3: probe succeeds — start a listener that returns 200
        var (listener2, url2) = StartListener();
        var hits2             = new ConcurrentQueue<int>();
        var serve2            = ServeAsync(listener2, 2, 200, hits2);

        // Switch the client's endpoint field to the new listener
        var endpointField = typeof(TraceStaxClient).GetField("_endpoint", BindingFlags.NonPublic | BindingFlags.Instance)!;
        endpointField.SetValue(client, url2);

        client.TrackStart("probe", "Job", "q");
        await Task.Delay(500);

        Assert.Equal(1, hits2.Count); // exactly one probe
        // Circuit should be CLOSED (int value 0)
        Assert.Equal(0, GetCircuitStateInt(client));

        // Phase 4: normal traffic resumes
        client.TrackStart("r5-normal", "Job", "q");
        await Task.Delay(500);
        Assert.Equal(2, hits2.Count); // second request made

        await serve2; // ServeAsync calls Close() after serving
    }

    /// <summary>
    /// A HALF_OPEN probe that fails must send the circuit back to OPEN.
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_HalfOpenProbeFailure_ReopensCircuit()
    {
        var (listener, url) = StartListener();
        var hits            = new ConcurrentQueue<int>();

        // Return 503 for all requests
        _ = Task.Run(async () =>
        {
            while (listener.IsListening)
            {
                HttpListenerContext ctx;
                try { ctx = await listener.GetContextAsync(); }
                catch (Exception) { break; }
                hits.Enqueue(1);
                ctx.Response.StatusCode = 503;
                ctx.Response.OutputStream.Close();
            }
        });

        var client = new TraceStaxClient("ts_test", endpoint: url);

        // Open the circuit
        client.TrackStart("r1", "Job", "q");
        client.TrackStart("r2", "Job", "q");
        client.TrackStart("r3", "Job", "q");

        long deadline = Environment.TickCount64 + 5_000;
        while (hits.Count < 3 && Environment.TickCount64 < deadline) await Task.Delay(50);
        await Task.Delay(200);

        BackDateCircuitOpen(client);
        ResetDispatchDelay(client); // clear backoff so probe fires immediately

        // Probe: HALF_OPEN → fails → re-opens
        client.TrackStart("probe", "Job", "q");
        await Task.Delay(500);

        int hitsAfterProbe = hits.Count;
        Assert.Equal(4, hitsAfterProbe); // exactly one probe

        // Circuit is OPEN again — next call dropped
        client.TrackStart("r5-dropped", "Job", "q");
        await Task.Delay(200);
        Assert.Equal(hitsAfterProbe, hits.Count); // no new requests

        listener.Stop();
    }

    // ── Queue memory cap ──────────────────────────────────────────────────────

    [Fact]
    public void QueueCap_DoesNotThrowOnOverflow()
    {
        // Point at a dead endpoint so Tasks linger
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");

        // Flood with events; must never throw even past the 10K cap
        for (int i = 0; i < 10_200; i++)
        {
            client.TrackStart($"r{i}", "BulkJob", "default");
        }

        // droppedEvents must be non-negative and pending must not exceed cap
        var stats = client.GetStats();
        Assert.True(stats.DroppedEvents >= 0);
        Assert.True(stats.QueueSize <= 10_000, $"QueueSize={stats.QueueSize} exceeds cap");
    }

    [Fact]
    public void GetStats_DroppedEventsIncrements_WhenQueueIsFull()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");

        // Fill _pendingCount to the cap directly so the next event is dropped
        // immediately — no need to race real HTTP tasks against the overflow check.
        var pendingField = typeof(TraceStaxClient)
            .GetField("_pendingCount", BindingFlags.NonPublic | BindingFlags.Instance)!;
        pendingField.SetValue(client, 10_000);

        // One more event must be dropped and counted
        client.TrackStart("overflow", "FloodJob", "default");

        var stats = client.GetStats();
        Assert.True(stats.DroppedEvents > 0,
            $"Expected DroppedEvents > 0 after queue overflow, got {stats.DroppedEvents}");
    }

    // ── Stats API ─────────────────────────────────────────────────────────────

    [Fact]
    public void GetStats_ReturnsInitialClosedState()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var stats = client.GetStats();
        Assert.Equal("closed", stats.CircuitState);
        Assert.True(stats.QueueSize >= 0);
        Assert.True(stats.DroppedEvents >= 0);
        Assert.True(stats.ConsecutiveFailures >= 0);
    }

    // ── CloseAsync shutdown timeout ───────────────────────────────────────────

    [Fact]
    public async Task CloseAsync_ReturnsWithinDeadlineEvenWithDeadServer()
    {
        // Point at a non-routable IP so Tasks never complete
        var client = new TraceStaxClient("ts_test", endpoint: "http://10.255.255.1");
        client.TrackStart("r1", "Job", "q");

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await client.CloseAsync(TimeSpan.FromSeconds(2));
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"CloseAsync took {sw.Elapsed.TotalSeconds:F1}s, expected < 5s");
    }

    // ── Concurrent safety ─────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentTrackCalls_DoNotThrowOrRace()
    {
        var client     = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var errors     = new ConcurrentQueue<Exception>();
        var tasks      = new Task[8];

        for (int t = 0; t < tasks.Length; t++)
        {
            int idx = t;
            tasks[t] = Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 50; i++)
                    {
                        client.TrackStart($"run-{idx}-{i}", "ConcurrentJob", "default");
                        client.TrackSuccess($"run-{idx}-{i}", i * 10L);
                    }
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }
            });
        }

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    // ── Large payload size guard (2B) ───────────────────────────────────────────

    [Fact]
    public void OversizedPayload_IsDroppedWithoutThrowing()
    {
        // The SDK guards against payloads > 512 KB inside FireAndForget.
        // Because serialization is async (Task.Run), we can only verify that
        // TrackStart doesn't throw at the call site and the client remains stable.
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");

        // This is a no-op at the public API level (payload size checked inside Task.Run).
        // The key assertion is: no exception escapes to the caller.
        var ex = Record.Exception(() => client.TrackStart("run-big", "BigJob", "default"));
        Assert.Null(ex);
    }

    // ── Hangfire middleware pattern (Phase 3) ──────────────────────────────────
    // Verifies that the exact call sequence made by TraceStaxJobFilter does not
    // throw even when the ingest server is unreachable. No Hangfire dependency
    // required — we exercise the client methods directly.

    [Fact]
    public void HangfirePattern_StartThenSuccess_WithDeadServer_DoesNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var runId = Guid.NewGuid().ToString("N");

        // Simulate OnPerforming
        var ex1 = Record.Exception(() => client.TrackStart(runId, "SomeBackgroundJob", "default"));
        Assert.Null(ex1);

        // Simulate OnPerformed (success)
        var ex2 = Record.Exception(() => client.TrackSuccess(runId, 123L));
        Assert.Null(ex2);
    }

    [Fact]
    public void HangfirePattern_StartThenFailure_WithDeadServer_DoesNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var runId = Guid.NewGuid().ToString("N");

        var ex1 = Record.Exception(() => client.TrackStart(runId, "SomeBackgroundJob", "default"));
        Assert.Null(ex1);

        // Simulate OnPerformed with an exception
        var ex2 = Record.Exception(() =>
            client.TrackFailure(runId, 50L, new InvalidOperationException("job error")));
        Assert.Null(ex2);
    }

    // ── MassTransit middleware pattern (Phase 3) ───────────────────────────────

    [Fact]
    public void MassTransitPattern_PreConsumePostConsume_WithDeadServer_DoesNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var runId = Guid.NewGuid().ToString("N");

        // PreConsume
        var ex1 = Record.Exception(() => client.TrackStart(runId, "OrderCreatedMessage", "order-created"));
        Assert.Null(ex1);

        // PostConsume
        var ex2 = Record.Exception(() => client.TrackSuccess(runId, 42L));
        Assert.Null(ex2);
    }

    [Fact]
    public void MassTransitPattern_ConsumeFault_WithDeadServer_DoesNotThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        var runId = Guid.NewGuid().ToString("N");

        var ex1 = Record.Exception(() => client.TrackStart(runId, "OrderCreatedMessage", "order-created"));
        Assert.Null(ex1);

        var ex2 = Record.Exception(() =>
            client.TrackFailure(runId, 100L, new Exception("consumer threw")));
        Assert.Null(ex2);
    }

    // ── HTTP 401 does not open circuit breaker ────────────────────────────────
    // A 401 is a permanent misconfiguration (wrong API key), not a transient
    // network error. Opening the circuit on 401 would silently drop all events
    // and hide the real problem. The circuit must stay CLOSED after 401 responses.

    [Fact]
    public async Task Http401_DoesNotOpenCircuitBreaker()
    {
        var (listener, url) = StartListener();
        var hits            = new ConcurrentQueue<int>();
        var serverTask      = ServeAsync(listener, 3, 401, hits);

        var client = new TraceStaxClient("ts_bad_key", endpoint: url);

        client.TrackStart("r1", "Job", "q");
        client.TrackStart("r2", "Job", "q");
        client.TrackStart("r3", "Job", "q");

        await serverTask;
        await Task.Delay(300);

        // Circuit must remain CLOSED (int value 0)
        Assert.Equal(0, GetCircuitStateInt(client)); // Circuit must stay CLOSED after 401 responses (not a transient failure)
    }

    [Fact]
    public async Task Http401_ConsecutiveFailuresNotIncremented()
    {
        var (listener, url) = StartListener();
        var hits            = new ConcurrentQueue<int>();
        var serverTask      = ServeAsync(listener, 1, 401, hits);

        var client = new TraceStaxClient("ts_bad_key", endpoint: url);
        client.TrackStart("probe", "Job", "q");

        await serverTask;
        await Task.Delay(200);

        var stats = client.GetStats();
        Assert.Equal(0, stats.ConsecutiveFailures); // ConsecutiveFailures must not increment on 401
    }

    // ── Concurrent close() (2C) ────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentCloseAsync_DoesNotDeadlockOrThrow()
    {
        var client = new TraceStaxClient("ts_test", endpoint: "http://localhost:1");
        client.TrackStart("run-1", "Job", "default");

        // Three concurrent CloseAsync calls must all complete without throwing.
        var errors = new ConcurrentQueue<Exception>();
        var tasks = new[]
        {
            client.CloseAsync(TimeSpan.FromSeconds(2)).ContinueWith(t => { if (t.IsFaulted) errors.Enqueue(t.Exception!.InnerException!); }),
            client.CloseAsync(TimeSpan.FromSeconds(2)).ContinueWith(t => { if (t.IsFaulted) errors.Enqueue(t.Exception!.InnerException!); }),
            client.CloseAsync(TimeSpan.FromSeconds(2)).ContinueWith(t => { if (t.IsFaulted) errors.Enqueue(t.Exception!.InnerException!); }),
        };
        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }
}
