using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;

namespace TraceStax;

/// <summary>
/// Core TraceStax client. All network calls are fire-and-forget — exceptions are
/// swallowed silently so instrumentation can never destabilise the host process.
///
/// Resilience features:
/// - Circuit breaker: 3 consecutive failures → OPEN (silent drop) → 30 s cooldown
/// - X-Retry-After header support
/// - Backpressure directive from heartbeat response
/// - Thread dump capture via Environment.StackTrace / StackTrace class
/// </summary>
public sealed class TraceStaxClient
{
    // Shared across all client instances; HttpClient is designed to be reused.
    private static readonly HttpClient s_http = new()
    {
        Timeout = TimeSpan.FromSeconds(10)
    };

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
#if NET8_0_OR_GREATER
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
#else
        PropertyNamingPolicy = SnakeCaseNamingPolicy.Instance
#endif
    };

#if !NET8_0_OR_GREATER
    /// <summary>snake_case naming policy for .NET 6/7 where JsonNamingPolicy.SnakeCaseLower is unavailable.</summary>
    private sealed class SnakeCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly SnakeCaseNamingPolicy Instance = new();
        public override string ConvertName(string name)
        {
            if (string.IsNullOrEmpty(name)) return name;
            var sb = new System.Text.StringBuilder(name.Length + 4);
            sb.Append(char.ToLowerInvariant(name[0]));
            for (int i = 1; i < name.Length; i++)
            {
                if (char.IsUpper(name[i]))
                {
                    sb.Append('_');
                    sb.Append(char.ToLowerInvariant(name[i]));
                }
                else sb.Append(name[i]);
            }
            return sb.ToString();
        }
    }
#endif

    private const string SdkVersion = "0.1.0";
    private const string Language = "csharp";

    // Circuit breaker constants
    private const int    CircuitOpenThreshold = 3;
    private static readonly TimeSpan CircuitCooldown = TimeSpan.FromSeconds(30);

    // Queue depth guard
    private const int MaxPendingCount = 10_000;

    private enum CircuitState { Closed, Open, HalfOpen }

    private readonly string _apiKey;
    private readonly string _endpoint;
    private readonly bool   _enabled;
    private readonly bool   _dryRun;

    // Worker key for thread dumps (set by the framework monitor layer)
    public string? WorkerKey { get; set; }

    // Resilience state
    private int           _consecutiveFailures;
    private CircuitState  _circuitState = CircuitState.Closed;
    private long          _circuitOpenedAt; // Environment.TickCount64
    private long          _pauseUntilMs;    // epoch ms (0 = not paused)
    /// <summary>
    /// Grows exponentially on consecutive failures (up to 60 s), halves on success.
    /// Applied as a pre-dispatch delay inside FireAndForget tasks.
    /// </summary>
    private int           _dispatchDelayMs;
    private readonly object _lock = new();

    // Queue depth guard
    private int  _pendingCount;
    private long _droppedEvents;

    /// <summary>Stores job identity (jobType, queue) from TrackStart so success/failure events reuse it.</summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, (string jobType, string queue)>
        _pendingTasks = new();

    /// <summary>
    /// Creates a new <see cref="TraceStaxClient"/>.
    /// </summary>
    public TraceStaxClient(string apiKey, string endpoint = "https://ingest.tracestax.com", bool? enabled = null, bool? dryRun = null)
    {
        _enabled = enabled ?? !string.Equals(Environment.GetEnvironmentVariable("TRACESTAX_ENABLED"), "false", StringComparison.OrdinalIgnoreCase);
        _dryRun = dryRun ?? string.Equals(Environment.GetEnvironmentVariable("TRACESTAX_DRY_RUN"), "true", StringComparison.OrdinalIgnoreCase);

        // Only require a real key when the client will actually send events.
        // Disabled or dry-run clients are valid with an empty key (e.g. in tests).
        if (_enabled && !_dryRun && string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException("API key must not be empty.", nameof(apiKey));

        _apiKey = apiKey ?? "";
        _endpoint = endpoint.TrimEnd('/');
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Records that a job has started. Fire-and-forget.
    /// </summary>
    public void TrackStart(string runId, string jobType, string? queue = null)
    {
        _pendingTasks[runId] = (jobType, queue ?? "default");
        FireAndForget("/v1/ingest", BuildTaskEvent(runId, jobType, queue, "started", 0, null));
    }

    /// <summary>
    /// Records that a job completed successfully. Fire-and-forget.
    /// </summary>
    public void TrackSuccess(string runId, long durationMs)
    {
        if (!_pendingTasks.TryRemove(runId, out var id))
            Console.Error.WriteLine($"tracestax: WARNING TrackSuccess called with unknown runId: {runId} — job name and queue will be reported as unknown/default");
        FireAndForget("/v1/ingest", BuildTaskEvent(runId, id.jobType, id.queue, "succeeded", durationMs, null));
    }

    /// <summary>
    /// Records that a job failed. Fire-and-forget.
    /// </summary>
    public void TrackFailure(string runId, long durationMs, Exception? ex = null)
    {
        if (!_pendingTasks.TryRemove(runId, out var id))
            Console.Error.WriteLine($"tracestax: WARNING TrackFailure called with unknown runId: {runId} — job name and queue will be reported as unknown/default");
        ErrorInfo? errorInfo = ex is null ? null : new ErrorInfo(ex.GetType().Name, ex.Message, ex.StackTrace);
        FireAndForget("/v1/ingest", BuildTaskEvent(runId, id.jobType, id.queue, "failed", durationMs, errorInfo));
    }

    /// <summary>
    /// Sends a worker heartbeat synchronously and returns the server directives, or null on error.
    /// </summary>
    public async Task<HeartbeatDirectives?> HeartbeatAsync(string workerId, string[]? queues = null, int? threadCount = null)
    {
        if (!_enabled || _dryRun) return null;

        var payload = new HeartbeatPayload(
            Framework: "dotnet",
            Language: Language,
            SdkVersion: SdkVersion,
            Worker: new WorkerInfo(
                Key: workerId,
                Hostname: Environment.MachineName,
                Pid: Environment.ProcessId,
                Concurrency: threadCount ?? 1,
                Queues: queues is { Length: > 0 } ? queues : ["default"]),
            Timestamp: DateTimeOffset.UtcNow.ToString("O"));

        return await PostForDirectivesAsync("/v1/heartbeat", payload).ConfigureAwait(false);
    }

    /// <summary>
    /// Fire-and-forget heartbeat (backwards-compatible; directives ignored).
    /// </summary>
    public void Heartbeat(string workerId, string[]? queues = null, int? threadCount = null)
        => _ = HeartbeatAsync(workerId, queues, threadCount);

    /// <summary>
    /// Pauses ingest delivery until the given epoch millisecond timestamp.
    /// </summary>
    public void SetPauseUntil(long epochMs)
    {
        lock (_lock) { _pauseUntilMs = epochMs; }
    }

    /// <summary>
    /// Executes a server-issued command. Currently supports "thread_dump".
    /// </summary>
    public void ExecuteCommand(HeartbeatCommand cmd)
    {
        if (cmd.Type != "thread_dump") return;
        var wk = WorkerKey ?? $"dotnet:{Environment.ProcessId}";
        var dump = CaptureThreadDump();
        FireAndForget("/v1/dump", new DumpPayload(
            CmdId: cmd.Id,
            WorkerKey: wk,
            DumpText: dump,
            Language: Language,
            SdkVersion: SdkVersion,
            CapturedAt: DateTimeOffset.UtcNow.ToString("O")));
    }

    /// <summary>
    /// Sends a queue-depth snapshot. Fire-and-forget.
    /// </summary>
    public void Snapshot(string queueName, int depth, int? activeCount = null, int? failedCount = null)
        => FireAndForget("/v1/snapshot", new SnapshotPayload(
            Framework: "dotnet",
            Language: Language,
            SdkVersion: SdkVersion,
            Timestamp: DateTimeOffset.UtcNow.ToString("O"),
            QueueName: queueName,
            Depth: depth,
            ActiveCount: activeCount,
            FailedCount: failedCount));

    /// <summary>
    /// Waits for any in-flight fire-and-forget tasks to complete, up to
    /// <paramref name="timeout"/>. A slow or unreachable server cannot hang the
    /// host process beyond this deadline.
    /// </summary>
    public async Task CloseAsync(TimeSpan? timeout = null)
    {
        var deadline = timeout ?? TimeSpan.FromSeconds(5);
        var started = Environment.TickCount64;
        while (Interlocked.CompareExchange(ref _pendingCount, 0, 0) > 0)
        {
            if (TimeSpan.FromMilliseconds(Environment.TickCount64 - started) >= deadline)
            {
                Console.Error.WriteLine("[tracestax] CloseAsync timed out; dropping remaining in-flight events");
                return;
            }
            await Task.Delay(50).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns a snapshot of the client's internal health metrics.
    /// </summary>
    public ClientStats GetStats()
    {
        lock (_lock)
        {
            return new ClientStats(
                QueueSize: Interlocked.CompareExchange(ref _pendingCount, 0, 0),
                DroppedEvents: Interlocked.Read(ref _droppedEvents),
                CircuitState: _circuitState.ToString().ToLowerInvariant(),
                ConsecutiveFailures: _consecutiveFailures);
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static TaskEventPayload BuildTaskEvent(
        string runId, string? jobType, string? queue, string status, long durationMs, ErrorInfo? error)
        => new(
            Framework: "dotnet",
            Language: Language,
            SdkVersion: SdkVersion,
            Type: "task_event",
            Worker: new WorkerInfo(
                Key: $"{Environment.MachineName}:{Environment.ProcessId}",
                Hostname: Environment.MachineName,
                Pid: Environment.ProcessId,
                Concurrency: 1,
                Queues: ["default"]),
            Task: new TaskInfo(Name: jobType ?? "unknown", Id: runId, Queue: queue ?? "default", Attempt: 1),
            Status: status,
            Metrics: new MetricsInfo(DurationMs: durationMs),
            Error: error);

    private async Task<HeartbeatDirectives?> PostForDirectivesAsync<T>(string path, T body)
    {
        try
        {
            string json = JsonSerializer.Serialize(body, s_jsonOptions);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint + path) { Content = content };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);

            using var response = await s_http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
                .ConfigureAwait(false);

            // Honor X-Retry-After
            if (response.Headers.TryGetValues("X-Retry-After", out var raValues) && int.TryParse(raValues.FirstOrDefault(), out int secs) && secs > 0)
                SetPauseUntil(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + secs * 1_000L);

            if ((int)response.StatusCode == 401)
            {
                // Auth failures are NOT counted as circuit-breaker failures — the
                // circuit would open and silently drop all events, masking the real problem.
                Console.Error.WriteLine("[tracestax] Auth failed (401) – check your API key.");
                return null;
            }
            if (!response.IsSuccessStatusCode)
            {
                RecordFailure();
                return null;
            }

            RecordSuccess();
            // Cap response body at 1 MB to prevent OOM from large error pages.
            using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var buf = new byte[1_048_576];
            int total = 0, read;
            while (total < buf.Length && (read = await stream.ReadAsync(buf, total, buf.Length - total).ConfigureAwait(false)) > 0)
                total += read;
            var respJson = System.Text.Encoding.UTF8.GetString(buf, 0, total);
            var respObj = JsonSerializer.Deserialize<JsonElement>(respJson);
            if (respObj.TryGetProperty("directives", out var dirs))
            {
                bool pauseIngest = dirs.TryGetProperty("pause_ingest", out var pi) && pi.GetBoolean();
                long? pauseUntilMs = dirs.TryGetProperty("pause_until_ms", out var pum) && pum.ValueKind == JsonValueKind.Number
                    ? pum.GetInt64() : null;

                var commands = new List<HeartbeatCommand>();
                if (dirs.TryGetProperty("commands", out var cmds) && cmds.ValueKind == JsonValueKind.Array)
                {
                    foreach (var c in cmds.EnumerateArray())
                    {
                        var id = c.TryGetProperty("id", out var idEl) ? idEl.GetString() : null;
                        var type = c.TryGetProperty("type", out var typeEl) ? typeEl.GetString() : null;
                        if (id != null && type != null)
                            commands.Add(new HeartbeatCommand(id, type));
                    }
                }
                return new HeartbeatDirectives(pauseIngest, pauseUntilMs, commands);
            }
            return new HeartbeatDirectives(false, null, []);
        }
        catch (HttpRequestException ex)
        {
            Console.Error.WriteLine($"[tracestax] heartbeat parse failed: {ex.GetType().Name}: {ex.Message}");
            RecordFailure();
            return null;
        }
        catch (JsonException ex)
        {
            Console.Error.WriteLine($"[tracestax] heartbeat parse failed: {ex.GetType().Name}: {ex.Message}");
            RecordFailure();
            return null;
        }
        catch (TaskCanceledException ex)
        {
            Console.Error.WriteLine($"[tracestax] heartbeat parse failed: {ex.GetType().Name}: {ex.Message}");
            RecordFailure();
            return null;
        }
    }

    private void FireAndForget<T>(string path, T body)
    {
        if (!_enabled) return;
        if (_dryRun)
        {
            Console.WriteLine($"[tracestax dry-run] {path} {JsonSerializer.Serialize(body, s_jsonOptions)}");
            return;
        }
        if (!CircuitAllow()) return;
        lock (_lock)
        {
            if (_pauseUntilMs > 0 && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < _pauseUntilMs)
                return;
        }
        // Queue depth guard — prevent unbounded Task.Run growth when server is slow/down
        if (Interlocked.CompareExchange(ref _pendingCount, 0, 0) >= MaxPendingCount)
        {
            Interlocked.Increment(ref _droppedEvents);
            Console.Error.WriteLine("[tracestax] event queue full, dropping event");
            return;
        }
        Interlocked.Increment(ref _pendingCount);
        _ = Task.Run(async () =>
        {
            try
            {
                // Apply adaptive backoff delay before dispatching to avoid hammering
                // a failing backend. Read delay under the lock, then sleep without it.
                int delay;
                lock (_lock) { delay = _dispatchDelayMs; }
                if (delay > 0)
                    await Task.Delay(delay).ConfigureAwait(false);

                string json = JsonSerializer.Serialize(body, s_jsonOptions);
                // Guard against huge payloads (512 KB matches all other SDKs).
                if (Encoding.UTF8.GetByteCount(json) > 512 * 1024)
                {
                    Console.Error.WriteLine("[tracestax] payload exceeds 512 KB, dropping");
                    Interlocked.Decrement(ref _pendingCount);
                    return;
                }
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint + path) { Content = content };
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);

                using var response = await s_http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead)
                    .ConfigureAwait(false);

                // Honor X-Retry-After on ingest responses too
                if (response.Headers.TryGetValues("X-Retry-After", out var raValues) && int.TryParse(raValues.FirstOrDefault(), out int secs) && secs > 0)
                    SetPauseUntil(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + secs * 1_000L);

                if (response.IsSuccessStatusCode) RecordSuccess();
                else if ((int)response.StatusCode == 401)
                    // Auth failures are NOT counted as circuit-breaker failures — the
                    // circuit would open and silently drop all events, masking the real problem.
                    Console.Error.WriteLine("[tracestax] Auth failed (401) – check your API key.");
                else RecordFailure();
            }
            catch (HttpRequestException ex)
            {
                Console.Error.WriteLine($"[tracestax] dispatch failed: {ex.GetType().Name}: {ex.Message}");
                RecordFailure();
            }
            catch (JsonException ex)
            {
                Console.Error.WriteLine($"[tracestax] dispatch failed: {ex.GetType().Name}: {ex.Message}");
                RecordFailure();
            }
            catch (TaskCanceledException ex)
            {
                Console.Error.WriteLine($"[tracestax] dispatch failed: {ex.GetType().Name}: {ex.Message}");
                RecordFailure();
            }
            finally
            {
                Interlocked.Decrement(ref _pendingCount);
            }
        });
    }

    private bool CircuitAllow()
    {
        lock (_lock)
        {
            if (_circuitState == CircuitState.Open)
            {
                var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - _circuitOpenedAt);
                if (elapsed < CircuitCooldown) return false;
                _circuitState = CircuitState.HalfOpen;
            }
            return true;
        }
    }

    private void RecordSuccess()
    {
        lock (_lock)
        {
            _consecutiveFailures = 0;
            _circuitState = CircuitState.Closed;
            _dispatchDelayMs = Math.Max(0, _dispatchDelayMs / 2);
        }
    }

    private void RecordFailure()
    {
        lock (_lock)
        {
            _consecutiveFailures++;
            _dispatchDelayMs = Math.Min(60_000, _dispatchDelayMs == 0 ? 500 : _dispatchDelayMs * 2);
            if (_consecutiveFailures >= CircuitOpenThreshold && _circuitState == CircuitState.Closed)
            {
                _circuitState = CircuitState.Open;
                _circuitOpenedAt = Environment.TickCount64;
                Console.Error.WriteLine("[tracestax] TraceStax unreachable, circuit open, events dropped");
            }
            else if (_circuitState == CircuitState.HalfOpen)
            {
                _circuitState = CircuitState.Open;
                _circuitOpenedAt = Environment.TickCount64;
            }
        }
    }

    private static string CaptureThreadDump()
    {
        var sb = new StringBuilder("=== TraceStax .NET Thread Dump ===\n");
        sb.AppendLine($"PID: {Environment.ProcessId}");
        sb.AppendLine($"Timestamp: {DateTimeOffset.UtcNow:O}");
        sb.AppendLine();
        sb.AppendLine("=== Current Thread Stack ===");
        sb.AppendLine($"{new StackTrace(true)}");
        var result = sb.ToString();
        return result.Length > 500_000 ? result[..500_000] : result;
    }
}

// -------------------------------------------------------------------------
// Payload records
// -------------------------------------------------------------------------

internal sealed record TaskEventPayload(
    string Framework, string Language, string SdkVersion, string Type,
    WorkerInfo Worker, TaskInfo Task, string Status, MetricsInfo Metrics, ErrorInfo? Error);

internal sealed record HeartbeatPayload(
    string Framework, string Language, string SdkVersion, WorkerInfo Worker, string Timestamp);

internal sealed record WorkerInfo(string Key, string Hostname, int Pid, int? Concurrency, string[] Queues);

internal sealed record TaskInfo(string Name, string Id, string Queue, int Attempt);

internal sealed record MetricsInfo(long DurationMs);

internal sealed record ErrorInfo(string Type, string Message, string? StackTrace);

internal sealed record SnapshotPayload(
    string Framework, string Language, string SdkVersion, string Timestamp,
    string QueueName, int Depth, int? ActiveCount, int? FailedCount);

internal sealed record DumpPayload(
    string CmdId, string WorkerKey, string DumpText, string Language, string SdkVersion, string CapturedAt);

/// <summary>Directives returned by the server in the heartbeat response.</summary>
public sealed record HeartbeatDirectives(bool PauseIngest, long? PauseUntilMs, IReadOnlyList<HeartbeatCommand> Commands);

/// <summary>A single server-issued command.</summary>
public sealed record HeartbeatCommand(string Id, string Type);

/// <summary>Snapshot of SDK health metrics, returned by <see cref="TraceStaxClient.GetStats"/>.</summary>
public sealed record ClientStats(int QueueSize, long DroppedEvents, string CircuitState, int ConsecutiveFailures);
