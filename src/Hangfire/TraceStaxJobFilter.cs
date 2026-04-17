// The Hangfire integration lives in a sub-namespace so it is only compiled when
// the consumer's project already references Hangfire.Core. The file compiles
// against the public Hangfire contracts (IServerFilter, IElectStateFilter) that
// are part of Hangfire.Core — no additional NuGet package is introduced by
// TraceStax itself.
//
// Usage:
//   GlobalJobFilters.Filters.Add(new TraceStaxJobFilter(client));

using System.Diagnostics;
using Hangfire.Server;
using Hangfire.States;

namespace TraceStax.Hangfire;

/// <summary>
/// Hangfire server filter that automatically records job lifecycle events to
/// TraceStax.
/// </summary>
/// <remarks>
/// Register once during application startup:
/// <code>
/// GlobalJobFilters.Filters.Add(new TraceStaxJobFilter(client));
/// </code>
/// </remarks>
public sealed class TraceStaxJobFilter : IServerFilter, IElectStateFilter
{
    // Key used to stash the Stopwatch in PerformingContext.Items so it is
    // accessible in OnPerformed without any shared mutable state.
    private const string StopwatchKey = "tracestax:stopwatch";
    private const string RunIdKey = "tracestax:run_id";

    private readonly TraceStaxClient _client;

    /// <summary>
    /// Creates a new <see cref="TraceStaxJobFilter"/>.
    /// </summary>
    /// <param name="client">Configured <see cref="TraceStaxClient"/> instance.</param>
    public TraceStaxJobFilter(TraceStaxClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    // -------------------------------------------------------------------------
    // IServerFilter
    // -------------------------------------------------------------------------

    /// <inheritdoc />
    public void OnPerforming(PerformingContext context)
    {
        // Generate a stable run ID for this execution attempt. Hangfire's own
        // job ID is reused across retries, so we append the attempt number to
        // keep each attempt distinct on the TraceStax side.
        string jobId = context.BackgroundJob.Id;
        int attempt = GetAttemptNumber(context);
        string runId = $"{jobId}-{attempt}";

        context.Items[RunIdKey] = runId;
        context.Items[StopwatchKey] = Stopwatch.StartNew();

        string jobType = context.BackgroundJob.Job?.Type?.Name ?? "unknown";
        string? queue = TryGetQueue(context);

        _client.TrackStart(runId, jobType, queue);
    }

    /// <inheritdoc />
    public void OnPerformed(PerformedContext context)
    {
        string runId = context.Items.TryGetValue(RunIdKey, out object? id)
            ? id as string ?? context.BackgroundJob.Id
            : context.BackgroundJob.Id;

        long durationMs = 0;
        if (context.Items.TryGetValue(StopwatchKey, out object? sw) && sw is Stopwatch stopwatch)
        {
            stopwatch.Stop();
            durationMs = stopwatch.ElapsedMilliseconds;
        }

        if (context.Exception is not null && !context.ExceptionHandled)
        {
            _client.TrackFailure(runId, durationMs, context.Exception);
        }
        else
        {
            _client.TrackSuccess(runId, durationMs);
        }
    }

    // -------------------------------------------------------------------------
    // IElectStateFilter — catches terminal state transitions so that jobs
    // that are deleted or moved to the Failed state by Hangfire's own
    // machinery (e.g. exceeded retry limit) are also recorded.
    // -------------------------------------------------------------------------

    /// <inheritdoc />
    public void OnStateElection(ElectStateContext context)
    {
        // Act when Hangfire elects a Failed terminal state. This covers jobs
        // whose retry limit has been exceeded — in that scenario Hangfire does
        // not call OnPerformed again, so this is the only opportunity to record
        // the final failure. TraceStax deduplicates events server-side by run ID,
        // so sending a second failure event for a job that was already captured
        // by OnPerformed is harmless.
        if (context.CandidateState is FailedState failedState)
        {
            _client.TrackFailure(
                runId: context.BackgroundJob.Id,
                durationMs: 0,
                ex: failedState.Exception);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int GetAttemptNumber(PerformingContext context)
    {
        try
        {
            // Hangfire stores the retry count in job parameters under the key
            // "RetryCount". It is absent on the first attempt.
            string? raw = context.Connection
                .GetJobParameter(context.BackgroundJob.Id, "RetryCount");

            if (int.TryParse(raw, out int retries))
                return retries + 1; // convert 0-based retry count to 1-based attempt
        }
        catch (InvalidOperationException)
        {
            // Swallow - this is best-effort metadata.
        }

        return 1;
    }

    private static string? TryGetQueue(PerformingContext context)
    {
        try
        {
            // Hangfire stores the queue name as a job parameter under the key
            // "Queue". It is set by QueueAttribute or the default queue config.
            return context.Connection
                .GetJobParameter(context.BackgroundJob.Id, "Queue");
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }
}
