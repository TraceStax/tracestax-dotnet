using Microsoft.Extensions.Hosting;

namespace TraceStax.Extensions;

/// <summary>
/// Abstract base class for hosted background services that automatically tracks
/// each job invocation with TraceStax.
/// </summary>
/// <remarks>
/// Subclasses implement <see cref="ExecuteJobAsync"/> rather than
/// <see cref="BackgroundService.ExecuteAsync"/>. The base class wraps each
/// invocation with <c>TrackStart</c> / <c>TrackSuccess</c> / <c>TrackFailure</c>
/// calls so instrumentation requires no boilerplate in the derived class.
///
/// <para>For periodic workloads prefer <see cref="TraceStaxPeriodicBackgroundService"/>
/// which additionally handles the wait interval between ticks.</para>
/// </remarks>
public abstract class TraceStaxBackgroundService : BackgroundService
{
    /// <summary>Gets the <see cref="TraceStaxClient"/> used for instrumentation.</summary>
    protected TraceStaxClient TraceStaxClient { get; }

    /// <summary>
    /// Gets the logical name of the job, used as the task type on the
    /// TraceStax dashboard (e.g. <c>"OrderExpiryJob"</c>).
    /// </summary>
    protected abstract string JobName { get; }

    /// <summary>
    /// Gets the queue or worker-group name reported to TraceStax
    /// (e.g. <c>"critical"</c>, <c>"default"</c>).
    /// </summary>
    protected abstract string QueueName { get; }

    /// <summary>
    /// Initialises the base class.
    /// </summary>
    /// <param name="client">Configured <see cref="TraceStaxClient"/> instance.</param>
    protected TraceStaxBackgroundService(TraceStaxClient client)
    {
        TraceStaxClient = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Implement this method to perform one unit of work.  The base class calls
    /// it in a loop from <see cref="ExecuteAsync"/> and handles all tracking.
    /// </summary>
    /// <param name="ct">Token that is cancelled when the host is stopping.</param>
    protected abstract Task ExecuteJobAsync(CancellationToken ct);

    /// <summary>
    /// Runs the service loop.  Derived classes that need a custom loop
    /// strategy (e.g. event-driven) may override this, but most should
    /// implement <see cref="ExecuteJobAsync"/> instead.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await RunTrackedAsync(stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes one <see cref="ExecuteJobAsync"/> invocation wrapped in
    /// TraceStax tracking.  Cancellation is re-thrown so the loop in
    /// <see cref="ExecuteAsync"/> can exit cleanly.
    /// </summary>
    protected async Task RunTrackedAsync(CancellationToken ct)
    {
        string runId = Guid.NewGuid().ToString("N");
        long startTick = Environment.TickCount64;

        TraceStaxClient.TrackStart(runId, JobName, QueueName);
        try
        {
            await ExecuteJobAsync(ct).ConfigureAwait(false);
            long durationMs = Math.Max(0L, Environment.TickCount64 - startTick);
            TraceStaxClient.TrackSuccess(runId, durationMs);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host is shutting down — record the cancellation as a failure so
            // in-progress runs are visible on the dashboard, then re-throw to
            // allow the host to complete its shutdown sequence.
            long durationMs = Math.Max(0L, Environment.TickCount64 - startTick);
            TraceStaxClient.TrackFailure(runId, durationMs);
            throw;
        }
#pragma warning disable CA1031 // codeql[cs/catch-of-base-type] - intentional: background service must never crash
        catch (Exception ex)
        {
            long durationMs = Math.Max(0L, Environment.TickCount64 - startTick);
            TraceStaxClient.TrackFailure(runId, durationMs, ex);
            // Do not re-throw non-cancellation exceptions by default - a single
            // failing tick should not bring down the entire service.  Derived
            // classes that want different behaviour can override ExecuteAsync.
        }
#pragma warning restore CA1031
    }
}

/// <summary>
/// A <see cref="TraceStaxBackgroundService"/> that uses a <see cref="PeriodicTimer"/>
/// to run <see cref="TraceStaxBackgroundService.ExecuteJobAsync"/> on a fixed interval.
/// </summary>
/// <remarks>
/// <code>
/// public class MyJob : TraceStaxPeriodicBackgroundService
/// {
///     public MyJob(TraceStaxClient client)
///         : base(client, interval: TimeSpan.FromMinutes(5)) { }
///
///     protected override string JobName  => "MyJob";
///     protected override string QueueName => "default";
///
///     protected override async Task ExecuteJobAsync(CancellationToken ct)
///     {
///         // do work …
///     }
/// }
/// </code>
/// </remarks>
public abstract class TraceStaxPeriodicBackgroundService : TraceStaxBackgroundService
{
    private readonly TimeSpan _interval;

    /// <summary>
    /// Initialises the periodic background service.
    /// </summary>
    /// <param name="client">Configured <see cref="TraceStaxClient"/> instance.</param>
    /// <param name="interval">How long to wait between each invocation of <see cref="TraceStaxBackgroundService.ExecuteJobAsync"/>.</param>
    protected TraceStaxPeriodicBackgroundService(TraceStaxClient client, TimeSpan interval)
        : base(client)
    {
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be positive.");

        _interval = interval;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Waits for the next timer tick before each invocation so that the
    /// interval is honoured regardless of how long <see cref="TraceStaxBackgroundService.ExecuteJobAsync"/> takes.
    /// </remarks>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);

        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            await RunTrackedAsync(stoppingToken).ConfigureAwait(false);
        }
    }
}
