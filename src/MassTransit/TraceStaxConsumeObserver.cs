// The MassTransit integration lives in a sub-namespace so it only compiles when
// the consumer's project already references MassTransit. No additional NuGet
// package is introduced by TraceStax itself; the IConsumeObserver interface is
// resolved at compile-time only when the consumer has MassTransit installed.
//
// Usage:
//   busControl.ConnectConsumeObserver(new TraceStaxConsumeObserver(client));
//   // or via the extension method:
//   busControl.AddTraceStax(client);

using System.Collections.Concurrent;
using MassTransit;

namespace TraceStax.MassTransit;

/// <summary>
/// MassTransit consume observer that automatically records message consumption
/// lifecycle events to TraceStax.
/// </summary>
/// <remarks>
/// Register during application startup, after building the bus:
/// <code>
/// busControl.ConnectConsumeObserver(new TraceStaxConsumeObserver(client));
/// </code>
/// Or use the convenience extension method:
/// <code>
/// busControl.AddTraceStax(client);
/// </code>
/// </remarks>
public sealed class TraceStaxConsumeObserver : IConsumeObserver
{
    // Maps the MassTransit MessageId (always set by the framework) to a tuple
    // of (runId, startTick).  Using MessageId as the dictionary key guarantees
    // that PreConsume, PostConsume, and ConsumeFault all refer to the same
    // entry — even when CorrelationId is absent and a synthetic run ID was
    // generated in PreConsume.  ConcurrentDictionary is required because
    // MassTransit processes messages on multiple threads simultaneously.
    private readonly ConcurrentDictionary<Guid, (string RunId, long StartTick)> _inflight = new();

    private readonly TraceStaxClient _client;

    /// <summary>
    /// Creates a new <see cref="TraceStaxConsumeObserver"/>.
    /// </summary>
    /// <param name="client">Configured <see cref="TraceStaxClient"/> instance.</param>
    public TraceStaxConsumeObserver(TraceStaxClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    // -------------------------------------------------------------------------
    // IConsumeObserver
    // -------------------------------------------------------------------------

    /// <inheritdoc />
    /// <remarks>
    /// Records the wall-clock start time and fires a <c>TrackStart</c> event.
    /// The run ID reported to TraceStax is the message's CorrelationId when
    /// present, otherwise a newly generated GUID.  The entry is keyed by
    /// <c>MessageId</c> (always set by MassTransit) so that the same run ID is
    /// used across all three lifecycle callbacks even when no CorrelationId is
    /// present.
    /// </remarks>
    public Task PreConsume<T>(ConsumeContext<T> context) where T : class
    {
        string runId = context.CorrelationId.HasValue
            ? context.CorrelationId.Value.ToString("N")
            : Guid.NewGuid().ToString("N");

        string taskName = typeof(T).Name;
        string queueName = ResolveQueueName(context);

        // Store under MessageId so PostConsume/ConsumeFault can look it up.
        Guid messageKey = context.MessageId ?? Guid.NewGuid();
        _inflight[messageKey] = (runId, Environment.TickCount64);

        _client.TrackStart(runId, taskName, queueName);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <remarks>Fires a <c>TrackSuccess</c> event with the elapsed duration.</remarks>
    public Task PostConsume<T>(ConsumeContext<T> context) where T : class
    {
        (string runId, long durationMs) = DequeueInflight(context);
        _client.TrackSuccess(runId, durationMs);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <remarks>Fires a <c>TrackFailure</c> event with the elapsed duration and the fault exception.</remarks>
    public Task ConsumeFault<T>(ConsumeContext<T> context, Exception exception) where T : class
    {
        (string runId, long durationMs) = DequeueInflight(context);
        _client.TrackFailure(runId, durationMs, exception);

        return Task.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Looks up the in-flight entry for the message identified by
    /// <c>MessageId</c>, removes it from the dictionary, and returns the
    /// (runId, elapsedMs) pair.  If the entry is not found (e.g. because
    /// <c>PreConsume</c> was not called) a fallback run ID and zero duration
    /// are returned.
    /// </summary>
    private (string RunId, long DurationMs) DequeueInflight<T>(ConsumeContext<T> context) where T : class
    {
        if (context.MessageId.HasValue
            && _inflight.TryRemove(context.MessageId.Value, out var entry))
        {
            long durationMs = Math.Max(0L, Environment.TickCount64 - entry.StartTick);
            return (entry.RunId, durationMs);
        }

        // Fallback: generate a run ID from CorrelationId or a new GUID.
        string fallbackRunId = context.CorrelationId.HasValue
            ? context.CorrelationId.Value.ToString("N")
            : Guid.NewGuid().ToString("N");

        return (fallbackRunId, 0L);
    }

    /// <summary>
    /// Derives a human-readable queue name from the last non-empty path segment
    /// of the receive endpoint's input address, e.g.
    /// <c>rabbitmq://localhost/order-created</c> → <c>order-created</c>.
    /// Falls back to the full absolute path when no segment can be extracted.
    /// </summary>
    private static string ResolveQueueName<T>(ConsumeContext<T> context) where T : class
    {
        try
        {
            Uri address = context.ReceiveContext.InputAddress;
            // Trim trailing slash and take the last segment.
            string path = address.AbsolutePath.TrimEnd('/');
            int lastSlash = path.LastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < path.Length - 1)
                return path[(lastSlash + 1)..];

            return path.Length > 0 ? path : address.Host;
        }
        catch (Exception) // codeql[cs/catch-of-base-type]
        {
            return "default";
        }
    }

}
