using MassTransit;

namespace TraceStax.MassTransit;

/// <summary>
/// Extension methods for integrating TraceStax with MassTransit.
/// </summary>
public static class TraceStaxMassTransitExtensions
{
    /// <summary>
    /// Attaches a <see cref="TraceStaxConsumeObserver"/> to the bus control so
    /// that all message consumption events are automatically tracked in TraceStax.
    /// </summary>
    /// <param name="busControl">The MassTransit bus control instance.</param>
    /// <param name="client">Configured <see cref="TraceStaxClient"/> instance.</param>
    /// <returns>
    /// The same <paramref name="busControl"/> instance to allow fluent chaining.
    /// </returns>
    /// <example>
    /// <code>
    /// var busControl = Bus.Factory.CreateUsingRabbitMq(cfg => { ... });
    /// busControl.AddTraceStax(tracestaxClient);
    /// await busControl.StartAsync();
    /// </code>
    /// </example>
    public static IBusControl AddTraceStax(this IBusControl busControl, TraceStaxClient client)
    {
        ArgumentNullException.ThrowIfNull(busControl);
        ArgumentNullException.ThrowIfNull(client);

        busControl.ConnectConsumeObserver(new TraceStaxConsumeObserver(client));
        return busControl;
    }
}
