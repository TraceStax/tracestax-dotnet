using Microsoft.Extensions.DependencyInjection;

namespace TraceStax.Extensions;

/// <summary>
/// Extension methods for registering TraceStax services in the .NET dependency
/// injection container.
/// </summary>
public static class TraceStaxServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="TraceStaxClient"/> singleton in the service collection.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="apiKey">TraceStax API key.</param>
    /// <param name="endpoint">
    /// Optional base URL of the TraceStax ingest service.  When <c>null</c> the
    /// default production endpoint (<c>https://ingest.tracestax.com</c>) is used.
    /// </param>
    /// <returns>The same <paramref name="services"/> instance for fluent chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddTraceStax(
    ///     apiKey: builder.Configuration["TraceStax:ApiKey"]!,
    ///     endpoint: builder.Configuration["TraceStax:Endpoint"]);
    /// </code>
    /// </example>
    public static IServiceCollection AddTraceStax(
        this IServiceCollection services,
        string apiKey,
        string? endpoint = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException("API key must not be empty.", nameof(apiKey));

        var client = endpoint is null
            ? new TraceStaxClient(apiKey)
            : new TraceStaxClient(apiKey, endpoint);

        services.AddSingleton(client);
        return services;
    }
}
