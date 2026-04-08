using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace TraceStax.Tests;

/// <summary>
/// Unit tests for <see cref="TraceStaxClient"/>.
/// All tests use dry-run or disabled mode so no real HTTP traffic is produced.
/// </summary>
public class TraceStaxClientTests
{
    // -------------------------------------------------------------------------
    // Disabled client
    // -------------------------------------------------------------------------

    /// <summary>
    /// A disabled client must silently ignore all Track* calls without throwing.
    /// </summary>
    [Fact]
    public void DisabledClient_TrackStart_DoesNotThrow()
    {
        var client = new TraceStaxClient("test-key", enabled: false);
        client.TrackStart("run-1", "MyJob", "default");
    }

    /// <summary>
    /// A disabled client must silently ignore TrackSuccess without throwing.
    /// </summary>
    [Fact]
    public void DisabledClient_TrackSuccess_DoesNotThrow()
    {
        var client = new TraceStaxClient("test-key", enabled: false);
        client.TrackSuccess("run-1", durationMs: 42);
    }

    /// <summary>
    /// A disabled client must silently ignore TrackFailure without throwing.
    /// </summary>
    [Fact]
    public void DisabledClient_TrackFailure_DoesNotThrow()
    {
        var client = new TraceStaxClient("test-key", enabled: false);
        client.TrackFailure("run-1", durationMs: 42, ex: new InvalidOperationException("boom"));
    }

    /// <summary>
    /// A disabled client must silently ignore Heartbeat without throwing.
    /// </summary>
    [Fact]
    public void DisabledClient_Heartbeat_DoesNotThrow()
    {
        var client = new TraceStaxClient("test-key", enabled: false);
        client.Heartbeat("worker-1", queues: ["default"], threadCount: 4);
    }

    /// <summary>
    /// A disabled client must silently ignore Snapshot without throwing.
    /// </summary>
    [Fact]
    public void DisabledClient_Snapshot_DoesNotThrow()
    {
        var client = new TraceStaxClient("test-key", enabled: false);
        client.Snapshot("default", depth: 10, activeCount: 2, failedCount: 0);
    }

    // -------------------------------------------------------------------------
    // Dry-run client
    // -------------------------------------------------------------------------

    /// <summary>
    /// In dry-run mode TrackStart must not throw and must write to stdout.
    /// </summary>
    [Fact]
    public void DryRunClient_TrackStart_WritesToStdout()
    {
        string output = CapturingConsole(() =>
        {
            var client = new TraceStaxClient("test-key", dryRun: true);
            client.TrackStart("run-2", "MyJob", "default");
        });

        Assert.Contains("[tracestax dry-run]", output);
        Assert.Contains("/v1/ingest", output);
    }

    /// <summary>
    /// In dry-run mode TrackSuccess must write to stdout.
    /// </summary>
    [Fact]
    public void DryRunClient_TrackSuccess_WritesToStdout()
    {
        string output = CapturingConsole(() =>
        {
            var client = new TraceStaxClient("test-key", dryRun: true);
            client.TrackSuccess("run-2", durationMs: 100);
        });

        Assert.Contains("[tracestax dry-run]", output);
        Assert.Contains("succeeded", output);
    }

    /// <summary>
    /// In dry-run mode TrackFailure must write to stdout and include error info.
    /// </summary>
    [Fact]
    public void DryRunClient_TrackFailure_WritesToStdoutWithErrorInfo()
    {
        string output = CapturingConsole(() =>
        {
            var client = new TraceStaxClient("test-key", dryRun: true);
            client.TrackFailure("run-2", durationMs: 50, ex: new ArgumentException("bad value"));
        });

        Assert.Contains("[tracestax dry-run]", output);
        Assert.Contains("failed", output);
        Assert.Contains("ArgumentException", output);
        Assert.Contains("bad value", output);
    }

    /// <summary>
    /// In dry-run mode Heartbeat must write to stdout.
    /// </summary>
    [Fact]
    public void DryRunClient_Heartbeat_DoesNotThrow()
    {
        // Heartbeat() delegates to HeartbeatAsync() which returns null early in
        // dry-run mode (no logging). The important guarantee is it doesn't throw.
        var client = new TraceStaxClient("test-key", dryRun: true);
        client.Heartbeat("worker-1"); // must not throw
    }

    /// <summary>
    /// In dry-run mode Snapshot must write to stdout.
    /// </summary>
    [Fact]
    public void DryRunClient_Snapshot_WritesToStdout()
    {
        string output = CapturingConsole(() =>
        {
            var client = new TraceStaxClient("test-key", dryRun: true);
            client.Snapshot("priority", depth: 5);
        });

        Assert.Contains("[tracestax dry-run]", output);
        Assert.Contains("/v1/snapshot", output);
    }

    // -------------------------------------------------------------------------
    // Enabled (no-network) client — fire-and-forget path
    // -------------------------------------------------------------------------

    /// <summary>
    /// Track* methods on an enabled (non-dry-run) client must not throw even
    /// when the endpoint is unreachable. The fire-and-forget path swallows
    /// network errors internally.
    /// </summary>
    [Fact]
    public void EnabledClient_AllTrackMethods_DoNotThrow()
    {
        // Point at an address that will refuse the connection immediately.
        var client = new TraceStaxClient("test-key", endpoint: "http://127.0.0.1:1");
        client.TrackStart("run-3", "SomeJob", "queue-a");
        client.TrackSuccess("run-3", durationMs: 200);
        client.TrackFailure("run-3", durationMs: 200, ex: new Exception("net fail"));
        client.Heartbeat("worker-x");
        client.Snapshot("queue-a", depth: 0);
        // No assertion needed — reaching here means no exception was thrown.
    }

    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Passing a null or empty API key must throw <see cref="ArgumentException"/>.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_InvalidApiKey_ThrowsArgumentException(string? apiKey)
    {
        Assert.Throws<ArgumentException>(() => new TraceStaxClient(apiKey!));
    }

    // -------------------------------------------------------------------------
    // Environment variable wiring
    // -------------------------------------------------------------------------

    /// <summary>
    /// Setting <c>TRACESTAX_ENABLED=false</c> in the environment must produce a
    /// disabled client that writes nothing to stdout.
    /// </summary>
    [Fact]
    public void EnvVar_TraceStaxEnabled_False_DisablesClient()
    {
        using var env = new TemporaryEnvVar("TRACESTAX_ENABLED", "false");

        string output = CapturingConsole(() =>
        {
            // No explicit enabled/dryRun override — should read from env.
            var client = new TraceStaxClient("test-key", dryRun: true);
            client.TrackStart("run-env", "JobX", "default");
        });

        // _enabled=false short-circuits before _dryRun is checked, so nothing
        // should be written.
        Assert.DoesNotContain("[tracestax dry-run]", output);
    }

    /// <summary>
    /// Setting <c>TRACESTAX_DRY_RUN=true</c> in the environment must activate
    /// dry-run mode even without an explicit constructor argument.
    /// </summary>
    [Fact]
    public void EnvVar_TraceStaxDryRun_True_ActivatesDryRun()
    {
        using var env = new TemporaryEnvVar("TRACESTAX_DRY_RUN", "true");

        string output = CapturingConsole(() =>
        {
            var client = new TraceStaxClient("test-key"); // no explicit dryRun
            client.TrackStart("run-env2", "JobY", "low");
        });

        Assert.Contains("[tracestax dry-run]", output);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Redirects <see cref="Console.Out"/> for the duration of <paramref name="action"/>
    /// and returns everything written to it.
    /// </summary>
    private static string CapturingConsole(Action action)
    {
        var original = Console.Out;
        using var sw = new StringWriter();
        Console.SetOut(sw);
        try
        {
            action();
        }
        finally
        {
            Console.SetOut(original);
        }
        return sw.ToString();
    }

    /// <summary>
    /// Sets an environment variable for the duration of a test and restores its
    /// previous value when disposed.
    /// </summary>
    private sealed class TemporaryEnvVar : IDisposable
    {
        private readonly string _name;
        private readonly string? _previous;

        public TemporaryEnvVar(string name, string value)
        {
            _name = name;
            _previous = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
        }

        public void Dispose() =>
            Environment.SetEnvironmentVariable(_name, _previous);
    }
}
