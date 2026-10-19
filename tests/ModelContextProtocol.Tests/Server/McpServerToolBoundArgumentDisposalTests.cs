using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ModelContextProtocol.Tests.Server;

/// <summary>
/// Verifies that values returned by binders from <see cref="McpServerToolCreateOptions.ConfigureParameterBinding"/> are
/// disposed when the tool invocation ends.
/// </summary>
public partial class McpServerToolBoundArgumentDisposalTests : ClientServerTestBase
{
    private readonly BindingLog _log = new();

    public McpServerToolBoundArgumentDisposalTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override void ConfigureServices(ServiceCollection services, IMcpServerBuilder mcpServerBuilder)
    {
        services.AddSingleton(_log);
        mcpServerBuilder.WithTools<DisposalTools>(
            static (method, options) => options.ConfigureParameterBinding = BindTrackedValue,
            DisposalJsonContext.Default.Options);
    }

    private static AIFunctionFactoryOptions.ParameterBindingOptions BindTrackedValue(ParameterInfo parameter)
    {
        if (parameter.ParameterType == typeof(TrackedValue))
        {
            return new()
            {
                BindParameter = static (parameter, arguments) =>
                    new TrackedValue(parameter.Name!, Bind(parameter, arguments)),
            };
        }

        if (parameter.ParameterType == typeof(AsyncTrackedValue))
        {
            return new()
            {
                BindParameter = static (parameter, arguments) =>
                    new AsyncTrackedValue(parameter.Name!, Bind(parameter, arguments)),
            };
        }

        return default;
    }

    private static BindingLog Bind(ParameterInfo parameter, AIFunctionArguments arguments)
    {
        if (arguments.TryGetValue(parameter.Name!, out object? value) && value is JsonElement { ValueKind: JsonValueKind.String } element &&
            element.ValueEquals("fail"))
        {
            throw new McpException($"Cannot bind '{parameter.Name}'.");
        }

        BindingLog log = arguments.Services!.GetRequiredService<BindingLog>();
        log.Add($"bound:{parameter.Name}");
        return log;
    }

    [Fact]
    public async Task BoundValues_AreDisposedInReverseOrder_WhenToolSucceeds()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "two_values",
            new Dictionary<string, object?> { ["first"] = "a", ["second"] = "b" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEqual(true, result.IsError);
        Assert.Equal(["bound:first", "bound:second", "invoked", "disposeAsync:second", "dispose:first"], _log.Entries);
    }

    [Fact]
    public async Task BoundValues_AreDisposed_WhenToolThrows()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "throws",
            new Dictionary<string, object?> { ["first"] = "a" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.IsError);
        Assert.Equal(["bound:first", "invoked", "dispose:first"], _log.Entries);
    }

    [Fact]
    public async Task AlreadyBoundValues_AreDisposed_WhenBindingALaterParameterFails()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "two_values",
            new Dictionary<string, object?> { ["first"] = "a", ["second"] = "fail" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.IsError);
        Assert.Equal(["bound:first", "dispose:first"], _log.Entries);
    }

    [Fact]
    public async Task ServicesBoundBySdk_AreNotDisposed()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "with_service",
            new Dictionary<string, object?> { ["first"] = "a" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEqual(true, result.IsError);
        Assert.Equal(["bound:first", "invoked", "dispose:first"], _log.Entries);
        Assert.False(_log.IsDisposed);
    }

    public sealed class BindingLog : IDisposable
    {
        private readonly ConcurrentQueue<string> _entries = new();

        public string[] Entries => [.. _entries];

        public bool IsDisposed { get; private set; }

        public void Add(string entry) => _entries.Enqueue(entry);

        public void Dispose() => IsDisposed = true;
    }

    public sealed class TrackedValue(string name, BindingLog log) : IDisposable
    {
        public void Dispose() => log.Add($"dispose:{name}");
    }

    public sealed class AsyncTrackedValue(string name, BindingLog log) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            log.Add($"disposeAsync:{name}");
            return default;
        }
    }

    [McpServerToolType]
    public sealed class DisposalTools
    {
        [McpServerTool(Name = "two_values")]
        public static string TwoValues(TrackedValue first, AsyncTrackedValue second, BindingLog log)
        {
            log.Add("invoked");
            return "ok";
        }

        [McpServerTool(Name = "throws")]
        public static string Throws(TrackedValue first, BindingLog log)
        {
            log.Add("invoked");
            throw new InvalidOperationException("The tool failed.");
        }

        [McpServerTool(Name = "with_service")]
        public static string WithService(TrackedValue first, BindingLog log)
        {
            log.Add("invoked");
            return "ok";
        }
    }

    [JsonSerializable(typeof(string))]
    [JsonSerializable(typeof(TrackedValue))]
    [JsonSerializable(typeof(AsyncTrackedValue))]
    partial class DisposalJsonContext : JsonSerializerContext;
}
