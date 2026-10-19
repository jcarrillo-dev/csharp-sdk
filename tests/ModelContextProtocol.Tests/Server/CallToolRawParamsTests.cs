#if !NET472
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using ModelContextProtocol.Tests.Utils;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ModelContextProtocol.Tests.Server;

/// <summary>
/// Verifies how tools/call parameters are deserialized from the JSON-RPC message the server read, with and without
/// incoming message filters that read or modify <see cref="JsonRpcRequest.Params"/>.
/// </summary>
public sealed class CallToolRawParamsTests : LoggedTest, IAsyncDisposable
{
    private readonly Pipe _clientToServer = new();
    private readonly Pipe _serverToClient = new();
    private readonly CancellationTokenSource _cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
    private readonly Task _serverTask;
    private readonly ServiceProvider _services;
    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;
    private FilterMode _filterMode;

    private enum FilterMode { None, Read, ModifyInPlace, Replace }

    public CallToolRawParamsTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(XunitLoggerProvider));
        services
            .AddMcpServer(options =>
            {
                options.ServerInfo = new Implementation { Name = "raw-params-server", Version = "1.0.0" };
            })
            .WithStreamServerTransport(_clientToServer.Reader.AsStream(), _serverToClient.Writer.AsStream())
            .WithMessageFilters(filters => filters.AddIncomingFilter(next => async (context, cancellationToken) =>
            {
                if (context.JsonRpcMessage is JsonRpcRequest { Method: RequestMethods.ToolsCall } request)
                {
                    switch (_filterMode)
                    {
                        case FilterMode.Read:
                            Assert.NotNull(request.Params?["arguments"]?["value"]);
                            break;

                        case FilterMode.ModifyInPlace:
                            request.Params!["arguments"]!["value"] = "modified";
                            break;

                        case FilterMode.Replace:
                            request.Params = new JsonObject
                            {
                                ["name"] = "raw",
                                ["arguments"] = new JsonObject { ["value"] = "replaced" },
                            };
                            break;
                    }
                }

                await next(context, cancellationToken);
            }))
            .WithTools([
                McpServerTool.Create((JsonElement value) => value.GetRawText(), new() { Name = "raw" }),
            ]);

        _services = services.BuildServiceProvider(validateScopes: true);
        var server = _services.GetRequiredService<McpServer>();
        _serverTask = server.RunAsync(_cts.Token);

        _writer = new StreamWriter(_clientToServer.Writer.AsStream(), new UTF8Encoding(false)) { AutoFlush = true, NewLine = "\n" };
        _reader = new StreamReader(_serverToClient.Reader.AsStream(), Encoding.UTF8);
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _clientToServer.Writer.Complete();
        _serverToClient.Writer.Complete();
        try { await _serverTask; } catch { /* expected on cancellation */ }
        await _services.DisposeAsync();
        _cts.Dispose();
        Dispose();
    }

    [Fact]
    public async Task CallTool_WithoutFilterAccess_ToolSeesArgumentAsSentByClient()
    {
        await InitializeAsync();

        // The argument is escaped differently from how System.Text.Json would write it.
        string text = await CallToolAsync("raw", @"""péA""");

        Assert.Equal(@"""péA""", text);
    }

    [Fact]
    public async Task CallTool_FilterModifiesParamsInPlace_ToolSeesModifiedArgument()
    {
        await InitializeAsync();

        _filterMode = FilterMode.ModifyInPlace;
        string text = await CallToolAsync("raw", @"""original""");

        Assert.Equal(@"""modified""", text);
    }

    [Fact]
    public async Task CallTool_FilterReplacesParams_ToolSeesReplacedArgument()
    {
        await InitializeAsync();

        _filterMode = FilterMode.Replace;
        string text = await CallToolAsync("raw", @"""original""");

        Assert.Equal(@"""replaced""", text);
    }

    [Fact]
    public async Task CallTool_FilterReadsParams_ToolSeesSameValue()
    {
        await InitializeAsync();

        _filterMode = FilterMode.Read;
        string text = await CallToolAsync("raw", @"""p\u00e9\u0041""");

        // The filter exposed the params, so they are written back from the JsonNode before deserializing.
        Assert.Equal(@"""p\u00E9A""", text);
    }

    [Fact]
    public async Task CallTool_NullParams_ReturnsError()
    {
        await InitializeAsync();

        await _writer.WriteLineAsync(@"{""jsonrpc"":""2.0"",""id"":3,""method"":""tools/call"",""params"":null}");
        JsonNode response = await ReadAsync();

        Assert.Equal(3, response["id"]!.GetValue<int>());
        Assert.NotNull(response["error"]);
    }

    private async Task InitializeAsync()
    {
        await _writer.WriteLineAsync(
            @"{""jsonrpc"":""2.0"",""id"":1,""method"":""initialize"",""params"":{""protocolVersion"":""2025-06-18"",""capabilities"":{},""clientInfo"":{""name"":""raw"",""version"":""1.0""}}}");
        JsonNode response = await ReadAsync();
        Assert.NotNull(response["result"]);

        await _writer.WriteLineAsync(@"{""jsonrpc"":""2.0"",""method"":""notifications/initialized""}");
    }

    private async Task<string> CallToolAsync(string name, string rawValue)
    {
        await _writer.WriteLineAsync(
            @"{""jsonrpc"":""2.0"",""id"":2,""method"":""tools/call"",""params"":{""name"":""" + name + @""",""arguments"":{""value"":" + rawValue + "}}}");
        JsonNode response = await ReadAsync();

        JsonNode result = response["result"]!;
        Assert.NotEqual(true, result["isError"]?.GetValue<bool>());
        return result["content"]![0]!["text"]!.GetValue<string>();
    }

    private async Task<JsonNode> ReadAsync()
    {
        var line = await _reader.ReadLineAsync(_cts.Token).ConfigureAwait(false);
        Assert.NotNull(line);
        return JsonNode.Parse(line!)!;
    }
}
#endif
