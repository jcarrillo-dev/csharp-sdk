using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.AspNetCore.Tests.Utils;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using ModelContextProtocol.Tests.Utils;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ModelContextProtocol.AspNetCore.Tests;

/// <summary>
/// Verifies how tools/call parameters received over Streamable HTTP are deserialized, after the HTTP handler has
/// validated them against the request headers, with and without incoming message filters that read or modify
/// <see cref="JsonRpcRequest.Params"/>.
/// </summary>
public class HttpCallToolRawParamsTests(ITestOutputHelper outputHelper) : KestrelInMemoryTest(outputHelper), IAsyncDisposable
{
    private WebApplication? _app;
    private FilterMode _filterMode;

    private enum FilterMode { None, Read, ModifyInPlace }

    private async Task StartAsync()
    {
        Builder.Services
            .AddMcpServer(options =>
            {
                options.ServerInfo = new Implementation { Name = nameof(HttpCallToolRawParamsTests), Version = "1.0" };
            })
            .WithHttpTransport()
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
                    }
                }

                await next(context, cancellationToken);
            }))
            .WithTools([McpServerTool.Create((JsonElement value) => value.GetRawText(), new() { Name = "raw" })]);

        _app = Builder.Build();
        _app.MapMcp();
        await _app.StartAsync(TestContext.Current.CancellationToken);

        HttpClient.DefaultRequestHeaders.Accept.Add(new("application/json"));
        HttpClient.DefaultRequestHeaders.Accept.Add(new("text/event-stream"));
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.DisposeAsync();
        }
        base.Dispose();
    }

    [Fact]
    public async Task CallTool_WithoutFilterAccess_ToolSeesArgumentAsSentByClient()
    {
        await StartAsync();

        // The argument is escaped differently from how System.Text.Json would write it, so the tool only sees it
        // unchanged if the params were not written back from a JsonNode.
        string text = await CallToolAsync(@"""p\u00e9\u0041""");

        Assert.Equal(@"""p\u00e9\u0041""", text);
    }

    [Fact]
    public async Task CallTool_FilterReadsParams_ToolSeesSameValue()
    {
        await StartAsync();

        _filterMode = FilterMode.Read;
        string text = await CallToolAsync(@"""p\u00e9\u0041""");

        // The filter exposed the params, so they are written back from the JsonNode before deserializing.
        Assert.Equal(@"""p\u00E9A""", text);
    }

    [Fact]
    public async Task CallTool_FilterModifiesParamsInPlace_ToolSeesModifiedArgument()
    {
        await StartAsync();

        _filterMode = FilterMode.ModifyInPlace;
        string text = await CallToolAsync(@"""original""");

        Assert.Equal(@"""modified""", text);
    }

    private async Task<string> CallToolAsync(string rawValue)
    {
        string body =
            @"{""jsonrpc"":""2.0"",""id"":1,""method"":""tools/call"",""params"":{""name"":""raw"",""arguments"":{""value"":" + rawValue + "}," +
            @"""_meta"":{""io.modelcontextprotocol/protocolVersion"":""" + McpProtocolVersions.July2026ProtocolVersion +
            @""",""io.modelcontextprotocol/clientInfo"":{""name"":""raw"",""version"":""1.0""}," +
            @"""io.modelcontextprotocol/clientCapabilities"":{}}}}";

        using var request = new HttpRequestMessage(HttpMethod.Post, "") { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        request.Headers.Add("MCP-Protocol-Version", McpProtocolVersions.July2026ProtocolVersion);
        request.Headers.Add("Mcp-Method", "tools/call");
        request.Headers.Add("Mcp-Name", "raw");
        using var response = await HttpClient.SendAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        JsonNode json = await ReadJsonResponseAsync(response);
        JsonNode result = json["result"]!;
        Assert.NotEqual(true, result["isError"]?.GetValue<bool>());
        return result["content"]![0]!["text"]!.GetValue<string>();
    }

    private static async Task<JsonNode> ReadJsonResponseAsync(HttpResponseMessage response)
    {
        string body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        if (response.Content.Headers.ContentType?.MediaType == "text/event-stream")
        {
            foreach (string line in body.Split('\n'))
            {
                if (line.StartsWith("data:", StringComparison.Ordinal) && line.Substring("data:".Length).Trim() is { Length: > 0 } data)
                {
                    return JsonNode.Parse(data)!;
                }
            }

            throw new InvalidOperationException("SSE response did not contain a JSON data event. Body: " + body);
        }

        return JsonNode.Parse(body)!;
    }
}
