using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ModelContextProtocol.Tests.Server;

public partial class McpServerToolParameterBindingTests : ClientServerTestBase
{
    public McpServerToolParameterBindingTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    protected override void ConfigureServices(ServiceCollection services, IMcpServerBuilder mcpServerBuilder)
    {
        services.AddSingleton(new BindingService());
        mcpServerBuilder.WithTools<BindingTools>(
            static (method, options) => options.ConfigureParameterBinding = BindRelativeDuration,
            ParameterBindingJsonContext.Default.Options);
    }

    private static AIFunctionFactoryOptions.ParameterBindingOptions BindRelativeDuration(ParameterInfo parameter)
    {
        if (parameter.ParameterType != typeof(TimeSpan))
        {
            return default;
        }

        return new()
        {
            BindParameter = static (parameter, arguments) =>
            {
                if (!arguments.TryGetValue(parameter.Name!, out object? value) || value is not JsonElement element)
                {
                    throw new McpException($"Missing argument '{parameter.Name}'.");
                }

                if (element.ValueKind == JsonValueKind.String &&
                    element.GetString() is { Length: > 1 } text &&
                    text[^1] == 'm' &&
                    int.TryParse(text.Substring(0, text.Length - 1), NumberStyles.None, CultureInfo.InvariantCulture, out int minutes) &&
                    minutes > 0)
                {
                    return TimeSpan.FromMinutes(minutes);
                }

                throw new McpException("Invalid duration: use a positive number of minutes followed by 'm', for example 15m.");
            },
        };
    }

    [Fact]
    public async Task ConfigureParameterBinding_BindsParameter_AndKeepsSdkBinding()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "relative_delay",
            new Dictionary<string, object?> { ["delay"] = "15m" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEqual(true, result.IsError);
        var content = Assert.IsType<TextContentBlock>(Assert.Single(result.Content));
        Assert.Equal("900|relative_delay|binding-service", content.Text);
    }

    [Fact]
    public async Task ConfigureParameterBinding_ErrorMessage_IsReturnedToClient()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var result = await client.CallToolAsync(
            "relative_delay",
            new Dictionary<string, object?> { ["delay"] = "00:15:00" },
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.IsError);
        var content = Assert.IsType<TextContentBlock>(Assert.Single(result.Content));
        Assert.Contains("Invalid duration", content.Text);
    }

    [Fact]
    public async Task ConfigureParameterBinding_KeepsAttributeMetadataAndSchema()
    {
        await using McpClient client = await CreateMcpClientForServer();

        var tools = await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken);
        McpClientTool tool = Assert.Single(tools);

        Assert.Equal("Relative delay", tool.ProtocolTool.Title);
        Assert.True(tool.ProtocolTool.Annotations?.ReadOnlyHint);

        JsonElement properties = tool.ProtocolTool.InputSchema.GetProperty("properties");
        Assert.True(properties.TryGetProperty("delay", out _));
        Assert.False(properties.TryGetProperty("context", out _));
        Assert.False(properties.TryGetProperty("service", out _));

        McpServerTool serverTool = Assert.Single(ServiceProvider.GetServices<McpServerTool>());
        Assert.Contains(serverTool.Metadata, m => m is McpServerToolAttribute);
    }

    [Fact]
    public void ConfigureParameterBinding_ReturningDefault_UsesSdkBinding()
    {
        McpServerTool tool = McpServerTool.Create(
            typeof(BindingTools).GetMethod(nameof(BindingTools.RelativeDelay))!,
            target: null,
            new McpServerToolCreateOptions
            {
                Services = ServiceProvider,
                SerializerOptions = ParameterBindingJsonContext.Default.Options,
                ConfigureParameterBinding = static _ => default,
            });

        JsonElement properties = tool.ProtocolTool.InputSchema.GetProperty("properties");
        Assert.True(properties.TryGetProperty("delay", out _));
        Assert.False(properties.TryGetProperty("service", out _));
    }

    public sealed class BindingService
    {
        public string Name => "binding-service";
    }

    [McpServerToolType]
    public sealed class BindingTools
    {
        [McpServerTool(Name = "relative_delay", Title = "Relative delay", ReadOnly = true)]
        public static string RelativeDelay(
            [Description("A relative duration, for example 15m.")] TimeSpan delay,
            RequestContext<CallToolRequestParams> context,
            BindingService service) =>
            $"{delay.TotalSeconds.ToString(CultureInfo.InvariantCulture)}|{context.Params?.Name}|{service.Name}";
    }

    [JsonSerializable(typeof(TimeSpan))]
    [JsonSerializable(typeof(string))]
    partial class ParameterBindingJsonContext : JsonSerializerContext;
}
