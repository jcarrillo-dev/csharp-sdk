using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace ModelContextProtocol.Protocol;

/// <summary>
/// Represents a request message in the JSON-RPC protocol.
/// </summary>
/// <remarks>
/// Requests are messages that require a response from the receiver. Each request includes a unique ID
/// that will be included in the corresponding response message (either a success response or an error).
///
/// The receiver of a request message is expected to execute the specified method with the provided parameters
/// and return either a <see cref="JsonRpcResponse"/> with the result, or a <see cref="JsonRpcError"/>
/// if the method execution fails.
/// </remarks>
public sealed class JsonRpcRequest : JsonRpcMessageWithId
{
    /// <summary>
    /// Gets or sets the name of the method to invoke.
    /// </summary>
    [JsonPropertyName("method")]
    public required string Method { get; set; }

    private JsonNode? _params;
    private JsonElement _rawParams;
    private JsonNodeOptions _rawParamsNodeOptions;
    private bool _isRawParamsCurrent;

    /// <summary>
    /// Gets or sets optional parameters for the method.
    /// </summary>
    [JsonPropertyName("params")]
    public JsonNode? Params
    {
        get
        {
            JsonNode? node = ParamsNode;

            // The caller may modify the returned node, so the raw JSON the request was read from
            // can no longer be assumed to match the parameters.
            _isRawParamsCurrent = false;
            return node;
        }
        set
        {
            _params = value;
            _rawParams = default;
            _isRawParamsCurrent = false;
        }
    }

    /// <summary>
    /// Gets the parameters as a <see cref="JsonNode"/> for SDK code that only reads them.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="Params"/>, this does not prevent <see cref="TryGetRawParams"/> from returning the raw JSON,
    /// so callers must not modify the returned node.
    /// </remarks>
    internal JsonNode? ParamsNode
    {
        get
        {
            if (_params is null && _isRawParamsCurrent)
            {
                _params = CreateNode(_rawParams, _rawParamsNodeOptions);
            }

            return _params;
        }
    }

    /// <summary>
    /// Sets the parameters from the raw JSON the request was read from. The <see cref="JsonNode"/> is created on demand.
    /// </summary>
    internal void SetRawParams(JsonElement rawParams, JsonNodeOptions nodeOptions)
    {
        _params = null;
        _rawParams = rawParams;
        _rawParamsNodeOptions = nodeOptions;
        _isRawParamsCurrent = true;
    }

    /// <summary>
    /// Gets the raw JSON the parameters were read from, if <see cref="Params"/> has not been set or exposed since.
    /// </summary>
    /// <remarks>
    /// Deserializing from the raw JSON avoids writing the <see cref="JsonNode"/> back to UTF-8 and parsing it again.
    /// It returns <see langword="false"/> once anything outside the SDK could have modified the parameters, for example
    /// a message filter that read <see cref="Params"/>.
    /// </remarks>
    internal bool TryGetRawParams(out JsonElement rawParams)
    {
        rawParams = _rawParams;
        return _isRawParamsCurrent;
    }

    /// <summary>
    /// Creates a <see cref="JsonNode"/> backed by <paramref name="element"/>, as deserializing a <see cref="JsonNode"/> does.
    /// </summary>
    internal static JsonNode? CreateNode(JsonElement element, JsonNodeOptions options) =>
        element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Object => JsonObject.Create(element, options),
            JsonValueKind.Array => JsonArray.Create(element, options),
            _ => JsonValue.Create(element, options),
        };

    internal JsonRpcRequest WithId(RequestId id)
    {
        return new JsonRpcRequest
        {
            JsonRpc = JsonRpc,
            Id = id,
            Method = Method,
            Params = Params,
            Context = Context,
        };
    }
}
