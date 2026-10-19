using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ModelContextProtocol.Server;

/// <summary>
/// Provides an <see cref="ITransport"/> implemented using a pair of input and output streams.
/// </summary>
/// <remarks>
/// The <see cref="StreamServerTransport"/> class implements bidirectional JSON-RPC messaging over arbitrary
/// streams, allowing MCP communication with clients through various I/O channels such as network sockets,
/// memory streams, or pipes.
/// </remarks>
public class StreamServerTransport : TransportBase
{
    private static readonly byte[] s_newlineBytes = "\n"u8.ToArray();

    private readonly ILogger _logger;

    private static ReadOnlySpan<byte> Utf8Preamble => [0xEF, 0xBB, 0xBF];

    private readonly Stream _inputStream;
    private readonly PipeReader _inputReader;
    private readonly Stream _outputStream;

    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _shutdownCts = new();

    private readonly Task _readLoopCompleted;
    private int _disposed = 0;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamServerTransport"/> class with explicit input/output streams.
    /// </summary>
    /// <param name="inputStream">The input <see cref="Stream"/> to use as standard input.</param>
    /// <param name="outputStream">The output <see cref="Stream"/> to use as standard output.</param>
    /// <param name="serverName">Optional name of the server, used for diagnostic purposes, like logging.</param>
    /// <param name="loggerFactory">Optional logger factory used for logging employed by the transport.</param>
    /// <exception cref="ArgumentNullException"><paramref name="inputStream"/> or <paramref name="outputStream"/> is <see langword="null"/>.</exception>
    public StreamServerTransport(Stream inputStream, Stream outputStream, string? serverName = null, ILoggerFactory? loggerFactory = null)
        : base(serverName is not null ? $"Server (stream) ({serverName})" : "Server (stream)", loggerFactory)
    {
        Throw.IfNull(inputStream);
        Throw.IfNull(outputStream);

        _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;

        // Messages are read as UTF-8 bytes and deserialized without materializing each line as a string.
        // The stream is disposed by DisposeAsync, which also unblocks a pending read.
        _inputStream = inputStream;
        _inputReader = PipeReader.Create(inputStream, new StreamPipeReaderOptions(leaveOpen: true));
        _outputStream = outputStream;

        SetConnected();
        _readLoopCompleted = Task.Run(ReadMessagesAsync, _shutdownCts.Token);
    }

    /// <inheritdoc/>
    public override async Task SendMessageAsync(JsonRpcMessage message, CancellationToken cancellationToken = default)
    {
        if (!IsConnected)
        {
            return;
        }

        using var _ = await _sendLock.LockAsync(cancellationToken).ConfigureAwait(false);

        string id = "(no id)";
        if (message is JsonRpcMessageWithId messageWithId)
        {
            id = messageWithId.Id.ToString();
        }

        try
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(message, McpJsonUtilities.JsonContext.Default.JsonRpcMessage);
            LogTransportSendingMessageSensitive(message);
            await _outputStream.WriteAsync(json, cancellationToken).ConfigureAwait(false);
            await _outputStream.WriteAsync(s_newlineBytes, cancellationToken).ConfigureAwait(false);
            await _outputStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogTransportSendFailed(Name, id, ex);
            throw new IOException("Failed to send message.", ex);
        }
    }

    private async Task ReadMessagesAsync()
    {
        CancellationToken shutdownToken = _shutdownCts.Token;
        Exception? error = null;
        try
        {
            LogTransportEnteringReadMessagesLoop(Name);

            bool isStartOfStream = true;
            while (!shutdownToken.IsCancellationRequested)
            {
                ReadResult result = await _inputReader.ReadAsync(shutdownToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                if (isStartOfStream)
                {
                    // Like StreamReader, skip a UTF-8 byte order mark at the start of the stream.
                    if (buffer.Length < Utf8Preamble.Length && !result.IsCompleted && StartsWithPartialPreamble(buffer))
                    {
                        _inputReader.AdvanceTo(buffer.Start, buffer.End);
                        continue;
                    }

                    isStartOfStream = false;
                    if (StartsWithPreamble(buffer))
                    {
                        buffer = buffer.Slice(Utf8Preamble.Length);
                    }
                }

                while (TryReadLine(ref buffer, out ReadOnlySequence<byte> line))
                {
                    await ProcessLineAsync(line, shutdownToken).ConfigureAwait(false);
                }

                if (result.IsCompleted)
                {
                    // Like StreamReader.ReadLine, a final line without a line terminator is still a line.
                    if (!buffer.IsEmpty)
                    {
                        await ProcessLineAsync(buffer, shutdownToken).ConfigureAwait(false);
                    }

                    _inputReader.AdvanceTo(buffer.End);
                    LogTransportEndOfStream(Name);
                    break;
                }

                _inputReader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        catch (OperationCanceledException)
        {
            LogTransportReadMessagesCancelled(Name);
        }
        catch (Exception ex)
        {
            LogTransportReadMessagesFailed(Name, ex);
            error = ex;
        }
        finally
        {
            await _inputReader.CompleteAsync().ConfigureAwait(false);
            SetDisconnected(error);
        }
    }

    private async ValueTask ProcessLineAsync(ReadOnlySequence<byte> line, CancellationToken shutdownToken)
    {
        if (IsWhiteSpace(line))
        {
            return;
        }

        if (Logger.IsEnabled(LogLevel.Trace))
        {
            LogTransportReceivedMessageSensitive(Name, GetString(line));
        }

        JsonRpcMessage? message;
        RequestId failedRequestId = default;
        bool hasFailedRequestId = false;
        try
        {
            message = Deserialize(line, (JsonTypeInfo<JsonRpcMessage>)McpJsonUtilities.DefaultOptions.GetTypeInfo(typeof(JsonRpcMessage)));
        }
        catch (JsonException ex)
        {
            message = null;

            if (Logger.IsEnabled(LogLevel.Trace))
            {
                LogTransportMessageParseFailedSensitive(Name, GetString(line), ex);
            }
            else
            {
                LogTransportMessageParseFailed(Name, ex);
            }

            // Deserializing the full message failed, for example because the params object was nested
            // more deeply than the JSON reader's MaxDepth allows. If the message still carried a request
            // id, reply with a JSON-RPC parse error using that id so the caller's pending request
            // completes instead of hanging until it times out. If no id can be recovered, the message
            // was either a notification or too malformed to correlate, so we just continue reading.
            hasFailedRequestId = TryRecoverRequestId(line, out failedRequestId);
            if (!hasFailedRequestId)
            {
                return;
            }
        }

        // The line is not used after this point, so the caller may release the buffer that backs it.
        if (message is not null)
        {
            await WriteMessageAsync(message, shutdownToken).ConfigureAwait(false);
        }
        else if (hasFailedRequestId)
        {
            var errorResponse = new JsonRpcError
            {
                Id = failedRequestId,
                Error = new JsonRpcErrorDetail
                {
                    Code = (int)McpErrorCode.ParseError,
                    Message = "Failed to parse the JSON-RPC request.",
                },
            };

            try
            {
                await SendMessageAsync(errorResponse, shutdownToken).ConfigureAwait(false);
            }
            catch (Exception sendEx) when (sendEx is not OperationCanceledException)
            {
                // Swallow so a failed error-send does not tear down the read loop. No logging
                // here because SendMessageAsync already logs send failures before it throws.
            }
        }
        else if (Logger.IsEnabled(LogLevel.Trace))
        {
            LogTransportMessageParseUnexpectedTypeSensitive(Name, GetString(line));
        }
    }

    /// <summary>
    /// Deserializes a single JSON-RPC message from a line of UTF-8 bytes.
    /// </summary>
    /// <remarks>
    /// This has the same behavior as <see cref="JsonSerializer.Deserialize(string, JsonTypeInfo)"/>: the reader options
    /// come from the serializer options, and anything other than whitespace after the message is rejected.
    /// </remarks>
    private static JsonRpcMessage? Deserialize(ReadOnlySequence<byte> line, JsonTypeInfo<JsonRpcMessage> typeInfo)
    {
        if (line.IsSingleSegment)
        {
            return JsonSerializer.Deserialize(line.First.Span, typeInfo);
        }

        JsonSerializerOptions options = typeInfo.Options;
        var reader = new Utf8JsonReader(line, new JsonReaderOptions
        {
            AllowTrailingCommas = options.AllowTrailingCommas,
            CommentHandling = options.ReadCommentHandling,
            MaxDepth = options.MaxDepth,
        });

        JsonRpcMessage? message = JsonSerializer.Deserialize(ref reader, typeInfo);

        // Reading past the message either returns false (only whitespace remains) or throws a JsonException.
        if (reader.Read())
        {
            throw new JsonException("The JSON-RPC message is followed by additional data.");
        }

        return message;
    }

    /// <summary>
    /// Splits the next line off <paramref name="buffer"/>, treating '\n', '\r' and "\r\n" as line terminators
    /// like <see cref="TextReader.ReadLine"/> does. A "\r\n" pair yields an additional empty line, which is skipped.
    /// </summary>
    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        long offset = 0;
        foreach (ReadOnlyMemory<byte> segment in buffer)
        {
            int index = segment.Span.IndexOfAny((byte)'\n', (byte)'\r');
            if (index >= 0)
            {
                SequencePosition end = buffer.GetPosition(offset + index);
                line = buffer.Slice(0, end);
                buffer = buffer.Slice(buffer.GetPosition(1, end));
                return true;
            }

            offset += segment.Length;
        }

        line = default;
        return false;
    }

    /// <summary>Gets whether a line has the same meaning as a line for which <see cref="string.IsNullOrWhiteSpace"/> is true.</summary>
    private static bool IsWhiteSpace(ReadOnlySequence<byte> line)
    {
        bool hasNonAscii = false;
        foreach (ReadOnlyMemory<byte> segment in line)
        {
            foreach (byte b in segment.Span)
            {
                if (b >= 0x80)
                {
                    hasNonAscii = true;
                }
                else if (!char.IsWhiteSpace((char)b))
                {
                    return false;
                }
            }
        }

        // Only non-ASCII bytes are left to check; this is rare, so decode them.
        return !hasNonAscii || string.IsNullOrWhiteSpace(GetString(line));
    }

    private static bool StartsWithPreamble(ReadOnlySequence<byte> buffer)
    {
        if (buffer.Length < Utf8Preamble.Length)
        {
            return false;
        }

        Span<byte> start = stackalloc byte[3];
        buffer.Slice(0, Utf8Preamble.Length).CopyTo(start);
        return start.SequenceEqual(Utf8Preamble);
    }

    private static bool StartsWithPartialPreamble(ReadOnlySequence<byte> buffer)
    {
        Span<byte> start = stackalloc byte[3];
        int length = (int)buffer.Length;
        buffer.CopyTo(start);
        return start.Slice(0, length).SequenceEqual(Utf8Preamble.Slice(0, length));
    }

    private static string GetString(ReadOnlySequence<byte> line) => Encoding.UTF8.GetString(line.ToArray());

    /// <summary>
    /// Attempts to recover the JSON-RPC request id from a line that failed full deserialization.
    /// </summary>
    /// <remarks>
    /// This walks only the top-level object looking for an "id" property and skips every other value,
    /// using a large reader depth so a deeply nested "params" value cannot make recovery itself fail.
    /// </remarks>
    private static bool TryRecoverRequestId(ReadOnlySequence<byte> line, out RequestId id)
    {
        id = default;

        try
        {
            var reader = new Utf8JsonReader(line, new JsonReaderOptions
            {
                // Use the maximum reader depth so that an over-nested "params" value cannot make id
                // recovery throw for the same reason the original parse did.
                MaxDepth = int.MaxValue,
            });

            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return false;
            }

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                bool isId = reader.ValueTextEquals("id"u8);

                if (!reader.Read())
                {
                    break;
                }

                if (isId)
                {
                    switch (reader.TokenType)
                    {
                        case JsonTokenType.String:
                            id = new RequestId(reader.GetString()!);
                            return true;

                        case JsonTokenType.Number when reader.TryGetInt64(out long longId):
                            id = new RequestId(longId);
                            return true;

                        default:
                            // An id that is neither a string nor an integer cannot be correlated, so
                            // there is no point sending an error response for it.
                            return false;
                    }
                }

                // Skip the value of any property other than id, including a deeply nested params object.
                reader.Skip();
            }
        }
        catch (Exception)
        {
            // Recovery is best effort. Whether the line was too malformed to locate a top-level id or
            // the reader failed for any other reason, swallow it here and return false. Letting an
            // exception escape would propagate out of the read loop and disconnect the transport,
            // turning a single bad message into a full session teardown.
        }

        return false;
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            LogTransportShuttingDown(Name);

            // Signal to the stdin reading loop to stop.
            await _shutdownCts.CancelAsync().ConfigureAwait(false);
            _shutdownCts.Dispose();

            // Dispose of stdin/out. Cancellation may not be able to wake up operations
            // synchronously blocked in a syscall; we need to forcefully close the handle / file descriptor.
            _inputStream.Dispose();
            _outputStream?.Dispose();

            // Make sure the work has quiesced.
            try
            {
                await _readLoopCompleted.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                LogTransportCleanupReadTaskFailed(Name, ex);
            }
        }
        finally
        {
            SetDisconnected();
            LogTransportShutDown(Name);
        }

        GC.SuppressFinalize(this);
    }
}
