using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.Json;
using System.Globalization;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

public static class WebDavWorkerProgram
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.General);

    public static async Task<int> RunAsync(IReadOnlyList<string> args,
        CancellationToken cancellationToken = default)
    {
        AdapterWorkerProcessArguments arguments;
        try { arguments = AdapterWorkerProcessArguments.Parse(args); }
        catch (ArgumentException) { return 2; }
        await using AdapterNamedPipeClient pipe = await AdapterNamedPipeClient.ConnectAsync(
            arguments.PipeName, TimeSpan.FromSeconds(15), cancellationToken).ConfigureAwait(false);
        var channel = new AdapterControlChannel(pipe, arguments.InstanceId, arguments.WorkerSessionId);
        Guid helloId = Guid.NewGuid();
        await channel.SendAsync("Hello", helloId, false, new
        {
            adapterId = "com.mirrorpulse.adapter.webdav",
            minimumProtocolVersion = 1,
            maximumProtocolVersion = 1,
        }, cancellationToken).ConfigureAwait(false);
        try
        {
            AdapterControlFrame ready = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (ready.MessageType != "Ready" || !ready.IsResponse || ready.RequestId != helloId)
                throw new InvalidDataException("The Host did not accept the WebDAV Worker handshake.");
            Dictionary<string, string> config = ready.Payload.Deserialize<Dictionary<string, string>>(Options)
                ?? throw new InvalidDataException("The WebDAV configuration is missing.");
            Uri endpoint = new(config.GetValueOrDefault("endpoint")
                ?? throw new InvalidDataException("The WebDAV endpoint is missing."));
            using var client = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.All });
            string? credentialReference = config.GetValueOrDefault("credentialReference");
            if (!string.IsNullOrWhiteSpace(credentialReference))
            {
                Guid credentialId = Guid.NewGuid();
                await channel.SendAsync("CredentialRequest", credentialId, false,
                    new { referenceId = credentialReference }, cancellationToken).ConfigureAwait(false);
                AdapterControlFrame credential = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (credential.MessageType != "CredentialResponse" || !credential.IsResponse ||
                    credential.RequestId != credentialId)
                    throw new InvalidDataException("The Host did not provide WebDAV credentials.");
                string secret = credential.Payload.GetProperty("secret").GetString()
                    ?? throw new InvalidDataException("The WebDAV credential is empty.");
                string kind = config.GetValueOrDefault("authentication") ?? "Basic";
                if (kind.Equals("Bearer", StringComparison.OrdinalIgnoreCase))
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", secret);
                else
                {
                    string user = config.GetValueOrDefault("username")
                        ?? throw new InvalidDataException("The WebDAV username is missing.");
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                        "Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{secret}")));
                }
            }

            await channel.SendAsync("Connected", helloId, false, new { encrypted = endpoint.Scheme == "https" },
                cancellationToken).ConfigureAwait(false);
            var protocol = new WebDavTransferProtocol(channel, client, endpoint,
                arguments.InstanceId, arguments.WorkerSessionId);
            while (true)
            {
                AdapterControlFrame command = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (command.IsResponse) throw new InvalidDataException("The Host sent an unexpected response.");
                if (command.MessageType == "Stop")
                {
                    await channel.SendAsync("Stopped", command.RequestId, true, new { }, cancellationToken);
                    return 0;
                }
                await protocol.HandleAsync(command, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return 0; }
        catch (Exception exception)
        {
            string code = exception switch
            {
                InvalidDataException or JsonException => "InvalidConfiguration",
                HttpRequestException or IOException => "NetworkUnavailable",
                _ => "ConnectionFailed",
            };
            await channel.SendAsync("Error", helloId, false, new { code }, CancellationToken.None);
            return 1;
        }
    }
}

internal sealed class WebDavTransferProtocol(AdapterControlChannel channel, HttpClient client,
    Uri baseUri, Guid instanceId, Guid sessionId)
{
    private const int MaximumRangeBytes = 1024 * 1024;

    public async Task HandleAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        try
        {
            switch (command.MessageType)
            {
                case "Stat": await StatAsync(command, cancellationToken); break;
                case "ReadRange": await ReadRangeAsync(command, cancellationToken); break;
                case "Upload": await UploadAsync(command, cancellationToken); break;
                default: throw new InvalidDataException("The WebDAV Worker received an unsupported command.");
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            string code = exception is WebDavRevisionConflictException ? "RemoteConflict" :
                exception is InvalidDataException or ArgumentException or JsonException ? "InvalidRequest" :
                "RetryableTransferFailure";
            await channel.SendAsync("OperationError", command.RequestId, true, new { code }, CancellationToken.None);
        }
    }

    private async Task StatAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString() ?? throw new InvalidDataException("Path missing.");
        using var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, Resolve(path)), cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            await channel.SendAsync("StatResult", command.RequestId, true, new { revision = (string?)null }, cancellationToken);
            return;
        }
        response.EnsureSuccessStatusCode();
        await channel.SendAsync("StatResult", command.RequestId, true,
            new { revision = Revision(response), length = response.Content.Headers.ContentLength }, cancellationToken);
    }

    private async Task ReadRangeAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString() ?? throw new InvalidDataException("Path missing.");
        long offset = command.Payload.GetProperty("offset").GetInt64();
        int length = command.Payload.GetProperty("length").GetInt32();
        if (offset < 0 || length is < 0 or > MaximumRangeBytes) throw new InvalidDataException("Range invalid.");
        using var request = new HttpRequestMessage(HttpMethod.Get, Resolve(path));
        request.Headers.Range = new RangeHeaderValue(offset, offset + length - 1);
        using HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        byte[] bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        if (bytes.Length > length || (offset > 0 && response.StatusCode != HttpStatusCode.PartialContent))
            throw new InvalidDataException("The WebDAV server returned an invalid range.");
        Guid streamId = Guid.NewGuid();
        await channel.SendAsync("ReadRangeReady", command.RequestId, true, new { streamId, length = bytes.Length }, cancellationToken);
        await channel.SendChunkAsync(new AdapterBinaryChunk(command.RequestId, instanceId, sessionId,
            streamId, offset, bytes, true), cancellationToken);
    }

    private async Task UploadAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString() ?? throw new InvalidDataException("Path missing.");
        string? expected = command.Payload.GetProperty("expectedRevision").GetString();
        long length = command.Payload.GetProperty("length").GetInt64();
        Guid streamId = command.Payload.GetProperty("streamId").GetGuid();
        if (length < 0 || streamId == Guid.Empty) throw new InvalidDataException("Upload metadata invalid.");
        string cache = Environment.GetEnvironmentVariable("MP_TRANSFER_CACHE_DIR")
            ?? throw new InvalidDataException("Transfer cache missing.");
        Directory.CreateDirectory(cache);
        string staged = Path.Combine(cache, $"webdav-{command.RequestId:N}.tmp");
        await channel.SendAsync("UploadReady", command.RequestId, true, new { streamId }, cancellationToken);
        try
        {
            await using (var output = new FileStream(staged, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024, true))
            {
                long received = 0;
                while (true)
                {
                    AdapterBinaryChunk chunk = await channel.ReadChunkAsync(cancellationToken);
                    if (chunk.RequestId != command.RequestId || chunk.StreamId != streamId || chunk.Offset != received || chunk.Data.Length > length - received)
                        throw new InvalidDataException("Upload chunk invalid.");
                    await output.WriteAsync(chunk.Data, cancellationToken); received += chunk.Data.Length;
                    if (chunk.EndOfStream) { if (received != length) throw new InvalidDataException("Upload truncated."); break; }
                }
            }
            Uri destination = Resolve(path);
            using var put = new HttpRequestMessage(HttpMethod.Put, destination)
            { Content = new StreamContent(File.OpenRead(staged)) };
            using HttpResponseMessage putResponse = await client.SendAsync(put, cancellationToken);
            putResponse.EnsureSuccessStatusCode();
            string? actual = await ReadRevisionAsync(destination, cancellationToken);
            if (expected is not null && !string.Equals(expected, actual, StringComparison.Ordinal)) throw new WebDavRevisionConflictException();
            await channel.SendAsync("UploadComplete", command.RequestId, true, new { revision = actual ?? $"{length}" }, cancellationToken);
        }
        finally { File.Delete(staged); }
    }

    private async Task<string?> ReadRevisionAsync(Uri uri, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, uri), cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode(); return Revision(response);
    }

    private Uri Resolve(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path.StartsWith('/') || path.Contains('?') || path.Contains('#') ||
            path.Replace('\\', '/').Split('/').Any(x => x is "." or "..")) throw new InvalidDataException("Unsafe path.");
        return new Uri(baseUri, path.Replace('\\', '/') );
    }

    private static string? Revision(HttpResponseMessage response) =>
        response.Headers.ETag?.Tag ?? response.Content.Headers.ContentLength?.ToString(CultureInfo.InvariantCulture);
}

internal sealed class WebDavRevisionConflictException : IOException;
