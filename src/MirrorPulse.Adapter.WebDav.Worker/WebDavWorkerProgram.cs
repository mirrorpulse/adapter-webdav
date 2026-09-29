using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.Json;
using System.Globalization;
using System.Net;
using System.Text;
using System.Xml.Linq;
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
            string endpointText = config.GetValueOrDefault("endpoint")
                ?? throw new InvalidDataException("The WebDAV endpoint is missing.");
            Uri endpoint = new(endpointText.EndsWith('/') ? endpointText : endpointText + "/");
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
                case "List": await ListAsync(command, cancellationToken); break;
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

    private async Task ListAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString() ?? string.Empty;
        int pageSize = command.Payload.GetProperty("pageSize").GetInt32();
        if (pageSize is < 1 or > 512) throw new InvalidDataException("Page size invalid.");
        int offset = ParseCursor(command.Payload);
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), ResolveDirectory(path));
        request.Headers.Add("Depth", "1");
        request.Content = new StringContent("""
            <?xml version="1.0" encoding="utf-8" ?>
            <d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/><d:getcontentlength/>
            <d:getetag/><d:getlastmodified/><d:creationdate/></d:prop></d:propfind>
            """, Encoding.UTF8, "application/xml");
        using HttpResponseMessage response = await client.SendAsync(request,
            HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        XDocument document = XDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        XNamespace dav = "DAV:";
        string requestedPrefix = path.Trim('/');
        var all = new List<WebDavDirectoryEntry>();
        foreach (XElement responseElement in document.Descendants(dav + "response"))
        {
            string? href = responseElement.Element(dav + "href")?.Value;
            if (string.IsNullOrWhiteSpace(href)) continue;
            Uri itemUri = Uri.TryCreate(href, UriKind.Absolute, out Uri? absolute)
                ? absolute : new Uri(baseUri, href);
            string relative = Uri.UnescapeDataString(baseUri.MakeRelativeUri(itemUri).ToString())
                .Trim('/').Replace('\\', '/');
            if (relative.Length == 0 || !IsImmediateChild(relative, requestedPrefix)) continue;
            XElement? prop = responseElement.Descendants(dav + "prop").FirstOrDefault();
            bool isDirectory = prop?.Element(dav + "resourcetype")?.Element(dav + "collection") is not null;
            long? length = long.TryParse(prop?.Element(dav + "getcontentlength")?.Value,
                NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedLength) ? parsedLength : null;
            DateTimeOffset? creation = ParseDate(prop?.Element(dav + "creationdate")?.Value);
            DateTimeOffset? lastWrite = ParseDate(prop?.Element(dav + "getlastmodified")?.Value);
            string? revision = prop?.Element(dav + "getetag")?.Value?.Trim();
            revision ??= lastWrite?.UtcTicks.ToString(CultureInfo.InvariantCulture);
            revision ??= length?.ToString(CultureInfo.InvariantCulture);
            revision ??= "0";
            all.Add(new(relative, revision, isDirectory ? "Directory" : "File", relative,
                isDirectory ? null : length, creation, lastWrite, false));
        }

        WebDavDirectoryEntry[] ordered = all.OrderBy(item => item.RelativePath,
                StringComparer.OrdinalIgnoreCase).ThenBy(item => item.RelativePath, StringComparer.Ordinal).ToArray();
        if (offset > ordered.Length) throw new InvalidDataException("Cursor is past the directory.");
        WebDavDirectoryEntry[] page = ordered.Skip(offset).Take(pageSize).ToArray();
        int next = offset + page.Length;
        bool complete = next >= ordered.Length;
        await channel.SendAsync("DirectoryPage", command.RequestId, true, new
        {
            entries = page,
            cursor = complete ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(
                next.ToString(CultureInfo.InvariantCulture))),
            isComplete = complete,
        }, cancellationToken);
    }

    private static bool IsImmediateChild(string relative, string parent)
    {
        string prefix = parent.Length == 0 ? string.Empty : parent.TrimEnd('/') + "/";
        if (!relative.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) return false;
        return relative[prefix.Length..].IndexOf('/') < 0;
    }

    private static int ParseCursor(JsonElement payload)
    {
        if (!payload.TryGetProperty("cursor", out JsonElement cursor) ||
            cursor.ValueKind is JsonValueKind.Null || string.IsNullOrEmpty(cursor.GetString())) return 0;
        string text = Encoding.UTF8.GetString(Convert.FromBase64String(cursor.GetString()!));
        return int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int offset) && offset >= 0
            ? offset : throw new InvalidDataException("Cursor invalid.");
    }

    private static DateTimeOffset? ParseDate(string? value) =>
        DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset parsed)
            ? parsed : null;

    private Uri ResolveDirectory(string path) => string.IsNullOrEmpty(path) ? baseUri : Resolve(path);

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
            string? current = await ReadRevisionAsync(destination, cancellationToken);
            if (!string.Equals(expected, current, StringComparison.Ordinal))
                throw new WebDavRevisionConflictException();
            using var put = new HttpRequestMessage(HttpMethod.Put, destination)
            { Content = new StreamContent(File.OpenRead(staged)) };
            if (expected is null)
                put.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Any);
            else if (EntityTagHeaderValue.TryParse(expected, out EntityTagHeaderValue? etag))
                put.Headers.IfMatch.Add(etag);
            using HttpResponseMessage putResponse = await client.SendAsync(put, cancellationToken);
            if (putResponse.StatusCode == HttpStatusCode.PreconditionFailed)
                throw new WebDavRevisionConflictException();
            putResponse.EnsureSuccessStatusCode();
            string? actual = await ReadRevisionAsync(destination, cancellationToken);
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

    private sealed record WebDavDirectoryEntry(
        string RemoteId,
        string RemoteRevision,
        string ItemKind,
        string RelativePath,
        long? Length,
        DateTimeOffset? CreationTime,
        DateTimeOffset? LastWriteTime,
        bool IsDeleted);
}

internal sealed class WebDavRevisionConflictException : IOException;
