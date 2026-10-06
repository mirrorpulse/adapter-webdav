using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Linq;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

internal static class WebDavOperations
{
    public static async Task<string?> RevisionAsync(WebDavWorkerRoot root, string path, CancellationToken token, bool directory = false)
    {
        Uri uri = WebDavUriPolicy.Resolve(root.Endpoint, path, directory || path.Length == 0);
        using var request = new HttpRequestMessage(HttpMethod.Head, uri);
        using HttpResponseMessage response = await root.Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        if (!directory && path.Length > 0 && response.StatusCode is HttpStatusCode.MovedPermanently or HttpStatusCode.PermanentRedirect &&
            response.Headers.Location is { } location && new Uri(uri, location) == WebDavUriPolicy.Resolve(root.Endpoint, path, directory: true))
            return await RevisionAsync(root, path, token, directory: true).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        RequireSuccessful(response);
        return ResponseRevision(response);
    }

    internal static string ResponseRevision(HttpResponseMessage response) => response.Headers.ETag?.ToString() ??
        "metadata:" + response.Content.Headers.ContentLength?.ToString(CultureInfo.InvariantCulture) +
        ":" + response.Content.Headers.LastModified?.UtcTicks.ToString(CultureInfo.InvariantCulture);

    public static async Task<object> ListAsync(WebDavWorkerRoot root, AdapterFileAddress address, int size, string? cursor, CancellationToken token)
    {
        if (size is < 1 or > 512) throw new InvalidDataException("InvalidPageSize");
        int offset = 0;
        if (cursor is not null)
        {
            if (cursor.Length > 8192) throw new InvalidDataException("InvalidCursor");
            string[] parts = Encoding.UTF8.GetString(Convert.FromBase64String(cursor)).Split('\0');
            if (parts.Length != 3 || parts[0] != address.RootKey || parts[1] != address.Path ||
                !int.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out offset) || offset < 0)
                throw new InvalidDataException("InvalidCursor");
        }
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), WebDavUriPolicy.Resolve(root.Endpoint, address.Path, directory: true));
        request.Headers.Add("Depth", "1");
        request.Content = new StringContent("<d:propfind xmlns:d=\"DAV:\"><d:prop><d:resourcetype/><d:getcontentlength/><d:getetag/><d:getlastmodified/><d:creationdate/></d:prop></d:propfind>", Encoding.UTF8, "application/xml");
        using HttpResponseMessage response = await root.Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        RequireSuccessful(response);
        XDocument document = await WebDavResponseReader.ReadDirectoryAsync(response, token).ConfigureAwait(false);
        XNamespace dav = "DAV:";
        var entries = new List<DirectoryEntry>();
        string prefix = address.Path.Length == 0 ? "" : address.Path + "/";
        foreach (XElement item in document.Descendants(dav + "response"))
        {
            string href = item.Element(dav + "href")?.Value ?? throw new InvalidDataException("InvalidDirectoryResponse");
            string relative = WebDavUriPolicy.RelativeHref(root.Endpoint, href);
            XElement[] successful = item.Elements(dav + "propstat").Where(stat => IsSuccessfulStatus(stat.Element(dav + "status")?.Value)).ToArray();
            if (successful.Length == 0 || item.Elements(dav + "propstat").Any(stat => !IsSuccessfulStatus(stat.Element(dav + "status")?.Value)))
                throw new InvalidDataException("DirectoryEnumerationIncomplete");
            if (!relative.StartsWith(prefix, StringComparison.Ordinal) || relative.Length == prefix.Length || relative[prefix.Length..].Contains('/')) continue;
            var properties = successful.SelectMany(stat => stat.Elements(dav + "prop").Elements()).ToDictionary(element => element.Name, element => element.Value);
            bool directory = successful.SelectMany(stat => stat.Elements(dav + "prop")).Any(prop => prop.Element(dav + "resourcetype")?.Element(dav + "collection") is not null);
            long? length = long.TryParse(properties.GetValueOrDefault(dav + "getcontentlength"), NumberStyles.None, CultureInfo.InvariantCulture, out long parsed) && parsed >= 0 ? parsed : null;
            DateTimeOffset? creation = Date(properties.GetValueOrDefault(dav + "creationdate"));
            DateTimeOffset? modified = Date(properties.GetValueOrDefault(dav + "getlastmodified"));
            string revision = properties.GetValueOrDefault(dav + "getetag")?.Trim() ?? "metadata:" + length?.ToString(CultureInfo.InvariantCulture) + ":" + modified?.UtcTicks.ToString(CultureInfo.InvariantCulture);
            entries.Add(new(relative, revision, directory ? "Directory" : "File", relative, directory ? null : length, creation, modified, false));
        }
        DirectoryEntry[] ordered = entries.OrderBy(item => item.RelativePath, StringComparer.Ordinal).ToArray();
        if (offset > ordered.Length) throw new InvalidDataException("InvalidCursor");
        DirectoryEntry[] page = ordered.Skip(offset).Take(size).ToArray();
        bool complete = offset + page.Length >= ordered.Length;
        return new
        {
            rootKey = address.RootKey,
            entries = page,
            isComplete = complete,
            cursor = complete ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(address.RootKey + "\0" + address.Path + "\0" +
                (offset + page.Length).ToString(CultureInfo.InvariantCulture)))
        };
    }

    public static async Task<string> UploadAsync(WebDavWorkerRoot root, AdapterOperationRequest operation, Stream content, string digest, CancellationToken token)
    {
        var condition = operation.Preconditions ?? new();
        if (condition.ExpectedRevision is not null) RequireStrongTag(condition.ExpectedRevision);
        string? before = await RevisionAsync(root, operation.Path, token).ConfigureAwait(false);
        if (before != condition.ExpectedRevision || (condition.DestinationMustBeAbsent && before is not null))
            throw new InvalidDataException("RemoteConflict");
        long length = content.Length;
        content.Position = 0;
        using var request = new HttpRequestMessage(HttpMethod.Put, WebDavUriPolicy.Resolve(root.Endpoint, operation.Path)) { Content = new StreamContent(content) };
        if (condition.ExpectedRevision is null) request.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Any);
        else request.Headers.IfMatch.Add(RequireStrongTag(condition.ExpectedRevision));
        try
        {
            using HttpResponseMessage response = await root.Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.PreconditionFailed) throw new InvalidDataException("RemoteConflict");
            if (response.StatusCode == HttpStatusCode.Unauthorized) throw new InvalidDataException("CredentialRejected");
            if (response.StatusCode == HttpStatusCode.Forbidden) throw new InvalidDataException("AccessDenied");
            if (response.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.NoContent or HttpStatusCode.OK))
                throw new InvalidDataException("MutationOutcomeAmbiguous");
            return await WebDavAcceptance.ConfirmAsync(root, operation.Path, response.Headers.ETag?.ToString(),
                new(length, digest), directory: false, token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is HttpRequestException or IOException or OperationCanceledException)
        { throw new InvalidDataException("MutationOutcomeAmbiguous", exception); }
    }

    public static EntityTagHeaderValue RequireStrongTag(string? revision) => EntityTagHeaderValue.TryParse(revision, out EntityTagHeaderValue? value) &&
        !value.IsWeak && value != EntityTagHeaderValue.Any ? value : throw new InvalidDataException("CapabilityUnavailable");

    private static bool IsSuccessfulStatus(string? value) => value?.Split(' ', StringSplitOptions.RemoveEmptyEntries) is { Length: >= 2 } parts && parts[1] == "200";
    private static DateTimeOffset? Date(string? value) => DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out DateTimeOffset parsed) ? parsed : null;
    private static void RequireSuccessful(HttpResponseMessage response)
    {
        if (response.StatusCode == HttpStatusCode.Unauthorized) throw new InvalidDataException("CredentialRejected");
        if (response.StatusCode == HttpStatusCode.Forbidden) throw new InvalidDataException("AccessDenied");
        response.EnsureSuccessStatusCode();
    }
    private sealed record DirectoryEntry(string RemoteId, string RemoteRevision, string ItemKind, string RelativePath,
        long? Length, DateTimeOffset? CreationTime, DateTimeOffset? LastWriteTime, bool IsDeleted);
}
