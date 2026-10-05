using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Xml;
using System.Xml.Linq;

namespace MirrorPulse.Adapter.WebDav.Worker;

/// <summary>Bounds untrusted response bodies before exposing bytes or directory metadata.</summary>
internal static class WebDavResponseReader
{
    internal const int MaximumRangeBytes = 1024 * 1024;
    internal const int MaximumXmlBytes = 4 * 1024 * 1024;
    internal const int MaximumDirectoryEntries = 8192;

    public static async Task<byte[]> ReadRangeAsync(HttpClient client, Uri uri, long offset, int length,
        CancellationToken cancellationToken)
    {
        if (offset < 0 || length is < 0 or > MaximumRangeBytes || offset > long.MaxValue - length)
            throw new InvalidDataException("The requested WebDAV range is invalid.");
        if (length == 0) return [];
        using var headRequest = new HttpRequestMessage(HttpMethod.Head, uri);
        using HttpResponseMessage head = await client.SendAsync(headRequest,
            HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        head.EnsureSuccessStatusCode();
        ValidateEncoding(head);
        long? total = head.Content.Headers.ContentLength;
        if (total is not null && offset >= total)
        {
            if (offset == total) return [];
            throw new InvalidDataException("The requested WebDAV range is past the file.");
        }
        long end = total is null ? offset + length - 1 : Math.Min(offset + length - 1, total.Value - 1);
        EntityTagHeaderValue? expectedTag = head.Headers.ETag;
        DateTimeOffset? expectedModified = head.Content.Headers.LastModified;
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Range = new RangeHeaderValue(offset, end);
        request.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
        if (expectedTag is { IsWeak: false }) request.Headers.IfMatch.Add(expectedTag);
        else if (expectedModified is not null) request.Headers.IfUnmodifiedSince = expectedModified;
        using HttpResponseMessage response = await client.SendAsync(request,
            HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.PreconditionFailed)
            throw new WebDavRevisionConflictException(expectedTag?.ToString(), response.Headers.ETag?.ToString());
        if (response.StatusCode != HttpStatusCode.PartialContent)
            throw new InvalidDataException("The WebDAV server did not honor the requested range.");
        ValidateEncoding(response);
        ContentRangeHeaderValue? range = response.Content.Headers.ContentRange;
        if (range is null || !range.HasRange || !range.HasLength || !range.Unit.Equals("bytes", StringComparison.OrdinalIgnoreCase) ||
            range.From != offset || range.Length <= offset ||
            (total is not null && range.Length != total))
            throw new InvalidDataException("The WebDAV Content-Range does not match the request.");
        long responseTotal = range.Length ?? throw new InvalidDataException("The WebDAV range total is missing.");
        long actualEnd = Math.Min(end, responseTotal - 1);
        if (range.To != actualEnd)
            throw new InvalidDataException("The WebDAV Content-Range end does not match the request.");
        if ((expectedTag is not null && response.Headers.ETag?.ToString() != expectedTag.ToString()) ||
            (expectedTag is null && expectedModified is not null && response.Content.Headers.LastModified != expectedModified))
            throw new WebDavRevisionConflictException(expectedTag?.ToString(), response.Headers.ETag?.ToString());
        int expectedLength = checked((int)(actualEnd - offset + 1));
        byte[] bytes = await ReadBoundedAsync(response, expectedLength, cancellationToken).ConfigureAwait(false);
        if (bytes.Length != expectedLength)
            throw new InvalidDataException("The WebDAV range body is truncated.");
        return bytes;
    }

    public static async Task<XDocument> ReadDirectoryAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        ValidateEncoding(response);
        byte[] bytes = await ReadBoundedAsync(response, MaximumXmlBytes, cancellationToken).ConfigureAwait(false);
        using var input = new MemoryStream(bytes, writable: false);
        using XmlReader reader = XmlReader.Create(input, new XmlReaderSettings
        {
            Async = true,
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            MaxCharactersInDocument = MaximumXmlBytes,
        });
        XDocument document = await XDocument.LoadAsync(reader, LoadOptions.None, cancellationToken).ConfigureAwait(false);
        XNamespace dav = "DAV:";
        if (document.Descendants(dav + "response").Take(MaximumDirectoryEntries + 1).Count() > MaximumDirectoryEntries)
            throw new InvalidDataException("The WebDAV directory exceeds the current entry limit.");
        return document;
    }

    private static void ValidateEncoding(HttpResponseMessage response)
    {
        if (response.Content.Headers.ContentEncoding.Any(value => !value.Equals("identity", StringComparison.OrdinalIgnoreCase)))
            throw new InvalidDataException("Encoded WebDAV range or directory responses are not accepted.");
    }

    private static async Task<byte[]> ReadBoundedAsync(HttpResponseMessage response, int maximumBytes,
        CancellationToken cancellationToken)
    {
        if (response.Content.Headers.ContentLength > maximumBytes)
            throw new InvalidDataException("The WebDAV response exceeds its body limit.");
        await using Stream input = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var output = new MemoryStream(Math.Min(maximumBytes, 64 * 1024));
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (true)
            {
                int count = Math.Min(buffer.Length, maximumBytes - checked((int)output.Length) + 1);
                int read = await input.ReadAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
                if (read == 0) return output.ToArray();
                if (read > maximumBytes - output.Length)
                    throw new InvalidDataException("The WebDAV response exceeds its body limit.");
                await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }
}
