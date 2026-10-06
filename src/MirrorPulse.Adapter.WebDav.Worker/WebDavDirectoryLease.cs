using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Linq;

namespace MirrorPulse.Adapter.WebDav.Worker;

/// <summary>An exclusive infinite-depth server lease protects empty-directory mutations.</summary>
internal sealed class WebDavDirectoryLease(HttpClient client, Uri uri, string value) : IAsyncDisposable
{
    public string Token { get; } = value;

    public static async Task<WebDavDirectoryLease> AcquireAsync(HttpClient client, Uri uri, EntityTagHeaderValue tag, CancellationToken token)
    {
        using var request = new HttpRequestMessage(new HttpMethod("LOCK"), uri);
        request.Headers.IfMatch.Add(tag);
        request.Headers.TryAddWithoutValidation("Depth", "infinity");
        request.Headers.TryAddWithoutValidation("Timeout", "Second-30");
        request.Content = new StringContent("<d:lockinfo xmlns:d=\"DAV:\"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>", Encoding.UTF8, "application/xml");
        using HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.PreconditionFailed) throw new InvalidDataException("RemoteConflict");
        if (response.StatusCode != HttpStatusCode.OK) throw new InvalidDataException("CapabilityUnavailable");
        string[] values = response.Headers.TryGetValues("Lock-Token", out IEnumerable<string>? headers) ? headers.ToArray() : [];
        string value = values.Length == 1 && values[0].StartsWith('<') && values[0].EndsWith('>') ? values[0][1..^1] : "";
        if (value.Length is < 1 or > 1024 || value.Any(character => char.IsWhiteSpace(character) || char.IsControl(character) || character is '<' or '>') ||
            !Uri.TryCreate(value, UriKind.Absolute, out _)) throw new InvalidDataException("CapabilityUnavailable");
        var lease = new WebDavDirectoryLease(client, uri, value);
        try
        {
            XDocument document = await WebDavResponseReader.ReadDirectoryAsync(response, token).ConfigureAwait(false);
            XNamespace dav = "DAV:";
            XElement[] active = document.Descendants(dav + "activelock").ToArray();
            if (active.Length != 1 || active[0].Element(dav + "lockscope")?.Element(dav + "exclusive") is null ||
                active[0].Element(dav + "locktype")?.Element(dav + "write") is null || active[0].Element(dav + "depth")?.Value != "infinity" ||
                !Uri.TryCreate(uri, active[0].Element(dav + "lockroot")?.Element(dav + "href")?.Value, out Uri? grantedRoot) || grantedRoot != uri ||
                active[0].Element(dav + "locktoken")?.Element(dav + "href")?.Value != value ||
                active[0].Element(dav + "timeout")?.Value is not { } timeout || !timeout.StartsWith("Second-", StringComparison.Ordinal) ||
                !int.TryParse(timeout[7..], NumberStyles.None, CultureInfo.InvariantCulture, out int seconds) || seconds is < 1 or > 60)
                throw new InvalidDataException("CapabilityUnavailable");
            return lease;
        }
        catch { await lease.DisposeAsync().ConfigureAwait(false); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        using var request = new HttpRequestMessage(new HttpMethod("UNLOCK"), uri);
        request.Headers.TryAddWithoutValidation("Lock-Token", "<" + Token + ">");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            using HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token).ConfigureAwait(false);
            if (response.StatusCode is not (HttpStatusCode.NoContent or HttpStatusCode.NotFound or HttpStatusCode.Conflict))
                throw new InvalidDataException("MutationOutcomeAmbiguous");
        }
        catch (Exception exception) when (exception is HttpRequestException or IOException or OperationCanceledException)
        { throw new InvalidDataException("MutationOutcomeAmbiguous", exception); }
    }
}
