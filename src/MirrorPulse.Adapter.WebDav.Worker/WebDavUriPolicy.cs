namespace MirrorPulse.Adapter.WebDav.Worker;

/// <summary>Confines every request and returned href to the configured WebDAV origin and prefix.</summary>
internal static class WebDavUriPolicy
{
    public static void ValidateBase(Uri endpoint)
    {
        if (!endpoint.IsAbsoluteUri || endpoint.Scheme is not ("http" or "https") ||
            endpoint.UserInfo.Length != 0 || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0 ||
            !endpoint.AbsolutePath.EndsWith('/'))
            throw new InvalidDataException("The WebDAV endpoint must be an HTTP directory without embedded credentials, query or fragment.");
    }

    public static Uri Resolve(Uri endpoint, string path, bool directory = false)
    {
        ValidateBase(endpoint);
        if (path.Length == 0 && directory) return endpoint;
        string portable = path.Replace('\\', '/');
        if (directory) portable = portable.TrimEnd('/');
        string[] segments = portable.Split('/');
        foreach (string segment in segments) ValidateSegment(segment);
        string relative = string.Join('/', segments.Select(Uri.EscapeDataString));
        Uri result = new(endpoint, relative + (directory ? "/" : string.Empty));
        ValidateBoundary(endpoint, result);
        return result;
    }

    public static string RelativeHref(Uri endpoint, string href)
    {
        ValidateBase(endpoint);
        foreach (string rawSegment in href.Split('/'))
        {
            string decoded = Uri.UnescapeDataString(rawSegment);
            if (decoded is "." or ".." || decoded.Contains('/') || decoded.Contains('\\') || HasEncodedEscape(decoded))
                throw new InvalidDataException("The WebDAV href contains a noncanonical segment.");
        }
        if (!Uri.TryCreate(endpoint, href, out Uri? item)) throw new InvalidDataException("The WebDAV href is invalid.");
        ValidateBoundary(endpoint, item);
        string relative = item.AbsolutePath[endpoint.AbsolutePath.Length..].TrimEnd('/');
        if (relative.Length == 0) return string.Empty;
        string[] segments = relative.Split('/').Select(Uri.UnescapeDataString).ToArray();
        foreach (string segment in segments) ValidateSegment(segment);
        return string.Join('/', segments);
    }

    private static void ValidateSegment(string segment)
    {
        if (string.IsNullOrWhiteSpace(segment) || segment is "." or ".." ||
            segment.Any(character => char.IsControl(character) || character is ':' or '/' or '\\'))
            throw new InvalidDataException("The WebDAV path must contain canonical relative segments.");
        if (HasEncodedEscape(segment))
            throw new InvalidDataException("Pre-encoded WebDAV path segments are not accepted.");
    }

    private static bool HasEncodedEscape(string segment)
    {
        for (int index = 0; index + 2 < segment.Length; index++)
        {
            if (segment[index] == '%' && Uri.IsHexDigit(segment[index + 1]) && Uri.IsHexDigit(segment[index + 2]))
                return true;
        }
        return false;
    }

    private static void ValidateBoundary(Uri endpoint, Uri item)
    {
        if (item.Scheme != endpoint.Scheme || !string.Equals(item.IdnHost, endpoint.IdnHost, StringComparison.OrdinalIgnoreCase) ||
            item.Port != endpoint.Port || item.UserInfo.Length != 0 || item.Query.Length != 0 || item.Fragment.Length != 0 ||
            !item.AbsolutePath.StartsWith(endpoint.AbsolutePath, StringComparison.Ordinal))
            throw new InvalidDataException("The WebDAV URI is outside the authorized origin or prefix.");
    }
}

internal static class WebDavHttpClientFactory
{
    public static HttpClient Create(Uri endpoint)
    {
        WebDavUriPolicy.ValidateBase(endpoint);
        return new HttpClient(new HttpClientHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = System.Net.DecompressionMethods.None,
        });
    }
}
