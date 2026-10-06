using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;

namespace MirrorPulse.Adapter.WebDav.Worker;

/// <summary>Never substitutes a later unverified representation for the accepted mutation.</summary>
internal static class WebDavAcceptance
{
    public sealed record ContentProof(long Length, string Sha256);

    public static async Task<ContentProof> ReadContentAsync(WebDavWorkerRoot root, string path, string revision,
        long? expectedLength, CancellationToken token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, WebDavUriPolicy.Resolve(root.Endpoint, path));
        request.Headers.IfMatch.Add(WebDavOperations.RequireStrongTag(revision));
        request.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
        using HttpResponseMessage response = await root.Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.PreconditionFailed) throw new InvalidDataException("RemoteConflict");
        if (response.StatusCode != HttpStatusCode.OK || response.Headers.ETag?.ToString() != revision ||
            response.Content.Headers.ContentEncoding.Any(value => !value.Equals("identity", StringComparison.OrdinalIgnoreCase)) ||
            response.Content.Headers.ContentLength is not { } length || length < 0 || (expectedLength is not null && length != expectedLength))
            throw new InvalidDataException("ContentProofUnavailable");
        await using Stream input = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            long remaining = length;
            while (remaining > 0)
            {
                int count = await input.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)), token).ConfigureAwait(false);
                if (count == 0) throw new InvalidDataException("ContentProofUnavailable");
                hash.AppendData(buffer, 0, count);
                remaining -= count;
            }
            if (await input.ReadAsync(buffer.AsMemory(0, 1), token).ConfigureAwait(false) != 0)
                throw new InvalidDataException("ContentProofUnavailable");
            return new(length, Convert.ToHexString(hash.GetHashAndReset()));
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }

    public static async Task<string> ConfirmAsync(WebDavWorkerRoot root, string path, string? nativeRevision,
        ContentProof? content, bool directory, CancellationToken token)
    {
        try
        {
            string revision = await WebDavOperations.RevisionAsync(root, path, token, directory).ConfigureAwait(false)
                ?? throw new InvalidDataException("MutationOutcomeAmbiguous");
            WebDavOperations.RequireStrongTag(revision);
            if (EntityTagHeaderValue.TryParse(nativeRevision, out EntityTagHeaderValue? tag) && !tag.IsWeak && tag != EntityTagHeaderValue.Any)
            {
                if (tag.ToString() != revision) throw new InvalidDataException("MutationOutcomeAmbiguous");
            }
            else if (directory || content is null ||
                await ReadContentAsync(root, path, revision, content.Length, token).ConfigureAwait(false) != content)
                throw new InvalidDataException("MutationOutcomeAmbiguous");
            return revision;
        }
        catch (Exception exception) when (exception is HttpRequestException or IOException or InvalidDataException or OperationCanceledException)
        { throw new InvalidDataException("MutationOutcomeAmbiguous", exception); }
    }
}
