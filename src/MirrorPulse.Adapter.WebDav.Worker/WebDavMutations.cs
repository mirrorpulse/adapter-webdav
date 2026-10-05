using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

internal static class WebDavMutations
{
    public static async Task<string?> ExecuteAsync(string kind, WebDavWorkerRoot source, AdapterOperationRequest operation,
        WebDavWorkerRoot? destination, CancellationToken token)
    {
        Uri uri = WebDavUriPolicy.Resolve(source.Endpoint, operation.Path, operation.IsDirectory);
        if (operation.Path.Length == 0) throw new InvalidDataException("RootMutationForbidden");
        if (kind == "CreateDirectory")
        {
            using var create = new HttpRequestMessage(new HttpMethod("MKCOL"), uri);
            create.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Any);
            string? accepted = await SendMutationAsync(source.Client, create, token).ConfigureAwait(false);
            return await WebDavAcceptance.ConfirmAsync(source, operation.Path, accepted, null, directory: true, token).ConfigureAwait(false);
        }
        string? expected = operation.Preconditions?.ExpectedRevision;
        EntityTagHeaderValue tag = WebDavOperations.RequireStrongTag(expected);
        if (await WebDavOperations.RevisionAsync(source, operation.Path, token, operation.IsDirectory).ConfigureAwait(false) != expected)
            throw new InvalidDataException("RemoteConflict");
        Uri? target = null;
        if (kind == "Move")
        {
            if (operation.Preconditions?.DestinationMustBeAbsent == false) throw new InvalidDataException("CapabilityUnavailable");
            if (destination is null || source.Endpoint.Scheme != destination.Endpoint.Scheme || source.Endpoint.IdnHost != destination.Endpoint.IdnHost ||
                source.Endpoint.Port != destination.Endpoint.Port || source.Client.DefaultRequestHeaders.Authorization?.ToString() != destination.Client.DefaultRequestHeaders.Authorization?.ToString())
                throw new InvalidDataException("CrossEndpointMoveUnavailable");
            if (operation.DestinationPath is null or "") throw new InvalidDataException("RootMutationForbidden");
            target = WebDavUriPolicy.Resolve(destination.Endpoint, operation.DestinationPath, operation.IsDirectory);
            if (target == uri) throw new InvalidDataException("InvalidRequest");
            if (await WebDavOperations.RevisionAsync(destination, operation.DestinationPath, token, operation.IsDirectory).ConfigureAwait(false) is not null)
                throw new InvalidDataException("DestinationExists");
        }
        WebDavAcceptance.ContentProof? content = kind == "Move" && !operation.IsDirectory
            ? await WebDavAcceptance.ReadContentAsync(source, operation.Path, expected!, null, token).ConfigureAwait(false) : null;
        await using WebDavDirectoryLease? lease = operation.IsDirectory
            ? await WebDavDirectoryLease.AcquireAsync(source.Client, uri, tag, token).ConfigureAwait(false) : null;
        if (operation.IsDirectory)
        {
            JsonElement page = AdapterProtocolJson.ToElement(await WebDavOperations.ListAsync(source,
                new(operation.RootKey, operation.Path), 1, null, token).ConfigureAwait(false));
            if (page.GetProperty("entries").GetArrayLength() != 0 || !page.GetProperty("isComplete").GetBoolean())
                throw new InvalidDataException("DirectoryNotEmpty");
        }
        using var request = new HttpRequestMessage(kind == "Move" ? new HttpMethod("MOVE") : HttpMethod.Delete, uri);
        request.Headers.IfMatch.Add(tag);
        if (lease is not null) request.Headers.TryAddWithoutValidation("If", "(<" + lease.Token + "> [" + tag + "])");
        if (target is not null)
        {
            request.Headers.TryAddWithoutValidation("Destination", target.AbsoluteUri);
            request.Headers.TryAddWithoutValidation("Overwrite", "F");
        }
        if (operation.IsDirectory) request.Headers.TryAddWithoutValidation("Depth", "infinity");
        string? nativeRevision = await SendMutationAsync(source.Client, request, token).ConfigureAwait(false);
        if (kind == "Delete") return null;
        return await WebDavAcceptance.ConfirmAsync(destination!, operation.DestinationPath!, nativeRevision, content, operation.IsDirectory, token).ConfigureAwait(false);
    }

    private static async Task<string?> SendMutationAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
    {
        try
        {
            using HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            if (response.StatusCode is HttpStatusCode.PreconditionFailed or HttpStatusCode.Conflict or HttpStatusCode.MethodNotAllowed)
                throw new InvalidDataException("RemoteConflict");
            if (response.StatusCode == HttpStatusCode.Locked) throw new InvalidDataException("RemoteConflict");
            if (response.StatusCode == HttpStatusCode.Unauthorized) throw new InvalidDataException("CredentialRejected");
            if (response.StatusCode == HttpStatusCode.Forbidden) throw new InvalidDataException("AccessDenied");
            if (response.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.NoContent or HttpStatusCode.OK))
                throw new InvalidDataException("MutationOutcomeAmbiguous");
            return response.Headers.ETag?.ToString();
        }
        catch (Exception exception) when (exception is HttpRequestException or IOException or OperationCanceledException)
        { throw new InvalidDataException("MutationOutcomeAmbiguous", exception); }
    }
}
