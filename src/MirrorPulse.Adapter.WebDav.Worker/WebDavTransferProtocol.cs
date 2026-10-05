using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

/// <summary>One reader multiplexes control and bounded SDK transfer frames for all authorized roots.</summary>
internal sealed class WebDavTransferProtocol(AdapterControlChannel channel, AdapterWorkerProcessArguments arguments,
    WebDavWorkerRoots roots, string cache) : IAsyncDisposable
{
    private readonly Dictionary<Guid, PendingUpload> _uploads = [];
    private readonly Dictionary<Guid, AcceptedUpload> _accepted = [];
    private readonly Queue<Guid> _acceptedOrder = [];

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            AdapterWorkerFrame frame = await channel.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            if (frame.Chunk is { } chunk)
            {
                await ReceiveAsync(chunk, cancellationToken).ConfigureAwait(false);
                continue;
            }
            AdapterControlFrame command = frame.Control!;
            if (command.IsResponse) throw new InvalidDataException("UnexpectedResponse");
            if (command.MessageType == "Stop")
            {
                await ReplyAsync(command, "Stopped", new { }, cancellationToken).ConfigureAwait(false);
                return;
            }
            try
            {
                if (command.MessageType == "Cancel")
                {
                    await CancelAsync(command, cancellationToken).ConfigureAwait(false);
                    continue;
                }
                AdapterFileAddress address = AdapterProtocolJson.ReadAddress(command.Payload, 2);
                WebDavWorkerRoot paths = roots.Get(address.RootKey);
                switch (command.MessageType)
                {
                    case "Stat":
                        await ReplyAsync(command, "StatResult", new
                        {
                            rootKey = address.RootKey,
                            revision = await WebDavOperations.RevisionAsync(paths, address.Path, cancellationToken).ConfigureAwait(false)
                        }, cancellationToken).ConfigureAwait(false);
                        break;
                    case "List": await ListAsync(command, address, paths, cancellationToken).ConfigureAwait(false); break;
                    case "ReadRange": await ReadAsync(command, address, paths, cancellationToken).ConfigureAwait(false); break;
                    case "Upload": await BeginUploadAsync(command, address, paths, cancellationToken).ConfigureAwait(false); break;
                    case "Move":
                    case "Delete":
                    case "CreateDirectory": await MutateAsync(command, address, paths, cancellationToken).ConfigureAwait(false); break;
                    default: throw new InvalidDataException("CapabilityUnavailable");
                }
            }
            catch (Exception exception) when (IsOperationFailure(exception))
            {
                await ErrorAsync(command, exception, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ListAsync(AdapterControlFrame command, AdapterFileAddress address, WebDavWorkerRoot root, CancellationToken token)
    {
        string? cursor = command.Payload.TryGetProperty("cursor", out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        object page = await WebDavOperations.ListAsync(root, address, command.Payload.GetProperty("pageSize").GetInt32(), cursor, token).ConfigureAwait(false);
        await ReplyAsync(command, "DirectoryPage", page, token).ConfigureAwait(false);
    }
    private async Task ReadAsync(AdapterControlFrame command, AdapterFileAddress address, WebDavWorkerRoot paths, CancellationToken token)
    {
        long offset = command.Payload.GetProperty("offset").GetInt64();
        long length = command.Payload.GetProperty("length").GetInt64();
        if (offset < 0 || length is < 1 or > AdapterBinaryChunkV2Codec.MaximumChunkBytes) throw new InvalidDataException("InvalidRange");
        string? expected = command.Payload.TryGetProperty("expectedRevision", out JsonElement revision) && revision.ValueKind == JsonValueKind.String ? revision.GetString() : null;
        if (expected is not null && await WebDavOperations.RevisionAsync(paths, address.Path, token).ConfigureAwait(false) != expected)
            throw new InvalidDataException("RemoteConflict");
        byte[] bytes = await WebDavResponseReader.ReadRangeAsync(paths.Client, WebDavUriPolicy.Resolve(paths.Endpoint, address.Path), offset, checked((int)length), token).ConfigureAwait(false);
        if (bytes.Length != length) throw new InvalidDataException("InvalidRange");
        Guid stream = Guid.NewGuid();
        await ReplyAsync(command, "ReadRangeReady", new { rootKey = address.RootKey, streamId = stream, length }, token).ConfigureAwait(false);
        await channel.SendChunkAsync(new(command.RequestId, arguments.InstanceId, arguments.WorkerSessionId, stream, offset, bytes, true)
        { RootKey = address.RootKey }, token).ConfigureAwait(false);
    }

    private async Task BeginUploadAsync(AdapterControlFrame command, AdapterFileAddress address, WebDavWorkerRoot paths, CancellationToken token)
    {
        AdapterOperationRequest operation = DecodeOperation(command);
        AdapterProtocolJson.ValidateMutation(operation, requiresDestination: false);
        long length = command.Payload.GetProperty("length").GetInt64();
        Guid stream = command.Payload.GetProperty("streamId").GetGuid();
        if (length < 0 || stream == Guid.Empty || _uploads.Count >= 4) throw new InvalidDataException("UploadLimit");
        if (_uploads.Values.Any(pending => pending.Operation.OperationId == operation.OperationId)) throw new InvalidDataException("OperationInProgress");
        _ = WebDavUriPolicy.Resolve(paths.Endpoint, address.Path);
        if (operation.Preconditions?.ExpectedRevision is { } expected) WebDavOperations.RequireStrongTag(expected);
        string fingerprint = Fingerprint(command.MessageType, operation, length);
        bool replay = _accepted.TryGetValue(operation.OperationId, out AcceptedUpload? accepted);
        if (replay && accepted!.Fingerprint != fingerprint) throw new InvalidDataException("OperationBindingMismatch");
        if (replay && accepted!.Ambiguous) throw new InvalidDataException("MutationOutcomeAmbiguous");
        if (!replay)
        {
            string? current = await WebDavOperations.RevisionAsync(paths, address.Path, token).ConfigureAwait(false);
            AdapterMutationPreconditions conditions = operation.Preconditions ?? new();
            if (current != conditions.ExpectedRevision || (conditions.DestinationMustBeAbsent && current is not null))
                throw new InvalidDataException("RemoteConflict");
        }

        var lease = new AdapterTransferLease(cache);
        try
        {
            _uploads.Add(command.RequestId, new(command, operation, paths, fingerprint, replay,
                new(command.RequestId, arguments.InstanceId, arguments.WorkerSessionId, stream, address.RootKey, 0, length), lease));
        }
        catch { await lease.DisposeAsync().ConfigureAwait(false); throw; }
        await ReplyAsync(command, "UploadReady", new { rootKey = address.RootKey, operationId = operation.OperationId, streamId = stream }, token).ConfigureAwait(false);
    }

    private async Task ReceiveAsync(AdapterBinaryChunk chunk, CancellationToken token)
    {
        if (!_uploads.TryGetValue(chunk.RequestId, out PendingUpload? upload)) throw new InvalidDataException("UnexpectedChunk");
        try
        {
            upload.Binding.Accept(chunk);
            await upload.Lease.Stream.WriteAsync(chunk.Data, token).ConfigureAwait(false);
            if (!upload.Binding.Completed) return;
            upload.Lease.Stream.Position = 0;
            string digest = Convert.ToHexString(await SHA256.HashDataAsync(upload.Lease.Stream, token).ConfigureAwait(false));
            string? revision;
            if (upload.Replay)
            {
                AcceptedUpload accepted = _accepted[upload.Operation.OperationId];
                if (accepted.Digest != digest) throw new InvalidDataException("OperationBindingMismatch");
                revision = accepted.Revision;
            }
            else
            {
                revision = await WebDavOperations.UploadAsync(upload.Paths, upload.Operation, upload.Lease.Stream, digest, token).ConfigureAwait(false);
                _accepted.Add(upload.Operation.OperationId, new(upload.Fingerprint, digest, revision));
                _acceptedOrder.Enqueue(upload.Operation.OperationId);
                if (_acceptedOrder.Count > 256) _accepted.Remove(_acceptedOrder.Dequeue());
            }
            await upload.Lease.DisposeAsync().ConfigureAwait(false);
            await ReplyAsync(upload.Command, "UploadComplete", new
            {
                rootKey = upload.Operation.RootKey,
                operationId = upload.Operation.OperationId,
                revision
            }, token).ConfigureAwait(false);
            _uploads.Remove(chunk.RequestId);
        }
        catch (Exception exception) when (IsOperationFailure(exception))
        {
            _uploads.Remove(chunk.RequestId);
            if (exception is InvalidDataException { Message: "MutationOutcomeAmbiguous" })
                RememberAmbiguous(upload.Operation.OperationId, upload.Fingerprint);
            await upload.Lease.DisposeAsync().ConfigureAwait(false);
            await ErrorAsync(upload.Command, exception, token).ConfigureAwait(false);
        }
    }

    private async Task CancelAsync(AdapterControlFrame cancel, CancellationToken token)
    {
        string root = cancel.Payload.GetProperty("rootKey").GetString() ?? throw new InvalidDataException("RootRequired");
        roots.Get(root);
        Guid target = cancel.Payload.GetProperty("targetRequestId").GetGuid();
        string status = "alreadyCompleted";
        if (_uploads.TryGetValue(target, out PendingUpload? upload))
        {
            if (upload.Operation.RootKey != root) throw new InvalidDataException("CancelRootMismatch");
            if (cancel.Payload.TryGetProperty("operationId", out JsonElement operation) && operation.ValueKind != JsonValueKind.Null &&
                operation.GetGuid() != upload.Operation.OperationId) throw new InvalidDataException("CancelOperationMismatch");
            _uploads.Remove(target);
            await upload.Lease.CancelAsync().ConfigureAwait(false);
            await ErrorAsync(upload.Command, new InvalidDataException("Canceled"), token).ConfigureAwait(false);
            status = "canceled";
        }
        await ReplyAsync(cancel, "CancelAck", new { rootKey = root, targetRequestId = target, status }, token).ConfigureAwait(false);
    }

    private static AdapterOperationRequest DecodeOperation(AdapterControlFrame command) =>
        AdapterProtocolJson.Decode<AdapterOperationRequest>(Encoding.UTF8.GetBytes(command.Payload.GetRawText()));

    private async Task MutateAsync(AdapterControlFrame command, AdapterFileAddress address, WebDavWorkerRoot paths, CancellationToken token)
    {
        AdapterOperationRequest operation;
        if (command.MessageType == "CreateDirectory")
        {
            AdapterCreateDirectoryRequest create = AdapterProtocolJson.Decode<AdapterCreateDirectoryRequest>(Encoding.UTF8.GetBytes(command.Payload.GetRawText()));
            operation = new(create.OperationId, create.RootKey, create.Path, Preconditions: new(null, create.MustBeAbsent), IsDirectory: true);
        }
        else operation = DecodeOperation(command);
        AdapterProtocolJson.ValidateMutation(operation, requiresDestination: command.MessageType == "Move");
        if (address.Path.Length == 0 || operation.DestinationPath?.Length == 0) throw new InvalidDataException("RootMutationForbidden");
        WebDavWorkerRoot? destination = command.MessageType == "Move" ? roots.Get(operation.DestinationRootKey!) : null;
        string fingerprint = Fingerprint(command.MessageType, operation, null);
        string? revision;
        if (_accepted.TryGetValue(operation.OperationId, out AcceptedUpload? accepted))
        {
            if (accepted.Fingerprint != fingerprint) throw new InvalidDataException("OperationBindingMismatch");
            if (accepted.Ambiguous) throw new InvalidDataException("MutationOutcomeAmbiguous");
            revision = accepted.Revision;
        }
        else
        {
            try { revision = await WebDavMutations.ExecuteAsync(command.MessageType, paths, operation, destination, token).ConfigureAwait(false); }
            catch (InvalidDataException exception) when (exception.Message == "MutationOutcomeAmbiguous")
            {
                RememberAmbiguous(operation.OperationId, fingerprint);
                throw;
            }
            _accepted.Add(operation.OperationId, new(fingerprint, null, revision));
            _acceptedOrder.Enqueue(operation.OperationId);
            if (_acceptedOrder.Count > 256) _accepted.Remove(_acceptedOrder.Dequeue());
        }
        await ReplyAsync(command, "MutationComplete", new { rootKey = operation.RootKey, operationId = operation.OperationId, revision }, token).ConfigureAwait(false);
    }

    private static string Fingerprint(string type, AdapterOperationRequest operation, long? length) =>
        Convert.ToHexString(SHA256.HashData(AdapterProtocolJson.Encode(new { type, operation, length })));

    private void RememberAmbiguous(Guid operationId, string fingerprint)
    {
        _accepted.Add(operationId, new(fingerprint, null, null, true));
        _acceptedOrder.Enqueue(operationId);
        if (_acceptedOrder.Count > 256) _accepted.Remove(_acceptedOrder.Dequeue());
    }

    private ValueTask ReplyAsync(AdapterControlFrame command, string type, object payload, CancellationToken token) =>
        channel.SendAsync(type, command.RequestId, true, payload, token);

    private static bool IsOperationFailure(Exception exception) => exception is IOException or InvalidDataException or ArgumentException or JsonException or UnauthorizedAccessException or FormatException or KeyNotFoundException or HttpRequestException or OperationCanceledException;

    private ValueTask ErrorAsync(AdapterControlFrame command, Exception exception, CancellationToken token)
    {
        string[] codes = ["UnknownRoot", "RootOffline", "InvalidCursor", "InvalidPageSize", "InvalidRange", "RemoteConflict",
            "OperationBindingMismatch", "OperationInProgress", "CapabilityUnavailable", "Canceled", "CancelRootMismatch",
            "CancelOperationMismatch", "UploadLimit", "MutationOutcomeAmbiguous", "CredentialRejected", "AccessDenied", "DestinationExists", "CrossEndpointMoveUnavailable", "RootMutationForbidden", "DirectoryNotEmpty", "DirectoryEnumerationIncomplete"];
        string code = exception is InvalidDataException && codes.Contains(exception.Message, StringComparer.Ordinal)
            ? exception.Message : exception is HttpRequestException or IOException or OperationCanceledException ? "RetryableTransferFailure" : "InvalidRequest";
        string? rootKey = command.Payload.TryGetProperty("rootKey", out JsonElement root) && root.ValueKind == JsonValueKind.String ? root.GetString() : null;
        return ReplyAsync(command, "OperationError", new { rootKey, code }, token);
    }
    public async ValueTask DisposeAsync()
    {
        foreach (PendingUpload upload in _uploads.Values) await upload.Lease.DisposeAsync().ConfigureAwait(false);
        _uploads.Clear();
    }

    private sealed record AcceptedUpload(string Fingerprint, string? Digest, string? Revision, bool Ambiguous = false);
    private sealed record PendingUpload(AdapterControlFrame Command, AdapterOperationRequest Operation, WebDavWorkerRoot Paths,
        string Fingerprint, bool Replay, AdapterStreamBinding Binding, AdapterTransferLease Lease);
}
