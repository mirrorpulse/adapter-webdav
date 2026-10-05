using System.Text;
using System.Text.Json;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

public static class WebDavWorkerProgram
{
    public static async Task<int> RunAsync(IReadOnlyList<string> args,
        CancellationToken cancellationToken = default)
    {
        AdapterWorkerProcessArguments arguments;
        try { arguments = AdapterWorkerProcessArguments.Parse(args); }
        catch (ArgumentException) { return 2; }
        await using AdapterNamedPipeClient pipe = await AdapterNamedPipeClient.ConnectAsync(
            arguments.PipeName, TimeSpan.FromSeconds(15), cancellationToken).ConfigureAwait(false);
        await using var channel = new AdapterControlChannel(pipe, arguments.InstanceId, arguments.WorkerSessionId);
        Guid helloId = Guid.NewGuid();
        var offer = new AdapterHello(new(2, 2), AdapterHandshake.V2Capabilities);
        await channel.SendAsync("Hello", helloId, false, offer, cancellationToken).ConfigureAwait(false);
        try
        {
            AdapterControlFrame frame = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (frame.MessageType == "HandshakeRejected") return 4;
            if (frame.MessageType != "Ready" || !frame.IsResponse || frame.RequestId != helloId)
                throw new InvalidDataException("InvalidReady");
            AdapterReady ready = AdapterProtocolJson.Decode<AdapterReady>(Encoding.UTF8.GetBytes(frame.Payload.GetRawText()));
            AdapterHandshake.ValidateReady(ready, offer);
            channel.SelectProtocol(ready.SelectedVersion);
            string cache = Environment.GetEnvironmentVariable("MP_TRANSFER_CACHE_DIR")
                ?? throw new InvalidDataException("TransferCacheRequired");
            using var roots = await WebDavWorkerRoots.CreateAsync(ready, channel, cancellationToken).ConfigureAwait(false);
            await using var protocol = new WebDavTransferProtocol(channel, arguments, roots, cache);
            await channel.SendAsync("Connected", helloId, false, new { }, cancellationToken).ConfigureAwait(false);
            await protocol.RunAsync(cancellationToken).ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return 0; }
        catch (Exception exception) when (exception is IOException or InvalidDataException or ArgumentException or JsonException or UnauthorizedAccessException or HttpRequestException)
        {
            // Paths and configuration values must not become ordinary diagnostic output.
            string code = exception is UnauthorizedAccessException ? "AccessDenied" : "InvalidConfiguration";
            await channel.SendAsync("Error", helloId, false, new { code }, CancellationToken.None).ConfigureAwait(false);
            return 1;
        }
    }
}
