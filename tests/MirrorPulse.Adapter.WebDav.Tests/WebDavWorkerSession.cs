using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Pipes;
using System.Text.Json;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Tests;

internal sealed class WebDavWorkerSession : IAsyncDisposable
{
    private readonly NamedPipeServerStream _pipe;
    private readonly Process _process;
    private readonly Guid _instance = Guid.NewGuid();
    private readonly Guid _session = Guid.NewGuid();
    private readonly CancellationTokenSource _deadline = new(TimeSpan.FromSeconds(30));
    private int _protocol = 1;
    public WebDavHttpFixture Left { get; } = new("left", "Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("user-left:secret-left")));
    public WebDavHttpFixture Right { get; } = new("right", "Bearer secret-right");

    private WebDavWorkerSession(string root)
    {
        Root = root;
        Directory.CreateDirectory(root);
        string pipeName = "mp-webdav-test-" + Guid.NewGuid().ToString("N");
        _pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);
        string? configuredWorker = Environment.GetEnvironmentVariable("MP_WEBDAV_TEST_WORKER_EXE");
        string executable = configuredWorker ?? Path.Combine(FindRepository(), "src", "MirrorPulse.Adapter.WebDav.Worker", "bin", "Release",
            "net10.0-windows", "MirrorPulse.Adapter.WebDav.Worker.exe");
        var start = new ProcessStartInfo(executable)
        { UseShellExecute = false, CreateNoWindow = true };
        foreach (string argument in new[] { "--instance-id", _instance.ToString("D"), "--worker-session-id", _session.ToString("D"), "--pipe-name", pipeName })
            start.ArgumentList.Add(argument);
        Cache = Path.Combine(root, "transfers");
        start.Environment["MP_TRANSFER_CACHE_DIR"] = Cache;
        if (configuredWorker is not null)
        {
            string unavailable = Path.Combine(root, "unavailable-runtime");
            Directory.CreateDirectory(unavailable);
            foreach (string name in new[] { "DOTNET_ROOT", "DOTNET_ROOT_X64", "DOTNET_ROOT_ARM64" }) start.Environment[name] = unavailable;
            start.Environment["DOTNET_MULTILEVEL_LOOKUP"] = "0";
            start.Environment["PATH"] = Environment.SystemDirectory;
        }
        _process = Process.Start(start) ?? throw new InvalidOperationException("Worker launch failed.");
    }

    public string Root { get; }
    public string Cache { get; }

    public static async Task<WebDavWorkerSession> StartAsync(Uri? leftEndpoint = null)
    {
        string root = Path.Combine(Path.GetTempPath(), "mp-webdav-v2-" + Guid.NewGuid().ToString("N"));
        var session = new WebDavWorkerSession(root);
        try
        {
            await session._pipe.WaitForConnectionAsync(session._deadline.Token);
            AdapterControlFrame hello = await session.ReadAsync();
            Assert.AreEqual("Hello", hello.MessageType);
            Assert.AreEqual(1, hello.ProtocolVersion);
            Assert.AreEqual(2, AdapterHandshake.Negotiate(AdapterHandshake.ReadHello(hello.Payload), 3).SelectedVersion);
            AdapterRootBinding[] roots = [
                new("left", true, new Dictionary<string, string> { ["endpoint"] = (leftEndpoint ?? session.Left.Endpoint).AbsoluteUri, ["credentialReference"] = "left-credential", ["username"] = "user-left" }),
                new("right", true, new Dictionary<string, string> { ["endpoint"] = session.Right.Endpoint.AbsoluteUri, ["credentialReference"] = "right-credential", ["authentication"] = "Bearer" }),
                new("offline", false, new Dictionary<string, string> { ["endpoint"] = "not-an-endpoint", ["credentialReference"] = "offline-credential" })];
            await session.SendAsync("Ready", hello.RequestId, new AdapterReady(2, AdapterHandshake.V2Capabilities, roots, new Dictionary<string, string>()), response: true);
            session._protocol = 2;
            foreach (string key in new[] { "left", "right" })
            {
                AdapterControlFrame credential = await session.ReadAsync();
                Assert.AreEqual("CredentialRequest", credential.MessageType);
                Assert.AreEqual(key, credential.Payload.GetProperty("rootKey").GetString());
                Assert.AreEqual(key + "-credential", credential.Payload.GetProperty("referenceId").GetString());
                await session.SendAsync("CredentialResponse", credential.RequestId, new { secret = "secret-" + key }, response: true);
            }
            AdapterControlFrame connected = await session.ReadAsync();
            Assert.AreEqual("Connected", connected.MessageType);
            Assert.AreEqual(2, connected.ProtocolVersion);
            Assert.IsFalse(connected.Payload.TryGetProperty("sourceDirectory", out _));
            if (Environment.GetEnvironmentVariable("MP_WEBDAV_TEST_WORKER_EXE") is { } executable)
            {
                string privateRuntime = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(executable)!, "coreclr.dll"));
                ProcessModule[] modules = session._process.Modules.Cast<ProcessModule>().ToArray();
                Assert.AreEqual(privateRuntime, modules.Single(module => module.ModuleName.Equals("coreclr.dll", StringComparison.OrdinalIgnoreCase)).FileName, ignoreCase: true);
            }
            return session;
        }
        catch { await session.DisposeAsync(); throw; }
    }

    public async Task<AdapterControlFrame> RequestAsync(string type, object payload)
    {
        Guid request = Guid.NewGuid();
        await SendAsync(type, request, payload);
        AdapterControlFrame result = await ReadAsync();
        Assert.AreEqual(request, result.RequestId);
        Assert.IsTrue(result.IsResponse);
        JsonElement expected = AdapterProtocolJson.ToElement(payload);
        if (expected.TryGetProperty("operationId", out JsonElement operation))
            Assert.AreEqual(operation.GetGuid(), result.Payload.GetProperty("operationId").GetGuid());
        return result;
    }

    public Task SendAsync(string type, Guid request, object payload, bool response = false) => WriteFrameAsync(AdapterProtocolJson.Encode(
        new AdapterControlFrame(_protocol, type, request, _instance, _session, response, AdapterProtocolJson.ToElement(payload))));

    public Task SendChunkAsync(Guid request, Guid stream, string root, long offset, byte[] bytes, bool last) => WriteFrameAsync(
        AdapterBinaryChunkV2Codec.Encode(new(request, _instance, _session, stream, offset, bytes, last) { RootKey = root }));

    public async Task<AdapterControlFrame> ReadAsync()
    {
        AdapterControlFrame result = AdapterProtocolJson.Decode<AdapterControlFrame>(await ReadFrameAsync());
        Assert.AreEqual(_protocol, result.ProtocolVersion);
        Assert.AreEqual(_instance, result.InstanceId);
        Assert.AreEqual(_session, result.WorkerSessionId);
        return result;
    }

    public async Task<byte[]> ReadRangeAsync(string root, string path, int length)
    {
        AdapterControlFrame ready = await RequestAsync("ReadRange", new { rootKey = root, path, offset = 0, length });
        Assert.AreEqual("ReadRangeReady", ready.MessageType);
        Assert.AreEqual(root, ready.Payload.GetProperty("rootKey").GetString());
        AdapterBinaryChunk chunk = AdapterBinaryChunkV2Codec.Decode(await ReadFrameAsync());
        var binding = new AdapterStreamBinding(ready.RequestId, _instance, _session, ready.Payload.GetProperty("streamId").GetGuid(), root, 0, length);
        binding.Accept(chunk);
        Assert.IsTrue(binding.Completed);
        return chunk.Data.ToArray();
    }

    public async Task<AdapterControlFrame> UploadAsync(string root, string path, byte[] content, Guid? operation = null,
        AdapterMutationPreconditions? preconditions = null)
    {
        Guid request = Guid.NewGuid();
        Guid stream = Guid.NewGuid();
        Guid stableOperation = operation ?? Guid.NewGuid();
        await SendAsync("Upload", request, new
        {
            rootKey = root,
            path,
            operationId = stableOperation,
            streamId = stream,
            length = content.Length,
            preconditions = preconditions ?? new AdapterMutationPreconditions()
        });
        AdapterControlFrame ready = await ReadAsync();
        Assert.AreEqual(stableOperation, ready.Payload.GetProperty("operationId").GetGuid());
        if (ready.MessageType == "OperationError") return ready;
        Assert.AreEqual("UploadReady", ready.MessageType);
        int offset = 0;
        do
        {
            int count = Math.Min(AdapterBinaryChunkV2Codec.MaximumChunkBytes, content.Length - offset);
            await SendChunkAsync(request, stream, root, offset, content.AsSpan(offset, count).ToArray(), offset + count == content.Length);
            offset += count;
        } while (offset < content.Length);
        AdapterControlFrame complete = await ReadAsync();
        Assert.AreEqual(request, complete.RequestId);
        Assert.AreEqual(stableOperation, complete.Payload.GetProperty("operationId").GetGuid());
        return complete;
    }

    private async Task WriteFrameAsync(byte[] bytes)
    {
        byte[] prefix = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(prefix, checked((uint)bytes.Length));
        await _pipe.WriteAsync(prefix, _deadline.Token);
        await _pipe.WriteAsync(bytes, _deadline.Token);
        await _pipe.FlushAsync(_deadline.Token);
    }

    private async Task<byte[]> ReadFrameAsync()
    {
        byte[] prefix = new byte[4];
        await _pipe.ReadExactlyAsync(prefix, _deadline.Token);
        uint length = BinaryPrimitives.ReadUInt32LittleEndian(prefix);
        Assert.IsTrue(length is > 0 and <= 2 * 1024 * 1024);
        byte[] payload = new byte[checked((int)length)];
        await _pipe.ReadExactlyAsync(payload, _deadline.Token);
        return payload;
    }

    private static string FindRepository()
    {
        string? directory = AppContext.BaseDirectory;
        while (directory is not null && !File.Exists(Path.Combine(directory, "eng", "adapter-sdk.lock.json")))
            directory = Directory.GetParent(directory)?.FullName;
        return directory ?? throw new DirectoryNotFoundException("Repository not found.");
    }

    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited) { _process.Kill(entireProcessTree: true); await _process.WaitForExitAsync(); }
        _process.Dispose();
        await Left.DisposeAsync();
        await Right.DisposeAsync();
        await _pipe.DisposeAsync();
        _deadline.Dispose();
        Directory.Delete(Root, recursive: true);
    }
}
