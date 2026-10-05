using System.Text;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Tests;

[TestClass]
public sealed class WebDavWorkerProcessTests
{
    [TestMethod]
    public async Task TwoEndpointsKeepCredentialsIdenticalPathsAndNewUploadsIsolated()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("left"), await session.ReadRangeAsync("left", "same.txt", 4));
        CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("right"), await session.ReadRangeAsync("right", "same.txt", 5));
        byte[] content = new byte[AdapterBinaryChunkV2Codec.MaximumChunkBytes + 17];
        Random.Shared.NextBytes(content);
        Guid operation = Guid.NewGuid();
        Assert.AreEqual("UploadComplete", (await session.UploadAsync("left", "new.bin", content, operation)).MessageType);
        CollectionAssert.AreEqual(content, session.Left.Files["new.bin"]);
        Assert.IsFalse(session.Right.Files.ContainsKey("new.bin"));
        Assert.AreEqual("UploadComplete", (await session.UploadAsync("left", "new.bin", content, operation)).MessageType);
        Assert.AreEqual(1, session.Left.Puts);
        content[0] ^= 1;
        Assert.AreEqual("OperationBindingMismatch", (await session.UploadAsync("left", "new.bin", content, operation)).Payload.GetProperty("code").GetString());
        Assert.AreEqual(1, session.Left.Puts);
        Assert.IsEmpty(Directory.EnumerateFiles(session.Cache));
    }

    [TestMethod]
    public async Task RootAndDirectoryCursorCannotBeReboundAndOfflineHasNoRequests()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        var page = await session.RequestAsync("List", new { rootKey = "left", path = "", pageSize = 1, cursor = (string?)null });
        string cursor = page.Payload.GetProperty("cursor").GetString()!;
        var wrong = await session.RequestAsync("List", new { rootKey = "right", path = "", pageSize = 1, cursor });
        Assert.AreEqual("InvalidCursor", wrong.Payload.GetProperty("code").GetString());
        var wrongPath = await session.RequestAsync("List", new { rootKey = "left", path = "child", pageSize = 1, cursor });
        Assert.AreEqual("InvalidCursor", wrongPath.Payload.GetProperty("code").GetString());
        int requests = session.Left.Requests + session.Right.Requests;
        var unknown = await session.RequestAsync("Stat", new { rootKey = "unknown", path = "same.txt" });
        Assert.AreEqual("UnknownRoot", unknown.Payload.GetProperty("code").GetString());
        var offline = await session.RequestAsync("Stat", new { rootKey = "offline", path = "same.txt" });
        Assert.AreEqual("RootOffline", offline.Payload.GetProperty("code").GetString());
        Assert.AreEqual(requests, session.Left.Requests + session.Right.Requests);
        var escaping = await session.RequestAsync("Stat", new { rootKey = "left", path = "../outside" });
        Assert.AreEqual("OperationError", escaping.MessageType);
        Assert.AreEqual(requests, session.Left.Requests + session.Right.Requests);
    }

    [TestMethod]
    public async Task CancelCleansTemporaryLeaseBeforeAckAndPreservesBothEndpoints()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        Guid request = Guid.NewGuid();
        Guid operation = Guid.NewGuid();
        Guid stream = Guid.NewGuid();
        await session.SendAsync("Upload", request, new
        {
            rootKey = "left",
            path = "canceled.bin",
            operationId = operation,
            streamId = stream,
            length = 2,
            preconditions = new AdapterMutationPreconditions()
        });
        Assert.AreEqual("UploadReady", (await session.ReadAsync()).MessageType);
        await session.SendChunkAsync(request, stream, "left", 0, [1], false);
        Guid cancel = Guid.NewGuid();
        await session.SendAsync("Cancel", cancel, new { rootKey = "left", targetRequestId = request, operationId = operation });
        Assert.AreEqual("Canceled", (await session.ReadAsync()).Payload.GetProperty("code").GetString());
        Assert.AreEqual("CancelAck", (await session.ReadAsync()).MessageType);
        Assert.IsEmpty(Directory.EnumerateFiles(session.Cache));
        Assert.IsFalse(session.Left.Files.ContainsKey("canceled.bin"));
        Assert.AreEqual(0, session.Left.Puts);
        Assert.AreEqual("UploadComplete", (await session.UploadAsync("right", "after.bin", [2])).MessageType);
        Assert.IsFalse(session.Left.Files.ContainsKey("after.bin"));
    }
}
