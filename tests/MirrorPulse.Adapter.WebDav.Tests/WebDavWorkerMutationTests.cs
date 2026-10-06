using System.Text;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Tests;

[TestClass]
public sealed class WebDavWorkerMutationTests
{
    [TestMethod]
    public async Task ConditionalReplacementRejectsStaleWeakAndConcurrentRevisions()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        string accepted = WebDavHttpFixture.Revision(session.Left.Files["same.txt"]);
        Assert.AreEqual("RemoteConflict", (await session.UploadAsync("left", "same.txt", [1], preconditions: new("\"stale\"", false))).Payload.GetProperty("code").GetString());
        Assert.AreEqual("CapabilityUnavailable", (await session.UploadAsync("left", "same.txt", [1], preconditions: new("W/" + accepted, false))).Payload.GetProperty("code").GetString());
        session.Left.BeforeMutation = () => session.Left.Files["same.txt"] = Encoding.UTF8.GetBytes("external writer");
        Assert.AreEqual("RemoteConflict", (await session.UploadAsync("left", "same.txt", [1], preconditions: new(accepted, false))).Payload.GetProperty("code").GetString());
        Assert.AreEqual("external writer", Encoding.UTF8.GetString(session.Left.Files["same.txt"]));
        string current = WebDavHttpFixture.Revision(session.Left.Files["same.txt"]);
        Assert.AreEqual("UploadComplete", (await session.UploadAsync("left", "same.txt", [2, 3], preconditions: new(current, false))).MessageType);
        CollectionAssert.AreEqual(new byte[] { 2, 3 }, session.Left.Files["same.txt"]);
        Assert.AreEqual("right", Encoding.UTF8.GetString(session.Right.Files["same.txt"]));
        Assert.IsEmpty(Directory.EnumerateFiles(session.Cache));
    }

    [TestMethod]
    public async Task MovePreservesExistingAndConcurrentDestinationsAndBindsReplay()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        string revision = WebDavHttpFixture.Revision(session.Left.Files["same.txt"]);
        Guid operation = Guid.NewGuid();
        var move = new AdapterOperationRequest(operation, "left", "same.txt", "left", "moved.txt", new(revision));
        session.Left.BeforeMutation = () => session.Left.Files["moved.txt"] = [8];
        Assert.AreEqual("RemoteConflict", (await session.RequestAsync("Move", move)).Payload.GetProperty("code").GetString());
        CollectionAssert.AreEqual(new byte[] { 8 }, session.Left.Files["moved.txt"]);
        Assert.IsTrue(session.Left.Files.ContainsKey("same.txt"));
        session.Left.Files.TryRemove("moved.txt", out _);
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("Move", move)).MessageType);
        int requests = session.Left.MutationRequests;
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("Move", move)).MessageType);
        Assert.AreEqual(requests, session.Left.MutationRequests);
        Assert.AreEqual("OperationBindingMismatch", (await session.RequestAsync("Move", move with { DestinationPath = "other.txt" })).Payload.GetProperty("code").GetString());
        Assert.AreEqual("CrossEndpointMoveUnavailable", (await session.RequestAsync("Move", new AdapterOperationRequest(Guid.NewGuid(), "left",
            "moved.txt", "right", "another.txt", new(revision)))).Payload.GetProperty("code").GetString());
        Assert.IsFalse(session.Right.Files.ContainsKey("another.txt"));
    }

    [TestMethod]
    public async Task EmptyDirectoryMutationsUseExclusiveLeaseAndNeverDeleteUnacceptedChildren()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("CreateDirectory", new AdapterCreateDirectoryRequest(Guid.NewGuid(), "left", "empty"))).MessageType);
        Assert.AreEqual("RemoteConflict", (await session.RequestAsync("CreateDirectory", new AdapterCreateDirectoryRequest(Guid.NewGuid(), "left", "empty"))).Payload.GetProperty("code").GetString());
        bool externalAdded = true;
        session.Left.BeforeMutation = () => externalAdded = session.Left.TryExternalAdd("empty/late.txt", [1]);
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("Move", new AdapterOperationRequest(Guid.NewGuid(), "left", "empty", "left",
            "moved", new("\"directory\""), true))).MessageType);
        Assert.IsFalse(externalAdded, "The fixture enforces the granted infinite-depth lease.");
        Assert.IsTrue(session.Left.Directories.ContainsKey("moved"));
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("Delete", new AdapterOperationRequest(Guid.NewGuid(), "left", "moved", Preconditions: new("\"directory\""), IsDirectory: true))).MessageType);
        session.Left.Directories["nonempty"] = true;
        session.Left.Files["nonempty/child.txt"] = [9];
        Assert.AreEqual("DirectoryNotEmpty", (await session.RequestAsync("Delete", new AdapterOperationRequest(Guid.NewGuid(), "left", "nonempty", Preconditions: new("\"directory\""), IsDirectory: true))).Payload.GetProperty("code").GetString());
        Assert.IsTrue(session.Left.Files.ContainsKey("nonempty/child.txt"));
        session.Left.FailedPropstat = true;
        Assert.AreEqual("DirectoryEnumerationIncomplete", (await session.RequestAsync("Delete", new AdapterOperationRequest(Guid.NewGuid(), "left", "nonempty", Preconditions: new("\"directory\""), IsDirectory: true))).Payload.GetProperty("code").GetString());
        session.Left.FailedPropstat = false;
        session.Left.LocksSupported = false;
        Assert.AreEqual("CapabilityUnavailable", (await session.RequestAsync("Delete", new AdapterOperationRequest(Guid.NewGuid(), "left", "nonempty", Preconditions: new("\"directory\""), IsDirectory: true))).Payload.GetProperty("code").GetString());
        Assert.IsEmpty(session.Left.Locks);
        Assert.IsTrue(session.Left.Directories.ContainsKey("nonempty"));
    }

    [TestMethod]
    public async Task PartialAndLostAcknowledgmentsCannotBeBlindlyReplayed()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        var delete = new AdapterOperationRequest(Guid.NewGuid(), "left", "same.txt", Preconditions: new(WebDavHttpFixture.Revision(session.Left.Files["same.txt"])));
        session.Left.PartialMutation = true;
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("Delete", delete)).Payload.GetProperty("code").GetString());
        int requests = session.Left.MutationRequests;
        session.Left.PartialMutation = false;
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("Delete", delete)).Payload.GetProperty("code").GetString());
        Assert.AreEqual(requests, session.Left.MutationRequests);
        Assert.IsTrue(session.Left.Files.ContainsKey("same.txt"));
        session.Left.DropMutationAck = true;
        var lost = delete with { OperationId = Guid.NewGuid() };
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("Delete", lost)).Payload.GetProperty("code").GetString());
        Assert.IsFalse(session.Left.Files.ContainsKey("same.txt"));
        requests = session.Left.MutationRequests;
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("Delete", lost)).Payload.GetProperty("code").GetString());
        Assert.AreEqual(requests, session.Left.MutationRequests);
    }

    [TestMethod]
    public async Task AcceptedUploadCannotBorrowAConcurrentWritersRevision()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        Guid operation = Guid.NewGuid();
        session.Left.AfterMutation = () => session.Left.Files["created.txt"] = [8, 9];
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.UploadAsync("left", "created.txt", [1, 2], operation)).Payload.GetProperty("code").GetString());
        CollectionAssert.AreEqual(new byte[] { 8, 9 }, session.Left.Files["created.txt"]);
        int requests = session.Left.MutationRequests;
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.UploadAsync("left", "created.txt", [1, 2], operation)).Payload.GetProperty("code").GetString());
        Assert.AreEqual(requests, session.Left.MutationRequests);
        Assert.IsEmpty(Directory.EnumerateFiles(session.Cache));
    }

    [TestMethod]
    public async Task MissingUploadTagRequiresBoundedConditionalContentProof()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        session.Left.OmitMutationTag = true;
        byte[] bytes = Enumerable.Range(0, 1024 * 1024 + 19).Select(index => (byte)(index % 251)).ToArray();
        Assert.AreEqual("UploadComplete", (await session.UploadAsync("left", "verified.bin", bytes)).MessageType);
        session.Left.AfterMutation = () => session.Left.Files["competing.bin"] = [8, 9];
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.UploadAsync("left", "competing.bin", [1, 2])).Payload.GetProperty("code").GetString());
        session.Left.OversizedProof = true;
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.UploadAsync("left", "oversized.bin", [1, 2])).Payload.GetProperty("code").GetString());
        Assert.IsEmpty(Directory.EnumerateFiles(session.Cache));
    }

    [TestMethod]
    public async Task MoveAndDirectoryCreationNeedAnAcceptedRevisionProof()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        string revision = WebDavHttpFixture.Revision(session.Left.Files["same.txt"]);
        session.Left.OmitMutationTag = true;
        Assert.AreEqual("MutationComplete", (await session.RequestAsync("Move", new AdapterOperationRequest(Guid.NewGuid(), "left", "same.txt",
            "left", "moved.txt", new(revision)))).MessageType);
        session.Left.AfterMutation = () => session.Left.Files["competing.txt"] = [9];
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("Move", new AdapterOperationRequest(Guid.NewGuid(), "left", "moved.txt",
            "left", "competing.txt", new(revision)))).Payload.GetProperty("code").GetString());
        Guid operation = Guid.NewGuid();
        var create = new AdapterCreateDirectoryRequest(operation, "left", "unproven");
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("CreateDirectory", create)).Payload.GetProperty("code").GetString());
        int requests = session.Left.MutationRequests;
        Assert.IsTrue(session.Left.Directories.ContainsKey("unproven"));
        Assert.AreEqual("MutationOutcomeAmbiguous", (await session.RequestAsync("CreateDirectory", create)).Payload.GetProperty("code").GetString());
        Assert.AreEqual(requests, session.Left.MutationRequests);
    }

    [TestMethod]
    public async Task UnusableLeaseAndFailedCollectionResponseCannotAuthorizeDeletion()
    {
        await using var session = await WebDavWorkerSession.StartAsync();
        session.Left.Directories["empty"] = true;
        var delete = new AdapterOperationRequest(Guid.NewGuid(), "left", "empty", Preconditions: new("\"directory\""), IsDirectory: true);
        session.Left.InvalidLease = true;
        Assert.AreEqual("CapabilityUnavailable", (await session.RequestAsync("Delete", delete)).Payload.GetProperty("code").GetString());
        Assert.IsEmpty(session.Left.Locks);
        Assert.AreEqual(0, session.Left.MutationRequests);
        session.Left.InvalidLease = false;
        session.Left.FailedOwnPropstat = true;
        Assert.AreEqual("DirectoryEnumerationIncomplete", (await session.RequestAsync("Delete", delete)).Payload.GetProperty("code").GetString());
        Assert.IsEmpty(session.Left.Locks);
        Assert.AreEqual(0, session.Left.MutationRequests);
        Assert.IsTrue(session.Left.Directories.ContainsKey("empty"));
    }
}
