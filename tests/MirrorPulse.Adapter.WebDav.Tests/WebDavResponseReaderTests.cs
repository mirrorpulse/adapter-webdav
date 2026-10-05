using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Xml;
using MirrorPulse.Adapter.WebDav.Worker;

namespace MirrorPulse.Adapter.WebDav.Tests;

[TestClass]
public sealed class WebDavResponseReaderTests
{
    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    [DataRow(2)]
    [DataRow(3)]
    [DataRow(4)]
    [DataRow(5)]
    [DataRow(6)]
    public async Task InvalidRangeHeadersRevisionAndBodiesNeverReturnBytes(int mode)
    {
        using var body = new GeneratedStream(mode == 4 ? long.MaxValue : mode == 5 ? 2 : 4);
        using var handler = new FixtureHandler(request =>
        {
            if (request.Method == HttpMethod.Head) return Head();
            Assert.AreEqual("bytes=2-5", request.Headers.Range?.ToString());
            Assert.AreEqual("\"one\"", request.Headers.IfMatch.Single().ToString());
            Assert.AreEqual("identity", request.Headers.AcceptEncoding.Single().Value);
            HttpResponseMessage response = Partial(body);
            if (mode == 0) response.StatusCode = HttpStatusCode.OK;
            if (mode == 1) response.Content.Headers.ContentRange = new ContentRangeHeaderValue(1, 4, 10);
            if (mode == 2) response.Headers.ETag = new EntityTagHeaderValue("\"two\"");
            if (mode == 3) response.Content.Headers.ContentEncoding.Add("gzip");
            if (mode == 6) response.Content.Headers.ContentRange = new ContentRangeHeaderValue(2, 5, 11);
            return response;
        });
        using var client = new HttpClient(handler);
        if (mode == 2)
            await Assert.ThrowsExactlyAsync<WebDavRevisionConflictException>(() => WebDavResponseReader.ReadRangeAsync(client,
                new Uri("https://fixture.test/file"), 2, 4, CancellationToken.None));
        else
            await Assert.ThrowsExactlyAsync<InvalidDataException>(() => WebDavResponseReader.ReadRangeAsync(client,
                new Uri("https://fixture.test/file"), 2, 4, CancellationToken.None));
        Assert.IsLessThanOrEqualTo(5L, body.BytesRead, "A hostile response must consume at most the range budget plus one byte.");
    }

    [TestMethod]
    public async Task HugeChunkedXmlStopsAtBudgetAndEntryLimitIsEnforced()
    {
        using var body = new GeneratedStream(long.MaxValue);
        using var response = new HttpResponseMessage(HttpStatusCode.MultiStatus) { Content = new StreamContent(body) };
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => WebDavResponseReader.ReadDirectoryAsync(response, CancellationToken.None));
        Assert.AreEqual(WebDavResponseReader.MaximumXmlBytes + 1L, body.BytesRead);
        string xml = "<d:multistatus xmlns:d='DAV:'>" + string.Concat(Enumerable.Repeat("<d:response/>",
            WebDavResponseReader.MaximumDirectoryEntries + 1)) + "</d:multistatus>";
        using var entries = new HttpResponseMessage(HttpStatusCode.MultiStatus) { Content = new StringContent(xml) };
        await Assert.ThrowsExactlyAsync<InvalidDataException>(() => WebDavResponseReader.ReadDirectoryAsync(entries, CancellationToken.None));
    }

    [TestMethod]
    public async Task XmlDtdIsRejectedAndStalledBodyHonorsCancellation()
    {
        using var malicious = new HttpResponseMessage(HttpStatusCode.MultiStatus)
        { Content = new StringContent("<!DOCTYPE root [<!ENTITY data SYSTEM 'file:///never-read'>]><root>&data;</root>") };
        await Assert.ThrowsExactlyAsync<XmlException>(() => WebDavResponseReader.ReadDirectoryAsync(malicious, CancellationToken.None));
        using var body = new GeneratedStream(4, stall: true);
        using var handler = new FixtureHandler(request => request.Method == HttpMethod.Head ? Head() : Partial(body));
        using var client = new HttpClient(handler);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAsync<OperationCanceledException>(() => WebDavResponseReader.ReadRangeAsync(client,
            new Uri("https://fixture.test/file"), 2, 4, cancellation.Token));
        Assert.AreEqual(0L, body.BytesRead);
    }

    [TestMethod]
    public async Task RealServerReceivesRevisionConditionAndReturnsExactOffsetBytes()
    {
        var socket = new TcpListener(IPAddress.Loopback, 0);
        socket.Start();
        int port = ((IPEndPoint)socket.LocalEndpoint).Port;
        socket.Stop();
        using var listener = new HttpListener();
        Uri endpoint = new($"http://localhost:{port}/dav/");
        listener.Prefixes.Add(endpoint.AbsoluteUri);
        listener.Start();
        using HttpClient client = WebDavHttpClientFactory.Create(endpoint);
        Task<byte[]> reading = WebDavResponseReader.ReadRangeAsync(client, new Uri(endpoint, "file"), 2, 4, CancellationToken.None);
        for (int index = 0; index < 2; index++)
        {
            HttpListenerContext context = await listener.GetContextAsync().WaitAsync(TimeSpan.FromSeconds(3));
            context.Response.Headers["ETag"] = "\"one\"";
            if (context.Request.HttpMethod == "HEAD") context.Response.ContentLength64 = 10;
            else
            {
                Assert.AreEqual("bytes=2-5", context.Request.Headers["Range"]);
                Assert.AreEqual("\"one\"", context.Request.Headers["If-Match"]);
                context.Response.StatusCode = 206;
                context.Response.Headers["Content-Range"] = "bytes 2-5/10";
                byte[] bytes = Encoding.UTF8.GetBytes("2345");
                context.Response.ContentLength64 = bytes.Length;
                await context.Response.OutputStream.WriteAsync(bytes);
            }
            context.Response.Close();
        }
        CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("2345"), await reading.WaitAsync(TimeSpan.FromSeconds(3)));
    }

    private static HttpResponseMessage Head() => new(HttpStatusCode.OK)
    { Content = new ByteArrayContent(new byte[10]), Headers = { ETag = new EntityTagHeaderValue("\"one\"") } };

    private static HttpResponseMessage Partial(Stream body)
    {
        var response = new HttpResponseMessage(HttpStatusCode.PartialContent)
        { Content = new StreamContent(body), Headers = { ETag = new EntityTagHeaderValue("\"one\"") } };
        response.Content.Headers.ContentRange = new ContentRangeHeaderValue(2, 5, 10);
        return response;
    }

    private sealed class FixtureHandler(Func<HttpRequestMessage, HttpResponseMessage> handle) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(handle(request));
    }

    private sealed class GeneratedStream(long length, bool stall = false) : Stream
    {
        public long BytesRead { get; private set; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (stall) await Task.Delay(Timeout.Infinite, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            int count = (int)Math.Min(buffer.Length, length - BytesRead);
            buffer.Span[..count].Fill((byte)'x');
            BytesRead += count;
            return count;
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
