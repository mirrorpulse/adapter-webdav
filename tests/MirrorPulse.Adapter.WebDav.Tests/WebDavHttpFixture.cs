using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security;
using System.Security.Cryptography;
using System.Text;

namespace MirrorPulse.Adapter.WebDav.Tests;

internal sealed class WebDavHttpFixture : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly Task _loop;
    private readonly string _authorization;
    private bool _disposed;
    public ConcurrentDictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);
    public int Requests { get; private set; }
    public int Puts { get; private set; }
    public Uri Endpoint { get; }

    public WebDavHttpFixture(string label, string authorization)
    {
        var socket = new TcpListener(IPAddress.Loopback, 0);
        socket.Start();
        int port = ((IPEndPoint)socket.LocalEndpoint).Port;
        socket.Stop();
        Endpoint = new($"http://localhost:{port}/dav/");
        _authorization = authorization;
        Files["same.txt"] = Encoding.UTF8.GetBytes(label);
        Files["second.txt"] = Encoding.UTF8.GetBytes("second-" + label);
        _listener.Prefixes.Add(Endpoint.AbsoluteUri);
        _listener.Start();
        _loop = RunAsync();
    }

    public static string Revision(byte[] bytes) => "\"" + Convert.ToHexString(SHA256.HashData(bytes)) + "\"";

    private async Task RunAsync()
    {
        try
        {
            while (_listener.IsListening)
            {
                HttpListenerContext context = await _listener.GetContextAsync();
                try { await HandleAsync(context); }
                finally { context.Response.Close(); }
            }
        }
        catch (HttpListenerException) when (_disposed) { }
        catch (ObjectDisposedException) when (_disposed) { }
    }

    private async Task HandleAsync(HttpListenerContext context)
    {
        Requests++;
        HttpListenerRequest request = context.Request;
        HttpListenerResponse response = context.Response;
        if (request.Headers["Authorization"] != _authorization) { response.StatusCode = 401; return; }
        string path = Uri.UnescapeDataString(request.Url!.AbsolutePath[Endpoint.AbsolutePath.Length..]);
        if (request.HttpMethod == "PROPFIND")
        {
            string xml = "<d:multistatus xmlns:d=\"DAV:\">" + string.Join("", Files.OrderBy(item => item.Key, StringComparer.Ordinal).Select(pair =>
                "<d:response><d:href>" + SecurityElement.Escape(Endpoint.AbsolutePath + Uri.EscapeDataString(pair.Key)) +
                "</d:href><d:propstat><d:prop><d:getetag>" + SecurityElement.Escape(Revision(pair.Value)) +
                "</d:getetag><d:getcontentlength>" + pair.Value.Length +
                "</d:getcontentlength></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>")) + "</d:multistatus>";
            byte[] body = Encoding.UTF8.GetBytes(xml);
            response.StatusCode = 207;
            response.ContentType = "application/xml";
            response.ContentLength64 = body.Length;
            await response.OutputStream.WriteAsync(body);
            return;
        }
        if (request.HttpMethod == "PUT")
        {
            Assert.AreEqual("*", request.Headers["If-None-Match"]);
            if (Files.ContainsKey(path)) { response.StatusCode = 412; return; }
            using var buffer = new MemoryStream();
            await request.InputStream.CopyToAsync(buffer);
            byte[] bytes = buffer.ToArray();
            if (!Files.TryAdd(path, bytes)) { response.StatusCode = 412; return; }
            Puts++;
            response.StatusCode = 201;
            response.Headers["ETag"] = Revision(bytes);
            return;
        }
        if (!Files.TryGetValue(path, out byte[]? content)) { response.StatusCode = 404; return; }
        string revision = Revision(content);
        response.Headers["ETag"] = revision;
        if (request.HttpMethod == "HEAD") { response.ContentLength64 = content.Length; return; }
        if (request.HttpMethod == "GET")
        {
            if (request.Headers["If-Match"] != revision) { response.StatusCode = 412; return; }
            string[] range = request.Headers["Range"]!["bytes=".Length..].Split('-');
            int first = int.Parse(range[0], System.Globalization.CultureInfo.InvariantCulture);
            int last = int.Parse(range[1], System.Globalization.CultureInfo.InvariantCulture);
            response.StatusCode = 206;
            response.Headers["Content-Range"] = $"bytes {first}-{last}/{content.Length}";
            response.ContentLength64 = last - first + 1;
            await response.OutputStream.WriteAsync(content.AsMemory(first, last - first + 1));
            return;
        }
        response.StatusCode = 405;
    }

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        _listener.Stop();
        _listener.Close();
        await _loop;
    }
}
