using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using MirrorPulse.Adapter.WebDav.Worker;

namespace MirrorPulse.Adapter.WebDav.Tests;

[TestClass]
public sealed class WebDavUriPolicyTests
{
    private static readonly Uri Endpoint = new("https://example.test/authorized/");

    [TestMethod]
    [DataRow("https://other.test/file")]
    [DataRow("//other.test/file")]
    [DataRow("/outside/file")]
    [DataRow("../file")]
    [DataRow("nested/../../file")]
    [DataRow("%2e%2e/file")]
    [DataRow("%252e%252e/file")]
    [DataRow("nested%2ffile")]
    [DataRow("nested/%255c../file")]
    [DataRow("a//file")]
    public void RequestPathsCannotChangeOriginOrEscapePrefix(string path)
        => Assert.ThrowsExactly<InvalidDataException>(() => WebDavUriPolicy.Resolve(Endpoint, path));

    [TestMethod]
    [DataRow("https://other.test/authorized/file")]
    [DataRow("https://example.test/outside/file")]
    [DataRow("https://example.test/authorized-other/file")]
    [DataRow("/authorized/%2e%2e/file")]
    [DataRow("/authorized/a/%2e%2e/file")]
    [DataRow("/authorized/a/../file")]
    [DataRow("/authorized/a/%2e/file")]
    [DataRow("/authorized/%252e%252e/file")]
    [DataRow("/authorized/nested%2ffile")]
    [DataRow("/authorized/file?query")]
    public void ResponseHrefsCannotIntroduceUnownedRemoteIds(string href)
        => Assert.ThrowsExactly<InvalidDataException>(() => WebDavUriPolicy.RelativeHref(Endpoint, href));

    [TestMethod]
    public void RawUnicodeSpacesAndReservedUriCharactersAreEncodedPerSegment()
    {
        Uri resolved = WebDavUriPolicy.Resolve(Endpoint, "目录/a # &%.txt");
        Assert.AreEqual("https://example.test/authorized/%E7%9B%AE%E5%BD%95/a%20%23%20%26%25.txt", resolved.AbsoluteUri);
        Assert.AreEqual("目录/a # &%.txt", WebDavUriPolicy.RelativeHref(Endpoint, resolved.AbsoluteUri));
        Assert.AreEqual(Endpoint, WebDavUriPolicy.Resolve(Endpoint, string.Empty, directory: true));
        Assert.AreEqual("https://example.test/authorized/a/b/", WebDavUriPolicy.Resolve(Endpoint, "a\\b", directory: true).AbsoluteUri);
    }

    [TestMethod]
    public async Task CrossOriginRedirectCannotReceiveAuthorization()
    {
        using var source = Listener("authorized/");
        using var external = Listener("outside/");
        Uri endpoint = new(source.Prefixes.Single());
        using HttpClient client = WebDavHttpClientFactory.Create(endpoint);
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "fixture-only-marker");
        Task<HttpListenerContext> externalRequest = external.GetContextAsync();
        Task<HttpResponseMessage> response = client.GetAsync(WebDavUriPolicy.Resolve(endpoint, "file"));
        HttpListenerContext request = await source.GetContextAsync().WaitAsync(TimeSpan.FromSeconds(3));
        Assert.AreEqual("Bearer fixture-only-marker", request.Request.Headers["Authorization"]);
        request.Response.StatusCode = 302;
        request.Response.RedirectLocation = new Uri(new Uri(external.Prefixes.Single()), "file").AbsoluteUri;
        request.Response.Close();
        using HttpResponseMessage result = await response.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.AreEqual(HttpStatusCode.Found, result.StatusCode);
        await Assert.ThrowsExactlyAsync<TimeoutException>(async () => await externalRequest.WaitAsync(TimeSpan.FromMilliseconds(200)));
        external.Stop();
        try { await externalRequest; } catch (HttpListenerException) { }
    }

    private static HttpListener Listener(string prefix)
    {
        var socket = new TcpListener(IPAddress.Loopback, 0);
        socket.Start();
        int port = ((IPEndPoint)socket.LocalEndpoint).Port;
        socket.Stop();
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/{prefix}");
        listener.Start();
        return listener;
    }
}
