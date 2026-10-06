using System.Net.Http.Headers;
using System.Text;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.WebDav.Worker;

internal sealed record WebDavWorkerRoot(string Key, Uri Endpoint, HttpClient Client);

internal sealed class WebDavWorkerRoots : IDisposable
{
    private readonly Dictionary<string, WebDavWorkerRoot?> _roots = new(StringComparer.Ordinal);

    public static async Task<WebDavWorkerRoots> CreateAsync(AdapterReady ready, AdapterControlChannel channel, CancellationToken token)
    {
        if (ready.Roots.Count is < 1 or > 64) throw new InvalidDataException("InvalidRoots");
        var result = new WebDavWorkerRoots();
        try
        {
            foreach (AdapterRootBinding binding in ready.Roots)
            {
                if (!binding.Enabled) { result._roots.Add(binding.RootKey, null); continue; }
                string text = binding.Configuration.GetValueOrDefault("endpoint") ?? throw new InvalidDataException("EndpointRequired");
                var endpoint = new Uri(text.EndsWith('/') ? text : text + "/");
                HttpClient client = WebDavHttpClientFactory.Create(endpoint);
                client.Timeout = TimeSpan.FromSeconds(30);
                result._roots.Add(binding.RootKey, new(binding.RootKey, endpoint, client));
                if (binding.Configuration.GetValueOrDefault("credentialReference") is { Length: > 0 } reference)
                {
                    Guid request = Guid.NewGuid();
                    await channel.SendAsync("CredentialRequest", request, false, new { rootKey = binding.RootKey, referenceId = reference }, token).ConfigureAwait(false);
                    AdapterControlFrame response = await channel.ReadAsync(token).ConfigureAwait(false);
                    if (response.MessageType != "CredentialResponse" || !response.IsResponse || response.RequestId != request)
                        throw new InvalidDataException("InvalidCredentialResponse");
                    string secret = response.Payload.GetProperty("secret").GetString() ?? throw new InvalidDataException("CredentialRequired");
                    string kind = binding.Configuration.GetValueOrDefault("authentication") ?? "Basic";
                    if (kind == "Bearer") client.DefaultRequestHeaders.Authorization = new("Bearer", secret);
                    else if (kind == "Basic")
                    {
                        string user = binding.Configuration.GetValueOrDefault("username") ?? throw new InvalidDataException("UsernameRequired");
                        client.DefaultRequestHeaders.Authorization = new("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + secret)));
                    }
                    else throw new InvalidDataException("UnsupportedAuthentication");
                }
            }
            return result;
        }
        catch { result.Dispose(); throw; }
    }

    public WebDavWorkerRoot Get(string key) => !_roots.TryGetValue(key, out WebDavWorkerRoot? root)
        ? throw new InvalidDataException("UnknownRoot") : root ?? throw new InvalidDataException("RootOffline");

    public void Dispose()
    {
        foreach (WebDavWorkerRoot? root in _roots.Values) root?.Client.Dispose();
    }
}
