namespace MirrorPulse.Adapter.WebDav.Worker;

internal sealed class WebDavRevisionConflictException(string? expectedRevision = null, string? actualRevision = null)
    : IOException("The WebDAV source changed before the operation completed.")
{
    public string? ExpectedRevision { get; } = expectedRevision;
    public string? ActualRevision { get; } = actualRevision;
}
