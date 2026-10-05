# MirrorPulse WebDAV Adapter

This is the official repository for the MirrorPulse WebDAV Adapter.

The repository contains the independently buildable Adapter SDK and a WebDAV protocol Worker. The Worker runs in its own process over the current-user Named Pipe protocol and supports authenticated WebDAV HEAD/GET range reads, conditional staged uploads, and transfer-cache cleanup for x64 and ARM64 packages.

Run `pwsh ./eng/verify.ps1` to validate the SDK and Worker. Signed releases are produced by the repository workflow.

Requests use raw relative path segments, encoded exactly once by the Worker.
Absolute URIs, traversal, encoded aliases and returned hrefs outside the configured
origin/directory are rejected. HTTP redirects are not followed, so authentication
is never forwarded to another origin. Unicode names, spaces and literal URI
characters are encoded per segment. Boundary tests include an actual HTTP redirect
fixture and run in the repository CI.

Licensed under Apache-2.0. See [LICENSE](LICENSE).
