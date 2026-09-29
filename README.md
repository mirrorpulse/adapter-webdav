# MirrorPulse WebDAV Adapter

This is the official repository for the MirrorPulse WebDAV Adapter.

The repository contains the independently buildable Adapter SDK and a WebDAV protocol Worker. The Worker runs in its own process over the current-user Named Pipe protocol and supports authenticated WebDAV HEAD/GET range reads, conditional staged uploads, and transfer-cache cleanup for x64 and ARM64 packages.

Run `pwsh ./eng/verify.ps1` to validate the SDK and Worker. Signed releases are produced by the repository workflow.

Licensed under Apache-2.0. See [LICENSE](LICENSE).
