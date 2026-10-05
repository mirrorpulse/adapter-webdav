# MirrorPulse WebDAV Adapter

This is the official repository for the MirrorPulse WebDAV Adapter.

The Worker consumes the fixed, hash-verified published `MirrorPulse.Adapter.Sdk`
0.2.1 package and runs over the current-user Named Pipe v2 protocol. Each enabled
root has its own endpoint, HTTP client and Host credential reference. Basic and
Bearer authentication stay scoped to that root; disabled roots do not parse their
endpoint, request credentials or send HTTP requests.

Root-bound listing, stat, range reads, conditional uploads, move, delete, directory
creation, stable session replay and upload cancellation are implemented. Streams
use bounded SDK frames and temporary transfer leases, removed before cancellation
acknowledgment. Previously published v1 assets are unchanged.

Mutations require strong ETags. PUT uses `If-Match` or `If-None-Match: *`; MOVE
also uses `Overwrite: F` and only crosses roots on the same origin with identical
authorization. A competing destination is preserved. An accepted revision must
match the native response ETag; when a file mutation omits that tag, a conditional
GET verifies its exact length and SHA256 using a bounded streaming buffer. A
later writer's unverified HEAD revision is never reported as upload acceptance.

Directory MOVE and DELETE require an exclusive infinite-depth WebDAV write lock,
a complete empty-directory listing, and the lock token plus ETag in the mutation
condition. Nonempty trees, unusable locks and failed `propstat` responses are
refused. Locks are released after the operation and also after validation fails.
MKCOL must provide a strong accepted response ETag: a server that omits it leaves
an ambiguous result for Host reconciliation rather than an invented revision.
These requirements intentionally expose unsupported server capabilities.

Partial HTTP 207 mutation responses, lost acknowledgments and failed acceptance
proofs report `MutationOutcomeAmbiguous`. The Worker retains at most 256 bound
receipts in its current session; identical ambiguous operations are not retried.
The Host owns durable intent and reconciliation across Worker restarts. WebDAV
server conditions and locks provide concurrency protection; they are not a
transaction spanning multiple independent Host operations. See
[RFC 4918](https://www.rfc-editor.org/rfc/rfc4918).

Packages include native x64 and ARM64 apphosts, their private .NET runtime,
dependencies, and runtime license notices. Both payloads are signed in one
immutable inventory. The package has a 256 MiB size boundary. The conformance
profile launches the actual extracted Worker with global runtime discovery
disabled and verifies its loaded `coreclr.dll` path.

Run `pwsh ./eng/verify.ps1` for locked restore, Release builds, complete formatting,
HTTP/URI boundaries and actual Worker process tests with two independently
authenticated disposable HTTP endpoints.

Requests use raw relative path segments, encoded exactly once by the Worker.
Absolute URIs, traversal, encoded aliases and returned hrefs outside the configured
origin/directory are rejected. HTTP redirects are not followed, so authentication
is never forwarded to another origin. Unicode names, spaces and literal URI
characters are encoded per segment. Boundary tests include an actual HTTP redirect
fixture and run in the repository CI.

Range reads require HTTP 206, matching byte offsets and total length, identity
encoding, and a bounded body of at most 1 MiB. The Worker obtains HEAD metadata,
uses a strong ETag condition when available, and compares the GET revision before
returning bytes. Servers that ignore Range are rejected. The current contract checks
each read; it does not pin one revision across separate Host range requests.
PROPFIND is limited to 4 MiB and 8,192 responses, with DTDs disabled. Oversized
directories fail explicitly; Worker pagination does not bypass this server-response
budget.

Licensed under Apache-2.0. See [LICENSE](LICENSE).

## Release governance

The release scripts and pinned staged workflow follow the template at commit
544c594. Version/tag inputs enter scripts through environment data and are
validated before paths or builds are created. Build has no signing secrets;
signing uses the `adapter-signing` environment; publishing alone has write
permission and uses `adapter-release`. Manual dispatch defaults to a verified
signed artifact without publishing a tag or Release.

Run `pwsh ./eng/verify-release.ps1` for hostile input rejection and a dual-RID
package signed with a disposable in-memory key. Production keys are read only
from signing-step environment variables. No private key file is read or exported.
The embedded inventory is verified before upload; MirrorPulse independently
verifies publisher trust at installation.

The repository owner must configure environment reviewers, trusted branch/tag
rules and signing-secret scope. YAML environment names alone do not enforce those
protections. Existing organization secrets remain compatible until that migration.
Source packages now require protocol v2 and include private runtimes for both RIDs.
Previously released v1 packages retain their original identities and payloads.

The release workflow also verifies the newly signed candidate using MirrorPulse
16c6742 and real Local/WebDAV/SMB/FTP/SFTP Host/Worker fixtures on a disposable
runner. It records both source commits and the candidate package hash. Publishing
requires that protocol gate; signed dry-run assets remain unpublished.
