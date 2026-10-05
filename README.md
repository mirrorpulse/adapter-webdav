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
characters are encoded per segment. HEAD accepts only the exact canonical trailing
slash for the same authorized directory; arbitrary redirects remain refused.
Boundary tests include an actual HTTP redirect
fixture and run in the repository CI.

Range reads require HTTP 206, matching byte offsets and total length, identity
encoding, and a bounded body of at most 1 MiB. The Worker obtains HEAD metadata,
uses a strong ETag condition when available, and compares the GET revision before
returning bytes. Servers that ignore Range are rejected. The current contract checks
each read and honors the Host's expected revision through its HEAD and conditional
GET; it does not hold a remote lock across separate Host range requests.
PROPFIND is limited to 4 MiB and 8,192 responses, with DTDs disabled. Oversized
directories fail explicitly; Worker pagination does not bypass this server-response
budget.

Licensed under Apache-2.0. See [LICENSE](LICENSE).

## Release governance

Provider publication follows the shared template controller. A reviewed,
classified `develop` to `main` merge creates a stable version; explicit Preview
dispatches on `develop` create `X.Y.Z-preview.N` versions without taking stable
`latest`. Manual dispatch defaults to a verified candidate without publication.
Version, event, branch, source SHA and confirmation inputs are validated before
paths or builds are created. Build has no signing secrets; signing uses the
branch-restricted `adapter-signing` environment. Stable publication uses the
protected `stable` environment and its human approval rule.

Run `pwsh ./eng/verify-release.ps1` for hostile input rejection and a dual-RID
package signed with a disposable in-memory key. Production keys are read only
from signing-step environment variables. No private key file is read or exported.
The embedded inventory is verified before upload; MirrorPulse independently
verifies publisher trust at installation.

Publication freezes one package, detached signature and public verification key
with source, version, length and SHA256 metadata. Both native jobs verify these
same assets before the publication job; approval never rebuilds or replaces them.
Existing releases and tags are immutable. Actual repository and environment
protection must be verified separately from YAML names.
Source packages now require protocol v2 and include private runtimes for both RIDs.
Previously released v1 packages retain their original identities and payloads.

The release workflow runs fourteen actual WebDAV HTTP/Worker cases on both native
architectures and verifies production installation, Host-owned credentials,
Named Pipe routing, CfSharp demand reads and conditional mutations using fixed
MirrorPulse source `4324988f8e7f4262cc27fb399d1dc61741fcd3eb`. It records both source
commits, the exact published SDK 0.2.1 source and the candidate package hash.
Official candidates must pass the product's fixed publisher trust; exported public
keys authorize only disposable dry-run verification. Previously published v1
assets remain available until a reviewed stable v2 release supersedes `latest`.
