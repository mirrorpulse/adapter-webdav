[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
& pwsh -NoProfile -File (Join-Path $PSScriptRoot 'restore-adapter-sdk.ps1')
if ($LASTEXITCODE -ne 0) { throw 'Fixed SDK verification failed.' }
$projects = @("src/MirrorPulse.Adapter.WebDav.Worker/MirrorPulse.Adapter.WebDav.Worker.csproj", "tests/MirrorPulse.Adapter.WebDav.Tests/MirrorPulse.Adapter.WebDav.Tests.csproj", "tools/MirrorPulse.Adapter.WebDav.Conformance/MirrorPulse.Adapter.WebDav.Conformance.csproj")
foreach ($project in $projects) {
    & dotnet restore $project --locked-mode
    if ($LASTEXITCODE -ne 0) { throw "Restore failed for $project." }
    & dotnet build $project --configuration Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $project." }
    & dotnet format $project --verify-no-changes --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Formatting failed for $project." }
}
& dotnet test $projects[1] --configuration Release --no-build --no-restore --logger trx --results-directory artifacts/test-results
if ($LASTEXITCODE -ne 0) { throw 'WebDAV boundary tests failed.' }
[xml]$trx = Get-Content -LiteralPath (@(Get-ChildItem -LiteralPath artifacts/test-results -Filter '*.trx' | Sort-Object LastWriteTimeUtc -Descending)[0].FullName) -Raw
$counts = $trx.TestRun.ResultSummary.Counters
if ($counts.total -ne 46 -or $counts.executed -ne 46 -or $counts.passed -ne 46 -or $counts.notExecuted -ne 0) {
    throw 'All WebDAV URI/HTTP and real Worker process cases must execute without skips.'
}
