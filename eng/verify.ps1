[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$projects = @("src/MirrorPulse.Adapter.Sdk/MirrorPulse.Adapter.Sdk.csproj", "src/MirrorPulse.Adapter.WebDav.Worker/MirrorPulse.Adapter.WebDav.Worker.csproj", "tests/MirrorPulse.Adapter.WebDav.Tests/MirrorPulse.Adapter.WebDav.Tests.csproj")
foreach ($project in $projects) {
    & dotnet restore $project --locked-mode
    if ($LASTEXITCODE -ne 0) { throw "Restore failed for $project." }
    & dotnet build $project --configuration Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $project." }
}
& dotnet test $projects[-1] --configuration Release --no-build --logger trx --results-directory TestResults
if ($LASTEXITCODE -ne 0) { throw 'WebDAV boundary tests failed.' }
