param(
    [switch]$All,
    [string]$Subtype = "",
    [string]$InstanceId = "",
    [switch]$GitPull,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\\..\\..")
Set-Location $projectRoot

if ($GitPull) {
    Write-Host "Running git pull origin main..." -ForegroundColor Cyan
    git pull origin main
}

$restoreScript = Join-Path $scriptDir "restore_snapshot_archives.py"

$args = @($restoreScript)
if ($All) {
    $args += "--all"
}
if ($Subtype) {
    $args += "--subtype"
    $args += $Subtype
}
if ($InstanceId) {
    $args += "--instance-id"
    $args += $InstanceId
}
if ($Force) {
    $args += "--force"
}

if (-not $All -and -not $Subtype -and -not $InstanceId) {
    Write-Host "No selector provided, defaulting to --all" -ForegroundColor Yellow
    $args += "--all"
}

Write-Host "Restoring snapshot archives..." -ForegroundColor Cyan
python @args
