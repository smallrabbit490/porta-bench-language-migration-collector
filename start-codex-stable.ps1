$ErrorActionPreference = "Stop"

$stableRoot = "D:\codex-workspaces"
$stablePath = Join-Path $stableRoot "SWEbench_zzc"
$sourcePath = (Resolve-Path $PSScriptRoot).Path
$proxyUrl = "http://127.0.0.1:7897"

if (!(Test-Path $stableRoot)) {
    New-Item -ItemType Directory -Path $stableRoot | Out-Null
}

if (!(Test-Path $stablePath)) {
    New-Item -ItemType Junction -Path $stablePath -Target $sourcePath | Out-Null
}

$env:HTTP_PROXY = $proxyUrl
$env:HTTPS_PROXY = $proxyUrl
$env:ALL_PROXY = $proxyUrl
$env:NO_PROXY = "127.0.0.1,localhost"

Write-Host "Stable workspace: $stablePath"
Write-Host "HTTP_PROXY=$($env:HTTP_PROXY)"
Write-Host "HTTPS_PROXY=$($env:HTTPS_PROXY)"

$runningCode = Get-Process Code -ErrorAction SilentlyContinue
if ($null -ne $runningCode) {
    Write-Host ""
    Write-Host "VS Code is already running."
    Write-Host "Close all VS Code windows first, then run this script again."
    Write-Host "The first Code process must inherit the proxy variables."
    exit 1
}

$codeCommand = Get-Command code -ErrorAction SilentlyContinue
if ($null -eq $codeCommand) {
    Write-Host ""
    Write-Host "VS Code command 'code' was not found."
    Write-Host "Open this folder manually in VS Code:"
    Write-Host "  $stablePath"
    Write-Host ""
    Write-Host "If you want Codex CLI in this same shell, run:"
    Write-Host "  codex -C `"$stablePath`""
    exit 0
}

Start-Process -FilePath $codeCommand.Source -WorkingDirectory $stablePath -ArgumentList "-n", $stablePath
