param(
    [switch]$All,
    [string]$Subtype = "",
    [string]$InstanceId = "",
    [int]$Limit = 0,
    [switch]$Force,
    [switch]$BuildList,
    [switch]$GitPull,
    [switch]$UseClashProxy = $false
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..\..")
Set-Location $projectRoot

# 直接访问 GitHub，无需代理
if ($UseClashProxy) {
    Write-Host "Using proxy configuration..." -ForegroundColor Yellow
    $env:http_proxy = "http://127.0.0.1:7890"
    $env:https_proxy = "http://127.0.0.1:7890"
    $env:HTTP_PROXY = "http://127.0.0.1:7890"
    $env:HTTPS_PROXY = "http://127.0.0.1:7890"
}

if ($GitPull) {
    Write-Host "Running git pull origin main..." -ForegroundColor Cyan
    git pull origin main
}

$pythonScript = Join-Path $scriptDir "pull_snapshots_from_github.py"
$argsList = @($pythonScript)

if ($BuildList) {
    $argsList += "--build-list"
} else {
    if ($All) {
        $argsList += "--all"
    }
    if ($Subtype) {
        $argsList += "--subtype"
        $argsList += $Subtype
    }
    if ($InstanceId) {
        $argsList += "--instance-id"
        $argsList += $InstanceId
    }
    if ($Limit -gt 0) {
        $argsList += "--limit"
        $argsList += "$Limit"
    }
    if ($Force) {
        $argsList += "--force"
    }
    if (-not $All -and -not $Subtype -and -not $InstanceId) {
        Write-Host "No selector provided, defaulting to --all" -ForegroundColor Yellow
        $argsList += "--all"
    }
}

python @argsList
