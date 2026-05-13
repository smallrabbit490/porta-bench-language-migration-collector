param(
    [int]$Attempts = 3,
    [switch]$WriteReviewResults
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$LogDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$runs = @(
    @{ Subtype = "java_python"; Workers = 2 },
    @{ Subtype = "python_cpp"; Workers = 2 },
    @{ Subtype = "python_java"; Workers = 1 }
)

foreach ($run in $runs) {
    $subtype = $run.Subtype
    $workers = $run.Workers
    $logPath = Join-Path $LogDir "auto_review_$subtype.log"
    Add-Content -LiteralPath $logPath -Value "`n--- start $(Get-Date -Format o) workers=$workers attempts=$Attempts ---" -Encoding UTF8

    $args = @(
        "workflow/auto_review/run_auto_review.py",
        "--stage", "run-batch",
        "--subtype", $subtype,
        "--workers", [string]$workers,
        "--attempts", [string]$Attempts,
        "--resume"
    )
    if ($WriteReviewResults) {
        $args += "--write-review-results"
    }

    $command = @"
`$env:PYTHONUNBUFFERED='1'
`$env:PYTHONIOENCODING='utf-8'
python $($args -join ' ') *> '$logPath'
"@

    $process = Start-Process powershell `
        -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $command `
        -WorkingDirectory $ProjectRoot `
        -WindowStyle Hidden `
        -PassThru

    Write-Host "$subtype started. pid=$($process.Id), workers=$workers, log=$logPath"
}

Write-Host "Total GLM workers for remaining subtypes: 5"
Write-Host "Watch logs with: Get-Content logs/auto_review_java_python.log -Wait"
