$ErrorActionPreference = "Stop"

# update-kiro-rs: check for upstream updates and rebuild if needed.
# Pulls latest from upstream, compares Cargo.toml/src, rebuilds if changed.

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$gitdir = Join-Path $root ".git"

if (-not (Test-Path -LiteralPath $gitdir)) {
    return  # not a repo, skip
}

Write-Host "[update] Checking upstream for kiro-rs updates..." -ForegroundColor Cyan

try {
    Push-Location $root
    $beforeHeadLine = git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { return }

    git fetch upstream master --quiet 2>$null
    
    $status = git status --porcelain
    if ($status) {
        git stash push -u --quiet | Out-Null
    }

    $pullResult = git pull --ff-only --quiet 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[update]   Pull failed (merge needed?); skipping rebuild." -ForegroundColor Yellow
        return
    }

    $afterHeadLine = git rev-parse HEAD
    if ($beforeHeadLine -eq $afterHeadLine) {
        return  # no upstream changes
    }

    Write-Host "[update]   Upstream updated. Checking for rebuild need..." -ForegroundColor Cyan
    
    $changed = git diff $beforeHeadLine..HEAD --name-only -- Cargo.toml Cargo.lock "src/*"
    if (-not $changed) {
        return  # no rebuild needed
    }

    Write-Host "[update]   Changes detected in Cargo.toml or src/. Rebuilding..." -ForegroundColor Cyan
    $buildStart = Get-Date

    # Stop kiro-rs if running
    $procs = Get-Process -Name "kiro-rs" -ErrorAction SilentlyContinue
    if ($procs) {
        Write-Host "[update]   Stopping kiro-rs..." -ForegroundColor Yellow
        $procs | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }

    # Rebuild
    cargo build --release 2>&1 | Select-Object -Last 3
    if ($LASTEXITCODE -ne 0) {
        throw "[update]   Rebuild failed."
    }

    $buildDuration = ((Get-Date) - $buildStart).TotalSeconds
    Write-Host "[update]   Rebuild complete in ${buildDuration}s" -ForegroundColor Green

} finally {
    Pop-Location
}
