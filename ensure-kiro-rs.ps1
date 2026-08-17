$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$binary = Join-Path $root "target\release\kiro-rs.exe"
$config = Join-Path $root "data\config.json"
$credentials = Join-Path $root "data\credentials.json"
$stdout = Join-Path $root "data\kiro-rs-native.log"
$stderr = Join-Path $root "data\kiro-rs-native-error.log"

New-Item -ItemType Directory -Path (Join-Path $root "data") -Force | Out-Null

if (Get-Process -Name "kiro-rs" -ErrorAction SilentlyContinue) {
    exit 0
}
if (!(Test-Path -LiteralPath $binary)) {
    throw "kiro-rs binary not found: $binary"
}

& (Join-Path $root "sync-kiro-cli-auth.ps1") -Once -CredsPath $credentials

try {
    Start-Process `
        -FilePath $binary `
        -ArgumentList "-c", $config, "--credentials", $credentials `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr | Out-Null
} catch {
    if ($_.Exception.Message -match "Application Control policy") {
        throw "Windows Smart App Control is blocking unsigned $binary. Turn SAC Off in Windows Security > App & browser control > Smart App Control settings, reboot, then retry. $($_.Exception.Message)"
    }
    throw
}
