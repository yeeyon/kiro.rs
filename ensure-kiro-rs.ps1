$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$binary = Join-Path $root "target\release\kiro-rs.exe"
$config = Join-Path $root "data\config.json"
$credentials = Join-Path $root "data\credentials.json"
$stdout = Join-Path $root "data\kiro-rs-native.log"
$stderr = Join-Path $root "data\kiro-rs-native-error.log"

if (Get-Process -Name "kiro-rs" -ErrorAction SilentlyContinue) {
    exit 0
}
if (!(Test-Path -LiteralPath $binary)) {
    throw "kiro-rs binary not found: $binary"
}

Start-Process `
    -FilePath $binary `
    -ArgumentList "-c", $config, "--credentials", $credentials `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr | Out-Null
