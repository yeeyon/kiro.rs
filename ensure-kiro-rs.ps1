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

# Launch detached via cmd.exe rather than -RedirectStandardOutput/-RedirectStandardError.
#
# Those parameters force Start-Process down the CreateProcess path with
# bInheritHandles=TRUE, so the long-lived server inherits *every* inheritable
# handle this PowerShell holds - including the write end of a pipe when this
# script is invoked as `ensure-kiro-rs.ps1 | tee log` or from CI. The script
# itself exits in under a second, but the reader stays blocked until the
# server exits, i.e. forever. Letting cmd.exe do the redirection keeps the log
# files identical while ShellExecute drops handle inheritance entirely.
$inner = '"{0}" -c "{1}" --credentials "{2}"' -f $binary, $config, $credentials
$cmdArgs = '/c "{0} 1>"{1}" 2>"{2}""' -f $inner, $stdout, $stderr

try {
    Start-Process -FilePath "cmd.exe" -ArgumentList $cmdArgs -WindowStyle Hidden | Out-Null
} catch {
    if ($_.Exception.Message -match "Application Control policy") {
        throw "Windows Smart App Control is blocking unsigned $binary. Turn SAC Off in Windows Security > App & browser control > Smart App Control settings, reboot, then retry. $($_.Exception.Message)"
    }
    throw
}

# cmd.exe always starts, so a failing binary now surfaces in the child instead
# of as a thrown exception here. Confirm the server actually came up and
# report whatever it wrote to stderr if it did not.
$deadline = (Get-Date).AddSeconds(15)
while ((Get-Date) -lt $deadline) {
    if (Get-Process -Name "kiro-rs" -ErrorAction SilentlyContinue) { exit 0 }
    Start-Sleep -Milliseconds 250
}

$detail = ""
if (Test-Path -LiteralPath $stderr) {
    # -Raw yields $null for an empty file, so guard before calling .Trim().
    $raw = Get-Content -Raw -LiteralPath $stderr
    if ($raw) { $detail = $raw.Trim() }
}
if ($detail -match "Application Control policy") {
    throw "Windows Smart App Control is blocking unsigned $binary. Turn SAC Off in Windows Security > App & browser control > Smart App Control settings, reboot, then retry. $detail"
}
throw "kiro-rs did not start within 15s. See $stderr$(if ($detail) { ": $detail" })"
