<#
.SYNOPSIS
  Mirror the kiro-cli (Amazon Q for CLI) auth into kiro-rs credentials.json.

.DESCRIPTION
  kiro-cli stores its login at:
      %USERPROFILE%\.aws\sso\cache\kiro-auth-token.json   (tokens)
      %USERPROFILE%\.aws\sso\cache\<clientIdHash>.json     (OIDC client reg)
  This script reads both and writes/updates a single stable credential entry
  (default id = 2) inside kiro-rs credentials.json, preserving any other
  entries. kiro-rs itself self-heals: on the next request after the file
  changes, it re-reads credentials.json, matches by id, and adopts the new
  refreshToken automatically (no restart needed for token rotation).

.PARAMETER Once
  Sync a single time and exit.

.PARAMETER Watch
  Stay running and re-sync whenever the kiro-cli cache changes.

.NOTES
  Both kiro-cli and kiro-rs refresh against AWS SSO OIDC independently and the
  refresh token rotates on each refresh. This script makes kiro-cli the source
  of truth for kiro-rs. If kiro-rs happens to rotate the shared token first,
  kiro-cli may need a re-login. Treat kiro-cli as the place you log in.
#>
[CmdletBinding()]
param(
    [switch]$Once,
    [switch]$Watch,
    [string]$CacheDir     = (Join-Path $env:USERPROFILE ".aws\sso\cache"),
    [string]$TokenFile    = "kiro-auth-token.json",
    [string]$CredsPath,
    [int]   $EntryId      = 2,
    [int]   $Priority     = 0
)

$ErrorActionPreference = "Stop"

if (-not $CredsPath) {
    $CredsPath = Join-Path $PSScriptRoot "data\credentials.json"
}

function Write-Log([string]$msg) {
    Write-Host ("[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg)
}

function Sync-Once {
    $tokenPath = Join-Path $CacheDir $TokenFile
    if (-not (Test-Path $tokenPath)) {
        Write-Log "kiro-cli token not found at $tokenPath - skipping."
        return $false
    }

    $tok = Get-Content -Raw -LiteralPath $tokenPath | ConvertFrom-Json

    if (-not $tok.refreshToken) {
        Write-Log "Token file has no refreshToken - skipping."
        return $false
    }

    # Resolve the OIDC client registration via clientIdHash (== filename)
    $clientId     = $null
    $clientSecret = $null
    if ($tok.clientIdHash) {
        $regPath = Join-Path $CacheDir ("{0}.json" -f $tok.clientIdHash)
        if (Test-Path $regPath) {
            $reg = Get-Content -Raw -LiteralPath $regPath | ConvertFrom-Json
            $clientId     = $reg.clientId
            $clientSecret = $reg.clientSecret
        } else {
            Write-Log "Client reg file not found: $regPath (IdC refresh will fail without it)."
        }
    }

    # Normalise authMethod: kiro-cli uses 'IdC' / 'Social'; kiro-rs wants 'idc' / 'social'.
    $authMethod = "idc"
    if ($tok.authMethod -and $tok.authMethod.ToLower() -eq "social") { $authMethod = "social" }

    # Build the kiro-rs credential entry (camelCase keys).
    $entry = [ordered]@{
        id           = $EntryId
        accessToken  = $tok.accessToken
        refreshToken = $tok.refreshToken
        expiresAt    = $tok.expiresAt
        authMethod   = $authMethod
        endpoint     = "cli"
        priority     = $Priority
        disabled     = $false
    }
    if ($tok.provider) { $entry.provider = $tok.provider }
    if ($tok.region)   { $entry.region   = $tok.region }
    if ($clientId)     { $entry.clientId     = $clientId }
    if ($clientSecret) { $entry.clientSecret = $clientSecret }

    # Load existing credentials.json (array or single object), preserve other entries.
    $existing = @()
    if ((Test-Path $CredsPath) -and ((Get-Content -Raw -LiteralPath $CredsPath).Trim().Length -gt 0)) {
        $parsed = Get-Content -Raw -LiteralPath $CredsPath | ConvertFrom-Json
        if ($parsed -is [array]) { $existing = $parsed } else { $existing = @($parsed) }
    }

    # Detect no-op: same refreshToken already present for this id.
    $prev = $existing | Where-Object { $_.id -eq $EntryId } | Select-Object -First 1
    if ($prev -and $prev.refreshToken -eq $entry.refreshToken -and $prev.endpoint -eq $entry.endpoint) {
        Write-Log "No change (id=$EntryId refreshToken identical)."
        return $false
    }

    # Drop the old entry for this id, keep the rest, append the fresh one.
    $others = @($existing | Where-Object { $_.id -ne $EntryId })
    $merged = @()
    foreach ($o in $others) { $merged += $o }
    $merged += [pscustomobject]$entry

    $json = ConvertTo-Json -Depth 10 -InputObject @($merged)

    # Atomic write: temp file in same dir, then move over.
    # Use BOM-less UTF-8 (PowerShell 5.1 -Encoding UTF8 emits a BOM that serde_json rejects).
    $dir = Split-Path -Parent $CredsPath
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    $tmp = Join-Path $dir (".credentials.{0}.tmp" -f ([guid]::NewGuid().ToString("N")))
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($tmp, $json, $utf8NoBom)
    Move-Item -LiteralPath $tmp -Destination $CredsPath -Force

    Write-Log "Synced kiro-cli auth -> $CredsPath (id=$EntryId, provider=$($tok.provider), authMethod=$authMethod). kiro-rs will self-heal on next request."
    return $true
}

if (-not $Once -and -not $Watch) { $Once = $true }

if ($Once) {
    [void](Sync-Once)
}

if ($Watch) {
    Write-Log "Watching $CacheDir for kiro-cli login changes... (Ctrl+C to stop)"
    [void](Sync-Once)  # initial sync on start

    $fsw = New-Object System.IO.FileSystemWatcher
    $fsw.Path = $CacheDir
    $fsw.Filter = "*.json"
    $fsw.IncludeSubdirectories = $false
    $fsw.NotifyFilter = [System.IO.NotifyFilters]::LastWrite -bor [System.IO.NotifyFilters]::FileName
    $fsw.EnableRaisingEvents = $true

    # Debounced loop: poll the event queue, coalesce bursts of writes.
    $action = {
        try { Start-Sleep -Milliseconds 400; [void](Sync-Once) }
        catch { Write-Log "Sync error: $($_.Exception.Message)" }
    }
    Register-ObjectEvent -InputObject $fsw -EventName Changed -Action $action | Out-Null
    Register-ObjectEvent -InputObject $fsw -EventName Created -Action $action | Out-Null
    Register-ObjectEvent -InputObject $fsw -EventName Renamed -Action $action | Out-Null

    try { while ($true) { Start-Sleep -Seconds 3600 } }
    finally { $fsw.EnableRaisingEvents = $false; $fsw.Dispose() }
}
