# Debug startup for kiro-rs with verbose logging
$root = $PSScriptRoot
$data_path = Join-Path $root "data"
$bin_path = Join-Path $root "target\release\kiro-rs.exe"
$config_path = Join-Path $data_path "config.json"
$credentials_path = Join-Path $data_path "credentials.json"
$log_file = Join-Path $data_path "kiro-rs-native.log"
$log_err_file = Join-Path $data_path "kiro-rs-native-error.log"

New-Item -ItemType Directory -Path $data_path -Force | Out-Null
& (Join-Path $root "sync-kiro-cli-auth.ps1") -Once -CredsPath $credentials_path

Stop-Process -Name "kiro-rs" -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

$env:RUST_LOG = "kiro_rs=debug,info"

Start-Process -FilePath $bin_path `
  -ArgumentList '-c', $config_path, '--credentials', $credentials_path `
  -RedirectStandardOutput $log_file `
  -RedirectStandardError $log_err_file `
  -NoNewWindow

Start-Sleep -Seconds 2
$proc = Get-Process -Name "kiro-rs" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host ("kiro-rs running PID=" + $proc.Id + " RUST_LOG=" + $env:RUST_LOG)
} else {
    Write-Host "kiro-rs FAILED to start"
}
