# Debug startup for kiro-rs with verbose logging
$bin_path = "c:\Users\User\kiro\target\release\kiro-rs.exe"
$config_path = "c:\Users\User\kiro\data\config.json"
$credentials_path = "c:\Users\User\kiro\data\credentials.json"
$log_file = "c:\Users\User\kiro\data\kiro-rs-native.log"
$log_err_file = "c:\Users\User\kiro\data\kiro-rs-native-error.log"

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
