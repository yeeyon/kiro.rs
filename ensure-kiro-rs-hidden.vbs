Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\Users\User\kiro\ensure-kiro-rs.ps1""", 0, True
