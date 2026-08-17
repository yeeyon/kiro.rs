Set WshShell = CreateObject("WScript.Shell")
Set FileSystem = CreateObject("Scripting.FileSystemObject")
Root = FileSystem.GetParentFolderName(WScript.ScriptFullName)
EnsureScript = Root & "\ensure-kiro-rs.ps1"
WshShell.Run "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & EnsureScript & """", 0, False
