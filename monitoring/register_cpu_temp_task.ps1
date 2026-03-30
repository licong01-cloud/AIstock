# Self-elevate if not admin
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Start-Process powershell "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

Write-Host "=== Creating CollectCpuTemp scheduled task ===" -ForegroundColor Cyan

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -NonInteractive -File F:\Dev\AIstock\monitoring\textfile_collector\collect_cpu_temp.ps1"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration ([TimeSpan]::MaxValue)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -ExecutionTimeLimit (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "CollectCpuTemp" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force

Write-Host "[OK] Task registered." -ForegroundColor Green

# Run immediately
Start-ScheduledTask -TaskName "CollectCpuTemp"
Write-Host "Task started. Waiting 5 seconds..."
Start-Sleep -Seconds 5

# Verify
$content = Get-Content "F:\Dev\AIstock\monitoring\textfile_collector\cpu_temp.prom"
Write-Host "`n=== Output ===" -ForegroundColor Cyan
$content | ForEach-Object { Write-Host $_ }

$ts = (Get-Item "F:\Dev\AIstock\monitoring\textfile_collector\cpu_temp.prom").LastWriteTime
Write-Host "`nFile updated at: $ts" -ForegroundColor Green

Read-Host "`nPress Enter to close"
