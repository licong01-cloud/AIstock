@echo off
:: Run this script as Administrator to create the CPU temperature collection scheduled task
:: Only needs to be run once

echo Creating scheduled task: CollectCpuTemp ...

schtasks /create /tn "CollectCpuTemp" /tr "powershell.exe -ExecutionPolicy Bypass -NonInteractive -File F:\Dev\AIstock\monitoring\textfile_collector\collect_cpu_temp.ps1" /sc minute /mo 1 /ru SYSTEM /rl HIGHEST /f

if %errorlevel% equ 0 (
    echo [OK] Task created successfully.
    echo Starting task for initial run...
    schtasks /run /tn "CollectCpuTemp"
    timeout /t 5 /nobreak >nul
    echo.
    echo Verifying output:
    type F:\Dev\AIstock\monitoring\textfile_collector\cpu_temp.prom
) else (
    echo [ERROR] Failed to create task. Make sure you run this as Administrator.
)

pause
