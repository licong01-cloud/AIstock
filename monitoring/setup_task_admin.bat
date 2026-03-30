@echo off
echo Creating CollectCpuTemp task...
schtasks /create /tn "CollectCpuTemp" /tr "powershell.exe -ExecutionPolicy Bypass -NonInteractive -File F:\Dev\AIstock\monitoring\textfile_collector\collect_cpu_temp.ps1" /sc minute /mo 1 /ru SYSTEM /rl HIGHEST /f
echo.
echo Running task...
schtasks /run /tn "CollectCpuTemp"
timeout /t 6 /nobreak
echo.
echo === Result ===
type F:\Dev\AIstock\monitoring\textfile_collector\cpu_temp.prom
echo.
pause
