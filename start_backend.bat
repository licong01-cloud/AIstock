@echo off
setlocal
set "AIROOT=%~dp0"
set "AISTOCK_CONDA_BAT=C:\Users\lc999\miniconda3\condabin\conda.bat"
set "AISTOCK_BACKEND_PYTHON=C:\Users\lc999\miniconda3\envs\AIstock\python.exe"

if not exist "%AISTOCK_CONDA_BAT%" (
  echo ERROR: Conda activation script not found: %AISTOCK_CONDA_BAT%
  exit /b 1
)
if not exist "%AISTOCK_BACKEND_PYTHON%" (
  echo ERROR: AIstock Python not found: %AISTOCK_BACKEND_PYTHON%
  exit /b 1
)
if /i "%~1"=="--check" (
  echo Backend-only launcher prerequisites found. No tab or service started.
  exit /b 0
)
if /i "%~1"=="--child" goto child
if not "%~1"=="" (
  echo ERROR: Usage: start_backend.bat [--check]
  exit /b 1
)
where wt.exe >nul 2>nul
if errorlevel 1 (
  echo ERROR: Windows Terminal is required for a new backend tab.
  exit /b 1
)
REM Dispatch immediately, before Conda activation, like the original tab launcher.
REM -w 0 selects the most recently active Windows Terminal window.
wt.exe -w 0 new-tab --title "AIstock Backend" --suppressApplicationTitle --startingDirectory "%AIROOT%." cmd.exe /d /k call "%~f0" --child
if errorlevel 1 (
  echo ERROR: Failed to create backend tab. No fallback window or retry was started.
  exit /b 1
)
echo Backend tab requested. Check its output for readiness or errors.
exit /b 0

:child
cd /d "%AIROOT%" || exit /b 1
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"
call "%AISTOCK_CONDA_BAT%" activate AIstock
if errorlevel 1 (
  echo ERROR: AIstock Conda activation failed. Backend not started.
  exit /b 1
)
REM Bind without listening, then close: refuse an occupied port, never stop its owner.
"%AISTOCK_BACKEND_PYTHON%" -X utf8 -c "import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1); s.bind(('0.0.0.0',8001)); s.close()"
if errorlevel 1 (
  echo ERROR: Port 8001 unavailable. Close the old backend tab first. No process was stopped.
  exit /b 1
)
"%AISTOCK_BACKEND_PYTHON%" -X utf8 -m uvicorn backend.main:app --host 0.0.0.0 --port 8001
set "AISTOCK_EXIT=%errorlevel%"
echo Backend exited with code %AISTOCK_EXIT%.
exit /b %AISTOCK_EXIT%
