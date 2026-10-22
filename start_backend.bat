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
if /i "%~1"=="--check-env" goto child
if not "%~1"=="" (
  echo ERROR: Usage: start_backend.bat [--check or --check-env]
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
REM Keep concurrent tabs out of Conda's shared RANDOM-based temporary filenames.
set "AISTOCK_CONDA_ID="
for /f "delims=" %%T in ('call "%AISTOCK_BACKEND_PYTHON%" -I -c "import uuid; print(uuid.uuid4().hex)"') do set "AISTOCK_CONDA_ID=%%T"
if not defined AISTOCK_CONDA_ID exit /b 1
set "AISTOCK_CONDA_TEMP=%AIROOT%..\AIstock_runtime\startup\conda\%AISTOCK_CONDA_ID%"
mkdir "%AISTOCK_CONDA_TEMP%" || exit /b 1
set "AISTOCK_OLD_TEMP=%TEMP%"
set "AISTOCK_OLD_TMP=%TMP%"
set "AISTOCK_OLD_TMPDIR=%TMPDIR%"
set "TEMP=%AISTOCK_CONDA_TEMP%"
set "TMP=%AISTOCK_CONDA_TEMP%"
set "TMPDIR=%AISTOCK_CONDA_TEMP%"
echo Conda activation temp: "%AISTOCK_CONDA_TEMP%"
call "%AISTOCK_CONDA_BAT%" activate AIstock
set "AISTOCK_CONDA_EXIT=%errorlevel%"
set "TEMP=%AISTOCK_OLD_TEMP%"
set "TMP=%AISTOCK_OLD_TMP%"
set "TMPDIR=%AISTOCK_OLD_TMPDIR%"
rmdir "%AISTOCK_CONDA_TEMP%" 2>nul
if exist "%AISTOCK_CONDA_TEMP%\" echo WARNING: Activation temp retained: "%AISTOCK_CONDA_TEMP%"
if not "%AISTOCK_CONDA_EXIT%"=="0" (
  echo ERROR: AIstock Conda activation failed. Backend not started.
  exit /b 1
)
if /i "%~1"=="--check-env" goto check_env
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

:check_env
"%AISTOCK_BACKEND_PYTHON%" -X utf8 -c "import os,sys; assert os.path.normcase(sys.prefix)==os.path.normcase(os.environ['CONDA_PREFIX']); print('AIstock environment ready: '+sys.prefix)"
exit /b %errorlevel%
