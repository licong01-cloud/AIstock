@echo off
setlocal
set "AIROOT=%~dp0"
set "AISTOCK_CONDA_BAT=C:\Users\lc999\miniconda3\condabin\conda.bat"
set "AISTOCK_BACKEND_PYTHON=C:\Users\lc999\miniconda3\envs\AIstock\python.exe"

if "%~1"=="" goto tabs
if /i "%~1"=="--check" if "%~2"=="" goto run
if /i "%~1"=="--service" (
  if not "%~3"=="" goto usage
  if "%~2"=="tdx" goto run
  if "%~2"=="backend" goto run
  if "%~2"=="frontend" goto run
)
:usage
echo ERROR: Usage: start_all_ai_stock.bat [--check]
exit /b 1

:tabs
where wt.exe >nul 2>nul
if errorlevel 1 (
  echo ERROR: Windows Terminal is required for three service tabs.
  exit /b 1
)
REM One immediate request selects the active/MRU window once, before slow setup.
wt.exe -w 0 new-tab --title "AIstock TDX" --suppressApplicationTitle --startingDirectory "%AIROOT%." cmd.exe /d /k call "%~f0" --service tdx ; new-tab --title "AIstock Backend" --suppressApplicationTitle --startingDirectory "%AIROOT%." cmd.exe /d /k call "%~f0" --service backend ; new-tab --title "AIstock Frontend" --suppressApplicationTitle --startingDirectory "%AIROOT%." cmd.exe /d /k call "%~f0" --service frontend
if errorlevel 1 (
  echo ERROR: Three-tab dispatch failed. No fallback or automatic retry attempted.
  exit /b 1
)
echo Three service tabs requested. Check each tab for readiness or errors.
exit /b 0

:run
if not exist "%AISTOCK_CONDA_BAT%" (
  echo ERROR: Conda activation script not found: %AISTOCK_CONDA_BAT%
  pause
  exit /b 1
)
if not exist "%AISTOCK_BACKEND_PYTHON%" (
  echo ERROR: AIstock backend Python not found: %AISTOCK_BACKEND_PYTHON%
  pause
  exit /b 1
)
cd /d "%AIROOT%" || exit /b 1
call "%AISTOCK_CONDA_BAT%" activate AIstock
if errorlevel 1 (
  echo ERROR: AIstock Conda activation failed. No services started.
  pause
  exit /b 1
)
"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\start_ai_stock.py" %*
set "AISTOCK_EXIT=%errorlevel%"
if not "%AISTOCK_EXIT%"=="0" pause
exit /b %AISTOCK_EXIT%
