@echo off
setlocal
set "AIROOT=%~dp0"
set "AISTOCK_CONDA_BAT=C:\Users\lc999\miniconda3\condabin\conda.bat"
set "AISTOCK_BACKEND_PYTHON=C:\Users\lc999\miniconda3\envs\AIstock\python.exe"

if "%~1"=="" goto tabs
if /i "%~1"=="--check" if "%~2"=="" goto run
if /i "%~1"=="--check-env" if "%~2"=="" goto run
if /i "%~1"=="--service" (
  if not "%~3"=="" goto usage
  if "%~2"=="tdx" goto run
  if "%~2"=="backend" goto run
  if "%~2"=="frontend" goto run
)
:usage
echo ERROR: Usage: start_all_ai_stock.bat [--check or --check-env]
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
if not exist "%AISTOCK_BACKEND_PYTHON%" (
  echo ERROR: AIstock backend Python not found: %AISTOCK_BACKEND_PYTHON%
  pause
  exit /b 1
)
cd /d "%AIROOT%" || exit /b 1
REM Go and Node services (and read-only inventory) need no Conda activation.
if /i "%~1"=="--check" goto launch
if /i "%~1"=="--service" if not "%~2"=="backend" goto launch
if not exist "%AISTOCK_CONDA_BAT%" (
  echo ERROR: Conda activation script not found: %AISTOCK_CONDA_BAT%
  pause
  exit /b 1
)
REM Conda's RANDOM-based filenames are unsafe in a shared concurrent TEMP.
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
REM Remove only our empty private directory, never a recursive/shared temp tree.
rmdir "%AISTOCK_CONDA_TEMP%" 2>nul
if exist "%AISTOCK_CONDA_TEMP%\" echo WARNING: Activation temp retained: "%AISTOCK_CONDA_TEMP%"
if not "%AISTOCK_CONDA_EXIT%"=="0" (
  echo ERROR: AIstock Conda activation failed. Backend not started; other service tabs are independent.
  pause
  exit /b 1
)
if /i "%~1"=="--check-env" goto check_env
:launch
"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\start_ai_stock.py" %*
set "AISTOCK_EXIT=%errorlevel%"
if not "%AISTOCK_EXIT%"=="0" pause
exit /b %AISTOCK_EXIT%

:check_env
"%AISTOCK_BACKEND_PYTHON%" -X utf8 -c "import os,sys; assert os.path.normcase(sys.prefix)==os.path.normcase(os.environ['CONDA_PREFIX']); print('AIstock environment ready: '+sys.prefix)"
exit /b %errorlevel%
