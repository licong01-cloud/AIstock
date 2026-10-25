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
if /i "%~1"=="--check" if "%~2"=="" (
  echo Backend-only launcher prerequisites found. No tab or service started.
  exit /b 0
)
if /i "%~1"=="--child" if not "%~2"=="" if "%~3"=="" goto child
if /i "%~1"=="--check-env" if "%~2"=="" goto child
if not "%~2"=="" goto usage
if "%~1"=="" goto dispatch
if /i "%~1"=="--restart" goto dispatch
:usage
echo ERROR: Usage: start_backend.bat [--restart or --check or --check-env]
exit /b 1

:dispatch
cd /d "%AIROOT%" || exit /b 1
REM The caller waits for a nonce-bound fresh-process health/identity receipt.
"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\backend_lifecycle.py" %*
exit /b %errorlevel%

:child
cd /d "%AIROOT%" || exit /b 1
call :activate
set "AISTOCK_ACTIVATION_EXIT=%errorlevel%"
if /i "%~1"=="--check-env" goto check_env
if not "%AISTOCK_ACTIVATION_EXIT%"=="0" (
  "%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\backend_lifecycle.py" --fail "%~2"
  exit /b 1
)
"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\backend_lifecycle.py" --serve "%~2"
exit /b %errorlevel%

:activate
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
exit /b 0

:check_env
if not "%AISTOCK_ACTIVATION_EXIT%"=="0" exit /b %AISTOCK_ACTIVATION_EXIT%
"%AISTOCK_BACKEND_PYTHON%" -X utf8 -c "import os,sys; assert os.path.normcase(sys.prefix)==os.path.normcase(os.environ['CONDA_PREFIX']); print('AIstock environment ready: '+sys.prefix)"
exit /b %errorlevel%
