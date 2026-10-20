@echo off
setlocal
set "AIROOT=%~dp0"
set "AISTOCK_CONDA_BAT=C:\Users\lc999\miniconda3\condabin\conda.bat"
set "AISTOCK_BACKEND_PYTHON=C:\Users\lc999\miniconda3\envs\AIstock\python.exe"

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
"%AISTOCK_BACKEND_PYTHON%" "%AIROOT%scripts\start_ai_stock.py" %*
set "AISTOCK_EXIT=%errorlevel%"
if not "%AISTOCK_EXIT%"=="0" pause
exit /b %AISTOCK_EXIT%
