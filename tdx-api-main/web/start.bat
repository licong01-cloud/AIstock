@echo off
chcp 65001 >nul
echo ========================================
echo   股票数据查询Web系统
echo ========================================
echo.
echo 正在启动服务器...
echo.

cd /d %~dp0
if not defined AISTOCK_BACKEND_PYTHON set "AISTOCK_BACKEND_PYTHON=C:\Users\lc999\miniconda3\envs\AIstock\python.exe"
set "TDX_HTTP_PORT=19080"
"%AISTOCK_BACKEND_PYTHON%" "%~dp0..\..\scripts\start_tdx_go_backend.py" --database-target production --env-file "%~dp0..\..\.env"

pause

