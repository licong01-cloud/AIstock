@echo off
REM 一键补齐 2025-04-30 和 2025-12-01 的分钟线数据（kline_minute_raw）
REM 依赖 Python 脚本 scripts\fix_minute_gaps.py 和现有的 scripts\ingest_incremental.py

setlocal enabledelayedexpansion

REM 切换到项目根目录（假定本 bat 位于 AIstock\scripts 目录）
cd /d "%~dp0.."

REM 使用当前环境中的 python 解释器执行修复脚本
python scripts\fix_minute_gaps.py

if errorlevel 1 (
  echo [fix_minute_gaps.bat] 修复脚本执行失败，错误码：%errorlevel%
  exit /b %errorlevel%
) else (
  echo [fix_minute_gaps.bat] 修复脚本执行完成。
)

endlocal
