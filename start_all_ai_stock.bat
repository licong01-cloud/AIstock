@echo off
REM === AIstock 一键启动脚本 ===
REM 根据需要修改根目录路径
set AIROOT=F:\Dev\AIstock

where wt >nul 2>nul
if %errorlevel%==0 (
  wt -w 0 new-tab --title "TDX Go Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT%\tdx-api-main\web & set TDX_HTTP_PORT=19080 & go run ." ^
    ; new-tab --title "AIstock Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT% & call conda activate AIstock & uvicorn backend.main:app --host 127.0.0.1 --port 8001" ^
    ; new-tab --title "AIstock Frontend" cmd /k "chcp 65001>nul & cd /d %AIROOT%\frontend & call conda activate AIstock & npm run dev"
  goto :done
)

REM === 1. 启动 TDX Go 后端（端口 19080） ===
start "TDX Go Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT%\tdx-api-main\web & set TDX_HTTP_PORT=19080 & go run ."

REM === 2. 启动 AIstock 后端（FastAPI, 端口 8001） ===
start "AIstock Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT% & call conda activate AIstock & uvicorn backend.main:app --host 127.0.0.1 --port 8001"

REM === 3. 启动 Next.js 前端（端口 3000） ===
start "AIstock Frontend" cmd /k "chcp 65001>nul & cd /d %AIROOT%\frontend & call conda activate AIstock & npm run dev"

echo 已尝试启动：TDX Go 后端 + AIstock 后端 + 前端（各在独立窗口）
:done
pause
