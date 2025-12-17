@echo off
chcp 65001 >nul
echo ========================================
echo   TDX股票数据API - Windows编译脚本
echo ========================================
echo.

cd /d %~dp0
cd ..

echo [1/4] 检查Go环境...
go version
if %errorlevel% neq 0 (
    echo 错误: 未找到Go环境，请先安装Go 1.20+
    pause
    exit /b 1
)

echo.
echo [2/4] 下载依赖...
go mod download
if %errorlevel% neq 0 (
    echo 错误: 依赖下载失败
    pause
    exit /b 1
)

echo.
echo [3/4] 编译Web服务器...
cd web
go build -ldflags="-s -w" -o stock-api.exe .
if %errorlevel% neq 0 (
    echo 错误: 编译失败
    pause
    exit /b 1
)

echo.
echo [4/4] 编译完成！
echo.
echo ========================================
echo   编译成功！
echo ========================================
echo.
echo 生成的文件: web\stock-api.exe
echo.
echo 运行方式:
echo   1. 双击 stock-api.exe
echo   2. 或命令行运行: stock-api.exe
echo   3. 访问: http://localhost:8080
echo.
echo 注意: 确保 static 目录与 exe 在同一目录下
echo.

pause

