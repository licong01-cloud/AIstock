@echo off
chcp 65001 >nul
echo ========================================
echo   TDX股票数据API - 修复并编译脚本
echo ========================================
echo.
echo 注意: 如果编译失败，请检查server.go文件的编码问题
echo.

cd /d %~dp0
cd ..

echo [1/3] 设置Go环境变量...
set GOPROXY=https://goproxy.cn,direct
set CGO_ENABLED=0

echo.
echo [2/3] 检查Go环境...
go version
if %errorlevel% neq 0 (
    echo 错误: 未找到Go环境，请先安装Go 1.20+
    pause
    exit /b 1
)

echo.
echo [3/3] 编译Web服务器...
cd web
go build -ldflags="-s -w" -o stock-api.exe .

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo   编译成功！
    echo ========================================
    echo.
    echo 生成的文件: web\stock-api.exe
    echo.
    if exist stock-api.exe (
        dir stock-api.exe | find "stock-api.exe"
        echo.
        echo 运行方式:
        echo   1. 双击 stock-api.exe
        echo   2. 或命令行运行: stock-api.exe
        echo   3. 访问: http://localhost:8080
    )
) else (
    echo.
    echo ========================================
    echo   编译失败！
    echo ========================================
    echo.
    echo 可能的原因:
    echo   1. server.go文件编码问题（包含NUL字符）
    echo   2. 字符串未正确闭合
    echo   3. 语法错误
    echo.
    echo 建议:
    echo   1. 使用git恢复文件: git checkout web/server.go
    echo   2. 或手动检查并修复server.go文件
    echo   3. 确保文件使用UTF-8编码（无BOM）
)

echo.
pause

