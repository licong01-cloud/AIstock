@echo off
REM 在AIstock conda环境中安装shadcn/ui组件
REM 使用方法: 在PowerShell中执行 .\install_shadcn_components.bat

echo ========================================
echo 安装shadcn/ui组件到AIstock前端项目
echo ========================================

REM 激活AIstock conda环境并安装组件
call conda activate AIstock

echo.
echo 正在安装shadcn组件...
echo.

REM 使用新的shadcn命令安装组件
call npx shadcn@latest add card --yes
call npx shadcn@latest add button --yes
call npx shadcn@latest add badge --yes
call npx shadcn@latest add dialog --yes
call npx shadcn@latest add input --yes
call npx shadcn@latest add label --yes
call npx shadcn@latest add select --yes
call npx shadcn@latest add textarea --yes
call npx shadcn@latest add tabs --yes
call npx shadcn@latest add alert --yes

echo.
echo ========================================
echo 组件安装完成！
echo ========================================
echo.

pause
