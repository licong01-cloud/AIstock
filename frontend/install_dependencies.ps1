# AIstock前端依赖安装脚本
# 使用方法: 在PowerShell中执行 .\install_dependencies.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "开始安装AIstock前端依赖" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 步骤1：安装TailwindCSS
Write-Host "步骤1/4: 安装TailwindCSS和PostCSS..." -ForegroundColor Yellow
npm install -D tailwindcss postcss autoprefixer
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误：TailwindCSS安装失败！" -ForegroundColor Red
    exit 1
}
Write-Host "✓ TailwindCSS安装成功" -ForegroundColor Green
Write-Host ""

# 步骤2：安装shadcn/ui依赖
Write-Host "步骤2/4: 安装shadcn/ui依赖..." -ForegroundColor Yellow
npm install class-variance-authority clsx tailwind-merge tailwindcss-animate
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误：shadcn/ui依赖安装失败！" -ForegroundColor Red
    exit 1
}
Write-Host "✓ shadcn/ui依赖安装成功" -ForegroundColor Green
Write-Host ""

# 步骤3：安装Radix UI组件
Write-Host "步骤3/4: 安装Radix UI组件..." -ForegroundColor Yellow
npm install @radix-ui/react-dialog @radix-ui/react-label @radix-ui/react-select @radix-ui/react-slot @radix-ui/react-tabs
if ($LASTEXITCODE -ne 0) {
    Write-Host "错误：Radix UI组件安装失败！" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Radix UI组件安装成功" -ForegroundColor Green
Write-Host ""

# 步骤4：安装shadcn组件
Write-Host "步骤4/4: 安装shadcn组件..." -ForegroundColor Yellow
$components = @("card", "button", "badge", "dialog", "input", "label", "select", "textarea", "tabs", "alert")
$total = $components.Count
$current = 0

foreach ($component in $components) {
    $current++
    Write-Host "  [$current/$total] 安装 $component..." -ForegroundColor Cyan
    npx shadcn@latest add $component --yes
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  警告：$component 安装失败，继续..." -ForegroundColor Yellow
    } else {
        Write-Host "  ✓ $component 安装成功" -ForegroundColor Green
    }
}
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "安装完成！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "下一步：" -ForegroundColor Yellow
Write-Host "1. 启动前端服务: npm run dev" -ForegroundColor White
Write-Host "2. 访问新页面: http://localhost:3000/config/rdagent-llm" -ForegroundColor White
Write-Host ""
