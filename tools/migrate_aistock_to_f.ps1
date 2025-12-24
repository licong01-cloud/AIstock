param(
    [string]$SourceRoot = 'C:\Users\lc999\NewAIstock\AIstock',
    [string]$TargetDevRoot = 'F:\Dev'
)

Write-Host '=== AIstock copy to F drive (copy only; keep C drive intact; RD-Agent not included) ===' -ForegroundColor Cyan
Write-Host "Source: $SourceRoot"
Write-Host "Target root: $TargetDevRoot"
Write-Host ''

# 1. 检查源目录是否存在
if (-not (Test-Path -LiteralPath $SourceRoot)) {
    Write-Error "Source folder not found: $SourceRoot"
    exit 1
}

# 2. 创建 F:\Dev\AIstock 目标目录
$targetAIstock = Join-Path $TargetDevRoot 'AIstock'

Write-Host 'Create target folders:' -ForegroundColor Yellow
Write-Host "  $TargetDevRoot"
New-Item -ItemType Directory -Path $TargetDevRoot -Force | Out-Null

Write-Host "  $targetAIstock"
New-Item -ItemType Directory -Path $targetAIstock -Force | Out-Null

# 3. 拷贝 AIstock 仓库（含 .git）
Write-Host ''
Write-Host '=== Copying AIstock folder ===' -ForegroundColor Cyan
Write-Host "From: $SourceRoot"
Write-Host "To:   $targetAIstock"

# robocopy mirrors to target (target may be overwritten), source is never deleted.
$roboAIArgs = @(
    $SourceRoot,
    $targetAIstock,
    '/MIR',
    '/R:2',
    '/W:2'
)

Write-Host ("Run: robocopy " + ($roboAIArgs -join ' ')) -ForegroundColor DarkCyan
robocopy @roboAIArgs | Out-Null

Write-Host 'Copy finished.' -ForegroundColor Green

Write-Host ''
Write-Host '=== Done ===' -ForegroundColor Cyan
Write-Host "C drive folder kept: $SourceRoot"
Write-Host "F drive folder:      $targetAIstock" -ForegroundColor Yellow
Write-Host ''
Write-Host 'Next steps:' -ForegroundColor Yellow
Write-Host '1. Open F:\Dev in VSCode as the workspace root.'
Write-Host '2. In F:\Dev\AIstock, update .env and start_all_ai_stock.bat following docs\migration_to_F_drive.md.'
