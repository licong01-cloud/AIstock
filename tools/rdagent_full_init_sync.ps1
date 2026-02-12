param(
    [string]$ApiBase = $env:AISTOCK_API_BASE,
    [string]$RDAgentBundlesDir,
    [string]$AIstockBundlesDir = "F:\Dev\AIstock\backend\data\rdagent_assets\production_bundles",
    [switch]$SkipHttpCatalogSync
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ApiBase)) {
    $ApiBase = "http://127.0.0.1:8001/api/v1"
}

if ([string]::IsNullOrWhiteSpace($RDAgentBundlesDir)) {
    throw "RDAgentBundlesDir 不能为空。请传入 RD-Agent 侧 production_bundles 目录路径（例如：F:\\Dev\\RD-Agent-main\\RDagentDB\\production_bundles）"
}

Write-Host "[AIstock] 全量初始化同步开始" -ForegroundColor Cyan
Write-Host "- ApiBase: $ApiBase"
Write-Host "- RDAgentBundlesDir: $RDAgentBundlesDir"
Write-Host "- AIstockBundlesDir: $AIstockBundlesDir"

if (!(Test-Path -LiteralPath $RDAgentBundlesDir)) {
    throw "RDAgentBundlesDir 不存在: $RDAgentBundlesDir"
}

if (!(Test-Path -LiteralPath $AIstockBundlesDir)) {
    New-Item -ItemType Directory -Path $AIstockBundlesDir -Force | Out-Null
}

if (-not $SkipHttpCatalogSync) {
    Write-Host "[AIstock] Step1: 触发 Catalog 全量刷新（full_refresh + sync_metadata_only=true）..." -ForegroundColor Yellow
    $body = @{ mode = "full_refresh"; clean = $true; sync_metadata_only = $true; sync_assets_only = $false } | ConvertTo-Json
    $url = "$ApiBase/rdagent/sync/run"

    try {
        $resp = Invoke-RestMethod -Method Post -Uri $url -ContentType "application/json" -Body $body
        Write-Host "[AIstock] 已触发同步任务，当前状态: $($resp.state)" -ForegroundColor Green
        Write-Host "[AIstock] 建议打开 /rdagent/sync 页面查看进度，或调用 $ApiBase/rdagent/sync/status" -ForegroundColor DarkGray
    } catch {
        throw "触发 Catalog 全量刷新失败: $($_.Exception.Message)"
    }
} else {
    Write-Host "[AIstock] SkipHttpCatalogSync=true：跳过 HTTP Catalog 同步，仅执行 bundles 目录拷贝" -ForegroundColor Yellow
}

Write-Host "[AIstock] Step2: 拷贝 RD-Agent production_bundles 到 AIstock 本地缓存..." -ForegroundColor Yellow
Write-Host "    源: $RDAgentBundlesDir" -ForegroundColor DarkGray
Write-Host "    目标: $AIstockBundlesDir" -ForegroundColor DarkGray

$src = (Resolve-Path -LiteralPath $RDAgentBundlesDir).Path
$dst = (Resolve-Path -LiteralPath $AIstockBundlesDir).Path

Copy-Item -Path (Join-Path $src "*") -Destination $dst -Recurse -Force

Write-Host "[AIstock] Step3: 基础校验（manifest.json 是否存在）..." -ForegroundColor Yellow
$bundleDirs = Get-ChildItem -LiteralPath $dst -Directory -ErrorAction Stop
$missing = @()
foreach ($d in $bundleDirs) {
    $m = Join-Path $d.FullName "manifest.json"
    if (!(Test-Path -LiteralPath $m)) {
        $missing += $d.FullName
    }
}

if ($missing.Count -gt 0) {
    Write-Host "[AIstock] 发现缺少 manifest.json 的 bundle 目录（仅提示，不中断）：" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host "- $_" -ForegroundColor Red }
} else {
    Write-Host "[AIstock] manifest.json 基础校验通过（共 $($bundleDirs.Count) 个 bundle）" -ForegroundColor Green
}

Write-Host "[AIstock] 全量初始化同步脚本执行完毕" -ForegroundColor Cyan
