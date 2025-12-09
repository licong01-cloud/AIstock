# Qlib CSV -> bin 导出 & 校验测试脚本（WSL + conda 环境：rdagent-gpu）
# 使用方法：在 PowerShell 中执行：
#   pwsh -File .\scripts\qlib_dump_test.ps1
# 如需调整路径/日期，请修改下方变量。

param()

# ---------------- 用户可调整区域 ----------------
# Windows 下的 CSV 输入路径（导出的 K 线 CSV）
$CSV_PATH   = "C:\\Users\\lc999\\NewAIstock\\AIstock\\qlib_snapshots\\export_daily.csv"
# Windows 下的 bin 输出目录（将写入 qlib 目录结构）
$BIN_DIR    = "C:\\Users\\lc999\\NewAIstock\\AIstock\\qlib_snapshots\\qlib_export_test"
# 导出与校验的日期区间
$START_DATE = "2020-01-01"
$END_DATE   = "2020-01-10"
# WSL 发行版名称
$WSL_DISTRO = "Ubuntu"
# 直接指定 Python 路径（WSL 内）。使用此路径，不再依赖 conda activate。
$PYTHON_BIN = "/home/lc999/miniconda3/envs/rdagent-gpu/bin/python"
# 若需不同 qlib region，可修改为 "cn"、"us" 等
$QLIB_REGION = "cn"
# ------------------------------------------------

function Convert-ToWslPath([string]$winPath) {
  # 将 Windows 路径转换为 /mnt/<drive>/<path> 形式
  $winPath = $winPath -replace "\\", "/"
  if ($winPath -match "^([A-Za-z]):(.*)$") {
    $drive = $matches[1].ToLower()
    $rest  = $matches[2]
    $path = "/mnt/$drive$rest"
    $path = $path -replace "/+", "/"  # 去重斜杠
    return $path
  }
  return $winPath
}

$csvWsl = Convert-ToWslPath $CSV_PATH
$binWsl = Convert-ToWslPath $BIN_DIR

Write-Host "[Info] CSV (Win): $CSV_PATH" -ForegroundColor Cyan
Write-Host "[Info] CSV (WSL): $csvWsl" -ForegroundColor Cyan
Write-Host "[Info] BIN (Win): $BIN_DIR" -ForegroundColor Cyan
Write-Host "[Info] BIN (WSL): $binWsl" -ForegroundColor Cyan

# 确保输出目录存在
if (!(Test-Path $BIN_DIR)) {
  New-Item -ItemType Directory -Path $BIN_DIR | Out-Null
}

# 使用 rdagent-gpu conda 环境的 qlib（避免引用 AIstock 目录下的源码），显式清空 PYTHONPATH
$condaInit = "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && export PYTHONPATH= && cd ~ && "

# step1: CSV -> bin
$cmd1 = $condaInit + "python -m qlib.tools.dump_bin --csv-path `"$csvWsl`" --qlib-dir `"$binWsl`" --freq day --symbol-field instrument --date-field date"
wsl -d $WSL_DISTRO bash -lc $cmd1

# step2: 校验 bin
$pyValidate = @"
import qlib
from qlib.data import D

qlib.init(provider_uri="$binWsl", region="$QLIB_REGION", auto_mount=False)
insts = D.instruments(market="all")
print("instruments size:", len(insts))
df = D.features(insts, ["$close"], start_time="$START_DATE", end_time="$END_DATE", freq="day")
print("features shape:", df.shape)
print(df.head())
"@
$cmd2 = $condaInit + "cat <<'PY' | python`n$pyValidate`nPY"
wsl -d $WSL_DISTRO bash -lc $cmd2

Write-Host "[Done] script finished." -ForegroundColor Green
