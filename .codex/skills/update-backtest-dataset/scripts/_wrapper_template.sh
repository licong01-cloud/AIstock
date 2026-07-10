#!/bin/bash
# 回测数据集更新 — 通用 wrapper 模板(避坑样板)。复制后填入具体 python 命令。
# 用法: wsl -d Ubuntu -- bash -lc "sed -i 's/\r$//' /mnt/c/.../x.sh; bash /mnt/c/.../x.sh"
# 注意: 勿 set -u (破坏 conda cuda_env.sh); 勿内联带 $VAR 到 wsl -lc (不展开)。
cd /mnt/f/Dev/AIstock || exit 1
set -a
# .env 是 CRLF+GBK: 只按需取 TDX_DB_*, 去回车, 勿整体 source
eval "$(grep -E '^TDX_DB_[A-Z_]*=' .env | tr -d '\r' | sed 's/^/export /')"
set +a
export TDX_API_BASE=http://localhost:19080 PYTHONPATH=/mnt/f/Dev/AIstock PYTHONWARNINGS=ignore
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
source /home/lc999/miniconda3/etc/profile.d/conda.sh
conda activate rdagent-gpu
echo "=== START $(date +%F_%H:%M:%S) ==="

# ---- 在此填入具体命令, 例如日线导出 (CUTOFF 硬编码, 勿用未展开变量) ----
# python scripts/qlib_authoritative_bin_export.py --dataset stock_daily --stage all \
#   --snapshot-id qlib_bin_st_pit_active_daily_candidate_20180801_20260630 \
#   --start 2018-08-01 --end 2026-06-30 --basis-start 2018-08-01 --basis-end 2026-06-30 \
#   --stock-universe-mode pit_spans --universe-key shsz_st_pit_active_v1 \
#   --exchanges sh,sz --bin-root /home/lc999/data

echo "=== EXIT=$? $(date +%F_%H:%M:%S) ==="
