#!/bin/bash
mkdir -p /home/lc999/data/rl_sector_stats
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/gen_sector_stats.py \
    --pickle-dir /home/lc999/data/rl_backtest \
    --output-dir /home/lc999/data/rl_sector_stats \
    >> /home/lc999/data/rl_sector_stats/gen.log 2>&1
