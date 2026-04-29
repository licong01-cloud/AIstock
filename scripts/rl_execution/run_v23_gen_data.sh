#!/bin/bash
cd /mnt/f/Dev/AIstock

echo "Starting v23 data generation at $(date)"

/home/lc999/miniconda3/envs/rdagent-gpu/bin/python -u scripts/rl_execution/v23_gen_correction_data.py \
  --order-dir /home/lc999/data/rl_orders \
  --pickle-dir /home/lc999/data/rl_backtest \
  --v19-model /home/lc999/data/rl_models/v19_plan/v19_best.pt \
  --output /home/lc999/data/rl_models/v23/correction_data.npz \
  --max-orders 200000 \
  --device cpu

echo "Finished at $(date)"
