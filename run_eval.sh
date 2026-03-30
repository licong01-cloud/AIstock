#!/bin/bash
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/evaluate.py \
  --policy /mnt/f/Dev/AIstock/rl_data/checkpoints_v5/policy_best.pt \
  --config /mnt/f/Dev/AIstock/rl_execution/config/train_ppo_v5.yaml \
  --n-episodes 3000 \
  > /mnt/f/Dev/AIstock/rl_data/eval_v5.log 2>&1
