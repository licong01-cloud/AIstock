#!/bin/bash
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/train.py \
  --config rl_execution/config/train_ppo_v7.yaml \
  2>&1 | tee /mnt/f/Dev/AIstock/rl_data/train_v7.log
