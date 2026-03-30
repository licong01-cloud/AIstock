#!/bin/bash
mkdir -p /home/lc999/data/rl_logs
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/train.py --config rl_execution/config/train_ppo_v9.yaml > /home/lc999/data/rl_logs/train_v9_wsl.log 2>&1
