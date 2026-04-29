#!/bin/bash
mkdir -p /home/lc999/data/rl_logs
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/train.py --config rl_execution/config/train_ppo_v10.yaml > /mnt/f/Dev/AIstock/rl_data/train_v10.log 2>&1
