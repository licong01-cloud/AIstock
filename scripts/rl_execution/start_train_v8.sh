#!/bin/bash
cd /mnt/f/Dev/AIstock
/home/lc999/miniconda3/envs/rdagent-gpu/bin/python scripts/rl_execution/train.py --config rl_execution/config/train_ppo_v8.yaml > /mnt/f/Dev/AIstock/rl_data/train_v8_wsl.log 2>&1
