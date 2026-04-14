#!/bin/bash
pkill -f train_ppo_v10.yaml 2>/dev/null && echo 'v10 killed' || echo 'v10 not found'
pkill -f train_ppo_v11.yaml 2>/dev/null && echo 'v11 killed' || echo 'v11 not found'
sleep 2
ps aux | grep train.py | grep -v grep || echo 'No train processes running'
