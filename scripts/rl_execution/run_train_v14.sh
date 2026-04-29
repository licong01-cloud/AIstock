#!/bin/bash
# RL Layer B v14 训练启动脚本
# 改动: Sequence BPTT + TWAP reward + 24动作 + 分离optimizer + value clip
set -e
cd /mnt/f/Dev/AIstock

export PYTHONPATH="/mnt/f/Dev/AIstock:$PYTHONPATH"

if [ -f /mnt/f/Dev/AIstock/.env ]; then
    set -a; source <(sed 's/\r$//' /mnt/f/Dev/AIstock/.env); set +a
fi

CONDA_BIN=/home/lc999/miniconda3/envs/rdagent-gpu/bin/python
CONFIG=/mnt/f/Dev/AIstock/rl_execution/config/train_ppo_v14.yaml
LOG=/mnt/f/Dev/AIstock/rl_data/train_v14.log

mkdir -p /mnt/f/Dev/AIstock/rl_data/checkpoints_v14

echo "Starting v14 training at $(date)" >> "$LOG"

nohup $CONDA_BIN -u /mnt/f/Dev/AIstock/scripts/rl_execution/train_v14.py \
    --config "$CONFIG" \
    >> "$LOG" 2>> "$LOG" &

PID=$!
echo "PID: $PID" | tee -a "$LOG"
echo "Log: $LOG"

sleep 3
if kill -0 $PID 2>/dev/null; then
    echo "Process $PID running OK"
else
    echo "ERROR: Process exited immediately, check $LOG"
    tail -20 "$LOG"
fi
