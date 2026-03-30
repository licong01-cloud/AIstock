#!/bin/bash
# RL Layer B v11 训练启动脚本
# 改动: 大盘特征 3维→6维 (STATE_DIM 29→32)
set -e
cd /mnt/f/Dev/AIstock

export PYTHONPATH="/mnt/f/Dev/AIstock:$PYTHONPATH"

if [ -f /mnt/f/Dev/AIstock/.env ]; then
    set -a; source <(sed 's/\r$//' /mnt/f/Dev/AIstock/.env); set +a
fi

CONDA_BIN=/home/lc999/miniconda3/envs/rdagent-gpu/bin/python
CONFIG=/mnt/f/Dev/AIstock/rl_execution/config/train_ppo_v11.yaml
LOG=/mnt/f/Dev/AIstock/rl_data/train_v11.log

mkdir -p /mnt/f/Dev/AIstock/rl_data/checkpoints_v11

echo "Starting v11 training at $(date)" >> "$LOG"

nohup $CONDA_BIN /mnt/f/Dev/AIstock/scripts/rl_execution/train.py \
    --config "$CONFIG" \
    >> "$LOG" 2>> "$LOG" &

PID=$!
echo "PID: $PID" | tee -a "$LOG"
echo "Log: $LOG"

# 等待3秒确认进程未立即退出
sleep 3
if kill -0 $PID 2>/dev/null; then
    echo "Process $PID running OK"
else
    echo "ERROR: Process exited immediately, check $LOG"
    tail -20 "$LOG"
fi
