#!/bin/bash
# v22 Decision Transformer 训练流程
# 在 WSL 环境下运行

set -e

ORDER_DIR="/home/lc999/data/rl_orders"
PICKLE_DIR="/home/lc999/data/rl_backtest"
OUTPUT_DIR="/home/lc999/data/rl_models/v22_dt"
DT_DATA="${OUTPUT_DIR}/dt_trajectories.pkl"

mkdir -p ${OUTPUT_DIR}

echo "=========================================="
echo "v22 Decision Transformer Training Pipeline"
echo "=========================================="

# Step 1: 生成DT训练数据 (DP最优轨迹 → 序列格式)
echo ""
echo "[Step 1/2] Generating DT training data..."
python scripts/rl_execution/v22_gen_dt_data.py \
    --order-dir ${ORDER_DIR} \
    --pickle-dir ${PICKLE_DIR} \
    --output ${DT_DATA} \
    --max-orders 200000 \
    --workers 8

# Step 2: 训练 Decision Transformer
echo ""
echo "[Step 2/2] Training Decision Transformer..."
python scripts/rl_execution/v22_train_dt.py \
    --data ${DT_DATA} \
    --output-dir ${OUTPUT_DIR} \
    --epochs 100 \
    --batch-size 256 \
    --lr 1e-4 \
    --context-len 60 \
    --n-layers 4 \
    --n-heads 4 \
    --n-embd 128 \
    --pa-threshold -5.0 \
    --patience 15

# Step 3: 评估
echo ""
echo "[Evaluate] Running evaluation..."
python scripts/rl_execution/v22_evaluate.py \
    --model ${OUTPUT_DIR}/dt_best.pt \
    --order-dir ${ORDER_DIR} \
    --pickle-dir ${PICKLE_DIR} \
    --target-rtg 15.0 \
    --n-eval 10000

echo ""
echo "Done! Model saved to ${OUTPUT_DIR}/dt_best.pt"
