#!/bin/bash
set -e
cd /mnt/f/Dev/AIstock

PYTHON=/home/lc999/miniconda3/envs/rdagent-gpu/bin/python
V23_DIR=/home/lc999/data/rl_models/v23
LOG=$V23_DIR/v23_pipeline.log

echo "=== v23 Pipeline Start: $(date) ===" | tee $LOG

# Step 1: Data generation
echo "--- Step 1: Data Generation ---" | tee -a $LOG
$PYTHON -u scripts/rl_execution/v23_gen_correction_data.py \
  --order-dir /home/lc999/data/rl_orders \
  --pickle-dir /home/lc999/data/rl_backtest \
  --v19-model /home/lc999/data/rl_models/v19_plan/v19_best.pt \
  --output $V23_DIR/correction_data.npz \
  --max-orders 50000 \
  --device cpu \
  2>&1 | tee -a $LOG

# Step 2: SL Training
echo "--- Step 2: SL Training ---" | tee -a $LOG
$PYTHON -u scripts/rl_execution/v23_train_correction.py \
  --data $V23_DIR/correction_data.npz \
  --output $V23_DIR/v23_correction_sl.pt \
  --device cuda \
  --epochs 30 \
  --patience 5 \
  2>&1 | tee -a $LOG

# Step 3: Evaluation
echo "--- Step 3: Evaluation ---" | tee -a $LOG
$PYTHON -u scripts/rl_execution/v23_evaluate.py \
  --plan-model /home/lc999/data/rl_models/v19_plan/v19_best.pt \
  --correction-model $V23_DIR/v23_correction_sl.pt \
  --order-dir /home/lc999/data/rl_orders \
  --pickle-dir /home/lc999/data/rl_backtest \
  --n-eval 10000 \
  2>&1 | tee -a $LOG

echo "=== v23 Pipeline Done: $(date) ===" | tee -a $LOG
