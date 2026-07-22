#!/usr/bin/env bash
# DEV acceptance worker, bounded --drain mode (NEVER touches production).
# Usage: dev_worker.sh <zerocopy|cold|warm|cold10|warm10> [extra worker args...]
#
# cold/warm semantics (approved ruling 3):
#   cold*  = task-scoped EMPTY prediction-store root + task-scoped cache dir
#            -> artifacts are downloaded from the QE workspace in this run
#   warm*  = SAME roots as the matching cold run, after cold completed
#            -> artifacts resolve from the workspace cache without download
#   zerocopy = production-normal prediction store root (zero-copy bypass)
set -euo pipefail
MODE="${1:?mode required: zerocopy|cold|warm|cold10|warm10}"
shift
cd "$(dirname "$0")/../.."

ROOT=F:/Dev/hmm_acceptance_20260721

export TDX_DB_HOST=127.0.0.1
export TDX_DB_PORT=5433
export TDX_DB_NAME=aistock_dev
export TDX_DB_USER=postgres
export TDX_DB_PASSWORD=rRA8jgnD1HTy3MlIyXw1rnkjYFmxuiK1
export HMM_EVOLUTION_RUNTIME_MODE=api_worker
export PYTHONIOENCODING=utf-8
# authoritative AIstock-owned filtered stock-pool cache (read-only; the exact
# files the QE tasks consumed, with sha256 verified against ST-PIT receipts)
export STOCK_POOL_OUTPUT_DIR=F:/Dev/AIstock/stock_pools

case "$MODE" in
  zerocopy)
    export AISTOCK_PREDICTION_STORE_ROOT=F:/Dev/AIstock/rdagent_assets/prediction_store
    export HMM_EVOLUTION_ARTIFACT_CACHE_DIR=$ROOT/cache_zerocopy
    ;;
  cold)
    export AISTOCK_PREDICTION_STORE_ROOT=$ROOT/store_cold1
    export HMM_EVOLUTION_ARTIFACT_CACHE_DIR=$ROOT/cache_cold1
    ;;
  warm)
    export AISTOCK_PREDICTION_STORE_ROOT=$ROOT/store_cold1
    export HMM_EVOLUTION_ARTIFACT_CACHE_DIR=$ROOT/cache_cold1
    ;;
  cold10)
    export AISTOCK_PREDICTION_STORE_ROOT=$ROOT/store_cold10
    export HMM_EVOLUTION_ARTIFACT_CACHE_DIR=$ROOT/cache_cold10
    ;;
  warm10)
    export AISTOCK_PREDICTION_STORE_ROOT=$ROOT/store_cold10
    export HMM_EVOLUTION_ARTIFACT_CACHE_DIR=$ROOT/cache_cold10
    ;;
  *)
    echo "unknown mode: $MODE" >&2
    exit 2
    ;;
esac

mkdir -p "$AISTOCK_PREDICTION_STORE_ROOT" "$HMM_EVOLUTION_ARTIFACT_CACHE_DIR"
exec python scripts/hmm_evolution_worker.py --drain --max-jobs 50 \
  --owner-id "hmm-dev-acceptance-$MODE" "$@"
