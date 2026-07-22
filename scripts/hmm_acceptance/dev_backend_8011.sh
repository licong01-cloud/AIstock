#!/usr/bin/env bash
# DEV acceptance backend on 127.0.0.1:8011 (NEVER production 8001).
# All config comes from explicit process env; the worktree has no .env.
set -euo pipefail
cd "$(dirname "$0")/../.."

# hard guard: refuse to start if a production port argument sneaks in
for arg in "$@"; do
  case "$arg" in
    *8001*|*3000*|*19080*)
      echo "REFUSE: production port in arguments: $arg" >&2
      exit 2
      ;;
  esac
done

export TDX_DB_HOST=127.0.0.1
export TDX_DB_PORT=5433
export TDX_DB_NAME=aistock_dev
export TDX_DB_USER=postgres
export TDX_DB_PASSWORD=rRA8jgnD1HTy3MlIyXw1rnkjYFmxuiK1

export HMM_EVOLUTION_RUNTIME_MODE=api_worker
# production-normal prediction store (read-only usage by HMM preparation)
export AISTOCK_PREDICTION_STORE_ROOT=F:/Dev/AIstock/rdagent_assets/prediction_store
export STOCK_POOL_OUTPUT_DIR=F:/Dev/AIstock/stock_pools

# no side effects: no QMT, no schedulers
export MINIQMT_ENABLED=false
export DISABLE_INGESTION_SCHEDULER=1
export DISABLE_STRATEGY_SCHEDULER=1
export DISABLE_NODE_HEALTH_SCHEDULER=1
# Windows GBK console cannot encode the app's unicode log glyphs
export PYTHONIOENCODING=utf-8

exec python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
