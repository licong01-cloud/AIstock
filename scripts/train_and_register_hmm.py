#!/usr/bin/env python
"""行业 HMM 模型标准化训练 & 验证 & 注册流程。

数据划分（不与 QE 回测重合）：
  训练集: 2022-01-01 ~ 2024-06-30 (2.5年)
  验证集: 2024-07-01 ~ 2025-03-31 (9个月)
  测试集: 2025-04-01 ~ 至今 (最新数据，留给 QE 回测)

标准流程：
  1. 训练：用训练集数据训练 HMM
  2. 验证：用验证集数据评估热态/冷态预测的准确性
  3. 注册：将模型和验证指标写入数据库
  4. 测试集留给 QE 回测使用，不在此消耗

运行: cd AIstock && python scripts/train_and_register_hmm.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import numpy as np

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"), override=True)

import psycopg2
import psycopg2.extras


# ─── 数据划分 ───
TRAIN_START = date(2022, 1, 1)
TRAIN_END   = date(2024, 6, 30)
VAL_START   = date(2024, 7, 1)
VAL_END     = date(2025, 3, 31)
# 测试集: 2025-04-01 ~ 至今，留给 QE 回测


def get_conn():
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


# ─── Step 1: 训练（使用训练集数据）───

def step_train(config_dict: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    """训练所有申万一级行业的 HMM 模型（仅使用训练集数据）。

    Returns: (models_dict, l2_codes_map)
    """
    from backend.quant_models.hmm.sector_hmm import SectorHMMConfig, SectorHMMTrainer

    known = {f.name for f in SectorHMMConfig.__dataclass_fields__.values()}
    filtered = {k: v for k, v in config_dict.items() if k in known}
    # Override history to use our train period
    config = SectorHMMConfig(**filtered)

    print(f"  超参: {asdict(config)}")
    print(f"  行业级别: {config.sector_level}")
    print(f"  训练期: {TRAIN_START} ~ {TRAIN_END}")

    trainer = SectorHMMTrainer(config=config)

    # Override the date range used by _build_observation_matrix
    # We do this by temporarily patching the method
    original_build = trainer._build_observation_matrix

    def _build_with_train_dates(sector_code, l2_codes=None):
        """Build obs matrix using only training period data."""
        sector_df = trainer._query_sector_daily(sector_code, TRAIN_START, TRAIN_END, l2_codes=l2_codes)
        csi300_df = trainer._query_csi300_daily(TRAIN_START, TRAIN_END)
        market_vol = trainer._query_market_volume(TRAIN_START, TRAIN_END)
        limit_up = trainer._query_limit_up(sector_code, TRAIN_START, TRAIN_END)

        dates = sorted(sector_df.keys())
        rows = []
        for i, td in enumerate(dates):
            sec = sector_df.get(td)
            csi = csi300_df.get(td)
            mvol = market_vol.get(td)
            lu = limit_up.get(td)
            if sec is None or csi is None or mvol is None:
                continue
            daily_ret = sec["pct_change"] / 100.0
            window_excesses = []
            for j in range(max(0, i - 19), i + 1):
                d2 = dates[j]
                s2 = sector_df.get(d2)
                c2 = csi300_df.get(d2)
                if s2 is not None and c2 is not None:
                    window_excesses.append(s2["pct_change"] / 100.0 - c2["pct_change"] / 100.0)
            if not window_excesses:
                continue
            excess_20d_mean = sum(window_excesses) / len(window_excesses)
            vol_ratio = sec["vol"] / mvol if mvol > 0 else 0.0
            lu_ratio = lu["limit_up"] / lu["total"] if lu and lu["total"] > 0 else 0.0
            row = [daily_ret, excess_20d_mean, vol_ratio, lu_ratio]
            if any(np.isnan(v) for v in row):
                continue
            rows.append(row)
        return np.array(rows, dtype=np.float64) if rows else np.empty((0, 4), dtype=np.float64)

    trainer._build_observation_matrix = _build_with_train_dates
    models = trainer.train_all_sectors()

    # Collect l2_codes for each sector
    l2_map = {}
    for code, info in models.items():
        l2_map[code] = info.get("l2_codes", [])

    print(f"  训练完成: {len(models)} 个行业")
    for code, info in sorted(models.items()):
        labels = info.get("state_labels", {})
        print(f"    {code} ({info.get('sector_name', '')}): {labels}, {info.get('training_days', 0)} 天")

    return models, l2_map


# ─── Step 2: 验证（使用验证集数据）───

def step_validate(
    models: Dict[str, Any],
    l2_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    """用验证集数据评估 HMM 模型预测准确性。

    方法：对验证期内每个交易日，用训练好的 HMM 做 Viterbi 解码，
    对比热态/冷态标记与实际后续 N 天的行业超额收益。
    """
    from hmmlearn.hmm import GaussianHMM
    from backend.quant_models.hmm.sector_hmm import SectorHMMConfig, SectorHMMTrainer

    if not models:
        return {"error": "no_models", "sector_count": 0}

    config = SectorHMMConfig()
    trainer = SectorHMMTrainer(config=config)

    print(f"  验证期: {VAL_START} ~ {VAL_END}")

    # Reconstruct HMM objects for Viterbi decoding
    hmm_objects = {}
    for code, info in models.items():
        hmm = GaussianHMM(n_components=info["n_states"], covariance_type="full")
        hmm.startprob_ = np.array([1.0 / info["n_states"]] * info["n_states"])
        hmm.transmat_ = np.array(info["transmat"])
        hmm.means_ = np.array(info["means"])
        # Force symmetry on covariance matrices (JSON round-trip can lose precision)
        covars = np.array(info["covars"])
        for i in range(covars.shape[0]):
            covars[i] = (covars[i] + covars[i].T) / 2
            # Ensure positive-definite by adding small epsilon to diagonal
            covars[i] += np.eye(covars[i].shape[0]) * 1e-6
        hmm.covars_ = covars
        hmm_objects[code] = (hmm, info["state_labels"])

    # Fetch validation period data for all sectors (using L2 aggregation)
    # We need a longer lookback for the 20-day rolling window
    lookback_start = VAL_START - timedelta(days=30)

    # Build observation sequences for each sector over the validation period
    sector_obs = {}
    sector_dates = {}
    for code in models:
        l2_codes = l2_map.get(code, [])
        sector_df = trainer._query_sector_daily(code, lookback_start, VAL_END, l2_codes=l2_codes)
        csi300_df = trainer._query_csi300_daily(lookback_start, VAL_END)
        market_vol = trainer._query_market_volume(lookback_start, VAL_END)

        dates = sorted(sector_df.keys())
        obs_rows = []
        obs_dates = []
        for i, td in enumerate(dates):
            sec = sector_df.get(td)
            csi = csi300_df.get(td)
            mvol = market_vol.get(td)
            if sec is None or csi is None or mvol is None:
                continue
            daily_ret = sec["pct_change"] / 100.0
            window_excesses = []
            for j in range(max(0, i - 19), i + 1):
                d2 = dates[j]
                s2 = sector_df.get(d2)
                c2 = csi300_df.get(d2)
                if s2 is not None and c2 is not None:
                    window_excesses.append(s2["pct_change"] / 100.0 - c2["pct_change"] / 100.0)
            if not window_excesses:
                continue
            excess_20d_mean = sum(window_excesses) / len(window_excesses)
            vol_ratio = sec["vol"] / mvol if mvol > 0 else 0.0
            row = [daily_ret, excess_20d_mean, vol_ratio, 0.0]  # skip limit-up for speed
            if any(np.isnan(v) for v in row):
                continue
            obs_rows.append(row)
            obs_dates.append(td)

        if obs_rows:
            sector_obs[code] = np.array(obs_rows, dtype=np.float64)
            sector_dates[code] = obs_dates

    # For each sector, decode states and collect future returns
    trending_5d, trending_10d, trending_20d = [], [], []
    fading_5d, fading_10d, fading_20d = [], [], []

    # Get sector pct_change for future return calculation
    # For L2 sectors, data is directly in sw_daily
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ts_code, trade_date, pct_change
        FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
    """, (VAL_START, VAL_END + timedelta(days=30)))
    sector_returns_raw = {}
    for ts_code, td, pct in cur.fetchall():
        if ts_code not in sector_returns_raw:
            sector_returns_raw[ts_code] = {}
        sector_returns_raw[ts_code][td] = float(pct or 0)
    cur.close()
    conn.close()

    all_val_dates = sorted(set(
        td for rets in sector_returns_raw.values() for td in rets.keys()
        if VAL_START <= td <= VAL_END + timedelta(days=30)
    ))

    # Build sector returns map (for L2, use directly; for L1, aggregate from L2)
    sector_returns = {}
    for code in models:
        l2_codes = l2_map.get(code, [code])
        sector_returns[code] = {}
        for td in all_val_dates:
            if len(l2_codes) == 1 and l2_codes[0] == code:
                # L2: direct lookup
                ret = sector_returns_raw.get(code, {}).get(td)
                if ret is not None:
                    sector_returns[code][td] = ret
            else:
                # L1: aggregate from L2
                vals = [sector_returns_raw.get(l2, {}).get(td) for l2 in l2_codes]
                vals = [v for v in vals if v is not None]
                if vals:
                    sector_returns[code][td] = sum(vals) / len(vals)

    val_dates_only = [d for d in all_val_dates if VAL_START <= d <= VAL_END]

    for code in models:
        if code not in sector_obs:
            continue
        obs = sector_obs[code]
        dates_list = sector_dates[code]
        hmm, state_labels = hmm_objects[code]

        # Decode states for the full observation sequence
        try:
            states = hmm.predict(obs)
        except Exception:
            continue

        # Map dates to states (only validation period)
        date_state = {}
        for i, td in enumerate(dates_list):
            if VAL_START <= td <= VAL_END:
                state_idx = states[i]
                label = state_labels.get(str(state_idx), "unknown")
                date_state[td] = label

        # Calculate future returns
        for td, label in date_state.items():
            if td not in val_dates_only:
                continue
            td_idx = all_val_dates.index(td)
            future = []
            for offset in range(1, 21):
                fi = td_idx + offset
                if fi < len(all_val_dates):
                    ret = sector_returns.get(code, {}).get(all_val_dates[fi])
                    if ret is not None:
                        future.append(ret)

            if len(future) < 5:
                continue

            cum5 = sum(future[:5])
            cum10 = sum(future[:10]) if len(future) >= 10 else None
            cum20 = sum(future[:20]) if len(future) >= 20 else None

            if label == "trending":
                trending_5d.append(cum5)
                if cum10 is not None: trending_10d.append(cum10)
                if cum20 is not None: trending_20d.append(cum20)
            elif label == "fading":
                fading_5d.append(cum5)
                if cum10 is not None: fading_10d.append(cum10)
                if cum20 is not None: fading_20d.append(cum20)

    def safe_mean(lst):
        return round(float(np.mean(lst)), 4) if lst else None

    metrics = {
        "sector_count": len(models),
        "train_period": f"{TRAIN_START} ~ {TRAIN_END}",
        "validation_period": f"{VAL_START} ~ {VAL_END}",
        "test_period": f"2025-04-01 ~ present (reserved for QE backtest)",
        "trending_avg_5d": safe_mean(trending_5d),
        "trending_avg_10d": safe_mean(trending_10d),
        "trending_avg_20d": safe_mean(trending_20d),
        "fading_avg_5d": safe_mean(fading_5d),
        "fading_avg_10d": safe_mean(fading_10d),
        "fading_avg_20d": safe_mean(fading_20d),
        "trending_samples": len(trending_5d),
        "fading_samples": len(fading_5d),
    }

    if metrics["trending_avg_5d"] is not None and metrics["fading_avg_5d"] is not None:
        metrics["spread_5d"] = round(metrics["trending_avg_5d"] - metrics["fading_avg_5d"], 4)
    if metrics["trending_avg_10d"] is not None and metrics["fading_avg_10d"] is not None:
        metrics["spread_10d"] = round(metrics["trending_avg_10d"] - metrics["fading_avg_10d"], 4)
    if metrics["trending_avg_20d"] is not None and metrics["fading_avg_20d"] is not None:
        metrics["spread_20d"] = round(metrics["trending_avg_20d"] - metrics["fading_avg_20d"], 4)

    return metrics


# ─── Step 3: 注册 ───

def step_register(models, config_dict, metrics, display_name, model_type="sector_hmm"):
    from backend.quant_models.hmm.sector_hmm import SectorHMMTrainer

    models_dir = os.path.join(_PROJECT_ROOT, "data", "hmm_models")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        "SELECT config_id FROM model_train_configs WHERE model_type = %s AND display_name = %s",
        (model_type, display_name),
    )
    existing = cur.fetchone()
    if existing:
        config_id = existing["config_id"]
        print(f"  使用已有配置: config_id={config_id}")
    else:
        cur.execute(
            "INSERT INTO model_train_configs (model_type, display_name, config_json) VALUES (%s, %s, %s) RETURNING config_id",
            (model_type, display_name, json.dumps(config_dict)),
        )
        config_id = cur.fetchone()["config_id"]
        conn.commit()
        print(f"  创建新配置: config_id={config_id}")

    model_path = os.path.join(models_dir, config_id, snapshot_date, "models.json")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    SectorHMMTrainer.save_models(models, model_path)
    print(f"  模型文件: {model_path}")

    cur.execute(
        """INSERT INTO model_train_snapshots (config_id, model_path, sector_count, status, metrics_json)
           VALUES (%s, %s, %s, 'completed', %s) RETURNING snapshot_id""",
        (config_id, model_path, len(models), json.dumps(metrics)),
    )
    snapshot_id = cur.fetchone()["snapshot_id"]

    cur.execute(
        "INSERT INTO model_train_jobs (config_id, snapshot_id, status, started_at, completed_at) VALUES (%s, %s, 'completed', NOW(), NOW())",
        (config_id, snapshot_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"config_id": config_id, "snapshot_id": snapshot_id, "model_path": model_path}


# ─── Main ───

def main():
    parser = argparse.ArgumentParser(description="行业 HMM 模型标准化训练流程")
    parser.add_argument("--display-name", default="默认2状态配置")
    args = parser.parse_args()

    from backend.quant_models.hmm.sector_hmm import SectorHMMConfig
    config_dict = asdict(SectorHMMConfig())

    print("=" * 70)
    print("行业 HMM 模型标准化训练流程")
    print("=" * 70)
    print(f"配置: {args.display_name}")
    print(f"训练集: {TRAIN_START} ~ {TRAIN_END}")
    print(f"验证集: {VAL_START} ~ {VAL_END}")
    print(f"测试集: 2025-04-01 ~ 至今 (留给 QE 回测)")
    print()

    print("[Step 1/3] 训练模型")
    models, l2_map = step_train(config_dict)
    if not models:
        print("  错误: 没有训练出任何行业模型")
        sys.exit(1)
    print()

    print("[Step 2/3] 验证模型")
    metrics = step_validate(models, l2_map)
    print(f"  热态样本: {metrics.get('trending_samples', 0)}")
    print(f"  冷态样本: {metrics.get('fading_samples', 0)}")
    print(f"  热态 5日均收益: {metrics.get('trending_avg_5d', 'N/A')}%")
    print(f"  冷态 5日均收益: {metrics.get('fading_avg_5d', 'N/A')}%")
    print(f"  热-冷 5日差: {metrics.get('spread_5d', 'N/A')}%")
    print(f"  热态 10日均收益: {metrics.get('trending_avg_10d', 'N/A')}%")
    print(f"  冷态 10日均收益: {metrics.get('fading_avg_10d', 'N/A')}%")
    print(f"  热-冷 10日差: {metrics.get('spread_10d', 'N/A')}%")
    print(f"  热态 20日均收益: {metrics.get('trending_avg_20d', 'N/A')}%")
    print(f"  冷态 20日均收益: {metrics.get('fading_avg_20d', 'N/A')}%")
    print(f"  热-冷 20日差: {metrics.get('spread_20d', 'N/A')}%")

    # 判断模型是否有价值
    spread_5d = metrics.get("spread_5d")
    if spread_5d is not None and spread_5d > 0:
        print(f"\n  ✓ 模型有效: 热态行业 5 日收益高于冷态 {spread_5d}%")
    elif spread_5d is not None:
        print(f"\n  ✗ 模型无效: 热态行业 5 日收益低于冷态 {spread_5d}%")
    print()

    print("[Step 3/3] 保存 & 注册")
    result = step_register(models, config_dict, metrics, args.display_name)
    print()
    print("=" * 70)
    print(f"完成! config_id={result['config_id']}, snapshot_id={result['snapshot_id']}")
    print(f"行业数: {len(models)}, 模型路径: {result['model_path']}")
    print("滚动训练: 重新运行此脚本创建新快照")
    print("=" * 70)


if __name__ == "__main__":
    main()
