#!/usr/bin/env python
"""在测试集（2025-04-01 ~ 至今）上评估已训练的 L2 HMM 模型效果。"""
from __future__ import annotations
import json, os, sys
import numpy as np
from datetime import date, timedelta

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"), override=True)

import psycopg2, psycopg2.extras
from hmmlearn.hmm import GaussianHMM

# 模型路径（L2 二级行业模型）
MODEL_PATH = os.path.join(
    _PROJECT_ROOT, "data", "hmm_models",
    "7be4ca8e-58ca-4c7f-8f87-a2af6b073ab8", "2026-04-01", "models.json"
)

TEST_START = date(2025, 4, 1)
TEST_END = date(2026, 4, 1)  # 至今


def get_conn():
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


def main():
    print("=" * 70)
    print("HMM L2 模型测试集评估")
    print(f"测试期: {TEST_START} ~ {TEST_END}")
    print(f"模型: {MODEL_PATH}")
    print("=" * 70)

    # Load model
    with open(MODEL_PATH, "r", encoding="utf-8") as f:
        models = json.load(f)
    print(f"加载 {len(models)} 个行业模型")

    # Reconstruct HMM objects
    hmm_objects = {}
    for code, info in models.items():
        hmm = GaussianHMM(n_components=info["n_states"], covariance_type="full")
        hmm.startprob_ = np.array([1.0 / info["n_states"]] * info["n_states"])
        hmm.transmat_ = np.array(info["transmat"])
        hmm.means_ = np.array(info["means"])
        covars = np.array(info["covars"])
        for i in range(covars.shape[0]):
            covars[i] = (covars[i] + covars[i].T) / 2
            covars[i] += np.eye(covars[i].shape[0]) * 1e-6
        hmm.covars_ = covars
        hmm_objects[code] = (hmm, info["state_labels"])

    # Fetch test period data
    conn = get_conn()
    cur = conn.cursor()

    lookback_start = TEST_START - timedelta(days=30)

    # Sector daily data
    cur.execute("""
        SELECT ts_code, trade_date, pct_change, vol, amount
        FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY ts_code, trade_date
    """, (lookback_start, TEST_END))

    sector_data = {}
    for ts_code, td, pct, vol, amt in cur.fetchall():
        if ts_code not in sector_data:
            sector_data[ts_code] = []
        sector_data[ts_code].append({
            "date": td, "pct_change": float(pct or 0),
            "vol": float(vol or 0), "amount": float(amt or 0),
        })

    # CSI300 data
    cur.execute("""
        SELECT trade_date, pct_chg FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (lookback_start, TEST_END))
    csi300 = {td: float(pct or 0) for td, pct in cur.fetchall()}

    # Market total volume
    cur.execute("""
        SELECT trade_date, SUM(vol) FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date ORDER BY trade_date
    """, (lookback_start, TEST_END))
    market_vol = {td: float(v or 0) for td, v in cur.fetchall()}

    cur.close()
    conn.close()

    # Build observation sequences and decode states
    trending_5d, trending_10d, trending_20d = [], [], []
    fading_5d, fading_10d, fading_20d = [], [], []

    all_dates = sorted(set(td for td, _ in csi300.items() if TEST_START <= td <= TEST_END))
    sector_pct = {}  # {code: {date: pct_change}}
    for code in models:
        if code not in sector_data:
            continue
        sector_pct[code] = {r["date"]: r["pct_change"] for r in sector_data[code]}

    decoded_count = 0
    for code in models:
        if code not in sector_data:
            continue

        rows = sector_data[code]
        dates_list = [r["date"] for r in rows]

        # Build obs matrix
        obs_rows = []
        obs_dates = []
        for i, r in enumerate(rows):
            td = r["date"]
            csi_pct = csi300.get(td)
            mvol = market_vol.get(td)
            if csi_pct is None or mvol is None:
                continue
            daily_ret = r["pct_change"] / 100.0
            # 20-day rolling excess return
            window = []
            for j in range(max(0, i - 19), i + 1):
                r2 = rows[j]
                c2 = csi300.get(r2["date"])
                if c2 is not None:
                    window.append(r2["pct_change"] / 100.0 - c2 / 100.0)
            if not window:
                continue
            excess = sum(window) / len(window)
            vol_ratio = r["vol"] / mvol if mvol > 0 else 0
            obs_rows.append([daily_ret, excess, vol_ratio, 0.0])
            obs_dates.append(td)

        if len(obs_rows) < 10:
            continue

        obs = np.array(obs_rows, dtype=np.float64)
        hmm, state_labels = hmm_objects[code]

        try:
            states = hmm.predict(obs)
        except Exception:
            continue

        decoded_count += 1

        # Collect future returns for test period dates
        for i, td in enumerate(obs_dates):
            if td < TEST_START:
                continue
            label = state_labels.get(str(states[i]), "unknown")
            td_idx = all_dates.index(td) if td in all_dates else -1
            if td_idx < 0:
                continue

            future = []
            for offset in range(1, 21):
                fi = td_idx + offset
                if fi < len(all_dates):
                    ret = sector_pct.get(code, {}).get(all_dates[fi])
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

    print(f"\n解码行业数: {decoded_count}/{len(models)}")
    print(f"测试期交易日: {len(all_dates)}")
    print(f"\n{'指标':<25} {'热态':>10} {'冷态':>10} {'差值':>10}")
    print("-" * 60)

    for label, t_list, f_list in [
        ("5日累计收益(%)", trending_5d, fading_5d),
        ("10日累计收益(%)", trending_10d, fading_10d),
        ("20日累计收益(%)", trending_20d, fading_20d),
    ]:
        t_val = safe_mean(t_list)
        f_val = safe_mean(f_list)
        spread = round(t_val - f_val, 4) if t_val is not None and f_val is not None else None
        print(f"{label:<25} {t_val or 'N/A':>10} {f_val or 'N/A':>10} {spread or 'N/A':>10}")

    print(f"\n热态样本: {len(trending_5d)}, 冷态样本: {len(fading_5d)}")

    spread_5d = safe_mean(trending_5d) - safe_mean(fading_5d) if trending_5d and fading_5d else None
    if spread_5d is not None and spread_5d > 0:
        print(f"\n✓ 测试集验证通过: 热态行业 5 日收益高于冷态 {spread_5d:.4f}%")
    elif spread_5d is not None:
        print(f"\n✗ 测试集验证失败: 热态行业 5 日收益低于冷态 {spread_5d:.4f}%")
    else:
        print("\n⚠ 数据不足，无法验证")


if __name__ == "__main__":
    main()
