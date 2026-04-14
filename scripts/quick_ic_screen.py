#!/usr/bin/env python3
"""
快速 IC 筛选脚本 — 因子开发两阶段流程的 Stage 1

用途: 因子执行完 result.h5 后，30秒内判断是否值得进入全量指标计算
原理: 复用 rdagent 的 qlib_data_reader 加载 close，只算 out_sample 窗口的 IC

IC 计算方式与全量引擎一致:
  - Pearson IC: 因子+收益均做 RobustZScore (中位数+MAD, clip=3) 后 Pearson 相关
  - Rank IC: 因子+收益均做 rank 后 Pearson 相关 (= Spearman)

判定标准 (综合 IC 和 Rank IC):
  - PASS: |IC| >= 0.015 且 |Rank IC| >= 0.015
  - MARGINAL: |IC| >= 0.005 或 |Rank IC| >= 0.010
  - KILL: 其余

用法:
  python quick_ic_screen.py /home/lc999/factor_workspace/_factor_m_xxx
  python quick_ic_screen.py dir1 dir2 dir3  # 批量快筛
"""
import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import rankdata

sys.path.insert(0, "/mnt/f/Dev/RD-Agent-main")
from rdagent.app.factor_metrics.qlib_data_reader import read_close_prices

# === 配置 ===
OUT_SAMPLE_START = "2024-07-01"
IC_PASS = 0.015       # |IC| >= 此值 且 |Rank IC| >= 此值 → PASS
IC_MARGINAL = 0.005   # |IC| >= 此值 或 |Rank IC| >= 0.010 → MARGINAL


def _robust_zscore_row(arr: np.ndarray, clip: float = 3.0) -> np.ndarray:
    """RobustZScore: (x - median) / (1.4826 * MAD), clipped to [-clip, clip].
    与 engine.py 的 _robust_zscore_matrix 一致，但按单行操作。"""
    med = np.nanmedian(arr)
    mad = np.nanmedian(np.abs(arr - med))
    scale = 1.4826 * mad if mad > 1e-12 else 1.0
    z = (arr - med) / scale
    return np.clip(z, -clip, clip)


def quick_ic(result_h5_path: Path, close_unstacked: pd.DataFrame,
             fwd_ret: pd.DataFrame) -> dict:
    """计算单个因子的快速 IC (与全量引擎一致的计算方式)"""
    t0 = time.time()

    df = pd.read_hdf(result_h5_path)
    factor_name = df.columns[0]
    factor_wide = df[factor_name].unstack("instrument")

    oos_start = pd.Timestamp(OUT_SAMPLE_START)
    factor_oos = factor_wide.loc[factor_wide.index >= oos_start]
    fwd_oos = fwd_ret.loc[fwd_ret.index >= oos_start]

    common_dates = factor_oos.index.intersection(fwd_oos.index)
    common_insts = factor_oos.columns.intersection(fwd_oos.columns)

    if len(common_dates) < 20 or len(common_insts) < 100:
        return {
            "factor_name": factor_name,
            "verdict": "KILL",
            "reason": f"insufficient data: {len(common_dates)} dates, {len(common_insts)} stocks",
            "elapsed_sec": round(time.time() - t0, 2),
        }

    f_arr = factor_oos.loc[common_dates, common_insts].values
    r_arr = fwd_oos.loc[common_dates, common_insts].values

    ics_robust = []  # RobustZScore Pearson IC (与引擎一致)
    ics_rank = []    # Rank IC (Spearman)

    for i in range(len(common_dates)):
        f_row = f_arr[i]
        r_row = r_arr[i]
        mask = np.isfinite(f_row) & np.isfinite(r_row)
        if mask.sum() < 50:
            continue

        fv = f_row[mask]
        rv = r_row[mask]

        # RobustZScore Pearson IC (与 engine.py 一致)
        fv_z = _robust_zscore_row(fv)
        rv_z = _robust_zscore_row(rv)
        ic = np.corrcoef(fv_z, rv_z)[0, 1]
        if np.isfinite(ic):
            ics_robust.append(ic)

        # Rank IC (Spearman)
        ric = np.corrcoef(rankdata(fv), rankdata(rv))[0, 1]
        if np.isfinite(ric):
            ics_rank.append(ric)

    if len(ics_robust) < 20:
        return {
            "factor_name": factor_name,
            "verdict": "KILL",
            "reason": f"only {len(ics_robust)} valid IC days",
            "elapsed_sec": round(time.time() - t0, 2),
        }

    ic_mean = np.mean(ics_robust)
    ic_std = np.std(ics_robust, ddof=1)
    icir = ic_mean / ic_std if ic_std > 0 else 0
    rank_ic_mean = np.mean(ics_rank) if ics_rank else 0
    rank_ic_std = np.std(ics_rank, ddof=1) if len(ics_rank) > 1 else 0
    rank_icir = rank_ic_mean / rank_ic_std if rank_ic_std > 0 else 0

    abs_ic = abs(ic_mean)
    abs_ric = abs(rank_ic_mean)

    # 综合判定: IC 和 Rank IC 都看
    if abs_ic >= IC_PASS and abs_ric >= IC_PASS:
        verdict = "PASS"
    elif abs_ic >= IC_MARGINAL or abs_ric >= 0.010:
        verdict = "MARGINAL"
    else:
        verdict = "KILL"

    # IC/RankIC 差距警告
    ratio = abs_ric / abs_ic if abs_ic > 0.001 else float('inf')
    divergence_flag = "DIVERGENT" if ratio > 3.0 else ""

    return {
        "factor_name": factor_name,
        "ic_mean": round(ic_mean, 6),
        "abs_ic": round(abs_ic, 6),
        "icir": round(icir, 4),
        "rank_ic": round(rank_ic_mean, 6),
        "abs_rank_ic": round(abs_ric, 6),
        "rank_icir": round(rank_icir, 4),
        "ic_rank_ic_ratio": round(ratio, 2),
        "divergence_flag": divergence_flag,
        "ic_days": len(ics_robust),
        "stocks": len(common_insts),
        "verdict": verdict,
        "elapsed_sec": round(time.time() - t0, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Quick IC screening for factor development")
    parser.add_argument("dirs", nargs="+", help="Factor workspace directories (containing result.h5)")
    args = parser.parse_args()

    print("Loading close prices from qlib bin...", file=sys.stderr)
    t_load = time.time()
    close_df = read_close_prices(start_date="2024-01-01")
    close_unstacked = close_df["close"].unstack("instrument")
    fwd_ret = close_unstacked.shift(-2) / close_unstacked.shift(-1) - 1
    print(f"Close loaded in {time.time() - t_load:.1f}s, "
          f"{close_unstacked.shape[0]} dates x {close_unstacked.shape[1]} stocks",
          file=sys.stderr)

    results = []
    for d in args.dirs:
        result_h5 = Path(d) / "result.h5"
        if not result_h5.exists():
            results.append({"dir": d, "verdict": "ERROR", "reason": "result.h5 not found"})
            continue

        r = quick_ic(result_h5, close_unstacked, fwd_ret)
        r["dir"] = d
        results.append(r)

        v = r["verdict"]
        name = r.get("factor_name", "?")
        ic_s = f"IC={r.get('ic_mean', '?'):+.4f}" if "ic_mean" in r else ""
        ric_s = f"RankIC={r.get('rank_ic', '?'):+.4f}" if "rank_ic" in r else ""
        flag = f" ⚠{r['divergence_flag']}" if r.get("divergence_flag") else ""
        print(f"  {v:8s} {name:40s} {ic_s}  {ric_s}{flag}", file=sys.stderr)

    print(json.dumps(results, indent=2, ensure_ascii=False))

    passed = sum(1 for r in results if r["verdict"] == "PASS")
    marginal = sum(1 for r in results if r["verdict"] == "MARGINAL")
    killed = sum(1 for r in results if r["verdict"] == "KILL")
    print(f"\nSummary: {passed} PASS / {marginal} MARGINAL / {killed} KILL "
          f"(total {len(results)})", file=sys.stderr)


if __name__ == "__main__":
    main()
