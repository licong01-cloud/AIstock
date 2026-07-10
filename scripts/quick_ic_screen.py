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
  python quick_ic_screen.py --horizon 20 --split-manifest split.json dir1  # 正式 h20 快筛
"""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import rankdata

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


def build_forward_returns(close: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Build the bare T+1 -> T+(horizon+1) return label."""
    if horizon < 1:
        raise ValueError("horizon must be a positive integer")
    return close.shift(-(horizon + 1)) / close.shift(-1) - 1


def _newey_west_long_run_variance(values: list[float], lag: int) -> float | None:
    """Return Bartlett-kernel Newey-West long-run variance.

    This is the variance of the IC process, not the variance of its sample mean.
    Consequently ``mean / sqrt(LRV)`` is a dependence-adjusted ICIR rather than
    a t-statistic.
    """
    if lag < 0:
        raise ValueError("lag must be non-negative")
    arr = np.asarray(values, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size < max(20, lag + 1):
        return None

    centered = arr - arr.mean()
    if np.allclose(centered, 0.0):
        return None

    n_obs = centered.size
    lrv = float(np.dot(centered, centered) / n_obs)
    for offset in range(1, lag + 1):
        covariance = float(np.dot(centered[offset:], centered[:-offset]) / n_obs)
        weight = 1.0 - offset / (lag + 1.0)
        lrv += 2.0 * weight * covariance

    if not np.isfinite(lrv) or lrv <= 0:
        return None
    return lrv


def _hac_icir(values: list[float], lag: int) -> float | None:
    arr = np.asarray(values, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    lrv = _newey_west_long_run_variance(arr.tolist(), lag)
    if lrv is None:
        return None
    return float(arr.mean() / np.sqrt(lrv))


def _classify_ic(ic_mean: float, rank_ic_mean: float, expected_direction: int | None) -> str:
    if expected_direction is None:
        if abs(ic_mean) >= IC_PASS and abs(rank_ic_mean) >= IC_PASS:
            return "PASS"
        if abs(ic_mean) >= IC_MARGINAL or abs(rank_ic_mean) >= 0.010:
            return "MARGINAL"
        return "KILL"

    if expected_direction not in {-1, 1}:
        raise ValueError("expected_direction must be -1, 1, or None")
    directional_ic = expected_direction * ic_mean
    directional_rank_ic = expected_direction * rank_ic_mean
    if directional_ic >= IC_PASS and directional_rank_ic >= IC_PASS:
        return "PASS"
    if (
        directional_ic >= 0
        and directional_rank_ic >= 0
        and (directional_ic >= IC_MARGINAL or directional_rank_ic >= 0.010)
    ):
        return "MARGINAL"
    return "KILL"


def load_split_manifest(path: Path, horizon: int, cli_direction: int | None) -> dict:
    """Load and validate the pre-purged split receipt used by a formal screen."""
    raw = path.read_bytes()
    manifest = json.loads(raw.decode("utf-8"))
    required = {
        "manifest_version",
        "trial_id",
        "split_id",
        "split_role",
        "signal_start",
        "signal_end",
        "label_horizon_days",
        "purge_days",
        "embargo_days",
        "expected_direction",
        "data_snapshot_sha256",
    }
    missing = sorted(required.difference(manifest))
    if missing:
        raise ValueError(f"split manifest missing required fields: {missing}")
    if manifest["manifest_version"] != 1:
        raise ValueError("split manifest_version must be 1")
    if manifest["split_role"] not in {"train", "validation", "test"}:
        raise ValueError("split_role must be train, validation, or test")
    if int(manifest["label_horizon_days"]) != horizon:
        raise ValueError("split manifest label_horizon_days does not match --horizon")

    direction = int(manifest["expected_direction"])
    if direction not in {-1, 1}:
        raise ValueError("split manifest expected_direction must be -1 or 1")
    if cli_direction is not None and cli_direction != direction:
        raise ValueError("--direction conflicts with split manifest expected_direction")

    purge_days = int(manifest["purge_days"])
    embargo_days = int(manifest["embargo_days"])
    if purge_days < horizon:
        raise ValueError("split manifest purge_days must cover the label horizon")
    if embargo_days < 0:
        raise ValueError("split manifest embargo_days must be non-negative")

    signal_start = pd.Timestamp(manifest["signal_start"])
    signal_end = pd.Timestamp(manifest["signal_end"])
    if signal_end < signal_start:
        raise ValueError("split manifest signal_end precedes signal_start")

    snapshot_sha256 = str(manifest["data_snapshot_sha256"]).lower()
    if len(snapshot_sha256) != 64 or any(char not in "0123456789abcdef" for char in snapshot_sha256):
        raise ValueError("data_snapshot_sha256 must be a 64-character hexadecimal digest")

    return {
        **manifest,
        "expected_direction": direction,
        "purge_days": purge_days,
        "embargo_days": embargo_days,
        "signal_start": str(signal_start.date()),
        "signal_end": str(signal_end.date()),
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
    }


def quick_ic(
    result_h5_path: Path,
    close_unstacked: pd.DataFrame,
    fwd_ret: pd.DataFrame,
    horizon: int = 1,
    expected_direction: int | None = None,
    eval_start: str = OUT_SAMPLE_START,
    eval_end: str | None = None,
) -> dict:
    """计算单个因子的快速 IC (与全量引擎一致的计算方式)"""
    if horizon < 1:
        raise ValueError("horizon must be a positive integer")
    if expected_direction not in {None, -1, 1}:
        raise ValueError("expected_direction must be -1, 1, or None")
    start_ts = pd.Timestamp(eval_start)
    end_ts = pd.Timestamp(eval_end) if eval_end else None
    if end_ts is not None and end_ts < start_ts:
        raise ValueError("eval_end must not precede eval_start")
    t0 = time.time()
    horizon_contract = {
        "return_horizon_days": horizon,
        "return_horizon_label": f"T{horizon + 1}T1",
        "hac_lag": horizon - 1,
        "expected_direction": expected_direction,
        "verdict_basis": "frozen_direction" if expected_direction is not None else "legacy_absolute",
        "eval_start": str(start_ts.date()),
        "eval_end": str(end_ts.date()) if end_ts is not None else None,
        "label_source_end": (
            str(pd.Timestamp(close_unstacked.index.max()).date())
            if not close_unstacked.empty
            else None
        ),
    }

    df = pd.read_hdf(result_h5_path)
    factor_name = df.columns[0]
    factor_wide = df[factor_name].unstack("instrument")

    factor_oos = factor_wide.loc[factor_wide.index >= start_ts]
    fwd_oos = fwd_ret.loc[fwd_ret.index >= start_ts]
    if end_ts is not None:
        factor_oos = factor_oos.loc[factor_oos.index <= end_ts]
        fwd_oos = fwd_oos.loc[fwd_oos.index <= end_ts]

    common_dates = factor_oos.index.intersection(fwd_oos.index)
    common_insts = factor_oos.columns.intersection(fwd_oos.columns)
    if len(common_dates) and len(common_insts):
        label_valid = np.isfinite(
            fwd_oos.loc[common_dates, common_insts].to_numpy(dtype=np.float64, copy=False)
        ).any(axis=1)
        valid_dates = common_dates[label_valid]
        horizon_contract["last_evaluable_signal_date"] = (
            str(pd.Timestamp(valid_dates[-1]).date()) if len(valid_dates) else None
        )
    else:
        horizon_contract["last_evaluable_signal_date"] = None

    if len(common_dates) < 20 or len(common_insts) < 100:
        return {
            "factor_name": factor_name,
            **horizon_contract,
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
            **horizon_contract,
            "verdict": "KILL",
            "reason": f"only {len(ics_robust)} valid IC days",
            "elapsed_sec": round(time.time() - t0, 2),
        }

    ic_mean = np.mean(ics_robust)
    ic_std_ddof0 = np.std(ics_robust, ddof=0)
    ic_std = np.std(ics_robust, ddof=1)
    icir = ic_mean / ic_std if ic_std > 0 else 0
    rank_ic_mean = np.mean(ics_rank) if ics_rank else 0
    rank_ic_std_ddof0 = np.std(ics_rank, ddof=0) if ics_rank else 0
    rank_ic_std = np.std(ics_rank, ddof=1) if len(ics_rank) > 1 else 0
    rank_icir = rank_ic_mean / rank_ic_std if rank_ic_std > 0 else 0
    hac_lag = horizon - 1
    icir_hac = _hac_icir(ics_robust, hac_lag)
    rank_icir_hac = _hac_icir(ics_rank, hac_lag)

    abs_ic = abs(ic_mean)
    abs_ric = abs(rank_ic_mean)

    verdict = _classify_ic(ic_mean, rank_ic_mean, expected_direction)

    # IC/RankIC 差距警告
    ratio = abs_ric / abs_ic if abs_ic > 0.001 else float('inf')
    divergence_flag = "DIVERGENT" if ratio > 3.0 else ""

    return {
        "factor_name": factor_name,
        "ic_mean": round(ic_mean, 6),
        "abs_ic": round(abs_ic, 6),
        "ic_std_ddof0": round(ic_std_ddof0, 6),
        "icir_ddof0": round(ic_mean / ic_std_ddof0, 4) if ic_std_ddof0 > 0 else None,
        "icir": round(icir, 4),
        "icir_hac": None if icir_hac is None else round(icir_hac, 4),
        "rank_ic": round(rank_ic_mean, 6),
        "abs_rank_ic": round(abs_ric, 6),
        "rank_ic_std_ddof0": round(rank_ic_std_ddof0, 6),
        "rank_icir_ddof0": (
            round(rank_ic_mean / rank_ic_std_ddof0, 4) if rank_ic_std_ddof0 > 0 else None
        ),
        "rank_icir": round(rank_icir, 4),
        "rank_icir_hac": None if rank_icir_hac is None else round(rank_icir_hac, 4),
        **horizon_contract,
        "directional_ic": (
            None if expected_direction is None else round(expected_direction * ic_mean, 6)
        ),
        "directional_rank_ic": (
            None if expected_direction is None else round(expected_direction * rank_ic_mean, 6)
        ),
        "ic_rank_ic_ratio": round(ratio, 2),
        "divergence_flag": divergence_flag,
        "ic_days": len(ics_robust),
        "stocks": len(common_insts),
        "verdict": verdict,
        "elapsed_sec": round(time.time() - t0, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Quick IC screening for factor development")
    parser.add_argument(
        "--horizon",
        type=int,
        default=1,
        help="Forward return horizon N for T+1 to T+(N+1); default: 1",
    )
    parser.add_argument(
        "--direction",
        type=int,
        choices=(-1, 1),
        default=None,
        help="Direction frozen on train/validation; omitted keeps legacy unsigned diagnostic verdict",
    )
    parser.add_argument(
        "--start-date",
        default=OUT_SAMPLE_START,
        help="Diagnostic signal start date; a formal split manifest overrides it",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Diagnostic signal end date; a formal split manifest overrides it",
    )
    parser.add_argument(
        "--split-manifest",
        type=Path,
        default=None,
        help="Pre-purged split JSON receipt required for a formal h20 gate",
    )
    parser.add_argument("dirs", nargs="+", help="Factor workspace directories (containing result.h5)")
    args = parser.parse_args()

    if args.horizon < 1:
        parser.error("--horizon must be a positive integer")
    split_receipt = None
    eval_start = args.start_date
    eval_end = args.end_date
    if args.split_manifest is not None:
        try:
            split_receipt = load_split_manifest(
                args.split_manifest,
                horizon=args.horizon,
                cli_direction=args.direction,
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            parser.error(f"invalid --split-manifest: {exc}")
        args.direction = split_receipt["expected_direction"]
        eval_start = split_receipt["signal_start"]
        eval_end = split_receipt["signal_end"]
    if args.horizon > 1 and split_receipt is None:
        print(
            "WARNING: h20 without --split-manifest is a diagnostic only; "
            "it is not eligible for the formal Stage 1 gate even when --direction is supplied.",
            file=sys.stderr,
        )

    sys.path.insert(0, "/mnt/f/Dev/RD-Agent-main")
    from rdagent.app.factor_metrics.qlib_data_reader import read_close_prices

    print("Loading close prices from qlib bin...", file=sys.stderr)
    t_load = time.time()
    load_start = pd.Timestamp(eval_start) - pd.Timedelta(days=max(365, args.horizon * 3))
    close_df = read_close_prices(start_date=str(load_start.date()))
    close_unstacked = close_df["close"].unstack("instrument")
    fwd_ret = build_forward_returns(close_unstacked, args.horizon)
    print(f"Close loaded in {time.time() - t_load:.1f}s, "
          f"{close_unstacked.shape[0]} dates x {close_unstacked.shape[1]} stocks",
          file=sys.stderr)

    results = []
    for d in args.dirs:
        result_h5 = Path(d) / "result.h5"
        if not result_h5.exists():
            results.append({"dir": d, "verdict": "ERROR", "reason": "result.h5 not found"})
            continue

        r = quick_ic(
            result_h5,
            close_unstacked,
            fwd_ret,
            horizon=args.horizon,
            expected_direction=args.direction,
            eval_start=eval_start,
            eval_end=eval_end,
        )
        r["formal_gate_eligible"] = split_receipt is not None
        r["split_receipt"] = split_receipt
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
