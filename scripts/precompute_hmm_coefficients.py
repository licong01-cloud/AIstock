#!/usr/bin/env python3
"""WSL端 HMM 预计算脚本 — 在 conda rdagent-gpu 环境中运行.

每个行业构建一次完整观测序列，一次前向滤波解码，直接取对应日期的状态。
131 行业 × 1 次前向滤波 = 131 次（而非逐日调用的 131×400 = 52,400 次）。

输出 daily_coefficients + stock_sector_map 的 JSON 到 stdout。

Usage (from Windows config_composer via subprocess):
    wsl bash -c "source ~/miniconda3/etc/profile.d/conda.sh && \
        conda activate rdagent-gpu && \
        python /mnt/f/Dev/AIstock/scripts/precompute_hmm_coefficients.py"

stdin JSON 参数:
    {
        "model_path": "/home/lc999/model_training_ws/hmm/.../models.json",
        "test_start": "2024-01-01",
        "backtest_end": "2026-03-31",
        "preset_coeffs": {"trending": 1.05, "neutral": 1.00, "fading": 0.96},
        "preset_key": "preset_A",
        "db_host": "127.0.0.1",
        "db_port": 5432,
        "db_name": "aistock",
        "db_user": "postgres",
        "db_password": ""
    }

stdout: JSON 结果
stderr: 日志信息
"""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

# 添加项目路径以复用 sector_hmm 模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def forward_filter_states(hmm, obs: np.ndarray) -> np.ndarray:
    """前向滤波：fwd_lattice[t] 仅依赖 obs[0:t+1]，严格因果无前瞻偏差.

    Returns: (T,) 每日最大后验概率状态索引
    """
    from hmmlearn import _hmmc
    from hmmlearn.utils import normalize as hmm_normalize

    log_startprob = np.log(hmm.startprob_ + 1e-300)
    log_transmat = np.log(hmm.transmat_ + 1e-300)
    log_frameprob = hmm._compute_log_likelihood(obs)

    _, fwd_lattice = _hmmc.forward_log(
        log_startprob, log_transmat, log_frameprob,
    )
    posteriors = np.exp(fwd_lattice)
    hmm_normalize(posteriors, axis=1)
    return posteriors.argmax(axis=1)


def main() -> None:
    import argparse
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from hmmlearn.hmm import GaussianHMM

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default=None,
                        help="额外将结果写入此文件路径（stdout 仍然输出）")
    args, _ = parser.parse_known_args()

    # ── 读取 stdin 参数 ──
    raw = sys.stdin.read()
    if not raw.strip():
        print("ERROR: 未从 stdin 收到参数 JSON", file=sys.stderr)
        sys.exit(1)

    params = json.loads(raw)

    model_path = params["model_path"]
    test_start = params["test_start"]
    backtest_end = params["backtest_end"]
    preset_coeffs = params.get("preset_coeffs", {
        "trending": 1.05, "neutral": 1.00, "fading": 0.96,
    })
    preset_key = params.get("preset_key")
    output_trade_date = params.get("output_trade_date")
    as_of_trade_date = params.get("as_of_trade_date")
    generation_mode = params.get("generation_mode")
    snapshot_id = params.get("snapshot_id")
    config_id = params.get("config_id")
    input_data_max_dates = params.get("input_data_max_dates")

    db_host = params.get("db_host", "127.0.0.1")
    db_port = params.get("db_port", 5432)
    db_name = params.get("db_name", "aistock")
    db_user = params.get("db_user", "postgres")
    db_password = params.get("db_password", "")

    print(f"HMM 预计算: model={model_path}", file=sys.stderr)
    print(f"  日期范围: {test_start} ~ {backtest_end}", file=sys.stderr)
    print(f"  preset: {preset_key}, coeffs: {preset_coeffs}", file=sys.stderr)
    print(f"  DB: {db_user}@{db_host}:{db_port}/{db_name}", file=sys.stderr)

    # ── 加载模型 ──
    with open(model_path, "r", encoding="utf-8") as f:
        models = json.load(f)
    if not models:
        print(f"ERROR: HMM 模型文件为空: {model_path}", file=sys.stderr)
        sys.exit(1)
    print(f"  已加载 {len(models)} 个行业模型", file=sys.stderr)

    # 读取模型参数
    first = next(iter(models.values()))
    rolling_window = first.get("rolling_window", 5)
    n_features = len(first.get("means", [[]])[0]) if first.get("means") else 4
    has_zscore = "zscore_mean" in first
    zscore_mean = np.array(first["zscore_mean"]) if has_zscore else None
    zscore_std = np.array(first["zscore_std"]) if has_zscore else None

    print(f"  模型特征维度: {n_features}, 滚动窗口: {rolling_window}, zscore: {has_zscore}", file=sys.stderr)

    # ── 重建 HMM 对象 ──
    hmm_objs: Dict[str, Any] = {}
    for code, info in models.items():
        try:
            n_states = info["n_states"]
            cov_type = info.get("covariance_type", "diag")
            hmm = GaussianHMM(n_components=n_states, covariance_type=cov_type)
            hmm.startprob_ = np.full(n_states, 1.0 / n_states)
            hmm.transmat_ = np.array(info["transmat"], dtype=np.float64)
            hmm.means_ = np.array(info["means"], dtype=np.float64)
            covars = np.array(info["covars"], dtype=np.float64)
            if cov_type == "diag":
                if covars.ndim == 3:
                    covars = np.array([np.diag(covars[i]) for i in range(covars.shape[0])])
                covars = np.maximum(covars, 1e-6)
            elif cov_type == "full":
                for i in range(covars.shape[0]):
                    covars[i] = (covars[i] + covars[i].T) / 2
                    covars[i] += np.eye(covars[i].shape[0]) * 1e-6
            hmm.covars_ = covars
            hmm_objs[code] = (hmm, info["state_labels"])
        except Exception as e:
            print(f"  WARNING: 重建 HMM 失败 {code}: {e}", file=sys.stderr)

    if not hmm_objs:
        print("ERROR: 所有 HMM 模型重建失败", file=sys.stderr)
        sys.exit(1)
    print(f"  重建 {len(hmm_objs)}/{len(models)} 个 HMM 模型", file=sys.stderr)

    # ── DB 连接 ──
    conn = psycopg2.connect(
        host=db_host, port=db_port,
        dbname=db_name, user=db_user, password=db_password,
    )
    conn.autocommit = True

    start_d = date.fromisoformat(test_start)
    end_d = date.fromisoformat(backtest_end)
    history_start = start_d - timedelta(days=int(3.0 * 365 + 30))

    # ── 一次性批量加载所有数据到内存 ──
    print("  批量加载数据...", file=sys.stderr)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. sector_data (L2 行业行情+资金流)
    cur.execute("""
        SELECT m.l2_code AS sector_code, sd.trade_date,
               sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
               sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt
        FROM market.sector_data sd
        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
        WHERE sd.trade_date BETWEEN %s AND %s
          AND m.in_date <= sd.trade_date
          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
    """, (history_start, end_d))
    all_sector_rows = cur.fetchall()

    # 按 sector_code → [{trade_date, ...}] 分组
    sector_data: Dict[str, List[Dict]] = {}
    for row in all_sector_rows:
        code = row["sector_code"]
        if code not in sector_data:
            sector_data[code] = {}
        td = row["trade_date"]
        if td not in sector_data[code]:
            sector_data[code][td] = row

    # 2. CSI300（使用 000300.SH — 上交所标准代码，数据最全）
    cur.execute("""
        SELECT trade_date, pct_chg FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """, (history_start, end_d))
    csi300 = {r["trade_date"]: float(r["pct_chg"] or 0) for r in cur.fetchall()}

    # 3. 全市场成交量
    cur.execute("""
        SELECT trade_date, SUM(vol) AS total_vol FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date ORDER BY trade_date
    """, (history_start, end_d))
    market_vol = {r["trade_date"]: float(r["total_vol"] or 0) for r in cur.fetchall()}

    # 4. 股票→L2行业映射
    cur.execute(
        "SELECT ts_code, l2_code FROM market.sw_index_member "
        "WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)",
        (end_d, start_d),
    )
    stock_sector_map = {}
    for r in cur.fetchall():
        if r["ts_code"] and r["l2_code"]:
            stock_sector_map[r["ts_code"]] = r["l2_code"]

    cur.close()
    conn.close()

    print(f"  数据加载完成: {len(sector_data)} 行业, {len(csi300)} CSI300, "
          f"{len(market_vol)} 市场量, {len(stock_sector_map)} 股票映射", file=sys.stderr)

    if not stock_sector_map:
        print("ERROR: 股票行业映射查询结果为空", file=sys.stderr)
        sys.exit(1)

    # ── 逐行业：构建完整 obs → 一次前向滤波 → 取对应日期的状态 ──
    print("  逐行业前向滤波解码...", file=sys.stderr)
    # {sector_code: {date_str: label}}
    sector_date_labels: Dict[str, Dict[str, str]] = {}
    win = max(rolling_window, 2)

    for idx, (code, (hmm, labels)) in enumerate(hmm_objs.items()):
        if code not in sector_data:
            continue

        # 构建完整观测矩阵（history_start ~ end_d）
        sorted_dates = sorted(sector_data[code].keys())
        rows = []
        dates_out = []

        for i, td in enumerate(sorted_dates):
            rec = sector_data[code][td]
            pct = float(rec["sw2_pct_change"] or 0)
            vol = float(rec["sw2_vol"] or 0)
            amount = float(rec["sw2_amount"] or 0)
            mf_net = float(rec["sw2_mf_net_amt"] or 0)
            mf_buy_elg = float(rec["sw2_mf_buy_elg_amt"] or 0)
            mf_sell_elg = float(rec["sw2_mf_sell_elg_amt"] or 0)

            csi_pct = csi300.get(td)
            mvol = market_vol.get(td)
            if csi_pct is None or mvol is None:
                continue

            daily_ret = pct / 100.0

            # 超额收益 N 日均值
            csi_window = []
            for j in range(max(0, i - win + 1), i + 1):
                d2 = sorted_dates[j]
                c2 = csi300.get(d2)
                if c2 is not None and d2 in sector_data[code]:
                    csi_window.append(
                        float(sector_data[code][d2]["sw2_pct_change"] or 0) / 100.0 - c2 / 100.0
                    )
            excess_nd = float(np.mean(csi_window)) if csi_window else 0.0

            vol_ratio = vol / mvol if mvol > 0 else 0.0
            lu_ratio = 0.0  # 涨停占比在推断阶段不可用

            # N 日波动率
            ret_window = []
            for j in range(max(0, i - win + 1), i + 1):
                d2 = sorted_dates[j]
                if d2 in sector_data[code]:
                    ret_window.append(float(sector_data[code][d2]["sw2_pct_change"] or 0) / 100.0)
            volatility = float(np.std(ret_window)) if len(ret_window) > 1 else 0.0

            mf_net_ratio = mf_net / amount if amount > 0 else 0.0
            elg_net = mf_buy_elg - mf_sell_elg
            elg_ratio = elg_net / amount if amount > 0 else 0.0

            if n_features >= 7:
                row = [daily_ret, excess_nd, vol_ratio, lu_ratio, volatility, mf_net_ratio, elg_ratio]
            else:
                # 4 维模型: daily_return, excess_return_20d, volume_ratio, limit_up_ratio
                row = [daily_ret, excess_nd, vol_ratio, lu_ratio]
            if any(np.isnan(v) or np.isinf(v) for v in row):
                continue
            rows.append(row)
            dates_out.append(td)

        if len(rows) < 20:
            continue

        obs = np.array(rows, dtype=np.float64)

        # z-score 标准化
        if zscore_mean is not None:
            obs = (obs - zscore_mean) / zscore_std

        # 一次前向滤波（整个序列）
        try:
            states = forward_filter_states(hmm, obs)
        except Exception as e:
            print(f"  WARNING: 前向滤波失败 {code}: {e}", file=sys.stderr)
            continue

        # 提取回测日期范围内的状态标签
        date_labels = {}
        for i, td in enumerate(dates_out):
            if start_d <= td <= end_d:
                state_idx = states[i]
                date_labels[td.isoformat()] = labels.get(str(state_idx), "neutral")
        sector_date_labels[code] = date_labels

        if (idx + 1) % 20 == 0:
            print(f"  已处理 {idx + 1}/{len(hmm_objs)} 行业", file=sys.stderr)

    print(f"  解码完成: {len(sector_date_labels)} 个行业 (前向滤波)", file=sys.stderr)

    # ── 构建输出 JSON ──
    all_dates = set()
    for dl in sector_date_labels.values():
        all_dates.update(dl.keys())

    daily_coefficients = {}
    for d in sorted(all_dates):
        day_coeffs = {}
        for code, dl in sector_date_labels.items():
            label = dl.get(d, "neutral")
            day_coeffs[code] = preset_coeffs.get(label, 1.0)
        daily_coefficients[d] = day_coeffs

    if output_trade_date:
        source_trade_date = as_of_trade_date or backtest_end
        source_coefficients = daily_coefficients.get(source_trade_date)
        if not isinstance(source_coefficients, dict) or not source_coefficients:
            print(
                "ERROR: HMM daily generation did not produce coefficients for "
                f"as_of_trade_date={source_trade_date}; available={sorted(daily_coefficients.keys())[-5:]}",
                file=sys.stderr,
            )
            sys.exit(1)
        daily_coefficients = {str(output_trade_date): source_coefficients}

    result = {
        "model_path": model_path,
        "preset_key": preset_key,
        "preset_coeffs": preset_coeffs,
        "test_start": test_start,
        "backtest_end": backtest_end,
        "sector_count": len(sector_date_labels),
        "daily_coefficients": daily_coefficients,
        "stock_sector_map": stock_sector_map,
    }
    if output_trade_date:
        result.update({
            "generation_mode": generation_mode or "daily_asof_prediction_v1",
            "as_of_trade_date": as_of_trade_date or backtest_end,
            "effective_trade_date": str(output_trade_date),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_id": snapshot_id,
            "config_id": config_id,
            "input_data_max_dates": input_data_max_dates,
        })

    result_json = json.dumps(result, ensure_ascii=False)
    print(result_json)

    # 额外写入文件（供后续实验直接读取）
    if args.output_path:
        import os
        os.makedirs(os.path.dirname(args.output_path) or ".", exist_ok=True)
        with open(args.output_path, "w", encoding="utf-8") as f:
            f.write(result_json)
        print(f"  结果已写入: {args.output_path}", file=sys.stderr)

    print("HMM 预计算完成", file=sys.stderr)


if __name__ == "__main__":
    main()
