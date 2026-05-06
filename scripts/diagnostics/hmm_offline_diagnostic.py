"""Offline HMM overlay diagnostics for QE loops.

The script reads QE artifacts only through QEWorkspaceClient-compatible HTTP
endpoints. It does not touch QE strategy code or remote workspace paths.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import math
import pickle
import re
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient  # noqa: E402

DB_DEFAULT = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "aistock",
    "user": "postgres",
    "password": "lc78080808",
}


@dataclass
class LoopInfo:
    loop_index: int
    loop_id: str
    status: str
    experiment_id: str | None
    node_id: str | None
    label: str | None
    has_hmm: bool
    snapshot_id: str | None
    model_path: str | None
    annualized_return: float | None
    max_drawdown: float | None
    sharpe: float | None
    ic: float | None
    rank_ic: float | None


@dataclass
class RecorderRef:
    experiment_id: str
    recorder_id: str


def db_connect():
    return psycopg2.connect(**DB_DEFAULT)


def pct(x: Any, digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "NA"
    return f"{float(x) * 100:.{digits}f}%"


def num(x: Any, digits: int = 4) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "NA"
    return f"{float(x):.{digits}f}"


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def extract_metrics(result_metrics: dict[str, Any] | None, metric_json: dict[str, Any] | None) -> tuple[float | None, ...]:
    rm = result_metrics or {}
    mj = metric_json or {}
    ann = safe_float(rm.get("annualized_return") or mj.get("annualized_return") or mj.get("annual_return"))
    dd = safe_float(rm.get("max_drawdown") or mj.get("max_drawdown"))
    sharpe = safe_float(rm.get("sharpe") or mj.get("sharpe") or mj.get("sharpe_ratio"))
    ic = safe_float(rm.get("IC") or rm.get("ic") or mj.get("IC") or mj.get("ic"))
    rank_ic = safe_float(rm.get("Rank IC") or rm.get("Rank_IC") or rm.get("rank_ic") or mj.get("rank_ic"))
    return ann, dd, sharpe, ic, rank_ic


def load_loops(task_id: str) -> tuple[list[LoopInfo], str, str]:
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.node_id, n.api_base_url
            FROM qe_evolution_tasks t
            LEFT JOIN infra.compute_nodes n ON n.node_id = t.node_id
            WHERE t.task_id = %s
            """,
            (task_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"QE task not found: {task_id}")
        task_node_id, api_base_url = row
        if not api_base_url:
            raise RuntimeError(f"QE task has no compute-node api_base_url: {task_id}")

        cur.execute(
            """
            SELECT l.loop_index, l.loop_id, l.status, l.experiment_id, l.node_id,
                   l.config_json, l.metrics_json,
                   e.custom_params, e.result_metrics,
                   e.annualized_return, e.max_drawdown, e.ic, e.rank_ic
            FROM qe_evolution_loops l
            LEFT JOIN qe_experiments e ON e.experiment_id = l.experiment_id
            WHERE l.task_id = %s
            ORDER BY l.loop_index
            """,
            (task_id,),
        )
        loops: list[LoopInfo] = []
        for row in cur.fetchall():
            (
                loop_index,
                loop_id,
                status,
                experiment_id,
                node_id,
                cfg,
                metrics_json,
                custom_params,
                result_metrics,
                e_ann,
                e_dd,
                e_ic,
                e_rank_ic,
            ) = row
            cfg = cfg or {}
            cp = custom_params or cfg.get("custom_params") or cfg.get("strategy_params") or {}
            ann, dd, sharpe, ic, rank_ic = extract_metrics(result_metrics, metrics_json)
            if e_ann is not None:
                ann = safe_float(e_ann)
            if e_dd is not None:
                dd = safe_float(e_dd)
            if e_ic is not None:
                ic = safe_float(e_ic)
            if e_rank_ic is not None:
                rank_ic = safe_float(e_rank_ic)
            loops.append(
                LoopInfo(
                    loop_index=int(loop_index),
                    loop_id=str(loop_id),
                    status=str(status),
                    experiment_id=experiment_id,
                    node_id=node_id or task_node_id,
                    label=cfg.get("label") or cp.get("label"),
                    has_hmm=bool(cp.get("enable_sector_hmm")),
                    snapshot_id=cp.get("hmm_model_version_id"),
                    model_path=cp.get("sector_hmm_model_path"),
                    annualized_return=ann,
                    max_drawdown=dd,
                    sharpe=sharpe,
                    ic=ic,
                    rank_ic=rank_ic,
                )
            )
    return loops, str(task_node_id), str(api_base_url).rstrip("/")


def enhanced_metrics_by_experiment(experiment_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not experiment_ids:
        return {}
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT experiment_id, result_metrics FROM qe_experiments WHERE experiment_id = ANY(%s)",
            (experiment_ids,),
        )
        out = {}
        for exp_id, rm in cur.fetchall():
            if not isinstance(rm, dict):
                continue
            out[exp_id] = rm.get("enhanced_metrics") if isinstance(rm.get("enhanced_metrics"), dict) else rm
        return out


def parse_recorder_ref(run_log: str) -> RecorderRef:
    latest = re.search(
        r"Latest recorder: .*?'id': '([0-9a-f]+)'.*?'experiment_id': '(\d+)'",
        run_log,
        flags=re.S,
    )
    if latest:
        return RecorderRef(experiment_id=latest.group(2), recorder_id=latest.group(1))
    start = re.search(r"Recorder ([0-9a-f]+) starts running under Experiment (\d+)", run_log)
    if start:
        return RecorderRef(experiment_id=start.group(2), recorder_id=start.group(1))
    raise RuntimeError("Cannot locate mlflow recorder id in run.log")


def loads_pickle(data: bytes) -> Any:
    return pickle.loads(data)


async def download_loop_artifacts(
    client: QEWorkspaceClient,
    task_id: str,
    loop: LoopInfo,
    out_dir: Path,
    need_pred: bool = False,
    need_label: bool = False,
) -> dict[str, Any]:
    loop_dir = out_dir / f"L{loop.loop_index}_{loop.loop_id}"
    loop_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, Any] = {}
    run_log = await client.get_workspace_file(task_id, loop.loop_id, "run.log")
    if not isinstance(run_log, str):
        run_log = json.dumps(run_log, ensure_ascii=False)
    (loop_dir / "run.log").write_text(run_log, encoding="utf-8")
    rec = parse_recorder_ref(run_log)
    artifacts["recorder"] = rec

    conf = await client.get_workspace_file(task_id, loop.loop_id, "conf.yaml")
    if isinstance(conf, str):
        (loop_dir / "conf.yaml").write_text(conf, encoding="utf-8")

    try:
        enhanced = await client.get_workspace_file(task_id, loop.loop_id, "qlib_results_enhanced.json")
        (loop_dir / "qlib_results_enhanced.json").write_text(
            json.dumps(enhanced, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        artifacts["enhanced"] = enhanced
    except Exception as exc:
        artifacts["enhanced_error"] = str(exc)

    art_prefix = f"mlruns/{rec.experiment_id}/{rec.recorder_id}/artifacts"
    if need_pred:
        pred_bytes = await client.download_workspace_file_bytes(task_id, loop.loop_id, f"{art_prefix}/pred.pkl")
        (loop_dir / "pred.pkl").write_bytes(pred_bytes)
        artifacts["pred"] = loads_pickle(pred_bytes)
    if need_label:
        label_bytes = await client.download_workspace_file_bytes(task_id, loop.loop_id, f"{art_prefix}/label.pkl")
        (loop_dir / "label.pkl").write_bytes(label_bytes)
        artifacts["label"] = loads_pickle(label_bytes)

    try:
        report_bytes = await client.download_workspace_file_bytes(
            task_id, loop.loop_id, f"{art_prefix}/portfolio_analysis/report_normal_1day.pkl"
        )
        (loop_dir / "report_normal_1day.pkl").write_bytes(report_bytes)
        artifacts["report"] = loads_pickle(report_bytes)
    except Exception as exc:
        artifacts["report_error"] = str(exc)

    if loop.has_hmm:
        hmm = await client.get_workspace_file(task_id, loop.loop_id, "hmm_sector_coefficients.json")
        if not isinstance(hmm, dict):
            hmm = json.loads(hmm)
        (loop_dir / "hmm_sector_coefficients.json").write_text(
            json.dumps(hmm, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        artifacts["hmm"] = hmm
    return artifacts


def pred_to_series(pred: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(pred, pd.DataFrame):
        col = "score" if "score" in pred.columns else pred.columns[0]
        ser = pred[col]
    else:
        ser = pred
    ser = ser.dropna()
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("pred.pkl index must be MultiIndex(datetime, instrument)")
    ser.index = ser.index.set_levels(pd.to_datetime(ser.index.levels[0]), level=0)
    return ser.sort_index()


def label_to_series(label: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(label, pd.DataFrame):
        col = "LABEL0" if "LABEL0" in label.columns else label.columns[0]
        ser = label[col]
    else:
        ser = label
    if not isinstance(ser.index, pd.MultiIndex):
        raise RuntimeError("label.pkl index must be MultiIndex(datetime, instrument)")
    ser.index = ser.index.set_levels(pd.to_datetime(ser.index.levels[0]), level=0)
    return ser.sort_index()


def get_label_value(label_ser: pd.Series, dt: pd.Timestamp, symbol: str) -> float | None:
    try:
        value = label_ser.loc[(dt, symbol)]
        if isinstance(value, pd.Series):
            value = value.iloc[0]
        return safe_float(value)
    except Exception:
        return None


def compute_replacements(
    pred_ser: pd.Series,
    label_ser: pd.Series,
    hmm: dict[str, Any],
    topk: int,
    loop_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    daily_coeffs = hmm["daily_coefficients"]
    stock_sector_map = hmm["stock_sector_map"]
    dates = sorted(set(pd.to_datetime(list(daily_coeffs.keys()))) & set(pred_ser.index.get_level_values(0).unique()))
    rows: list[dict[str, Any]] = []
    day_rows: list[dict[str, Any]] = []
    sector_rows: list[dict[str, Any]] = []

    pred_by_day = {dt: s.droplevel(0) for dt, s in pred_ser.groupby(level=0, sort=True)}
    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")
        scores = pred_by_day.get(dt)
        if scores is None or scores.empty:
            continue
        scores = scores.dropna().astype(float)
        day_coeff = daily_coeffs.get(date_str) or {}
        sector = pd.Series(scores.index, index=scores.index).map(stock_sector_map)
        coeff = sector.map(lambda x: day_coeff.get(str(x), 1.0) if x is not None else 1.0).astype(float)
        adjusted = scores * coeff

        raw_sorted = scores.sort_values(ascending=False, kind="mergesort")
        adj_sorted = adjusted.sort_values(ascending=False, kind="mergesort")
        raw_rank = pd.Series(np.arange(1, len(raw_sorted) + 1), index=raw_sorted.index)
        adj_rank = pd.Series(np.arange(1, len(adj_sorted) + 1), index=adj_sorted.index)
        raw_top = set(raw_sorted.head(topk).index)
        adj_top = set(adj_sorted.head(topk).index)
        entered = sorted(adj_top - raw_top)
        dropped = sorted(raw_top - adj_top)
        common = len(raw_top & adj_top)

        entered_label = []
        dropped_label = []
        for typ, symbols in (("entered_by_hmm", entered), ("dropped_by_hmm", dropped)):
            for sym in symbols:
                lab = get_label_value(label_ser, dt, sym)
                if lab is not None:
                    if typ == "entered_by_hmm":
                        entered_label.append(lab)
                    else:
                        dropped_label.append(lab)
                sec = sector.get(sym)
                cf = safe_float(coeff.get(sym)) or 1.0
                rows.append(
                    {
                        "loop_label": loop_label,
                        "date": date_str,
                        "symbol": sym,
                        "replacement_type": typ,
                        "sector_code": sec,
                        "coefficient": cf,
                        "raw_score": safe_float(scores.get(sym)),
                        "adjusted_score": safe_float(adjusted.get(sym)),
                        "raw_rank": int(raw_rank.get(sym)) if sym in raw_rank.index else None,
                        "adjusted_rank": int(adj_rank.get(sym)) if sym in adj_rank.index else None,
                        "label_10d": lab,
                    }
                )
        non_neutral = [float(v) for v in day_coeff.values() if abs(float(v) - 1.0) > 1e-9]
        day_rows.append(
            {
                "loop_label": loop_label,
                "date": date_str,
                "raw_top_count": len(raw_top),
                "adjusted_top_count": len(adj_top),
                "common_count": common,
                "entered_count": len(entered),
                "dropped_count": len(dropped),
                "replacement_count": len(entered) + len(dropped),
                "mean_entered_label_10d": float(np.nanmean(entered_label)) if entered_label else np.nan,
                "mean_dropped_label_10d": float(np.nanmean(dropped_label)) if dropped_label else np.nan,
                "net_enter_minus_drop_label_10d": (
                    float(np.nanmean(entered_label) - np.nanmean(dropped_label))
                    if entered_label and dropped_label else np.nan
                ),
                "non_neutral_sector_count": len(non_neutral),
                "min_sector_coeff": float(np.nanmin(list(day_coeff.values()))) if day_coeff else np.nan,
                "max_sector_coeff": float(np.nanmax(list(day_coeff.values()))) if day_coeff else np.nan,
                "mean_sector_coeff": float(np.nanmean(list(day_coeff.values()))) if day_coeff else np.nan,
            }
        )
        for sec_code, cf_value in day_coeff.items():
            sector_rows.append(
                {
                    "loop_label": loop_label,
                    "date": date_str,
                    "sector_code": sec_code,
                    "coefficient": float(cf_value),
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(day_rows), pd.DataFrame(sector_rows)


def enrich_db_forward_returns(replacements: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    if replacements.empty:
        return replacements
    symbols = sorted(replacements["symbol"].dropna().unique().tolist())
    min_date = pd.to_datetime(replacements["date"]).min().date()
    max_date = (pd.to_datetime(replacements["date"]).max() + pd.Timedelta(days=max(horizons) * 3 + 15)).date()
    with db_connect() as conn:
        price = pd.read_sql_query(
            """
            SELECT trade_date, RTRIM(ts_code) AS symbol, close_li::double precision AS close_li
            FROM market.kline_daily_raw
            WHERE trade_date BETWEEN %s AND %s
              AND RTRIM(ts_code) = ANY(%s)
              AND close_li IS NOT NULL AND close_li > 0
            ORDER BY symbol, trade_date
            """,
            conn,
            params=(min_date, max_date, symbols),
        )
    if price.empty:
        return replacements
    price["trade_date"] = pd.to_datetime(price["trade_date"]).dt.strftime("%Y-%m-%d")
    price["close"] = price["close_li"].astype(float)
    price = price.sort_values(["symbol", "trade_date"])
    for h in horizons:
        price[f"db_ret_{h}d"] = price.groupby("symbol")["close"].shift(-h) / price["close"] - 1.0
    ret_cols = ["trade_date", "symbol"] + [f"db_ret_{h}d" for h in horizons]
    merged = replacements.merge(
        price[ret_cols],
        left_on=["date", "symbol"],
        right_on=["trade_date", "symbol"],
        how="left",
    ).drop(columns=["trade_date"], errors="ignore")
    return merged


def summarize_replacements(rep: pd.DataFrame, day: pd.DataFrame) -> dict[str, Any]:
    if rep.empty:
        return {}
    out: dict[str, Any] = {
        "days": int(day["date"].nunique()) if not day.empty else 0,
        "changed_days": int((day["replacement_count"] > 0).sum()) if not day.empty else 0,
        "avg_entered_per_day": safe_float(day["entered_count"].mean()) if not day.empty else None,
        "max_entered_per_day": safe_float(day["entered_count"].max()) if not day.empty else None,
        "total_enter_rows": int((rep["replacement_type"] == "entered_by_hmm").sum()),
        "total_drop_rows": int((rep["replacement_type"] == "dropped_by_hmm").sum()),
        "unique_enter_symbols": int(rep.loc[rep["replacement_type"] == "entered_by_hmm", "symbol"].nunique()),
        "unique_drop_symbols": int(rep.loc[rep["replacement_type"] == "dropped_by_hmm", "symbol"].nunique()),
        "mean_net_label_10d_by_day": safe_float(day["net_enter_minus_drop_label_10d"].mean()) if not day.empty else None,
        "positive_net_label_day_ratio": safe_float((day["net_enter_minus_drop_label_10d"] > 0).mean()) if not day.empty else None,
    }
    for col in ["label_10d", "db_ret_5d", "db_ret_10d", "db_ret_20d"]:
        if col not in rep.columns:
            continue
        ent = rep.loc[rep["replacement_type"] == "entered_by_hmm", col].dropna()
        drp = rep.loc[rep["replacement_type"] == "dropped_by_hmm", col].dropna()
        out[f"entered_mean_{col}"] = safe_float(ent.mean()) if not ent.empty else None
        out[f"dropped_mean_{col}"] = safe_float(drp.mean()) if not drp.empty else None
        out[f"net_mean_{col}"] = safe_float(ent.mean() - drp.mean()) if not ent.empty and not drp.empty else None
    return out


def enhanced_stock_contribution(base: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    def stock_profit(em: dict[str, Any]) -> dict[str, float]:
        out = {}
        for row in em.get("all_stocks") or []:
            code = row.get("code")
            profit = safe_float(row.get("profit"))
            if code and profit is not None:
                out[code] = profit
        return out

    b = stock_profit(base)
    o = stock_profit(other)
    common = set(b) & set(o)
    only_o = set(o) - set(b)
    only_b = set(b) - set(o)
    return {
        "common_count": len(common),
        "hmm_only_count": len(only_o),
        "base_only_count": len(only_b),
        "common_profit_diff": sum(o[s] - b[s] for s in common),
        "hmm_only_profit": sum(o[s] for s in only_o),
        "base_only_profit": sum(b[s] for s in only_b),
        "total_proxy_diff": sum(o[s] - b[s] for s in common) + sum(o[s] for s in only_o) - sum(b[s] for s in only_b),
    }


def daily_return_diff(base_report: pd.DataFrame | None, other_report: pd.DataFrame | None) -> dict[str, Any]:
    if base_report is None or other_report is None:
        return {}
    if not isinstance(base_report, pd.DataFrame) or not isinstance(other_report, pd.DataFrame):
        return {}
    b = base_report[["return"]].rename(columns={"return": "base_return"}).copy()
    o = other_report[["return"]].rename(columns={"return": "hmm_return"}).copy()
    df = b.join(o, how="inner")
    if df.empty:
        return {}
    df["diff"] = df["hmm_return"] - df["base_return"]
    worst = df.sort_values("diff").head(5)
    best = df.sort_values("diff", ascending=False).head(5)
    return {
        "days": len(df),
        "mean_daily_diff": safe_float(df["diff"].mean()),
        "positive_diff_ratio": safe_float((df["diff"] > 0).mean()),
        "worst_days": [(idx.strftime("%Y-%m-%d"), float(row["diff"])) for idx, row in worst.iterrows()],
        "best_days": [(idx.strftime("%Y-%m-%d"), float(row["diff"])) for idx, row in best.iterrows()],
    }


def write_markdown(
    path: Path,
    task_id: str,
    loops: list[LoopInfo],
    base_loop: LoopInfo,
    summaries: dict[str, dict[str, Any]],
    enhanced_comp: dict[str, dict[str, Any]],
    return_comp: dict[str, dict[str, Any]],
    day_paths: dict[str, Path],
    rep_paths: dict[str, Path],
) -> None:
    lines: list[str] = []
    lines.append(f"# HMM 离线诊断报告 - {task_id}")
    lines.append("")
    lines.append("## 执行边界")
    lines.append("- 本次诊断只通过 QEWorkspaceClient/节点 HTTP API 下载 artifact，没有修改 QE 策略代码。")
    lines.append("- Top50 重放口径是 raw score 与 HMM adjusted score 的排名替换，不等同于最终成交；最终成交还会受 n_drop、已有持仓、停牌/涨跌停、分钟执行影响。")
    lines.append("- label_10d 来自本次 QE 的 label.pkl；db_ret_5d/10d/20d 来自本地 market.kline_daily_raw，作为辅助诊断口径。")
    lines.append("")
    lines.append("## Loop 基线")
    lines.append("```text")
    lines.append("Loop  Status     HMM  Label        AnnRet    MaxDD      Sharpe    IC        RankIC    Snapshot")
    lines.append("----  ---------  ---  -----------  --------  ---------  --------  --------  --------  ------------------------------------")
    for lp in loops:
        lines.append(
            f"L{lp.loop_index:<3}  {lp.status:<9}  {str(lp.has_hmm):<3}  {(lp.label or ''):<11}  "
            f"{pct(lp.annualized_return):>8}  {pct(lp.max_drawdown):>9}  {num(lp.sharpe):>8}  "
            f"{num(lp.ic):>8}  {num(lp.rank_ic):>8}  {(lp.snapshot_id or '')[:36]}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## Top50 替换摘要")
    lines.append("```text")
    lines.append("Loop/Version                 Days  ChgDays  AvgEnter  MaxEnter  EnterSym  DropSym  NetLabel10D  NetDB5D    NetDB10D   NetDB20D")
    lines.append("---------------------------  ----  -------  --------  --------  --------  -------  ----------  --------  --------  --------")
    for label, s in summaries.items():
        lines.append(
            f"{label[:27]:<27}  {s.get('days', 0):>4}  {s.get('changed_days', 0):>7}  "
            f"{num(s.get('avg_entered_per_day'), 2):>8}  {num(s.get('max_entered_per_day'), 0):>8}  "
            f"{s.get('unique_enter_symbols', 0):>8}  {s.get('unique_drop_symbols', 0):>7}  "
            f"{pct(s.get('net_mean_label_10d')):>10}  {pct(s.get('net_mean_db_ret_5d')):>8}  "
            f"{pct(s.get('net_mean_db_ret_10d')):>8}  {pct(s.get('net_mean_db_ret_20d')):>8}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## 交易贡献粗归因（enhanced_metrics）")
    lines.append("```text")
    lines.append("Loop/Version                 Common  HMMOnly  BaseOnly  CommonProfitDiff  HMMOnlyProfit  BaseOnlyProfit  TotalProxyDiff")
    lines.append("---------------------------  ------  -------  --------  ----------------  -------------  --------------  --------------")
    for label, c in enhanced_comp.items():
        lines.append(
            f"{label[:27]:<27}  {c.get('common_count', 0):>6}  {c.get('hmm_only_count', 0):>7}  "
            f"{c.get('base_only_count', 0):>8}  {c.get('common_profit_diff', 0):>16,.0f}  "
            f"{c.get('hmm_only_profit', 0):>13,.0f}  {c.get('base_only_profit', 0):>14,.0f}  "
            f"{c.get('total_proxy_diff', 0):>14,.0f}"
        )
    lines.append("```")
    lines.append("")
    lines.append("## 日收益差异")
    lines.append("```text")
    lines.append("Loop/Version                 Days  MeanDailyDiff  PositiveRatio")
    lines.append("---------------------------  ----  -------------  -------------")
    for label, c in return_comp.items():
        lines.append(f"{label[:27]:<27}  {c.get('days', 0):>4}  {pct(c.get('mean_daily_diff'), 4):>13}  {pct(c.get('positive_diff_ratio')):>13}")
    lines.append("```")
    lines.append("")
    for label, c in return_comp.items():
        if not c:
            continue
        lines.append(f"### {label} 最影响日")
        lines.append("- 最拖累: " + ", ".join([f"{d}({pct(v, 3)})" for d, v in c.get("worst_days", [])]))
        lines.append("- 最贡献: " + ", ".join([f"{d}({pct(v, 3)})" for d, v in c.get("best_days", [])]))
        lines.append("")
    lines.append("## 产物")
    for label, p in rep_paths.items():
        lines.append(f"- {label} replacement rows: `{p}`")
    for label, p in day_paths.items():
        lines.append(f"- {label} daily summary: `{p}`")
    lines.append("")
    lines.append("## 当前判断")
    lines.append("- 如果 NetLabel10D/NetDB10D 为负，说明 HMM 替换进来的股票后验收益低于被挤出的 raw Top50 股票，是 overlay 直接拖累收益的证据。")
    lines.append("- 如果 NetLabel10D 为正但交易收益差，问题更可能在 n_drop、已有持仓、执行/停牌过滤或权重分配。")
    lines.append("- 如果替换数量极少但收益差明显，重点查少数高金额/长持仓股票；如果替换数量很大且净收益为负，重点查 HMM 系数方向与强度。")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def run(task_id: str, out_dir: Path, topk: int) -> Path:
    loops, _node_id, api_base = load_loops(task_id)
    completed = [lp for lp in loops if lp.status == "completed" and lp.experiment_id]
    if not completed:
        raise RuntimeError(f"No completed loops found for {task_id}")
    base_loop = next((lp for lp in completed if not lp.has_hmm), completed[0])
    client = QEWorkspaceClient(base_url=f"{api_base}/api/v1/qe_workspace")
    artifact_dir = out_dir / task_id / "artifacts"
    table_dir = out_dir / task_id / "tables"
    report_dir = ROOT / "docs" / "analysis"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    table_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    try:
        base_art = await download_loop_artifacts(client, task_id, base_loop, artifact_dir, need_pred=True, need_label=True)
        pred_ser = pred_to_series(base_art["pred"])
        label_ser = label_to_series(base_art["label"])
        loop_artifacts: dict[int, dict[str, Any]] = {base_loop.loop_index: base_art}
        for lp in completed:
            if lp.loop_index == base_loop.loop_index:
                continue
            loop_artifacts[lp.loop_index] = await download_loop_artifacts(client, task_id, lp, artifact_dir)
    finally:
        await client.close()

    exp_ids = [lp.experiment_id for lp in completed if lp.experiment_id]
    enhanced = enhanced_metrics_by_experiment(exp_ids)
    base_enhanced = enhanced.get(base_loop.experiment_id or "", {})
    summaries: dict[str, dict[str, Any]] = {}
    enhanced_comp: dict[str, dict[str, Any]] = {}
    return_comp: dict[str, dict[str, Any]] = {}
    rep_paths: dict[str, Path] = {}
    day_paths: dict[str, Path] = {}

    base_report = loop_artifacts.get(base_loop.loop_index, {}).get("report")
    for lp in completed:
        if not lp.has_hmm:
            continue
        art = loop_artifacts.get(lp.loop_index) or {}
        hmm = art.get("hmm")
        if not hmm:
            continue
        label = f"L{lp.loop_index}_{lp.label or 'HMM'}"
        rep, day, sectors = compute_replacements(pred_ser, label_ser, hmm, topk, label)
        rep = enrich_db_forward_returns(rep, horizons=[5, 10, 20])
        summary = summarize_replacements(rep, day)
        summaries[label] = summary
        rep_path = table_dir / f"{label}_top{topk}_replacements.csv"
        day_path = table_dir / f"{label}_top{topk}_daily_summary.csv"
        sector_path = table_dir / f"{label}_sector_coefficients.csv"
        rep.to_csv(rep_path, index=False, encoding="utf-8-sig")
        day.to_csv(day_path, index=False, encoding="utf-8-sig")
        sectors.to_csv(sector_path, index=False, encoding="utf-8-sig")
        rep_paths[label] = rep_path
        day_paths[label] = day_path
        other_enhanced = enhanced.get(lp.experiment_id or "", {})
        if base_enhanced and other_enhanced:
            enhanced_comp[label] = enhanced_stock_contribution(base_enhanced, other_enhanced)
        return_comp[label] = daily_return_diff(base_report, art.get("report"))

    report_path = report_dir / f"hmm_offline_diagnostic_{task_id}.md"
    write_markdown(
        report_path,
        task_id,
        loops,
        base_loop,
        summaries,
        enhanced_comp,
        return_comp,
        day_paths,
        rep_paths,
    )
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id")
    parser.add_argument("--out-dir", default=str(ROOT / ".codex_tmp" / "hmm_offline_diag"))
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()
    report = asyncio.run(run(args.task_id, Path(args.out_dir), args.topk))
    print(report)


if __name__ == "__main__":
    main()
