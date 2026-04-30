#!/usr/bin/env python3
"""Compare current DB HMM artifacts with offline dynamic HMM candidates.

Guardrails:
- no database writes;
- no QE experiment submission;
- no AIstock backend/frontend runtime changes.

The comparison uses the same qlib daily Top50/5D-rebalance proxy as the
dynamic HMM tuning loop, then adds existing DB coefficient artifacts that cover
the full one-year validation window.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2.extras

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import hmm_dynamic_offline_experiments as dyn  # noqa: E402
import hmm_horizon_v2_train as base  # noqa: E402


@dataclass(frozen=True)
class CompareArtifact:
    name: str
    group: str
    source: str
    preset_key: str
    payload: dict[str, Any]
    coefficients_path: str
    model_path: str | None
    config_id: str | None
    snapshot_id: str | None
    train_period: str | None
    val_period: str | None
    pit_status: str
    note: str


def parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def parse_period(text: str | None) -> tuple[date | None, date | None]:
    if not text or " ~ " not in str(text):
        return None, None
    left, right = str(text).split(" ~ ", 1)
    try:
        return parse_date(left), parse_date(right)
    except Exception:
        return None, None


def period_text(start: date | None, end: date | None) -> str | None:
    if start and end:
        return f"{start.isoformat()} ~ {end.isoformat()}"
    return None


def split_from_metrics_config(metrics: dict[str, Any], config: dict[str, Any]) -> tuple[str | None, str | None]:
    train_period = metrics.get("train_period")
    val_period = metrics.get("val_period")
    if not train_period:
        train_period = period_text(parse_date(config.get("train_start")), parse_date(config.get("train_end")))
    if not val_period:
        val_period = period_text(parse_date(config.get("val_start")), parse_date(config.get("val_end")))
    return str(train_period) if train_period else None, str(val_period) if val_period else None


def classify_pit(train_period: str | None, val_period: str | None, test_start: date) -> str:
    _, train_end = parse_period(train_period)
    _, val_end = parse_period(val_period)
    if train_end is None or val_end is None:
        return "diagnostic-only: unknown train/validation split"
    if train_end < test_start and val_end < test_start:
        return "PIT-compatible"
    return "diagnostic-only: train/validation overlaps 1Y backtest"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def json_dates(payload: dict[str, Any]) -> list[date]:
    daily = payload.get("daily_coefficients", {})
    if not isinstance(daily, dict):
        return []
    dates: list[date] = []
    for key in daily:
        try:
            dates.append(date.fromisoformat(str(key)[:10]))
        except Exception:
            continue
    return sorted(set(dates))


def covers_window(payload: dict[str, Any], start: date, end: date) -> bool:
    daily = payload.get("daily_coefficients", {})
    return isinstance(daily, dict) and start.isoformat() in daily and end.isoformat() in daily


def load_db_hmm_artifacts(conn, start: date, end: date) -> tuple[list[CompareArtifact], list[dict[str, Any]]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT c.config_id, c.display_name, c.config_json,
               s.snapshot_id, s.model_path, s.metrics_json, s.status, s.trained_at
        FROM model_train_configs c
        JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.model_type = 'sector_hmm'
          AND s.status IN ('completed', 'ready', 'success', 'succeeded')
        ORDER BY s.trained_at, c.display_name
        """
    )
    artifacts: list[CompareArtifact] = []
    excluded: list[dict[str, Any]] = []
    for row in cur.fetchall():
        config_json = row["config_json"] if isinstance(row["config_json"], dict) else {}
        metrics_json = row["metrics_json"] if isinstance(row["metrics_json"], dict) else {}
        train_period, val_period = split_from_metrics_config(metrics_json, config_json)
        pit_status = classify_pit(train_period, val_period, start)
        model_path = str(row["model_path"])
        model_file = Path(base.windows_to_wsl_path(model_path))
        model_dir = model_file.parent
        if not model_dir.exists():
            excluded.append(
                {
                    "source": "db",
                    "display_name": row["display_name"],
                    "model_path": model_path,
                    "reason": "model directory not found",
                }
            )
            continue
        for coeff_path in sorted(model_dir.glob("coefficients_*.json")):
            payload = read_json(coeff_path)
            dates = json_dates(payload)
            preset_key = str(payload.get("preset_key") or payload.get("preset") or coeff_path.stem)
            if not covers_window(payload, start, end):
                excluded.append(
                    {
                        "source": "db",
                        "display_name": row["display_name"],
                        "config_id": str(row["config_id"]),
                        "snapshot_id": str(row["snapshot_id"]),
                        "preset_key": preset_key,
                        "coefficients_path": base.wsl_to_windows_path(coeff_path),
                        "coverage_start": dates[0].isoformat() if dates else None,
                        "coverage_end": dates[-1].isoformat() if dates else None,
                        "reason": "coefficient artifact does not cover full 1Y window",
                    }
                )
                continue
            display_name = str(row["display_name"])
            artifacts.append(
                CompareArtifact(
                    name=f"{display_name}::{preset_key}",
                    group=display_name,
                    source="db",
                    preset_key=preset_key,
                    payload=payload,
                    coefficients_path=base.wsl_to_windows_path(coeff_path),
                    model_path=model_path,
                    config_id=str(row["config_id"]),
                    snapshot_id=str(row["snapshot_id"]),
                    train_period=train_period,
                    val_period=val_period,
                    pit_status=pit_status,
                    note="existing DB artifact",
                )
            )
    cur.close()
    return artifacts, excluded


def find_dynamic_candidate_coefficients(repo_root: Path) -> list[Path]:
    patterns = [
        ".codex_tmp/hmm_dynamic_tuning_pass8_20260429/models/"
        "offline_p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075_*/"
        "coefficients_p8_pup_w20_50_clip_0p9800_1p0150_conf_0p075_2025-03-11_2026-03-03.json",
        ".codex_tmp/hmm_dynamic_tuning_pass8_20260429/models/"
        "offline_p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10_*/"
        "coefficients_p8_pup_w20_50_clip_0p9800_1p0150_conf_0p10_2025-03-11_2026-03-03.json",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        matches = sorted(repo_root.glob(pattern), key=lambda p: p.stat().st_mtime)
        if matches:
            paths.append(matches[-1])
    return paths


def load_dynamic_artifacts(repo_root: Path, start: date, end: date) -> tuple[list[CompareArtifact], list[dict[str, Any]]]:
    artifacts: list[CompareArtifact] = []
    excluded: list[dict[str, Any]] = []
    for coeff_path in find_dynamic_candidate_coefficients(repo_root):
        payload = read_json(coeff_path)
        dates = json_dates(payload)
        variant_name = str(payload.get("variant_name") or coeff_path.parent.name)
        train_period = str(payload.get("train_period")) if payload.get("train_period") else None
        val_period = str(payload.get("validation_period")) if payload.get("validation_period") else None
        pit_status = classify_pit(train_period, val_period, start)
        if not covers_window(payload, start, end):
            excluded.append(
                {
                    "source": "offline_dynamic",
                    "variant_name": variant_name,
                    "coefficients_path": str(coeff_path),
                    "coverage_start": dates[0].isoformat() if dates else None,
                    "coverage_end": dates[-1].isoformat() if dates else None,
                    "reason": "coefficient artifact does not cover full 1Y window",
                }
            )
            continue
        artifacts.append(
            CompareArtifact(
                name=f"OFFLINE_DYNAMIC::{variant_name}",
                group=variant_name,
                source="offline_dynamic",
                preset_key=variant_name,
                payload=payload,
                coefficients_path=str(coeff_path),
                model_path=payload.get("model_path"),
                config_id=None,
                snapshot_id=None,
                train_period=train_period,
                val_period=val_period,
                pit_status=pit_status,
                note="not in DB; recommended offline candidate",
            )
        )
    return artifacts, excluded


def max_drawdown(nav: list[float]) -> float:
    peak = -float("inf")
    worst = 0.0
    for value in nav:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, value / peak - 1.0)
    return worst


def annualized_return(total_return: float, periods: int, rebalance_days: int) -> float:
    if periods <= 0:
        return 0.0
    return (1.0 + total_return) ** (252.0 / (periods * rebalance_days)) - 1.0


def avg_or_none(values: list[Any]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return float(np.mean(cleaned)) if cleaned else None


def run_version_backtest(
    *,
    artifact: CompareArtifact | None,
    by_date: dict[date, pd.DataFrame],
    signal_dates: list[date],
    date_sector_maps: dict[date, dict[str, str]],
    rebalance_days: int,
    topk: int,
) -> dict[str, Any]:
    name = artifact.name if artifact else "NO_HMM_BASELINE"
    payload = artifact.payload if artifact else None
    daily_coefficients = payload.get("daily_coefficients", {}) if payload else {}
    daily_sector_signals = payload.get("daily_sector_signals", {}) if payload else {}
    method = str(payload.get("method") or "multiplicative") if payload else "baseline"
    additive_beta = float(payload.get("additive_beta", 0.0)) if payload else 0.0

    nav = [1.0]
    period_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    previous: set[str] | None = None
    missing_map_count = 0
    missing_coeff_count = 0

    for signal_date in signal_dates:
        frame = by_date.get(signal_date)
        sector_map = date_sector_maps.get(signal_date, {})
        if frame is None or frame.empty or not sector_map:
            continue
        candidates = frame.dropna(subset=[f"fwd_{rebalance_days}d", "fwd_10d", "fwd_20d"]).copy()
        if candidates.empty:
            continue
        candidates["sector_code"] = candidates["ts_code"].map(sector_map)
        missing_map_count += int(candidates["sector_code"].isna().sum())
        candidates = candidates.dropna(subset=["sector_code"])
        if candidates.empty:
            continue

        raw_top = candidates.nlargest(topk, "raw_score")
        if payload:
            day_coeffs = daily_coefficients.get(signal_date.isoformat(), {})
            day_signals = daily_sector_signals.get(signal_date.isoformat(), {})
            coeffs: list[float] = []
            signals: list[float] = []
            for sector in candidates["sector_code"]:
                sector_key = str(sector)
                coeff = day_coeffs.get(sector_key)
                if coeff is None:
                    missing_coeff_count += 1
                    coeff = 1.0
                signal_info = day_signals.get(sector_key, {})
                coeffs.append(float(coeff))
                signals.append(float(signal_info.get("normalized_signal", 0.0)) if isinstance(signal_info, dict) else 0.0)
            candidates["hmm_coeff"] = coeffs
            candidates["hmm_signal"] = signals
            if method == "additive_pup":
                candidates["adjusted_score"] = candidates["raw_score"] + additive_beta * candidates["hmm_signal"]
            else:
                candidates["adjusted_score"] = candidates["raw_score"] * candidates["hmm_coeff"]
        else:
            candidates["hmm_coeff"] = 1.0
            candidates["hmm_signal"] = 0.0
            candidates["adjusted_score"] = candidates["raw_score"]

        selected = candidates.nlargest(topk, "adjusted_score")
        selected_set = set(selected["ts_code"])
        raw_set = set(raw_top["ts_code"])
        hmm_only = selected[selected["ts_code"].isin(selected_set - raw_set)]
        raw_only = raw_top[raw_top["ts_code"].isin(raw_set - selected_set)]
        period_return = float(selected[f"fwd_{rebalance_days}d"].mean()) if not selected.empty else 0.0
        nav.append(nav[-1] * (1.0 + period_return))
        turnover = None if previous is None else 1.0 - len(selected_set & previous) / max(1, topk)
        previous = selected_set

        selected_records: list[dict[str, Any]] = []
        weight = 1.0 / max(1, len(selected))
        for _, item in selected.iterrows():
            fwd_return = float(item[f"fwd_{rebalance_days}d"])
            contribution = weight * fwd_return
            record = {
                "ts_code": str(item["ts_code"]),
                "sector_code": str(item["sector_code"]),
                "hmm_only_label": bool(item["ts_code"] not in raw_set),
                "weight": weight,
                "fwd_return": fwd_return,
                "contribution": contribution,
                "raw_score": float(item["raw_score"]),
                "adjusted_score": float(item["adjusted_score"]),
                "hmm_coeff": float(item["hmm_coeff"]),
                "hmm_signal": float(item["hmm_signal"]),
            }
            selected_records.append(record)
            contribution_rows.append(
                {
                    "date": signal_date.isoformat(),
                    "ts_code": record["ts_code"],
                    "sector_code": record["sector_code"],
                    "hmm_only_label": record["hmm_only_label"],
                    "weight": weight,
                    "fwd_return": fwd_return,
                    "contribution": contribution,
                }
            )

        row: dict[str, Any] = {
            "date": signal_date.isoformat(),
            "period_return": period_return,
            "nav": nav[-1],
            "selected_count": int(len(selected)),
            "raw_selected_count": int(len(raw_top)),
            "overlap_raw": int(len(selected_set & raw_set)),
            "replaced_count": int(len(hmm_only)),
            "turnover_proxy": turnover,
            "avg_coeff": float(selected["hmm_coeff"].mean()) if not selected.empty else 1.0,
            "avg_signal": float(selected["hmm_signal"].mean()) if not selected.empty else 0.0,
            "coeff_gt1": int((selected["hmm_coeff"] > 1.000001).sum()),
            "coeff_lt1": int((selected["hmm_coeff"] < 0.999999).sum()),
            "hmm_only_symbols": sorted(selected_set - raw_set),
            "raw_only_symbols": sorted(raw_set - selected_set),
            "final_symbols": sorted(selected_set),
            "selected_records": selected_records,
        }
        for horizon in (5, 10, 20):
            row[f"selected_fwd{horizon}"] = float(selected[f"fwd_{horizon}d"].mean()) if len(selected) else None
            row[f"hmm_only_fwd{horizon}"] = float(hmm_only[f"fwd_{horizon}d"].mean()) if len(hmm_only) else None
            row[f"raw_only_fwd{horizon}"] = float(raw_only[f"fwd_{horizon}d"].mean()) if len(raw_only) else None
        period_rows.append(row)

    returns = [float(row["period_return"]) for row in period_rows]
    total_return = nav[-1] - 1.0 if nav else 0.0
    mean = float(np.mean(returns)) if returns else 0.0
    std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (mean / std * math.sqrt(252.0 / rebalance_days)) if std > 1e-12 else 0.0
    monthly: dict[str, float] = {}
    for row in period_rows:
        month = row["date"][:7]
        monthly[month] = (1.0 + monthly.get(month, 0.0)) * (1.0 + row["period_return"]) - 1.0

    summary: dict[str, Any] = {
        "name": name,
        "group": artifact.group if artifact else "No-HMM",
        "source": artifact.source if artifact else "baseline",
        "preset_key": artifact.preset_key if artifact else "raw",
        "snapshot_id": artifact.snapshot_id if artifact else None,
        "config_id": artifact.config_id if artifact else None,
        "train_period": artifact.train_period if artifact else None,
        "val_period": artifact.val_period if artifact else None,
        "pit_status": artifact.pit_status if artifact else "PIT-compatible",
        "note": artifact.note if artifact else "raw Top50 baseline",
        "coefficients_path": artifact.coefficients_path if artifact else None,
        "method": method,
        "periods": len(period_rows),
        "total_return": total_return,
        "annualized_return": annualized_return(total_return, len(period_rows), rebalance_days),
        "sharpe": sharpe,
        "max_drawdown": max_drawdown(nav),
        "monthly_win_rate": float(np.mean([1.0 if v > 0 else 0.0 for v in monthly.values()])) if monthly else 0.0,
        "avg_period_return": mean,
        "avg_replaced_count": avg_or_none([row["replaced_count"] for row in period_rows]),
        "avg_overlap_raw": avg_or_none([row["overlap_raw"] for row in period_rows]),
        "avg_turnover_proxy": avg_or_none([row["turnover_proxy"] for row in period_rows]),
        "avg_coeff": avg_or_none([row["avg_coeff"] for row in period_rows]),
        "avg_coeff_gt1": avg_or_none([row["coeff_gt1"] for row in period_rows]),
        "avg_coeff_lt1": avg_or_none([row["coeff_lt1"] for row in period_rows]),
        "capital_utilization_proxy": (avg_or_none([row["selected_count"] for row in period_rows]) or 0.0) / topk if topk else None,
        "buy_unfilled_rate_proxy": 0.0,
        "execution_proxy_note": "qlib close-to-close TopK proxy; no minute/order fill simulation",
        "missing_map_count": missing_map_count,
        "missing_coeff_count": missing_coeff_count,
        "final_holdings_count": len(period_rows[-1]["final_symbols"]) if period_rows else 0,
        "final_holdings": period_rows[-1]["final_symbols"] if period_rows else [],
        "period_rows": period_rows,
        "monthly_returns": monthly,
    }
    for horizon in (5, 10, 20):
        hmm_vals = [row[f"hmm_only_fwd{horizon}"] for row in period_rows if row[f"hmm_only_fwd{horizon}"] is not None]
        raw_vals = [row[f"raw_only_fwd{horizon}"] for row in period_rows if row[f"raw_only_fwd{horizon}"] is not None]
        summary[f"selected_fwd{horizon}"] = avg_or_none([row[f"selected_fwd{horizon}"] for row in period_rows])
        summary[f"hmm_only_fwd{horizon}"] = avg_or_none(hmm_vals)
        summary[f"raw_only_fwd{horizon}"] = avg_or_none(raw_vals)
        summary[f"replacement_spread_{horizon}"] = (
            float(np.mean(hmm_vals) - np.mean(raw_vals)) if hmm_vals and raw_vals else None
        )

    contribution_summary: list[dict[str, Any]] = []
    if contribution_rows:
        contrib_df = pd.DataFrame(contribution_rows)
        grouped = contrib_df.groupby("ts_code", as_index=False).agg(
            sector_code=("sector_code", "last"),
            total_contribution=("contribution", "sum"),
            selected_periods=("date", "count"),
            avg_fwd_return=("fwd_return", "mean"),
            hmm_only_periods=("hmm_only_label", "sum"),
            win_periods=("fwd_return", lambda values: int((values > 0).sum())),
        )
        grouped["win_ratio"] = grouped["win_periods"] / grouped["selected_periods"].clip(lower=1)
        top = grouped.nlargest(10, "total_contribution").assign(bucket="top")
        bottom = grouped.nsmallest(10, "total_contribution").assign(bucket="bottom")
        contribution_summary = pd.concat([top, bottom], ignore_index=True).to_dict(orient="records")
    summary["contribution_summary"] = contribution_summary
    return summary


def pct(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except Exception:
        return ""
    if not math.isfinite(number):
        return ""
    return f"{number * 100:.2f}%"


def write_outputs(output_root: Path, docs_report: Path | None, result: dict[str, Any]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "run_summary.json"
    summary_path = output_root / "summary.csv"
    monthly_path = output_root / "monthly.csv"
    contribution_path = output_root / "contributions.csv"
    report_path = output_root / "report.md"

    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_rows = [
        {k: v for k, v in item.items() if k not in {"period_rows", "monthly_returns", "contribution_summary", "final_holdings"}}
        for item in result["summaries"]
    ]
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False, encoding="utf-8-sig")

    monthly_rows = []
    baseline = next((item for item in result["summaries"] if item["name"] == "NO_HMM_BASELINE"), None)
    baseline_monthly = baseline.get("monthly_returns", {}) if baseline else {}
    for item in result["summaries"]:
        for month, ret in item.get("monthly_returns", {}).items():
            monthly_rows.append(
                {
                    "name": item["name"],
                    "source": item["source"],
                    "pit_status": item["pit_status"],
                    "month": month,
                    "return": ret,
                    "diff_vs_no_hmm": ret - baseline_monthly.get(month, 0.0),
                }
            )
    pd.DataFrame(monthly_rows).to_csv(monthly_path, index=False, encoding="utf-8-sig")

    contribution_rows = []
    for item in result["summaries"]:
        for row in item.get("contribution_summary", []):
            contribution_rows.append({"name": item["name"], **row})
    pd.DataFrame(contribution_rows).to_csv(contribution_path, index=False, encoding="utf-8-sig")

    ranked = sorted(result["summaries"], key=lambda x: (x["total_return"], x["sharpe"], -x["max_drawdown"]), reverse=True)
    pit_ranked = [item for item in ranked if item["pit_status"] == "PIT-compatible"]
    formal_best = pit_ranked[0] if pit_ranked else None
    baseline_total = baseline["total_return"] if baseline else 0.0

    lines = [
        "# HMM DB版本 vs 动态候选 1年脚本对比验证",
        "",
        f"- 生成时间: {result['created_at']}",
        f"- 窗口: {result['start']} ~ {result['end']}",
        f"- 方法: qlib daily Top{result['topk']} 等权, {result['rebalance_days']}D rebalance, trailing 5D/10D/20D raw score",
        "- 范围: 不写数据库、不启动QE实验、不修改AIstock后端/前端业务代码",
        "",
        "## 结论",
        "",
    ]
    if formal_best:
        lines.append(
            f"- 正式口径只看 PIT-compatible，当前最优是 `{formal_best['name']}`，"
            f"总收益 {pct(formal_best['total_return'])}，Sharpe {formal_best['sharpe']:.3f}，"
            f"相对 No-HMM {pct(formal_best['total_return'] - baseline_total)}。"
        )
    lines.extend(
        [
            "- DB中覆盖完整1年窗口的既有系数多为 diagnostic-only，因为训练/验证截止晚于 2025-03-11，不能作为正式最优结论。",
            "- 未覆盖完整1年窗口的DB系数没有参与排名，避免把6个月或单日结果混入1年对比。",
            "",
            "## 排名",
            "",
            "| Rank | Version | Source | PIT | Total | Ann. | Sharpe | MaxDD | Monthly Win | Avg Replaced | 5D Spread | 10D Spread | 20D Spread |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for idx, item in enumerate(ranked, 1):
        pit = "Y" if item["pit_status"] == "PIT-compatible" else "N"
        lines.append(
            f"| {idx} | `{item['name']}` | {item['source']} | {pit} | "
            f"{pct(item['total_return'])} | {pct(item['annualized_return'])} | {item['sharpe']:.3f} | "
            f"{pct(item['max_drawdown'])} | {pct(item['monthly_win_rate'])} | "
            f"{item.get('avg_replaced_count') or 0:.2f} | {pct(item.get('replacement_spread_5'))} | "
            f"{pct(item.get('replacement_spread_10'))} | {pct(item.get('replacement_spread_20'))} |"
        )

    lines.extend(["", "## 入库判断", ""])
    if formal_best and formal_best["source"] == "offline_dynamic" and formal_best["total_return"] > baseline_total:
        lines.append(
            f"- 建议进入“待入库候选”但暂不写DB：`{formal_best['group']}` 明显优于 No-HMM，"
            "也优于本次可正式比较的PIT版本。"
        )
        lines.append("- 入库前建议再由你确认：是否只入库主候选，还是同时保留 conf=0.10 作为稳健备选。")
    else:
        lines.append("- 暂不建议入库：正式PIT口径没有证明新动态候选显著优于可比基线。")
    lines.extend(
        [
            "",
            "## 未纳入完整1年排名的DB系数",
            "",
            "| Version | Preset | Coverage | Reason |",
            "|---|---|---|---|",
        ]
    )
    for item in result["excluded_artifacts"]:
        if item.get("source") != "db":
            continue
        coverage = f"{item.get('coverage_start')} ~ {item.get('coverage_end')}"
        lines.append(
            f"| `{item.get('display_name')}` | `{item.get('preset_key')}` | {coverage} | {item.get('reason')} |"
        )

    lines.extend(
        [
            "",
            "## 产物",
            "",
            f"- JSON: `{json_path}`",
            f"- Summary CSV: `{summary_path}`",
            f"- Monthly CSV: `{monthly_path}`",
            f"- Contributions CSV: `{contribution_path}`",
        ]
    )
    report_text = "\n".join(lines) + "\n"
    report_path.write_text(report_text, encoding="utf-8")
    if docs_report:
        docs_report.parent.mkdir(parents=True, exist_ok=True)
        docs_report.write_text(report_text, encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare existing DB HMM versions with offline dynamic candidates")
    parser.add_argument("--start", default="2025-03-11")
    parser.add_argument("--end", default="2026-03-03")
    parser.add_argument("--output-root", default="/mnt/f/Dev/AIstock/.codex_tmp/hmm_db_vs_dynamic_1y_20260429")
    parser.add_argument(
        "--docs-report",
        default="/mnt/f/Dev/AIstock/docs/analysis/hmm_db_vs_dynamic_1y_comparison_report_20260429.md",
    )
    parser.add_argument("--qlib-uri", default="/home/lc999/data/qlib_bin")
    parser.add_argument("--instruments-file", default="/home/lc999/data/qlib_bin/instruments/all.txt")
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    start = parse_date(args.start)
    end = parse_date(args.end)
    assert start and end
    repo_root = Path(__file__).resolve().parents[1]
    output_root = Path(base.windows_to_wsl_path(args.output_root)).resolve()
    docs_report = Path(base.windows_to_wsl_path(args.docs_report)).resolve() if args.docs_report else None

    conn = base.connect_db(
        base.DBConfig(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
        )
    )
    db_artifacts, excluded_db = load_db_hmm_artifacts(conn, start, end)
    memberships = dyn.fetch_sector_memberships(conn)
    conn.close()

    dynamic_artifacts, excluded_dynamic = load_dynamic_artifacts(repo_root, start, end)
    artifacts = db_artifacts + dynamic_artifacts
    print(f"Included artifacts: DB={len(db_artifacts)}, dynamic={len(dynamic_artifacts)}")
    print(f"Excluded artifacts: DB={len(excluded_db)}, dynamic={len(excluded_dynamic)}")

    qlib_start = start - timedelta(days=80)
    qlib_end = end + timedelta(days=12)
    qlib_df = dyn.load_qlib_prices(
        args.qlib_uri,
        Path(args.instruments_file),
        qlib_start,
        qlib_end,
        args.max_symbols,
    )
    scored = dyn.prepare_stock_scores(qlib_df)
    test_dates = sorted(d for d in scored["trade_date"].unique() if start <= d <= end)
    signal_dates = test_dates[:: args.rebalance_days]
    signal_date_set = set(signal_dates)
    by_date = {td: frame for td, frame in scored.groupby("trade_date") if td in signal_date_set}
    date_sector_maps = dyn.build_date_sector_maps(memberships, signal_dates)
    print(f"qlib_rows={len(scored)}, test_dates={len(test_dates)}, signal_dates={len(signal_dates)}")

    summaries = [
        run_version_backtest(
            artifact=None,
            by_date=by_date,
            signal_dates=signal_dates,
            date_sector_maps=date_sector_maps,
            rebalance_days=args.rebalance_days,
            topk=args.topk,
        )
    ]
    for artifact in artifacts:
        print(f"Backtesting {artifact.name}")
        summaries.append(
            run_version_backtest(
                artifact=artifact,
                by_date=by_date,
                signal_dates=signal_dates,
                date_sector_maps=date_sector_maps,
                rebalance_days=args.rebalance_days,
                topk=args.topk,
            )
        )

    result = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "topk": args.topk,
        "rebalance_days": args.rebalance_days,
        "scope": {
            "db_write": False,
            "qe_experiment": False,
            "application_code_change": False,
            "qlib_uri": args.qlib_uri,
        },
        "artifact_count": {
            "db_included": len(db_artifacts),
            "dynamic_included": len(dynamic_artifacts),
            "db_excluded": len(excluded_db),
            "dynamic_excluded": len(excluded_dynamic),
        },
        "excluded_artifacts": excluded_db + excluded_dynamic,
        "summaries": summaries,
    }
    write_outputs(output_root, docs_report, result)
    compact = [
        {k: v for k, v in item.items() if k not in {"period_rows", "monthly_returns", "contribution_summary", "final_holdings"}}
        for item in sorted(summaries, key=lambda x: (x["total_return"], x["sharpe"]), reverse=True)
    ]
    print(json.dumps({"output_root": str(output_root), "docs_report": str(docs_report), "summary": compact}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
