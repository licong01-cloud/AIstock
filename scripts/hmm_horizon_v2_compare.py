#!/usr/bin/env python3
"""Script-only six-month comparison of HMM coefficient artifacts.

This does not start QE experiments. It uses a causal trailing-return raw score
and compares HMM overlays through equal-weight Top50 5-day rebalances.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2


@dataclass(frozen=True)
class HMMArtifact:
    name: str
    display_name: str
    snapshot_id: str | None
    model_path: str | None
    coefficients_path: str
    preset_key: str
    payload: dict[str, Any]
    leak_note: str


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def windows_to_wsl_path(path: str) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def local_path(path: str) -> Path:
    return Path(windows_to_wsl_path(path))


def candidate_hosts(initial: str) -> list[str]:
    hosts: list[str] = []
    for item in (initial, os.getenv("TDX_DB_HOST"), "127.0.0.1", "localhost"):
        if item and item not in hosts:
            hosts.append(item)
    try:
        ip = subprocess.check_output(
            "sed -n 's/^nameserver //p' /etc/resolv.conf | head -1",
            shell=True,
            text=True,
            timeout=3,
        ).strip()
        if ip and ip not in hosts:
            hosts.append(ip)
    except Exception:
        pass
    return hosts


def connect_db(args: argparse.Namespace):
    errors: list[str] = []
    for host in candidate_hosts(args.db_host):
        try:
            conn = psycopg2.connect(
                host=host,
                port=args.db_port,
                dbname=args.db_name,
                user=args.db_user,
                password=args.db_password or os.getenv("TDX_DB_PASSWORD", ""),
                connect_timeout=5,
            )
            print(f"DB connected via host={host}")
            return conn
        except Exception as exc:
            errors.append(f"{host}: {str(exc).splitlines()[0]}")
    raise RuntimeError("Cannot connect to DB. Tried: " + "; ".join(errors))


def covers_period(payload: dict[str, Any], start: date, end: date) -> bool:
    daily = payload.get("daily_coefficients")
    if not isinstance(daily, dict):
        return False
    return start.isoformat() in daily and end.isoformat() in daily


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_period_from_metrics(metrics: dict[str, Any], key: str) -> tuple[date | None, date | None]:
    text = str(metrics.get(key) or "")
    if " ~ " not in text:
        return None, None
    left, right = text.split(" ~ ", 1)
    try:
        return parse_date(left.strip()), parse_date(right.strip())
    except Exception:
        return None, None


def discover_hmm_artifacts(conn, start: date, end: date, extra_result: str | None) -> list[HMMArtifact]:
    artifacts: list[HMMArtifact] = []
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.display_name, s.snapshot_id, s.model_path, s.metrics_json
        FROM model_train_configs c
        JOIN model_train_snapshots s ON s.config_id = c.config_id
        WHERE c.model_type = 'sector_hmm' AND s.status IN ('completed', 'ready', 'success', 'succeeded')
        ORDER BY s.trained_at
        """
    )
    for display_name, snapshot_id, model_path, metrics_json in cur.fetchall():
        model_file = local_path(str(model_path))
        model_dir = model_file.parent
        if not model_dir.exists():
            continue
        metrics = metrics_json if isinstance(metrics_json, dict) else {}
        train_start, train_end = parse_period_from_metrics(metrics, "train_period")
        val_start, val_end = parse_period_from_metrics(metrics, "val_period")
        leak_note = "PIT-compatible"
        if (train_end and train_end >= start) or (val_end and val_end >= start):
            leak_note = "diagnostic-only: train/val overlaps backtest"
        for coeff_file in sorted(model_dir.glob("coefficients_*.json")):
            payload = read_json(coeff_file)
            if not covers_period(payload, start, end):
                continue
            preset_key = str(payload.get("preset_key") or payload.get("preset") or coeff_file.stem)
            artifacts.append(
                HMMArtifact(
                    name=f"{display_name}::{preset_key}",
                    display_name=str(display_name),
                    snapshot_id=str(snapshot_id),
                    model_path=str(model_path),
                    coefficients_path=str(coeff_file),
                    preset_key=preset_key,
                    payload=payload,
                    leak_note=leak_note,
                )
            )
    cur.close()

    if extra_result:
        result = read_json(local_path(extra_result))
        coeff_path = local_path(result["coefficients_path"])
        payload = read_json(coeff_path)
        if covers_period(payload, start, end):
            artifacts.append(
                HMMArtifact(
                    name=f"{result['display_name']}::{payload.get('preset_key', 'preset_horizon_v2')}",
                    display_name=result["display_name"],
                    snapshot_id=result.get("snapshot_id"),
                    model_path=result.get("model_path"),
                    coefficients_path=str(coeff_path),
                    preset_key=str(payload.get("preset_key", "preset_horizon_v2")),
                    payload=payload,
                    leak_note="PIT-compatible",
                )
            )
    # Remove duplicates by coefficient path.
    unique: dict[str, HMMArtifact] = {}
    for artifact in artifacts:
        unique[artifact.coefficients_path] = artifact
    return list(unique.values())


def fetch_stock_daily(conn, start: date, end: date) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ON (ts_code, trade_date)
            trade_date, ts_code, close_li::double precision AS close_li,
            volume_hand::double precision AS volume_hand
        FROM market.kline_daily_raw
        WHERE trade_date BETWEEN %s AND %s
          AND close_li IS NOT NULL
          AND close_li > 0
          AND volume_hand IS NOT NULL
          AND volume_hand > 0
        ORDER BY ts_code, trade_date, adjust_type
    """
    df = pd.read_sql(sql, conn, params=(start, end))
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["close"] = df["close_li"].astype(float)
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    return df


def prepare_stock_scores(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("ts_code", group_keys=False)
    for window in (5, 10, 20):
        df[f"past_{window}d"] = grouped["close"].pct_change(window)
        df[f"fwd_{window}d"] = grouped["close"].shift(-window) / df["close"] - 1.0
    for window in (5, 10, 20):
        df[f"rank_past_{window}d"] = df.groupby("trade_date")[f"past_{window}d"].rank(pct=True)
    df["raw_score"] = (
        0.35 * df["rank_past_5d"]
        + 0.35 * df["rank_past_10d"]
        + 0.30 * df["rank_past_20d"]
    )
    return df[np.isfinite(df["raw_score"])].copy()


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


def run_one_version(
    *,
    name: str,
    artifact: HMMArtifact | None,
    by_date: dict[date, pd.DataFrame],
    signal_dates: list[date],
    rebalance_days: int,
    topk: int,
) -> dict[str, Any]:
    nav = [1.0]
    period_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    previous: set[str] | None = None
    missing_map_count = 0
    missing_coeff_count = 0
    daily_coefficients = artifact.payload.get("daily_coefficients", {}) if artifact else {}
    stock_sector_map = artifact.payload.get("stock_sector_map", {}) if artifact else {}

    for signal_date in signal_dates:
        frame = by_date.get(signal_date)
        if frame is None or frame.empty:
            continue
        candidates = frame.dropna(subset=[f"fwd_{rebalance_days}d", "fwd_10d", "fwd_20d"]).copy()
        if candidates.empty:
            continue
        raw_top = candidates.nlargest(topk, "raw_score")
        if artifact:
            day_coeffs = daily_coefficients.get(signal_date.isoformat(), {})
            coeffs: list[float] = []
            for symbol in candidates["ts_code"]:
                sector = stock_sector_map.get(symbol)
                if not sector:
                    coeffs.append(1.0)
                    missing_map_count += 1
                    continue
                coeff = day_coeffs.get(str(sector))
                if coeff is None:
                    missing_coeff_count += 1
                    coeff = 1.0
                coeffs.append(float(coeff))
            candidates["hmm_coeff"] = coeffs
            candidates["adjusted_score"] = candidates["raw_score"] * candidates["hmm_coeff"]
            selected = candidates.nlargest(topk, "adjusted_score")
        else:
            candidates["hmm_coeff"] = 1.0
            candidates["adjusted_score"] = candidates["raw_score"]
            selected = candidates.nlargest(topk, "adjusted_score")

        selected_set = set(selected["ts_code"])
        raw_set = set(raw_top["ts_code"])
        period_return = float(selected[f"fwd_{rebalance_days}d"].mean())
        selected_records = []
        for _, item in selected.iterrows():
            fwd_return = float(item[f"fwd_{rebalance_days}d"])
            weight = 1.0 / max(1, len(selected))
            contribution = weight * fwd_return
            selected_records.append(
                {
                    "ts_code": str(item["ts_code"]),
                    "weight": weight,
                    "fwd_return": fwd_return,
                    "contribution": contribution,
                    "raw_score": float(item["raw_score"]),
                    "adjusted_score": float(item["adjusted_score"]),
                    "hmm_coeff": float(item["hmm_coeff"]),
                }
            )
            contribution_rows.append(
                {
                    "date": signal_date.isoformat(),
                    "ts_code": str(item["ts_code"]),
                    "weight": weight,
                    "fwd_return": fwd_return,
                    "contribution": contribution,
                }
            )
        nav.append(nav[-1] * (1.0 + period_return))
        overlap_raw = len(selected_set & raw_set)
        turnover = None if previous is None else 1.0 - len(selected_set & previous) / max(1, topk)
        previous = selected_set
        hmm_only = selected[selected["ts_code"].isin(selected_set - raw_set)]
        raw_only = raw_top[raw_top["ts_code"].isin(raw_set - selected_set)]
        period_rows.append(
            {
                "date": signal_date.isoformat(),
                "period_return": period_return,
                "nav": nav[-1],
                "overlap_raw": overlap_raw,
                "turnover": turnover,
                "selected_count": len(selected),
                "avg_coeff": float(selected["hmm_coeff"].mean()),
                "coeff_gt1": int((selected["hmm_coeff"] > 1.000001).sum()),
                "coeff_lt1": int((selected["hmm_coeff"] < 0.999999).sum()),
                "fwd5_mean": float(selected["fwd_5d"].mean()),
                "fwd10_mean": float(selected["fwd_10d"].mean()),
                "fwd20_mean": float(selected["fwd_20d"].mean()),
                "hmm_only_count": int(len(hmm_only)),
                "hmm_only_fwd5": float(hmm_only["fwd_5d"].mean()) if len(hmm_only) else None,
                "hmm_only_fwd10": float(hmm_only["fwd_10d"].mean()) if len(hmm_only) else None,
                "hmm_only_fwd20": float(hmm_only["fwd_20d"].mean()) if len(hmm_only) else None,
                "raw_only_fwd5": float(raw_only["fwd_5d"].mean()) if len(raw_only) else None,
                "raw_only_fwd10": float(raw_only["fwd_10d"].mean()) if len(raw_only) else None,
                "raw_only_fwd20": float(raw_only["fwd_20d"].mean()) if len(raw_only) else None,
                "final_symbols": sorted(selected_set),
                "selected_records": selected_records,
            }
        )

    returns = [row["period_return"] for row in period_rows]
    total_return = nav[-1] - 1.0 if nav else 0.0
    vol = float(np.std(returns, ddof=1) * math.sqrt(252.0 / rebalance_days)) if len(returns) > 1 else 0.0
    ann = annualized_return(total_return, len(returns), rebalance_days)
    sharpe = ann / vol if vol > 1e-12 else 0.0
    monthly: dict[str, float] = {}
    for row in period_rows:
        month = row["date"][:7]
        monthly[month] = (1.0 + monthly.get(month, 0.0)) * (1.0 + row["period_return"]) - 1.0

    def avg(key: str) -> float | None:
        vals = [row[key] for row in period_rows if row.get(key) is not None]
        return float(np.mean(vals)) if vals else None

    summary = {
        "name": name,
        "preset_key": artifact.preset_key if artifact else "raw",
        "snapshot_id": artifact.snapshot_id if artifact else None,
        "coefficients_path": artifact.coefficients_path if artifact else None,
        "leak_note": artifact.leak_note if artifact else "raw",
        "periods": len(period_rows),
        "total_return": total_return,
        "annualized_return": ann,
        "annualized_volatility": vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown(nav),
        "win_period_ratio": float(np.mean([ret > 0 for ret in returns])) if returns else 0.0,
        "avg_period_return": float(np.mean(returns)) if returns else 0.0,
        "avg_overlap_raw": avg("overlap_raw"),
        "avg_turnover": avg("turnover"),
        "avg_coeff": avg("avg_coeff"),
        "avg_coeff_gt1": avg("coeff_gt1"),
        "avg_coeff_lt1": avg("coeff_lt1"),
        "avg_fwd5": avg("fwd5_mean"),
        "avg_fwd10": avg("fwd10_mean"),
        "avg_fwd20": avg("fwd20_mean"),
        "avg_hmm_only_fwd5": avg("hmm_only_fwd5"),
        "avg_hmm_only_fwd10": avg("hmm_only_fwd10"),
        "avg_hmm_only_fwd20": avg("hmm_only_fwd20"),
        "avg_raw_only_fwd5": avg("raw_only_fwd5"),
        "avg_raw_only_fwd10": avg("raw_only_fwd10"),
        "avg_raw_only_fwd20": avg("raw_only_fwd20"),
        "avg_hmm_only_count": avg("hmm_only_count"),
        "avg_selected_count": avg("selected_count"),
        "capital_utilization_proxy": avg("selected_count") / topk if topk else None,
        "buy_unfilled_rate_proxy": 0.0,
        "execution_proxy_note": "TopK close-to-close proxy; no minute/order fill simulation",
        "missing_map_count": missing_map_count,
        "missing_coeff_count": missing_coeff_count,
        "final_holdings_count": len(period_rows[-1]["final_symbols"]) if period_rows else 0,
        "final_holdings": period_rows[-1]["final_symbols"] if period_rows else [],
    }

    contribution_summary: list[dict[str, Any]] = []
    if contribution_rows:
        contrib_df = pd.DataFrame(contribution_rows)
        grouped = contrib_df.groupby("ts_code", as_index=False).agg(
            total_contribution=("contribution", "sum"),
            selected_periods=("date", "count"),
            avg_fwd_return=("fwd_return", "mean"),
            win_periods=("fwd_return", lambda values: int((values > 0).sum())),
        )
        grouped["win_ratio"] = grouped["win_periods"] / grouped["selected_periods"].clip(lower=1)
        top = grouped.nlargest(10, "total_contribution").assign(bucket="top")
        bottom = grouped.nsmallest(10, "total_contribution").assign(bucket="bottom")
        contribution_summary = pd.concat([top, bottom], ignore_index=True).to_dict(orient="records")

    return {
        "summary": summary,
        "periods": period_rows,
        "monthly": monthly,
        "contribution_summary": contribution_summary,
    }


def pct(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value) * 100:.2f}%"


def write_report(path: Path, result: dict[str, Any]) -> None:
    rows = result["summary"]
    lines = [
        "# HMM Horizon v2 Script-Only Six-Month Backtest Report",
        "",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"Window: {result['start']} to {result['end']}",
        f"Method: Top{result['topk']} equal-weight, {result['rebalance_days']}D rebalance, causal trailing 5D/10D/20D raw score.",
        "",
        "## Summary",
        "",
        "| Version | Preset | Total | AnnRet | MaxDD | Sharpe | Avg Overlap Raw | Avg Turnover | Avg Fwd5 | Avg HMM-only Fwd5 | Note |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {name} | {preset} | {total} | {ann} | {dd} | {sharpe:.3f} | {overlap:.2f} | {turnover} | {fwd5} | {hmm5} | {note} |".format(
                name=row["name"],
                preset=row["preset_key"],
                total=pct(row["total_return"]),
                ann=pct(row["annualized_return"]),
                dd=pct(row["max_drawdown"]),
                sharpe=float(row["sharpe"]),
                overlap=float(row["avg_overlap_raw"] or 0.0),
                turnover=pct(row["avg_turnover"]),
                fwd5=pct(row["avg_fwd5"]),
                hmm5=pct(row["avg_hmm_only_fwd5"]),
                note=row["leak_note"],
            )
        )
    lines += [
        "",
        "## Diagnostics",
        "",
        "- This is not a QE experiment and does not include V25 minute execution.",
        "- Capital utilization and buy-unfilled-rate are close-to-close proxies only: Top50 fully invested by construction, with no real order-fill model.",
        "- Existing HMM snapshots whose train/validation period overlaps the backtest are marked diagnostic-only.",
        "- Raw score uses only trailing 5D/10D/20D ranks, so HMM versions are compared against the same non-QE stock signal.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare HMM coefficient artifacts through a script-only 5D Top50 backtest")
    parser.add_argument("--start", default="2025-09-01")
    parser.add_argument("--end", default="2026-03-03")
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--topk", type=int, default=50)
    parser.add_argument("--new-result-json", default=None)
    parser.add_argument("--output-prefix", default=".codex_tmp/hmm_horizon_v2_backtest_20260428")
    parser.add_argument("--db-host", default=os.getenv("TDX_DB_HOST", "127.0.0.1"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("TDX_DB_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("TDX_DB_NAME", "aistock"))
    parser.add_argument("--db-user", default=os.getenv("TDX_DB_USER", "postgres"))
    parser.add_argument("--db-password", default=os.getenv("TDX_DB_PASSWORD", ""))
    args = parser.parse_args()

    start = parse_date(args.start)
    end = parse_date(args.end)
    data_start = start - pd.Timedelta(days=60).to_pytimedelta()
    data_end = end + pd.Timedelta(days=30).to_pytimedelta()
    conn = connect_db(args)
    artifacts = discover_hmm_artifacts(conn, start, end, args.new_result_json)
    print(f"Discovered {len(artifacts)} HMM coefficient artifacts covering {start} to {end}")

    stock_df = fetch_stock_daily(conn, data_start, data_end)
    conn.close()
    stock_df = prepare_stock_scores(stock_df)
    stock_df = stock_df[(stock_df["trade_date"] >= start) & (stock_df["trade_date"] <= end)].copy()
    by_date = {td: frame.copy() for td, frame in stock_df.groupby("trade_date")}
    all_dates = sorted(by_date)
    signal_dates = all_dates[:: args.rebalance_days]
    print(f"Stock rows={len(stock_df)}, signal_dates={len(signal_dates)}")

    outputs: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "rebalance_days": args.rebalance_days,
        "topk": args.topk,
        "summary": [],
        "versions": {},
        "monthly": {},
        "contributions": {},
    }
    raw_result = run_one_version(
        name="RAW_NO_HMM",
        artifact=None,
        by_date=by_date,
        signal_dates=signal_dates,
        rebalance_days=args.rebalance_days,
        topk=args.topk,
    )
    outputs["summary"].append(raw_result["summary"])
    outputs["versions"]["RAW_NO_HMM"] = raw_result["periods"]
    outputs["monthly"]["RAW_NO_HMM"] = raw_result["monthly"]
    outputs["contributions"]["RAW_NO_HMM"] = raw_result["contribution_summary"]

    for artifact in artifacts:
        version_result = run_one_version(
            name=artifact.name,
            artifact=artifact,
            by_date=by_date,
            signal_dates=signal_dates,
            rebalance_days=args.rebalance_days,
            topk=args.topk,
        )
        outputs["summary"].append(version_result["summary"])
        outputs["versions"][artifact.name] = version_result["periods"]
        outputs["monthly"][artifact.name] = version_result["monthly"]
        outputs["contributions"][artifact.name] = version_result["contribution_summary"]

    out_prefix = Path(args.output_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = out_prefix.with_suffix(".json")
    md_path = out_prefix.with_suffix(".md")
    summary_path = out_prefix.with_name(out_prefix.name + "_summary.csv")
    monthly_path = out_prefix.with_name(out_prefix.name + "_monthly.csv")
    json_path.write_text(json.dumps(outputs, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(outputs["summary"]).to_csv(summary_path, index=False)
    pd.DataFrame(outputs["monthly"]).sort_index(axis=1).to_csv(monthly_path)
    write_report(md_path, outputs)
    print(f"Wrote {json_path}")
    print(f"Wrote {summary_path}")
    print(f"Wrote {monthly_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
