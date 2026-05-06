from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


DEFAULT_PROVIDER_URI = "/home/lc999/data/qlib_bin_20260428_shsz_candidate"
DEFAULT_SNAPSHOT_DIR = "/mnt/f/Dev/AIstock/qlib_snapshots/qlib_20260428_shsz_candidate"
DEFAULT_OUTPUT_DIR = "/mnt/f/Dev/AIstock/reports/qlib_multi_dataset_smoke_20260428"

PREFERRED_INSTRUMENTS = [
    "000001.SZ",
    "000002.SZ",
    "000063.SZ",
    "000333.SZ",
    "000651.SZ",
    "000725.SZ",
    "000858.SZ",
    "002415.SZ",
    "002594.SZ",
    "300059.SZ",
    "300750.SZ",
    "600000.SH",
    "600009.SH",
    "600031.SH",
    "600036.SH",
    "600050.SH",
    "600276.SH",
    "600519.SH",
    "601318.SH",
    "601688.SH",
    "603259.SH",
]

STATIC_FEATURE_GROUPS = {
    "daily_basic": ["db_turnover_rate", "db_pe_ttm", "db_pb", "db_total_mv"],
    "moneyflow": [
        "mf_total_net_amt_ratio_5d",
        "mf_main_net_amt_ratio_5d",
        "mf_elg_net_amt_ratio_5d",
        "mf_total_net_amt_ratio_20d",
    ],
    "bak_basic": ["bb_rev_yoy", "bb_profit_yoy", "bb_gpr", "bb_holder_num"],
    "cyq_perf": ["cp_winner_rate", "cp_weight_avg", "cp_cost_50pct"],
    "sector_data": ["sw2_pct_change", "sw2_pe", "sw2_pb", "sw2_mf_net_amt"],
}

QLIB_FIELDS = [
    "$open",
    "$high",
    "$low",
    "$close",
    "$volume",
    "$amount",
    "$factor",
    "$up_limit_price",
    "$down_limit_price",
    "$prev_close",
    "$limit_up",
    "$limit_down",
]


@dataclass
class InstrumentRange:
    instrument: str
    start: pd.Timestamp
    end: pd.Timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a small-universe Qlib smoke backtest that reads Qlib bin fields "
            "and snapshot static factors from multiple AIstock source datasets."
        )
    )
    parser.add_argument("--provider-uri", default=DEFAULT_PROVIDER_URI)
    parser.add_argument("--snapshot-dir", default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--feature-start", default="2025-04-01")
    parser.add_argument("--train-start", default="2025-07-01")
    parser.add_argument("--train-end", default="2026-02-27")
    parser.add_argument("--test-start", default="2026-03-10")
    parser.add_argument("--test-end", default="2026-04-28")
    parser.add_argument("--num-stocks", type=int, default=20)
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument("--drop", type=int, default=2)
    parser.add_argument(
        "--min-feature-coverage",
        type=float,
        default=0.85,
        help="Fail if any selected static/raw factor has lower coverage in train+test.",
    )
    parser.add_argument(
        "--min-signal-dates",
        type=int,
        default=20,
        help="Fail if the generated no-leak test signal has fewer dates.",
    )
    return parser.parse_args()


def ts(value: str | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def read_instrument_ranges(provider_uri: Path) -> list[InstrumentRange]:
    path = provider_uri / "instruments" / "all.txt"
    if not path.exists():
        raise FileNotFoundError(f"Qlib all.txt not found: {path}")
    ranges: list[InstrumentRange] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        parts = raw.strip().split()
        if len(parts) < 3:
            continue
        ranges.append(InstrumentRange(parts[0].upper(), ts(parts[1]), ts(parts[2])))
    if not ranges:
        raise RuntimeError(f"No instruments parsed from {path}")
    return ranges


def choose_backtest_end(provider_uri: Path, test_start: pd.Timestamp, test_end: pd.Timestamp) -> tuple[pd.Timestamp, str | None]:
    path = provider_uri / "calendars" / "day.txt"
    if not path.exists():
        raise FileNotFoundError(f"Qlib day calendar not found: {path}")
    cal = pd.to_datetime([line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]).normalize()
    if len(cal) < 2:
        raise RuntimeError(f"Qlib day calendar is too short: {path}")
    max_calendar = cal.max()
    in_test = cal[(cal >= test_start) & (cal <= test_end)]
    if len(in_test) < 2:
        raise RuntimeError(f"Not enough calendar dates in test range {test_start.date()} ~ {test_end.date()}")
    # Qlib's daily TradeCalendarManager asks for the next calendar point after the
    # requested end date. If the dataset stops exactly at test_end, use the prior
    # trading day for portfolio simulation while still validating test_end data.
    if test_end >= max_calendar:
        adjusted = in_test[-2]
        return adjusted, (
            f"Qlib calendar ends at {max_calendar.date()}, so portfolio backtest end "
            f"was adjusted from {test_end.date()} to {adjusted.date()} to avoid requiring a future calendar row."
        )
    return test_end, None


def choose_instruments(provider_uri: Path, train_start: pd.Timestamp, test_end: pd.Timestamp, num_stocks: int) -> list[str]:
    ranges = read_instrument_ranges(provider_uri)
    eligible = {
        row.instrument: row
        for row in ranges
        if row.start <= train_start and row.end >= test_end and (row.instrument.endswith(".SZ") or row.instrument.endswith(".SH"))
    }
    selected = [code for code in PREFERRED_INSTRUMENTS if code in eligible]
    if len(selected) < num_stocks:
        for code in sorted(eligible):
            if code not in selected:
                selected.append(code)
            if len(selected) >= num_stocks:
                break
    selected = selected[:num_stocks]
    if len(selected) < max(2, min(num_stocks, 5)):
        raise RuntimeError(f"Only {len(selected)} eligible instruments found for {train_start.date()} ~ {test_end.date()}")
    return selected


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("$") for col in out.columns]
    idx = out.index
    if not isinstance(idx, pd.MultiIndex) or idx.nlevels != 2:
        raise RuntimeError(f"Expected a 2-level Qlib feature index, got {type(idx).__name__} with nlevels={getattr(idx, 'nlevels', None)}")
    frame = idx.to_frame(index=False)
    names = list(idx.names)
    if "datetime" in names and "instrument" in names:
        dt_col = names.index("datetime")
        inst_col = names.index("instrument")
    else:
        parsed0 = pd.to_datetime(frame.iloc[:, 0], errors="coerce")
        parsed1 = pd.to_datetime(frame.iloc[:, 1], errors="coerce")
        if parsed0.notna().sum() >= parsed1.notna().sum():
            dt_col, inst_col = 0, 1
        else:
            dt_col, inst_col = 1, 0
    normalized = pd.DataFrame(
        {
            "datetime": pd.to_datetime(frame.iloc[:, dt_col]).dt.normalize(),
            "instrument": frame.iloc[:, inst_col].astype(str).str.upper(),
        }
    )
    out.index = pd.MultiIndex.from_frame(normalized, names=["datetime", "instrument"])
    out = out.sort_index()
    return out


def read_static_factors(snapshot_dir: Path, instruments: list[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    path = snapshot_dir / "static_factors.parquet"
    if not path.exists():
        raise FileNotFoundError(f"static_factors.parquet not found: {path}")
    columns = sorted({col for group in STATIC_FEATURE_GROUPS.values() for col in group})
    parquet_columns = columns + ["datetime", "instrument"]
    filters = [
        ("instrument", "in", instruments),
        ("datetime", ">=", start),
        ("datetime", "<=", end),
    ]
    try:
        df = pd.read_parquet(path, columns=parquet_columns, filters=filters)
    except Exception as exc:
        print(f"[WARN] Parquet filter read failed, falling back to selected-column scan: {exc}")
        df = pd.read_parquet(path, columns=parquet_columns)
        df = df[
            df["instrument"].isin(instruments)
            & (pd.to_datetime(df["datetime"]) >= start)
            & (pd.to_datetime(df["datetime"]) <= end)
        ].copy()
    if df.empty:
        raise RuntimeError("static_factors selection returned no rows")
    if isinstance(df.index, pd.MultiIndex) and set(df.index.names) >= {"datetime", "instrument"}:
        df = df.reset_index()
    elif "datetime" not in df.columns or "instrument" not in df.columns:
        raise RuntimeError(
            "static_factors parquet did not expose datetime/instrument as columns or index; "
            f"columns={list(df.columns)[:10]}, index_names={df.index.names}"
        )
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.normalize()
    df["instrument"] = df["instrument"].astype(str).str.upper()
    return df.set_index(["datetime", "instrument"]).sort_index()


def rolling_by_instrument(series: pd.Series, window: int, func: str) -> pd.Series:
    grouped = series.groupby(level="instrument", group_keys=False)
    if func == "mean":
        return grouped.rolling(window).mean().droplevel(0)
    if func == "std":
        return grouped.rolling(window).std().droplevel(0)
    raise ValueError(func)


def build_factor_panel(daily: pd.DataFrame, static: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    panel = daily.join(static, how="left")
    close = pd.to_numeric(panel["close"], errors="coerce")
    factor = pd.to_numeric(panel["factor"], errors="coerce")
    raw_close = close / factor.replace(0, np.nan)
    ret_1d = close.groupby(level="instrument").pct_change()
    label_next_1d = close.groupby(level="instrument").shift(-1) / close - 1.0

    features = pd.DataFrame(index=panel.index)
    features["pv_ret_5d"] = close.groupby(level="instrument").pct_change(5)
    features["pv_ret_20d"] = close.groupby(level="instrument").pct_change(20)
    features["pv_volatility_10d"] = rolling_by_instrument(ret_1d, 10, "std")
    features["pv_amount_log"] = np.log1p(pd.to_numeric(panel["amount"], errors="coerce").clip(lower=0))
    features["pv_volume_ratio_5d"] = pd.to_numeric(panel["volume"], errors="coerce") / rolling_by_instrument(
        pd.to_numeric(panel["volume"], errors="coerce"), 5, "mean"
    )
    features["limit_dist_to_up"] = pd.to_numeric(panel["up_limit_price"], errors="coerce") / raw_close - 1.0
    features["limit_dist_to_down"] = raw_close / pd.to_numeric(panel["down_limit_price"], errors="coerce") - 1.0

    for group_cols in STATIC_FEATURE_GROUPS.values():
        for col in group_cols:
            features[col] = pd.to_numeric(panel[col], errors="coerce")

    features = features.replace([np.inf, -np.inf], np.nan)
    return features.sort_index(), label_next_1d.sort_index(), ret_1d.sort_index()


def cross_sectional_rank(features: pd.DataFrame) -> pd.DataFrame:
    ranked = features.groupby(level="datetime").rank(method="average", pct=True)
    return ranked - 0.5


def daily_spearman(feature: pd.Series, label: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> list[float]:
    df = pd.concat([feature.rename("feature"), label.rename("label")], axis=1).dropna()
    dates = df.index.get_level_values("datetime")
    df = df[(dates >= start) & (dates <= end)]
    values: list[float] = []
    for _, sub in df.groupby(level="datetime"):
        if len(sub) < 5 or sub["feature"].nunique(dropna=True) < 2 or sub["label"].nunique(dropna=True) < 2:
            continue
        corr = sub["feature"].corr(sub["label"], method="spearman")
        if corr is not None and math.isfinite(corr):
            values.append(float(corr))
    return values


def compute_ic_weights(ranked: pd.DataFrame, label: pd.Series, train_start: pd.Timestamp, train_end: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for col in ranked.columns:
        values = daily_spearman(ranked[col], label, train_start, train_end)
        rows.append(
            {
                "feature": col,
                "mean_ic": float(np.nanmean(values)) if values else np.nan,
                "ic_std": float(np.nanstd(values)) if values else np.nan,
                "ic_count": int(len(values)),
            }
        )
    ic = pd.DataFrame(rows).sort_values("mean_ic", key=lambda s: s.abs(), ascending=False)
    if ic["mean_ic"].abs().fillna(0).sum() <= 1e-12:
        raise RuntimeError("All training IC weights are zero/NaN; cannot build validation signal")
    return ic


def build_signal(
    ranked: pd.DataFrame,
    ic: pd.DataFrame,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.Series]:
    weights = ic.set_index("feature")["mean_ic"].reindex(ranked.columns).fillna(0.0)
    raw_score = ranked.fillna(0.0).mul(weights, axis=1).sum(axis=1)
    no_leak_score = raw_score.groupby(level="instrument").shift(1)
    dates = no_leak_score.index.get_level_values("datetime")
    signal = no_leak_score[(dates >= test_start) & (dates <= test_end)].dropna().to_frame("score")
    signal = signal.sort_index()
    if signal.empty:
        raise RuntimeError("No test signal generated")
    return signal, weights


def coverage_table(features: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    dates = features.index.get_level_values("datetime")
    part = features[(dates >= start) & (dates <= end)]
    rows = []
    for col in features.columns:
        non_null = int(part[col].notna().sum())
        total = int(len(part))
        if col.startswith("pv_") or col.startswith("limit_"):
            source = "qlib_daily_bin"
        else:
            source = next((name for name, cols in STATIC_FEATURE_GROUPS.items() if col in cols), "static_factors")
        rows.append(
            {
                "source": source,
                "feature": col,
                "rows": total,
                "non_null": non_null,
                "coverage": float(non_null / total) if total else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values(["source", "feature"]).reset_index(drop=True)


def summarize_backtest(portfolio_metric_dict: dict) -> tuple[dict, pd.DataFrame | None]:
    report = None
    if isinstance(portfolio_metric_dict, dict):
        for value in portfolio_metric_dict.values():
            if isinstance(value, tuple) and len(value) >= 1 and isinstance(value[0], pd.DataFrame):
                report = value[0]
                break
            if isinstance(value, pd.DataFrame):
                report = value
                break
    if report is None:
        return {"report_found": False}, None

    summary = {"report_found": True, "report_rows": int(len(report))}
    if "return" in report.columns:
        returns = pd.to_numeric(report["return"], errors="coerce").fillna(0.0)
        nav = (1.0 + returns).cumprod()
        summary.update(
            {
                "total_return": float(nav.iloc[-1] - 1.0) if len(nav) else np.nan,
                "annualized_return": float(nav.iloc[-1] ** (252 / len(nav)) - 1.0) if len(nav) else np.nan,
                "daily_return_mean": float(returns.mean()),
                "daily_return_std": float(returns.std()),
                "sharpe_annualized": float(np.sqrt(252) * returns.mean() / returns.std()) if returns.std() > 0 else np.nan,
                "max_drawdown": float((nav / nav.cummax() - 1.0).min()) if len(nav) else np.nan,
                "nav_last": float(nav.iloc[-1]) if len(nav) else np.nan,
            }
        )
    if "turnover" in report.columns:
        summary["turnover_mean"] = float(pd.to_numeric(report["turnover"], errors="coerce").mean())
    if "cost" in report.columns:
        summary["cost_sum"] = float(pd.to_numeric(report["cost"], errors="coerce").sum())
    return summary, report


def to_jsonable(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d")
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        if np.isnan(obj):
            return None
        return float(obj)
    if isinstance(obj, float) and math.isnan(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def df_to_markdown(df: pd.DataFrame, index: bool = False) -> str:
    table = df.reset_index() if index else df.copy()
    if table.empty:
        return "_empty_"
    cols = [str(col) for col in table.columns]
    rows = []
    for row in table.itertuples(index=False):
        values = []
        for value in row:
            if isinstance(value, float):
                values.append("" if math.isnan(value) else f"{value:.6g}")
            else:
                values.append(str(value))
        rows.append(values)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(v.replace("|", "\\|") for v in row) + " |" for row in rows]
    return "\n".join([header, sep] + body)


def write_markdown(report_path: Path, data: dict, coverage: pd.DataFrame, ic: pd.DataFrame, backtest_report: pd.DataFrame | None) -> None:
    lines = [
        "# Qlib Multi-Dataset Smoke Backtest",
        "",
        f"- ok: `{data['ok']}`",
        f"- provider_uri: `{data['provider_uri']}`",
        f"- snapshot_dir: `{data['snapshot_dir']}`",
        f"- instruments: {', '.join(data['instruments'])}",
        f"- train: `{data['ranges']['train_start']} ~ {data['ranges']['train_end']}`",
        f"- test: `{data['ranges']['test_start']} ~ {data['ranges']['test_end']}`",
        f"- qlib backtest: `{data['ranges']['qlib_backtest_start']} ~ {data['ranges']['qlib_backtest_end']}`",
        f"- signal rows/dates: `{data['signal']['rows']}` / `{data['signal']['dates']}`",
        "",
        "## Coverage",
        "",
        df_to_markdown(coverage, index=False),
        "",
        "## Training IC Weights",
        "",
        df_to_markdown(ic, index=False),
        "",
        "## Backtest Summary",
        "",
        "```json",
        json.dumps(data["backtest_summary"], ensure_ascii=False, indent=2, default=to_jsonable),
        "```",
    ]
    if data.get("notes"):
        lines.extend(["## Notes", ""])
        lines.extend([f"- {note}" for note in data["notes"]])
        lines.append("")
    if backtest_report is not None:
        tail = backtest_report.tail(10).copy()
        lines.extend(["", "## Backtest Report Tail", "", df_to_markdown(tail, index=True)])
    if data.get("failures"):
        lines.extend(["", "## Failures", ""])
        lines.extend([f"- {failure}" for failure in data["failures"]])
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    provider_uri = Path(args.provider_uri)
    snapshot_dir = Path(args.snapshot_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_start = ts(args.feature_start)
    train_start = ts(args.train_start)
    train_end = ts(args.train_end)
    test_start = ts(args.test_start)
    test_end = ts(args.test_end)
    backtest_end, backtest_end_note = choose_backtest_end(provider_uri, test_start, test_end)

    if feature_start > train_start:
        raise ValueError("--feature-start must be <= --train-start")
    if not (train_start <= train_end < test_start <= test_end):
        raise ValueError("Expected train_start <= train_end < test_start <= test_end")

    instruments = choose_instruments(provider_uri, train_start, test_end, args.num_stocks)
    print(f"[INFO] Selected {len(instruments)} instruments: {', '.join(instruments)}")

    import qlib
    from qlib.backtest import backtest as qlib_backtest
    from qlib.backtest.executor import SimulatorExecutor
    from qlib.config import C
    from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
    from qlib.data import D

    C["kernels"] = 1
    qlib.init(provider_uri=str(provider_uri), region="cn", dataset_cache=None, expression_cache=None)

    daily_raw = D.features(instruments, QLIB_FIELDS, start_time=str(feature_start.date()), end_time=str(test_end.date()), freq="day")
    if daily_raw.empty:
        raise RuntimeError("Qlib D.features returned empty daily data")
    daily = flatten_columns(daily_raw)
    static = read_static_factors(snapshot_dir, instruments, feature_start, test_end)
    features, label_next_1d, ret_1d = build_factor_panel(daily, static)
    ranked = cross_sectional_rank(features)
    coverage = coverage_table(features, train_start, test_end)

    low_coverage = coverage[coverage["coverage"] < args.min_feature_coverage]
    failures = []
    if not low_coverage.empty:
        failures.append(
            "Low feature coverage: "
            + ", ".join(f"{row.feature}={row.coverage:.2%}" for row in low_coverage.itertuples(index=False))
        )

    ic = compute_ic_weights(ranked, label_next_1d, train_start, train_end)
    signal, weights = build_signal(ranked, ic, test_start, test_end)
    signal_dates = int(signal.index.get_level_values("datetime").nunique())
    if signal_dates < args.min_signal_dates:
        failures.append(f"Only {signal_dates} signal dates, below required {args.min_signal_dates}")

    print(f"[INFO] Signal rows={len(signal)}, dates={signal_dates}, topk={args.topk}, drop={args.drop}")
    portfolio_metric_dict, indicator_dict = qlib_backtest(
        start_time=str(test_start.date()),
        end_time=str(backtest_end.date()),
        strategy=TopkDropoutStrategy(signal=signal, topk=args.topk, n_drop=args.drop),
        executor=SimulatorExecutor(time_per_step="day", generate_portfolio_metrics=True),
        benchmark=None,
        account=10_000_000,
        exchange_kwargs={
            "freq": "day",
            "limit_threshold": ("$limit_up", "$limit_down"),
            "deal_price": "close",
            "open_cost": 0.000095,
            "close_cost": 0.000595,
            "min_cost": 5,
            "trade_unit": 100,
            "subscribe_fields": [
                "$open",
                "$high",
                "$low",
                "$close",
                "$volume",
                "$factor",
                "$prev_close",
                "$up_limit_price",
                "$down_limit_price",
                "$limit_up",
                "$limit_down",
            ],
        },
    )
    backtest_summary, backtest_report = summarize_backtest(portfolio_metric_dict)
    if not backtest_summary.get("report_found"):
        failures.append("Qlib backtest completed but no portfolio report was returned")

    source_coverage = (
        coverage.groupby("source")["coverage"].agg(["min", "mean"]).reset_index().to_dict(orient="records")
    )
    report = {
        "ok": not failures,
        "failures": failures,
        "provider_uri": str(provider_uri),
        "snapshot_dir": str(snapshot_dir),
        "output_dir": str(output_dir),
        "instruments": instruments,
        "ranges": {
            "feature_start": str(feature_start.date()),
            "train_start": str(train_start.date()),
            "train_end": str(train_end.date()),
            "test_start": str(test_start.date()),
            "test_end": str(test_end.date()),
            "qlib_backtest_start": str(test_start.date()),
            "qlib_backtest_end": str(backtest_end.date()),
        },
        "notes": [backtest_end_note] if backtest_end_note else [],
        "daily_rows": int(len(daily)),
        "static_rows": int(len(static)),
        "feature_count": int(features.shape[1]),
        "feature_sources": source_coverage,
        "signal": {
            "rows": int(len(signal)),
            "dates": signal_dates,
            "instruments": int(signal.index.get_level_values("instrument").nunique()),
            "topk": int(args.topk),
            "drop": int(args.drop),
            "weights_abs_sum": float(weights.abs().sum()),
        },
        "backtest_summary": backtest_summary,
        "indicator_keys": sorted(list(indicator_dict.keys())) if isinstance(indicator_dict, dict) else [],
    }

    coverage_path = output_dir / "feature_coverage.csv"
    ic_path = output_dir / "train_ic_weights.csv"
    signal_path = output_dir / "test_signal.parquet"
    json_path = output_dir / "report.json"
    md_path = output_dir / "report.md"

    coverage.to_csv(coverage_path, index=False)
    ic.to_csv(ic_path, index=False)
    signal.to_parquet(signal_path)
    if backtest_report is not None:
        backtest_report.to_csv(output_dir / "portfolio_report.csv")
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=to_jsonable), encoding="utf-8")
    write_markdown(md_path, report, coverage, ic, backtest_report)

    print(f"[INFO] Wrote report: {json_path}")
    print(f"[INFO] Wrote markdown: {md_path}")
    print(json.dumps(report["backtest_summary"], ensure_ascii=False, indent=2, default=to_jsonable))
    if failures:
        print("[ERROR] Validation failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("[INFO] Qlib multi-dataset smoke backtest PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
