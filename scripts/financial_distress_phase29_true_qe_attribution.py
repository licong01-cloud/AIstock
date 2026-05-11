"""Phase-29 research-only attribution for financial-distress true QE smokes.

The script explains why a cheap overlay can look stronger than a real
``qrun_limit_minute.py --pred-backtest`` result.  It reads copied QE recorder
artifacts, materializer traces, and report/position pickles, then compares
rank penalties, actual end-of-day holdings, and true daily return deltas.

Run under the WSL ``rdagent-gpu`` environment because Qlib's ``Position``
objects are required when unpickling ``positions_normal_1day.pkl``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import pickle
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import pandas as pd


REPORT_VERSION = "financial_distress_phase29_true_qe_attribution_v1_20260511"
DEFAULT_EXPERIMENT_ROOT_WSL = Path(
    "/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2/mlruns/301029085745548565"
)
DEFAULT_EXPERIMENT_ROOT_WIN = Path(
    "F:/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2/mlruns/301029085745548565"
)
DEFAULT_OUTPUT_DIR = Path("reports/event_signal/financial_distress_phase29_true_qe_attribution")
DEFAULT_DOC_PATH = Path("docs/analysis/event_signal_financial_distress_phase29_true_qe_attribution_result_20260511.md")
BASELINE_RECORDER_ID = "7b57828280ad40b988e6574c9a083da6"


@dataclass(frozen=True)
class CaseSpec:
    case_key: str
    title: str
    rule_key: str
    profile: str
    trace_csv: str
    adjusted_recorder_id: str


@dataclass(frozen=True)
class Phase29Summary:
    output_json: str
    output_md: str
    cases: int
    best_true_return_case: str
    best_hit_precision_case: str


def _default_experiment_root() -> Path:
    return DEFAULT_EXPERIMENT_ROOT_WSL if DEFAULT_EXPERIMENT_ROOT_WSL.exists() else DEFAULT_EXPERIMENT_ROOT_WIN


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=indent, default=str)


def _date_key(value: Any) -> dt.date:
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    return pd.Timestamp(value).date()


def _pct(value: Any, digits: int = 3) -> str:
    if value is None:
        return "NA"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    if not math.isfinite(number):
        return "NA"
    return f"{number * 100:.{digits}f}%"


def _num(value: Any, digits: int = 6) -> str:
    if value is None:
        return "NA"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    if not math.isfinite(number):
        return "NA"
    return f"{number:.{digits}f}"


def _fixed_width_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    values = [[str(item) for item in row] for row in rows]
    widths = [len(str(header)) for header in headers]
    for row in values:
        for idx, item in enumerate(row):
            widths[idx] = max(widths[idx], len(item))
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def fmt(row: Sequence[Any]) -> str:
        return "| " + " | ".join(str(item).ljust(widths[idx]) for idx, item in enumerate(row)) + " |"

    return [border, fmt(headers), border, *[fmt(row) for row in values], border]


def _recorder_portfolio_dir(experiment_root: Path, recorder_id: str) -> Path:
    return experiment_root / recorder_id / "artifacts" / "portfolio_analysis"


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as fh:
        return pickle.load(fh)


def _load_positions(experiment_root: Path, recorder_id: str) -> dict[dt.date, Any]:
    raw = _load_pickle(_recorder_portfolio_dir(experiment_root, recorder_id) / "positions_normal_1day.pkl")
    return {_date_key(key): value for key, value in raw.items()}


def _load_report(experiment_root: Path, recorder_id: str) -> pd.DataFrame:
    report = pd.read_pickle(_recorder_portfolio_dir(experiment_root, recorder_id) / "report_normal_1day.pkl")
    report = report.copy()
    report.index = pd.to_datetime(report.index).date
    return report


def _position_symbols(position: Any) -> set[str]:
    data = getattr(position, "position", {})
    return {str(key) for key in data if key not in {"cash", "now_account_value"}}


def _position_weight(position: Any, symbol: str) -> float:
    data = getattr(position, "position", {})
    if symbol not in data:
        return 0.0
    try:
        return float(data[symbol].get("weight") or 0.0)
    except (AttributeError, TypeError, ValueError):
        return 0.0


def _safe_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _load_trace(trace_csv: Path, *, top_k: int) -> dict[str, Any]:
    usecols = [
        "rank_date",
        "ts_code",
        "rank_penalty_pct",
        "original_rank",
        "adjusted_rank",
        "rank_delta",
        "penalized",
        "dropped_from_topk",
    ]
    trace = pd.read_csv(trace_csv, usecols=usecols, low_memory=False)
    penalty_rows = trace[trace["original_rank"].isna()].copy()
    rank_rows = trace[trace["original_rank"].notna()].copy()
    if not rank_rows.empty:
        rank_rows["rank_date"] = pd.to_datetime(rank_rows["rank_date"]).dt.date
        rank_rows["ts_code"] = rank_rows["ts_code"].astype(str)
        rank_rows["original_rank"] = pd.to_numeric(rank_rows["original_rank"], errors="coerce")
        rank_rows["adjusted_rank"] = pd.to_numeric(rank_rows["adjusted_rank"], errors="coerce")
        rank_rows["rank_delta"] = pd.to_numeric(rank_rows["rank_delta"], errors="coerce")
        rank_rows["rank_penalty_pct"] = pd.to_numeric(rank_rows["rank_penalty_pct"], errors="coerce").fillna(0.0)
        rank_rows["penalized"] = _safe_bool_series(rank_rows["penalized"])
        rank_rows["dropped_from_topk"] = _safe_bool_series(rank_rows["dropped_from_topk"])
    if not penalty_rows.empty:
        penalty_rows["rank_date"] = pd.to_datetime(penalty_rows["rank_date"]).dt.date
        penalty_rows["ts_code"] = penalty_rows["ts_code"].astype(str)

    penalized_rows = rank_rows[rank_rows["penalized"]].copy()
    penalized_topk_rows = penalized_rows[penalized_rows["original_rank"] <= top_k].copy()
    dropped_rows = rank_rows[rank_rows["dropped_from_topk"]].copy()
    return {
        "trace": trace,
        "penalty_rows": penalty_rows,
        "rank_rows": rank_rows,
        "penalized_rows": penalized_rows,
        "penalized_topk_rows": penalized_topk_rows,
        "dropped_rows": dropped_rows,
    }


def _date_maps(dates: Sequence[dt.date]) -> tuple[dict[dt.date, dt.date], dict[dt.date, dt.date]]:
    ordered = sorted(dates)
    next_map = {ordered[idx]: ordered[idx + 1] for idx in range(len(ordered) - 1)}
    prev_map = {ordered[idx]: ordered[idx - 1] for idx in range(1, len(ordered))}
    return next_map, prev_map


def _event_state_frame(
    events: pd.DataFrame,
    *,
    dates: Sequence[dt.date],
    baseline_positions: Mapping[dt.date, Any],
    adjusted_positions: Mapping[dt.date, Any],
    baseline_report: pd.DataFrame,
    adjusted_report: pd.DataFrame,
) -> pd.DataFrame:
    next_map, prev_map = _date_maps(dates)
    rows: list[dict[str, Any]] = []
    for event in events.itertuples(index=False):
        rank_date = _date_key(event.rank_date)
        trade_date = next_map.get(rank_date)
        if trade_date is None:
            continue
        symbol = str(event.ts_code)
        previous_date = prev_map.get(trade_date)
        baseline_position = baseline_positions.get(trade_date)
        adjusted_position = adjusted_positions.get(trade_date)
        baseline_previous = baseline_positions.get(previous_date) if previous_date is not None else None
        adjusted_previous = adjusted_positions.get(previous_date) if previous_date is not None else None
        baseline_symbols = _position_symbols(baseline_position) if baseline_position is not None else set()
        adjusted_symbols = _position_symbols(adjusted_position) if adjusted_position is not None else set()
        baseline_prev_symbols = _position_symbols(baseline_previous) if baseline_previous is not None else set()
        adjusted_prev_symbols = _position_symbols(adjusted_previous) if adjusted_previous is not None else set()
        rows.append(
            {
                "rank_date": rank_date,
                "trade_date": trade_date,
                "ts_code": symbol,
                "original_rank": int(event.original_rank),
                "adjusted_rank": int(event.adjusted_rank),
                "rank_delta": int(event.adjusted_rank - event.original_rank),
                "baseline_hold": symbol in baseline_symbols,
                "adjusted_hold": symbol in adjusted_symbols,
                "baseline_new_buy": symbol in baseline_symbols and symbol not in baseline_prev_symbols,
                "adjusted_new_buy": symbol in adjusted_symbols and symbol not in adjusted_prev_symbols,
                "removed_by_adjusted": symbol in baseline_symbols and symbol not in adjusted_symbols,
                "baseline_weight": _position_weight(baseline_position, symbol) if baseline_position is not None else 0.0,
                "adjusted_weight": _position_weight(adjusted_position, symbol) if adjusted_position is not None else 0.0,
                "true_return_delta": float(adjusted_report.loc[trade_date, "return"] - baseline_report.loc[trade_date, "return"]),
            }
        )
    return pd.DataFrame(rows)


def _position_diff_frame(
    *,
    dates: Sequence[dt.date],
    baseline_positions: Mapping[dt.date, Any],
    adjusted_positions: Mapping[dt.date, Any],
    baseline_report: pd.DataFrame,
    adjusted_report: pd.DataFrame,
    penalized_topk_rows: pd.DataFrame,
    dropped_rows: pd.DataFrame,
) -> pd.DataFrame:
    _, prev_map = _date_maps(dates)
    topk_by_rank_date = {
        rank_date: set(group["ts_code"].astype(str))
        for rank_date, group in penalized_topk_rows.groupby("rank_date", sort=False)
    }
    dropped_by_rank_date = {
        rank_date: set(group["ts_code"].astype(str))
        for rank_date, group in dropped_rows.groupby("rank_date", sort=False)
    }
    rows: list[dict[str, Any]] = []
    for trade_date in dates:
        baseline_symbols = _position_symbols(baseline_positions[trade_date])
        adjusted_symbols = _position_symbols(adjusted_positions[trade_date])
        removed = baseline_symbols - adjusted_symbols
        added = adjusted_symbols - baseline_symbols
        rank_date = prev_map.get(trade_date)
        risk_topk = topk_by_rank_date.get(rank_date, set()) if rank_date is not None else set()
        risk_dropped = dropped_by_rank_date.get(rank_date, set()) if rank_date is not None else set()
        rows.append(
            {
                "trade_date": trade_date,
                "removed": len(removed),
                "added": len(added),
                "changed_holdings": len(removed) + len(added),
                "removed_risk_topk": len(removed & risk_topk),
                "removed_dropped": len(removed & risk_dropped),
                "removed_weight": sum(_position_weight(baseline_positions[trade_date], symbol) for symbol in removed),
                "added_weight": sum(_position_weight(adjusted_positions[trade_date], symbol) for symbol in added),
                "true_return_delta": float(adjusted_report.loc[trade_date, "return"] - baseline_report.loc[trade_date, "return"]),
            }
        )
    return pd.DataFrame(rows)


def _state_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {
            "events": 0,
            "baseline_hold": 0,
            "adjusted_hold": 0,
            "baseline_new_buy": 0,
            "adjusted_new_buy": 0,
            "removed_by_adjusted": 0,
        }
    return {
        "events": int(len(frame)),
        "baseline_hold": int(frame["baseline_hold"].sum()),
        "adjusted_hold": int(frame["adjusted_hold"].sum()),
        "baseline_new_buy": int(frame["baseline_new_buy"].sum()),
        "adjusted_new_buy": int(frame["adjusted_new_buy"].sum()),
        "removed_by_adjusted": int(frame["removed_by_adjusted"].sum()),
    }


def _top_date_rows(frame: pd.DataFrame, *, ascending: bool, n: int = 5) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    columns = [
        "trade_date",
        "removed",
        "added",
        "removed_risk_topk",
        "removed_dropped",
        "true_return_delta",
    ]
    return frame.sort_values("true_return_delta", ascending=ascending).head(n)[columns].to_dict("records")


def analyze_case(
    case: CaseSpec,
    *,
    experiment_root: Path,
    baseline_recorder_id: str,
    top_k: int,
) -> dict[str, Any]:
    baseline_positions = _load_positions(experiment_root, baseline_recorder_id)
    adjusted_positions = _load_positions(experiment_root, case.adjusted_recorder_id)
    baseline_report = _load_report(experiment_root, baseline_recorder_id)
    adjusted_report = _load_report(experiment_root, case.adjusted_recorder_id)
    dates = sorted(set(baseline_positions).intersection(adjusted_positions).intersection(baseline_report.index))
    trace_parts = _load_trace(Path(case.trace_csv), top_k=top_k)
    topk_state = _event_state_frame(
        trace_parts["penalized_topk_rows"],
        dates=dates,
        baseline_positions=baseline_positions,
        adjusted_positions=adjusted_positions,
        baseline_report=baseline_report,
        adjusted_report=adjusted_report,
    )
    dropped_state = _event_state_frame(
        trace_parts["dropped_rows"],
        dates=dates,
        baseline_positions=baseline_positions,
        adjusted_positions=adjusted_positions,
        baseline_report=baseline_report,
        adjusted_report=adjusted_report,
    )
    position_diff = _position_diff_frame(
        dates=dates,
        baseline_positions=baseline_positions,
        adjusted_positions=adjusted_positions,
        baseline_report=baseline_report,
        adjusted_report=adjusted_report,
        penalized_topk_rows=trace_parts["penalized_topk_rows"],
        dropped_rows=trace_parts["dropped_rows"],
    )
    changed_days = position_diff[position_diff["changed_holdings"] > 0]
    total_return_delta = float((adjusted_report.loc[dates, "return"] - baseline_report.loc[dates, "return"]).sum())
    avg_return_delta = float((adjusted_report.loc[dates, "return"] - baseline_report.loc[dates, "return"]).mean())
    penalty_rows = trace_parts["penalty_rows"]
    penalized_rows = trace_parts["penalized_rows"]
    penalized_topk_rows = trace_parts["penalized_topk_rows"]
    dropped_rows = trace_parts["dropped_rows"]
    penalty_row_count = int(len(penalty_rows))
    topk_row_count = int(len(penalized_topk_rows))
    dropped_row_count = int(len(dropped_rows))
    summary = {
        "case": asdict(case),
        "trace_summary": {
            "penalty_rows": penalty_row_count,
            "penalty_symbols": int(penalty_rows["ts_code"].nunique()) if not penalty_rows.empty else 0,
            "rank_penalized_rows": int(len(penalized_rows)),
            "penalized_topk_rows": topk_row_count,
            "dropped_from_topk_rows": dropped_row_count,
            "dropped_symbols": int(dropped_rows["ts_code"].nunique()) if not dropped_rows.empty else 0,
            "topk_per_penalty_row": (topk_row_count / penalty_row_count) if penalty_row_count else None,
            "drop_per_penalty_row": (dropped_row_count / penalty_row_count) if penalty_row_count else None,
            "drop_per_penalized_topk_row": (dropped_row_count / topk_row_count) if topk_row_count else None,
            "avg_original_rank_topk": float(penalized_topk_rows["original_rank"].mean()) if topk_row_count else None,
            "avg_rank_delta_topk": float(penalized_topk_rows["rank_delta"].mean()) if topk_row_count else None,
        },
        "topk_event_state": _state_counts(topk_state),
        "dropped_event_state": _state_counts(dropped_state),
        "position_diff_summary": {
            "dates": int(len(dates)),
            "changed_days": int(len(changed_days)),
            "removed_symbol_days": int(position_diff["removed"].sum()),
            "added_symbol_days": int(position_diff["added"].sum()),
            "removed_risk_topk_symbol_days": int(position_diff["removed_risk_topk"].sum()),
            "removed_dropped_symbol_days": int(position_diff["removed_dropped"].sum()),
            "avg_removed_weight_changed_days": float(changed_days["removed_weight"].mean()) if not changed_days.empty else 0.0,
            "avg_added_weight_changed_days": float(changed_days["added_weight"].mean()) if not changed_days.empty else 0.0,
            "true_return_delta_sum": total_return_delta,
            "true_return_delta_avg": avg_return_delta,
            "changed_day_return_delta_avg": float(changed_days["true_return_delta"].mean()) if not changed_days.empty else None,
        },
        "top_positive_dates": _top_date_rows(position_diff, ascending=False),
        "top_negative_dates": _top_date_rows(position_diff, ascending=True),
    }
    return summary


def _case_specs() -> list[CaseSpec]:
    return [
        CaseSpec(
            case_key="phase28_q_ocf_fixed15_90td",
            title="q_ocf_to_sales < 0 >=10bn / fixed_15 / 90td",
            rule_key="indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn",
            profile="fixed_15_90td_previous_top50",
            trace_csv="/mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260511_q_ocf_qe20260507_loop2/materialized_fixed15/indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn_fixed15_trace.csv",
            adjusted_recorder_id="8afe567e2bec4dc88a1f3fe15768567b",
        ),
        CaseSpec(
            case_key="phase19_indicator_decline_ctx60",
            title="indicator_large_decline_mv_10_30bn / ctx-balanced / 60td",
            rule_key="indicator_large_decline_mv_10_30bn",
            profile="rank_decay_balanced_60td_previous_top50",
            trace_csv="/mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/event_signal_pred_backtest/trace.csv",
            adjusted_recorder_id="59eaf3f33f864ade97b79ce561a13f2a",
        ),
        CaseSpec(
            case_key="phase23_loss_mv_fixed20_242td",
            title="loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 / 242td",
            rule_key="loss_to_market_cap_ge_50pct_mv_lt_10bn",
            profile="fixed_20_242td_previous_top50",
            trace_csv="/mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/materialized_fixed20/loss_to_market_cap_ge_50pct_mv_lt_10bn_fixed20_trace.csv",
            adjusted_recorder_id="34ecffc282ac4b44869dcd1261a55301",
        ),
    ]


def _best_case_key(cases: Sequence[Mapping[str, Any]], metric_path: Sequence[str]) -> str:
    def metric(case: Mapping[str, Any]) -> float:
        value: Any = case
        for key in metric_path:
            if not isinstance(value, Mapping):
                return float("-inf")
            value = value.get(key)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("-inf")

    return str(max(cases, key=metric)["case"]["case_key"])


def write_report_md(path: Path, payload: Mapping[str, Any]) -> None:
    cases = list(payload["case_results"])
    density_rows = []
    state_rows = []
    diff_rows = []
    decision_rows = []
    for case in cases:
        trace = case["trace_summary"]
        topk_state = case["topk_event_state"]
        dropped_state = case["dropped_event_state"]
        diff = case["position_diff_summary"]
        density_rows.append(
            [
                case["case"]["case_key"],
                trace["penalty_rows"],
                trace["penalty_symbols"],
                trace["penalized_topk_rows"],
                trace["dropped_from_topk_rows"],
                trace["dropped_symbols"],
                _pct(trace["topk_per_penalty_row"]),
                _pct(trace["drop_per_penalty_row"]),
                _num(trace["avg_original_rank_topk"], 2),
            ]
        )
        state_rows.append(
            [
                case["case"]["case_key"],
                topk_state["events"],
                topk_state["baseline_hold"],
                topk_state["removed_by_adjusted"],
                dropped_state["events"],
                dropped_state["baseline_hold"],
                dropped_state["removed_by_adjusted"],
            ]
        )
        diff_rows.append(
            [
                case["case"]["case_key"],
                diff["changed_days"],
                diff["removed_symbol_days"],
                diff["added_symbol_days"],
                diff["removed_risk_topk_symbol_days"],
                diff["removed_dropped_symbol_days"],
                _pct(diff["true_return_delta_sum"]),
                _pct(diff["changed_day_return_delta_avg"]),
            ]
        )
        decision = "KEEP_BENCHMARK" if "phase19" in case["case"]["case_key"] else "KEEP_RESEARCH"
        if "q_ocf" in case["case"]["case_key"]:
            decision = "DIAGNOSE_BROAD_LOW_PRECISION"
        if "loss_mv" in case["case"]["case_key"]:
            decision = "CALIBRATION_ONLY"
        decision_rows.append(
            [
                case["case"]["case_key"],
                decision,
                case.get("interpretation", "research-only attribution"),
            ]
        )

    positive_rows = []
    negative_rows = []
    for case in cases:
        for row in case["top_positive_dates"][:3]:
            positive_rows.append(
                [
                    case["case"]["case_key"],
                    row["trade_date"],
                    row["removed"],
                    row["added"],
                    row["removed_risk_topk"],
                    row["removed_dropped"],
                    _pct(row["true_return_delta"]),
                ]
            )
        for row in case["top_negative_dates"][:3]:
            negative_rows.append(
                [
                    case["case"]["case_key"],
                    row["trade_date"],
                    row["removed"],
                    row["added"],
                    row["removed_risk_topk"],
                    row["removed_dropped"],
                    _pct(row["true_return_delta"]),
                ]
            )

    lines = [
        "# Phase 29 Financial Distress True QE Attribution - 2026-05-11",
        "",
        "Research-only attribution for the true-QE smoke gap. It reads copied QE artifacts and materializer traces, and it does not change QE runtime, Selection Center, Paper Trading, QMT, live trading, database schema, or production backend `8001`.",
        "",
        "## Scope",
        "",
        "```text",
        f"version          : {payload['version']}",
        f"baseline recorder: {payload['baseline_recorder_id']}",
        f"experiment root  : {payload['experiment_root']}",
        f"cases            : {len(cases)}",
        "```",
        "",
        "## Rank Hit Density",
        "",
        "The q_ocf candidate is broad: many active penalty rows, but only a small fraction reach original Top50 and even fewer drop out of Top50.",
        "",
        "```text",
        *_fixed_width_table(
            ["case", "penalty", "symbols", "top50", "drops", "drop_sym", "top50/pen", "drop/pen", "avg_rank"],
            density_rows,
        ),
        "```",
        "",
        "## Actual Holding Hit State",
        "",
        "Top50 rank events often do not become actual removed holdings. This is the direct bridge from rank simulation to true QE execution.",
        "",
        "```text",
        *_fixed_width_table(
            ["case", "top50_evt", "top50_base_hold", "top50_removed", "drop_evt", "drop_base_hold", "drop_removed"],
            state_rows,
        ),
        "```",
        "",
        "## End-Of-Day Position Difference",
        "",
        "End-of-day holding differences are only an approximation because the V25 minute execution path can create intraday PnL even when end-of-day holdings converge.",
        "",
        "```text",
        *_fixed_width_table(
            ["case", "changed_days", "removed", "added", "risk_removed", "drop_removed", "true_ret_sum", "changed_avg"],
            diff_rows,
        ),
        "```",
        "",
        "## Largest Positive True-Return Delta Dates",
        "",
        "```text",
        *_fixed_width_table(
            ["case", "date", "removed", "added", "risk_removed", "drop_removed", "ret_delta"],
            positive_rows,
        ),
        "```",
        "",
        "## Largest Negative True-Return Delta Dates",
        "",
        "```text",
        *_fixed_width_table(
            ["case", "date", "removed", "added", "risk_removed", "drop_removed", "ret_delta"],
            negative_rows,
        ),
        "```",
        "",
        "## Interpretation",
        "",
        "```text",
        *_fixed_width_table(["case", "decision", "interpretation"], decision_rows),
        "```",
        "",
        "## Conclusion",
        "",
        "- Phase28 q_ocf is directionally positive in true QE, but its broad signal coverage has low Top50/drop precision.",
        "- Phase19 remains the better one-loop true-smoke benchmark because a much larger share of penalties are concentrated on original Top50 candidates.",
        "- Phase23 shows that high Top50/drop density alone is not enough; the removed names and replacement timing must also improve realized PnL.",
        "- The next cheap research should prefer higher-conviction intersections or rank-aware filters instead of simply increasing q_ocf penalty strength.",
        "- Do not promote any financial signal to runtime yet; no buy ban, forced sell, Paper/Selection/QE hook, or DB policy write is justified by this attribution.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_phase29(
    *,
    experiment_root: Path,
    output_dir: Path,
    doc_path: Path,
    top_k: int,
    baseline_recorder_id: str,
) -> Phase29Summary:
    case_results = [
        analyze_case(case, experiment_root=experiment_root, baseline_recorder_id=baseline_recorder_id, top_k=top_k)
        for case in _case_specs()
    ]
    case_results[0][
        "interpretation"
    ] = "broad active overlay; low Top50/drop precision explains weak true-QE materiality"
    case_results[1][
        "interpretation"
    ] = "more focused Top50 penalties; remains the stronger true-smoke benchmark"
    case_results[2][
        "interpretation"
    ] = "clean benchmark but sparse and weaker true-return improvement"

    output_dir.mkdir(parents=True, exist_ok=True)
    output_json = output_dir / "financial_distress_phase29_true_qe_attribution.json"
    output_md = output_dir / "financial_distress_phase29_true_qe_attribution.md"
    payload = {
        "version": REPORT_VERSION,
        "experiment_root": str(experiment_root),
        "baseline_recorder_id": baseline_recorder_id,
        "top_k": top_k,
        "case_results": case_results,
        "research_boundary": {
            "writes_db": False,
            "changes_qe_runtime": False,
            "changes_selection_center": False,
            "changes_paper_trading": False,
            "changes_qmt_or_live_trading": False,
        },
    }
    output_json.write_text(_json_dumps(payload, indent=2), encoding="utf-8")
    write_report_md(output_md, payload)
    write_report_md(doc_path, payload)
    summary = Phase29Summary(
        output_json=str(output_json),
        output_md=str(output_md),
        cases=len(case_results),
        best_true_return_case=_best_case_key(case_results, ["position_diff_summary", "true_return_delta_sum"]),
        best_hit_precision_case=_best_case_key(case_results, ["trace_summary", "topk_per_penalty_row"]),
    )
    return summary


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase29 true-QE attribution for financial-distress smokes")
    parser.add_argument("--experiment-root", default=str(_default_experiment_root()))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--doc-path", default=str(DEFAULT_DOC_PATH))
    parser.add_argument("--top-k", type=int, default=50)
    parser.add_argument("--baseline-recorder-id", default=BASELINE_RECORDER_ID)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    summary = run_phase29(
        experiment_root=Path(args.experiment_root),
        output_dir=Path(args.output_dir),
        doc_path=Path(args.doc_path),
        top_k=args.top_k,
        baseline_recorder_id=args.baseline_recorder_id,
    )
    print(_json_dumps(asdict(summary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
