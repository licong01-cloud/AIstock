"""Research-only prediction materializer for financial-distress QE reruns.

The module rewrites an existing QE ``pred.pkl`` by applying the same
context-aware rank demotion used by offline event-signal overlay research.  It
is intentionally detached from QE runtime code: the generated pkl can be passed
to ``qrun_limit_minute.py --pred-backtest`` from a copied experiment workspace.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import pickle
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from backend.services.event_signal.financial_distress_qe_overlay_research import (
    CONTEXT_SCORE_DOWN_PROFILES,
    DEFAULT_SCORE_DOWN_RANKING_DATE_MODE,
    DEFAULT_SCORE_DOWN_TOP_K,
    CandidateScore,
    _date_key,
    _fixed_width_table,
    _rank_date_for_context,
    build_context_score_down_penalty_by_date,
    build_variable_score_down_ranking,
)

MATERIALIZER_VERSION = "financial_distress_pred_materializer_v1_20260509"


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, indent=indent, sort_keys=True, default=str)


def load_prediction_pickle(path: Path) -> pd.DataFrame:
    with path.open("rb") as fh:
        pred = pickle.load(fh)
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    if not isinstance(pred, pd.DataFrame):
        raise TypeError(f"prediction artifact must be DataFrame or Series, got {type(pred).__name__}")
    if not isinstance(pred.index, pd.MultiIndex) or pred.index.nlevels < 2:
        raise ValueError("prediction artifact index must be MultiIndex(datetime, instrument)")
    if len(pred.columns) < 1:
        raise ValueError("prediction artifact must have at least one score column")
    return pred.copy()


def save_prediction_pickle(pred: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        pickle.dump(pred, fh, protocol=pickle.HIGHEST_PROTOCOL)


def prediction_score_column(pred: pd.DataFrame) -> Any:
    return "score" if "score" in pred.columns else pred.columns[0]


def candidate_scores_from_prediction(pred: pd.DataFrame) -> dict[dt.date, list[CandidateScore]]:
    score_column = prediction_score_column(pred)
    frame = pred[[score_column]].reset_index()
    date_col = "datetime" if "datetime" in frame.columns else frame.columns[0]
    symbol_col = "instrument" if "instrument" in frame.columns else frame.columns[1]
    frame["trade_date"] = pd.to_datetime(frame[date_col]).dt.date
    frame["ts_code"] = frame[symbol_col].astype(str)
    frame["score_value"] = pd.to_numeric(frame[score_column], errors="coerce")
    frame = frame.dropna(subset=["score_value"])
    result: dict[dt.date, list[CandidateScore]] = {}
    for trade_date, group in frame.groupby("trade_date", sort=True):
        ordered = group.sort_values(["score_value", "ts_code"], ascending=[False, True], kind="mergesort")
        result[_date_key(trade_date)] = [
            CandidateScore(ts_code=str(row.ts_code), score=float(row.score_value))
            for row in ordered.itertuples(index=False)
        ]
    return result


def load_overlay_csv(path: Path) -> pd.DataFrame:
    overlay = pd.read_csv(path)
    required = {
        "trade_date",
        "ts_code",
        "can_buy",
        "force_exit",
        "earliest_effective_trade_date",
        "active_trading_days",
    }
    missing = required.difference(overlay.columns)
    if missing:
        raise ValueError(f"overlay csv missing columns required for materialization: {sorted(missing)}")
    overlay = overlay.copy()
    overlay["trade_date"] = pd.to_datetime(overlay["trade_date"]).dt.date
    overlay["earliest_effective_trade_date"] = pd.to_datetime(overlay["earliest_effective_trade_date"]).dt.date
    for column in ("can_buy", "force_exit"):
        if overlay[column].dtype == object:
            overlay[column] = overlay[column].astype(str).str.lower().isin(["true", "1", "yes"])
    return overlay


def trading_days_from_prediction(pred: pd.DataFrame) -> list[dt.date]:
    dates = pd.to_datetime(pred.index.get_level_values(0)).date
    return sorted({_date_key(value) for value in dates})


def build_rank_date_penalties(
    overlay: pd.DataFrame,
    *,
    candidate_scores: Mapping[dt.date, Sequence[CandidateScore]],
    trading_days: Sequence[dt.date],
    context_profile_key: str = "rank_decay_balanced",
    top_k: int = DEFAULT_SCORE_DOWN_TOP_K,
    ranking_date_mode: str = DEFAULT_SCORE_DOWN_RANKING_DATE_MODE,
) -> tuple[dict[dt.date, dict[str, float]], list[dict[str, Any]]]:
    """Map trade-date penalties to the prediction date consumed by Qlib.

    Qlib's signal strategies trade on date T using prediction rows from T-1.
    The offline overlay expresses active risk by trade date, so the materialized
    ``pred.pkl`` must apply each penalty to the matching rank date.
    """

    if ranking_date_mode not in {"current", "previous"}:
        raise ValueError("ranking_date_mode must be current or previous")
    if context_profile_key not in CONTEXT_SCORE_DOWN_PROFILES:
        raise ValueError(f"unknown context profile: {context_profile_key}")
    profile = CONTEXT_SCORE_DOWN_PROFILES[context_profile_key]
    trade_penalties = build_context_score_down_penalty_by_date(
        overlay,
        candidate_scores=candidate_scores,
        trading_days=trading_days,
        profile=profile,
        top_k=top_k,
        ranking_date_mode=ranking_date_mode,
    )
    overlay_by_date_symbol: dict[tuple[dt.date, str], dict[str, Any]] = {}
    for row in overlay.to_dict("records"):
        overlay_by_date_symbol[(row["trade_date"], str(row["ts_code"]))] = row

    rank_date_penalties: dict[dt.date, dict[str, float]] = {}
    trace_rows: list[dict[str, Any]] = []
    for trade_date in sorted(trade_penalties):
        rank_date = _rank_date_for_context(
            trade_date=trade_date,
            trading_days=trading_days,
            ranking_date_mode=ranking_date_mode,
        )
        for symbol, penalty in sorted(trade_penalties[trade_date].items()):
            rank_date_penalties.setdefault(rank_date, {})[symbol] = max(
                float(penalty),
                rank_date_penalties.get(rank_date, {}).get(symbol, 0.0),
            )
            source_row = overlay_by_date_symbol.get((trade_date, symbol), {})
            trace_rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "rank_date": rank_date.isoformat(),
                    "ts_code": symbol,
                    "rank_penalty_pct": float(penalty),
                    "context_profile": context_profile_key,
                    "ranking_date_mode": ranking_date_mode,
                    "source_signal_ids": source_row.get("source_signal_ids"),
                    "event_types": source_row.get("event_types"),
                    "active_signal_count": source_row.get("active_signal_count"),
                    "market_cap_buckets": source_row.get("market_cap_buckets"),
                    "industries": source_row.get("industries"),
                }
            )
    return rank_date_penalties, trace_rows


def materialize_score_down_prediction(
    pred: pd.DataFrame,
    *,
    rank_date_penalties: Mapping[dt.date, Mapping[str, float]],
    top_k: int = DEFAULT_SCORE_DOWN_TOP_K,
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    """Return a prediction frame whose score order follows rank demotion."""

    if top_k <= 0:
        raise ValueError("top_k must be positive")
    score_column = prediction_score_column(pred)
    adjusted = pred.copy()
    frame = pred[[score_column]].reset_index()
    date_col = "datetime" if "datetime" in frame.columns else frame.columns[0]
    symbol_col = "instrument" if "instrument" in frame.columns else frame.columns[1]
    frame["rank_date"] = pd.to_datetime(frame[date_col]).dt.date
    frame["ts_code"] = frame[symbol_col].astype(str)
    frame["score_value"] = pd.to_numeric(frame[score_column], errors="coerce")

    trace_rows: list[dict[str, Any]] = []
    rank_dates_touched = 0
    penalized_symbols = set()
    changed_symbols = set()
    topk_drop_count = 0
    tie_score_dates = 0

    for rank_date, group in frame.groupby("rank_date", sort=True):
        rank_date = _date_key(rank_date)
        penalties = {str(k): float(v) for k, v in dict(rank_date_penalties.get(rank_date, {})).items() if float(v) > 0}
        if not penalties:
            continue
        ordered = group.dropna(subset=["score_value"]).sort_values(
            ["score_value", "ts_code"],
            ascending=[False, True],
            kind="mergesort",
        )
        if ordered.empty:
            continue
        candidates = [
            CandidateScore(ts_code=str(row.ts_code), score=float(row.score_value))
            for row in ordered.itertuples(index=False)
        ]
        ranking = build_variable_score_down_ranking(
            candidates=candidates,
            symbol_rank_penalty_pct=penalties,
            top_k=top_k,
        )
        original_scores_by_rank = [candidate.score for candidate in candidates]
        original_rank_by_symbol = {candidate.ts_code: idx for idx, candidate in enumerate(candidates, start=1)}
        original_score_by_symbol = {candidate.ts_code: candidate.score for candidate in candidates}
        adjusted_score_by_symbol = {
            row.ts_code: original_scores_by_rank[row.adjusted_rank - 1]
            for row in ranking
            if row.adjusted_rank <= len(original_scores_by_rank)
        }
        if ordered["score_value"].duplicated().any():
            tie_score_dates += 1
        index_by_symbol = {str(row.ts_code): row.Index for row in ordered.itertuples()}
        rank_dates_touched += 1
        for row in ranking:
            symbol = row.ts_code
            adjusted_score = adjusted_score_by_symbol.get(symbol)
            if adjusted_score is None or symbol not in index_by_symbol:
                continue
            adjusted.iloc[int(index_by_symbol[symbol]), adjusted.columns.get_loc(score_column)] = adjusted_score
            original_rank = original_rank_by_symbol.get(symbol)
            original_score = original_score_by_symbol.get(symbol)
            rank_delta = row.adjusted_rank - row.original_rank
            is_penalized = symbol in penalties
            if is_penalized:
                penalized_symbols.add(symbol)
            if adjusted_score != original_score:
                changed_symbols.add(symbol)
            if is_penalized and original_rank is not None and original_rank <= top_k and row.adjusted_rank > top_k:
                topk_drop_count += 1
            if is_penalized or rank_delta != 0:
                trace_rows.append(
                    {
                        "rank_date": rank_date.isoformat(),
                        "ts_code": symbol,
                        "original_rank": row.original_rank,
                        "adjusted_rank": row.adjusted_rank,
                        "adjusted_sort_rank": row.adjusted_sort_rank,
                        "rank_delta": rank_delta,
                        "rank_penalty_pct": float(penalties.get(symbol, 0.0)),
                        "penalized": bool(is_penalized),
                        "original_score": original_score,
                        "materialized_score": adjusted_score,
                        "dropped_from_topk": bool(is_penalized and original_rank is not None and original_rank <= top_k and row.adjusted_rank > top_k),
                    }
                )

    metrics = {
        "materializer_version": MATERIALIZER_VERSION,
        "score_column": score_column,
        "top_k": top_k,
        "prediction_rows": int(len(pred)),
        "prediction_dates": int(len(set(pd.to_datetime(pred.index.get_level_values(0)).date))),
        "rank_dates_with_penalties": int(len(rank_date_penalties)),
        "rank_dates_touched": int(rank_dates_touched),
        "penalty_rows": int(sum(len(value) for value in rank_date_penalties.values())),
        "trace_rows": int(len(trace_rows)),
        "penalized_symbol_count": int(len(penalized_symbols)),
        "changed_symbol_count": int(len(changed_symbols)),
        "topk_drop_count": int(topk_drop_count),
        "tie_score_dates": int(tie_score_dates),
    }
    return adjusted, trace_rows, metrics


def write_trace_csv(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(list(rows)).to_csv(path, index=False, encoding="utf-8-sig")


def write_report_md(path: Path, payload: Mapping[str, Any]) -> None:
    metrics = dict(payload.get("metrics") or {})
    table_rows = [[key, value] for key, value in metrics.items()]
    lines = [
        "# Phase 17 Financial Distress Prediction Materialization Report",
        "",
        "Research-only artifact. It rewrites a copied pred.pkl for --pred-backtest and does not change QE runtime, Selection Center, Paper Trading, QMT, live trading, or DB data.",
        "",
        "## Metrics",
        "",
        *_fixed_width_table(["metric", "value"], table_rows),
        "",
        "## Inputs And Outputs",
        "",
        *_fixed_width_table(
            ["item", "path"],
            [[key, value] for key, value in dict(payload.get("paths") or {}).items()],
        ),
        "",
        "## Next Command Shape",
        "",
        "```bash",
        "python qrun_limit_minute.py conf.yaml --pred-backtest <materialized_pred.pkl>",
        "```",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def materialize_from_files(
    *,
    prediction_pkl: Path,
    overlay_csv: Path,
    output_pkl: Path,
    trace_csv: Path,
    meta_json: Path,
    report_md: Optional[Path] = None,
    context_profile_key: str = "rank_decay_balanced",
    top_k: int = DEFAULT_SCORE_DOWN_TOP_K,
    ranking_date_mode: str = DEFAULT_SCORE_DOWN_RANKING_DATE_MODE,
) -> dict[str, Any]:
    pred = load_prediction_pickle(prediction_pkl)
    overlay = load_overlay_csv(overlay_csv)
    candidate_scores = candidate_scores_from_prediction(pred)
    trading_days = trading_days_from_prediction(pred)
    rank_date_penalties, penalty_trace = build_rank_date_penalties(
        overlay,
        candidate_scores=candidate_scores,
        trading_days=trading_days,
        context_profile_key=context_profile_key,
        top_k=top_k,
        ranking_date_mode=ranking_date_mode,
    )
    adjusted, rank_trace, metrics = materialize_score_down_prediction(
        pred,
        rank_date_penalties=rank_date_penalties,
        top_k=top_k,
    )
    save_prediction_pickle(adjusted, output_pkl)
    write_trace_csv([*penalty_trace, *rank_trace], trace_csv)
    payload = {
        "version": MATERIALIZER_VERSION,
        "params": {
            "context_profile_key": context_profile_key,
            "top_k": top_k,
            "ranking_date_mode": ranking_date_mode,
        },
        "paths": {
            "prediction_pkl": str(prediction_pkl),
            "overlay_csv": str(overlay_csv),
            "output_pkl": str(output_pkl),
            "trace_csv": str(trace_csv),
            "meta_json": str(meta_json),
            "report_md": str(report_md) if report_md else None,
        },
        "metrics": metrics,
    }
    meta_json.parent.mkdir(parents=True, exist_ok=True)
    meta_json.write_text(_json_dumps(payload, indent=2), encoding="utf-8")
    if report_md is not None:
        write_report_md(report_md, payload)
    return payload


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize financial-distress score-down into a copied QE pred.pkl")
    parser.add_argument("--prediction-pkl", required=True)
    parser.add_argument("--overlay-csv", required=True)
    parser.add_argument("--output-pkl", required=True)
    parser.add_argument("--trace-csv", required=True)
    parser.add_argument("--meta-json", required=True)
    parser.add_argument("--report-md", default=None)
    parser.add_argument("--context-profile", default="rank_decay_balanced", choices=sorted(CONTEXT_SCORE_DOWN_PROFILES))
    parser.add_argument("--top-k", type=int, default=DEFAULT_SCORE_DOWN_TOP_K)
    parser.add_argument("--ranking-date-mode", choices=["current", "previous"], default=DEFAULT_SCORE_DOWN_RANKING_DATE_MODE)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    payload = materialize_from_files(
        prediction_pkl=Path(args.prediction_pkl),
        overlay_csv=Path(args.overlay_csv),
        output_pkl=Path(args.output_pkl),
        trace_csv=Path(args.trace_csv),
        meta_json=Path(args.meta_json),
        report_md=Path(args.report_md) if args.report_md else None,
        context_profile_key=args.context_profile,
        top_k=args.top_k,
        ranking_date_mode=args.ranking_date_mode,
    )
    print(_json_dumps(payload["metrics"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
