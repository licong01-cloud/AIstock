"""Offline QE overlay research for early financial-distress event signals.

This module converts the first batch of structured financial-distress
candidate rules into research-only daily buy-filter overlays, then evaluates
them on an existing QE loop artifact. It does not write database rows and does
not change QE, Selection Center, Paper, QMT, or live trading consumers.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd
from dotenv import load_dotenv

from backend.db.pg_pool import get_conn
from backend.services.event_signal.early_financial_distress_research import (
    DEFAULT_COMBO_WINDOW_DAYS,
    DEFAULT_TIME_MODE,
    FINANCIAL_RULE_VERSION,
    FinancialRiskSignal,
    build_precision_rows,
    enrich_precision_rows_with_industry,
    enrich_precision_rows_with_loss_history,
    enrich_precision_rows_with_market_cap,
    load_financial_signals,
    load_industries_for_keys,
    load_market_caps_for_keys,
    load_trading_days,
    required_market_cap_keys,
)
from backend.services.event_signal.qe_loop_overlay_validation import (
    CandidateScore,
    fetch_price_returns_from_db,
    find_loop_artifact_dir,
    find_portfolio_artifact_dir,
    load_positions,
    load_prediction_scores,
    load_price_returns_csv,
    load_report,
    run_cash_counterfactual,
    run_next_candidate_counterfactual,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EXPERIMENT_ID = "qe_20260507_132049_d4e7"
DEFAULT_LOOP_ID = "Loop1"
DEFAULT_ACTIVE_TRADING_DAYS = (60, 120, 242)
DEFAULT_SIMULATOR_MODES = ("cash", "next_candidate")
SIMULATOR_VERSION = "financial_distress_qe_overlay_research_v1_20260508"
GE50_LOSS_BUCKETS = {"loss_50pct_to_100pct_mv", "loss_ge_100pct_mv"}


@dataclass(frozen=True)
class FinancialDistressRule:
    rule_key: str
    title: str
    description: str
    policy_risk_level: str
    priority: int


@dataclass(frozen=True)
class OverlayRunSummary:
    report_id: str
    output_json: str
    output_md: str
    experiment_id: str
    loop_id: str
    date_from: dt.date
    date_to: dt.date
    rules: int
    validations: int


FIRST_BATCH_RULES: tuple[FinancialDistressRule, ...] = (
    FinancialDistressRule(
        rule_key="loss_to_market_cap_ge_50pct",
        title="loss / market cap >= 50%",
        description="Forecast or express loss is at least 50% of PIT market cap.",
        policy_risk_level="HIGH",
        priority=10,
    ),
    FinancialDistressRule(
        rule_key="forecast_loss_to_market_cap_ge_50pct",
        title="forecast loss / market cap >= 50%",
        description="Performance forecast loss is at least 50% of PIT market cap.",
        policy_risk_level="HIGH",
        priority=20,
    ),
    FinancialDistressRule(
        rule_key="loss_20_50pct_and_loss_reports_ge_4",
        title="loss / market cap 20%-50% and rolling losses >= 4",
        description="Relative loss is 20%-50% and the last 730 days contain at least four loss reports.",
        policy_risk_level="MEDIUM_HIGH",
        priority=30,
    ),
    FinancialDistressRule(
        rule_key="forecast_loss_and_loss_reports_ge_4",
        title="forecast loss and rolling losses >= 4",
        description="Performance forecast loss with at least four loss reports in the last 730 days.",
        policy_risk_level="MEDIUM_HIGH",
        priority=40,
    ),
    FinancialDistressRule(
        rule_key="loss_to_market_cap_20_50pct",
        title="loss / market cap 20%-50%",
        description="Forecast or express loss is 20%-50% of PIT market cap.",
        policy_risk_level="MEDIUM",
        priority=50,
    ),
)


def _json_dumps(value: Any, *, indent: Optional[int] = None) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=indent)


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return dt.date.fromisoformat(text)


def _date_key(value: Any) -> dt.date:
    return pd.Timestamp(value).date()


def _pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "NA"
    return f"{float(value) * 100:.2f}%"


def _money(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "NA"
    return f"{float(value):,.2f}"


def _metric_delta(baseline: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, float]:
    return {
        "final_account_delta": float(overlay["final_account"] - baseline["final_account"]),
        "total_return_delta": float(overlay["total_return"] - baseline["total_return"]),
        "cagr_delta": float(overlay["cagr"] - baseline["cagr"]),
        "max_drawdown_delta": float(overlay["max_drawdown"] - baseline["max_drawdown"]),
    }


def _max_drawdown(account: pd.Series) -> float:
    nav = account / float(account.iloc[0])
    drawdown = nav / nav.cummax() - 1.0
    return float(drawdown.min())


def _cagr(account: pd.Series) -> float:
    if len(account) <= 1:
        return 0.0
    years = (len(account) - 1) / 242.0
    if years <= 0:
        return 0.0
    return float((float(account.iloc[-1]) / float(account.iloc[0])) ** (1.0 / years) - 1.0)


def _metrics_from_account(report: pd.DataFrame, account: pd.Series) -> dict[str, Any]:
    returns = account.pct_change().fillna(0.0)
    return {
        "rows": int(len(account)),
        "start_date": _date_key(account.index[0]).isoformat(),
        "end_date": _date_key(account.index[-1]).isoformat(),
        "initial_account": float(account.iloc[0]),
        "final_account": float(account.iloc[-1]),
        "total_return": float(account.iloc[-1] / account.iloc[0] - 1.0),
        "cagr": _cagr(account),
        "max_drawdown": _max_drawdown(account),
        "avg_daily_return": float(returns.mean()),
        "daily_volatility": float(returns.std()) if len(returns) > 1 else 0.0,
        "avg_cash_ratio": (
            float((report["cash"] / report["account"]).mean())
            if {"cash", "account"}.issubset(report.columns)
            else None
        ),
    }


def _rule_applies(row: Mapping[str, Any], rule: FinancialDistressRule) -> bool:
    event_type = str(row.get("event_type") or "")
    loss_bucket = str(row.get("loss_to_market_cap_bucket") or "")
    loss_count_bucket = str(row.get("loss_report_count_730d_bucket") or "")
    if rule.rule_key == "loss_to_market_cap_ge_50pct":
        return loss_bucket in GE50_LOSS_BUCKETS
    if rule.rule_key == "forecast_loss_to_market_cap_ge_50pct":
        return event_type == "financial_forecast_loss" and loss_bucket in GE50_LOSS_BUCKETS
    if rule.rule_key == "loss_20_50pct_and_loss_reports_ge_4":
        return loss_bucket == "loss_20pct_to_50pct_mv" and loss_count_bucket == "loss_reports_ge_4"
    if rule.rule_key == "forecast_loss_and_loss_reports_ge_4":
        return event_type == "financial_forecast_loss" and loss_count_bucket == "loss_reports_ge_4"
    if rule.rule_key == "loss_to_market_cap_20_50pct":
        return loss_bucket == "loss_20pct_to_50pct_mv"
    raise ValueError(f"unsupported financial distress rule: {rule.rule_key}")


def _active_dates_for_signal(
    *,
    effective_trade_date: dt.date,
    trading_days: Sequence[dt.date],
    date_from: dt.date,
    date_to: dt.date,
    active_trading_days: int,
) -> list[dt.date]:
    if active_trading_days <= 0:
        raise ValueError("active_trading_days must be positive")
    start_index = bisect.bisect_left(trading_days, effective_trade_date)
    if start_index >= len(trading_days):
        return []
    end_index = min(len(trading_days), start_index + active_trading_days)
    active = trading_days[start_index:end_index]
    return [day for day in active if date_from <= day <= date_to]


def _signal_row_from_signal(signal: FinancialRiskSignal) -> dict[str, Any]:
    return {
        "signal_id": signal.signal_id,
        "ts_code": signal.ts_code,
        "source_type": signal.source_type,
        "event_type": signal.event_type,
        "risk_level": signal.risk_level,
        "action": signal.action,
        "signal_year": signal.effective_trade_date.year,
        "metric_bucket": signal.metric_bucket,
        "metric_detail": signal.metric_detail or {},
        "report_period": signal.report_period,
        "source_event_date": signal.source_event_date,
        "effective_trade_date": signal.effective_trade_date,
    }


def build_financial_signal_rows(
    *,
    signals: Sequence[FinancialRiskSignal],
    combo_window_days: int = DEFAULT_COMBO_WINDOW_DAYS,
    study_end: Optional[dt.date] = None,
) -> list[dict[str, Any]]:
    """Build candidate-ready signal rows without requiring ST labels."""

    rows = build_precision_rows(
        signals,
        cycles=[],
        study_start=None,
        study_end=study_end,
        combo_window_days=combo_window_days,
    )
    if rows:
        return rows
    return [_signal_row_from_signal(signal) for signal in signals if study_end is None or signal.effective_trade_date <= study_end]


def enrich_financial_signal_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    market_cap_keys = required_market_cap_keys(rows)
    with get_conn() as conn:
        market_caps = load_market_caps_for_keys(conn, market_cap_keys)
        industries = load_industries_for_keys(conn, market_cap_keys)
    enriched = enrich_precision_rows_with_market_cap(rows, market_caps)
    enriched = enrich_precision_rows_with_industry(enriched, industries)
    enriched = enrich_precision_rows_with_loss_history(enriched)
    return enriched


def load_enriched_financial_rows(
    *,
    date_from: dt.date,
    date_to: dt.date,
    active_trading_days: int,
    financial_rule_version: str = FINANCIAL_RULE_VERSION,
    time_mode: str = DEFAULT_TIME_MODE,
    combo_window_days: int = DEFAULT_COMBO_WINDOW_DAYS,
    limit: Optional[int] = None,
) -> tuple[list[dict[str, Any]], list[dt.date]]:
    signal_load_start = date_from - dt.timedelta(days=max(370, active_trading_days * 3))
    calendar_start = date_from - dt.timedelta(days=max(370, active_trading_days * 3 + 30))
    with get_conn() as conn:
        signals = load_financial_signals(
            conn,
            rule_version=financial_rule_version,
            time_mode=time_mode,
            start_date=signal_load_start,
            end_date=date_to,
            limit=limit,
        )
        trading_days = load_trading_days(conn, calendar_start, date_to)
    rows = build_financial_signal_rows(signals=signals, combo_window_days=combo_window_days, study_end=date_to)
    return enrich_financial_signal_rows(rows), trading_days


def build_overlay_frame(
    *,
    financial_rows: Sequence[Mapping[str, Any]],
    trading_days: Sequence[dt.date],
    date_from: dt.date,
    date_to: dt.date,
    rule: FinancialDistressRule,
    active_trading_days: int,
) -> pd.DataFrame:
    """Create a research-only buy-filter overlay for one candidate rule."""

    grouped: dict[tuple[dt.date, str], dict[str, Any]] = {}
    matched_rows = [row for row in financial_rows if _rule_applies(row, rule)]
    for row in matched_rows:
        effective_trade_date = row.get("effective_trade_date")
        if not isinstance(effective_trade_date, dt.date):
            continue
        active_dates = _active_dates_for_signal(
            effective_trade_date=effective_trade_date,
            trading_days=trading_days,
            date_from=date_from,
            date_to=date_to,
            active_trading_days=active_trading_days,
        )
        for trade_date in active_dates:
            key = (trade_date, str(row["ts_code"]))
            payload = grouped.setdefault(
                key,
                {
                    "trade_date": trade_date,
                    "ts_code": str(row["ts_code"]),
                    "can_buy": False,
                    "force_exit": False,
                    "policy_risk_level": rule.policy_risk_level,
                    "primary_action": "research_buy_filter_candidate",
                    "rule_key": rule.rule_key,
                    "rule_title": rule.title,
                    "active_trading_days": active_trading_days,
                    "source_signal_ids": [],
                    "event_types": set(),
                    "source_types": set(),
                    "market_cap_buckets": set(),
                    "industries": set(),
                    "max_loss_to_market_cap": None,
                    "loss_report_count_730d_max": 0,
                    "earliest_effective_trade_date": effective_trade_date,
                    "latest_effective_trade_date": effective_trade_date,
                },
            )
            payload["source_signal_ids"].append(int(row["signal_id"]))
            payload["event_types"].add(str(row.get("event_type") or "unknown"))
            payload["source_types"].add(str(row.get("source_type") or "unknown"))
            payload["market_cap_buckets"].add(str(row.get("market_cap_bucket") or "mv_unknown"))
            payload["industries"].add(str(row.get("industry") or "industry_unknown"))
            loss_to_mv = row.get("loss_to_market_cap")
            if isinstance(loss_to_mv, (int, float)) and math.isfinite(float(loss_to_mv)):
                current = payload.get("max_loss_to_market_cap")
                payload["max_loss_to_market_cap"] = max(float(loss_to_mv), float(current or 0.0))
            payload["loss_report_count_730d_max"] = max(
                int(payload.get("loss_report_count_730d_max") or 0),
                int(row.get("loss_report_count_730d") or 0),
            )
            payload["earliest_effective_trade_date"] = min(payload["earliest_effective_trade_date"], effective_trade_date)
            payload["latest_effective_trade_date"] = max(payload["latest_effective_trade_date"], effective_trade_date)

    records: list[dict[str, Any]] = []
    for payload in grouped.values():
        records.append(
            {
                "trade_date": payload["trade_date"].isoformat(),
                "ts_code": payload["ts_code"],
                "can_buy": payload["can_buy"],
                "force_exit": payload["force_exit"],
                "policy_risk_level": payload["policy_risk_level"],
                "primary_action": payload["primary_action"],
                "rule_key": payload["rule_key"],
                "rule_title": payload["rule_title"],
                "active_trading_days": payload["active_trading_days"],
                "source_signal_ids": ",".join(str(item) for item in sorted(set(payload["source_signal_ids"]))),
                "active_signal_count": len(set(payload["source_signal_ids"])),
                "event_types": "+".join(sorted(payload["event_types"])),
                "source_types": "+".join(sorted(payload["source_types"])),
                "market_cap_buckets": "+".join(sorted(payload["market_cap_buckets"])),
                "industries": "+".join(sorted(payload["industries"])),
                "max_loss_to_market_cap": payload["max_loss_to_market_cap"],
                "loss_report_count_730d_max": payload["loss_report_count_730d_max"],
                "earliest_effective_trade_date": payload["earliest_effective_trade_date"].isoformat(),
                "latest_effective_trade_date": payload["latest_effective_trade_date"].isoformat(),
            }
        )
    if not records:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "ts_code",
                "can_buy",
                "force_exit",
                "policy_risk_level",
                "primary_action",
                "rule_key",
                "rule_title",
                "active_trading_days",
            ]
        )
    return pd.DataFrame(records).sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _overlay_hit_distribution(overlay: pd.DataFrame) -> dict[str, Any]:
    if overlay.empty:
        return {
            "overlay_rows": 0,
            "overlay_symbols": 0,
            "by_event_types": {},
            "by_market_cap_buckets": {},
            "by_industries_top20": {},
        }
    return {
        "overlay_rows": int(len(overlay)),
        "overlay_symbols": int(overlay["ts_code"].nunique()),
        "by_event_types": overlay["event_types"].value_counts().head(20).to_dict(),
        "by_market_cap_buckets": overlay["market_cap_buckets"].value_counts().head(20).to_dict(),
        "by_industries_top20": overlay["industries"].value_counts().head(20).to_dict(),
    }


def _write_overlay_csv(overlay: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    overlay.to_csv(path, index=False)


def _load_or_fetch_price_returns(
    *,
    price_return_csv: Optional[Path],
    candidate_scores: Mapping[dt.date, Sequence[CandidateScore]],
    date_from: dt.date,
    date_to: dt.date,
) -> dict[dt.date, dict[str, float]]:
    if price_return_csv is not None:
        return load_price_returns_csv(price_return_csv)
    candidate_symbols = {score.ts_code for scores in candidate_scores.values() for score in scores}
    return fetch_price_returns_from_db(symbols=candidate_symbols, date_from=date_from, date_to=date_to)


def _run_one_mode(
    *,
    simulator_mode: str,
    positions: dict[dt.date, dict[str, dict[str, float]]],
    report: pd.DataFrame,
    overlay: pd.DataFrame,
    date_from: dt.date,
    date_to: dt.date,
    candidate_scores: Optional[dict[dt.date, list[CandidateScore]]] = None,
    price_returns: Optional[dict[dt.date, dict[str, float]]] = None,
) -> tuple[pd.Series, dict[str, Any]]:
    overlay_for_validator = overlay.copy()
    overlay_for_validator["trade_date"] = pd.to_datetime(overlay_for_validator["trade_date"]).dt.date
    if simulator_mode == "cash":
        return run_cash_counterfactual(
            positions=positions,
            report=report,
            overlay=overlay_for_validator,
            date_from=date_from,
            date_to=date_to,
        )
    if simulator_mode == "next_candidate":
        if candidate_scores is None or price_returns is None:
            raise ValueError("next_candidate mode requires candidate_scores and price_returns")
        return run_next_candidate_counterfactual(
            positions=positions,
            report=report,
            overlay=overlay_for_validator,
            candidate_scores=candidate_scores,
            price_returns=price_returns,
            date_from=date_from,
            date_to=date_to,
        )
    raise ValueError(f"unsupported simulator mode: {simulator_mode}")


def run_financial_distress_qe_overlay_research(
    *,
    experiment_id: str,
    loop_id: str,
    loop_path: Path,
    output_dir: Path,
    date_from: Optional[dt.date],
    date_to: Optional[dt.date],
    active_trading_days_values: Sequence[int] = DEFAULT_ACTIVE_TRADING_DAYS,
    simulator_modes: Sequence[str] = DEFAULT_SIMULATOR_MODES,
    price_return_csv: Optional[Path] = None,
    prediction_pkl: Optional[Path] = None,
    financial_rule_version: str = FINANCIAL_RULE_VERSION,
    time_mode: str = DEFAULT_TIME_MODE,
    limit: Optional[int] = None,
    write_overlay_csv: bool = True,
) -> OverlayRunSummary:
    artifact_dir = find_portfolio_artifact_dir(loop_path)
    report_path = artifact_dir / "report_normal_1day.pkl"
    positions_path = artifact_dir / "positions_normal_1day.pkl"
    report = load_report(report_path)
    positions = load_positions(positions_path)
    resolved_date_from = date_from or _date_key(report.index[0])
    resolved_date_to = date_to or _date_key(report.index[-1])
    report_window = report[(report.index.date >= resolved_date_from) & (report.index.date <= resolved_date_to)].copy()
    if report_window.empty:
        raise ValueError("QE report has no rows in requested date window")

    max_active_days = max(active_trading_days_values)
    financial_rows, trading_days = load_enriched_financial_rows(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        active_trading_days=max_active_days,
        financial_rule_version=financial_rule_version,
        time_mode=time_mode,
        limit=limit,
    )

    candidate_scores: Optional[dict[dt.date, list[CandidateScore]]] = None
    price_returns: Optional[dict[dt.date, dict[str, float]]] = None
    pred_path_for_snapshot: Optional[Path] = None
    if "next_candidate" in simulator_modes:
        root_artifact_dir = find_loop_artifact_dir(loop_path)
        pred_path_for_snapshot = prediction_pkl or (root_artifact_dir / "pred.pkl")
        candidate_scores = load_prediction_scores(pred_path_for_snapshot)
        price_returns = _load_or_fetch_price_returns(
            price_return_csv=price_return_csv,
            candidate_scores=candidate_scores,
            date_from=resolved_date_from,
            date_to=resolved_date_to,
        )

    baseline_metrics = _metrics_from_account(report_window, report_window["account"])
    validations: list[dict[str, Any]] = []
    overlays_summary: list[dict[str, Any]] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    for active_days in active_trading_days_values:
        for rule in FIRST_BATCH_RULES:
            overlay = build_overlay_frame(
                financial_rows=financial_rows,
                trading_days=trading_days,
                date_from=resolved_date_from,
                date_to=resolved_date_to,
                rule=rule,
                active_trading_days=active_days,
            )
            overlay_name = f"financial_distress_overlay_{experiment_id}_{loop_id}_{rule.rule_key}_{active_days}td"
            overlay_csv_path = output_dir / f"{overlay_name}.csv"
            if write_overlay_csv:
                _write_overlay_csv(overlay, overlay_csv_path)
            overlays_summary.append(
                {
                    "rule": asdict(rule),
                    "active_trading_days": active_days,
                    "overlay_csv": str(overlay_csv_path) if write_overlay_csv else None,
                    **_overlay_hit_distribution(overlay),
                }
            )
            for simulator_mode in simulator_modes:
                overlay_account, hit_stats = _run_one_mode(
                    simulator_mode=simulator_mode,
                    positions=positions,
                    report=report,
                    overlay=overlay,
                    date_from=resolved_date_from,
                    date_to=resolved_date_to,
                    candidate_scores=candidate_scores,
                    price_returns=price_returns,
                )
                overlay_metrics = _metrics_from_account(report_window, overlay_account)
                delta_metrics = _metric_delta(baseline_metrics, overlay_metrics)
                validations.append(
                    {
                        "rule_key": rule.rule_key,
                        "rule_title": rule.title,
                        "active_trading_days": active_days,
                        "simulator_mode": simulator_mode,
                        "overlay_metrics": overlay_metrics,
                        "delta_metrics": delta_metrics,
                        "hit_stats": hit_stats,
                        "interpretation": "research_only_not_a_live_trading_rule",
                    }
                )

    report_id = "financial_distress_qe_overlay_{}_{}_{}_{}".format(
        experiment_id,
        loop_id,
        resolved_date_from.isoformat(),
        dt.datetime.now().strftime("%Y%m%d_%H%M%S"),
    ).replace("-", "")
    json_path = output_dir / f"{report_id}.json"
    md_path = output_dir / f"{report_id}.md"
    payload = {
        "report_id": report_id,
        "simulator_version": SIMULATOR_VERSION,
        "experiment_id": experiment_id,
        "loop_id": loop_id,
        "loop_path": str(loop_path),
        "date_from": resolved_date_from,
        "date_to": resolved_date_to,
        "parameters": {
            "financial_rule_version": financial_rule_version,
            "time_mode": time_mode,
            "active_trading_days_values": list(active_trading_days_values),
            "simulator_modes": list(simulator_modes),
            "financial_rows_loaded": len(financial_rows),
            "trading_days_loaded": len(trading_days),
            "write_overlay_csv": write_overlay_csv,
        },
        "input_snapshot": {
            "artifact_dir": str(artifact_dir),
            "report_path": str(report_path),
            "positions_path": str(positions_path),
            "prediction_pkl": str(pred_path_for_snapshot) if pred_path_for_snapshot else None,
            "price_return_csv": str(price_return_csv) if price_return_csv else None,
            "candidate_score_dates": len(candidate_scores) if candidate_scores else 0,
            "price_return_dates": len(price_returns) if price_returns else 0,
        },
        "baseline_metrics": baseline_metrics,
        "overlays": overlays_summary,
        "validations": validations,
        "research_boundary": {
            "writes_db": False,
            "changes_qe_runtime": False,
            "changes_selection_center": False,
            "changes_paper_trading": False,
            "changes_qmt_or_live_trading": False,
            "financial_signals_hard_block_enabled": False,
            "financial_signals_force_exit_enabled": False,
        },
    }
    json_path.write_text(_json_dumps(payload, indent=2), encoding="utf-8")
    _write_report_md(md_path, payload)
    return OverlayRunSummary(
        report_id=report_id,
        output_json=str(json_path),
        output_md=str(md_path),
        experiment_id=experiment_id,
        loop_id=loop_id,
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        rules=len(FIRST_BATCH_RULES),
        validations=len(validations),
    )


def _fixed_width_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(str(headers[idx])), *(len(row[idx]) for row in text_rows)) if text_rows else len(str(headers[idx]))
        for idx in range(len(headers))
    ]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    lines = [border, "| " + " | ".join(str(headers[idx]).ljust(widths[idx]) for idx in range(len(headers))) + " |", border]
    for row in text_rows:
        lines.append("| " + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(headers))) + " |")
    lines.append(border)
    return lines


def _write_report_md(path: Path, payload: Mapping[str, Any]) -> None:
    baseline = payload["baseline_metrics"]
    validations = sorted(
        payload["validations"],
        key=lambda row: (
            row["active_trading_days"],
            row["rule_key"],
            row["simulator_mode"],
        ),
    )
    rows = []
    for row in validations:
        delta = row["delta_metrics"]
        hit_stats = row["hit_stats"]
        rows.append(
            [
                row["active_trading_days"],
                row["rule_key"],
                row["simulator_mode"],
                hit_stats.get("blocked_buy_events", 0),
                _pct(delta.get("total_return_delta")),
                _pct(delta.get("cagr_delta")),
                _pct(delta.get("max_drawdown_delta")),
                _money(delta.get("final_account_delta")),
            ]
        )

    lines = [
        f"# Financial Distress QE Overlay Research: {payload['experiment_id']} {payload['loop_id']}",
        "",
        "Research-only offline overlay. Financial distress signals are not promoted to hard buy bans or forced sells.",
        "",
        "## Baseline",
        "",
        "```text",
        f"Date range   : {payload['date_from']} -> {payload['date_to']}",
        f"Final account: {_money(baseline['final_account'])}",
        f"Total return : {_pct(baseline['total_return'])}",
        f"CAGR         : {_pct(baseline['cagr'])}",
        f"Max drawdown : {_pct(baseline['max_drawdown'])}",
        "```",
        "",
        "## Overlay Results",
        "",
        "```text",
        *_fixed_width_table(
            [
                "active_td",
                "rule_key",
                "mode",
                "blocked_buys",
                "return_delta",
                "cagr_delta",
                "mdd_delta",
                "final_delta",
            ],
            rows,
        ),
        "```",
        "",
        "## Boundary",
        "",
        "```text",
        "writes_db=false, changes_qe_runtime=false, changes_selection_center=false, changes_paper_trading=false, changes_qmt_or_live_trading=false",
        "financial_signals_hard_block_enabled=false, financial_signals_force_exit_enabled=false",
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only financial distress QE overlay research")
    parser.add_argument("--experiment-id", default=DEFAULT_EXPERIMENT_ID)
    parser.add_argument("--loop-id", default=DEFAULT_LOOP_ID)
    parser.add_argument("--loop-path", required=True)
    parser.add_argument("--output-dir", default="reports/event_signal/financial_distress_qe_overlay")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--active-trading-days", type=int, action="append", default=None)
    parser.add_argument("--simulator-mode", action="append", choices=["cash", "next_candidate"], default=None)
    parser.add_argument("--prediction-pkl", default=None)
    parser.add_argument("--price-return-csv", default=None)
    parser.add_argument("--financial-rule-version", default=FINANCIAL_RULE_VERSION)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default=DEFAULT_TIME_MODE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--no-overlay-csv", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv(ROOT / ".env", override=False)
    args = parse_args(argv)
    summary = run_financial_distress_qe_overlay_research(
        experiment_id=args.experiment_id,
        loop_id=args.loop_id,
        loop_path=Path(args.loop_path),
        output_dir=Path(args.output_dir),
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        active_trading_days_values=tuple(args.active_trading_days or DEFAULT_ACTIVE_TRADING_DAYS),
        simulator_modes=tuple(args.simulator_mode or DEFAULT_SIMULATOR_MODES),
        price_return_csv=Path(args.price_return_csv) if args.price_return_csv else None,
        prediction_pkl=Path(args.prediction_pkl) if args.prediction_pkl else None,
        financial_rule_version=args.financial_rule_version,
        time_mode=args.time_mode,
        limit=args.limit,
        write_overlay_csv=not args.no_overlay_csv,
    )
    print(_json_dumps(asdict(summary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
