"""QE-only sector-risk overlay evaluation aligned with F-014 evidence frames."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


ACTION_TYPES = {
    "ENTRY_BLOCK",
    "DE_RISK_BLOCKED_BY_HOLD",
    "DE_RISK_SELL",
    "EXIT",
    "REENTRY_BUY",
}
WARNING_ACTION_TYPES = {"ENTRY_BLOCK", "DE_RISK_SELL", "EXIT"}
EXIT_ACTION_TYPES = {"DE_RISK_SELL", "EXIT"}
LEAD_WINDOWS = (1, 3, 5, 10)


@dataclass(frozen=True)
class SectorRiskOverlayEvaluationResult:
    metrics: tuple[dict[str, Any], ...]
    aligned_exit_events: pd.DataFrame
    summary: dict[str, Any]


def _distribution(values) -> dict[str, Any]:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return {"count": 0, "mean": None, "median": None, "p10": None, "p90": None}
    return {
        "count": int(len(series)),
        "mean": float(series.mean()),
        "median": float(series.median()),
        "p10": float(series.quantile(0.10)),
        "p90": float(series.quantile(0.90)),
    }


def _metric(key: str, value: dict[str, Any], *, quality="ok", missing=()):
    return {
        "metric_scope": "sector_risk_overlay",
        "metric_key": key,
        "slice": "all_oos",
        "horizon": None,
        "barrier": None,
        "k": None,
        "value_num": None,
        "value_json": value,
        "quality_flag": quality,
        "missing_fields": list(missing),
    }


def normalize_sector_risk_actions(actions) -> pd.DataFrame:
    frame = pd.DataFrame(actions).copy(deep=True)
    required = {
        "trade_date",
        "instrument",
        "action_type",
        "risk_state",
        "order_generated",
        "reason",
        "policy_hash",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"QE sector-risk action ledger missing columns: {missing}")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    if frame["trade_date"].isna().any():
        raise ValueError("QE sector-risk action ledger contains invalid trade_date")
    frame["instrument"] = frame["instrument"].astype(str)
    frame["action_type"] = frame["action_type"].astype(str)
    unknown = sorted(set(frame["action_type"]) - ACTION_TYPES)
    if unknown:
        raise ValueError(f"QE sector-risk action ledger contains unknown action types: {unknown}")
    identity = ["trade_date", "instrument", "action_type", "policy_hash"]
    if frame.duplicated(identity).any():
        raise ValueError("QE sector-risk action ledger contains duplicate action identities")
    return frame.sort_values(["trade_date", "instrument", "action_type"], kind="mergesort").reset_index(drop=True)


def _trading_calendar(report: pd.DataFrame | None, actions: pd.DataFrame, episodes: pd.DataFrame):
    dates = set(actions["trade_date"].dropna())
    if isinstance(report, pd.DataFrame) and not report.empty:
        if isinstance(report.index, pd.DatetimeIndex):
            dates.update(pd.to_datetime(report.index).normalize())
        else:
            date_column = next(
                (name for name in ("report_date", "trade_date", "datetime", "date") if name in report),
                None,
            )
            if date_column:
                dates.update(pd.to_datetime(report[date_column], errors="coerce").dropna().dt.normalize())
    for column in ("exit_signal_date", "actual_exit_date", "entry_date", "exit_date"):
        if column in episodes:
            dates.update(pd.to_datetime(episodes[column], errors="coerce").dropna().dt.normalize())
    calendar = pd.Index(sorted(dates))
    return calendar, {pd.Timestamp(value): idx for idx, value in enumerate(calendar)}


def _normalize_episodes(episodes) -> pd.DataFrame:
    frame = pd.DataFrame(episodes).copy(deep=True)
    if frame.empty:
        return frame
    if "instrument" not in frame:
        raise ValueError("F-014 holding episodes require instrument")
    frame["instrument"] = frame["instrument"].astype(str)
    for column in ("exit_signal_date", "actual_exit_date", "entry_date", "exit_date"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.normalize()
    return frame


def evaluate_sector_risk_overlay(
    actions,
    *,
    holding_episodes=None,
    portfolio_report: pd.DataFrame | None = None,
) -> SectorRiskOverlayEvaluationResult:
    """Evaluate overlay actions without making missing metric families block other results."""
    action_frame = normalize_sector_risk_actions(actions)
    episodes = _normalize_episodes(holding_episodes)
    calendar, ordinal = _trading_calendar(portfolio_report, action_frame, episodes)
    metrics: list[dict[str, Any]] = []
    metrics.append(
        _metric(
            "action_summary",
            {
                "action_count": int(len(action_frame)),
                "action_type_counts": {
                    str(key): int(value)
                    for key, value in action_frame["action_type"].value_counts().items()
                },
                "risk_state_counts": {
                    str(key): int(value)
                    for key, value in action_frame["risk_state"].value_counts(dropna=False).items()
                },
                "order_generated_rate": float(action_frame["order_generated"].astype(bool).mean())
                if len(action_frame)
                else None,
            },
        )
    )

    aligned = pd.DataFrame()
    if not episodes.empty and "exit_signal_date" in episodes:
        warnings = action_frame.loc[action_frame["action_type"].isin(WARNING_ACTION_TYPES)]
        aligned_rows = []
        for episode in episodes.itertuples(index=False):
            exit_date = getattr(episode, "exit_signal_date", None)
            if pd.isna(exit_date):
                continue
            candidates = warnings.loc[
                warnings["instrument"].eq(str(episode.instrument))
                & warnings["trade_date"].le(exit_date)
            ]
            exit_ordinal = ordinal.get(pd.Timestamp(exit_date))
            if exit_ordinal is not None and not candidates.empty:
                candidates = candidates.loc[
                    candidates["trade_date"].map(
                        lambda value: 0
                        <= exit_ordinal - ordinal.get(pd.Timestamp(value), exit_ordinal + 1)
                        <= max(LEAD_WINDOWS)
                    )
                ]
            warning_date = candidates["trade_date"].min() if not candidates.empty else pd.NaT
            lead_days = (
                ordinal.get(pd.Timestamp(exit_date), -1) - ordinal.get(pd.Timestamp(warning_date), -1)
                if pd.notna(warning_date)
                else np.nan
            )
            row = dict(episode._asdict())
            row["overlay_warning_date"] = warning_date
            row["overlay_warning_lead_days"] = lead_days
            aligned_rows.append(row)
        aligned = pd.DataFrame(aligned_rows)
        leads = pd.to_numeric(aligned.get("overlay_warning_lead_days"), errors="coerce")
        metrics.append(
            _metric(
                "warning_lead_summary",
                {
                    "episode_count": int(len(aligned)),
                    "episode_with_prior_warning_count": int(leads.notna().sum()),
                    "lead_days": _distribution(leads),
                    "lead_at_least_n_days": {
                        str(window): int(leads.ge(window).sum()) for window in LEAD_WINDOWS
                    },
                    "lead_at_least_n_rate": {
                        str(window): float(leads.ge(window).mean()) if len(leads) else None
                        for window in LEAD_WINDOWS
                    },
                },
                quality="ok" if len(aligned) else "missing_local_evidence",
            )
        )
    else:
        metrics.append(
            _metric(
                "warning_lead_summary",
                {},
                quality="missing_local_evidence",
                missing=("holding_episodes.exit_signal_date",),
            )
        )

    episode_fields = {
        "post_exit_signal_mae",
        "post_exit_mfe",
        "false_early_exit",
        "episode_capture_ratio",
        "extended_capture_ratio",
    }
    available_episode_fields = sorted(episode_fields & set(aligned.columns))
    missing_episode_fields = sorted(episode_fields - set(aligned.columns))
    metrics.append(
        _metric(
            "exit_effect_summary",
            {
                "matched_episode_count": int(len(aligned)),
                "avoided_drawdown": _distribution(
                    -pd.to_numeric(aligned.get("post_exit_signal_mae"), errors="coerce").clip(upper=0)
                    if "post_exit_signal_mae" in aligned
                    else []
                ),
                "post_exit_mfe": _distribution(aligned.get("post_exit_mfe", [])),
                "false_early_exit_count": int(
                    aligned.get("false_early_exit", pd.Series(dtype="bool")).fillna(False).astype(bool).sum()
                ),
                "false_early_exit_rate": (
                    float(aligned["false_early_exit"].dropna().astype(bool).mean())
                    if "false_early_exit" in aligned and aligned["false_early_exit"].notna().any()
                    else None
                ),
                "episode_capture_ratio": _distribution(aligned.get("episode_capture_ratio", [])),
                "extended_capture_ratio": _distribution(aligned.get("extended_capture_ratio", [])),
                "available_fields": available_episode_fields,
            },
            quality="ok" if not missing_episode_fields else "computed_with_local_limitations",
            missing=missing_episode_fields,
        )
    )

    exits = action_frame.loc[action_frame["action_type"].eq("EXIT")]
    reentries = action_frame.loc[action_frame["action_type"].eq("REENTRY_BUY")]
    reentry_delays = []
    for row in exits.itertuples():
        candidate = reentries.loc[
            reentries["instrument"].eq(row.instrument)
            & reentries["trade_date"].gt(row.trade_date)
        ]
        if candidate.empty:
            continue
        reentry_date = candidate["trade_date"].min()
        reentry_delays.append(ordinal[reentry_date] - ordinal[row.trade_date])
    metrics.append(
        _metric(
            "reentry_delay_summary",
            {
                "full_exit_count": int(len(exits)),
                "reentered_exit_count": int(len(reentry_delays)),
                "trading_day_delay": _distribution(reentry_delays),
            },
            quality="ok" if len(exits) else "not_applicable",
        )
    )

    report_metrics = {"cost": None, "turnover": None, "report_days": 0}
    report_missing = []
    if isinstance(portfolio_report, pd.DataFrame) and not portfolio_report.empty:
        report_metrics["report_days"] = int(len(portfolio_report))
        if "cost" in portfolio_report:
            report_metrics["cost"] = _distribution(portfolio_report["cost"])
        else:
            report_missing.append("portfolio_report.cost")
        turnover_column = next(
            (name for name in ("turnover", "total_turnover") if name in portfolio_report),
            None,
        )
        if turnover_column:
            report_metrics["turnover"] = _distribution(portfolio_report[turnover_column])
        else:
            report_missing.append("portfolio_report.turnover")
    else:
        report_missing.append("portfolio_report")
    metrics.append(
        _metric(
            "cost_turnover_summary",
            report_metrics,
            quality="ok" if not report_missing else "computed_with_local_limitations",
            missing=report_missing,
        )
    )

    summary = {
        "schema_version": "qe_sector_risk_overlay_evaluation_v1",
        "action_count": int(len(action_frame)),
        "metric_count": len(metrics),
        "calendar_day_count": int(len(calendar)),
        "local_limitation_metric_count": sum(
            metric["quality_flag"] not in {"ok", "not_applicable"} for metric in metrics
        ),
    }
    return SectorRiskOverlayEvaluationResult(
        metrics=tuple(metrics),
        aligned_exit_events=aligned,
        summary=summary,
    )
