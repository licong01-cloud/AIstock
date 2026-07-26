"""Causal QE-only sector-risk overlay artifact computation.

The runtime artifact contains signal-date information shifted to the next
trading date.  Outcome prices and backtest results are intentionally absent.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd


FORMULA_VERSION = "qe_sector_risk_v1"
RUNTIME_SCHEMA_VERSION = "qe_sector_risk_overlay_runtime_v1"
MANIFEST_SCHEMA_VERSION = "qe_sector_risk_overlay_manifest_v1"
REQUIRED_DAILY_COLUMNS = frozenset({"close"})
REQUIRED_SECTOR_COLUMNS = frozenset(
    {"sw2_close", "sw2_amount", "sw2_mf_net_amt", "l2_code_id"}
)
COMPONENT_COLUMNS = (
    "rs_turn_risk",
    "breadth_deterioration",
    "flow_divergence_risk",
    "leadership_concentration",
    "vol_crowding_risk",
)
STATE_THRESHOLDS: Mapping[str, float] = {
    "caution": 0.60,
    "high": 0.80,
    "critical": 0.90,
}


class QESectorRiskOverlayError(RuntimeError):
    """Stable failure for invalid QE sector-risk inputs."""

    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class SectorRiskBuildResult:
    runtime: pd.DataFrame
    sector_daily: pd.DataFrame
    summary: Mapping[str, Any]


def _canonical_index(frame: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex):
        raise QESectorRiskOverlayError(
            f"{source} must use a MultiIndex",
            reason_code="qe_sector_risk_index_invalid",
            context={"source": source, "index_type": type(frame.index).__name__},
        )
    names = list(frame.index.names)
    if "datetime" not in names or "instrument" not in names:
        raise QESectorRiskOverlayError(
            f"{source} index must contain datetime and instrument",
            reason_code="qe_sector_risk_index_invalid",
            context={"source": source, "index_names": names},
        )
    result = frame.copy(deep=False)
    if result.index.has_duplicates:
        duplicates = int(result.index.duplicated(keep=False).sum())
        raise QESectorRiskOverlayError(
            f"{source} contains duplicate stock-date keys",
            reason_code="qe_sector_risk_duplicate_key",
            context={"source": source, "duplicate_rows": duplicates},
        )
    reset = result.reset_index()
    reset["datetime"] = pd.to_datetime(reset["datetime"], errors="coerce").dt.normalize()
    if reset["datetime"].isna().any():
        raise QESectorRiskOverlayError(
            f"{source} contains invalid datetime values",
            reason_code="qe_sector_risk_datetime_invalid",
            context={"source": source},
        )
    reset["instrument"] = reset["instrument"].astype(str)
    return reset.sort_values(["datetime", "instrument"], kind="mergesort").reset_index(drop=True)


def _require_columns(frame: pd.DataFrame, required: frozenset[str], *, source: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise QESectorRiskOverlayError(
            f"{source} is missing required columns: {missing}",
            reason_code="qe_sector_risk_schema_invalid",
            context={"source": source, "missing": missing},
        )


def _cross_section_rank(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.groupby("datetime", sort=False)[column].rank(method="average", pct=True)


def _state_from_score(score: pd.Series) -> pd.Series:
    if score.isna().any():
        raise QESectorRiskOverlayError(
            "sector-risk score contains incomplete component rows",
            reason_code="qe_sector_risk_components_incomplete",
            context={"incomplete_rows": int(score.isna().sum())},
        )
    conditions = [
        score.ge(STATE_THRESHOLDS["critical"]),
        score.ge(STATE_THRESHOLDS["high"]),
        score.ge(STATE_THRESHOLDS["caution"]),
    ]
    return pd.Series(
        np.select(conditions, ["CRITICAL", "HIGH", "CAUTION"], default="NORMAL"),
        index=score.index,
        dtype="string",
    )


def _validate_sector_repetition(sector: pd.DataFrame) -> None:
    value_columns = ["sw2_close", "sw2_amount", "sw2_mf_net_amt"]
    valid = sector.loc[sector["l2_code_id"].ge(0), ["datetime", "l2_code_id", *value_columns]]
    conflicts: dict[str, int] = {}
    grouped = valid.groupby(["datetime", "l2_code_id"], sort=False, observed=True)
    for column in value_columns:
        count = grouped[column].nunique(dropna=True)
        bad = int(count.gt(1).sum())
        if bad:
            conflicts[column] = bad
    if conflicts:
        raise QESectorRiskOverlayError(
            "sector_data contains conflicting repeated sector values",
            reason_code="qe_sector_risk_sector_repeat_conflict",
            context={"conflicts": conflicts},
        )


def build_sector_risk_runtime(
    daily_pv: pd.DataFrame,
    sector_data: pd.DataFrame,
    *,
    output_start: str,
    output_end: str,
    dataset_identity: str,
    minimum_mapped_rate: float = 0.80,
) -> SectorRiskBuildResult:
    """Build causal sector-risk rows and shift them one trading day forward."""
    if not str(dataset_identity).strip():
        raise QESectorRiskOverlayError(
            "dataset_identity is required",
            reason_code="qe_sector_risk_dataset_identity_missing",
        )
    daily = _canonical_index(daily_pv, source="daily_pv.h5")
    sector = _canonical_index(sector_data, source="sector_data.h5")
    _require_columns(daily, REQUIRED_DAILY_COLUMNS, source="daily_pv.h5")
    _require_columns(sector, REQUIRED_SECTOR_COLUMNS, source="sector_data.h5")

    sector["l2_code_id"] = pd.to_numeric(sector["l2_code_id"], errors="coerce")
    invalid_ids = sector["l2_code_id"].notna() & sector["l2_code_id"].lt(-1)
    non_integer = sector["l2_code_id"].notna() & sector["l2_code_id"].mod(1).ne(0)
    if invalid_ids.any() or non_integer.any():
        raise QESectorRiskOverlayError(
            "l2_code_id must contain integer ids >= -1",
            reason_code="qe_sector_risk_l2_code_invalid",
            context={"below_minus_one": int(invalid_ids.sum()), "non_integer": int(non_integer.sum())},
        )
    sector["l2_code_id"] = sector["l2_code_id"].fillna(-1).astype("int16")

    merged = daily.loc[:, ["datetime", "instrument", "close"]].merge(
        sector.loc[
            :,
            ["datetime", "instrument", "l2_code_id", "sw2_close", "sw2_amount", "sw2_mf_net_amt"],
        ],
        on=["datetime", "instrument"],
        how="left",
        validate="one_to_one",
    )
    merged["l2_code_id"] = merged["l2_code_id"].fillna(-1).astype("int16")
    output_mask = merged["datetime"].between(pd.Timestamp(output_start), pd.Timestamp(output_end))
    mapped_rate = float(merged.loc[output_mask, "l2_code_id"].ge(0).mean()) if output_mask.any() else 0.0
    if mapped_rate < float(minimum_mapped_rate):
        raise QESectorRiskOverlayError(
            "PIT sector mapping coverage is below the configured minimum",
            reason_code="qe_sector_risk_mapping_coverage_low",
            context={"mapped_rate": mapped_rate, "minimum_mapped_rate": minimum_mapped_rate},
        )

    _validate_sector_repetition(sector)
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    by_stock = merged.groupby("instrument", sort=False, observed=True)
    merged["stock_return_20"] = by_stock["close"].pct_change(20, fill_method=None)
    merged["stock_ma20"] = by_stock["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    merged["above_ma20"] = merged["close"].gt(merged["stock_ma20"]).astype("float64")

    valid_members = merged.loc[merged["l2_code_id"].ge(0)].copy()
    member_group = valid_members.groupby(["datetime", "l2_code_id"], observed=True, sort=False)
    member_stats = member_group.agg(
        breadth=("above_ma20", "mean"),
        return_median=("stock_return_20", "median"),
        return_q90=("stock_return_20", lambda s: s.quantile(0.90)),
        member_count=("instrument", "nunique"),
    ).reset_index()
    member_stats["leadership_spread"] = member_stats["return_q90"] - member_stats["return_median"]

    sector_unique = (
        sector.loc[sector["l2_code_id"].ge(0)]
        .sort_values(["datetime", "l2_code_id", "instrument"], kind="mergesort")
        .drop_duplicates(["datetime", "l2_code_id"], keep="first")
        .loc[:, ["datetime", "l2_code_id", "sw2_close", "sw2_amount", "sw2_mf_net_amt"]]
    )
    panel = sector_unique.merge(
        member_stats,
        on=["datetime", "l2_code_id"],
        how="inner",
        validate="one_to_one",
    ).sort_values(["l2_code_id", "datetime"], kind="mergesort")
    for column in ("sw2_close", "sw2_amount", "sw2_mf_net_amt"):
        panel[column] = pd.to_numeric(panel[column], errors="coerce")

    by_sector = panel.groupby("l2_code_id", sort=False, observed=True)
    panel["sector_return_20"] = by_sector["sw2_close"].pct_change(20, fill_method=None)
    panel["sector_return_1"] = by_sector["sw2_close"].pct_change(1, fill_method=None)
    panel["rs_rank_20"] = _cross_section_rank(panel, "sector_return_20")
    panel["rs_turn_raw"] = -(panel["rs_rank_20"] - by_sector["rs_rank_20"].shift(5))
    panel["breadth_deterioration_raw"] = -(panel["breadth"] - by_sector["breadth"].shift(5))

    denominator = panel["sw2_amount"].abs().clip(lower=1.0)
    panel["flow_ratio"] = panel["sw2_mf_net_amt"] / denominator
    panel["flow_5"] = by_sector["flow_ratio"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    panel["flow_divergence_raw"] = (
        (-panel["flow_5"]).clip(lower=0.0) * panel["sector_return_20"].clip(lower=0.0)
    )
    panel["leadership_concentration_raw"] = panel["leadership_spread"] * (
        1.0 + panel["breadth_deterioration_raw"].clip(lower=0.0)
    )
    panel["vol_10"] = by_sector["sector_return_1"].transform(lambda s: s.rolling(10, min_periods=10).std())
    panel["vol_60"] = by_sector["sector_return_1"].transform(lambda s: s.rolling(60, min_periods=40).std())
    panel["amount_5"] = by_sector["sw2_amount"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    panel["amount_20"] = by_sector["sw2_amount"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    panel["vol_crowding_raw"] = panel["vol_10"].div(panel["vol_60"].replace(0, np.nan)) + panel[
        "amount_5"
    ].div(panel["amount_20"].replace(0, np.nan))

    raw_to_component = {
        "rs_turn_raw": "rs_turn_risk",
        "breadth_deterioration_raw": "breadth_deterioration",
        "flow_divergence_raw": "flow_divergence_risk",
        "leadership_concentration_raw": "leadership_concentration",
        "vol_crowding_raw": "vol_crowding_risk",
    }
    for raw, component in raw_to_component.items():
        panel[component] = _cross_section_rank(panel, raw)
    panel["risk_score"] = panel.loc[:, COMPONENT_COLUMNS].mean(axis=1, skipna=False)

    trading_dates = pd.Index(sorted(merged["datetime"].dropna().unique()))
    next_date = {pd.Timestamp(trading_dates[i]): pd.Timestamp(trading_dates[i + 1]) for i in range(len(trading_dates) - 1)}
    panel["effective_trade_date"] = panel["datetime"].map(next_date)
    sector_runtime = panel.loc[
        panel["effective_trade_date"].between(pd.Timestamp(output_start), pd.Timestamp(output_end)),
        ["datetime", "effective_trade_date", "l2_code_id", "risk_score", *COMPONENT_COLUMNS],
    ].rename(columns={"datetime": "signal_date"})
    sector_runtime["risk_state"] = _state_from_score(sector_runtime["risk_score"])

    stock_map = merged.loc[
        merged["datetime"].between(pd.Timestamp(output_start) - pd.Timedelta(days=10), pd.Timestamp(output_end)),
        ["datetime", "instrument", "l2_code_id"],
    ].rename(columns={"datetime": "signal_date"})
    runtime = stock_map.merge(
        sector_runtime,
        on=["signal_date", "l2_code_id"],
        how="left",
        validate="many_to_one",
    )
    runtime["effective_trade_date"] = runtime["signal_date"].map(next_date)
    runtime = runtime.loc[
        runtime["effective_trade_date"].between(pd.Timestamp(output_start), pd.Timestamp(output_end))
    ].copy()
    unmapped = runtime["l2_code_id"].lt(0)
    runtime.loc[unmapped, "risk_state"] = "UNMAPPED"
    runtime = runtime.sort_values(["effective_trade_date", "instrument"], kind="mergesort").reset_index(drop=True)
    if runtime.duplicated(["effective_trade_date", "instrument"]).any():
        raise QESectorRiskOverlayError(
            "runtime artifact contains duplicate effective stock-date keys",
            reason_code="qe_sector_risk_duplicate_key",
        )

    complete = runtime["risk_state"].isin(["NORMAL", "CAUTION", "HIGH", "CRITICAL"])
    summary = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "formula_version": FORMULA_VERSION,
        "dataset_identity": str(dataset_identity),
        "output_start": str(pd.Timestamp(output_start).date()),
        "output_end": str(pd.Timestamp(output_end).date()),
        "effective_shift_trading_days": 1,
        "state_thresholds": dict(STATE_THRESHOLDS),
        "runtime_rows": int(len(runtime)),
        "sector_rows": int(len(sector_runtime)),
        "mapped_rate": mapped_rate,
        "complete_component_rate": float(complete.mean()) if len(runtime) else 0.0,
        "state_counts": {str(k): int(v) for k, v in runtime["risk_state"].value_counts(dropna=False).items()},
    }
    return SectorRiskBuildResult(runtime=runtime, sector_daily=sector_runtime, summary=summary)


def canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
