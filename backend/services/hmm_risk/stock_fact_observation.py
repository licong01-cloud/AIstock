"""C-007-A stock-fact-first construction of direct L1 HMM observations."""

from __future__ import annotations

import itertools
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from .state_model_set import (
    ALL_CORE_FEATURES,
    BASE_FEATURES,
    D6ValidationCalendarSeries,
    L1TrainingSeries,
    StateModelSetError,
    canonical_sha256,
)


OBSERVATION_VERSION = "hmm_risk_l1_stock_fact_observation_v1"
FORMULA_VERSION = "hmm_risk_l1_sector_factor_formula_v1"
C010_FORMULA_VERSION = "hmm_risk_l1_sector_factor_formula_v2_c010"
C010_POLICY_VERSION_V1 = "hmm_risk_c010_feature_domain_policy_v1"
C010_POLICY_VERSION = "hmm_risk_c010_feature_domain_policy_v2"
C010_CROSS_SECTION_RECEIPT_VERSION = "hmm_risk_c010_feature_cross_section_receipt_set_v1"
C010_AGGREGATE_RECEIPT_VERSION = "hmm_risk_c010_feature_domain_aggregate_evidence_v1"
C010_ELIGIBILITY_RECEIPT_VERSION_V1 = "hmm_risk_c010_train_observation_eligibility_v1"
C010_ELIGIBILITY_RECEIPT_VERSION = "hmm_risk_c010_train_observation_eligibility_v2"
C010_EXPECTED_OPPORTUNITY_CONTRACT_V1 = "hmm_risk_c010_expected_opportunity_dates_v1"
C010_EXPECTED_OPPORTUNITY_CONTRACT = "hmm_risk_c010_expected_opportunity_dates_v2"
C010_PROVIDER_ABSENCE_PARTITION_VERSION = "hmm_risk_c010_provider_absence_domain_partition_v1"
C010_APPROVED_TRAIN_START = date(2022, 1, 1)
C010_APPROVED_TRAIN_END = date(2024, 6, 30)
C010_APPROVED_TRAIN_TRADING_DATE_COUNT = 601
C010_APPROVED_TRAIN_TRADING_DATE_SHA256 = "b48fb5e911295d1c16920178b6ea48285c5890455aeaa31ad03ef7e11841f715"
C010_CROSS_SECTION_FEATURES = (
    "volume_ratio",
    "sf_range_vs_market_10d",
    "sf_vol_vs_market_20d",
    "sf_excess_breadth_5d",
)
C010_CROSS_SECTION_OPERATORS = {
    "volume_ratio": "sector_volume/sum_valid_sector_volume",
    "sf_range_vs_market_10d": "rolling_mean_10_min5(sector_range/median_valid_sector_range)",
    "sf_vol_vs_market_20d": "sector_vol20/median_valid_sector_vol20",
    "sf_excess_breadth_5d": "sector_breadth5-mean_valid_sector_breadth5",
}
C010_MONEYFLOW_DENOMINATOR_BY_FEATURE = {
    "net_mf_ratio": "moneyflow_contributor_amount",
    "elg_net_mf_ratio": "moneyflow_contributor_amount",
    "sf_mf_net_ratio_std_5d_neg": "moneyflow_contributor_amount",
    "sf_small_net_ratio_5d": "moneyflow_contributor_amount",
}
C010_FORMULA_DIFF_BY_FEATURE = {
    "net_mf_ratio": {
        "v1": "sector_net_mf_amount/price_domain_l1_amount",
        "v2": "complete_moneyflow_net_mf_amount/moneyflow_contributor_amount",
    },
    "elg_net_mf_ratio": {
        "v1": "sector_elg_net_mf_amount/price_domain_l1_amount",
        "v2": "complete_moneyflow_elg_net_mf_amount/moneyflow_contributor_amount",
    },
    "sf_mf_net_ratio_std_5d_neg": {
        "v1": "rolling_std_5_min5(net_mf_ratio_v1)_neg",
        "v2": "rolling_std_5_min5(net_mf_ratio_v2)_neg_with_current_moneyflow_post_mask",
    },
    "sf_small_net_ratio_5d": {
        "v1": "rolling_mean_5_min3(small_net_ratio_v1)",
        "v2": "rolling_mean_5_min3(small_net_ratio_v2)_with_current_moneyflow_post_mask",
    },
    "volume_ratio": {
        "v1": "sector_volume/sum_all_sector_volume",
        "v2": C010_CROSS_SECTION_OPERATORS["volume_ratio"],
    },
    "sf_range_vs_market_10d": {
        "v1": "exact_complete_global_mask_then_rolling_mean_10_min5",
        "v2": C010_CROSS_SECTION_OPERATORS["sf_range_vs_market_10d"],
    },
    "sf_vol_vs_market_20d": {
        "v1": "exact_complete_l1_return_surrogate_mask",
        "v2": C010_CROSS_SECTION_OPERATORS["sf_vol_vs_market_20d"],
    },
    "sf_excess_breadth_5d": {
        "v1": "exact_complete_l1_return_surrogate_mask",
        "v2": C010_CROSS_SECTION_OPERATORS["sf_excess_breadth_5d"],
    },
}
SOURCE_VERSION = "hmm_risk_l1_stock_fact_source_v1"
MIN_COVERAGE = 0.90
MIN_TRAINING_ROWS = 120


REQUIRED_STOCK_FIELDS = (
    "open_yuan",
    "high_yuan",
    "low_yuan",
    "close_yuan",
    "prev_close_yuan",
    "prev_close_5_yuan",
    "prev_close_10_yuan",
    "volume_shares",
    "amount_cny",
    "total_mv_cny",
    "prev_circ_mv_cny",
    "buy_sm_amount_cny",
    "sell_sm_amount_cny",
    "buy_elg_amount_cny",
    "sell_elg_amount_cny",
    "net_mf_amount_cny",
    "up_limit_yuan",
)
MONEYFLOW_STOCK_FIELDS = (
    "buy_sm_amount_cny",
    "sell_sm_amount_cny",
    "buy_elg_amount_cny",
    "sell_elg_amount_cny",
    "net_mf_amount_cny",
)
PRICE_STOCK_FIELDS = tuple(field for field in REQUIRED_STOCK_FIELDS if field not in MONEYFLOW_STOCK_FIELDS)


class ObservationCoverageError(StateModelSetError):
    """One L1/date is invalid but may be omitted with durable evidence."""

    def __init__(
        self,
        message: str,
        *,
        trade_date: date,
        l1_code: str,
        count_coverage: float,
        weight_coverage: float,
        missing_evidence: Sequence[Mapping[str, Any]],
        reason_code: str = "hmm_risk_model_stock_fact_coverage_insufficient",
    ) -> None:
        super().__init__(message)
        self.trade_date = trade_date
        self.l1_code = l1_code
        self.count_coverage = count_coverage
        self.weight_coverage = weight_coverage
        self.missing_evidence = tuple(dict(item) for item in missing_evidence)
        self.reason_code = reason_code


def _finite_number(value: Any, field: str, *, positive: bool = False, non_negative: bool = False) -> float:
    if isinstance(value, bool):
        raise StateModelSetError(f"{field} must be numeric")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise StateModelSetError(f"{field} must be numeric") from exc
    if not math.isfinite(normalized):
        raise StateModelSetError(f"{field} must be finite")
    if positive and normalized <= 0:
        raise StateModelSetError(f"{field} must be positive")
    if non_negative and normalized < 0:
        raise StateModelSetError(f"{field} must be non-negative")
    return normalized


def build_classification_lookup(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, str]]:
    """Map both industry_code and index_code to one canonical index identity."""

    lookup: dict[tuple[str, str], dict[str, str]] = {}
    canonical_seen: set[tuple[str, str]] = set()
    for row in rows:
        level = str(row.get("level") or "").upper()
        index_code = str(row.get("index_code") or "").strip()
        industry_code = str(row.get("industry_code") or "").strip()
        name = str(row.get("industry_name") or "").strip()
        if level not in {"L1", "L2"} or not index_code or not industry_code or not name:
            raise StateModelSetError("classification row is incomplete")
        canonical = (level, index_code)
        if canonical in canonical_seen:
            raise StateModelSetError(f"classification canonical identity is duplicated: {canonical}")
        canonical_seen.add(canonical)
        value = {"level": level, "index_code": index_code, "industry_code": industry_code, "name": name}
        for alias in (index_code, industry_code):
            key = (level, alias)
            if key in lookup and lookup[key] != value:
                raise StateModelSetError(f"classification alias maps to multiple identities: {key}")
            lookup[key] = value
    l1 = {value["index_code"] for key, value in lookup.items() if key[0] == "L1"}
    l2 = {value["index_code"] for key, value in lookup.items() if key[0] == "L2"}
    if len(l1) != 31 or len(l2) < 131:
        raise StateModelSetError(
            f"classification catalog must contain canonical L1=31 and at least the expected L2=131; "
            f"actual={len(l1)}/{len(l2)}"
        )
    return lookup


def canonicalize_mapping_rows(
    rows: Iterable[Mapping[str, Any]],
    classification_lookup: Mapping[tuple[str, str], Mapping[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Normalize equivalent historical code representations without selecting a row."""

    grouped: dict[tuple[str, date], list[dict[str, Any]]] = {}
    source_manifest_rows: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("ts_code") or row.get("symbol") or "").strip()
        trade_date = row.get("trade_date")
        if not symbol or not isinstance(trade_date, date):
            raise StateModelSetError("mapping row symbol/trade_date is invalid")
        try:
            l1 = classification_lookup[("L1", str(row.get("l1_code") or "").strip())]
            l2 = classification_lookup[("L2", str(row.get("l2_code") or "").strip())]
        except KeyError as exc:
            raise StateModelSetError(f"mapping classification alias is unknown for {symbol}/{trade_date}") from exc
        source = {
            "trade_date": trade_date.isoformat(),
            "symbol": symbol,
            "source_l1_code": str(row.get("l1_code") or ""),
            "source_l2_code": str(row.get("l2_code") or ""),
            "in_date": str(row.get("in_date") or ""),
            "out_date": None if row.get("out_date") is None else str(row.get("out_date")),
            "canonical_l1_code": l1["index_code"],
            "canonical_l2_code": l2["index_code"],
        }
        source_manifest_rows.append(source)
        grouped.setdefault((symbol, trade_date), []).append(
            {
                "trade_date": trade_date,
                "symbol": symbol,
                "l1_code": l1["index_code"],
                "l1_name": l1["name"],
                "l2_code": l2["index_code"],
                "l2_name": l2["name"],
            }
        )
    canonical_rows: list[dict[str, Any]] = []
    for identity, values in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])):
        unique = {(value["l1_code"], value["l1_name"], value["l2_code"], value["l2_name"]) for value in values}
        if len(unique) != 1:
            raise StateModelSetError(f"symbol/date resolves to multiple canonical sector identities: {identity}")
        canonical_rows.append(values[0])
    source_manifest_rows.sort(
        key=lambda item: (
            item["trade_date"],
            item["symbol"],
            item["canonical_l1_code"],
            item["canonical_l2_code"],
            item["in_date"],
            item["out_date"] or "",
        )
    )
    manifest = {
        "schema_version": "hmm_risk_pit_mapping_manifest_v1",
        "source_row_count": len(source_manifest_rows),
        "canonical_row_count": len(canonical_rows),
        "source_rows_hash": canonical_sha256(source_manifest_rows),
        "canonical_rows_hash": canonical_sha256(
            [
                {
                    **row,
                    "trade_date": row["trade_date"].isoformat(),
                }
                for row in canonical_rows
            ]
        ),
    }
    return canonical_rows, manifest


@dataclass(frozen=True)
class L1DailyAggregate:
    trade_date: date
    l1_code: str
    l1_name: str
    l2_codes: tuple[str, ...]
    l1_return: float
    l1_volume: float
    l1_amount: float
    l1_total_mv: float
    l1_range_ratio: float
    l1_true_range_ratio: float
    net_mf_amount: float
    buy_sm_amount: float
    sell_sm_amount: float
    buy_elg_amount: float
    sell_elg_amount: float
    limit_up_ratio: float
    breadth_1d: float
    breadth_5d: float
    breadth_10d: float
    dispersion_1d: float
    dispersion_5d: float
    dispersion_10d: float
    median_stock_return_1d: float
    median_stock_return_5d: float
    median_stock_return_10d: float
    mean_stock_return_1d: float
    mean_stock_return_5d: float
    mean_stock_return_10d: float
    count_coverage: float
    weight_coverage: float
    missing_evidence: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class FeatureDomainDailyAggregate(L1DailyAggregate):
    """Feature-domain aggregate with independently audited price and moneyflow evidence."""

    net_mf_amount: float | None
    buy_sm_amount: float | None
    sell_sm_amount: float | None
    buy_elg_amount: float | None
    sell_elg_amount: float | None
    moneyflow_amount: float | None = None
    moneyflow_count_coverage: float | None = None
    moneyflow_weight_coverage: float | None = None
    moneyflow_domain_status: str = "same_as_price_domain"
    moneyflow_excluded_symbols: tuple[str, ...] = ()
    price_expected_symbols: tuple[str, ...] = ()
    price_complete_symbols: tuple[str, ...] = ()
    price_expected_weight: float = 0.0
    price_complete_weight: float = 0.0
    moneyflow_expected_symbols: tuple[str, ...] = ()
    moneyflow_complete_symbols: tuple[str, ...] = ()
    moneyflow_expected_weight: float = 0.0
    moneyflow_complete_weight: float = 0.0


def _row_complete(row: Mapping[str, Any]) -> tuple[bool, list[str]]:
    missing: list[str] = []
    for field in REQUIRED_STOCK_FIELDS:
        value = row.get(field)
        try:
            _finite_number(
                value,
                field,
                positive=field
                in {
                    "open_yuan",
                    "high_yuan",
                    "low_yuan",
                    "close_yuan",
                    "prev_close_yuan",
                    "prev_close_5_yuan",
                    "prev_close_10_yuan",
                    "total_mv_cny",
                    "prev_circ_mv_cny",
                    "up_limit_yuan",
                },
                non_negative=field in {"volume_shares", "amount_cny"},
            )
        except StateModelSetError:
            missing.append(field)
    return not missing, missing


def _missing_row_evidence(row: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    evidence: dict[str, Any] = {"symbol": str(row.get("symbol") or ""), "fields": list(fields)}
    moneyflow_fields = {
        "buy_sm_amount_cny",
        "sell_sm_amount_cny",
        "buy_elg_amount_cny",
        "sell_elg_amount_cny",
        "net_mf_amount_cny",
    }
    if moneyflow_fields.intersection(fields):
        evidence["moneyflow_fact_status"] = str(row.get("moneyflow_fact_status") or "required_fields_invalid")
        identity = row.get("moneyflow_source_identity")
        if isinstance(identity, Mapping):
            evidence["moneyflow_source_identity"] = dict(identity)
        provider_absence = row.get("moneyflow_provider_absence")
        if isinstance(provider_absence, Mapping):
            evidence["moneyflow_provider_absence"] = dict(provider_absence)
    if "prev_circ_mv_cny" in fields:
        evidence["circ_mv_source_date"] = row.get("circ_mv_source_date")
        evidence["circ_mv_staleness_trading_days"] = row.get("circ_mv_staleness_trading_days")
    return evidence


def _aggregate_feature_domain_day(
    rows: Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    l1_code: str,
    l1_name: str,
    min_coverage: float,
    moneyflow_contributor_eligibility: Mapping[str, bool],
) -> FeatureDomainDailyAggregate:
    expected = sorted(
        (row for row in rows if not bool(row.get("is_suspended"))),
        key=lambda row: str(row.get("symbol") or ""),
    )
    if not expected:
        raise StateModelSetError(f"{l1_code}/{trade_date} has no observed denominator")
    expected_weights: list[float] = []
    missing_weight_evidence: list[dict[str, Any]] = []
    for row in expected:
        try:
            expected_weights.append(_finite_number(row.get("prev_circ_mv_cny"), "prev_circ_mv_cny", positive=True))
        except StateModelSetError:
            missing_weight_evidence.append(_missing_row_evidence(row, ["prev_circ_mv_cny"]))
    if missing_weight_evidence:
        known_count_coverage = (len(expected) - len(missing_weight_evidence)) / len(expected)
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} previous float-market-value denominator is incomplete "
            f"known_count={known_count_coverage:.6f}",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=float(known_count_coverage),
            weight_coverage=0.0,
            missing_evidence=missing_weight_evidence,
            reason_code="hmm_risk_c010_price_domain_weight_denominator_invalid",
        )
    expected_weight = float(math.fsum(expected_weights))
    if not math.isfinite(expected_weight) or expected_weight <= 0:
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} price-domain expected weight denominator is invalid",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=0.0,
            weight_coverage=0.0,
            missing_evidence=(),
            reason_code="hmm_risk_c010_price_domain_weight_denominator_invalid",
        )
    price_complete: list[Mapping[str, Any]] = []
    price_missing: list[dict[str, Any]] = []
    for row in expected:
        missing = []
        for field in PRICE_STOCK_FIELDS:
            try:
                _finite_number(
                    row.get(field),
                    field,
                    positive=field
                    in {
                        "open_yuan",
                        "high_yuan",
                        "low_yuan",
                        "close_yuan",
                        "prev_close_yuan",
                        "prev_close_5_yuan",
                        "prev_close_10_yuan",
                        "total_mv_cny",
                        "prev_circ_mv_cny",
                        "up_limit_yuan",
                    },
                    non_negative=field in {"volume_shares", "amount_cny"},
                )
            except StateModelSetError:
                missing.append(field)
        if missing:
            price_missing.append(_missing_row_evidence(row, missing))
        else:
            price_complete.append(row)
    price_count_coverage = len(price_complete) / len(expected)
    price_complete_weight = math.fsum(float(row["prev_circ_mv_cny"]) for row in price_complete)
    price_weight_coverage = price_complete_weight / expected_weight
    if (10 * len(price_complete)) < (9 * len(expected)) or price_weight_coverage < min_coverage:
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} price-domain stock coverage is insufficient "
            f"count={price_count_coverage:.6f} weight={price_weight_coverage:.6f}",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=float(price_count_coverage),
            weight_coverage=float(price_weight_coverage),
            missing_evidence=price_missing,
            reason_code="hmm_risk_c010_price_domain_coverage_insufficient",
        )

    moneyflow_expected = [
        row for row in expected if moneyflow_contributor_eligibility.get(str(row.get("symbol") or "")) is True
    ]
    moneyflow_complete: list[Mapping[str, Any]] = []
    moneyflow_missing: list[dict[str, Any]] = []
    for row in moneyflow_expected:
        missing = []
        for field in (*MONEYFLOW_STOCK_FIELDS, "amount_cny", "prev_circ_mv_cny"):
            try:
                _finite_number(
                    row.get(field),
                    field,
                    positive=field == "prev_circ_mv_cny",
                    non_negative=field == "amount_cny",
                )
            except StateModelSetError:
                missing.append(field)
        if missing:
            moneyflow_missing.append(_missing_row_evidence(row, missing))
        else:
            moneyflow_complete.append(row)
    if not moneyflow_expected:
        moneyflow_expected_weight = 0.0
        moneyflow_complete_weight = 0.0
        moneyflow_count_coverage = 0.0
        moneyflow_weight_coverage = 0.0
        moneyflow_status = "structurally_unavailable"
    else:
        moneyflow_expected_weight = math.fsum(float(row["prev_circ_mv_cny"]) for row in moneyflow_expected)
        moneyflow_complete_weight = math.fsum(float(row["prev_circ_mv_cny"]) for row in moneyflow_complete)
        if not math.isfinite(moneyflow_expected_weight) or moneyflow_expected_weight <= 0:
            raise ObservationCoverageError(
                f"{l1_code}/{trade_date} moneyflow-domain expected weight denominator is invalid",
                trade_date=trade_date,
                l1_code=l1_code,
                count_coverage=0.0,
                weight_coverage=0.0,
                missing_evidence=moneyflow_missing,
                reason_code="hmm_risk_c010_moneyflow_domain_weight_denominator_invalid",
            )
        if (
            not math.isfinite(moneyflow_complete_weight)
            or moneyflow_complete_weight < 0
            or moneyflow_complete_weight > moneyflow_expected_weight
        ):
            raise StateModelSetError(
                f"hmm_risk_c010_contributor_receipt_mismatch: {l1_code}/{trade_date} "
                "moneyflow complete weight escapes expected weight"
            )
        moneyflow_count_coverage = len(moneyflow_complete) / len(moneyflow_expected)
        moneyflow_weight_coverage = moneyflow_complete_weight / moneyflow_expected_weight
        moneyflow_status = (
            "available"
            if (10 * len(moneyflow_complete)) >= (9 * len(moneyflow_expected))
            and moneyflow_weight_coverage >= min_coverage
            else "coverage_insufficient"
        )

    weights = np.asarray([float(row["prev_circ_mv_cny"]) for row in price_complete], dtype=np.float64)
    weights /= weights.sum()
    close = np.asarray([float(row["close_yuan"]) for row in price_complete])
    previous = np.asarray([float(row["prev_close_yuan"]) for row in price_complete])
    previous_5 = np.asarray([float(row["prev_close_5_yuan"]) for row in price_complete])
    previous_10 = np.asarray([float(row["prev_close_10_yuan"]) for row in price_complete])
    high = np.asarray([float(row["high_yuan"]) for row in price_complete])
    low = np.asarray([float(row["low_yuan"]) for row in price_complete])
    returns_1 = close / previous - 1.0
    returns_5 = close / previous_5 - 1.0
    returns_10 = close / previous_10 - 1.0
    range_ratio = (high - low) / close
    true_range = np.maximum.reduce((high - low, np.abs(high - previous), np.abs(low - previous))) / previous
    limit_up = np.asarray(
        [float(row["close_yuan"]) >= float(row["up_limit_yuan"]) - 1e-4 for row in price_complete],
        dtype=np.float64,
    )
    price_values = {
        field: np.asarray([float(row[field]) for row in price_complete], dtype=np.float64)
        for field in ("volume_shares", "amount_cny", "total_mv_cny")
    }
    result_values = np.concatenate(
        [returns_1, returns_5, returns_10, range_ratio, true_range, limit_up, *price_values.values()]
    )
    if not np.isfinite(result_values).all():
        raise StateModelSetError(f"{l1_code}/{trade_date} feature-domain aggregation produced non-finite price values")
    moneyflow_values: dict[str, float] | None = None
    moneyflow_amount: float | None = None
    if moneyflow_status == "available":
        moneyflow_values = {
            field: float(math.fsum(float(row[field]) for row in moneyflow_complete)) for field in MONEYFLOW_STOCK_FIELDS
        }
        moneyflow_amount = float(math.fsum(float(row["amount_cny"]) for row in moneyflow_complete))
        if not all(math.isfinite(value) for value in (*moneyflow_values.values(), moneyflow_amount)):
            raise StateModelSetError(f"{l1_code}/{trade_date} feature-domain moneyflow aggregate is non-finite")
        if moneyflow_amount <= 0:
            moneyflow_status = "denominator_invalid"
            moneyflow_values = None
            moneyflow_amount = None
    excluded_evidence = [
        {
            "symbol": str(row.get("symbol") or ""),
            "fields": list(MONEYFLOW_STOCK_FIELDS),
            "moneyflow_eligibility_status": (
                "train_frozen_excluded"
                if str(row.get("symbol") or "") in moneyflow_contributor_eligibility
                else "train_eligibility_unavailable"
            ),
            "reason_code": (
                None
                if str(row.get("symbol") or "") in moneyflow_contributor_eligibility
                else "hmm_risk_c010_train_eligibility_unavailable"
            ),
        }
        for row in expected
        if moneyflow_contributor_eligibility.get(str(row.get("symbol") or "")) is not True
    ]
    return FeatureDomainDailyAggregate(
        trade_date=trade_date,
        l1_code=l1_code,
        l1_name=l1_name,
        l2_codes=tuple(sorted({str(row["l2_code"]) for row in price_complete})),
        l1_return=float(np.dot(weights, returns_1)),
        l1_volume=float(price_values["volume_shares"].sum()),
        l1_amount=float(price_values["amount_cny"].sum()),
        l1_total_mv=float(price_values["total_mv_cny"].sum()),
        l1_range_ratio=float(np.dot(weights, range_ratio)),
        l1_true_range_ratio=float(np.dot(weights, true_range)),
        net_mf_amount=None if moneyflow_values is None else moneyflow_values["net_mf_amount_cny"],
        buy_sm_amount=None if moneyflow_values is None else moneyflow_values["buy_sm_amount_cny"],
        sell_sm_amount=None if moneyflow_values is None else moneyflow_values["sell_sm_amount_cny"],
        buy_elg_amount=None if moneyflow_values is None else moneyflow_values["buy_elg_amount_cny"],
        sell_elg_amount=None if moneyflow_values is None else moneyflow_values["sell_elg_amount_cny"],
        limit_up_ratio=float(limit_up.mean()),
        breadth_1d=float((returns_1 > 0).mean()),
        breadth_5d=float((returns_5 > 0).mean()),
        breadth_10d=float((returns_10 > 0).mean()),
        dispersion_1d=float(np.std(returns_1, ddof=1)) if len(returns_1) > 1 else 0.0,
        dispersion_5d=float(np.std(returns_5, ddof=1)) if len(returns_5) > 1 else 0.0,
        dispersion_10d=float(np.std(returns_10, ddof=1)) if len(returns_10) > 1 else 0.0,
        median_stock_return_1d=float(np.median(returns_1)),
        median_stock_return_5d=float(np.median(returns_5)),
        median_stock_return_10d=float(np.median(returns_10)),
        mean_stock_return_1d=float(np.mean(returns_1)),
        mean_stock_return_5d=float(np.mean(returns_5)),
        mean_stock_return_10d=float(np.mean(returns_10)),
        count_coverage=float(price_count_coverage),
        weight_coverage=float(price_weight_coverage),
        missing_evidence=tuple([*price_missing, *moneyflow_missing, *excluded_evidence]),
        moneyflow_amount=moneyflow_amount,
        moneyflow_count_coverage=float(moneyflow_count_coverage),
        moneyflow_weight_coverage=float(moneyflow_weight_coverage),
        moneyflow_domain_status=moneyflow_status,
        moneyflow_excluded_symbols=tuple(sorted({item["symbol"] for item in excluded_evidence})),
        price_expected_symbols=tuple(str(row["symbol"]) for row in expected),
        price_complete_symbols=tuple(str(row["symbol"]) for row in price_complete),
        price_expected_weight=expected_weight,
        price_complete_weight=float(price_complete_weight),
        moneyflow_expected_symbols=tuple(str(row["symbol"]) for row in moneyflow_expected),
        moneyflow_complete_symbols=tuple(str(row["symbol"]) for row in moneyflow_complete),
        moneyflow_expected_weight=float(moneyflow_expected_weight),
        moneyflow_complete_weight=float(moneyflow_complete_weight),
    )


def aggregate_l1_day(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_coverage: float = MIN_COVERAGE,
    moneyflow_excluded_symbols: frozenset[str] | None = None,
    moneyflow_contributor_eligibility: Mapping[str, bool] | None = None,
) -> L1DailyAggregate:
    """Aggregate one sorted L1/date group with explicit count and cap-weight coverage."""

    if not rows:
        raise StateModelSetError("cannot aggregate an empty L1/date group")
    first = rows[0]
    trade_date = first.get("trade_date")
    l1_code = str(first.get("l1_code") or "")
    l1_name = str(first.get("l1_name") or "")
    if not isinstance(trade_date, date) or not l1_code or not l1_name:
        raise StateModelSetError("L1/date identity is incomplete")
    if any(row.get("trade_date") != trade_date or row.get("l1_code") != l1_code for row in rows):
        raise StateModelSetError("aggregate_l1_day received mixed identities")
    symbols = [str(row.get("symbol") or "") for row in rows]
    if any(not symbol for symbol in symbols) or len(symbols) != len(set(symbols)):
        raise StateModelSetError(f"{l1_code}/{trade_date} contains empty or duplicate canonical symbols")
    if moneyflow_excluded_symbols is not None and moneyflow_contributor_eligibility is not None:
        raise StateModelSetError("moneyflow exclusion set and contributor eligibility map are mutually exclusive")
    if moneyflow_excluded_symbols is not None or moneyflow_contributor_eligibility is not None:
        if min_coverage != MIN_COVERAGE:
            raise StateModelSetError(
                "hmm_risk_c010_feature_identity_drift: C-010 feature-domain coverage must remain exactly 0.90"
            )
        eligibility = (
            dict(moneyflow_contributor_eligibility)
            if moneyflow_contributor_eligibility is not None
            else {symbol: symbol not in moneyflow_excluded_symbols for symbol in symbols}
        )
        if any(not symbol or not isinstance(value, bool) for symbol, value in eligibility.items()):
            raise StateModelSetError("moneyflow contributor eligibility map is invalid")
        return _aggregate_feature_domain_day(
            rows,
            trade_date=trade_date,
            l1_code=l1_code,
            l1_name=l1_name,
            min_coverage=min_coverage,
            moneyflow_contributor_eligibility=eligibility,
        )

    expected = [row for row in rows if not bool(row.get("is_suspended"))]
    if not expected:
        raise StateModelSetError(f"{l1_code}/{trade_date} has no observed denominator")
    expected_weights: list[float] = []
    missing_weight_evidence: list[dict[str, Any]] = []
    for row in expected:
        try:
            expected_weights.append(_finite_number(row.get("prev_circ_mv_cny"), "prev_circ_mv_cny", positive=True))
        except StateModelSetError:
            missing_weight_evidence.append(_missing_row_evidence(row, ["prev_circ_mv_cny"]))
    if missing_weight_evidence:
        known_count_coverage = (len(expected) - len(missing_weight_evidence)) / len(expected)
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} previous float-market-value denominator is incomplete "
            f"known_count={known_count_coverage:.6f}",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=float(known_count_coverage),
            weight_coverage=0.0,
            missing_evidence=missing_weight_evidence,
        )
    expected_weight = float(sum(expected_weights))
    complete: list[Mapping[str, Any]] = []
    missing_evidence: list[dict[str, Any]] = []
    for row in expected:
        ok, missing = _row_complete(row)
        if ok:
            complete.append(row)
        else:
            missing_evidence.append(_missing_row_evidence(row, missing))
    count_coverage = len(complete) / len(expected)
    complete_weight = sum(float(row["prev_circ_mv_cny"]) for row in complete)
    weight_coverage = complete_weight / expected_weight
    if count_coverage < min_coverage or weight_coverage < min_coverage:
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} stock coverage is insufficient "
            f"count={count_coverage:.6f} weight={weight_coverage:.6f}",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=float(count_coverage),
            weight_coverage=float(weight_coverage),
            missing_evidence=missing_evidence,
        )
    weights = np.asarray([float(row["prev_circ_mv_cny"]) for row in complete], dtype=np.float64)
    weights /= weights.sum()
    close = np.asarray([float(row["close_yuan"]) for row in complete])
    previous = np.asarray([float(row["prev_close_yuan"]) for row in complete])
    previous_5 = np.asarray([float(row["prev_close_5_yuan"]) for row in complete])
    previous_10 = np.asarray([float(row["prev_close_10_yuan"]) for row in complete])
    high = np.asarray([float(row["high_yuan"]) for row in complete])
    low = np.asarray([float(row["low_yuan"]) for row in complete])
    returns_1 = close / previous - 1.0
    returns_5 = close / previous_5 - 1.0
    returns_10 = close / previous_10 - 1.0
    range_ratio = (high - low) / close
    true_range = np.maximum.reduce((high - low, np.abs(high - previous), np.abs(low - previous))) / previous
    limit_up = np.asarray(
        [float(row["close_yuan"]) >= float(row["up_limit_yuan"]) - 1e-4 for row in complete],
        dtype=np.float64,
    )
    values = {
        field: np.asarray([float(row[field]) for row in complete], dtype=np.float64)
        for field in (
            "volume_shares",
            "amount_cny",
            "total_mv_cny",
            "net_mf_amount_cny",
            "buy_sm_amount_cny",
            "sell_sm_amount_cny",
            "buy_elg_amount_cny",
            "sell_elg_amount_cny",
        )
    }
    result_values = np.concatenate(
        [
            returns_1,
            returns_5,
            returns_10,
            range_ratio,
            true_range,
            limit_up,
            *values.values(),
        ]
    )
    if not np.isfinite(result_values).all():
        raise StateModelSetError(f"{l1_code}/{trade_date} aggregation produced non-finite values")
    return L1DailyAggregate(
        trade_date=trade_date,
        l1_code=l1_code,
        l1_name=l1_name,
        l2_codes=tuple(sorted({str(row["l2_code"]) for row in complete})),
        l1_return=float(np.dot(weights, returns_1)),
        l1_volume=float(values["volume_shares"].sum()),
        l1_amount=float(values["amount_cny"].sum()),
        l1_total_mv=float(values["total_mv_cny"].sum()),
        l1_range_ratio=float(np.dot(weights, range_ratio)),
        l1_true_range_ratio=float(np.dot(weights, true_range)),
        net_mf_amount=float(values["net_mf_amount_cny"].sum()),
        buy_sm_amount=float(values["buy_sm_amount_cny"].sum()),
        sell_sm_amount=float(values["sell_sm_amount_cny"].sum()),
        buy_elg_amount=float(values["buy_elg_amount_cny"].sum()),
        sell_elg_amount=float(values["sell_elg_amount_cny"].sum()),
        limit_up_ratio=float(limit_up.mean()),
        breadth_1d=float((returns_1 > 0).mean()),
        breadth_5d=float((returns_5 > 0).mean()),
        breadth_10d=float((returns_10 > 0).mean()),
        dispersion_1d=float(np.std(returns_1, ddof=1)) if len(returns_1) > 1 else 0.0,
        dispersion_5d=float(np.std(returns_5, ddof=1)) if len(returns_5) > 1 else 0.0,
        dispersion_10d=float(np.std(returns_10, ddof=1)) if len(returns_10) > 1 else 0.0,
        median_stock_return_1d=float(np.median(returns_1)),
        median_stock_return_5d=float(np.median(returns_5)),
        median_stock_return_10d=float(np.median(returns_10)),
        mean_stock_return_1d=float(np.mean(returns_1)),
        mean_stock_return_5d=float(np.mean(returns_5)),
        mean_stock_return_10d=float(np.mean(returns_10)),
        count_coverage=float(count_coverage),
        weight_coverage=float(weight_coverage),
        missing_evidence=tuple(missing_evidence),
    )


def aggregate_stock_fact_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    min_coverage: float = MIN_COVERAGE,
) -> tuple[list[L1DailyAggregate], dict[str, Any]]:
    """Consume rows sorted by date/L1/symbol and return exact daily aggregates."""

    if not 0 < min_coverage <= 1:
        raise StateModelSetError("min_coverage must be in (0,1]")
    output: list[L1DailyAggregate] = []
    missing: list[dict[str, Any]] = []
    for (trade_date, l1_code), group in itertools.groupby(
        rows,
        key=lambda row: (row.get("trade_date"), str(row.get("l1_code") or "")),
    ):
        aggregate = aggregate_l1_day(list(group), min_coverage=min_coverage)
        output.append(aggregate)
        if aggregate.missing_evidence:
            missing.append(
                {
                    "trade_date": str(trade_date),
                    "l1_code": l1_code,
                    "count_coverage": aggregate.count_coverage,
                    "weight_coverage": aggregate.weight_coverage,
                    "missing": list(aggregate.missing_evidence),
                }
            )
    if not output:
        raise StateModelSetError("stock-fact source produced no L1 aggregates")
    manifest = {
        "schema_version": SOURCE_VERSION,
        "formula_version": FORMULA_VERSION,
        "min_count_coverage": min_coverage,
        "min_weight_coverage": min_coverage,
        "aggregate_row_count": len(output),
        "missing_evidence": missing,
        "aggregate_hash": canonical_sha256(
            [
                {
                    **item.__dict__,
                    "trade_date": item.trade_date.isoformat(),
                }
                for item in output
            ]
        ),
    }
    return output, manifest


def project_stock_fact_rows_for_direct_level(
    rows: Iterable[Mapping[str, Any]],
    *,
    sector_level: str,
) -> list[dict[str, Any]]:
    """Project stock facts to a direct L1/L2 grouping identity without cross-level model aggregation."""

    if sector_level not in {"L1", "L2"}:
        raise StateModelSetError("direct sector level must be L1 or L2")
    projected: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        if sector_level == "L2":
            code = str(row.get("l2_code") or "").strip()
            name = str(row.get("l2_name") or "").strip()
            if not code or not name:
                raise StateModelSetError("direct L2 stock fact identity is incomplete")
            row["l1_code"] = code
            row["l1_name"] = name
        projected.append(row)
    projected.sort(
        key=lambda row: (
            row.get("trade_date"),
            str(row.get("l1_code") or ""),
            str(row.get("symbol") or ""),
        )
    )
    return projected


def _rolling_rank(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    return (
        series.groupby(level="l1_code", group_keys=False)
        .rolling(
            window,
            min_periods=min_periods,
        )
        .rank(pct=True)
        .droplevel(0)
    )


def build_l1_feature_panel(
    aggregates: Sequence[L1DailyAggregate],
    *,
    trading_dates: Sequence[date],
    csi300_returns: Mapping[date, float],
    expected_sector_count: int = 31,
    direct_sector_level: str = "L1",
    cross_section_min_coverage: float | None = None,
    use_moneyflow_amount_denominator: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Recompute the approved 7/20 features from L1 raw aggregates."""

    if not aggregates:
        raise StateModelSetError("cannot build features without L1 aggregates")
    rows = [item.__dict__ for item in aggregates]
    panel = pd.DataFrame(rows)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    panel = panel.set_index(["trade_date", "l1_code"]).sort_index()
    codes = tuple(sorted(panel.index.get_level_values("l1_code").unique()))
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("direct sector feature panel requires L1/31 or L2/131")
    if len(codes) != expected_sector_count:
        raise StateModelSetError(
            f"feature panel requires {expected_sector_count} direct {direct_sector_level} sectors; actual={len(codes)}"
        )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_dates)), name="trade_date")
    expected_index = pd.MultiIndex.from_product([calendar, codes], names=["trade_date", "l1_code"])
    panel = panel.reindex(expected_index)
    market_volume = panel.groupby(level="trade_date")["l1_volume"].transform("sum")
    benchmark = pd.Series(
        {pd.Timestamp(key): _finite_number(value, "csi300_return") for key, value in csi300_returns.items()},
        dtype="float64",
    ).reindex(calendar)
    panel["benchmark_return"] = panel.index.get_level_values("trade_date").map(benchmark)
    by_sector = panel.groupby(level="l1_code", group_keys=False)
    panel["daily_return"] = panel["l1_return"]
    panel["daily_excess"] = panel["l1_return"] - panel["benchmark_return"]
    panel["excess_return_Nd"] = by_sector["daily_excess"].rolling(3, min_periods=3).mean().droplevel(0)
    panel["volume_ratio"] = panel["l1_volume"] / market_volume.replace(0, np.nan)
    panel["volatility_Nd"] = by_sector["l1_return"].rolling(3, min_periods=3).std(ddof=0).droplevel(0)
    moneyflow_denominator = panel["moneyflow_amount"] if use_moneyflow_amount_denominator else panel["l1_amount"]
    panel["net_mf_ratio"] = panel["net_mf_amount"] / moneyflow_denominator.replace(0, np.nan)
    panel["elg_net_mf_ratio"] = (panel["buy_elg_amount"] - panel["sell_elg_amount"]) / moneyflow_denominator.replace(
        0, np.nan
    )
    panel["sector_turnover"] = panel["l1_amount"] / panel["l1_total_mv"].replace(0, np.nan) * 100.0
    panel["sf_turnover_pctile_250d_neg"] = -_rolling_rank(panel["sector_turnover"], 250, 120)
    panel["sf_turnover_pctile_120d_neg"] = -_rolling_rank(panel["sector_turnover"], 120, 60)
    turn5 = by_sector["sector_turnover"].rolling(5, min_periods=3).mean().droplevel(0)
    turn20 = by_sector["sector_turnover"].rolling(20, min_periods=10).mean().droplevel(0)
    panel["sf_turnover_ma5_ma20_neg"] = -(turn5 / turn20.replace(0, np.nan) - 1.0)
    panel["sf_mf_net_ratio_std_5d_neg"] = -by_sector["net_mf_ratio"].rolling(5, min_periods=5).std(ddof=1).droplevel(0)
    panel["small_net_ratio"] = (panel["buy_sm_amount"] - panel["sell_sm_amount"]) / moneyflow_denominator.replace(
        0, np.nan
    )
    panel["sf_small_net_ratio_5d"] = by_sector["small_net_ratio"].rolling(5, min_periods=3).mean().droplevel(0)
    panel["sf_intraday_range_5d_neg"] = -by_sector["l1_range_ratio"].rolling(5, min_periods=3).mean().droplevel(0)
    panel["atr14"] = by_sector["l1_true_range_ratio"].rolling(14, min_periods=10).mean().droplevel(0)
    panel["sf_atr14_pctile_250d_neg"] = -_rolling_rank(panel["atr14"], 250, 120)

    complete_count = panel["l1_return"].notna().groupby(level="trade_date").transform("sum")
    if cross_section_min_coverage is None:
        cross_section_valid = complete_count == expected_sector_count
        cross_section_contract = "exact_complete"
    else:
        if not math.isfinite(cross_section_min_coverage) or not 0 < cross_section_min_coverage <= 1:
            raise StateModelSetError("cross-section minimum coverage must be in (0,1]")
        cross_section_valid = complete_count / expected_sector_count >= cross_section_min_coverage
        cross_section_contract = "coverage_aware_diagnostic"
    range_median = panel["l1_range_ratio"].groupby(level="trade_date").transform("median")
    range_vs_market = (panel["l1_range_ratio"] / range_median.replace(0, np.nan)).where(cross_section_valid)
    panel["sf_range_vs_market_10d"] = (
        range_vs_market.groupby(level="l1_code", group_keys=False)
        .rolling(
            10,
            min_periods=5,
        )
        .mean()
        .droplevel(0)
    )
    vol20 = by_sector["l1_return"].rolling(20, min_periods=10).std(ddof=1).droplevel(0)
    vol_median = vol20.groupby(level="trade_date").transform("median")
    panel["sf_vol_vs_market_20d"] = (vol20 / vol_median.replace(0, np.nan)).where(cross_section_valid)
    panel["sf_breadth_1d"] = panel["breadth_1d"]
    panel["sf_breadth_5d"] = panel["breadth_5d"]
    breadth_mean = panel["breadth_5d"].groupby(level="trade_date").transform("mean")
    panel["sf_excess_breadth_5d"] = (panel["breadth_5d"] - breadth_mean).where(cross_section_valid)
    panel["sf_dispersion_5d_neg"] = -panel["dispersion_5d"]
    panel = panel.replace([np.inf, -np.inf], np.nan)
    feature_definition = {
        "schema_version": FORMULA_VERSION,
        "observation_version": OBSERVATION_VERSION,
        "base_features": list(BASE_FEATURES),
        "all_core_features": list(ALL_CORE_FEATURES),
        "rolling_window": 3,
        "coverage_threshold": MIN_COVERAGE,
        "direct_sector_level": direct_sector_level,
        "cross_section_required_sector_count": expected_sector_count,
        "cross_section_required_l1_count": expected_sector_count if direct_sector_level == "L1" else None,
        "rank_tie_method": "pandas_average_pct",
    }
    if cross_section_min_coverage is not None or use_moneyflow_amount_denominator:
        feature_definition.update(
            {
                "diagnostic_only": True,
                "cross_section_contract": cross_section_contract,
                "cross_section_min_coverage": cross_section_min_coverage,
                "moneyflow_denominator_by_feature": {
                    "net_mf_ratio": "moneyflow_contributor_amount",
                    "elg_net_mf_ratio": "moneyflow_contributor_amount",
                    "sf_mf_net_ratio_std_5d_neg": "moneyflow_contributor_amount",
                    "sf_small_net_ratio_5d": "moneyflow_contributor_amount",
                }
                if use_moneyflow_amount_denominator
                else {
                    "net_mf_ratio": "l1_amount",
                    "elg_net_mf_ratio": "l1_amount",
                    "sf_mf_net_ratio_std_5d_neg": "l1_amount",
                    "sf_small_net_ratio_5d": "l1_amount",
                },
            }
        )
    return panel, feature_definition


def _cross_section_mask_hash(codes: Sequence[str], active: set[str]) -> str:
    return canonical_sha256([{"sector_code": code, "active": code in active} for code in codes])


def complete_c010_domain_receipts(
    evidence: Mapping[str, Any],
    *,
    trading_dates: Sequence[date],
    l1_sector_codes: Sequence[str],
    l2_sector_codes: Sequence[str] | None,
) -> dict[str, Any]:
    """Materialize one price-domain receipt for every frozen level/sector/date identity."""

    body = {key: value for key, value in dict(evidence).items() if key != "receipt_sha256"}
    l1_only = l2_sector_codes is None
    expected_schema = (
        "hmm_risk_c010_rotation_l1_feature_domain_aggregate_evidence_v1" if l1_only else C010_AGGREGATE_RECEIPT_VERSION
    )
    if body.get("schema_version") != expected_schema:
        raise StateModelSetError("C-010 aggregate evidence schema is invalid")
    calendar = tuple(trading_dates)
    if not calendar or tuple(sorted(set(calendar))) != calendar:
        raise StateModelSetError("C-010 aggregate receipt calendar is invalid")
    levels = [
        ("L1", tuple(sorted(str(value) for value in l1_sector_codes)), "l1_domain_receipts", "l1_invalid_price_domain")
    ]
    if l2_sector_codes is not None:
        levels.append(
            (
                "L2",
                tuple(sorted(str(value) for value in l2_sector_codes)),
                "l2_domain_receipts",
                "l2_invalid_price_domain",
            )
        )
    for level, codes, valid_field, invalid_field in levels:
        expected_count = 31 if level == "L1" else 131
        if len(codes) != expected_count or len(set(codes)) != expected_count or any(not value for value in codes):
            raise StateModelSetError(f"C-010 {level} aggregate receipt sector set is invalid")
        valid = list(body.get(valid_field) or ())
        invalid = list(body.get(invalid_field) or ())
        observed: set[tuple[str, str]] = set()
        for entry in itertools.chain(valid, invalid):
            if not isinstance(entry, Mapping):
                raise StateModelSetError(f"C-010 {level} aggregate receipt entry is invalid")
            key = (str(entry.get("sector_code") or ""), str(entry.get("trade_date") or ""))
            if key in observed:
                raise StateModelSetError(f"C-010 {level} aggregate receipt identity is duplicated: {key}")
            observed.add(key)
        for trade_date in calendar:
            trade_date_text = trade_date.isoformat()
            for code in codes:
                key = (code, trade_date_text)
                if key in observed:
                    continue
                missing_body = {
                    "direct_sector_level": level,
                    "trade_date": trade_date_text,
                    "sector_code": code,
                    "price_domain_status": "invalid",
                    "price_domain_reason_code": "hmm_risk_c010_expected_opportunity_missing",
                    "price_expected_symbols": [],
                    "price_expected_symbol_sha256": canonical_sha256([]),
                    "price_complete_symbols": [],
                    "price_complete_symbol_sha256": canonical_sha256([]),
                    "price_count_coverage": 0.0,
                    "price_expected_weight": None,
                    "price_complete_weight": None,
                    "price_weight_coverage": 0.0,
                    "missing_evidence": [
                        {
                            "sector_code": code,
                            "trade_date": trade_date_text,
                            "reason_code": "hmm_risk_c010_expected_opportunity_missing",
                        }
                    ],
                }
                invalid.append({**missing_body, "entry_sha256": canonical_sha256(missing_body)})
        valid.sort(key=lambda value: (str(value.get("trade_date") or ""), str(value.get("sector_code") or "")))
        invalid.sort(key=lambda value: (str(value.get("trade_date") or ""), str(value.get("sector_code") or "")))
        body[valid_field] = valid
        body[invalid_field] = invalid
        body[f"{level.lower()}_domain_expected_count"] = expected_count * len(calendar)
        body[f"{level.lower()}_domain_receipt_count"] = len(valid) + len(invalid)
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _c010_valid_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text.lower())


def _c010_require_canonical(value: Any, *, identity_field: str, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StateModelSetError(f"{label} is missing")
    identity = str(value.get(identity_field) or "")
    body = {key: item for key, item in value.items() if key != identity_field}
    if not _c010_valid_sha256(identity) or canonical_sha256(body) != identity:
        raise StateModelSetError(f"{label} canonical identity is invalid")
    return value


def _c010_require_ordered_strings(values: Any, *, label: str, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(values, list) or (not allow_empty and not values):
        raise StateModelSetError(f"{label} is missing")
    normalized = tuple(str(value) for value in values)
    if any(not value for value in normalized) or tuple(sorted(set(normalized))) != normalized:
        raise StateModelSetError(f"{label} must be ordered, unique, and non-empty")
    return normalized


def _c010_require_authority_identity(value: Any, *, label: str) -> Mapping[str, Any]:
    identity = _c010_require_canonical(value, identity_field="identity_sha256", label=label)
    if set(identity) != {"authority_type", "authority", "identity_sha256"}:
        raise StateModelSetError(f"{label} fields are invalid")
    if not str(identity.get("authority_type") or "") or not isinstance(identity.get("authority"), Mapping):
        raise StateModelSetError(f"{label} semantics are invalid")
    return identity


def _c010_validate_partition_predicate(value: Any, *, label: str) -> tuple[str, Mapping[str, Any]]:
    predicate = _c010_require_canonical(value, identity_field="receipt_sha256", label=label)
    if set(predicate) != {"status", "authority_receipt", "authority_receipt_sha256", "receipt_sha256"}:
        raise StateModelSetError(f"{label} fields are invalid")
    status = str(predicate.get("status") or "")
    authority = predicate.get("authority_receipt")
    if (
        status not in {"available", "unavailable", "invalid"}
        or not isinstance(authority, Mapping)
        or predicate.get("authority_receipt_sha256") != canonical_sha256(dict(authority))
    ):
        raise StateModelSetError(f"{label} semantics are invalid")
    return status, authority


def _c010_expected_partition_predicate_status(
    field: str,
    authority: Mapping[str, Any],
    *,
    resolver: Mapping[str, Any],
) -> str:
    """Rebuild predicate status from its typed evidence instead of trusting the claimed status."""

    candidates = authority.get("candidates")
    candidate_count = authority.get("candidate_count")
    if (
        not isinstance(candidates, list)
        or not isinstance(candidate_count, int)
        or isinstance(candidate_count, bool)
        or candidate_count != len(candidates)
        or any(not isinstance(item, Mapping) for item in candidates)
    ):
        raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: predicate candidates")
    common_fields = {"authority_identity_sha256", "candidate_count", "candidates"}
    if field == "price_authority_present":
        if set(authority) != common_fields | {"source_resolution"}:
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: price predicate fields")
        if authority.get("source_resolution") != resolver.get("price_source_resolution"):
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: price resolver drift")
    elif field in {"sw_l1_identity_valid", "sw_l2_identity_valid"}:
        expected_level = "L1" if field == "sw_l1_identity_valid" else "L2"
        if set(authority) != common_fields | {"level"} or authority.get("level") != expected_level:
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: SW predicate fields")
    elif field == "pit_eligible":
        if set(authority) != common_fields:
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: PIT predicate fields")
    else:
        raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: unknown predicate")
    if field in {"pit_eligible", "price_authority_present"}:
        return "available" if candidate_count == 1 else ("unavailable" if candidate_count == 0 else "invalid")
    if candidate_count == 0:
        return "unavailable"
    if candidate_count != 1:
        return "invalid"
    code_field = "l1_code" if field == "sw_l1_identity_valid" else "l2_code"
    return "available" if str(candidates[0].get(code_field) or "") else "unavailable"


def validate_c010_provider_absence_domain_partition(receipt: Any) -> dict[str, Any]:
    """Validate the complete P_all/P_in/P_out authority used by both writer and readback."""

    value = _c010_require_canonical(
        receipt,
        identity_field="receipt_sha256",
        label="C-010 provider-absence domain partition",
    )
    required_fields = {
        "schema_version",
        "contract_version",
        "policy_version",
        "train_start",
        "train_end",
        "provider_absence_manifest_identity",
        "security_resolver_identity",
        "pit_authority_identity",
        "price_source_identity",
        "sw_mapping_classify_identity",
        "p_all_entry_count",
        "p_in_entry_count",
        "p_out_entry_count",
        "p_all_ordered_key_sha256",
        "p_in_ordered_key_sha256",
        "p_out_ordered_key_sha256",
        "entries",
        "partition_complete",
        "diagnostic_only",
        "formal_policy_activated",
        "receipt_sha256",
    }
    if set(value) != required_fields:
        raise StateModelSetError("C-010 provider-absence domain partition fields are incomplete")
    if (
        value.get("schema_version") != C010_PROVIDER_ABSENCE_PARTITION_VERSION
        or value.get("contract_version") != C010_PROVIDER_ABSENCE_PARTITION_VERSION
        or value.get("policy_version") != C010_POLICY_VERSION
        or value.get("partition_complete") is not True
        or not isinstance(value.get("diagnostic_only"), bool)
        or value.get("formal_policy_activated") is not (not value["diagnostic_only"])
    ):
        raise StateModelSetError("C-010 provider-absence domain partition contract is invalid")
    try:
        train_start = date.fromisoformat(str(value.get("train_start") or ""))
        train_end = date.fromisoformat(str(value.get("train_end") or ""))
    except ValueError as exc:
        raise StateModelSetError("C-010 provider-absence partition train window is invalid") from exc
    if train_start > train_end:
        raise StateModelSetError("C-010 provider-absence partition train window is invalid")
    authority_type_by_field = {
        "provider_absence_manifest_identity": "provider_absence_manifest",
        "security_resolver_identity": "security_source_identity_manifest",
        "pit_authority_identity": "stock_universe_pit_state_and_spans",
        "price_source_identity": "market.kline_daily_raw",
        "sw_mapping_classify_identity": "sw_index_member_and_classify_mapping",
    }
    for field, expected_type in authority_type_by_field.items():
        authority = _c010_require_authority_identity(value.get(field), label=f"C-010 partition {field}")
        if authority.get("authority_type") != expected_type:
            raise StateModelSetError(f"C-010 partition {field} authority type is invalid")
    entries = value.get("entries")
    if not isinstance(entries, list):
        raise StateModelSetError("C-010 provider-absence partition entries are missing")
    required_entry_fields = {
        "canonical_ts_code",
        "source_ts_code",
        "stable_security_identity",
        "trade_date",
        "provider_row_hash",
        "security_resolver_receipt",
        "security_resolver_receipt_sha256",
        "pit_eligible",
        "price_authority_present",
        "sw_l1_identity_valid",
        "sw_l2_identity_valid",
        "failed_predicates",
        "partition",
        "primary_reason_code",
        "policy_version",
        "entry_sha256",
    }
    predicate_order = (
        "pit_eligible",
        "price_authority_present",
        "sw_l1_identity_valid",
        "sw_l2_identity_valid",
    )
    primary_reason_by_predicate = {
        "pit_eligible": "hmm_risk_c010_pit_ineligible_for_opportunity",
        "price_authority_present": "hmm_risk_c010_price_unavailable_for_opportunity",
        "sw_l1_identity_valid": "hmm_risk_c010_sw_identity_unavailable_for_opportunity",
        "sw_l2_identity_valid": "hmm_risk_c010_sw_identity_unavailable_for_opportunity",
    }
    all_keys: list[dict[str, str]] = []
    in_keys: list[dict[str, str]] = []
    out_keys: list[dict[str, str]] = []
    prior_key: tuple[str, str] | None = None
    observed_keys: set[tuple[str, str]] = set()
    for raw_entry in entries:
        entry = _c010_require_canonical(raw_entry, identity_field="entry_sha256", label="C-010 partition entry")
        if set(entry) != required_entry_fields:
            raise StateModelSetError("C-010 provider-absence partition entry fields are incomplete")
        canonical_ts_code = str(entry.get("canonical_ts_code") or "")
        source_ts_code = str(entry.get("source_ts_code") or "")
        trade_date_text = str(entry.get("trade_date") or "")
        stable_identity = str(entry.get("stable_security_identity") or "")
        key = (canonical_ts_code, trade_date_text)
        try:
            parsed_date = date.fromisoformat(trade_date_text)
        except ValueError as exc:
            raise StateModelSetError("C-010 provider-absence partition entry date is invalid") from exc
        resolver = entry.get("security_resolver_receipt")
        provider_resolution = (
            resolver.get("provider_absence_source_resolution") if isinstance(resolver, Mapping) else None
        )
        price_resolution = resolver.get("price_source_resolution") if isinstance(resolver, Mapping) else None
        if (
            not canonical_ts_code
            or not source_ts_code
            or stable_identity != f"canonical:{canonical_ts_code}"
            or key in observed_keys
            or (prior_key is not None and key <= prior_key)
            or not train_start <= parsed_date <= train_end
            or not _c010_valid_sha256(entry.get("provider_row_hash"))
            or not isinstance(resolver, Mapping)
            or set(resolver)
            != {
                "security_resolver_identity_sha256",
                "provider_absence_source_resolution",
                "price_source_resolution",
            }
            or resolver.get("security_resolver_identity_sha256")
            != value["security_resolver_identity"]["identity_sha256"]
            or not isinstance(provider_resolution, Mapping)
            or not isinstance(price_resolution, Mapping)
            or provider_resolution.get("canonical_ts_code") != canonical_ts_code
            or provider_resolution.get("source_ts_code") != source_ts_code
            or provider_resolution.get("source_dataset") != "market.moneyflow_ts"
            or price_resolution.get("canonical_ts_code") != canonical_ts_code
            or price_resolution.get("source_dataset") != "market.kline_daily_raw"
            or not str(price_resolution.get("source_ts_code") or "")
            or entry.get("security_resolver_receipt_sha256") != canonical_sha256(dict(resolver))
            or entry.get("policy_version") != C010_POLICY_VERSION
        ):
            raise StateModelSetError("C-010 provider-absence partition entry identity is invalid")
        predicates = {
            field: _c010_validate_partition_predicate(entry.get(field), label=f"C-010 partition {field}")
            for field in predicate_order
        }
        statuses = {field: predicate[0] for field, predicate in predicates.items()}
        expected_predicate_authority = {
            "pit_eligible": value["pit_authority_identity"]["identity_sha256"],
            "price_authority_present": value["price_source_identity"]["identity_sha256"],
            "sw_l1_identity_valid": value["sw_mapping_classify_identity"]["identity_sha256"],
            "sw_l2_identity_valid": value["sw_mapping_classify_identity"]["identity_sha256"],
        }
        if any(
            predicates[field][1].get("authority_identity_sha256") != expected_predicate_authority[field]
            for field in predicate_order
        ):
            raise StateModelSetError(
                "hmm_risk_c010_provider_absence_domain_partition_invalid: predicate authority drift"
            )
        rebuilt_statuses = {
            field: _c010_expected_partition_predicate_status(field, predicates[field][1], resolver=resolver)
            for field in predicate_order
        }
        if statuses != rebuilt_statuses:
            raise StateModelSetError(
                "hmm_risk_c010_provider_absence_domain_partition_invalid: predicate status/evidence drift"
            )
        if predicates["sw_l1_identity_valid"][1].get("candidate_count") != predicates["sw_l2_identity_valid"][1].get(
            "candidate_count"
        ) or predicates["sw_l1_identity_valid"][1].get("candidates") != predicates["sw_l2_identity_valid"][1].get(
            "candidates"
        ):
            raise StateModelSetError(
                "hmm_risk_c010_provider_absence_domain_partition_invalid: SW predicate evidence drift"
            )
        if "invalid" in statuses.values():
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: predicate invalid")
        failed = [field for field in predicate_order if statuses[field] == "unavailable"]
        expected_partition = "in_domain" if not failed else "out_of_domain"
        expected_primary_reason = None if not failed else primary_reason_by_predicate[failed[0]]
        if (
            entry.get("failed_predicates") != failed
            or entry.get("partition") != expected_partition
            or entry.get("primary_reason_code") != expected_primary_reason
        ):
            raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: classification drift")
        observed_keys.add(key)
        prior_key = key
        key_body = {"canonical_ts_code": canonical_ts_code, "trade_date": trade_date_text}
        all_keys.append(key_body)
        (in_keys if expected_partition == "in_domain" else out_keys).append(key_body)
    if (
        value.get("p_all_entry_count") != len(all_keys)
        or value.get("p_in_entry_count") != len(in_keys)
        or value.get("p_out_entry_count") != len(out_keys)
        or len(in_keys) + len(out_keys) != len(all_keys)
        or value.get("p_all_ordered_key_sha256") != canonical_sha256(all_keys)
        or value.get("p_in_ordered_key_sha256") != canonical_sha256(in_keys)
        or value.get("p_out_ordered_key_sha256") != canonical_sha256(out_keys)
    ):
        raise StateModelSetError("hmm_risk_c010_provider_absence_domain_partition_invalid: cardinality/hash mismatch")
    return dict(value)


def validate_c010_expected_opportunity_receipt(receipt: Any) -> dict[str, Any]:
    value = _c010_require_canonical(
        receipt,
        identity_field="receipt_sha256",
        label="C-010 expected-opportunity receipt",
    )
    required_fields = {
        "schema_version",
        "train_start",
        "train_end",
        "authority_identities",
        "entry_count",
        "opportunity_key_count",
        "opportunity_ordered_key_sha256",
        "entries",
        "receipt_sha256",
    }
    if set(value) != required_fields or value.get("schema_version") != C010_EXPECTED_OPPORTUNITY_CONTRACT:
        raise StateModelSetError("C-010 expected-opportunity receipt fields are incomplete")
    try:
        train_start = date.fromisoformat(str(value.get("train_start") or ""))
        train_end = date.fromisoformat(str(value.get("train_end") or ""))
    except ValueError as exc:
        raise StateModelSetError("C-010 expected-opportunity train window is invalid") from exc
    authorities = value.get("authority_identities")
    if not isinstance(authorities, list) or not authorities:
        raise StateModelSetError("C-010 expected-opportunity authorities are missing")
    authority_hashes = []
    for authority in authorities:
        validated = _c010_require_authority_identity(authority, label="C-010 expected-opportunity authority")
        authority_hashes.append(str(validated["identity_sha256"]))
    if authority_hashes != sorted(set(authority_hashes)):
        raise StateModelSetError("C-010 expected-opportunity authorities are not canonical")
    entries = value.get("entries")
    if not isinstance(entries, list) or not entries:
        raise StateModelSetError("C-010 expected-opportunity entries are missing")
    keys: list[dict[str, str]] = []
    prior_symbol: str | None = None
    required_entry_fields = {
        "canonical_ts_code",
        "opportunity_dates",
        "opportunity_count",
        "opportunity_date_sha256",
        "authority_identity_sha256",
        "entry_sha256",
    }
    combined_authority_sha256 = canonical_sha256(authorities)
    for raw_entry in entries:
        entry = _c010_require_canonical(raw_entry, identity_field="entry_sha256", label="C-010 opportunity entry")
        if set(entry) != required_entry_fields:
            raise StateModelSetError("C-010 expected-opportunity entry fields are incomplete")
        symbol = str(entry.get("canonical_ts_code") or "")
        dates = _c010_require_ordered_strings(entry.get("opportunity_dates"), label="C-010 opportunity dates")
        try:
            parsed_dates = tuple(date.fromisoformat(item) for item in dates)
        except ValueError as exc:
            raise StateModelSetError("C-010 expected-opportunity date is invalid") from exc
        if (
            not symbol
            or (prior_symbol is not None and symbol <= prior_symbol)
            or any(not train_start <= item <= train_end for item in parsed_dates)
            or entry.get("opportunity_count") != len(dates)
            or entry.get("opportunity_date_sha256") != canonical_sha256(list(dates))
            or entry.get("authority_identity_sha256") != combined_authority_sha256
        ):
            raise StateModelSetError("C-010 expected-opportunity entry semantics are invalid")
        prior_symbol = symbol
        keys.extend({"canonical_ts_code": symbol, "trade_date": item} for item in dates)
    if (
        value.get("entry_count") != len(entries)
        or value.get("opportunity_key_count") != len(keys)
        or value.get("opportunity_ordered_key_sha256") != canonical_sha256(keys)
    ):
        raise StateModelSetError("C-010 expected-opportunity receipt cardinality/hash mismatch")
    return dict(value)


def _validate_c010_eligibility_receipt_v1(receipt: Any) -> tuple[list[dict[str, Any]], list[str]]:
    value = _c010_require_canonical(receipt, identity_field="receipt_sha256", label="C-010 eligibility receipt")
    entries = value.get("entries")
    if (
        value.get("schema_version") != C010_ELIGIBILITY_RECEIPT_VERSION_V1
        or value.get("minimum_availability_ratio") != MIN_COVERAGE
        or value.get("availability_integer_contract") != "10*(expected-missing) >= 9*expected"
        or value.get("diagnostic_only") is not False
        or value.get("formal_policy_activated") is not True
        or not isinstance(entries, list)
        or value.get("entry_count") != len(entries)
        or not entries
    ):
        raise StateModelSetError("C-010 eligibility receipt contract is invalid")
    symbols: set[str] = set()
    normalized: list[dict[str, Any]] = []
    excluded: list[str] = []
    required_entry_fields = {
        "canonical_ts_code",
        "expected_opportunity_count",
        "expected_opportunity_contract",
        "expected_opportunity_date_sha256",
        "provider_absence_count",
        "availability_ratio",
        "moneyflow_contributor_eligible",
        "provider_absence_key_sha256",
        "entry_sha256",
    }
    for entry in entries:
        entry_value = _c010_require_canonical(entry, identity_field="entry_sha256", label="C-010 eligibility entry")
        if set(entry_value) != required_entry_fields:
            raise StateModelSetError("C-010 eligibility entry fields are incomplete")
        symbol = str(entry_value.get("canonical_ts_code") or "")
        expected = entry_value.get("expected_opportunity_count")
        missing = entry_value.get("provider_absence_count")
        eligible = entry_value.get("moneyflow_contributor_eligible")
        try:
            ratio = float(entry_value.get("availability_ratio"))
        except (TypeError, ValueError):
            ratio = math.nan
        if (
            not symbol
            or symbol in symbols
            or not isinstance(expected, int)
            or isinstance(expected, bool)
            or expected <= 0
            or not isinstance(missing, int)
            or isinstance(missing, bool)
            or not 0 <= missing <= expected
            or not isinstance(eligible, bool)
            or entry_value.get("expected_opportunity_contract") != C010_EXPECTED_OPPORTUNITY_CONTRACT_V1
            or not _c010_valid_sha256(entry_value.get("expected_opportunity_date_sha256"))
            or not _c010_valid_sha256(entry_value.get("provider_absence_key_sha256"))
            or not math.isfinite(ratio)
            or ratio != (expected - missing) / expected
            or eligible is not ((10 * (expected - missing)) >= (9 * expected))
        ):
            raise StateModelSetError("C-010 eligibility entry semantics are invalid")
        symbols.add(symbol)
        normalized.append(dict(entry_value))
        if not eligible:
            excluded.append(symbol)
    if [entry["canonical_ts_code"] for entry in normalized] != sorted(symbols):
        raise StateModelSetError("C-010 eligibility entries are not in canonical symbol order")
    if value.get("excluded_moneyflow_symbols") != excluded:
        raise StateModelSetError("C-010 eligibility exclusion identity is invalid")
    return normalized, excluded


def _validate_c010_eligibility_receipt_v2(receipt: Any) -> tuple[list[dict[str, Any]], list[str]]:
    value = _c010_require_canonical(receipt, identity_field="receipt_sha256", label="C-010 eligibility receipt")
    required_fields = {
        "schema_version",
        "train_start",
        "train_end",
        "minimum_availability_ratio",
        "availability_integer_contract",
        "entry_count",
        "entries",
        "excluded_moneyflow_symbols",
        "expected_opportunity_receipt",
        "expected_opportunity_receipt_sha256",
        "provider_absence_partition_receipt",
        "provider_absence_partition_receipt_sha256",
        "pit_universe_changed",
        "selection_universe_changed",
        "runtime_prediction_eligibility_changed",
        "diagnostic_only",
        "formal_policy_activated",
        "receipt_sha256",
    }
    entries = value.get("entries")
    if (
        set(value) != required_fields
        or value.get("schema_version") != C010_ELIGIBILITY_RECEIPT_VERSION
        or value.get("minimum_availability_ratio") != MIN_COVERAGE
        or value.get("availability_integer_contract") != "10*(expected-missing) >= 9*expected"
        or value.get("diagnostic_only") is not False
        or value.get("formal_policy_activated") is not True
        or not isinstance(entries, list)
        or value.get("entry_count") != len(entries)
        or not entries
    ):
        raise StateModelSetError("C-010 eligibility v2 receipt contract is invalid")
    opportunity = validate_c010_expected_opportunity_receipt(value.get("expected_opportunity_receipt"))
    partition = validate_c010_provider_absence_domain_partition(value.get("provider_absence_partition_receipt"))
    expected_authority_hashes = {
        str(partition[field]["identity_sha256"])
        for field in (
            "security_resolver_identity",
            "pit_authority_identity",
            "price_source_identity",
            "sw_mapping_classify_identity",
        )
    }
    if (
        value.get("expected_opportunity_receipt_sha256") != opportunity.get("receipt_sha256")
        or value.get("provider_absence_partition_receipt_sha256") != partition.get("receipt_sha256")
        or partition.get("formal_policy_activated") is not True
        or partition.get("diagnostic_only") is not False
        or {str(item["identity_sha256"]) for item in opportunity["authority_identities"]} != expected_authority_hashes
    ):
        raise StateModelSetError("C-010 eligibility v2 authority identity is invalid")
    opportunity_by_symbol = {
        str(entry["canonical_ts_code"]): tuple(entry["opportunity_dates"]) for entry in opportunity["entries"]
    }
    p_in_by_symbol: dict[str, list[str]] = {}
    expected_keys = {
        (symbol, opportunity_date)
        for symbol, opportunity_dates in opportunity_by_symbol.items()
        for opportunity_date in opportunity_dates
    }
    for entry in partition["entries"]:
        key = (str(entry["canonical_ts_code"]), str(entry["trade_date"]))
        if entry["partition"] == "in_domain":
            if key not in expected_keys:
                raise StateModelSetError("C-010 eligibility v2 P_in is outside O_sector")
            p_in_by_symbol.setdefault(key[0], []).append(key[1])
        elif key in expected_keys:
            raise StateModelSetError("C-010 eligibility v2 P_out intersects O_sector")
    symbols: set[str] = set()
    normalized: list[dict[str, Any]] = []
    excluded: list[str] = []
    required_entry_fields = {
        "canonical_ts_code",
        "expected_opportunity_count",
        "expected_opportunity_contract",
        "expected_opportunity_date_sha256",
        "provider_absence_count",
        "availability_ratio",
        "moneyflow_contributor_eligible",
        "provider_absence_key_sha256",
        "p_in_date_sha256",
        "entry_sha256",
    }
    for raw_entry in entries:
        entry = _c010_require_canonical(raw_entry, identity_field="entry_sha256", label="C-010 eligibility entry")
        if set(entry) != required_entry_fields:
            raise StateModelSetError("C-010 eligibility v2 entry fields are incomplete")
        symbol = str(entry.get("canonical_ts_code") or "")
        expected_dates = opportunity_by_symbol.get(symbol)
        p_in_dates = p_in_by_symbol.get(symbol, [])
        expected = entry.get("expected_opportunity_count")
        missing = entry.get("provider_absence_count")
        eligible = entry.get("moneyflow_contributor_eligible")
        try:
            ratio = float(entry.get("availability_ratio"))
        except (TypeError, ValueError):
            ratio = math.nan
        expected_p_in_key_hash = canonical_sha256(
            [{"canonical_ts_code": symbol, "trade_date": item} for item in p_in_dates]
        )
        if (
            not symbol
            or symbol in symbols
            or expected_dates is None
            or not isinstance(expected, int)
            or isinstance(expected, bool)
            or expected != len(expected_dates)
            or not isinstance(missing, int)
            or isinstance(missing, bool)
            or missing != len(p_in_dates)
            or any(item not in expected_dates for item in p_in_dates)
            or not isinstance(eligible, bool)
            or entry.get("expected_opportunity_contract") != C010_EXPECTED_OPPORTUNITY_CONTRACT
            or entry.get("expected_opportunity_date_sha256") != canonical_sha256(list(expected_dates))
            or entry.get("provider_absence_key_sha256") != expected_p_in_key_hash
            or entry.get("p_in_date_sha256") != canonical_sha256(p_in_dates)
            or not math.isfinite(ratio)
            or ratio != (expected - missing) / expected
            or eligible is not ((10 * (expected - missing)) >= (9 * expected))
        ):
            raise StateModelSetError("C-010 eligibility v2 entry semantics are invalid")
        symbols.add(symbol)
        normalized.append(dict(entry))
        if not eligible:
            excluded.append(symbol)
    if [entry["canonical_ts_code"] for entry in normalized] != sorted(opportunity_by_symbol):
        raise StateModelSetError("C-010 eligibility v2 entries do not cover the opportunity ledger")
    if value.get("excluded_moneyflow_symbols") != excluded:
        raise StateModelSetError("C-010 eligibility v2 exclusion identity is invalid")
    return normalized, excluded


def _validate_c010_eligibility_receipt(
    receipt: Any,
    *,
    policy_version: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    if policy_version == C010_POLICY_VERSION_V1:
        return _validate_c010_eligibility_receipt_v1(receipt)
    if policy_version == C010_POLICY_VERSION:
        return _validate_c010_eligibility_receipt_v2(receipt)
    raise StateModelSetError("C-010 eligibility policy version is unsupported")


def _validate_c010_domain_receipt_set(
    receipt: Any,
    *,
    dates: tuple[str, ...],
    level_codes: Mapping[str, tuple[str, ...]],
    moneyflow_eligibility: Mapping[str, bool],
) -> None:
    value = _c010_require_canonical(receipt, identity_field="receipt_sha256", label="C-010 aggregate receipt")
    if (
        value.get("schema_version") != C010_AGGREGATE_RECEIPT_VERSION
        or value.get("formal_policy_activated") is not True
    ):
        raise StateModelSetError("C-010 aggregate receipt contract is invalid")
    reason_by_status = {
        "available": None,
        "structurally_unavailable": "hmm_risk_c010_moneyflow_domain_structurally_unavailable",
        "coverage_insufficient": "hmm_risk_c010_moneyflow_domain_coverage_insufficient",
        "denominator_invalid": "hmm_risk_c010_moneyflow_denominator_invalid",
    }
    for level, prefix in (("L1", "l1"), ("L2", "l2")):
        valid = value.get(f"{prefix}_domain_receipts")
        invalid = value.get(f"{prefix}_invalid_price_domain")
        if not isinstance(valid, list) or not isinstance(invalid, list):
            raise StateModelSetError(f"C-010 {level} aggregate receipt entries are missing")
        if value.get(f"{prefix}_aggregate_count") != len(valid):
            raise StateModelSetError(f"C-010 {level} aggregate receipt count is invalid")
        expected_keys = {(code, trade_date) for trade_date in dates for code in level_codes[level]}
        if value.get(f"{prefix}_domain_expected_count") != len(expected_keys) or value.get(
            f"{prefix}_domain_receipt_count"
        ) != len(valid) + len(invalid):
            raise StateModelSetError(f"C-010 {level} aggregate receipt set is incomplete")
        observed_keys: set[tuple[str, str]] = set()
        for entry, is_valid_entry in itertools.chain(
            ((entry, True) for entry in valid),
            ((entry, False) for entry in invalid),
        ):
            item = _c010_require_canonical(entry, identity_field="entry_sha256", label=f"C-010 {level} domain entry")
            key = (str(item.get("sector_code") or ""), str(item.get("trade_date") or ""))
            if item.get("direct_sector_level") != level or key not in expected_keys or key in observed_keys:
                raise StateModelSetError(f"C-010 {level} aggregate receipt identity is invalid")
            observed_keys.add(key)
            expected_symbols = _c010_require_ordered_strings(
                item.get("price_expected_symbols"), label=f"C-010 {level} price expected symbols", allow_empty=True
            )
            complete_symbols = _c010_require_ordered_strings(
                item.get("price_complete_symbols"), label=f"C-010 {level} price complete symbols", allow_empty=True
            )
            if (
                not set(complete_symbols).issubset(expected_symbols)
                or item.get("price_expected_symbol_sha256") != canonical_sha256(list(expected_symbols))
                or item.get("price_complete_symbol_sha256") != canonical_sha256(list(complete_symbols))
                or not isinstance(item.get("missing_evidence"), list)
            ):
                raise StateModelSetError(f"C-010 {level} price-domain contributor receipt is invalid")
            if is_valid_entry:
                try:
                    price_expected_weight = float(item.get("price_expected_weight"))
                    price_complete_weight = float(item.get("price_complete_weight"))
                    price_count_coverage = float(item.get("price_count_coverage"))
                    price_weight_coverage = float(item.get("price_weight_coverage"))
                except (TypeError, ValueError) as exc:
                    raise StateModelSetError(f"C-010 {level} price-domain coverage values are invalid") from exc
                moneyflow_status = str(item.get("moneyflow_domain_status") or "")
                moneyflow_expected = _c010_require_ordered_strings(
                    item.get("moneyflow_expected_symbols"),
                    label=f"C-010 {level} moneyflow expected symbols",
                    allow_empty=True,
                )
                moneyflow_complete = _c010_require_ordered_strings(
                    item.get("moneyflow_complete_symbols"),
                    label=f"C-010 {level} moneyflow complete symbols",
                    allow_empty=True,
                )
                moneyflow_excluded = _c010_require_ordered_strings(
                    item.get("moneyflow_excluded_symbols"),
                    label=f"C-010 {level} moneyflow excluded symbols",
                    allow_empty=True,
                )
                expected_moneyflow = tuple(
                    symbol for symbol in expected_symbols if moneyflow_eligibility.get(symbol) is True
                )
                expected_moneyflow_set = set(expected_moneyflow)
                expected_excluded = tuple(symbol for symbol in expected_symbols if symbol not in expected_moneyflow_set)
                if (
                    item.get("price_domain_status") != "available"
                    or item.get("price_domain_reason_code") is not None
                    or not expected_symbols
                    or not set(moneyflow_complete).issubset(moneyflow_expected)
                    or moneyflow_expected != expected_moneyflow
                    or moneyflow_excluded != expected_excluded
                    or item.get("moneyflow_expected_symbol_sha256") != canonical_sha256(list(moneyflow_expected))
                    or item.get("moneyflow_complete_symbol_sha256") != canonical_sha256(list(moneyflow_complete))
                    or moneyflow_status not in reason_by_status
                    or item.get("moneyflow_domain_reason_code") != reason_by_status[moneyflow_status]
                    or not all(
                        math.isfinite(value)
                        for value in (
                            price_expected_weight,
                            price_complete_weight,
                            price_count_coverage,
                            price_weight_coverage,
                        )
                    )
                    or price_expected_weight <= 0
                    or not 0 <= price_complete_weight <= price_expected_weight
                    or price_count_coverage != len(complete_symbols) / len(expected_symbols)
                    or price_weight_coverage != price_complete_weight / price_expected_weight
                    or (10 * len(complete_symbols)) < (9 * len(expected_symbols))
                    or price_weight_coverage < MIN_COVERAGE
                ):
                    raise StateModelSetError(f"C-010 {level} available domain receipt semantics are invalid")
                try:
                    moneyflow_expected_weight = float(item.get("moneyflow_expected_weight"))
                    moneyflow_complete_weight = float(item.get("moneyflow_complete_weight"))
                    moneyflow_count_coverage = float(item.get("moneyflow_count_coverage"))
                    moneyflow_weight_coverage = float(item.get("moneyflow_weight_coverage"))
                except (TypeError, ValueError) as exc:
                    raise StateModelSetError(f"C-010 {level} moneyflow-domain coverage values are invalid") from exc
                if not all(
                    math.isfinite(value)
                    for value in (
                        moneyflow_expected_weight,
                        moneyflow_complete_weight,
                        moneyflow_count_coverage,
                        moneyflow_weight_coverage,
                    )
                ):
                    raise StateModelSetError(f"C-010 {level} moneyflow-domain coverage values are invalid")
                if moneyflow_status == "structurally_unavailable":
                    if moneyflow_expected or any(
                        value != 0.0
                        for value in (
                            moneyflow_expected_weight,
                            moneyflow_complete_weight,
                            moneyflow_count_coverage,
                            moneyflow_weight_coverage,
                        )
                    ):
                        raise StateModelSetError(f"C-010 {level} structurally unavailable domain is invalid")
                else:
                    if (
                        not moneyflow_expected
                        or moneyflow_expected_weight <= 0
                        or not 0 <= moneyflow_complete_weight <= moneyflow_expected_weight
                        or moneyflow_count_coverage != len(moneyflow_complete) / len(moneyflow_expected)
                        or moneyflow_weight_coverage != moneyflow_complete_weight / moneyflow_expected_weight
                    ):
                        raise StateModelSetError(f"C-010 {level} moneyflow-domain contributor receipt is invalid")
                    coverage_valid = (10 * len(moneyflow_complete)) >= (
                        9 * len(moneyflow_expected)
                    ) and moneyflow_weight_coverage >= MIN_COVERAGE
                    if moneyflow_status == "available" and not coverage_valid:
                        raise StateModelSetError(f"C-010 {level} moneyflow-domain coverage status is invalid")
                    if moneyflow_status == "coverage_insufficient" and coverage_valid:
                        raise StateModelSetError(f"C-010 {level} moneyflow-domain coverage status is invalid")
                    if moneyflow_status == "denominator_invalid" and not coverage_valid:
                        raise StateModelSetError(f"C-010 {level} moneyflow denominator status is invalid")
                amount = item.get("moneyflow_contributor_amount")
                if moneyflow_status == "available":
                    try:
                        amount_value = float(amount)
                    except (TypeError, ValueError) as exc:
                        raise StateModelSetError(f"C-010 {level} moneyflow denominator is invalid") from exc
                    if not math.isfinite(amount_value) or amount_value <= 0:
                        raise StateModelSetError(f"C-010 {level} moneyflow denominator is invalid")
                elif amount is not None:
                    raise StateModelSetError(f"C-010 {level} unavailable moneyflow denominator must remain NA")
            elif item.get("price_domain_status") != "invalid" or str(
                item.get("price_domain_reason_code") or ""
            ) not in {
                "hmm_risk_c010_expected_opportunity_missing",
                "hmm_risk_c010_price_domain_weight_denominator_invalid",
                "hmm_risk_c010_price_domain_coverage_insufficient",
            }:
                raise StateModelSetError(f"C-010 {level} invalid price-domain receipt semantics are invalid")
        if observed_keys != expected_keys:
            raise StateModelSetError(f"C-010 {level} aggregate receipt set is incomplete")
        if value.get(f"{prefix}_domain_expected_count") != len(expected_keys) or value.get(
            f"{prefix}_domain_receipt_count"
        ) != len(observed_keys):
            raise StateModelSetError(f"C-010 {level} aggregate receipt cardinality is invalid")


def _validate_c010_cross_section_receipt(
    receipt: Any,
    *,
    level: str,
    dates: tuple[str, ...],
    policy_version: str,
) -> tuple[str, ...]:
    value = _c010_require_canonical(receipt, identity_field="receipt_sha256", label=f"C-010 {level} cross-section")
    expected_count = 31 if level == "L1" else 131
    codes = _c010_require_ordered_strings(
        value.get("expected_sector_codes"), label=f"C-010 {level} expected sector codes"
    )
    entries = value.get("entries")
    if (
        value.get("schema_version") != C010_CROSS_SECTION_RECEIPT_VERSION
        or value.get("formula_version") != C010_FORMULA_VERSION
        or value.get("feature_domain_policy_version") != policy_version
        or value.get("direct_sector_level") != level
        or value.get("expected_sector_count") != expected_count
        or len(codes) != expected_count
        or value.get("expected_sector_sha256") != canonical_sha256(list(codes))
        or value.get("diagnostic_only") is not False
        or not isinstance(entries, list)
        or value.get("entry_count") != len(entries)
    ):
        raise StateModelSetError(f"C-010 {level} cross-section receipt contract is invalid")
    expected_keys = {(feature, trade_date) for feature in C010_CROSS_SECTION_FEATURES for trade_date in dates}
    observed_keys: set[tuple[str, str]] = set()
    entries_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    valid_code_sets_by_key: dict[tuple[str, str], set[str]] = {}
    reason_by_status = {
        "accepted": None,
        "coverage_insufficient": "hmm_risk_c010_feature_cross_section_coverage_insufficient",
        "reference_invalid": "hmm_risk_c010_feature_cross_section_reference_invalid",
        "output_non_finite": "hmm_risk_c010_feature_cross_section_output_non_finite",
    }
    for entry in entries:
        item = _c010_require_canonical(entry, identity_field="entry_sha256", label=f"C-010 {level} cross-section entry")
        key = (str(item.get("feature_name") or ""), str(item.get("trade_date") or ""))
        valid_codes = _c010_require_ordered_strings(
            item.get("valid_sector_codes"), label=f"C-010 {level} valid sector codes", allow_empty=True
        )
        missing_codes = _c010_require_ordered_strings(
            item.get("missing_sector_codes"), label=f"C-010 {level} missing sector codes", allow_empty=True
        )
        status = str(item.get("status") or "")
        feature_name = key[0]
        reference = item.get("reference_value")
        if isinstance(reference, bool):
            raise StateModelSetError(f"C-010 {level} cross-section reference is invalid")
        try:
            reference_value = float(reference) if reference is not None else None
        except (TypeError, ValueError) as exc:
            raise StateModelSetError(f"C-010 {level} cross-section reference is invalid") from exc
        positive_reference_required = feature_name != "sf_excess_breadth_5d"
        accepted_reference = reference_value is not None and math.isfinite(reference_value)
        if accepted_reference:
            accepted_reference = reference_value > 0 if positive_reference_required else 0 <= reference_value <= 1
        invalid_reference = reference_value is not None and math.isfinite(reference_value) and not accepted_reference
        coverage = len(valid_codes) / expected_count
        pre_mask_codes = set(valid_codes) if status in {"accepted", "output_non_finite"} else set()
        expected_pre_mask_hash = _cross_section_mask_hash(codes, pre_mask_codes)
        if (
            key not in expected_keys
            or key in observed_keys
            or item.get("direct_sector_level") != level
            or item.get("source_domain") != "price"
            or item.get("operator") != C010_CROSS_SECTION_OPERATORS.get(feature_name)
            or item.get("expected_sector_count") != expected_count
            or item.get("expected_sector_sha256") != canonical_sha256(list(codes))
            or set(valid_codes).intersection(missing_codes)
            or set(valid_codes).union(missing_codes) != set(codes)
            or item.get("valid_sector_count") != len(valid_codes)
            or item.get("valid_sector_sha256") != canonical_sha256(list(valid_codes))
            or item.get("missing_sector_sha256") != canonical_sha256(list(missing_codes))
            or item.get("feature_cross_section_coverage") != coverage
            or item.get("pre_mask_sha256") != expected_pre_mask_hash
            or not _c010_valid_sha256(item.get("post_mask_sha256"))
            or item.get("post_mask_subset_of_pre_mask") is not True
            or status not in reason_by_status
            or item.get("reason_code") != reason_by_status[status]
            or (status == "coverage_insufficient") is not ((10 * len(valid_codes)) < (9 * expected_count))
            or (status in {"accepted", "reference_invalid", "output_non_finite"} and coverage < MIN_COVERAGE)
            or (status == "coverage_insufficient" and reference is not None)
            or (status == "reference_invalid" and not invalid_reference)
            or (status in {"accepted", "output_non_finite"} and not accepted_reference)
        ):
            raise StateModelSetError(f"C-010 {level} cross-section entry semantics are invalid")
        observed_keys.add(key)
        entries_by_key[key] = item
        valid_code_sets_by_key[key] = set(valid_codes)
    if observed_keys != expected_keys:
        raise StateModelSetError(f"C-010 {level} cross-section receipt set is incomplete")
    for feature_name in C010_CROSS_SECTION_FEATURES:
        for date_index, trade_date in enumerate(dates):
            item = entries_by_key[(feature_name, trade_date)]
            status = str(item["status"])
            if status == "accepted":
                current_codes = valid_code_sets_by_key[(feature_name, trade_date)]
                if feature_name == "sf_range_vs_market_10d":
                    window_dates = dates[max(0, date_index - 9) : date_index + 1]
                    expected_post_codes = {
                        code
                        for code in current_codes
                        if sum(
                            entries_by_key[(feature_name, prior_date)]["status"] == "accepted"
                            and code in valid_code_sets_by_key[(feature_name, prior_date)]
                            for prior_date in window_dates
                        )
                        >= 5
                    }
                else:
                    expected_post_codes = current_codes
            else:
                expected_post_codes = set()
            if item.get("post_mask_sha256") != _cross_section_mask_hash(codes, expected_post_codes):
                raise StateModelSetError(f"C-010 {level} cross-section post-mask evidence is invalid")
    return codes


def _validate_c010_feature_definitions(value: Mapping[str, Any], *, policy_version: str) -> None:
    for level in ("l1", "l2"):
        definition = value.get(f"{level}_feature_definition")
        direct_level = level.upper()
        expected_count = 31 if direct_level == "L1" else 131
        if (
            not isinstance(definition, Mapping)
            or definition.get("schema_version") != C010_FORMULA_VERSION
            or definition.get("feature_domain_policy_version") != policy_version
            or definition.get("diagnostic_only") is not False
            or definition.get("direct_sector_level") != direct_level
            or definition.get("cross_section_required_sector_count") != expected_count
            or definition.get("cross_section_min_coverage") != MIN_COVERAGE
            or definition.get("cross_section_min_valid_sector_count") != (28 if direct_level == "L1" else 118)
            or tuple(definition.get("base_features") or ()) != BASE_FEATURES
            or tuple(definition.get("all_core_features") or ()) != ALL_CORE_FEATURES
            or tuple(definition.get("moneyflow_mandatory_fields") or ()) != MONEYFLOW_STOCK_FIELDS
            or definition.get("moneyflow_denominator_by_feature") != C010_MONEYFLOW_DENOMINATOR_BY_FEATURE
            or definition.get("cross_section_operator_by_feature") != C010_CROSS_SECTION_OPERATORS
            or definition.get("formula_diff_by_feature") != C010_FORMULA_DIFF_BY_FEATURE
            or definition.get("moneyflow_rolling_post_mask_required") is not True
            or definition.get("range_cross_section_rolling_post_mask_required") is not True
            or value.get(f"{level}_feature_definition_sha256") != canonical_sha256(dict(definition))
        ):
            raise StateModelSetError(f"C-010 {level.upper()} feature definition is invalid")


def validate_c010_policy_manifest(manifest: Any) -> dict[str, Any]:
    """Fail closed unless a C-010 policy is complete, self-contained, and semantically readback-safe."""

    value = _c010_require_canonical(manifest, identity_field="receipt_sha256", label="C-010 policy manifest")
    policy_version = str(value.get("schema_version") or "")
    required_fields = {
        "schema_version",
        "formula_version",
        "producer_commit",
        "train_start",
        "train_end",
        "receipt_trading_dates",
        "receipt_trading_date_count",
        "receipt_trading_date_sha256",
        "contributor_min_availability",
        "domain_min_count_coverage",
        "domain_min_weight_coverage",
        "feature_cross_section_min_coverage",
        "moneyflow_mandatory_fields",
        "eligibility_receipt",
        "eligibility_receipt_sha256",
        "eligibility_entry_count",
        "contributor_ledger",
        "contributor_ledger_sha256",
        "excluded_moneyflow_symbols",
        "excluded_moneyflow_symbol_sha256",
        "aggregate_receipt",
        "aggregate_receipt_sha256",
        "l1_cross_section_receipt",
        "l1_cross_section_receipt_sha256",
        "l2_cross_section_receipt",
        "l2_cross_section_receipt_sha256",
        "l1_feature_definition",
        "l1_feature_definition_sha256",
        "l2_feature_definition",
        "l2_feature_definition_sha256",
        "feature_order_by_family",
        "feature_order_sha256",
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "calendar_manifest_hash",
        "security_identity_manifest_sha256",
        "provider_absence_manifest_sha256",
        "causal_circ_mv_identity",
        "causal_circ_mv_identity_sha256",
        "pit_universe_changed",
        "selection_universe_changed",
        "runtime_prediction_eligibility_changed",
        "receipt_sha256",
    }
    if policy_version == C010_POLICY_VERSION:
        required_fields.update(
            {
                "expected_opportunity_receipt",
                "expected_opportunity_receipt_sha256",
                "provider_absence_partition_receipt",
                "provider_absence_partition_receipt_sha256",
            }
        )
    if set(value) != required_fields:
        raise StateModelSetError("C-010 policy manifest fields are incomplete or unapproved")
    if policy_version not in {C010_POLICY_VERSION_V1, C010_POLICY_VERSION}:
        raise StateModelSetError("C-010 policy manifest version is unsupported")
    dates = _c010_require_ordered_strings(value.get("receipt_trading_dates"), label="C-010 receipt trading dates")
    try:
        parsed_dates = tuple(date.fromisoformat(item) for item in dates)
    except ValueError as exc:
        raise StateModelSetError("C-010 receipt trading dates are invalid") from exc
    try:
        train_start = date.fromisoformat(str(value.get("train_start") or ""))
        train_end = date.fromisoformat(str(value.get("train_end") or ""))
    except ValueError as exc:
        raise StateModelSetError("C-010 policy train window is invalid") from exc
    train_trading_dates = tuple(
        trade_date.isoformat() for trade_date in parsed_dates if train_start <= trade_date <= train_end
    )
    producer_commit = str(value.get("producer_commit") or "")
    source_hash_fields = (
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "calendar_manifest_hash",
        "security_identity_manifest_sha256",
        "provider_absence_manifest_sha256",
    )
    expected_feature_order = {
        "legacy_covfix": list(BASE_FEATURES),
        "autocycle_all_core": list(ALL_CORE_FEATURES),
    }
    if (
        value.get("formula_version") != C010_FORMULA_VERSION
        or len(producer_commit) != 40
        or any(character not in "0123456789abcdef" for character in producer_commit.lower())
        or value.get("receipt_trading_date_count") != len(dates)
        or value.get("receipt_trading_date_sha256") != canonical_sha256(list(dates))
        or value.get("contributor_min_availability") != MIN_COVERAGE
        or value.get("domain_min_count_coverage") != MIN_COVERAGE
        or value.get("domain_min_weight_coverage") != MIN_COVERAGE
        or value.get("feature_cross_section_min_coverage") != MIN_COVERAGE
        or tuple(value.get("moneyflow_mandatory_fields") or ()) != MONEYFLOW_STOCK_FIELDS
        or value.get("feature_order_by_family") != expected_feature_order
        or value.get("feature_order_sha256") != canonical_sha256(expected_feature_order)
        or any(not _c010_valid_sha256(value.get(field)) for field in source_hash_fields)
        or value.get("pit_universe_changed") is not False
        or value.get("selection_universe_changed") is not False
        or value.get("runtime_prediction_eligibility_changed") is not False
        or train_start != C010_APPROVED_TRAIN_START
        or train_end != C010_APPROVED_TRAIN_END
        or len(train_trading_dates) != C010_APPROVED_TRAIN_TRADING_DATE_COUNT
        or canonical_sha256(list(train_trading_dates)) != C010_APPROVED_TRAIN_TRADING_DATE_SHA256
    ):
        raise StateModelSetError("C-010 policy manifest fixed contract is invalid")
    if tuple(sorted(parsed_dates)) != parsed_dates or len(set(parsed_dates)) != len(parsed_dates):
        raise StateModelSetError("C-010 receipt trading dates are not strictly increasing")
    _validate_c010_feature_definitions(value, policy_version=policy_version)
    ledger, excluded = _validate_c010_eligibility_receipt(
        value.get("eligibility_receipt"), policy_version=policy_version
    )
    if (
        value["eligibility_receipt"].get("train_start") != train_start.isoformat()
        or value["eligibility_receipt"].get("train_end") != train_end.isoformat()
        or value["eligibility_receipt"].get("pit_universe_changed") is not False
        or value["eligibility_receipt"].get("selection_universe_changed") is not False
        or value["eligibility_receipt"].get("runtime_prediction_eligibility_changed") is not False
        or value.get("eligibility_receipt_sha256") != value["eligibility_receipt"].get("receipt_sha256")
        or value.get("eligibility_entry_count") != len(ledger)
        or value.get("contributor_ledger") != ledger
        or value.get("contributor_ledger_sha256") != canonical_sha256(ledger)
        or value.get("excluded_moneyflow_symbols") != excluded
        or value.get("excluded_moneyflow_symbol_sha256") != canonical_sha256(excluded)
    ):
        raise StateModelSetError("C-010 policy contributor ledger identity is invalid")
    if policy_version == C010_POLICY_VERSION:
        opportunity = validate_c010_expected_opportunity_receipt(value.get("expected_opportunity_receipt"))
        partition = validate_c010_provider_absence_domain_partition(value.get("provider_absence_partition_receipt"))
        partition_provider_authority = partition["provider_absence_manifest_identity"]["authority"]
        partition_resolver_authority = partition["security_resolver_identity"]["authority"]
        partition_sw_authority = partition["sw_mapping_classify_identity"]["authority"]
        opportunity_authority_hashes = {str(item["identity_sha256"]) for item in opportunity["authority_identities"]}
        expected_opportunity_authority_hashes = {
            str(partition[field]["identity_sha256"])
            for field in (
                "security_resolver_identity",
                "pit_authority_identity",
                "price_source_identity",
                "sw_mapping_classify_identity",
            )
        }
        if (
            value.get("expected_opportunity_receipt_sha256") != opportunity.get("receipt_sha256")
            or value.get("provider_absence_partition_receipt_sha256") != partition.get("receipt_sha256")
            or value["eligibility_receipt"].get("expected_opportunity_receipt_sha256")
            != opportunity.get("receipt_sha256")
            or value["eligibility_receipt"].get("provider_absence_partition_receipt_sha256")
            != partition.get("receipt_sha256")
            or partition_provider_authority.get("manifest_sha256") != value.get("provider_absence_manifest_sha256")
            or partition_resolver_authority.get("manifest_sha256") != value.get("security_identity_manifest_sha256")
            or canonical_sha256(dict(partition_sw_authority)) != value.get("mapping_manifest_hash")
            or opportunity_authority_hashes != expected_opportunity_authority_hashes
        ):
            raise StateModelSetError("C-010 policy domain-partition authority identity is invalid")
    l1_codes = _validate_c010_cross_section_receipt(
        value.get("l1_cross_section_receipt"), level="L1", dates=dates, policy_version=policy_version
    )
    l2_codes = _validate_c010_cross_section_receipt(
        value.get("l2_cross_section_receipt"), level="L2", dates=dates, policy_version=policy_version
    )
    if value.get("l1_cross_section_receipt_sha256") != value["l1_cross_section_receipt"].get(
        "receipt_sha256"
    ) or value.get("l2_cross_section_receipt_sha256") != value["l2_cross_section_receipt"].get("receipt_sha256"):
        raise StateModelSetError("C-010 policy cross-section receipt identity is invalid")
    _validate_c010_domain_receipt_set(
        value.get("aggregate_receipt"),
        dates=dates,
        level_codes={"L1": l1_codes, "L2": l2_codes},
        moneyflow_eligibility={
            str(entry["canonical_ts_code"]): entry.get("moneyflow_contributor_eligible") is True for entry in ledger
        },
    )
    if value.get("aggregate_receipt_sha256") != value["aggregate_receipt"].get("receipt_sha256"):
        raise StateModelSetError("C-010 policy aggregate receipt identity is invalid")
    circ_mv = value.get("causal_circ_mv_identity")
    if (
        not isinstance(circ_mv, Mapping)
        or set(circ_mv) != {"L1", "L2"}
        or any(
            not isinstance(item, Mapping) or any(not field_value for field_value in item.values())
            for item in circ_mv.values()
        )
        or value.get("causal_circ_mv_identity_sha256") != canonical_sha256(dict(circ_mv))
    ):
        raise StateModelSetError("C-010 causal circ-mv identity is invalid")
    return dict(value)


def build_c010_feature_domain_panel(
    aggregates: Sequence[FeatureDomainDailyAggregate],
    *,
    trading_dates: Sequence[date],
    csi300_returns: Mapping[date, float],
    expected_sector_count: int = 31,
    direct_sector_level: str = "L1",
    diagnostic_only: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Build the approved C-010 formula-v2 panel and immutable per-feature receipts."""

    panel, feature_definition = build_l1_feature_panel(
        aggregates,
        trading_dates=trading_dates,
        csi300_returns=csi300_returns,
        expected_sector_count=expected_sector_count,
        direct_sector_level=direct_sector_level,
    )
    required_domain_columns = {
        "moneyflow_amount",
        "moneyflow_domain_status",
        "l1_volume",
        "l1_range_ratio",
        "l1_return",
        "breadth_5d",
    }
    missing_domain_columns = sorted(required_domain_columns - set(panel.columns))
    if missing_domain_columns:
        raise StateModelSetError(f"C-010 feature-domain panel lacks columns: {missing_domain_columns}")
    codes = tuple(sorted(str(value) for value in panel.index.get_level_values("l1_code").unique()))
    calendar = tuple(pd.Timestamp(value) for value in trading_dates)
    if (direct_sector_level, expected_sector_count) not in {("L1", 31), ("L2", 131)} or len(
        codes
    ) != expected_sector_count:
        raise StateModelSetError("C-010 feature-domain panel has an invalid canonical sector set")

    moneyflow_available = panel["moneyflow_domain_status"] == "available"
    moneyflow_denominator = panel["moneyflow_amount"].where(moneyflow_available)
    moneyflow_denominator = moneyflow_denominator.where(moneyflow_denominator > 0)
    panel["net_mf_ratio"] = (panel["net_mf_amount"] / moneyflow_denominator).where(moneyflow_available)
    panel["elg_net_mf_ratio"] = ((panel["buy_elg_amount"] - panel["sell_elg_amount"]) / moneyflow_denominator).where(
        moneyflow_available
    )
    panel["small_net_ratio"] = ((panel["buy_sm_amount"] - panel["sell_sm_amount"]) / moneyflow_denominator).where(
        moneyflow_available
    )
    by_sector = panel.groupby(level="l1_code", group_keys=False)
    panel["sf_mf_net_ratio_std_5d_neg"] = (
        -by_sector["net_mf_ratio"].rolling(5, min_periods=5).std(ddof=1).droplevel(0)
    ).where(moneyflow_available)
    panel["sf_small_net_ratio_5d"] = (by_sector["small_net_ratio"].rolling(5, min_periods=3).mean().droplevel(0)).where(
        moneyflow_available
    )

    vol20 = by_sector["l1_return"].rolling(20, min_periods=10).std(ddof=1).droplevel(0)
    inputs = {
        "volume_ratio": panel["l1_volume"],
        "sf_range_vs_market_10d": panel["l1_range_ratio"],
        "sf_vol_vs_market_20d": vol20,
        "sf_excess_breadth_5d": panel["breadth_5d"],
    }
    operators = dict(C010_CROSS_SECTION_OPERATORS)
    outputs = {name: pd.Series(np.nan, index=panel.index, dtype="float64") for name in inputs}
    pre_masks = {name: pd.Series(False, index=panel.index, dtype="bool") for name in inputs}
    entry_state: dict[tuple[str, pd.Timestamp], dict[str, Any]] = {}

    for feature_name, source in inputs.items():
        for timestamp in calendar:
            day = source.xs(timestamp, level="trade_date").reindex(codes)
            valid_codes: list[str] = []
            for code, raw_value in day.items():
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(value):
                    continue
                if feature_name in {"volume_ratio", "sf_range_vs_market_10d", "sf_vol_vs_market_20d"} and value < 0:
                    continue
                if feature_name == "sf_excess_breadth_5d" and not 0 <= value <= 1:
                    continue
                valid_codes.append(str(code))
            valid = tuple(sorted(valid_codes))
            missing = tuple(code for code in codes if code not in set(valid))
            coverage = len(valid) / expected_sector_count
            state = {
                "status": "accepted",
                "reason_code": None,
                "valid": valid,
                "missing": missing,
                "coverage": float(coverage),
                "reference": None,
                "pre_mask": (),
            }
            if (10 * len(valid)) < (9 * expected_sector_count):
                state["status"] = "coverage_insufficient"
                state["reason_code"] = "hmm_risk_c010_feature_cross_section_coverage_insufficient"
                entry_state[(feature_name, timestamp)] = state
                continue
            values = [float(day.loc[code]) for code in valid]
            if feature_name == "volume_ratio":
                reference = float(math.fsum(values))
            elif feature_name in {"sf_range_vs_market_10d", "sf_vol_vs_market_20d"}:
                reference = float(np.median(np.asarray(values, dtype=np.float64)))
            else:
                reference = float(math.fsum(values) / len(values))
            state["reference"] = reference
            positive_required = feature_name != "sf_excess_breadth_5d"
            if not math.isfinite(reference) or (positive_required and reference <= 0):
                state["status"] = "reference_invalid"
                state["reason_code"] = "hmm_risk_c010_feature_cross_section_reference_invalid"
                entry_state[(feature_name, timestamp)] = state
                continue
            index = pd.MultiIndex.from_product([[timestamp], valid], names=panel.index.names)
            pre_masks[feature_name].loc[index] = True
            state["pre_mask"] = valid
            if feature_name == "sf_excess_breadth_5d":
                outputs[feature_name].loc[index] = [float(day.loc[code]) - reference for code in valid]
            else:
                outputs[feature_name].loc[index] = [float(day.loc[code]) / reference for code in valid]
            entry_state[(feature_name, timestamp)] = state

    range_daily = outputs["sf_range_vs_market_10d"]
    range_rolling = (
        range_daily.groupby(level="l1_code", group_keys=False).rolling(10, min_periods=5).mean().droplevel(0)
    )
    outputs["sf_range_vs_market_10d"] = range_rolling.where(pre_masks["sf_range_vs_market_10d"])

    receipts: list[dict[str, Any]] = []
    for feature_name in inputs:
        output = outputs[feature_name]
        illegal_mask = output.notna() & ~pre_masks[feature_name]
        if bool(illegal_mask.any()):
            raise StateModelSetError(
                f"hmm_risk_c010_feature_cross_section_mask_mismatch: {direct_sector_level}/{feature_name}"
            )
        for timestamp in calendar:
            state = entry_state[(feature_name, timestamp)]
            day_output = output.xs(timestamp, level="trade_date").reindex(codes)
            finite_output_codes = {
                str(code) for code, value in day_output.items() if pd.notna(value) and math.isfinite(float(value))
            }
            non_finite_output = [
                str(code) for code, value in day_output.items() if pd.notna(value) and not math.isfinite(float(value))
            ]
            if non_finite_output:
                index = pd.MultiIndex.from_product([[timestamp], codes], names=panel.index.names)
                output.loc[index] = np.nan
                finite_output_codes.clear()
                state["status"] = "output_non_finite"
                state["reason_code"] = "hmm_risk_c010_feature_cross_section_output_non_finite"
            entry = {
                "feature_name": feature_name,
                "trade_date": timestamp.date().isoformat(),
                "direct_sector_level": direct_sector_level,
                "operator": operators[feature_name],
                "source_domain": "price",
                "expected_sector_count": expected_sector_count,
                "expected_sector_sha256": canonical_sha256(list(codes)),
                "valid_sector_count": len(state["valid"]),
                "valid_sector_codes": list(state["valid"]),
                "valid_sector_sha256": canonical_sha256(list(state["valid"])),
                "missing_sector_codes": list(state["missing"]),
                "missing_sector_sha256": canonical_sha256(list(state["missing"])),
                "feature_cross_section_coverage": state["coverage"],
                "reference_value": state["reference"],
                "pre_mask_sha256": _cross_section_mask_hash(codes, set(state["pre_mask"])),
                "post_mask_sha256": _cross_section_mask_hash(codes, finite_output_codes),
                "post_mask_subset_of_pre_mask": finite_output_codes.issubset(set(state["pre_mask"])),
                "status": state["status"],
                "reason_code": state["reason_code"],
            }
            receipts.append({**entry, "entry_sha256": canonical_sha256(entry)})
        panel[feature_name] = output

    panel = panel.replace([np.inf, -np.inf], np.nan)
    feature_definition.update(
        {
            "schema_version": C010_FORMULA_VERSION,
            "feature_domain_policy_version": C010_POLICY_VERSION,
            "diagnostic_only": diagnostic_only,
            "cross_section_contract": "feature_domain_coverage_v1",
            "cross_section_min_coverage": MIN_COVERAGE,
            "cross_section_min_valid_sector_count": 28 if expected_sector_count == 31 else 118,
            "moneyflow_mandatory_fields": list(MONEYFLOW_STOCK_FIELDS),
            "moneyflow_denominator_by_feature": dict(C010_MONEYFLOW_DENOMINATOR_BY_FEATURE),
            "cross_section_operator_by_feature": operators,
            "formula_diff_by_feature": {
                feature: dict(formula) for feature, formula in C010_FORMULA_DIFF_BY_FEATURE.items()
            },
            "moneyflow_rolling_post_mask_required": True,
            "range_cross_section_rolling_post_mask_required": True,
        }
    )
    evidence_body = {
        "schema_version": C010_CROSS_SECTION_RECEIPT_VERSION,
        "formula_version": C010_FORMULA_VERSION,
        "feature_domain_policy_version": C010_POLICY_VERSION,
        "direct_sector_level": direct_sector_level,
        "expected_sector_count": expected_sector_count,
        "expected_sector_codes": list(codes),
        "expected_sector_sha256": canonical_sha256(list(codes)),
        "entry_count": len(receipts),
        "entries": receipts,
        "diagnostic_only": diagnostic_only,
    }
    evidence = {**evidence_body, "receipt_sha256": canonical_sha256(evidence_body)}
    return panel, feature_definition, evidence


def _future_sum(series: pd.Series, horizon: int) -> pd.Series:
    pieces = [series.groupby(level="l1_code").shift(-offset) for offset in range(1, horizon + 1)]
    return pd.concat(pieces, axis=1).sum(axis=1, min_count=horizon)


def build_legacy_dense_diagnostic_series(
    panel: pd.DataFrame,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    validation_start: date,
    validation_end: date,
    constituent_manifest_by_l1: Mapping[str, Mapping[str, Any]],
    expected_sector_count: int = 31,
    direct_sector_level: str = "L1",
    frozen_input_identity: Mapping[str, Any] | None = None,
) -> dict[str, L1TrainingSeries]:
    """Reproduce the immutable dense input contract used only by historical C-008 diagnostics.

    Formal D6 execution must use :func:`build_l1_training_series`; this dedicated constructor
    prevents the historical diagnostic CLIs from consuming a calendar carrier while still
    expecting row-aligned dense utility arrays.
    """

    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("feature_names is not an approved family")
    work = panel.copy()
    future_components = {horizon: _future_sum(work["daily_excess"], horizon) for horizon in (5, 10, 20)}
    for horizon, values in future_components.items():
        work[f"validation_excess_return_{horizon}d"] = values
    work["validation_future_utility"] = (
        0.35 * future_components[5] + 0.35 * future_components[10] + 0.30 * future_components[20]
    )
    output: dict[str, L1TrainingSeries] = {}
    for code in sorted(work.index.get_level_values("l1_code").unique()):
        sector = work.xs(code, level="l1_code")
        sector_dates = sector.index.date
        train = sector.loc[
            (sector_dates >= train_start) & (sector_dates <= train_end),
            list(features),
        ].dropna()
        validation = sector.loc[
            (sector_dates >= validation_start) & (sector_dates <= validation_end),
            [
                *features,
                "validation_future_utility",
                "validation_excess_return_5d",
                "validation_excess_return_10d",
                "validation_excess_return_20d",
            ],
        ].dropna()
        if len(train) < MIN_TRAINING_ROWS or len(validation) < 30:
            raise StateModelSetError(
                f"{code} diagnostic observation coverage is insufficient "
                f"train={len(train)} validation={len(validation)}"
            )
        constituent = constituent_manifest_by_l1.get(str(code))
        if not isinstance(constituent, Mapping):
            raise StateModelSetError(f"{code} constituent manifest is missing")
        l2_codes = tuple(sorted(str(item) for item in constituent.get("l2_codes") or ()))
        if not l2_codes:
            raise StateModelSetError(f"{code} constituent manifest has no L2 codes")
        validation_dates = [item.date().isoformat() for item in validation.index]
        utility_components = {
            "excess_return_5d": validation["validation_excess_return_5d"].to_numpy(dtype=np.float64),
            "excess_return_10d": validation["validation_excess_return_10d"].to_numpy(dtype=np.float64),
            "excess_return_20d": validation["validation_excess_return_20d"].to_numpy(dtype=np.float64),
        }
        validation_observations = validation.loc[:, list(features)].to_numpy(dtype=np.float64)
        validation_utility = validation["validation_future_utility"].to_numpy(dtype=np.float64)
        validation_input_manifest = {
            **dict(frozen_input_identity or {}),
            "schema_version": "hmm_risk_d6_frozen_input_manifest_v1",
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "validation_dates": validation_dates,
            "validation_dates_sha256": canonical_sha256(validation_dates),
            "validation_observation_sha256": canonical_sha256(validation_observations.tolist()),
            "utility_component_sha256": {
                name: canonical_sha256(values.tolist()) for name, values in sorted(utility_components.items())
            },
            "combined_utility_sha256": canonical_sha256(validation_utility.tolist()),
            "source_cutoff": "2025-04-30",
            "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
            "benchmark_identity": "000300.SH",
        }
        output[str(code)] = L1TrainingSeries(
            sector_code=str(code),
            sector_name=str(sector["l1_name"].dropna().iloc[-1]),
            train_observations=train.to_numpy(dtype=np.float64),
            train_dates=tuple(item.date() for item in train.index),
            validation_observations=validation_observations,
            validation_dates=tuple(item.date() for item in validation.index),
            validation_future_utility=validation_utility,
            pit_l2_constituents=l2_codes,
            pit_constituent_manifest_hash=canonical_sha256(constituent),
            observation_manifest_hash=canonical_sha256(
                {
                    "observation_version": OBSERVATION_VERSION,
                    "direct_sector_level": direct_sector_level,
                    "sector_code": str(code),
                    "feature_names": list(features),
                    "train_dates": [item.date().isoformat() for item in train.index],
                    "validation_dates": validation_dates,
                    "train_sha256": canonical_sha256(train.to_numpy(dtype=np.float64).tolist()),
                    "validation_sha256": canonical_sha256(validation.to_numpy(dtype=np.float64).tolist()),
                }
            ),
            validation_future_components=utility_components,
            validation_utility_source_cutoff=date(2025, 4, 30),
            validation_utility_formula_version="hmm_risk_hard_future_excess_035_035_030_v1",
            validation_input_manifest=validation_input_manifest,
        )
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("diagnostic series requires an approved L1/31 or L2/131 contract")
    if len(output) != expected_sector_count:
        raise StateModelSetError(
            f"diagnostic series requires {expected_sector_count} direct {direct_sector_level} sectors; "
            f"actual={len(output)}"
        )
    return output


def build_l1_training_series(
    panel: pd.DataFrame,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    validation_start: date,
    validation_end: date,
    constituent_manifest_by_l1: Mapping[str, Mapping[str, Any]],
    expected_sector_count: int = 31,
    direct_sector_level: str = "L1",
    frozen_input_identity: Mapping[str, Any] | None = None,
    validation_calendar_dates: Sequence[date] | None = None,
) -> dict[str, L1TrainingSeries]:
    """Freeze train data and a full-calendar D6 validation carrier without NA compression."""

    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("feature_names is not an approved family")
    work = panel.copy()
    future_components = {horizon: _future_sum(work["daily_excess"], horizon) for horizon in (5, 10, 20)}
    for horizon, values in future_components.items():
        work[f"validation_excess_return_{horizon}d"] = values
    utility = 0.35 * future_components[5] + 0.35 * future_components[10] + 0.30 * future_components[20]
    work["validation_future_utility"] = utility
    if validation_calendar_dates is None:
        calendar_dates = tuple(
            sorted(
                {
                    timestamp.date()
                    for timestamp in work.index.get_level_values(0)
                    if validation_start <= timestamp.date() <= validation_end
                }
            )
        )
    else:
        calendar_dates = tuple(validation_calendar_dates)
    if (
        not calendar_dates
        or any(not isinstance(value, date) for value in calendar_dates)
        or tuple(sorted(calendar_dates)) != calendar_dates
        or len(set(calendar_dates)) != len(calendar_dates)
        or calendar_dates[0] != validation_start
        or calendar_dates[-1] != validation_end
    ):
        raise StateModelSetError("D6 validation calendar authority is invalid")
    source_identities = {
        field: str((frozen_input_identity or {}).get(field) or "")
        for field in (
            "dataset_manifest_hash",
            "mapping_manifest_hash",
            "calendar_manifest_hash",
            "l2_stock_fact_manifest_hash",
            "feature_domain_policy_sha256",
        )
    }
    if any(len(value) != 64 for value in source_identities.values()):
        raise StateModelSetError("D6 validation source identities are incomplete")
    calendar_index = pd.DatetimeIndex(calendar_dates)
    output: dict[str, L1TrainingSeries] = {}
    for code in sorted(work.index.get_level_values("l1_code").unique()):
        sector = work.xs(code, level="l1_code")
        sector_dates = sector.index.date
        train = sector.loc[
            (sector_dates >= train_start) & (sector_dates <= train_end),
            list(features),
        ].dropna()
        validation = sector.reindex(calendar_index).loc[
            :,
            [
                *features,
                "validation_future_utility",
                "validation_excess_return_5d",
                "validation_excess_return_10d",
                "validation_excess_return_20d",
            ],
        ]
        if len(train) < MIN_TRAINING_ROWS:
            raise StateModelSetError(f"{code} train observation coverage is insufficient train={len(train)}")
        constituent = constituent_manifest_by_l1.get(str(code))
        if not isinstance(constituent, Mapping):
            raise StateModelSetError(f"{code} constituent manifest is missing")
        l2_codes = tuple(sorted(str(item) for item in constituent.get("l2_codes") or ()))
        if not l2_codes:
            raise StateModelSetError(f"{code} constituent manifest has no L2 codes")
        observation_matrix = validation.loc[:, list(features)].to_numpy(dtype=np.float64)
        observation_mask = tuple(bool(value) for value in np.isfinite(observation_matrix).all(axis=1))
        observation_positions = tuple(index for index, value in enumerate(observation_mask) if value)
        observation_values = np.ascontiguousarray(
            observation_matrix[np.asarray(observation_mask, dtype=bool)], dtype=np.float64
        )
        component_columns = {
            "excess_return_5d": "validation_excess_return_5d",
            "excess_return_10d": "validation_excess_return_10d",
            "excess_return_20d": "validation_excess_return_20d",
        }
        component_masks: dict[str, tuple[bool, ...]] = {}
        component_positions: dict[str, tuple[int, ...]] = {}
        component_values: dict[str, np.ndarray] = {}
        for name, column in sorted(component_columns.items()):
            dense = validation[column].to_numpy(dtype=np.float64)
            mask = tuple(bool(value) for value in np.isfinite(dense))
            positions = tuple(index for index, value in enumerate(mask) if value)
            component_masks[name] = mask
            component_positions[name] = positions
            component_values[name] = np.ascontiguousarray(dense[np.asarray(mask, dtype=bool)], dtype=np.float64)
        utility_dense = validation["validation_future_utility"].to_numpy(dtype=np.float64)
        utility_mask = tuple(
            bool(np.isfinite(utility_dense[index])) and all(component_masks[name][index] for name in component_columns)
            for index in range(len(calendar_dates))
        )
        utility_positions = tuple(index for index, value in enumerate(utility_mask) if value)
        combined_values = np.ascontiguousarray(utility_dense[np.asarray(utility_mask, dtype=bool)], dtype=np.float64)
        availability_ledger: list[dict[str, Any]] = []
        for position, calendar_day in enumerate(calendar_dates):
            missing_features = [
                name
                for feature_index, name in enumerate(features)
                if not np.isfinite(observation_matrix[position, feature_index])
            ]
            missing_components = [name for name in sorted(component_columns) if not component_masks[name][position]]
            observation_receipt = {
                "sector_code": str(code),
                "date": calendar_day.isoformat(),
                "feature_names": list(features),
                "missing_feature_names": missing_features,
                "available": observation_mask[position],
                "source_identities": source_identities,
            }
            utility_receipt = {
                "sector_code": str(code),
                "date": calendar_day.isoformat(),
                "component_names": sorted(component_columns),
                "missing_component_names": missing_components,
                "available": utility_mask[position],
                "source_identities": source_identities,
            }
            availability_ledger.append(
                {
                    "date": calendar_day.isoformat(),
                    "position": position,
                    "observation_available": observation_mask[position],
                    "utility_available": utility_mask[position],
                    "mode": "emission_update" if observation_mask[position] else "transition_only",
                    "evidence_included": bool(observation_mask[position] and utility_mask[position]),
                    "missing_feature_names": missing_features,
                    "missing_component_names": missing_components,
                    "observation_unavailable_reason_codes": (
                        [] if observation_mask[position] else ["hmm_risk_semantic_validation_observation_unavailable"]
                    ),
                    "utility_unavailable_reason_codes": (
                        [] if utility_mask[position] else ["hmm_risk_semantic_validation_utility_unavailable"]
                    ),
                    "observation_source_receipt": observation_receipt,
                    "observation_source_receipt_sha256": canonical_sha256(observation_receipt),
                    "utility_source_receipt": utility_receipt,
                    "utility_source_receipt_sha256": canonical_sha256(utility_receipt),
                }
            )
        carrier = D6ValidationCalendarSeries(
            calendar_dates=calendar_dates,
            feature_names=features,
            observation_available_mask=observation_mask,
            observation_available_positions=observation_positions,
            observation_values_f64=observation_values,
            component_available_masks=component_masks,
            component_available_positions=component_positions,
            component_values_f64=component_values,
            utility_available_mask=utility_mask,
            utility_available_positions=utility_positions,
            combined_utility_values_f64=combined_values,
            availability_ledger=tuple(availability_ledger),
            source_identities=source_identities,
        )
        carrier.validate(len(features))
        carrier_payload = carrier.payload()
        validation_input_manifest = {
            **source_identities,
            "schema_version": "hmm_risk_d6_frozen_input_manifest_v2",
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "calendar_carrier_schema_version": carrier.schema_version,
            "calendar_carrier_payload": carrier_payload,
            "calendar_carrier_sha256": carrier.carrier_sha256,
            "validation_calendar_sha256": canonical_sha256(carrier_payload["calendar_dates"]),
            "feature_names_sha256": canonical_sha256(list(features)),
            "observation_available_mask_sha256": canonical_sha256(list(observation_mask)),
            "observation_available_positions_sha256": canonical_sha256(list(observation_positions)),
            "observation_values_sha256": canonical_sha256(observation_values.tolist()),
            "utility_component_sha256": {
                name: canonical_sha256(component_values[name].tolist()) for name in sorted(component_values)
            },
            "component_available_mask_sha256": {
                name: canonical_sha256(list(component_masks[name])) for name in sorted(component_masks)
            },
            "component_available_positions_sha256": {
                name: canonical_sha256(list(component_positions[name])) for name in sorted(component_positions)
            },
            "utility_available_mask_sha256": canonical_sha256(list(utility_mask)),
            "utility_available_positions_sha256": canonical_sha256(list(utility_positions)),
            "combined_utility_sha256": canonical_sha256(combined_values.tolist()),
            "availability_ledger_sha256": canonical_sha256(availability_ledger),
            "source_cutoff": "2025-04-30",
            "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
            "benchmark_identity": "000300.SH",
        }
        output[str(code)] = L1TrainingSeries(
            sector_code=str(code),
            sector_name=str(sector["l1_name"].dropna().iloc[-1]),
            train_observations=train.to_numpy(dtype=np.float64),
            train_dates=tuple(item.date() for item in train.index),
            validation_observations=observation_values,
            validation_dates=tuple(calendar_dates[position] for position in observation_positions),
            validation_future_utility=np.empty((0,), dtype=np.float64),
            pit_l2_constituents=l2_codes,
            pit_constituent_manifest_hash=canonical_sha256(constituent),
            observation_manifest_hash=canonical_sha256(
                {
                    "observation_version": OBSERVATION_VERSION,
                    "direct_sector_level": direct_sector_level,
                    "sector_code": str(code),
                    "feature_names": list(features),
                    "train_dates": [item.date().isoformat() for item in train.index],
                    "validation_calendar_dates": [item.isoformat() for item in calendar_dates],
                    "train_sha256": canonical_sha256(train.to_numpy(dtype=np.float64).tolist()),
                    "validation_calendar_carrier_sha256": carrier.carrier_sha256,
                }
            ),
            validation_future_components={},
            validation_utility_source_cutoff=date(2025, 4, 30),
            validation_utility_formula_version="hmm_risk_hard_future_excess_035_035_030_v1",
            validation_input_manifest=validation_input_manifest,
            validation_calendar_series=carrier,
        )
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("training series requires an approved L1/31 or L2/131 contract")
    if len(output) != expected_sector_count:
        raise StateModelSetError(
            f"training series requires {expected_sector_count} direct {direct_sector_level} sectors; actual={len(output)}"
        )
    return output
