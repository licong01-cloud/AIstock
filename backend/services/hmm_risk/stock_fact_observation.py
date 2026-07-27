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
    L1TrainingSeries,
    StateModelSetError,
    canonical_sha256,
)


OBSERVATION_VERSION = "hmm_risk_l1_stock_fact_observation_v1"
FORMULA_VERSION = "hmm_risk_l1_sector_factor_formula_v1"
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
    ) -> None:
        super().__init__(message)
        self.trade_date = trade_date
        self.l1_code = l1_code
        self.count_coverage = count_coverage
        self.weight_coverage = weight_coverage
        self.missing_evidence = tuple(dict(item) for item in missing_evidence)


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
    """Diagnostic aggregate whose extra fields never enter formal source manifests."""

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
    moneyflow_excluded_symbols: frozenset[str],
) -> FeatureDomainDailyAggregate:
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
    price_complete_weight = sum(float(row["prev_circ_mv_cny"]) for row in price_complete)
    price_weight_coverage = price_complete_weight / expected_weight
    if price_count_coverage < min_coverage or price_weight_coverage < min_coverage:
        raise ObservationCoverageError(
            f"{l1_code}/{trade_date} price-domain stock coverage is insufficient "
            f"count={price_count_coverage:.6f} weight={price_weight_coverage:.6f}",
            trade_date=trade_date,
            l1_code=l1_code,
            count_coverage=float(price_count_coverage),
            weight_coverage=float(price_weight_coverage),
            missing_evidence=price_missing,
        )

    moneyflow_expected = [row for row in expected if str(row.get("symbol") or "") not in moneyflow_excluded_symbols]
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
        moneyflow_count_coverage = 0.0
        moneyflow_weight_coverage = 0.0
        moneyflow_status = "structurally_unavailable"
    else:
        moneyflow_expected_weight = sum(float(row["prev_circ_mv_cny"]) for row in moneyflow_expected)
        moneyflow_complete_weight = sum(float(row["prev_circ_mv_cny"]) for row in moneyflow_complete)
        moneyflow_count_coverage = len(moneyflow_complete) / len(moneyflow_expected)
        moneyflow_weight_coverage = moneyflow_complete_weight / moneyflow_expected_weight
        moneyflow_status = (
            "available"
            if moneyflow_count_coverage >= min_coverage and moneyflow_weight_coverage >= min_coverage
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
            field: float(sum(float(row[field]) for row in moneyflow_complete)) for field in MONEYFLOW_STOCK_FIELDS
        }
        moneyflow_amount = float(sum(float(row["amount_cny"]) for row in moneyflow_complete))
        if not all(math.isfinite(value) for value in (*moneyflow_values.values(), moneyflow_amount)):
            raise StateModelSetError(f"{l1_code}/{trade_date} feature-domain moneyflow aggregate is non-finite")
    excluded_evidence = [
        {
            "symbol": str(row.get("symbol") or ""),
            "fields": list(MONEYFLOW_STOCK_FIELDS),
            "moneyflow_eligibility_status": "train_frozen_excluded",
        }
        for row in expected
        if str(row.get("symbol") or "") in moneyflow_excluded_symbols
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
    )


def aggregate_l1_day(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_coverage: float = MIN_COVERAGE,
    moneyflow_excluded_symbols: frozenset[str] | None = None,
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
    if moneyflow_excluded_symbols is not None:
        return _aggregate_feature_domain_day(
            rows,
            trade_date=trade_date,
            l1_code=l1_code,
            l1_name=l1_name,
            min_coverage=min_coverage,
            moneyflow_excluded_symbols=moneyflow_excluded_symbols,
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


def _future_sum(series: pd.Series, horizon: int) -> pd.Series:
    pieces = [series.groupby(level="l1_code").shift(-offset) for offset in range(1, horizon + 1)]
    return pd.concat(pieces, axis=1).sum(axis=1, min_count=horizon)


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
) -> dict[str, L1TrainingSeries]:
    """Freeze train/validation matrices and validation-only future utility."""

    features = tuple(str(item) for item in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("feature_names is not an approved family")
    work = panel.copy()
    future_components = {horizon: _future_sum(work["daily_excess"], horizon) for horizon in (5, 10, 20)}
    for horizon, values in future_components.items():
        work[f"validation_excess_return_{horizon}d"] = values
    utility = 0.35 * future_components[5] + 0.35 * future_components[10] + 0.30 * future_components[20]
    work["validation_future_utility"] = utility
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
                f"{code} observation coverage is insufficient train={len(train)} validation={len(validation)}"
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
        validation_input_manifest = {
            **dict(frozen_input_identity or {}),
            "schema_version": "hmm_risk_d6_frozen_input_manifest_v1",
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "validation_dates": validation_dates,
            "validation_dates_sha256": canonical_sha256(validation_dates),
            "validation_observation_sha256": canonical_sha256(
                validation.loc[:, list(features)].to_numpy(dtype=np.float64).tolist()
            ),
            "utility_component_sha256": {
                name: canonical_sha256(values.tolist()) for name, values in sorted(utility_components.items())
            },
            "combined_utility_sha256": canonical_sha256(
                validation["validation_future_utility"].to_numpy(dtype=np.float64).tolist()
            ),
            "source_cutoff": "2025-04-30",
            "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
            "benchmark_identity": "000300.SH",
        }
        output[str(code)] = L1TrainingSeries(
            sector_code=str(code),
            sector_name=str(sector["l1_name"].dropna().iloc[-1]),
            train_observations=train.to_numpy(dtype=np.float64),
            train_dates=tuple(item.date() for item in train.index),
            validation_observations=validation.loc[:, list(features)].to_numpy(dtype=np.float64),
            validation_dates=tuple(item.date() for item in validation.index),
            validation_future_utility=validation["validation_future_utility"].to_numpy(dtype=np.float64),
            pit_l2_constituents=l2_codes,
            pit_constituent_manifest_hash=canonical_sha256(constituent),
            observation_manifest_hash=canonical_sha256(
                {
                    "observation_version": OBSERVATION_VERSION,
                    "direct_sector_level": direct_sector_level,
                    "sector_code": str(code),
                    "feature_names": list(features),
                    "train_dates": [item.date().isoformat() for item in train.index],
                    "validation_dates": [item.date().isoformat() for item in validation.index],
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
        raise StateModelSetError("training series requires an approved L1/31 or L2/131 contract")
    if len(output) != expected_sector_count:
        raise StateModelSetError(
            f"training series requires {expected_sector_count} direct {direct_sector_level} sectors; actual={len(output)}"
        )
    return output
