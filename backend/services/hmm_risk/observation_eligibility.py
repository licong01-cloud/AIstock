"""Train-only C-010 observation eligibility and feature-mask diagnostics."""

from __future__ import annotations

import heapq
import itertools
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from .provider_absence import ProviderAbsenceEvidence
from .state_model_set import StateModelSetError, canonical_sha256
from .stock_fact_observation import (
    FeatureDomainDailyAggregate,
    MIN_COVERAGE,
    MIN_TRAINING_ROWS,
    L1DailyAggregate,
    ObservationCoverageError,
    aggregate_l1_day,
)

ELIGIBILITY_SCHEMA = "hmm_risk_c010_train_observation_eligibility_v1"
FEATURE_MASK_SCHEMA = "hmm_risk_c010_feature_mask_candidate_set_v1"
EXPECTED_OPPORTUNITY_CONTRACT = "hmm_risk_c010_expected_opportunity_dates_v1"

MONEYFLOW_FEATURES = frozenset(
    {
        "net_mf_ratio",
        "elg_net_mf_ratio",
        "sf_mf_net_ratio_std_5d_neg",
        "sf_small_net_ratio_5d",
    }
)


def _domain_receipt(aggregate: L1DailyAggregate, *, direct_sector_level: str) -> dict[str, Any]:
    if not isinstance(aggregate, FeatureDomainDailyAggregate):
        raise StateModelSetError("C-010 feature-domain aggregate receipt source is invalid")
    moneyflow_reason = {
        "available": None,
        "structurally_unavailable": "hmm_risk_c010_moneyflow_domain_structurally_unavailable",
        "coverage_insufficient": "hmm_risk_c010_moneyflow_domain_coverage_insufficient",
        "denominator_invalid": "hmm_risk_c010_moneyflow_denominator_invalid",
    }.get(aggregate.moneyflow_domain_status)
    if aggregate.moneyflow_domain_status not in {
        "available",
        "structurally_unavailable",
        "coverage_insufficient",
        "denominator_invalid",
    }:
        raise StateModelSetError("C-010 moneyflow domain status is invalid")
    body = {
        "direct_sector_level": direct_sector_level,
        "sector_code": aggregate.l1_code,
        "trade_date": aggregate.trade_date.isoformat(),
        "price_domain_status": "available",
        "price_domain_reason_code": None,
        "price_expected_symbols": list(aggregate.price_expected_symbols),
        "price_expected_symbol_sha256": canonical_sha256(list(aggregate.price_expected_symbols)),
        "price_complete_symbols": list(aggregate.price_complete_symbols),
        "price_complete_symbol_sha256": canonical_sha256(list(aggregate.price_complete_symbols)),
        "price_count_coverage": aggregate.count_coverage,
        "price_expected_weight": aggregate.price_expected_weight,
        "price_complete_weight": aggregate.price_complete_weight,
        "price_weight_coverage": aggregate.weight_coverage,
        "moneyflow_domain_status": aggregate.moneyflow_domain_status,
        "moneyflow_domain_reason_code": moneyflow_reason,
        "moneyflow_expected_symbols": list(aggregate.moneyflow_expected_symbols),
        "moneyflow_expected_symbol_sha256": canonical_sha256(list(aggregate.moneyflow_expected_symbols)),
        "moneyflow_complete_symbols": list(aggregate.moneyflow_complete_symbols),
        "moneyflow_complete_symbol_sha256": canonical_sha256(list(aggregate.moneyflow_complete_symbols)),
        "moneyflow_count_coverage": aggregate.moneyflow_count_coverage,
        "moneyflow_expected_weight": aggregate.moneyflow_expected_weight,
        "moneyflow_complete_weight": aggregate.moneyflow_complete_weight,
        "moneyflow_weight_coverage": aggregate.moneyflow_weight_coverage,
        "moneyflow_contributor_amount": aggregate.moneyflow_amount,
        "moneyflow_excluded_symbols": list(aggregate.moneyflow_excluded_symbols),
        "missing_evidence": [dict(value) for value in aggregate.missing_evidence],
    }
    return {**body, "entry_sha256": canonical_sha256(body)}


def _invalid_price_domain_receipt(
    error: ObservationCoverageError,
    rows: Sequence[Mapping[str, Any]],
    *,
    direct_sector_level: str,
) -> dict[str, Any]:
    expected = tuple(sorted(str(row.get("symbol") or "") for row in rows if not bool(row.get("is_suspended"))))
    missing = {str(value.get("symbol") or "") for value in error.missing_evidence}
    complete = tuple(symbol for symbol in expected if symbol not in missing)
    row_by_symbol = {str(row.get("symbol") or ""): row for row in rows if not bool(row.get("is_suspended"))}
    expected_weights: list[float] = []
    complete_weights: list[float] = []
    weights_valid = True
    for symbol in expected:
        try:
            weight = float(row_by_symbol[symbol].get("prev_circ_mv_cny"))
        except (TypeError, ValueError):
            weights_valid = False
            break
        if not math.isfinite(weight) or weight <= 0:
            weights_valid = False
            break
        expected_weights.append(weight)
        if symbol in complete:
            complete_weights.append(weight)
    expected_weight = float(math.fsum(expected_weights)) if weights_valid else None
    complete_weight = float(math.fsum(complete_weights)) if weights_valid else None
    return {
        "direct_sector_level": direct_sector_level,
        "trade_date": error.trade_date.isoformat(),
        "sector_code": error.l1_code,
        "price_domain_status": "invalid",
        "price_domain_reason_code": error.reason_code,
        "price_expected_symbols": list(expected),
        "price_expected_symbol_sha256": canonical_sha256(list(expected)),
        "price_complete_symbols": list(complete),
        "price_complete_symbol_sha256": canonical_sha256(list(complete)),
        "price_count_coverage": error.count_coverage,
        "price_expected_weight": expected_weight,
        "price_complete_weight": complete_weight,
        "price_weight_coverage": error.weight_coverage,
        "missing_evidence": [dict(value) for value in error.missing_evidence],
    }


@dataclass(frozen=True)
class ContributorEligibility:
    canonical_ts_code: str
    expected_opportunity_count: int
    expected_opportunity_date_sha256: str
    provider_absence_count: int
    availability_ratio: float
    moneyflow_contributor_eligible: bool
    provider_absence_key_sha256: str

    def evidence(self) -> dict[str, Any]:
        body = {
            "canonical_ts_code": self.canonical_ts_code,
            "expected_opportunity_count": self.expected_opportunity_count,
            "expected_opportunity_contract": EXPECTED_OPPORTUNITY_CONTRACT,
            "expected_opportunity_date_sha256": self.expected_opportunity_date_sha256,
            "provider_absence_count": self.provider_absence_count,
            "availability_ratio": self.availability_ratio,
            "moneyflow_contributor_eligible": self.moneyflow_contributor_eligible,
            "provider_absence_key_sha256": self.provider_absence_key_sha256,
        }
        return {**body, "entry_sha256": canonical_sha256(body)}


@dataclass(frozen=True)
class ObservationEligibility:
    train_start: date
    train_end: date
    minimum_availability_ratio: float
    entries: tuple[ContributorEligibility, ...]

    @property
    def excluded_moneyflow_symbols(self) -> frozenset[str]:
        return frozenset(entry.canonical_ts_code for entry in self.entries if not entry.moneyflow_contributor_eligible)

    @property
    def moneyflow_contributor_eligibility(self) -> dict[str, bool]:
        return {entry.canonical_ts_code: entry.moneyflow_contributor_eligible for entry in self.entries}

    def evidence(self, *, formal_policy: bool = False) -> dict[str, Any]:
        body = {
            "schema_version": ELIGIBILITY_SCHEMA,
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "minimum_availability_ratio": self.minimum_availability_ratio,
            "availability_integer_contract": "10*(expected-missing) >= 9*expected",
            "entry_count": len(self.entries),
            "entries": [entry.evidence() for entry in self.entries],
            "excluded_moneyflow_symbols": sorted(self.excluded_moneyflow_symbols),
            "pit_universe_changed": False,
            "selection_universe_changed": False,
            "runtime_prediction_eligibility_changed": False,
            "diagnostic_only": not formal_policy,
            "formal_policy_activated": formal_policy,
        }
        return {**body, "receipt_sha256": canonical_sha256(body)}


def build_train_only_observation_eligibility(
    provider_absence_rows: Iterable[ProviderAbsenceEvidence],
    *,
    expected_opportunity_dates_by_symbol: Mapping[str, Sequence[date]],
    train_start: date,
    train_end: date,
    minimum_availability_ratio: float = MIN_COVERAGE,
) -> ObservationEligibility:
    """Freeze moneyflow contributor eligibility from train-only audited absence."""

    if train_start > train_end:
        raise StateModelSetError("C-010 train window is invalid")
    if not math.isfinite(minimum_availability_ratio) or not 0 < minimum_availability_ratio <= 1:
        raise StateModelSetError("C-010 minimum availability ratio must be in (0,1]")
    if minimum_availability_ratio != MIN_COVERAGE:
        raise StateModelSetError(
            "hmm_risk_c010_policy_identity_mismatch: C-010 contributor availability must remain exactly 0.90"
        )
    filtered = [row for row in provider_absence_rows if train_start <= row.trade_date <= train_end]
    keys = [
        {
            "canonical_ts_code": row.canonical_ts_code,
            "trade_date": row.trade_date.isoformat(),
            "row_hash": row.row_hash,
        }
        for row in filtered
    ]
    if len({(item["canonical_ts_code"], item["trade_date"]) for item in keys}) != len(keys):
        raise StateModelSetError("C-010 provider absence keys are duplicated")
    absence_counts = Counter(row.canonical_ts_code for row in filtered)
    expected_symbols = {str(symbol).strip() for symbol in expected_opportunity_dates_by_symbol}
    if not expected_symbols or "" in expected_symbols:
        raise StateModelSetError(
            "hmm_risk_c010_expected_opportunity_missing: "
            "C-010 full-universe expected opportunity ledger is empty or invalid"
        )
    missing_expected_symbols = sorted(set(absence_counts) - expected_symbols)
    if missing_expected_symbols:
        raise StateModelSetError(
            "hmm_risk_c010_expected_opportunity_missing: "
            f"C-010 provider-absence symbols lack expected opportunity evidence: {missing_expected_symbols}"
        )
    entries: list[ContributorEligibility] = []
    for symbol in sorted(expected_symbols):
        expected_dates = tuple(sorted(expected_opportunity_dates_by_symbol.get(symbol, ())))
        if not expected_dates:
            raise StateModelSetError(
                "hmm_risk_c010_expected_opportunity_missing: "
                f"C-010 expected opportunity count is invalid: {symbol} expected=0 missing={absence_counts[symbol]}"
            )
        if len(expected_dates) != len(set(expected_dates)):
            raise StateModelSetError(
                f"hmm_risk_c010_contributor_receipt_mismatch: expected opportunity dates are duplicated: {symbol}"
            )
        if any(not isinstance(value, date) or value < train_start or value > train_end for value in expected_dates):
            raise StateModelSetError(
                "hmm_risk_c010_contributor_receipt_mismatch: "
                f"expected opportunity dates are outside the train window: {symbol}"
            )
        expected_date_set = frozenset(expected_dates)
        absence_dates = frozenset(row.trade_date for row in filtered if row.canonical_ts_code == symbol)
        unexpected_absence_dates = sorted(absence_dates - expected_date_set)
        if unexpected_absence_dates:
            raise StateModelSetError(
                "hmm_risk_c010_provider_absence_outside_opportunity: "
                f"C-010 provider absence is outside expected opportunities: {symbol} "
                f"dates={[value.isoformat() for value in unexpected_absence_dates]}"
            )
        expected = len(expected_dates)
        missing = int(absence_counts.get(symbol, 0))
        if expected <= 0 or missing > expected:
            raise StateModelSetError(
                "hmm_risk_c010_contributor_receipt_mismatch: "
                f"C-010 expected opportunity count is invalid: {symbol} expected={expected} missing={missing}"
            )
        availability = (expected - missing) / expected
        symbol_keys = [item for item in keys if item["canonical_ts_code"] == symbol]
        entries.append(
            ContributorEligibility(
                canonical_ts_code=symbol,
                expected_opportunity_count=expected,
                expected_opportunity_date_sha256=canonical_sha256(
                    [{"canonical_ts_code": symbol, "trade_date": value.isoformat()} for value in expected_dates]
                ),
                provider_absence_count=missing,
                availability_ratio=float(availability),
                moneyflow_contributor_eligible=(10 * (expected - missing)) >= (9 * expected),
                provider_absence_key_sha256=canonical_sha256(symbol_keys),
            )
        )
    return ObservationEligibility(
        train_start=train_start,
        train_end=train_end,
        minimum_availability_ratio=minimum_availability_ratio,
        entries=tuple(entries),
    )


def audit_feature_mask_candidates(
    panel: pd.DataFrame,
    *,
    family: str,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    direct_sector_level: str,
    expected_sector_count: int,
    moneyflow_unavailable_sector_codes: Iterable[str],
) -> dict[str, Any]:
    """Audit deterministic masks without fitting, selection, validation, or writes."""

    features = tuple(str(value) for value in feature_names)
    if len(features) != len(set(features)) or not features:
        raise StateModelSetError("C-010 feature list must be non-empty and unique")
    missing_columns = sorted(set(features) - set(panel.columns))
    if missing_columns:
        raise StateModelSetError(f"C-010 panel lacks feature columns: {missing_columns}")
    codes = tuple(sorted(str(value) for value in panel.index.get_level_values("l1_code").unique()))
    if len(codes) != expected_sector_count:
        raise StateModelSetError(
            f"C-010 expected {expected_sector_count} {direct_sector_level} sectors; actual={len(codes)}"
        )
    unavailable = frozenset(str(value) for value in moneyflow_unavailable_sector_codes)
    unknown = sorted(unavailable - set(codes))
    if unknown:
        raise StateModelSetError(f"C-010 unavailable sector set contains unknown codes: {unknown}")
    moneyflow_features = tuple(value for value in features if value in MONEYFLOW_FEATURES)
    mandatory_features = tuple(value for value in features if value not in MONEYFLOW_FEATURES)
    if not mandatory_features:
        raise StateModelSetError("C-010 feature mask cannot remove every feature")
    entries: list[dict[str, Any]] = []
    for code in codes:
        sector = panel.xs(code, level="l1_code")
        dates = sector.index.date
        train = sector.loc[(dates >= train_start) & (dates <= train_end)]
        full_count = int(train.loc[:, list(features)].dropna().shape[0])
        mandatory_count = int(train.loc[:, list(mandatory_features)].dropna().shape[0])
        if full_count >= MIN_TRAINING_ROWS:
            mask = features
            status = "full_feature_set"
        elif code in unavailable and moneyflow_features and mandatory_count >= MIN_TRAINING_ROWS:
            mask = mandatory_features
            status = "moneyflow_domain_excluded_candidate"
        else:
            mask = features
            status = "blocked_insufficient_train_rows"
        entry = {
            "sector_code": code,
            "full_feature_train_row_count": full_count,
            "masked_train_row_count": int(train.loc[:, list(mask)].dropna().shape[0]),
            "minimum_train_row_count": MIN_TRAINING_ROWS,
            "moneyflow_domain_structurally_unavailable": code in unavailable,
            "feature_mask": list(mask),
            "excluded_features": sorted(set(features) - set(mask)),
            "status": status,
        }
        entries.append({**entry, "entry_sha256": canonical_sha256(entry)})
    valid = all(entry["masked_train_row_count"] >= MIN_TRAINING_ROWS for entry in entries)
    body = {
        "schema_version": FEATURE_MASK_SCHEMA,
        "family": family,
        "direct_sector_level": direct_sector_level,
        "expected_sector_count": expected_sector_count,
        "entry_count": len(entries),
        "entries": entries,
        "feature_mask_candidate_valid": valid,
        "fit_performed": False,
        "selection_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "formal_policy_activated": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def load_feature_domain_direct_aggregates(
    reader: Any,
    eligibility: ObservationEligibility,
    *,
    min_coverage: float = MIN_COVERAGE,
    formal_policy: bool = False,
) -> tuple[list[L1DailyAggregate], list[L1DailyAggregate], dict[str, Any]]:
    """Build diagnostic direct L1/L2 aggregates from one canonical raw stream."""

    missing_rows = list(reader.iter_missing_price_rows())
    merged_rows = heapq.merge(
        reader.iter_stock_fact_rows(),
        iter(missing_rows),
        key=lambda row: (row["trade_date"], row["l1_code"], row["symbol"], row["l2_code"]),
    )
    contributor_eligibility = eligibility.moneyflow_contributor_eligibility
    excluded = eligibility.excluded_moneyflow_symbols
    l1_aggregates: list[L1DailyAggregate] = []
    l2_aggregates: list[L1DailyAggregate] = []
    l1_invalid_price_domain: list[dict[str, Any]] = []
    l2_invalid_price_domain: list[dict[str, Any]] = []
    impacted_l1: set[str] = set()
    impacted_l2: set[str] = set()
    train_eligibility_unavailable_symbols: set[str] = set()
    train_eligibility_unavailable_keys: set[tuple[str, date]] = set()
    raw_row_count = 0
    for _, day_group in itertools.groupby(merged_rows, key=lambda row: row["trade_date"]):
        day_rows = list(day_group)
        raw_row_count += len(day_rows)
        for row in day_rows:
            symbol = str(row["symbol"])
            if symbol not in contributor_eligibility:
                if eligibility.train_start <= row["trade_date"] <= eligibility.train_end:
                    raise StateModelSetError(
                        "hmm_risk_c010_contributor_receipt_mismatch: "
                        f"train contributor is absent from the full-universe ledger: {symbol}/{row['trade_date']}"
                    )
                train_eligibility_unavailable_symbols.add(symbol)
                train_eligibility_unavailable_keys.add((symbol, row["trade_date"]))
            if contributor_eligibility.get(symbol) is not True:
                impacted_l1.add(str(row["l1_code"]))
                impacted_l2.add(str(row["l2_code"]))
        l1_rows = sorted(day_rows, key=lambda row: (row["l1_code"], row["symbol"], row["l2_code"]))
        for _, group in itertools.groupby(l1_rows, key=lambda row: row["l1_code"]):
            rows = list(group)
            try:
                l1_aggregates.append(
                    aggregate_l1_day(
                        rows,
                        min_coverage=min_coverage,
                        moneyflow_contributor_eligibility=contributor_eligibility,
                    )
                )
            except ObservationCoverageError as exc:
                l1_invalid_price_domain.append(_invalid_price_domain_receipt(exc, rows, direct_sector_level="L1"))
        l2_rows = sorted(day_rows, key=lambda row: (row["l2_code"], row["symbol"], row["l1_code"]))
        projected_l2_rows = []
        for row in l2_rows:
            projected = dict(row)
            projected["l1_code"] = row["l2_code"]
            projected["l1_name"] = row["l2_name"]
            projected_l2_rows.append(projected)
        for _, group in itertools.groupby(projected_l2_rows, key=lambda row: row["l1_code"]):
            rows = list(group)
            try:
                l2_aggregates.append(
                    aggregate_l1_day(
                        rows,
                        min_coverage=min_coverage,
                        moneyflow_contributor_eligibility=contributor_eligibility,
                    )
                )
            except ObservationCoverageError as exc:
                l2_invalid_price_domain.append(_invalid_price_domain_receipt(exc, rows, direct_sector_level="L2"))
    if not l1_aggregates or not l2_aggregates:
        raise StateModelSetError("C-010 diagnostic produced no direct L1/L2 aggregates")
    evidence = {
        "schema_version": "hmm_risk_c010_feature_domain_aggregate_evidence_v1",
        "raw_row_count": raw_row_count,
        "missing_price_row_count": len(missing_rows),
        "excluded_moneyflow_symbols": sorted(excluded),
        "full_universe_eligibility_entry_count": len(contributor_eligibility),
        "train_eligibility_unavailable_symbol_count": len(train_eligibility_unavailable_symbols),
        "train_eligibility_unavailable_symbols": sorted(train_eligibility_unavailable_symbols),
        "train_eligibility_unavailable_symbol_sha256": canonical_sha256(sorted(train_eligibility_unavailable_symbols)),
        "train_eligibility_unavailable_key_count": len(train_eligibility_unavailable_keys),
        "train_eligibility_unavailable_key_sha256": canonical_sha256(
            [
                {"symbol": symbol, "trade_date": trade_date.isoformat()}
                for symbol, trade_date in sorted(
                    train_eligibility_unavailable_keys, key=lambda item: (item[1], item[0])
                )
            ]
        ),
        "impacted_l1_codes": sorted(impacted_l1),
        "impacted_l2_codes": sorted(impacted_l2),
        "l1_aggregate_count": len(l1_aggregates),
        "l2_aggregate_count": len(l2_aggregates),
        "l1_domain_receipts": [
            _domain_receipt(value, direct_sector_level="L1")
            for value in sorted(l1_aggregates, key=lambda item: (item.trade_date, item.l1_code))
        ],
        "l2_domain_receipts": [
            _domain_receipt(value, direct_sector_level="L2")
            for value in sorted(l2_aggregates, key=lambda item: (item.trade_date, item.l1_code))
        ],
        "l1_invalid_price_domain": [
            {**value, "entry_sha256": canonical_sha256(value)} for value in l1_invalid_price_domain
        ],
        "l2_invalid_price_domain": [
            {**value, "entry_sha256": canonical_sha256(value)} for value in l2_invalid_price_domain
        ],
        "formal_policy_activated": formal_policy,
        "database_write_performed": False,
    }
    return l1_aggregates, l2_aggregates, {**evidence, "receipt_sha256": canonical_sha256(evidence)}
