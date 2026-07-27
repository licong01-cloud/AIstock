"""Train-only C-010 observation eligibility and feature-mask diagnostics."""

from __future__ import annotations

import math
import heapq
import itertools
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from .provider_absence import ProviderAbsenceEvidence
from .state_model_set import StateModelSetError, canonical_sha256
from .stock_fact_observation import (
    MIN_COVERAGE,
    MIN_TRAINING_ROWS,
    L1DailyAggregate,
    ObservationCoverageError,
    aggregate_l1_day,
)

ELIGIBILITY_SCHEMA = "hmm_risk_c010_train_observation_eligibility_v1"
FEATURE_MASK_SCHEMA = "hmm_risk_c010_feature_mask_candidate_set_v1"

MONEYFLOW_FEATURES = frozenset(
    {
        "net_mf_ratio",
        "elg_net_mf_ratio",
        "sf_mf_net_ratio_std_5d_neg",
        "sf_small_net_ratio_5d",
    }
)


@dataclass(frozen=True)
class ContributorEligibility:
    canonical_ts_code: str
    expected_opportunity_count: int
    provider_absence_count: int
    availability_ratio: float
    moneyflow_contributor_eligible: bool
    provider_absence_key_sha256: str

    def evidence(self) -> dict[str, Any]:
        return {
            "canonical_ts_code": self.canonical_ts_code,
            "expected_opportunity_count": self.expected_opportunity_count,
            "provider_absence_count": self.provider_absence_count,
            "availability_ratio": self.availability_ratio,
            "moneyflow_contributor_eligible": self.moneyflow_contributor_eligible,
            "provider_absence_key_sha256": self.provider_absence_key_sha256,
        }


@dataclass(frozen=True)
class ObservationEligibility:
    train_start: date
    train_end: date
    minimum_availability_ratio: float
    entries: tuple[ContributorEligibility, ...]
    receipt_sha256: str

    @property
    def excluded_moneyflow_symbols(self) -> frozenset[str]:
        return frozenset(entry.canonical_ts_code for entry in self.entries if not entry.moneyflow_contributor_eligible)

    def evidence(self) -> dict[str, Any]:
        body = {
            "schema_version": ELIGIBILITY_SCHEMA,
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "minimum_availability_ratio": self.minimum_availability_ratio,
            "entries": [entry.evidence() for entry in self.entries],
            "excluded_moneyflow_symbols": sorted(self.excluded_moneyflow_symbols),
            "pit_universe_changed": False,
            "selection_universe_changed": False,
            "runtime_prediction_eligibility_changed": False,
            "diagnostic_only": True,
        }
        return {**body, "receipt_sha256": canonical_sha256(body)}


def build_train_only_observation_eligibility(
    provider_absence_rows: Iterable[ProviderAbsenceEvidence],
    *,
    expected_opportunity_count_by_symbol: Mapping[str, int],
    train_start: date,
    train_end: date,
    minimum_availability_ratio: float = MIN_COVERAGE,
) -> ObservationEligibility:
    """Freeze moneyflow contributor eligibility from train-only audited absence."""

    if train_start > train_end:
        raise StateModelSetError("C-010 train window is invalid")
    if not math.isfinite(minimum_availability_ratio) or not 0 < minimum_availability_ratio <= 1:
        raise StateModelSetError("C-010 minimum availability ratio must be in (0,1]")
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
    entries: list[ContributorEligibility] = []
    for symbol in sorted(absence_counts):
        expected = int(expected_opportunity_count_by_symbol.get(symbol, 0))
        missing = int(absence_counts[symbol])
        if expected <= 0 or missing > expected:
            raise StateModelSetError(
                f"C-010 expected opportunity count is invalid: {symbol} expected={expected} missing={missing}"
            )
        availability = (expected - missing) / expected
        symbol_keys = [item for item in keys if item["canonical_ts_code"] == symbol]
        entries.append(
            ContributorEligibility(
                canonical_ts_code=symbol,
                expected_opportunity_count=expected,
                provider_absence_count=missing,
                availability_ratio=float(availability),
                moneyflow_contributor_eligible=availability >= minimum_availability_ratio,
                provider_absence_key_sha256=canonical_sha256(symbol_keys),
            )
        )
    body = {
        "schema_version": ELIGIBILITY_SCHEMA,
        "train_start": train_start.isoformat(),
        "train_end": train_end.isoformat(),
        "minimum_availability_ratio": minimum_availability_ratio,
        "entries": [entry.evidence() for entry in entries],
    }
    return ObservationEligibility(
        train_start=train_start,
        train_end=train_end,
        minimum_availability_ratio=minimum_availability_ratio,
        entries=tuple(entries),
        receipt_sha256=canonical_sha256(body),
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
) -> tuple[list[L1DailyAggregate], list[L1DailyAggregate], dict[str, Any]]:
    """Build diagnostic direct L1/L2 aggregates from one canonical raw stream."""

    missing_rows = list(reader.iter_missing_price_rows())
    merged_rows = heapq.merge(
        reader.iter_stock_fact_rows(),
        iter(missing_rows),
        key=lambda row: (row["trade_date"], row["l1_code"], row["symbol"], row["l2_code"]),
    )
    excluded = eligibility.excluded_moneyflow_symbols
    l1_aggregates: list[L1DailyAggregate] = []
    l2_aggregates: list[L1DailyAggregate] = []
    l1_invalid_price_domain: list[dict[str, Any]] = []
    l2_invalid_price_domain: list[dict[str, Any]] = []
    impacted_l1: set[str] = set()
    impacted_l2: set[str] = set()
    raw_row_count = 0
    for _, day_group in itertools.groupby(merged_rows, key=lambda row: row["trade_date"]):
        day_rows = list(day_group)
        raw_row_count += len(day_rows)
        for row in day_rows:
            if str(row["symbol"]) in excluded:
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
                        moneyflow_excluded_symbols=excluded,
                    )
                )
            except ObservationCoverageError as exc:
                l1_invalid_price_domain.append(
                    {
                        "trade_date": exc.trade_date.isoformat(),
                        "sector_code": exc.l1_code,
                        "count_coverage": exc.count_coverage,
                        "weight_coverage": exc.weight_coverage,
                        "missing_evidence": list(exc.missing_evidence),
                    }
                )
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
                        moneyflow_excluded_symbols=excluded,
                    )
                )
            except ObservationCoverageError as exc:
                l2_invalid_price_domain.append(
                    {
                        "trade_date": exc.trade_date.isoformat(),
                        "sector_code": exc.l1_code,
                        "count_coverage": exc.count_coverage,
                        "weight_coverage": exc.weight_coverage,
                        "missing_evidence": list(exc.missing_evidence),
                    }
                )
    if not l1_aggregates or not l2_aggregates:
        raise StateModelSetError("C-010 diagnostic produced no direct L1/L2 aggregates")
    evidence = {
        "schema_version": "hmm_risk_c010_feature_domain_aggregate_evidence_v1",
        "raw_row_count": raw_row_count,
        "missing_price_row_count": len(missing_rows),
        "excluded_moneyflow_symbols": sorted(excluded),
        "impacted_l1_codes": sorted(impacted_l1),
        "impacted_l2_codes": sorted(impacted_l2),
        "l1_aggregate_count": len(l1_aggregates),
        "l2_aggregate_count": len(l2_aggregates),
        "l1_invalid_price_domain": l1_invalid_price_domain,
        "l2_invalid_price_domain": l2_invalid_price_domain,
        "formal_policy_activated": False,
        "database_write_performed": False,
    }
    return l1_aggregates, l2_aggregates, {**evidence, "receipt_sha256": canonical_sha256(evidence)}
