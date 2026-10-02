"""Typed full-scope corporate-action authority for PT-NEXT-020.

The local-data producer owns classification and source preparation.  This
module only validates the immutable hand-off and adapts the frozen v6 snapshot
to account semantics before any strategy outcome is read.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_corporate_actions import CorporateAction, CorporateActionBook
from .action_value_data import DailyCandidate, file_reference
from .contracts import canonical_sha256


AUTHORITY_SCHEMA = "local_data_corporate_action_full_scope_authority_v1"
ACCOUNT_CLASS = "GENERIC_PUBLIC_A_SHARE_ACCOUNT"
EXPECTED_CLASSIFICATION_COUNTS: Mapping[str, int] = {
    "DUPLICATE_SOURCE_RECORD": 1,
    "PREHISTORY_BOUNDARY_ACTION": 6,
    "SPECIAL_RESTRUCTURING_NON_PRO_RATA": 7,
    "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION": 1,
}
EXPECTED_REPLAY_APPLICATION: Mapping[str, str] = {
    "DUPLICATE_SOURCE_RECORD": "APPLY_ACCOUNT_ACTION",
    "PREHISTORY_BOUNDARY_ACTION": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
    "SPECIAL_RESTRUCTURING_NON_PRO_RATA": "REFERENCE_PRICE_ONLY",
    "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION": (
        "APPLY_ACCOUNT_ACTION_WITH_DISTINCT_REFERENCE_BASIS"
    ),
}
EXPECTED_CONSUMER_CONTRACT: Mapping[str, Any] = {
    "legacy_snapshot_schema": "position_timing_corporate_action_snapshot_v6",
    "typed_authority_reader_required": True,
    "account_economics_must_not_be_inferred_from_factor": True,
    "prehistory_actions_must_not_be_applied_to_new_window_positions": True,
}


def _decimal(value: Any, *, code: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if not parsed.is_finite():
        raise ActionValueError(code)
    return parsed


def _resolution_key(item: Mapping[str, Any]) -> tuple[str, date]:
    try:
        return str(item["symbol"]).upper(), date.fromisoformat(
            str(item["effective_trade_date"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_RESOLUTION_INVALID") from exc


@dataclass(frozen=True)
class FullScopeResolution:
    symbol: str
    effective_trade_date: date
    classification: str
    replay_application: str
    account_quantity_multiplier: Decimal
    account_cash_yuan_per_share: Decimal
    reference_quantity_multiplier: Decimal | None
    reference_cash_yuan_per_share: Decimal | None
    reference_factor_ratio: Decimal | None
    source_row_count: int
    source_rows_sha256: str
    factor_boundary_evidence: Mapping[str, Any]

    @property
    def key(self) -> tuple[str, date]:
        return self.symbol, self.effective_trade_date


@dataclass(frozen=True)
class FullScopeCorporateActionAuthority:
    authority_reference: Mapping[str, Any]
    authority_canonical_sha256: str
    candidate_manifest_reference: Mapping[str, Any]
    scope_start: date
    scope_end: date
    classification_counts: Mapping[str, int]
    resolutions: tuple[FullScopeResolution, ...]
    resolution_audit: Mapping[str, Any]
    _by_key: Mapping[tuple[str, date], FullScopeResolution] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        keys = tuple(item.key for item in self.resolutions)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_RESOLUTION_INVALID")
        object.__setattr__(self, "_by_key", dict(zip(keys, self.resolutions)))

    @property
    def resolution_audit_sha256(self) -> str:
        return str(self.resolution_audit["audit_sha256"])

    @property
    def keys(self) -> tuple[tuple[str, date], ...]:
        return tuple(self._by_key)

    def on(self, symbol: str, effective_trade_date: date) -> FullScopeResolution | None:
        return self._by_key.get((symbol.upper(), effective_trade_date))


def _validate_source_rows(
    item: Mapping[str, Any],
    *,
    symbol: str,
    effective_trade_date: date,
    classification: str,
) -> None:
    rows = item.get("row_classifications")
    row_count = item.get("source_row_count")
    if (
        isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count <= 0
        or not isinstance(rows, list)
        or len(rows) != row_count
    ):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_SOURCE_ROWS_INVALID")
    source_rows: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(row.get("source_row"), Mapping):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_SOURCE_ROWS_INVALID")
        source = row["source_row"]
        if (
            row.get("classification") != classification
            or row.get("may_accumulate_as_separate_account_action") is not False
            or row.get("source_row_sha256") != canonical_sha256(source)
            or str(source.get("ts_code")).upper() != symbol
            or str(source.get("ex_date")) != effective_trade_date.isoformat()
        ):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_SOURCE_ROWS_INVALID")
        source_rows.append(source)
    if item.get("source_rows_sha256") != canonical_sha256(source_rows):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_SOURCE_ROWS_INVALID")


def _validate_factor_evidence(
    item: Mapping[str, Any],
    *,
    candidate: DailyCandidate,
    effective_trade_date: date,
) -> None:
    symbol = str(item["symbol"]).upper()
    evidence = item.get("factor_boundary_evidence")
    if not isinstance(evidence, Mapping) or not isinstance(evidence.get("factor_file"), Mapping):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID")
    bars = candidate.bars(symbol)
    expected_reference = candidate.references.get(f"{symbol}:factor")
    factor_reference = evidence["factor_file"]
    try:
        actual_reference = file_reference(Path(str(factor_reference["path"])))
    except (KeyError, OSError, TypeError, ValueError) as exc:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_FILE_INVALID", symbol=symbol
        ) from exc
    if factor_reference != expected_reference or actual_reference != factor_reference:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_FILE_INVALID", symbol=symbol
        )

    factors = pd.to_numeric(bars["factor"], errors="coerce")
    valid = factors.where(np.isfinite(factors) & factors.gt(0)).dropna()
    if valid.empty:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )
    valid_dates = tuple(pd.Timestamp(index).date() for index in valid.index)
    valid_values = tuple(Decimal(str(value)) for value in valid.tolist())
    pit_rows = candidate.spans.loc[candidate.spans.symbol.eq(symbol)]
    if pit_rows.empty:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )
    first_pit_eligible = min(pd.Timestamp(value).date() for value in pit_rows.start)
    if (
        evidence.get("first_valid_factor_date") != valid_dates[0].isoformat()
        or evidence.get("first_pit_eligible_date") != first_pit_eligible.isoformat()
    ):
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )

    boundary_type = evidence.get("boundary_type")
    reference = item.get("reference_price_economics")
    if boundary_type == "NO_PRE_ACTION_FACTOR_IN_CANDIDATE":
        if (
            any(day < effective_trade_date for day in valid_dates)
            or first_pit_eligible < effective_trade_date
            or evidence.get("previous_factor_date") is not None
            or evidence.get("current_factor_date") != valid_dates[0].isoformat()
            or evidence.get("observed_factor_ratio") is not None
            or reference is not None
        ):
            raise ActionValueError(
                "PATTERN_FULL_SCOPE_AUTHORITY_PREHISTORY_BOUNDARY_INVALID",
                symbol=symbol,
            )
        return
    if boundary_type != "MATERIAL_FACTOR_INTERVAL" or not isinstance(reference, Mapping):
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )
    previous = [index for index, day in enumerate(valid_dates) if day < effective_trade_date]
    following = [index for index, day in enumerate(valid_dates) if day >= effective_trade_date]
    if not previous or not following:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )
    previous_index, current_index = previous[-1], following[0]
    observed = valid_values[current_index] / valid_values[previous_index]
    if (
        evidence.get("previous_factor_date")
        != valid_dates[previous_index].isoformat()
        or evidence.get("current_factor_date")
        != valid_dates[current_index].isoformat()
        or evidence.get("observed_factor_ratio") != str(observed)
        or reference.get("factor_ratio") != str(observed)
    ):
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_AUTHORITY_FACTOR_EVIDENCE_INVALID", symbol=symbol
        )


def open_full_scope_corporate_action_authority(
    path: Path,
    *,
    candidate: DailyCandidate,
    expected_candidate_manifest_sha256: str,
    expected_authority_canonical_sha256: str,
    expected_authority_file_sha256: str,
    expected_start: date,
    expected_end: date,
) -> FullScopeCorporateActionAuthority:
    """Open and completely validate the local-data typed authority."""

    resolved = path.resolve()
    try:
        authority_reference = file_reference(resolved)
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_UNAVAILABLE") from exc
    identity = {key: value for key, value in payload.items() if key != "canonical_sha256"}
    candidate_manifest_reference = file_reference(candidate.root / "qe_dataset_manifest.json")
    scope = payload.get("scope")
    safety = payload.get("safety")
    raw_resolutions = payload.get("resolutions")
    if (
        authority_reference.get("sha256") != expected_authority_file_sha256
        or payload.get("schema_version") != AUTHORITY_SCHEMA
        or payload.get("request_id") != "PT-NEXT-020"
        or payload.get("canonical_sha256") != canonical_sha256(identity)
        or payload.get("canonical_sha256") != expected_authority_canonical_sha256
        or payload.get("candidate_manifest") != candidate_manifest_reference
        or candidate_manifest_reference.get("sha256")
        != expected_candidate_manifest_sha256
        or payload.get("account_class") != ACCOUNT_CLASS
        or payload.get("consumer_contract") != EXPECTED_CONSUMER_CONTRACT
        or not isinstance(scope, Mapping)
        or scope.get("start") != expected_start.isoformat()
        or scope.get("end") != expected_end.isoformat()
        or scope.get("resolution_count") != 15
        or not isinstance(raw_resolutions, list)
        or len(raw_resolutions) != 15
        or payload.get("classification_counts") != EXPECTED_CLASSIFICATION_COUNTS
        or not isinstance(safety, Mapping)
        or safety.get("database_write_performed") is not False
        or safety.get("candidate_write_performed") is not False
        or safety.get("adj_factor_write_performed") is not False
        or safety.get("outcomes_read") is not False
        or safety.get("runtime_action_performed") is not False
    ):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_IDENTITY_MISMATCH")

    resolution_keys = tuple(_resolution_key(item) for item in raw_resolutions)
    if (
        resolution_keys != tuple(sorted(resolution_keys))
        or len(resolution_keys) != len(set(resolution_keys))
        or scope.get("symbols") != sorted({symbol for symbol, _ in resolution_keys})
    ):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_RESOLUTION_INVALID")

    resolutions: list[FullScopeResolution] = []
    for item, (symbol, effective_trade_date) in zip(raw_resolutions, resolution_keys):
        classification = item.get("classification")
        account = item.get("account_economics")
        reference = item.get("reference_price_economics")
        if (
            classification not in EXPECTED_REPLAY_APPLICATION
            or item.get("replay_application")
            != EXPECTED_REPLAY_APPLICATION[classification]
            or item.get("account_class") != ACCOUNT_CLASS
            or not isinstance(account, Mapping)
            or effective_trade_date < expected_start
            or effective_trade_date > expected_end
        ):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_RESOLUTION_INVALID")
        _validate_source_rows(
            item,
            symbol=symbol,
            effective_trade_date=effective_trade_date,
            classification=str(classification),
        )
        _validate_factor_evidence(
            item,
            candidate=candidate,
            effective_trade_date=effective_trade_date,
        )
        account_quantity = _decimal(
            account.get("quantity_multiplier"),
            code="PATTERN_FULL_SCOPE_AUTHORITY_ACCOUNT_ECONOMICS_INVALID",
        )
        account_cash = _decimal(
            account.get("cash_yuan_per_share"),
            code="PATTERN_FULL_SCOPE_AUTHORITY_ACCOUNT_ECONOMICS_INVALID",
        )
        if account_quantity < 1 or account_cash < 0:
            raise ActionValueError(
                "PATTERN_FULL_SCOPE_AUTHORITY_ACCOUNT_ECONOMICS_INVALID"
            )
        reference_quantity = (
            _decimal(
                reference.get("quantity_multiplier"),
                code="PATTERN_FULL_SCOPE_AUTHORITY_REFERENCE_ECONOMICS_INVALID",
            )
            if isinstance(reference, Mapping)
            and reference.get("quantity_multiplier") is not None
            else None
        )
        reference_cash = (
            _decimal(
                reference.get("cash_yuan_per_share"),
                code="PATTERN_FULL_SCOPE_AUTHORITY_REFERENCE_ECONOMICS_INVALID",
            )
            if isinstance(reference, Mapping)
            and reference.get("cash_yuan_per_share") is not None
            else None
        )
        reference_factor = (
            _decimal(
                reference.get("factor_ratio"),
                code="PATTERN_FULL_SCOPE_AUTHORITY_REFERENCE_ECONOMICS_INVALID",
            )
            if isinstance(reference, Mapping) and reference.get("factor_ratio") is not None
            else None
        )
        if classification == "SPECIAL_RESTRUCTURING_NON_PRO_RATA" and (
            account_quantity != 1 or account_cash != 0
        ):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_ACCOUNT_ECONOMICS_INVALID")
        if classification == "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION" and (
            reference_quantity is None
            or reference_cash is None
            or reference_quantity == account_quantity
            or reference_cash == account_cash
        ):
            raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_DUAL_BASIS_INVALID")
        resolutions.append(
            FullScopeResolution(
                symbol=symbol,
                effective_trade_date=effective_trade_date,
                classification=str(classification),
                replay_application=str(item["replay_application"]),
                account_quantity_multiplier=account_quantity,
                account_cash_yuan_per_share=account_cash,
                reference_quantity_multiplier=reference_quantity,
                reference_cash_yuan_per_share=reference_cash,
                reference_factor_ratio=reference_factor,
                source_row_count=int(item["source_row_count"]),
                source_rows_sha256=str(item["source_rows_sha256"]),
                factor_boundary_evidence=item["factor_boundary_evidence"],
            )
        )

    if dict(Counter(item.classification for item in resolutions)) != dict(
        EXPECTED_CLASSIFICATION_COUNTS
    ):
        raise ActionValueError("PATTERN_FULL_SCOPE_AUTHORITY_CLASSIFICATION_INVALID")
    audit_identity = {
        "schema_version": "position_timing_full_scope_resolution_audit_v1",
        "authority_file_sha256": authority_reference["sha256"],
        "authority_canonical_sha256": expected_authority_canonical_sha256,
        "candidate_manifest_sha256": expected_candidate_manifest_sha256,
        "scope": {
            "start": expected_start.isoformat(),
            "end": expected_end.isoformat(),
            "resolution_count": len(resolutions),
        },
        "classification_counts": dict(EXPECTED_CLASSIFICATION_COUNTS),
        "resolution_source_rows_sha256": canonical_sha256(
            [
                {
                    "symbol": item.symbol,
                    "effective_trade_date": item.effective_trade_date.isoformat(),
                    "source_rows_sha256": item.source_rows_sha256,
                }
                for item in resolutions
            ]
        ),
        "factor_boundary_evidence_sha256": canonical_sha256(
            [item.factor_boundary_evidence for item in resolutions]
        ),
        "unresolved_typed_resolution_count": 0,
        "outcomes_read": False,
        "factor_account_participation_inference": False,
    }
    resolution_audit = {
        **audit_identity,
        "audit_sha256": canonical_sha256(audit_identity),
    }
    return FullScopeCorporateActionAuthority(
        authority_reference=authority_reference,
        authority_canonical_sha256=expected_authority_canonical_sha256,
        candidate_manifest_reference=candidate_manifest_reference,
        scope_start=expected_start,
        scope_end=expected_end,
        classification_counts=dict(EXPECTED_CLASSIFICATION_COUNTS),
        resolutions=tuple(resolutions),
        resolution_audit=resolution_audit,
    )


def apply_full_scope_corporate_action_authority(
    corporate_actions: CorporateActionBook,
    authority: FullScopeCorporateActionAuthority,
    *,
    candidate_source_sha256: str,
) -> tuple[CorporateActionBook, Mapping[str, Any]]:
    """Apply typed account semantics without deriving them from factor moves."""

    if len(candidate_source_sha256) != 64:
        raise ActionValueError("PATTERN_FULL_SCOPE_APPLICATION_SCOPE_INVALID")
    replacements: dict[tuple[str, date], CorporateAction] = {}
    excluded: set[tuple[str, date]] = set()
    applications: list[dict[str, Any]] = []
    unresolved: list[str] = []
    for resolution in authority.resolutions:
        source = corporate_actions.on(*resolution.key)
        if source is None or source.source_row_count != resolution.source_row_count:
            unresolved.append(f"{resolution.symbol}/{resolution.effective_trade_date.isoformat()}")
            continue
        if resolution.replay_application == "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION":
            excluded.add(resolution.key)
            applied = None
        else:
            reference_cash = (
                resolution.reference_cash_yuan_per_share
                if resolution.reference_cash_yuan_per_share is not None
                else resolution.account_cash_yuan_per_share
            )
            applied = CorporateAction(
                symbol=resolution.symbol,
                effective_trade_date=resolution.effective_trade_date,
                quantity_multiplier=resolution.account_quantity_multiplier,
                cashflow_yuan_per_share=resolution.account_cash_yuan_per_share,
                reference_price_cash_yuan_per_share=reference_cash,
                cash_pay_date=source.cash_pay_date,
                share_listing_date=source.share_listing_date,
                source_available_at=source.source_available_at,
                source_row_count=resolution.source_row_count,
                source_rows_sha256=resolution.source_rows_sha256,
                source_economic_action_count=1,
                source_record_date_proxy_count=min(
                    source.source_record_date_proxy_count, 1
                ),
            )
            replacements[resolution.key] = applied
        applications.append(
            {
                "symbol": resolution.symbol,
                "effective_trade_date": resolution.effective_trade_date.isoformat(),
                "classification": resolution.classification,
                "replay_application": resolution.replay_application,
                "source_rows_sha256": resolution.source_rows_sha256,
                "account_quantity_multiplier": str(
                    resolution.account_quantity_multiplier
                ),
                "account_cash_yuan_per_share": str(
                    resolution.account_cash_yuan_per_share
                ),
                "reference_quantity_multiplier": (
                    str(resolution.reference_quantity_multiplier)
                    if resolution.reference_quantity_multiplier is not None
                    else None
                ),
                "reference_cash_yuan_per_share": (
                    str(resolution.reference_cash_yuan_per_share)
                    if resolution.reference_cash_yuan_per_share is not None
                    else None
                ),
                "account_action_applied": applied is not None,
                "factor_account_participation_inference": False,
            }
        )
    if unresolved:
        raise ActionValueError(
            "PATTERN_FULL_SCOPE_APPLICATION_UNRESOLVED",
            unresolved_typed_resolutions=unresolved,
        )
    retained = tuple(
        sorted(
            (
                replacements.get((action.symbol, action.effective_trade_date), action)
                for action in corporate_actions.actions
                if (action.symbol, action.effective_trade_date) not in excluded
            ),
            key=lambda action: (action.symbol, action.effective_trade_date),
        )
    )
    retained_keys = [(item.symbol, item.effective_trade_date) for item in retained]
    if len(retained_keys) != len(set(retained_keys)):
        raise ActionValueError("PATTERN_FULL_SCOPE_APPLICATION_DUPLICATE_ECONOMICS")
    audit_identity = {
        "schema_version": "position_timing_full_scope_application_audit_v1",
        "source_snapshot_sha256": corporate_actions.snapshot_sha256,
        "authority_canonical_sha256": authority.authority_canonical_sha256,
        "resolution_audit_sha256": authority.resolution_audit_sha256,
        "candidate_source_sha256": candidate_source_sha256,
        "resolution_count": len(authority.resolutions),
        "classification_counts": dict(authority.classification_counts),
        "applications": applications,
        "applied_account_action_count": len(replacements),
        "excluded_prehistory_action_count": len(excluded),
        "prevalidated_action_keys": [
            f"{symbol}/{effective.isoformat()}" for symbol, effective in sorted(replacements)
        ],
        "unresolved_typed_resolution_count": 0,
        "duplicate_economic_accumulation_count": 0,
        "factor_account_participation_inference": False,
        "outcomes_read": False,
    }
    audit = {
        **audit_identity,
        "application_sha256": canonical_sha256(audit_identity),
    }
    return CorporateActionBook(retained, audit["application_sha256"]), audit


__all__ = [
    "AUTHORITY_SCHEMA",
    "EXPECTED_CLASSIFICATION_COUNTS",
    "FullScopeCorporateActionAuthority",
    "FullScopeResolution",
    "apply_full_scope_corporate_action_authority",
    "open_full_scope_corporate_action_authority",
]
