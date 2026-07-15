"""Versioned contracts for the QE-only long-trend evaluation engine.

This module is deliberately pure: it owns immutable profiles, stable reason
codes, family-local evidence status, and deterministic evaluation identity.
It has no database, HTTP, scheduler, trading, or non-QE runtime dependency.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping


PROFILE_ID_V1 = "qe_long_trend_v1"
SCHEMA_VERSION_V1 = "qe_long_trend_eval_v1"
EVALUATOR_VERSION = "qelt_core_v1"

_SUPPORTED_CALENDAR_SLICES = {
    "all_oos",
    "last_252_signal_days",
    "last_126_signal_days",
}
_SUPPORTED_ENTRY_RULES = {"signal_T_entry_T_plus_1_close_qfq"}
_SUPPORTED_TERMINAL_RULES = {"T_plus_h_plus_1_close_qfq"}
_SUPPORTED_BARRIER_PROJECTIONS = {"future_close_qfq"}
_SUPPORTED_PATH_PROJECTIONS = {"future_high_low_qfq_diagnostic"}
_SUPPORTED_EXECUTION_BRIDGES = {"qe_archived_order_trade_position_reconciled_v1"}
_SUPPORTED_UNKNOWN_EXECUTION_POLICIES = {"explicit_not_verifiable"}
_SUPPORTED_SECTOR_PROJECTIONS = {"signal_date_sw_l2_l2_code_id"}
_SUPPORTED_MISSING_INPUT_POLICIES = {"family_local_status_and_data_action_plan"}

FAMILY_NAMES: tuple[str, ...] = (
    "signal_path",
    "position_episode",
    "portfolio_result",
    "order_fill",
    "execution_cause",
    "sector_regime",
)

INPUT_ARTIFACT_FIELDS: tuple[str, ...] = (
    "prediction_sha256",
    "label_sha256",
    "position_sha256",
    "portfolio_report_sha256",
    "indicator_object_sha256",
    "trade_sha256",
    "order_sha256",
)


class FamilyComputationStatus(str, Enum):
    COMPUTED = "COMPUTED"
    COMPUTED_WITH_LIMITATIONS = "COMPUTED_WITH_LIMITATIONS"
    NOT_COMPUTABLE = "NOT_COMPUTABLE"
    NOT_VERIFIABLE = "NOT_VERIFIABLE"


class QELongTrendReason(str, Enum):
    NON_QE_SOURCE_REJECTED = "QELT_NON_QE_SOURCE_REJECTED"
    PROFILE_INVALID = "QELT_PROFILE_INVALID"
    PROFILE_UNSUPPORTED_RUN_MODE = "QELT_PROFILE_UNSUPPORTED_RUN_MODE"
    PREDICTION_ARTIFACT_MISSING = "QELT_PREDICTION_ARTIFACT_MISSING"
    PREDICTION_SCHEMA_INVALID = "QELT_PREDICTION_SCHEMA_INVALID"
    FEATURE_DATASET_IDENTITY_MISSING = "QELT_FEATURE_DATASET_IDENTITY_MISSING"
    DATASET_ROOT_IDENTITY_MISMATCH = "QELT_DATASET_ROOT_IDENTITY_MISMATCH"
    OUTCOME_SNAPSHOT_NOT_EXTENSION = "QELT_OUTCOME_SNAPSHOT_NOT_EXTENSION"
    SNAPSHOT_OVERLAP_EMPTY = "QELT_SNAPSHOT_OVERLAP_EMPTY"
    DAILY_PV_SCHEMA_INVALID = "QELT_DAILY_PV_SCHEMA_INVALID"
    SECTOR_DATA_SCHEMA_INVALID = "QELT_SECTOR_DATA_SCHEMA_INVALID"
    LABEL_PARITY_FAILED = "QELT_LABEL_PARITY_FAILED"
    LABEL_PARITY_NO_OVERLAP = "QELT_LABEL_PARITY_NO_OVERLAP"
    ENTRY_COVERAGE_LOW = "QELT_ENTRY_COVERAGE_LOW"
    PATH_COVERAGE_LOW = "QELT_PATH_COVERAGE_LOW"
    INSTRUMENT_EXIT_UNRESOLVED = "QELT_INSTRUMENT_EXIT_UNRESOLVED"
    INSUFFICIENT_MATURITY = "QELT_INSUFFICIENT_MATURITY"
    EXECUTION_EVIDENCE_INSUFFICIENT = "QELT_EXECUTION_EVIDENCE_INSUFFICIENT"
    POSITION_ARTIFACT_MISSING = "QELT_POSITION_ARTIFACT_MISSING"
    POSITION_HISTORY_LEFT_CENSORED = "QELT_POSITION_HISTORY_LEFT_CENSORED"
    PORTFOLIO_REPORT_INVALID = "QELT_PORTFOLIO_REPORT_INVALID"
    PORTFOLIO_DIAGNOSTICS_INCOMPLETE = "QELT_PORTFOLIO_DIAGNOSTICS_INCOMPLETE"
    EPISODE_RECONCILIATION_FAILED = "QELT_EPISODE_RECONCILIATION_FAILED"
    EXECUTION_BRIDGE_RECONCILIATION_FAILED = "QELT_EXECUTION_BRIDGE_RECONCILIATION_FAILED"
    ARTIFACT_UPLOAD_FAILED = "QELT_ARTIFACT_UPLOAD_FAILED"
    ARCHIVE_PERSIST_FAILED = "QELT_ARCHIVE_PERSIST_FAILED"
    DUPLICATE_IDENTITY_CONFLICT = "QELT_DUPLICATE_IDENTITY_CONFLICT"
    RESOURCE_EVENT_INVALID = "QELT_RESOURCE_EVENT_INVALID"


class QELongTrendError(RuntimeError):
    """Structured fail-fast error for contract and input identity violations."""

    def __init__(
        self,
        reason_code: QELongTrendReason | str,
        message: str,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        code = reason_code.value if isinstance(reason_code, QELongTrendReason) else str(reason_code)
        super().__init__(f"{code}: {message}")
        self.reason_code = code
        self.message = message
        self.context = dict(context or {})


@dataclass(frozen=True)
class QELongTrendProfile:
    profile_id: str
    schema_version: str
    horizons: tuple[int, ...]
    barriers: tuple[float, ...]
    calendar_slices: tuple[str, ...]
    fixed_k: tuple[int, ...]
    include_strategy_topk_up_to: int
    entry_rule: str
    terminal_rule: str
    barrier_primary_projection: str
    path_projection: str
    execution_bridge: str
    execution_authority_order: tuple[str, ...]
    unknown_execution_policy: str
    sector_projection: str
    missing_input_policy: str
    entry_coverage_reference: float = 0.98
    path_coverage_reference: float = 0.98
    sector_coverage_reference: float = 0.98
    bootstrap_samples: int = 500
    bootstrap_seed: int = 20260715

    def __post_init__(self) -> None:
        if not self.profile_id or not self.schema_version:
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, "profile identity is empty")
        if not self.horizons or tuple(sorted(set(self.horizons))) != self.horizons:
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                "horizons must be unique positive integers in ascending order",
            )
        if any(not isinstance(value, int) or value <= 0 for value in self.horizons):
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, "horizons must be positive integers")
        if not self.barriers or tuple(sorted(set(self.barriers))) != self.barriers:
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                "barriers must be unique positive values in ascending order",
            )
        if any(value <= 0.0 for value in self.barriers):
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, "barriers must be positive")
        if any(value <= 0 for value in self.fixed_k):
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, "fixed_k must be positive")
        if tuple(sorted(set(self.fixed_k))) != self.fixed_k:
            raise QELongTrendError(QELongTrendReason.PROFILE_INVALID, "fixed_k must be sorted and unique")
        if self.include_strategy_topk_up_to <= 0:
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                "include_strategy_topk_up_to must be positive",
            )
        for name, value in (
            ("entry_coverage_reference", self.entry_coverage_reference),
            ("path_coverage_reference", self.path_coverage_reference),
            ("sector_coverage_reference", self.sector_coverage_reference),
        ):
            if not 0.0 < value <= 1.0:
                raise QELongTrendError(
                    QELongTrendReason.PROFILE_INVALID,
                    f"{name} must be in (0, 1]",
                )
        if self.bootstrap_samples <= 0:
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                "bootstrap_samples must be positive",
            )
        unsupported_slices = sorted(set(self.calendar_slices) - _SUPPORTED_CALENDAR_SLICES)
        if unsupported_slices or len(set(self.calendar_slices)) != len(self.calendar_slices):
            raise QELongTrendError(
                QELongTrendReason.PROFILE_INVALID,
                f"calendar_slices contain unsupported or duplicate values: {unsupported_slices}",
            )
        semantic_contracts = (
            ("entry_rule", self.entry_rule, _SUPPORTED_ENTRY_RULES),
            ("terminal_rule", self.terminal_rule, _SUPPORTED_TERMINAL_RULES),
            (
                "barrier_primary_projection",
                self.barrier_primary_projection,
                _SUPPORTED_BARRIER_PROJECTIONS,
            ),
            ("path_projection", self.path_projection, _SUPPORTED_PATH_PROJECTIONS),
            ("execution_bridge", self.execution_bridge, _SUPPORTED_EXECUTION_BRIDGES),
            (
                "unknown_execution_policy",
                self.unknown_execution_policy,
                _SUPPORTED_UNKNOWN_EXECUTION_POLICIES,
            ),
            ("sector_projection", self.sector_projection, _SUPPORTED_SECTOR_PROJECTIONS),
            (
                "missing_input_policy",
                self.missing_input_policy,
                _SUPPORTED_MISSING_INPUT_POLICIES,
            ),
        )
        for field_name, value, supported in semantic_contracts:
            if value not in supported:
                raise QELongTrendError(
                    QELongTrendReason.PROFILE_INVALID,
                    f"{field_name}={value!r} has no implemented evaluator semantics",
                )

    def canonical_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["horizons"] = list(self.horizons)
        payload["barriers"] = list(self.barriers)
        payload["calendar_slices"] = list(self.calendar_slices)
        payload["fixed_k"] = list(self.fixed_k)
        payload["execution_authority_order"] = list(self.execution_authority_order)
        return payload

    @property
    def profile_sha256(self) -> str:
        return canonical_sha256(self.canonical_payload())


QE_LONG_TREND_PROFILE_V1 = QELongTrendProfile(
    profile_id=PROFILE_ID_V1,
    schema_version=SCHEMA_VERSION_V1,
    horizons=(20, 40, 60, 120, 180),
    barriers=(0.30, 0.50, 0.70),
    calendar_slices=("all_oos", "last_252_signal_days", "last_126_signal_days"),
    fixed_k=(20, 50),
    include_strategy_topk_up_to=50,
    entry_rule="signal_T_entry_T_plus_1_close_qfq",
    terminal_rule="T_plus_h_plus_1_close_qfq",
    barrier_primary_projection="future_close_qfq",
    path_projection="future_high_low_qfq_diagnostic",
    execution_bridge="qe_archived_order_trade_position_reconciled_v1",
    execution_authority_order=(
        "qlib_indicator_object_amount_deal_amount_ffr",
        "reconciled_order_and_trade",
        "reconciled_trade",
        "position_transition",
        "daily_market_state_diagnostic",
    ),
    unknown_execution_policy="explicit_not_verifiable",
    sector_projection="signal_date_sw_l2_l2_code_id",
    missing_input_policy="family_local_status_and_data_action_plan",
)

_PROFILE_REGISTRY: dict[str, QELongTrendProfile] = {
    QE_LONG_TREND_PROFILE_V1.profile_id: QE_LONG_TREND_PROFILE_V1,
}


def get_long_trend_profile(profile_id: str) -> QELongTrendProfile:
    try:
        return _PROFILE_REGISTRY[str(profile_id)]
    except KeyError as exc:
        raise QELongTrendError(
            QELongTrendReason.PROFILE_INVALID,
            f"unregistered long-trend profile {profile_id!r}",
            context={"available_profile_ids": sorted(_PROFILE_REGISTRY)},
        ) from exc


def require_registered_profile(profile: QELongTrendProfile) -> QELongTrendProfile:
    registered = get_long_trend_profile(profile.profile_id)
    if profile != registered or profile.profile_sha256 != registered.profile_sha256:
        raise QELongTrendError(
            QELongTrendReason.PROFILE_INVALID,
            "runtime profile differs from the immutable registered profile",
            context={
                "profile_id": profile.profile_id,
                "requested_profile_sha256": profile.profile_sha256,
                "registered_profile_sha256": registered.profile_sha256,
            },
        )
    return registered


@dataclass(frozen=True)
class FamilyEvidenceStatus:
    status: FamilyComputationStatus
    available_inputs: tuple[str, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    coverage: Mapping[str, float | int | None] = field(default_factory=dict)
    limitations: tuple[str, ...] = ()
    supporting_artifacts: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    data_actions: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        required = {
            "action",
            "source_candidates",
            "required_fields",
            "time_range",
            "historical_backfill",
            "recoverable_family",
        }
        for index, item in enumerate(self.data_actions):
            missing = sorted(required - set(item))
            if missing:
                raise QELongTrendError(
                    QELongTrendReason.PROFILE_INVALID,
                    f"data_actions[{index}] is missing required fields: {missing}",
                )

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "available_inputs": list(self.available_inputs),
            "missing_inputs": list(self.missing_inputs),
            "coverage": dict(self.coverage),
            "limitations": list(self.limitations),
            "supporting_artifacts": list(self.supporting_artifacts),
            "reason_codes": list(self.reason_codes),
            "data_actions": [dict(item) for item in self.data_actions],
        }


@dataclass(frozen=True)
class QEDatasetSnapshotIdentity:
    snapshot_id: str
    manifest_sha256: str
    start_date: str
    end_date: str
    lineage_parent_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.snapshot_id or not self.manifest_sha256:
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "dataset snapshot identity requires snapshot_id and manifest_sha256",
            )
        try:
            start = _canonical_date(self.start_date)
            end = _canonical_date(self.end_date)
        except (TypeError, ValueError) as exc:
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "dataset snapshot identity contains invalid dates",
            ) from exc
        if end < start:
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "dataset snapshot end_date is earlier than start_date",
            )
        if self.snapshot_id in set(self.lineage_parent_ids):
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "dataset snapshot cannot declare itself as a lineage ancestor",
            )
        if any(not str(value).strip() for value in self.lineage_parent_ids) or len(set(self.lineage_parent_ids)) != len(
            self.lineage_parent_ids
        ):
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "dataset lineage parent ids must be non-empty and unique",
            )


@dataclass(frozen=True)
class SnapshotOverlapParityReceipt:
    feature_snapshot_id: str
    outcome_snapshot_id: str
    overlap_start: str
    overlap_end: str
    row_count: int
    column_count: int
    overlap_price_parity_sha256: str
    relation: str

    def __post_init__(self) -> None:
        if not self.feature_snapshot_id or not self.outcome_snapshot_id or not self.overlap_price_parity_sha256:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "snapshot overlap receipt identity is incomplete",
            )
        try:
            start = _canonical_date(self.overlap_start)
            end = _canonical_date(self.overlap_end)
        except (TypeError, ValueError) as exc:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "snapshot overlap receipt contains invalid dates",
            ) from exc
        if (
            end < start
            or not isinstance(self.row_count, int)
            or isinstance(self.row_count, bool)
            or self.row_count <= 0
            or not isinstance(self.column_count, int)
            or isinstance(self.column_count, bool)
            or self.column_count != 4
        ):
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "snapshot overlap receipt has an invalid window or qfq OHLC shape",
                context={
                    "row_count": self.row_count,
                    "column_count": self.column_count,
                },
            )
        if self.relation not in {"same_snapshot", "verified_extension"}:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                f"unsupported snapshot overlap relation {self.relation!r}",
            )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QELongTrendEvaluationContext:
    run_id: str
    evaluator_source_sha256: str
    feature_snapshot: QEDatasetSnapshotIdentity
    outcome_snapshot: QEDatasetSnapshotIdentity
    overlap_receipt: SnapshotOverlapParityReceipt
    input_artifact_hashes: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.run_id or not self.evaluator_source_sha256:
            raise QELongTrendError(
                QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
                "evaluation context requires run_id and evaluator_source_sha256",
            )
        if self.overlap_receipt.feature_snapshot_id != self.feature_snapshot.snapshot_id:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "overlap receipt feature identity does not match evaluation context",
            )
        if self.overlap_receipt.outcome_snapshot_id != self.outcome_snapshot.snapshot_id:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "overlap receipt outcome identity does not match evaluation context",
            )
        if (
            self.overlap_receipt.overlap_start != self.feature_snapshot.start_date
            or self.overlap_receipt.overlap_end != self.feature_snapshot.end_date
        ):
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "overlap receipt window does not equal the full feature snapshot window",
            )
        same_snapshot = (
            self.feature_snapshot.snapshot_id == self.outcome_snapshot.snapshot_id
            and self.feature_snapshot.manifest_sha256 == self.outcome_snapshot.manifest_sha256
            and self.feature_snapshot.start_date == self.outcome_snapshot.start_date
            and self.feature_snapshot.end_date == self.outcome_snapshot.end_date
        )
        if same_snapshot:
            relation_valid = self.overlap_receipt.relation == "same_snapshot"
        else:
            relation_valid = (
                self.overlap_receipt.relation == "verified_extension"
                and self.feature_snapshot.snapshot_id in set(self.outcome_snapshot.lineage_parent_ids)
                and _canonical_date(self.outcome_snapshot.start_date)
                <= _canonical_date(self.feature_snapshot.start_date)
                and _canonical_date(self.outcome_snapshot.end_date) > _canonical_date(self.feature_snapshot.end_date)
            )
        if not relation_valid:
            raise QELongTrendError(
                QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION,
                "overlap receipt relation is inconsistent with feature/outcome snapshot identity",
            )

    @property
    def input_manifest(self) -> dict[str, Any]:
        return canonical_input_manifest(self.input_artifact_hashes)

    @property
    def input_manifest_sha256(self) -> str:
        return canonical_sha256(self.input_manifest)

    def identity_input_manifest(
        self,
        *,
        evaluation_parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        manifest = dict(self.input_manifest)
        values = dict(evaluation_parameters or {})
        parameter_manifest: dict[str, Any] = {}
        for field_name in ("label_horizon", "strategy_topk"):
            value = values.pop(field_name, None)
            parameter_manifest[field_name] = value if value not in (None, "") else typed_null(field_name)
        for field_name in sorted(values):
            value = values[field_name]
            parameter_manifest[field_name] = value if value not in (None, "") else typed_null(field_name)
        manifest["evaluation_parameters"] = parameter_manifest
        return manifest

    def evaluation_id(
        self,
        *,
        profile_sha256: str,
        evaluation_parameters: Mapping[str, Any] | None = None,
    ) -> str:
        identity_manifest = self.identity_input_manifest(evaluation_parameters=evaluation_parameters)
        return build_evaluation_id(
            run_id=self.run_id,
            profile_sha256=profile_sha256,
            evaluator_source_sha256=self.evaluator_source_sha256,
            feature_dataset_manifest_sha256=self.feature_snapshot.manifest_sha256,
            outcome_dataset_manifest_sha256=self.outcome_snapshot.manifest_sha256,
            input_manifest_sha256=canonical_sha256(identity_manifest),
        )

    def as_dict(
        self,
        *,
        profile_sha256: str,
        evaluation_parameters: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        identity_manifest = self.identity_input_manifest(evaluation_parameters=evaluation_parameters)
        return {
            "evaluation_id": self.evaluation_id(
                profile_sha256=profile_sha256,
                evaluation_parameters=evaluation_parameters,
            ),
            "run_id": self.run_id,
            "evaluator_source_sha256": self.evaluator_source_sha256,
            "feature_snapshot": asdict(self.feature_snapshot),
            "outcome_snapshot": asdict(self.outcome_snapshot),
            "overlap_receipt": self.overlap_receipt.as_dict(),
            "input_manifest": identity_manifest,
            "input_manifest_sha256": canonical_sha256(identity_manifest),
        }


def data_action(
    *,
    action: str,
    recoverable_family: str,
    source_candidates: tuple[str, ...],
    required_fields: tuple[str, ...],
    time_range: Mapping[str, Any],
    historical_backfill: bool,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": action,
        "source_candidates": list(source_candidates),
        "required_fields": list(required_fields),
        "time_range": dict(time_range),
        "historical_backfill": bool(historical_backfill),
        "recoverable_family": recoverable_family,
    }
    payload.update(extra)
    return payload


def _canonical_date(value: str):
    from datetime import date

    return date.fromisoformat(str(value))


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def typed_null(field_name: str) -> dict[str, str]:
    return {"type": "explicit_null", "field": field_name}


def canonical_input_manifest(values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a complete manifest where absent inputs remain identity-bearing."""

    manifest: dict[str, Any] = {}
    for field_name in INPUT_ARTIFACT_FIELDS:
        value = values.get(field_name)
        manifest[field_name] = value if value not in (None, "") else typed_null(field_name)
    extra = sorted(set(values) - set(INPUT_ARTIFACT_FIELDS))
    for field_name in extra:
        value = values[field_name]
        manifest[field_name] = value if value not in (None, "") else typed_null(field_name)
    return manifest


def build_evaluation_id(
    *,
    run_id: str,
    profile_sha256: str,
    evaluator_source_sha256: str,
    feature_dataset_manifest_sha256: str | None,
    outcome_dataset_manifest_sha256: str | None,
    input_manifest_sha256: str,
) -> str:
    if not run_id or not profile_sha256 or not evaluator_source_sha256 or not input_manifest_sha256:
        raise QELongTrendError(
            QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING,
            "evaluation identity requires run, profile, evaluator, and input manifest hashes",
        )
    payload = {
        "run_id": run_id,
        "profile_sha256": profile_sha256,
        "evaluator_source_sha256": evaluator_source_sha256,
        "feature_dataset_manifest_sha256": (
            feature_dataset_manifest_sha256
            if feature_dataset_manifest_sha256
            else typed_null("feature_dataset_manifest_sha256")
        ),
        "outcome_dataset_manifest_sha256": (
            outcome_dataset_manifest_sha256
            if outcome_dataset_manifest_sha256
            else typed_null("outcome_dataset_manifest_sha256")
        ),
        "input_manifest_sha256": input_manifest_sha256,
    }
    return f"qelt_{canonical_sha256(payload)}"


def empty_family_statuses() -> dict[str, FamilyEvidenceStatus]:
    return {
        name: FamilyEvidenceStatus(
            status=FamilyComputationStatus.NOT_COMPUTABLE,
            missing_inputs=("not_evaluated",),
        )
        for name in FAMILY_NAMES
    }
