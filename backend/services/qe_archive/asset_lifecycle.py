"""Deterministic QE experiment value, warehouse and asset lifecycle contracts.

The module is deliberately side-effect free.  It decides whether one minimum
evaluable QE result is an A/B/C research fact or an X control-plane event and
builds the digests used for exact-duplicate detection.  Persistence, CAS
publication and workspace cleanup are separate steps.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .models import normalize_json, sha256_json


LIFECYCLE_SCHEMA_VERSION = "qe_asset_lifecycle_v1"
FORMAL_PURPOSES = frozenset({"research", "production_research", "evolution", "comparison"})
X_PURPOSES = frozenset({"validation", "fixture", "smoke", "test", "unit_test", "integration_test"})
SUCCESS_STATUSES = frozenset({"completed", "succeeded", "success", "finished"})
FAILURE_STATUSES = frozenset({"failed", "cancelled", "canceled", "timeout", "interrupted"})


class QEValueClass(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    X = "X"
    CLASSIFICATION_PENDING = "classification_pending"


class QEWarehouseStatus(str, Enum):
    NOT_ELIGIBLE = "not_eligible"
    PENDING = "pending"
    PERSISTED = "persisted"
    FAILED = "failed"


class QEAssetStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    PUBLISHED = "published"
    PARTIAL = "partial"
    FAILED = "failed"


class QEWorkspaceStatus(str, Enum):
    ACTIVE = "active"
    GRACE_PERIOD = "grace_period"
    CLEANUP_PENDING = "cleanup_pending"
    CLEANED = "cleaned"
    CLEANUP_INCOMPLETE = "cleanup_incomplete"


@dataclass(frozen=True)
class QEAssetLifecycleDecision:
    value_class: QEValueClass
    reason_code: str
    business_identity_sha256: str | None
    result_digest_sha256: str | None
    identity_complete: bool
    result_evaluable: bool
    result_digest_authoritative: bool
    warehouse_status: QEWarehouseStatus
    asset_status: QEAssetStatus
    workspace_status: QEWorkspaceStatus
    duplicate_of: str | None = None
    retention_class: str = "diagnostic_short"
    protected_owner_count: int = 0
    asset_reason_code: str | None = None
    classification_basis: tuple[str, ...] = ()
    classified_at: str | None = None

    @property
    def archive_eligible(self) -> bool:
        return self.value_class in {QEValueClass.A, QEValueClass.B, QEValueClass.C}

    def with_persistence(
        self,
        *,
        warehouse_status: QEWarehouseStatus,
        asset_status: QEAssetStatus | None = None,
        workspace_status: QEWorkspaceStatus | None = None,
        protected_owner_count: int | None = None,
        asset_reason_code: str | None = None,
    ) -> "QEAssetLifecycleDecision":
        return replace(
            self,
            warehouse_status=warehouse_status,
            asset_status=asset_status or self.asset_status,
            workspace_status=workspace_status or self.workspace_status,
            protected_owner_count=(
                self.protected_owner_count
                if protected_owner_count is None
                else protected_owner_count
            ),
            asset_reason_code=(
                self.asset_reason_code if asset_reason_code is None else asset_reason_code
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "value_class": self.value_class.value,
            "reason_code": self.reason_code,
            "business_identity_sha256": self.business_identity_sha256,
            "result_digest_sha256": self.result_digest_sha256,
            "identity_complete": self.identity_complete,
            "result_evaluable": self.result_evaluable,
            "result_digest_authoritative": self.result_digest_authoritative,
            "warehouse_status": self.warehouse_status.value,
            "asset_status": self.asset_status.value,
            "workspace_status": self.workspace_status.value,
            "duplicate_of": self.duplicate_of,
            "retention_class": self.retention_class,
            "protected_owner_count": self.protected_owner_count,
            "asset_reason_code": self.asset_reason_code,
            "classification_basis": list(self.classification_basis),
            "classified_at": self.classified_at,
        }


def classify_qe_result(
    payload: Mapping[str, Any],
    *,
    duplicate_of: str | None = None,
    now: datetime | None = None,
) -> QEAssetLifecycleDecision:
    """Classify one minimum evaluable result without any database writes."""

    data = dict(payload)
    registration = _registration(data)
    purpose = str(registration.get("purpose") or _first(data, "purpose") or "research").strip().lower()
    status = str(data.get("status") or "completed").strip().lower()
    metrics = _metrics(data)
    result_evaluable = _has_finite_metric(metrics)
    identity = build_business_identity(data)
    identity_complete = _identity_complete(data, identity)
    identity_digest = sha256_json(identity) if identity_complete else None
    result_digest = build_result_digest(data) if result_evaluable else None
    result_digest_authoritative = _result_digest_authoritative(data)
    classified_at = (now or datetime.now(timezone.utc)).isoformat()

    if purpose in X_PURPOSES:
        return _x_decision(
            reason_code=f"qe_lifecycle_purpose_{purpose}",
            identity_digest=identity_digest,
            result_digest=result_digest,
            identity_complete=identity_complete,
            result_evaluable=result_evaluable,
            result_digest_authoritative=result_digest_authoritative,
            basis=(f"purpose:{purpose}",),
            classified_at=classified_at,
        )
    technical_invalid_reason = _technical_invalid_reason(data)
    if technical_invalid_reason:
        return _x_decision(
            reason_code=f"qe_lifecycle_technical_invalid_{technical_invalid_reason}",
            identity_digest=identity_digest,
            result_digest=result_digest,
            identity_complete=identity_complete,
            result_evaluable=result_evaluable,
            result_digest_authoritative=result_digest_authoritative,
            basis=("technical_validity:false", f"invalid_reason:{technical_invalid_reason}"),
            classified_at=classified_at,
        )
    if duplicate_of:
        return _x_decision(
            reason_code="qe_lifecycle_exact_duplicate",
            identity_digest=identity_digest,
            result_digest=result_digest,
            identity_complete=identity_complete,
            result_evaluable=result_evaluable,
            result_digest_authoritative=result_digest_authoritative,
            basis=("exact_business_identity", "exact_result_digest"),
            duplicate_of=duplicate_of,
            classified_at=classified_at,
        )
    if status in FAILURE_STATUSES and not result_evaluable:
        return _x_decision(
            reason_code="qe_lifecycle_terminal_without_result",
            identity_digest=identity_digest,
            result_digest=None,
            identity_complete=identity_complete,
            result_evaluable=False,
            result_digest_authoritative=False,
            basis=(f"status:{status}", "no_evaluable_result"),
            classified_at=classified_at,
        )
    if status not in SUCCESS_STATUSES and not (result_evaluable and _is_child(data)):
        return QEAssetLifecycleDecision(
            value_class=QEValueClass.CLASSIFICATION_PENDING,
            reason_code="qe_lifecycle_not_terminal",
            business_identity_sha256=identity_digest,
            result_digest_sha256=result_digest,
            identity_complete=identity_complete,
            result_evaluable=result_evaluable,
            result_digest_authoritative=result_digest_authoritative,
            warehouse_status=QEWarehouseStatus.PENDING,
            asset_status=QEAssetStatus.PENDING,
            workspace_status=QEWorkspaceStatus.ACTIVE,
            classification_basis=(f"status:{status or 'unknown'}",),
            classified_at=classified_at,
        )
    if not result_evaluable:
        return _x_decision(
            reason_code="qe_lifecycle_no_evaluable_result",
            identity_digest=identity_digest,
            result_digest=None,
            identity_complete=identity_complete,
            result_evaluable=False,
            result_digest_authoritative=False,
            basis=("no_evaluable_result",),
            classified_at=classified_at,
        )
    if not identity_complete:
        return QEAssetLifecycleDecision(
            value_class=QEValueClass.CLASSIFICATION_PENDING,
            reason_code="qe_lifecycle_identity_incomplete",
            business_identity_sha256=None,
            result_digest_sha256=result_digest,
            identity_complete=False,
            result_evaluable=True,
            result_digest_authoritative=result_digest_authoritative,
            warehouse_status=QEWarehouseStatus.PENDING,
            asset_status=QEAssetStatus.PENDING,
            workspace_status=QEWorkspaceStatus.ACTIVE,
            classification_basis=("identity_incomplete",),
            classified_at=classified_at,
        )

    value_class, class_basis = _value_class(data, registration)
    publish_manifest = _asset_publish_manifest(data)
    asset_required = value_class in {QEValueClass.A, QEValueClass.B} or bool(publish_manifest)
    if publish_manifest:
        asset_reason_code = "qe_asset_publish_pending"
    elif value_class in {QEValueClass.A, QEValueClass.B}:
        asset_reason_code = "qe_asset_publish_manifest_missing"
    else:
        asset_reason_code = None
    retention_class = {
        QEValueClass.A: "protected",
        QEValueClass.B: "research_180d",
        QEValueClass.C: "research_180d",
    }[value_class]
    return QEAssetLifecycleDecision(
        value_class=value_class,
        reason_code=f"qe_lifecycle_valid_unique_{value_class.value.lower()}",
        business_identity_sha256=identity_digest,
        result_digest_sha256=result_digest,
        identity_complete=True,
        result_evaluable=True,
        result_digest_authoritative=result_digest_authoritative,
        warehouse_status=QEWarehouseStatus.PENDING,
        asset_status=QEAssetStatus.PENDING if asset_required else QEAssetStatus.NOT_REQUIRED,
        workspace_status=QEWorkspaceStatus.GRACE_PERIOD,
        retention_class=retention_class,
        asset_reason_code=asset_reason_code,
        classification_basis=tuple(class_basis),
        classified_at=classified_at,
    )


def build_business_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build the full business identity while excluding paths and timestamps."""

    data = dict(payload)
    config = _mapping(data.get("config"))
    raw_config = _mapping(data.get("raw_config"))
    registration = _registration(data)
    return normalize_json(
        {
            "schema_version": "qe_business_identity_v1",
            "run_type": data.get("run_type"),
            "model_type": data.get("model_type") or _first(config, "model_type", "model_id"),
            "factor_list": data.get("factor_list") or config.get("factor_list"),
            "config": _business_projection(config),
            "raw_config": _business_projection(raw_config),
            "dataset_release_id": _first_recursive(
                (registration, config, raw_config, data),
                "dataset_release_id",
                "release_id",
                "qlib_dataset_version",
                "dataset_snapshot_id",
            ),
            "dataset_manifest_sha256": _first_recursive(
                (registration, config, raw_config, data),
                "dataset_manifest_sha256",
                "manifest_sha256",
                "data_version_hash",
            ),
            "seed": _first_recursive((registration, config, raw_config, data), "random_seed", "seed"),
            "label_horizon": _first_recursive((registration, config, raw_config, data), "label_horizon"),
            "universe": _first_recursive(
                (registration, config, raw_config, data),
                "universe_pool_ids",
                "universe_pool_id",
                "stock_pool",
                "market",
                "universe",
            ),
            "benchmark": _first_recursive((registration, config, raw_config, data), "benchmark"),
            "execution_algo": _first_recursive(
                (registration, config, raw_config, data),
                "execution_algo",
                "execution_algorithm",
            ),
            "hmm": _policy_projection(config, raw_config, prefix="hmm"),
            "sector_blacklist": _policy_projection(config, raw_config, prefix="sector_blacklist"),
        }
    )


def build_result_digest(payload: Mapping[str, Any]) -> str:
    explicit = _explicit_result_digest(payload)
    if explicit:
        return explicit
    metrics = _metrics(payload)
    artifact_rows: list[dict[str, Any]] = []
    for item in _artifact_items(payload):
        digest = str(item.get("sha256") or "").strip().lower()
        if digest:
            artifact_rows.append(
                {
                    "artifact_type": item.get("artifact_type") or item.get("type"),
                    "artifact_name": item.get("artifact_name") or item.get("name"),
                    "sha256": digest,
                    "size_bytes": item.get("size_bytes"),
                }
            )
    return sha256_json(
        {
            "schema_version": "qe_business_result_digest_v1",
            "metrics": _business_projection(metrics),
            "artifacts": sorted(
                artifact_rows,
                key=lambda row: (str(row.get("artifact_type")), str(row.get("artifact_name"))),
            ),
        }
    )


def attach_lifecycle(
    payload: Mapping[str, Any],
    decision: QEAssetLifecycleDecision,
) -> dict[str, Any]:
    prepared = dict(payload)
    prepared["_qe_asset_lifecycle"] = decision.as_dict()
    return prepared


def lifecycle_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(payload.get("_qe_asset_lifecycle"))


def has_lifecycle_evidence(payload: Mapping[str, Any]) -> bool:
    config = _mapping(payload.get("config"))
    raw_config = _mapping(payload.get("raw_config"))
    runtime_flags = _mapping(config.get("runtime_flags"))
    custom_params = _mapping(config.get("custom_params"))
    raw_custom_params = _mapping(raw_config.get("custom_params"))
    return bool(
        payload.get("_qe_asset_lifecycle")
        or payload.get("metrics")
        or payload.get("result_metrics")
        or payload.get("_qe_run_registration")
        or config.get("_qe_run_registration")
        or runtime_flags.get("_qe_run_registration")
        or custom_params.get("_qe_run_registration")
        or raw_custom_params.get("_qe_run_registration")
    )


def _x_decision(
    *,
    reason_code: str,
    identity_digest: str | None,
    result_digest: str | None,
    identity_complete: bool,
    result_evaluable: bool,
    result_digest_authoritative: bool,
    basis: tuple[str, ...],
    classified_at: str,
    duplicate_of: str | None = None,
) -> QEAssetLifecycleDecision:
    return QEAssetLifecycleDecision(
        value_class=QEValueClass.X,
        reason_code=reason_code,
        business_identity_sha256=identity_digest,
        result_digest_sha256=result_digest,
        identity_complete=identity_complete,
        result_evaluable=result_evaluable,
        result_digest_authoritative=result_digest_authoritative,
        warehouse_status=QEWarehouseStatus.NOT_ELIGIBLE,
        asset_status=QEAssetStatus.NOT_REQUIRED,
        workspace_status=QEWorkspaceStatus.GRACE_PERIOD,
        duplicate_of=duplicate_of,
        retention_class="diagnostic_short",
        classification_basis=basis,
        classified_at=classified_at,
    )


def _value_class(
    payload: Mapping[str, Any],
    registration: Mapping[str, Any],
) -> tuple[QEValueClass, list[str]]:
    explicit = str(
        _first_recursive(
            (registration, _mapping(payload.get("config")), payload),
            "value_class",
            "archive_value_class",
        )
        or ""
    ).upper()
    if explicit in {"A", "B", "C"}:
        return QEValueClass(explicit), ["explicit_predeclared_class"]
    role = str(
        _first_recursive(
            (registration, _mapping(payload.get("config")), payload),
            "research_role",
            "evidence_role",
            "experiment_role",
        )
        or ""
    ).strip().lower()
    if role in {"anchor", "pareto", "champion", "strategy_package_candidate"}:
        return QEValueClass.A, [f"role:{role}"]
    if role in {
        "matched_baseline",
        "matched_variant",
        "negative_control",
        "seed_evidence",
        "vintage_evidence",
        "pool_evidence",
    }:
        return QEValueClass.B, [f"role:{role}"]
    return QEValueClass.C, ["valid_unique_default_minimum_c"]


def _identity_complete(payload: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    source_identity = bool(payload.get("source_id") or payload.get("experiment_id") or payload.get("task_id"))
    config = _mapping(payload.get("config"))
    factors_or_model = bool(
        payload.get("factor_list")
        or config.get("factor_list")
        or payload.get("model_type")
        or _first_recursive((config,), "model_id", "model_type")
    )
    dataset_identity = bool(identity.get("dataset_release_id") or identity.get("dataset_manifest_sha256"))
    registration = _registration(payload)
    formal_registration = str(registration.get("purpose") or "").strip().lower() in FORMAL_PURPOSES
    # Legacy formal records predate dataset-profile registration. They remain
    # classifiable when their captured config is non-empty; new registered
    # records fail closed if the dataset identity is absent.
    return source_identity and bool(config) and factors_or_model and (dataset_identity or not formal_registration)


def _registration(payload: Mapping[str, Any]) -> dict[str, Any]:
    config = _mapping(payload.get("config"))
    raw_config = _mapping(payload.get("raw_config"))
    candidates = (
        payload.get("_qe_run_registration"),
        config.get("_qe_run_registration"),
        _mapping(config.get("runtime_flags")).get("_qe_run_registration"),
        _mapping(config.get("custom_params")).get("_qe_run_registration"),
        _mapping(raw_config.get("custom_params")).get("_qe_run_registration"),
    )
    for candidate in candidates:
        mapped = _mapping(candidate)
        if mapped:
            return mapped
    return {}


def _metrics(payload: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(payload.get("metrics") or payload.get("result_metrics"))
    return metrics


def _has_finite_metric(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if isinstance(value, Mapping):
        return any(_has_finite_metric(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_has_finite_metric(item) for item in value)
    return False


def _is_child(payload: Mapping[str, Any]) -> bool:
    return bool(payload.get("loop_id") or payload.get("source_sub_id") or payload.get("parent_experiment_id"))


def _technical_invalid_reason(payload: Mapping[str, Any]) -> str | None:
    if payload.get("research_valid") is False:
        return str(payload.get("invalid_reason") or "source_marked_invalid").strip().lower()
    config = _mapping(payload.get("config"))
    context = _mapping(payload.get("data_context") or config.get("data_context"))
    freq = str(context.get("freq") or payload.get("freq") or "").strip().lower()
    authoritative = context.get("limit_suspend_authoritative")
    if freq in {"day", "daily", "1day", "1d"} and authoritative is False:
        return "daily_backtest_without_authoritative_limit_suspend"
    return None


def _asset_publish_manifest(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("qe_asset_publish_manifest")
    if raw is None:
        raw = _mapping(payload.get("config")).get("qe_asset_publish_manifest")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _artifact_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    manifest = _mapping(payload.get("prediction_store_manifest"))
    raw = manifest.get("artifacts") or payload.get("artifact_manifest") or []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _explicit_result_digest(payload: Mapping[str, Any]) -> str | None:
    candidates = (
        payload.get("result_digest_sha256"),
        payload.get("result_fingerprint_sha256"),
        _metrics(payload).get("result_digest_sha256"),
    )
    for candidate in candidates:
        digest = str(candidate or "").strip().lower()
        if len(digest) == 64 and all(ch in "0123456789abcdef" for ch in digest):
            return digest
    return None


def _result_digest_authoritative(payload: Mapping[str, Any]) -> bool:
    if _explicit_result_digest(payload):
        return True
    artifacts = _artifact_items(payload)
    if not artifacts:
        return False
    digests = [str(item.get("sha256") or "").strip().lower() for item in artifacts]
    return bool(digests) and all(
        len(digest) == 64 and all(ch in "0123456789abcdef" for ch in digest)
        for digest in digests
    )


_EPHEMERAL_KEYS = frozenset(
    {
        "_qe_asset_lifecycle",
        "_qe_artifact_retention",
        "workspace_path",
        "worktree",
        "log_path",
        "source_path",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "finished_at",
        "node_id",
        "execution_node_id",
        "task_id",
        "loop_id",
        "experiment_id",
        "run_id",
    }
)


def _business_projection(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _business_projection(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _EPHEMERAL_KEYS
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_business_projection(item) for item in value]
    return normalize_json(value)


def _policy_projection(config: Mapping[str, Any], raw_config: Mapping[str, Any], *, prefix: str) -> Any:
    keys = tuple(
        key
        for source in (config, raw_config)
        for key in source
        if str(key).lower().startswith(prefix)
    )
    return {
        key: _business_projection(_first_recursive((config, raw_config), key))
        for key in sorted(set(keys))
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if source.get(key) not in (None, "", [], {}):
            return source.get(key)
    return None


def _first_recursive(sources: Sequence[Mapping[str, Any]], *keys: str) -> Any:
    for source in sources:
        found = _find_recursive(source, set(keys))
        if found not in (None, "", [], {}):
            return found
    return None


def _find_recursive(value: Any, keys: set[str]) -> Any:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in keys and item not in (None, "", [], {}):
                return item
        for item in value.values():
            found = _find_recursive(item, keys)
            if found not in (None, "", [], {}):
                return found
    return None
