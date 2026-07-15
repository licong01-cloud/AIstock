"""Fail-closed LEGACY_B0 to B0_QUOTE_V2 binding route migration.

The source binding stays historically immutable.  A successful migration only
closes its effective window and inserts one immutable B0 successor carrying a
rebuildable migration marker.  No method in this module submits or cancels an
order.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import psycopg2
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.miniqmt_quote_contract_config import QuoteContractPolicy
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    QUOTE_CONTROL_BINDING_KEY,
    QuoteControlBindingV1,
)
from backend.services.trading_core.errors import InvalidStateTransitionError

from .models import (
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    assert_binding_payload_boundary,
    canonical_json_sha256,
)

INVENTORY_SCHEMA_VERSION = "miniqmt_route_migration_inventory_v1"
MARKER_SCHEMA_VERSION = "miniqmt_route_migration_marker_v1"
RECEIPT_SCHEMA_VERSION = "miniqmt_route_migration_receipt_v1"
RUNTIME_OWNER = "MiniQMTExecutionRuntime"
DEFAULT_RUNTIME_INVENTORY_LIMIT = 500


class MiniQMTRouteMigrationError(InvalidStateTransitionError):
    error_code = "MINIQMT_ROUTE_MIGRATION_FAILED"


def _required_text(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _required_sha256(value: Any, *, field_name: str) -> str:
    text = _required_text(value, field_name=field_name)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field_name} must be a lowercase sha256")
    return text


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class MiniQMTRouteMigrationInventoryV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["miniqmt_route_migration_inventory_v1"] = INVENTORY_SCHEMA_VERSION
    source_binding_id: str
    source_binding_hash: str
    target_release_id: str
    target_release_hash: str
    effective_trade_date: date
    runtime_ids_examined: tuple[str, ...] = ()
    active_parent_ids: tuple[str, ...] = ()
    active_child_order_ids: tuple[str, ...] = ()
    broker_open_order_ids: tuple[str, ...] = ()
    broker_attribution_conflicts: tuple[str, ...] = ()
    observed_at_utc: datetime
    inventory_sha256: str

    @field_validator("source_binding_id", "target_release_id")
    @classmethod
    def _text(cls, value: str, info: Any) -> str:
        return _required_text(value, field_name=info.field_name)

    @field_validator("source_binding_hash", "target_release_hash", "inventory_sha256")
    @classmethod
    def _sha(cls, value: str, info: Any) -> str:
        return _required_sha256(value, field_name=info.field_name)

    @field_validator("observed_at_utc")
    @classmethod
    def _observed_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="observed_at_utc")

    @model_validator(mode="after")
    def _hash_matches(self) -> "MiniQMTRouteMigrationInventoryV1":
        expected = canonical_json_sha256(self.canonical_payload())
        if self.inventory_sha256 != expected:
            raise ValueError("inventory_sha256 does not match canonical migration inventory")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"inventory_sha256"})

    @classmethod
    def build(cls, **values: Any) -> "MiniQMTRouteMigrationInventoryV1":
        normalized = {
            **values,
            "runtime_ids_examined": tuple(sorted(set(values.get("runtime_ids_examined") or ()))),
            "active_parent_ids": tuple(sorted(set(values.get("active_parent_ids") or ()))),
            "active_child_order_ids": tuple(sorted(set(values.get("active_child_order_ids") or ()))),
            "broker_open_order_ids": tuple(sorted(set(values.get("broker_open_order_ids") or ()))),
            "broker_attribution_conflicts": tuple(
                sorted(set(values.get("broker_attribution_conflicts") or ()))
            ),
        }
        candidate = cls.model_construct(**normalized, inventory_sha256="")
        return cls(**normalized, inventory_sha256=canonical_json_sha256(candidate.canonical_payload()))


class MiniQMTRouteMigrationMarkerV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["miniqmt_route_migration_marker_v1"] = MARKER_SCHEMA_VERSION
    source_binding_id: str
    source_binding_hash: str
    target_release_id: str
    target_release_hash: str
    effective_trade_date: date
    source_effective_to: date
    inventory_sha256: str
    inventory_observed_at_utc: datetime
    operator: str
    applied_at_utc: datetime
    marker_sha256: str

    @field_validator("source_binding_id", "target_release_id", "operator")
    @classmethod
    def _text(cls, value: str, info: Any) -> str:
        return _required_text(value, field_name=info.field_name)

    @field_validator(
        "source_binding_hash", "target_release_hash", "inventory_sha256", "marker_sha256"
    )
    @classmethod
    def _sha(cls, value: str, info: Any) -> str:
        return _required_sha256(value, field_name=info.field_name)

    @field_validator("inventory_observed_at_utc", "applied_at_utc")
    @classmethod
    def _time(cls, value: datetime, info: Any) -> datetime:
        return _aware_utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _valid(self) -> "MiniQMTRouteMigrationMarkerV1":
        if self.source_effective_to != self.effective_trade_date - timedelta(days=1):
            raise ValueError("source_effective_to must be the day before effective_trade_date")
        if self.marker_sha256 != canonical_json_sha256(self.canonical_payload()):
            raise ValueError("marker_sha256 does not match canonical migration marker")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"marker_sha256"})

    @classmethod
    def build(
        cls,
        *,
        inventory: MiniQMTRouteMigrationInventoryV1,
        operator: str,
        applied_at_utc: datetime,
    ) -> "MiniQMTRouteMigrationMarkerV1":
        values = {
            "source_binding_id": inventory.source_binding_id,
            "source_binding_hash": inventory.source_binding_hash,
            "target_release_id": inventory.target_release_id,
            "target_release_hash": inventory.target_release_hash,
            "effective_trade_date": inventory.effective_trade_date,
            "source_effective_to": inventory.effective_trade_date - timedelta(days=1),
            "inventory_sha256": inventory.inventory_sha256,
            "inventory_observed_at_utc": inventory.observed_at_utc,
            "operator": operator,
            "applied_at_utc": applied_at_utc,
        }
        candidate = cls.model_construct(**values, marker_sha256="")
        return cls(**values, marker_sha256=canonical_json_sha256(candidate.canonical_payload()))


class MiniQMTRouteMigrationReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["miniqmt_route_migration_receipt_v1"] = RECEIPT_SCHEMA_VERSION
    migration_id: str
    source_binding_id: str
    source_binding_hash: str
    target_binding_id: str
    target_binding_hash: str
    target_release_id: str
    target_release_hash: str
    effective_trade_date: date
    source_effective_to: date
    inventory_sha256: str
    marker_sha256: str
    runtime_owner: Literal["MiniQMTExecutionRuntime"] = RUNTIME_OWNER
    source_control_revision: Literal["LEGACY_B0"] = "LEGACY_B0"
    target_control_revision: Literal["B0_QUOTE_V2"] = "B0_QUOTE_V2"
    applied_at_utc: datetime
    receipt_sha256: str

    @field_validator(
        "source_binding_hash",
        "target_binding_hash",
        "target_release_hash",
        "inventory_sha256",
        "marker_sha256",
        "receipt_sha256",
    )
    @classmethod
    def _sha(cls, value: str, info: Any) -> str:
        return _required_sha256(value, field_name=info.field_name)

    @field_validator("applied_at_utc")
    @classmethod
    def _applied(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="applied_at_utc")

    @model_validator(mode="after")
    def _identity_matches(self) -> "MiniQMTRouteMigrationReceiptV1":
        payload = self.canonical_payload()
        digest = canonical_json_sha256(payload)
        if self.migration_id != f"mqrm_{digest[:24]}":
            raise ValueError("migration_id does not match canonical receipt")
        if self.receipt_sha256 != digest:
            raise ValueError("receipt_sha256 does not match canonical receipt")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"migration_id", "receipt_sha256"})

    @classmethod
    def build(cls, **values: Any) -> "MiniQMTRouteMigrationReceiptV1":
        candidate = cls.model_construct(**values, migration_id="", receipt_sha256="")
        digest = canonical_json_sha256(candidate.canonical_payload())
        return cls(**values, migration_id=f"mqrm_{digest[:24]}", receipt_sha256=digest)


class MiniQMTRouteMigrationPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    inventory: MiniQMTRouteMigrationInventoryV1
    target_binding: SimulationReleaseBinding
    marker: MiniQMTRouteMigrationMarkerV1


def _metadata_binding_id(metadata: Mapping[str, Any] | None) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    direct = str(metadata.get("binding_id") or "").strip()
    if direct:
        return direct
    for key in ("runtime_child_context", "managed_order_request", "broker_order"):
        nested = metadata.get(key)
        if isinstance(nested, Mapping):
            nested_id = _metadata_binding_id(nested)
            if nested_id:
                return nested_id
    return None


def _broker_order_id(row: Mapping[str, Any]) -> str | None:
    for key in ("broker_order_id", "order_id", "qmt_order_id", "native_order_id"):
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return None


def _broker_text(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return None


def _assert_target_release(source: SimulationReleaseBinding, target: StrategyRuntimeRelease) -> None:
    mismatches = {
        field_name: {"source": source_value, "target": target_value}
        for field_name, source_value, target_value in (
            ("package_id", source.package_id, target.package_id),
            ("manifest_sha256", source.manifest_sha256, target.manifest_sha256),
        )
        if source_value != target_value
    }
    if mismatches:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration target release changes frozen package identity",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_IDENTITY_CONFLICT",
                "mismatches": mismatches,
            },
        )
    execution_policy = target.release_config_json.get("execution_policy")
    policy_json = execution_policy.get("policy_json") if isinstance(execution_policy, Mapping) else None
    if not isinstance(policy_json, Mapping):
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration target release has no immutable execution policy",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_POLICY_INVALID"},
        )
    policy = QuoteContractPolicy.from_execution_policy(dict(policy_json))
    if policy.control_revision != ControlRevision.B0_QUOTE_V2.value:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration target release is not B0_QUOTE_V2",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_POLICY_INVALID",
                "control_revision": policy.control_revision,
            },
        )


def _assert_source_legacy(source: SimulationReleaseBinding) -> None:
    if source.broker_backend is not SimulationBrokerBackend.MINIQMT_SIM:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration source must use minqmt_sim",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_INVALID"},
        )
    control = QuoteControlBindingV1.from_binding_config(source.binding_config_json)
    if control.control_revision is not ControlRevision.LEGACY_B0:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration source must be LEGACY_B0",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_INVALID",
                "control_revision": control.control_revision.value,
            },
        )


def build_inventory(
    *,
    source_binding: SimulationReleaseBinding,
    target_release: StrategyRuntimeRelease,
    effective_trade_date: date,
    runtime_repository: Any,
    broker_open_orders: Sequence[Mapping[str, Any]],
    observed_at_utc: datetime,
    runtime_limit: int = DEFAULT_RUNTIME_INVENTORY_LIMIT,
) -> MiniQMTRouteMigrationInventoryV1:
    _assert_source_legacy(source_binding)
    _assert_target_release(source_binding, target_release)
    if source_binding.effective_from is not None and effective_trade_date <= source_binding.effective_from:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration effective date must follow the source binding start",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_EFFECTIVE_DATE_INVALID",
                "source_effective_from": source_binding.effective_from.isoformat(),
                "effective_trade_date": effective_trade_date.isoformat(),
            },
        )
    if runtime_limit <= 0:
        raise ValueError("runtime_limit must be positive")
    runtime_reader = getattr(runtime_repository, "list_runtimes_for_account", None)
    if not callable(runtime_reader):
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration requires bounded account-scoped runtime inventory",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_REPOSITORY_UNSUPPORTED"},
        )
    account_group_id = str(source_binding.account_group_id or source_binding.broker_account_id or "").strip()
    if not account_group_id:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration source binding has no account-group identity",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_SOURCE_INVALID"},
        )
    runtimes = list(runtime_reader(account_group_id=account_group_id, limit=runtime_limit + 1))
    if len(runtimes) > runtime_limit:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration runtime inventory exceeded its explicit bound",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_INVENTORY_LIMIT",
                "account_group_id": account_group_id,
                "runtime_limit": runtime_limit,
            },
        )

    runtime_ids: list[str] = []
    active_parent_ids: list[str] = []
    active_child_ids: list[str] = []
    source_broker_ids: set[str] = set()
    for runtime in runtimes:
        runtime_ids.append(str(runtime.runtime_id))
        active_algos = list(runtime_repository.list_algo_instances(runtime.runtime_id, active_only=True))
        active_children = list(runtime_repository.list_child_orders(runtime.runtime_id, active_only=True))
        historical_children = list(runtime_repository.list_child_orders(runtime.runtime_id, active_only=False))
        for algo in active_algos:
            if _metadata_binding_id(algo.metadata) == source_binding.binding_id:
                active_parent_ids.append(str(algo.parent_intent_id))
        for child in active_children:
            if _metadata_binding_id(child.metadata) == source_binding.binding_id:
                active_child_ids.append(str(child.child_order_id))
        for child in historical_children:
            if _metadata_binding_id(child.metadata) == source_binding.binding_id and child.broker_order_id:
                source_broker_ids.add(str(child.broker_order_id))

    open_order_ids: list[str] = []
    attribution_conflicts: list[str] = []
    expected_strategy = str(source_binding.strategy_name or "").strip()
    expected_prefix = str(source_binding.order_remark_prefix or "").strip()
    for index, raw_row in enumerate(broker_open_orders):
        if not isinstance(raw_row, Mapping):
            raise MiniQMTRouteMigrationError(
                "MiniQMT broker open-order inventory row must be an object",
                context={
                    "reason_code": "MINIQMT_ROUTE_MIGRATION_BROKER_SCHEMA_INVALID",
                    "row_index": index,
                },
            )
        row = dict(raw_row)
        order_id = _broker_order_id(row)
        if not order_id:
            raise MiniQMTRouteMigrationError(
                "MiniQMT broker open-order inventory row has no order id",
                context={
                    "reason_code": "MINIQMT_ROUTE_MIGRATION_BROKER_SCHEMA_INVALID",
                    "row_index": index,
                },
            )
        strategy_name = _broker_text(row, "strategy_name")
        order_remark = _broker_text(row, "order_remark", "remark")
        id_match = order_id in source_broker_ids
        strategy_match = bool(expected_strategy and strategy_name == expected_strategy)
        prefix_match = bool(expected_prefix and order_remark and order_remark.startswith(expected_prefix))
        if id_match or (strategy_match and prefix_match):
            open_order_ids.append(order_id)
        elif strategy_match != prefix_match:
            attribution_conflicts.append(order_id)

    return MiniQMTRouteMigrationInventoryV1.build(
        source_binding_id=source_binding.binding_id,
        source_binding_hash=source_binding.binding_hash or "",
        target_release_id=target_release.release_id,
        target_release_hash=target_release.release_hash or "",
        effective_trade_date=effective_trade_date,
        runtime_ids_examined=runtime_ids,
        active_parent_ids=active_parent_ids,
        active_child_order_ids=active_child_ids,
        broker_open_order_ids=open_order_ids,
        broker_attribution_conflicts=attribution_conflicts,
        observed_at_utc=observed_at_utc,
    )


def _assert_inventory_clear(inventory: MiniQMTRouteMigrationInventoryV1) -> None:
    blockers = {
        "active_parent_ids": list(inventory.active_parent_ids),
        "active_child_order_ids": list(inventory.active_child_order_ids),
        "broker_open_order_ids": list(inventory.broker_open_order_ids),
        "broker_attribution_conflicts": list(inventory.broker_attribution_conflicts),
    }
    blockers = {key: value for key, value in blockers.items() if value}
    if blockers:
        raise MiniQMTRouteMigrationError(
            "MiniQMT LEGACY_B0 binding cannot migrate while active or unattributed broker facts remain",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_ACTIVE_FACTS_PRESENT",
                "source_binding_id": inventory.source_binding_id,
                "inventory_sha256": inventory.inventory_sha256,
                "blockers": blockers,
                "broker_called": False,
            },
        )


def build_target_binding(
    *,
    source_binding: SimulationReleaseBinding,
    target_release: StrategyRuntimeRelease,
    marker: MiniQMTRouteMigrationMarkerV1,
) -> SimulationReleaseBinding:
    _assert_source_legacy(source_binding)
    _assert_target_release(source_binding, target_release)
    source_metadata = source_binding.binding_config_json.get("metadata")
    metadata = deepcopy(source_metadata) if isinstance(source_metadata, dict) else {}
    if "miniqmt_route_migration" in metadata:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration source metadata already owns a migration marker",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_MARKER_CONFLICT"},
        )
    metadata["miniqmt_route_migration"] = marker.model_dump(mode="json")
    quote_control = QuoteControlBindingV1(
        control_revision=ControlRevision.B0_QUOTE_V2,
        explicitly_configured=True,
    )
    binding_config: dict[str, Any] = {
        "schema_version": "simulation_release_binding_v1",
        "strategy_id": source_binding.strategy_id,
        "release_id": target_release.release_id,
        "release_hash": target_release.release_hash,
        "package_id": target_release.package_id,
        "manifest_sha256": target_release.manifest_sha256,
        "broker_backend": source_binding.broker_backend.value,
        "broker_account_id": source_binding.broker_account_id,
        "capital_allocation": float(source_binding.capital_allocation),
        "strategy_name": source_binding.strategy_name,
        "order_remark_prefix": source_binding.order_remark_prefix,
        "approval_state": source_binding.approval_state.value,
        "metadata": metadata,
        QUOTE_CONTROL_BINDING_KEY: quote_control.canonical_payload(),
    }
    if source_binding.account_group_id is not None:
        binding_config["account_group_id"] = source_binding.account_group_id
    if source_binding.strategy_slot_id is not None:
        binding_config["strategy_slot_id"] = source_binding.strategy_slot_id
    assert_binding_payload_boundary(binding_config, context={"source_binding_id": source_binding.binding_id})
    binding_hash = canonical_json_sha256(binding_config)
    return SimulationReleaseBinding(
        binding_id=f"simbind_{binding_hash[:16]}",
        strategy_id=source_binding.strategy_id,
        release_id=target_release.release_id,
        release_hash=target_release.release_hash or "",
        package_id=target_release.package_id,
        manifest_sha256=target_release.manifest_sha256,
        broker_backend=source_binding.broker_backend,
        broker_account_id=source_binding.broker_account_id,
        account_group_id=source_binding.account_group_id,
        strategy_slot_id=source_binding.strategy_slot_id,
        capital_allocation=source_binding.capital_allocation,
        strategy_name=source_binding.strategy_name,
        order_remark_prefix=source_binding.order_remark_prefix,
        effective_from=marker.effective_trade_date,
        effective_to=None,
        approval_state=source_binding.approval_state,
        binding_config_json=binding_config,
        binding_hash=binding_hash,
        created_by=marker.operator,
        created_reason="migrate LEGACY_B0 binding to B0_QUOTE_V2 canonical route",
        created_at=marker.applied_at_utc,
        updated_at=marker.applied_at_utc,
    )


def rebuild_receipt(
    *, source_binding: SimulationReleaseBinding, target_binding: SimulationReleaseBinding
) -> MiniQMTRouteMigrationReceiptV1:
    _assert_source_legacy(source_binding)
    target_control = QuoteControlBindingV1.from_binding_config(target_binding.binding_config_json)
    if target_control.control_revision is not ControlRevision.B0_QUOTE_V2:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration target readback is not B0_QUOTE_V2",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_READBACK_MISMATCH"},
        )
    metadata = target_binding.binding_config_json.get("metadata")
    raw_marker = metadata.get("miniqmt_route_migration") if isinstance(metadata, dict) else None
    try:
        marker = MiniQMTRouteMigrationMarkerV1.model_validate(raw_marker)
    except Exception as exc:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration target marker readback is invalid",
            context={"reason_code": "MINIQMT_ROUTE_MIGRATION_MARKER_INVALID"},
        ) from exc
    mismatches = {
        key: {"expected": expected, "actual": actual}
        for key, expected, actual in (
            ("source_binding_id", marker.source_binding_id, source_binding.binding_id),
            ("source_binding_hash", marker.source_binding_hash, source_binding.binding_hash),
            ("target_release_id", marker.target_release_id, target_binding.release_id),
            ("target_release_hash", marker.target_release_hash, target_binding.release_hash),
            ("source_effective_to", marker.source_effective_to, source_binding.effective_to),
            ("target_effective_from", marker.effective_trade_date, target_binding.effective_from),
        )
        if expected != actual
    }
    equivalent_fields = (
        "strategy_id",
        "package_id",
        "manifest_sha256",
        "broker_backend",
        "broker_account_id",
        "account_group_id",
        "strategy_slot_id",
        "capital_allocation",
        "strategy_name",
        "order_remark_prefix",
        "approval_state",
    )
    for field_name in equivalent_fields:
        source_value = getattr(source_binding, field_name)
        target_value = getattr(target_binding, field_name)
        if source_value != target_value:
            mismatches[field_name] = {"expected": source_value, "actual": target_value}
    if mismatches:
        raise MiniQMTRouteMigrationError(
            "MiniQMT route migration independent readback differs from its immutable identities",
            context={
                "reason_code": "MINIQMT_ROUTE_MIGRATION_READBACK_MISMATCH",
                "mismatches": mismatches,
            },
        )
    return MiniQMTRouteMigrationReceiptV1.build(
        source_binding_id=source_binding.binding_id,
        source_binding_hash=source_binding.binding_hash or "",
        target_binding_id=target_binding.binding_id,
        target_binding_hash=target_binding.binding_hash or "",
        target_release_id=target_binding.release_id,
        target_release_hash=target_binding.release_hash,
        effective_trade_date=marker.effective_trade_date,
        source_effective_to=marker.source_effective_to,
        inventory_sha256=marker.inventory_sha256,
        marker_sha256=marker.marker_sha256,
        applied_at_utc=marker.applied_at_utc,
    )


_RETRYABLE_DB_ERRORS = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
    psycopg2.errors.lookup("40001"),  # serialization_failure
    psycopg2.errors.lookup("40P01"),  # deadlock_detected
    psycopg2.errors.lookup("55P03"),  # lock_not_available
    psycopg2.errors.lookup("57014"),  # query_canceled / statement timeout
)


class MiniQMTRouteMigrationService:
    def __init__(
        self,
        *,
        simulation_repository: Any,
        runtime_repository: Any,
        broker_open_order_reader: Callable[[], Sequence[Mapping[str, Any]]],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        runtime_limit: int = DEFAULT_RUNTIME_INVENTORY_LIMIT,
        max_transaction_attempts: int = 3,
    ) -> None:
        if max_transaction_attempts != 3:
            raise ValueError("MiniQMT route migration transaction attempts are fixed at 3")
        self.simulation_repository = simulation_repository
        self.runtime_repository = runtime_repository
        self.broker_open_order_reader = broker_open_order_reader
        self.clock = clock
        self.runtime_limit = runtime_limit
        self.max_transaction_attempts = max_transaction_attempts

    def plan(
        self,
        *,
        source_binding_id: str,
        target_release_id: str,
        effective_trade_date: date,
        operator: str,
    ) -> MiniQMTRouteMigrationPlanV1:
        source = self.simulation_repository.get_simulation_release_binding(source_binding_id)
        target_release = self.simulation_repository.get_strategy_runtime_release(target_release_id)
        observed_at = _aware_utc(self.clock(), field_name="clock")
        broker_orders = self.broker_open_order_reader()
        if broker_orders is None:
            raise MiniQMTRouteMigrationError(
                "MiniQMT broker open-order readback returned no inventory",
                context={"reason_code": "MINIQMT_ROUTE_MIGRATION_BROKER_READBACK_MISSING"},
            )
        inventory = build_inventory(
            source_binding=source,
            target_release=target_release,
            effective_trade_date=effective_trade_date,
            runtime_repository=self.runtime_repository,
            broker_open_orders=broker_orders,
            observed_at_utc=observed_at,
            runtime_limit=self.runtime_limit,
        )
        _assert_inventory_clear(inventory)
        marker = MiniQMTRouteMigrationMarkerV1.build(
            inventory=inventory,
            operator=_required_text(operator, field_name="operator"),
            applied_at_utc=observed_at,
        )
        return MiniQMTRouteMigrationPlanV1(
            inventory=inventory,
            marker=marker,
            target_binding=build_target_binding(
                source_binding=source,
                target_release=target_release,
                marker=marker,
            ),
        )

    def apply(
        self,
        *,
        source_binding_id: str,
        target_release_id: str,
        effective_trade_date: date,
        operator: str,
    ) -> MiniQMTRouteMigrationReceiptV1:
        existing = self.simulation_repository.find_miniqmt_route_migration_target(
            source_binding_id=source_binding_id,
            effective_trade_date=effective_trade_date,
        )
        if existing is not None:
            if existing.release_id != target_release_id:
                raise MiniQMTRouteMigrationError(
                    "MiniQMT route migration already targets a different release",
                    context={
                        "reason_code": "MINIQMT_ROUTE_MIGRATION_TARGET_CONFLICT",
                        "existing_target_release_id": existing.release_id,
                        "requested_target_release_id": target_release_id,
                    },
                )
            return rebuild_receipt(
                source_binding=self.simulation_repository.get_simulation_release_binding(source_binding_id),
                target_binding=existing,
            )

        plan = self.plan(
            source_binding_id=source_binding_id,
            target_release_id=target_release_id,
            effective_trade_date=effective_trade_date,
            operator=operator,
        )
        source_effective_to = effective_trade_date - timedelta(days=1)
        last_retryable: BaseException | None = None
        for attempt in range(1, self.max_transaction_attempts + 1):
            try:
                self.simulation_repository.migrate_miniqmt_binding_route(
                    source_binding_id=source_binding_id,
                    expected_source_binding_hash=plan.inventory.source_binding_hash,
                    source_effective_to=source_effective_to,
                    target_binding=plan.target_binding,
                )
                break
            except _RETRYABLE_DB_ERRORS as exc:
                last_retryable = exc
                committed = self.simulation_repository.find_miniqmt_route_migration_target(
                    source_binding_id=source_binding_id,
                    effective_trade_date=effective_trade_date,
                )
                if committed is not None:
                    return rebuild_receipt(
                        source_binding=self.simulation_repository.get_simulation_release_binding(
                            source_binding_id
                        ),
                        target_binding=committed,
                    )
                if attempt == self.max_transaction_attempts:
                    raise MiniQMTRouteMigrationError(
                        "MiniQMT route migration exhausted its bounded transaction retries",
                        context={
                            "reason_code": "MINIQMT_ROUTE_MIGRATION_RETRY_EXHAUSTED",
                            "attempts": self.max_transaction_attempts,
                            "exception_type": type(exc).__name__,
                        },
                    ) from exc
        else:  # pragma: no cover - loop is fixed and always breaks or raises
            raise AssertionError(last_retryable)

        readback_target = self.simulation_repository.get_simulation_release_binding_by_hash(
            plan.target_binding.binding_hash or ""
        )
        if readback_target is None:
            raise MiniQMTRouteMigrationError(
                "MiniQMT route migration commit has no independent target readback",
                context={"reason_code": "MINIQMT_ROUTE_MIGRATION_READBACK_MISSING"},
            )
        readback_source = self.simulation_repository.get_simulation_release_binding(source_binding_id)
        return rebuild_receipt(source_binding=readback_source, target_binding=readback_target)


__all__ = [
    "DEFAULT_RUNTIME_INVENTORY_LIMIT",
    "MiniQMTRouteMigrationError",
    "MiniQMTRouteMigrationInventoryV1",
    "MiniQMTRouteMigrationMarkerV1",
    "MiniQMTRouteMigrationPlanV1",
    "MiniQMTRouteMigrationReceiptV1",
    "MiniQMTRouteMigrationService",
    "build_inventory",
    "build_target_binding",
    "rebuild_receipt",
]
