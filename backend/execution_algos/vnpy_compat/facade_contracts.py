"""Strict K4 vn.py façade contracts and immutable evidence carriers.

The module is a shadow-only dependency leaf.  It owns no repository, gateway,
broker, event loop, clock, random generator, or product-route decision.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import StrictBool, StrictInt, model_validator

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    FrozenJsonArrayV1,
    FrozenJsonObjectV1,
    canonical_utc_datetime_v1,
    canonical_decimal_string_v1,
    hash_hex_v1 as _canonical_hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CurrentThreeActiveOrderStateV3,
    CurrentThreeActiveOrderStatusV3,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    FrozenJsonFieldV1,
    FrozenJsonObjectFieldV1,
    FrozenStrictModel,
    GatewayCapabilityCatalogV1,
    IdentityV1,
    ExecutionAlgoPluginManifestV2,
    ExecutionCommandChildMappingV1,
    KernelCommandLifecycleProjectionV1,
    KernelProjectionTypeV1,
    NonNegativeIntV1,
    NormalizedOrderStatusV1,
    PositiveIntV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    Sha256V1,
    SideV1,
    TerminalOutcomeV1,
    TimerMutationV1,
    algo_transition_id_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    PluginCatalogSnapshotV1,
    PluginKeyV1,
    PluginRouteCompatibilityReceiptV1,
    VnpyCompatibilityReceiptV2,
)

_MAX_FACADE_FAILURES = 256
_RETAINED_FAILURES_WHEN_TRUNCATED = _MAX_FACADE_FAILURES - 1
_TRUNCATION_FIELD_PATH = "__failure_set__"
_TRUNCATION_REASON = "MINIQMT_VNPY_FACADE_FAILURES_TRUNCATED"


def _hash_ready_v1(value: Any) -> Any:
    """Convert strict carriers/enums to their exact canonical JSON payload."""

    if isinstance(value, FrozenStrictModel):
        return _hash_ready_v1(value.canonical_payload_v1())
    if isinstance(value, (FrozenJsonArrayV1, FrozenJsonObjectV1)):
        return thaw_json_v1(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _hash_ready_v1(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_hash_ready_v1(item) for item in value]
    return value


def hash_hex_v1(domain: str, payload: Any) -> str:
    """Use the shared hash authority after deterministic carrier projection."""

    return _canonical_hash_hex_v1(domain, _hash_ready_v1(payload))


class VnpyFacadeContractError(ValueError):
    """Typed, JSON-safe K4 contract failure."""

    def __init__(self, reason_code: str, message: str, *, context: Any) -> None:
        self.reason_code = reason_code
        self.message = message
        self.context = json_safe_evidence_v1(context)
        super().__init__(f"{reason_code}: {message}")


class VnpyFacadeSourceRoleV1(StrEnum):
    ALGORITHM = "ALGORITHM"
    HELPER = "HELPER"


class VnpyFacadeRegistrationDispositionV1(StrEnum):
    REGISTERED_CURRENT_THREE = "REGISTERED_CURRENT_THREE"
    CHARACTERIZATION_ONLY_K5 = "CHARACTERIZATION_ONLY_K5"
    FACADE_HELPER_ONLY = "FACADE_HELPER_ONLY"


class VnpyFacadeCompatibilityStatusV1(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"


class VnpyFacadeRuntimeBindingDispositionV1(StrEnum):
    PURE_PLUGIN_SHADOW_CONFORMANCE = "PURE_PLUGIN_SHADOW_CONFORMANCE"
    FACADE_BACKED_ADAPTER = "FACADE_BACKED_ADAPTER"


class VnpyFacadeCommandAuthorityDispositionV1(StrEnum):
    NOT_APPLICABLE_PURE_PLUGIN = "NOT_APPLICABLE_PURE_PLUGIN"
    SHADOW_ONLY_K2_V1 = "SHADOW_ONLY_K2_V1"


class VnpyFacadeFieldRoleV1(StrEnum):
    BASE = "BASE"
    PARAMETER = "PARAMETER"
    VARIABLE = "VARIABLE"
    ACTIVE_ORDER = "ACTIVE_ORDER"


class VnpyFacadeConstructorDispositionV1(StrEnum):
    INITIALIZE_ONLY = "INITIALIZE_ONLY"
    RESTORE_FROM_STATE = "RESTORE_FROM_STATE"


class VnpyFacadeIsolatedBindingOwnerV1(StrEnum):
    K1_PINNED_SOURCE = "K1_PINNED_SOURCE"
    K4_FACADE_IMPLEMENTATION = "K4_FACADE_IMPLEMENTATION"
    K4_DTO_PROJECTION = "K4_DTO_PROJECTION"
    K4_ENUM_PROJECTION = "K4_ENUM_PROJECTION"
    K4_PINNED_HELPER_IMPLEMENTATION = "K4_PINNED_HELPER_IMPLEMENTATION"
    K4_DETERMINISTIC_INPUT_ADAPTER = "K4_DETERMINISTIC_INPUT_ADAPTER"


def _sorted_unique_strings(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    if type(values) is not tuple or any(type(item) is not str or not item or item != item.strip() for item in values):
        raise TypeError(f"{field_name} must be a tuple of trim-stable strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicates")
    return tuple(sorted(values))


class VnpyFacadeSourceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_source_v1"] = "miniqmt_vnpy_facade_source_v1"
    source_role: VnpyFacadeSourceRoleV1
    algo_code_or_helper_name: IdentityV1
    upstream_namespace: Literal["VNPY_ALGOTRADING", "VNPY_CORE"]
    upstream_repo: IdentityV1
    upstream_commit: IdentityV1
    source_path: IdentityV1
    source_size: NonNegativeIntV1
    source_sha256: Sha256V1
    registration_disposition: VnpyFacadeRegistrationDispositionV1
    source_identity_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_source_v1",
            **values,
        }
        normalized = {
            **payload,
            "source_role": VnpyFacadeSourceRoleV1(payload["source_role"]),
            "registration_disposition": VnpyFacadeRegistrationDispositionV1(payload["registration_disposition"]),
        }
        canonical = {
            **normalized,
            "source_role": normalized["source_role"].value,
            "registration_disposition": normalized["registration_disposition"].value,
        }
        return cls(
            **normalized,
            source_identity_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_source_identity_v1",
                canonical,
            ),
        )

    def sort_key_v1(self) -> tuple[str, str, str, str]:
        return (
            self.source_role.value,
            self.registration_disposition.value,
            self.algo_code_or_helper_name,
            self.source_path,
        )

    @model_validator(mode="after")
    def _validate_source(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_source_identity_v1",
            self.canonical_payload_v1(exclude={"source_identity_sha256"}),
        )
        if self.source_identity_sha256 != expected:
            raise ValueError("facade source identity hash mismatch")
        if self.source_role is VnpyFacadeSourceRoleV1.HELPER:
            if self.registration_disposition is not VnpyFacadeRegistrationDispositionV1.FACADE_HELPER_ONLY:
                raise ValueError("helper source must use FACADE_HELPER_ONLY disposition")
        elif self.registration_disposition is VnpyFacadeRegistrationDispositionV1.FACADE_HELPER_ONLY:
            raise ValueError("algorithm source cannot use helper-only disposition")
        return self


class VnpyFacadeSourceManifestV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_source_manifest_v1"] = "miniqmt_vnpy_facade_source_manifest_v1"
    ordered_upstream_authority_sha256: tuple[Sha256V1, ...]
    ordered_sources: tuple[VnpyFacadeSourceV1, ...]
    manifest_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        upstream_authority_sha256: tuple[str, ...],
        sources: tuple[VnpyFacadeSourceV1, ...],
    ) -> Self:
        authorities = tuple(sorted(upstream_authority_sha256))
        ordered = tuple(sorted(sources, key=lambda item: item.sort_key_v1()))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_source_manifest_v1",
            "ordered_upstream_authority_sha256": list(authorities),
            "ordered_sources": [item.canonical_payload_v1() for item in ordered],
        }
        return cls(
            schema_version="miniqmt_vnpy_facade_source_manifest_v1",
            ordered_upstream_authority_sha256=authorities,
            ordered_sources=ordered,
            manifest_sha256=hash_hex_v1("miniqmt_vnpy_facade_source_manifest_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_manifest(self) -> Self:
        if len(self.ordered_upstream_authority_sha256) != 2 or len(set(self.ordered_upstream_authority_sha256)) != 2:
            raise ValueError("facade source manifest requires two unique upstream authorities")
        if self.ordered_upstream_authority_sha256 != tuple(sorted(self.ordered_upstream_authority_sha256)):
            raise ValueError("upstream authority hashes must be stable sorted")
        ordered = tuple(sorted(self.ordered_sources, key=lambda item: item.sort_key_v1()))
        if self.ordered_sources != ordered or len(ordered) != 6:
            raise ValueError("facade source manifest requires six canonically ordered sources")
        keys = tuple((item.algo_code_or_helper_name, item.source_path) for item in ordered)
        if len(keys) != len(set(keys)):
            raise ValueError("facade source manifest contains duplicate source identities")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_source_manifest_v1",
            self.canonical_payload_v1(exclude={"manifest_sha256"}),
        )
        if self.manifest_sha256 != expected:
            raise ValueError("facade source manifest hash mismatch")
        return self


class VnpyFacadeConformanceFailureV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_failure_v1"] = "miniqmt_vnpy_facade_conformance_failure_v1"
    field_path: IdentityV1
    reason_code: IdentityV1
    context: FrozenJsonFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(cls, *, field_path: str, reason_code: str, context: Any) -> Self:
        safe = json_safe_evidence_v1(context)
        return cls(
            field_path=field_path,
            reason_code=reason_code,
            context=safe,
            context_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_failure_context_v1",
                safe,
            ),
        )

    def sort_key_v1(self) -> tuple[str, str, str]:
        field_path = "\U0010ffff" if self.field_path == _TRUNCATION_FIELD_PATH else self.field_path
        return (field_path, self.reason_code, self.context_sha256)

    @model_validator(mode="after")
    def _validate_context(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_failure_context_v1",
            thaw_json_v1(self.context),
        )
        if self.context_sha256 != expected:
            raise ValueError("facade failure context hash mismatch")
        return self


class VnpyFacadeContractViewV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_contract_view_v1"] = "miniqmt_vnpy_facade_contract_view_v1"
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    symbol: IdentityV1
    exchange_member: Literal["SSE", "SZSE", "BSE"]
    gateway_name: IdentityV1
    min_volume: IdentityV1
    volume_increment: IdentityV1
    pricetick_decimal: IdentityV1
    contract_projection_sha256: Sha256V1
    gateway_catalog_sha256: Sha256V1
    route_receipt_sha256: Sha256V1
    contract_view_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "min_volume": canonical_decimal_string_v1(values["min_volume"], field_name="min_volume", allow_zero=False),
            "volume_increment": canonical_decimal_string_v1(
                values["volume_increment"], field_name="volume_increment", allow_zero=False
            ),
            "pricetick_decimal": canonical_decimal_string_v1(
                values["pricetick_decimal"], field_name="pricetick_decimal", allow_zero=False
            ),
        }
        payload = {"schema_version": "miniqmt_vnpy_facade_contract_view_v1", **normalized}
        return cls(
            **payload,
            contract_view_sha256=hash_hex_v1("miniqmt_vnpy_facade_contract_view_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_view(self) -> Self:
        suffix = self.symbol[-2:]
        expected_exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(suffix)
        if len(self.symbol) != 9 or self.symbol[6] != "." or expected_exchange != self.exchange_member:
            raise ValueError("facade contract symbol/exchange projection is invalid")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_contract_view_v1",
            self.canonical_payload_v1(exclude={"contract_view_sha256"}),
        )
        if self.contract_view_sha256 != expected:
            raise ValueError("facade contract view hash mismatch")
        return self


class VnpyFacadeStateValueV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_state_value_v1"] = "miniqmt_vnpy_facade_state_value_v1"
    name: IdentityV1
    value: FrozenJsonFieldV1
    value_type: IdentityV1
    value_sha256: Sha256V1

    @classmethod
    def create(cls, *, name: str, value: Any, value_type: str) -> Self:
        safe = json_safe_evidence_v1(value)
        return cls(
            name=name,
            value=safe,
            value_type=value_type,
            value_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_state_value_v1",
                {"name": name, "value": safe, "value_type": value_type},
            ),
        )

    @model_validator(mode="after")
    def _validate_value(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_state_value_v1",
            {"name": self.name, "value": thaw_json_v1(self.value), "value_type": self.value_type},
        )
        if self.value_sha256 != expected:
            raise ValueError("facade state value hash mismatch")
        return self


class VnpyFacadeActiveOrderV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_active_order_v1"] = "miniqmt_vnpy_facade_active_order_v1"
    local_vt_orderid: IdentityV1
    broker_order_id: IdentityV1 | None
    command_id: IdentityV1
    child_order_id: IdentityV1
    symbol: IdentityV1
    side: IdentityV1
    price_decimal: IdentityV1
    requested_quantity: PositiveIntV1
    cumulative_quantity: NonNegativeIntV1
    remaining_quantity: NonNegativeIntV1
    status: CurrentThreeActiveOrderStatusV3
    pending_command_type: BrokerCommandTypeV2 | None
    pending_command_id: IdentityV1 | None
    last_order_event_id: IdentityV1 | None
    last_trade_event_id: IdentityV1 | None
    last_command_outcome_event_id: IdentityV1 | None
    last_oms_reconcile_event_id: IdentityV1 | None
    terminal_order_status: NormalizedOrderStatusV1 | None
    terminal_observed_cumulative_filled_quantity: NonNegativeIntV1 | None
    market_data_lineage: FrozenJsonObjectFieldV1
    active_order_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "status": CurrentThreeActiveOrderStatusV3(values["status"]),
            "pending_command_type": (
                None
                if values.get("pending_command_type") is None
                else BrokerCommandTypeV2(values["pending_command_type"])
            ),
            "terminal_order_status": (
                None
                if values.get("terminal_order_status") is None
                else NormalizedOrderStatusV1(values["terminal_order_status"])
            ),
            "price_decimal": canonical_decimal_string_v1(
                values["price_decimal"], field_name="price_decimal", allow_zero=False
            ),
        }
        payload = {"schema_version": "miniqmt_vnpy_facade_active_order_v1", **normalized}
        return cls(
            **payload,
            active_order_sha256=hash_hex_v1("miniqmt_vnpy_facade_active_order_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_order(self) -> Self:
        if self.cumulative_quantity + self.remaining_quantity != self.requested_quantity:
            raise ValueError("active order quantity closure is invalid")
        validate_vnpy_facade_market_data_lineage_v1(thaw_json_v1(self.market_data_lineage))
        self.as_current_three_state_v3()
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_active_order_v1",
            self.canonical_payload_v1(exclude={"active_order_sha256"}),
        )
        if self.active_order_sha256 != expected:
            raise ValueError("facade active order hash mismatch")
        return self

    def as_current_three_state_v3(self) -> CurrentThreeActiveOrderStateV3:
        """Project the exact façade lifecycle carrier into the existing K2 authority."""

        return CurrentThreeActiveOrderStateV3.create(
            local_vt_orderid=self.local_vt_orderid,
            submit_command_id=self.command_id,
            broker_order_id=self.broker_order_id,
            symbol=self.symbol,
            side=self.side,
            status=self.status,
            pending_command_type=self.pending_command_type,
            pending_command_id=self.pending_command_id,
            requested_price_decimal=self.price_decimal,
            requested_quantity=self.requested_quantity,
            cumulative_filled_quantity=self.cumulative_quantity,
            remaining_quantity=self.remaining_quantity,
            last_order_event_id=self.last_order_event_id,
            last_trade_event_id=self.last_trade_event_id,
            last_command_outcome_event_id=self.last_command_outcome_event_id,
            last_oms_reconcile_event_id=self.last_oms_reconcile_event_id,
            terminal_order_status=self.terminal_order_status,
            terminal_observed_cumulative_filled_quantity=(self.terminal_observed_cumulative_filled_quantity),
            market_data_lineage=thaw_json_v1(self.market_data_lineage),
        )


def validate_vnpy_facade_market_data_lineage_v1(lineage: Any) -> dict[str, Any]:
    """Validate the exact native quote lineage retained by every submit child."""

    required = {
        "market_data_id",
        "event_id",
        "payload_sha256",
        "generation",
        "sequence",
        "exchange_time_utc",
        "session_phase",
    }
    if type(lineage) is not dict or set(lineage) != required:
        raise ValueError("facade active order requires the exact market-data lineage schema")
    for field_name in ("market_data_id", "event_id"):
        value = lineage[field_name]
        if type(value) is not str or not value or value != value.strip():
            raise ValueError(f"market-data lineage {field_name} must be a trim-stable identity")
    if re.fullmatch(r"[0-9a-f]{64}", lineage["payload_sha256"]) is None:
        raise ValueError("market-data lineage payload_sha256 must be lowercase SHA-256")
    for field_name in ("generation", "sequence"):
        value = lineage[field_name]
        if type(value) is not int or value < 0:
            raise ValueError(f"market-data lineage {field_name} must be a non-negative strict integer")
    canonical_utc_datetime_v1(lineage["exchange_time_utc"], field_name="market_data_lineage.exchange_time_utc")
    if lineage["session_phase"] not in {
        SessionPhaseV1.CONTINUOUS_AM.value,
        SessionPhaseV1.CONTINUOUS_PM.value,
    }:
        raise ValueError("facade submit lineage must come from a continuous native quote")
    return lineage


def build_vnpy_facade_market_data_lineage_v1(
    *,
    services: AlgoReadOnlyServicesV1,
    deterministic_context: DeterministicExecutionContextV1,
) -> dict[str, Any]:
    """Rebuild submit lineage from one immutable K2 market projection authority."""

    if not isinstance(services, AlgoReadOnlyServicesV1):
        raise TypeError("services must be AlgoReadOnlyServicesV1")
    if not isinstance(deterministic_context, DeterministicExecutionContextV1):
        raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
    if services.market_data_projection is None:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE",
            "submit command has no immutable native market projection",
            context={
                "runtime_id": services.runtime_id,
                "algo_instance_id": services.algo_instance_id,
                "event_id": services.event_id,
                "delivery_id": services.delivery_id,
            },
        )
    refs = tuple(
        item
        for item in services.execution_projection_set.ordered_projection_refs
        if item.projection_type is KernelProjectionTypeV1.MARKET_DATA
    )
    if len(refs) != 1:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
            "submit command requires one exact market projection ref",
            context={"market_projection_ref_count": len(refs)},
        )
    ref = refs[0]
    payload = thaw_json_v1(services.market_data_projection)
    required_projection = {
        "market_data_id",
        "source_event_id",
        "generation",
        "source_sequence",
        "exchange_time_utc",
        "exchange_trade_date",
        "session_epoch",
        "session_phase",
        "quote_source",
    }
    missing = sorted(required_projection - set(payload))
    if missing:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
            "market projection lacks exact native source lineage",
            context={"missing_fields": missing, "projection_id": ref.projection_id},
        )
    if (
        ref.projection_id != services.market_data_projection_id
        or ref.payload_sha256 != services.market_data_projection_sha256
        or ref.source_event_id != payload["source_event_id"]
        or payload["quote_source"] != "B0_QUOTE_V2"
        or payload["exchange_trade_date"] != deterministic_context.exchange_trade_date
        or payload["session_epoch"] != deterministic_context.session_epoch
        or payload["session_phase"] != deterministic_context.session_phase.value
        or canonical_utc_datetime_v1(payload["exchange_time_utc"]) != canonical_utc_datetime_v1(ref.logical_at_utc)
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
            "market projection native lineage conflicts with deterministic/ref authority",
            context={
                "projection_id": ref.projection_id,
                "source_event_id": ref.source_event_id,
                "session_phase": deterministic_context.session_phase.value,
            },
        )
    return validate_vnpy_facade_market_data_lineage_v1(
        {
            "market_data_id": payload["market_data_id"],
            "event_id": payload["source_event_id"],
            "payload_sha256": hash_hex_v1("miniqmt_runtime_event_payload_v2", payload),
            "generation": payload["generation"],
            "sequence": payload["source_sequence"],
            "exchange_time_utc": payload["exchange_time_utc"],
            "session_phase": payload["session_phase"],
        }
    )


def validate_vnpy_facade_transition_market_authority_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    services: AlgoReadOnlyServicesV1,
    deterministic_context: DeterministicExecutionContextV1,
) -> None:
    """Close current TICK bytes and prior-TICK refs without a process-cache fallback."""

    if services.market_data_projection is None:
        if event.event_type is EventTypeV2.TICK:
            raise ValueError("facade TICK transition requires its exact immutable market projection")
        return
    lineage = build_vnpy_facade_market_data_lineage_v1(
        services=services,
        deterministic_context=deterministic_context,
    )
    if event.event_type is not EventTypeV2.TICK:
        return
    event_payload = thaw_json_v1(event.payload)
    source_identity = thaw_json_v1(event.source_identity)
    if (
        event.source is not EventSourceV2.B0_QUOTE_V2
        or event.payload_schema_version != "miniqmt_market_data_view_v2"
        or thaw_json_v1(services.market_data_projection) != event_payload
        or lineage["event_id"] != event.event_id
        or lineage["market_data_id"] != source_identity.get("market_data_id")
        or lineage["payload_sha256"] != event.payload_sha256
        or lineage["sequence"] != event.sequence
    ):
        raise ValueError("facade TICK event and immutable market projection lineage differ")


class VnpyFacadeStateEnvelopeV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_state_envelope_v1"] = "miniqmt_vnpy_facade_state_envelope_v1"
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    algorithm_binding_sha256: Sha256V1
    algo_name: IdentityV1
    symbol: IdentityV1
    direction_member: IdentityV1
    offset_member: IdentityV1
    limit_price_decimal: IdentityV1
    target_volume_decimal: IdentityV1
    status_member: IdentityV1
    traded_volume_decimal: IdentityV1
    traded_price_decimal: IdentityV1
    contract_view: VnpyFacadeContractViewV1
    ordered_active_orders: tuple[VnpyFacadeActiveOrderV1, ...]
    ordered_parameters: tuple[VnpyFacadeStateValueV1, ...]
    ordered_variables: tuple[VnpyFacadeStateValueV1, ...]
    state_mapping_set_sha256: Sha256V1
    state_envelope_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        active = tuple(sorted(values["ordered_active_orders"], key=lambda item: item.local_vt_orderid))
        parameters = tuple(sorted(values["ordered_parameters"], key=lambda item: item.name))
        variables = tuple(sorted(values["ordered_variables"], key=lambda item: item.name))
        normalized = {
            **values,
            "limit_price_decimal": canonical_decimal_string_v1(
                values["limit_price_decimal"], field_name="limit_price_decimal", allow_zero=False
            ),
            "target_volume_decimal": canonical_decimal_string_v1(
                values["target_volume_decimal"], field_name="target_volume_decimal", allow_zero=False
            ),
            "traded_volume_decimal": canonical_decimal_string_v1(
                values["traded_volume_decimal"], field_name="traded_volume_decimal", allow_zero=True
            ),
            "traded_price_decimal": canonical_decimal_string_v1(
                values["traded_price_decimal"], field_name="traded_price_decimal", allow_zero=True
            ),
            "ordered_active_orders": active,
            "ordered_parameters": parameters,
            "ordered_variables": variables,
        }
        payload = {"schema_version": "miniqmt_vnpy_facade_state_envelope_v1", **normalized}
        return cls(
            **payload,
            state_envelope_sha256=hash_hex_v1("miniqmt_vnpy_facade_state_envelope_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_envelope(self) -> Self:
        if self.algo_name != self.algo_instance_id:
            raise ValueError("facade algo_name must equal deterministic algo_instance_id")
        for name, items, key in (
            ("active orders", self.ordered_active_orders, lambda item: item.local_vt_orderid),
            ("parameters", self.ordered_parameters, lambda item: item.name),
            ("variables", self.ordered_variables, lambda item: item.name),
        ):
            keys = tuple(key(item) for item in items)
            if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
                raise ValueError(f"facade {name} must be unique and sorted")
        if {item.name for item in self.ordered_parameters} & {item.name for item in self.ordered_variables}:
            raise ValueError("facade parameter and variable names must be disjoint")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_state_envelope_v1",
            self.canonical_payload_v1(exclude={"state_envelope_sha256"}),
        )
        if self.state_envelope_sha256 != expected:
            raise ValueError("facade state envelope hash mismatch")
        return self


def read_vnpy_facade_lifecycle_items_v1(
    state_payload: Any,
) -> tuple[CurrentThreeActiveOrderStateV3, ...]:
    """Strictly project one façade state envelope into existing K2 lifecycle items."""

    if type(state_payload) is not dict:
        raise TypeError("facade lifecycle state payload must be a strict object")
    envelope = VnpyFacadeStateEnvelopeV1.model_validate_json(
        json.dumps(state_payload, sort_keys=True, separators=(",", ":")),
        strict=True,
    )
    return tuple(item.as_current_three_state_v3() for item in envelope.ordered_active_orders)


class VnpyFacadeSourceStateEnvelopeV1(FrozenStrictModel):
    """Source-characterization state with no fabricated plugin, route, or binding identity."""

    schema_version: Literal["miniqmt_vnpy_facade_source_state_envelope_v1"] = (
        "miniqmt_vnpy_facade_source_state_envelope_v1"
    )
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    manifest_view_sha256: Sha256V1
    algo_name: IdentityV1
    symbol: IdentityV1
    direction_member: IdentityV1
    offset_member: IdentityV1
    limit_price_decimal: IdentityV1
    target_volume_decimal: IdentityV1
    status_member: IdentityV1
    traded_volume_decimal: IdentityV1
    traded_price_decimal: IdentityV1
    contract_projection: FrozenJsonObjectFieldV1
    contract_projection_sha256: Sha256V1
    ordered_active_orders: tuple[VnpyFacadeActiveOrderV1, ...]
    ordered_parameters: tuple[VnpyFacadeStateValueV1, ...]
    ordered_variables: tuple[VnpyFacadeStateValueV1, ...]
    state_mapping_set_sha256: Sha256V1
    state_envelope_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        active = tuple(sorted(values["ordered_active_orders"], key=lambda item: item.local_vt_orderid))
        parameters = tuple(sorted(values["ordered_parameters"], key=lambda item: item.name))
        variables = tuple(sorted(values["ordered_variables"], key=lambda item: item.name))
        contract = json_safe_evidence_v1(values["contract_projection"])
        normalized = {
            **values,
            "limit_price_decimal": canonical_decimal_string_v1(
                values["limit_price_decimal"], field_name="limit_price_decimal", allow_zero=False
            ),
            "target_volume_decimal": canonical_decimal_string_v1(
                values["target_volume_decimal"], field_name="target_volume_decimal", allow_zero=False
            ),
            "traded_volume_decimal": canonical_decimal_string_v1(
                values["traded_volume_decimal"], field_name="traded_volume_decimal", allow_zero=True
            ),
            "traded_price_decimal": canonical_decimal_string_v1(
                values["traded_price_decimal"], field_name="traded_price_decimal", allow_zero=True
            ),
            "contract_projection": contract,
            "contract_projection_sha256": hash_hex_v1("miniqmt_vnpy_facade_source_contract_projection_v1", contract),
            "ordered_active_orders": active,
            "ordered_parameters": parameters,
            "ordered_variables": variables,
        }
        payload = {"schema_version": "miniqmt_vnpy_facade_source_state_envelope_v1", **normalized}
        return cls(
            **payload,
            state_envelope_sha256=hash_hex_v1("miniqmt_vnpy_facade_source_state_envelope_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_source_envelope(self) -> Self:
        if self.algo_name != self.algo_instance_id:
            raise ValueError("source facade algo_name must equal deterministic algo_instance_id")
        for name, items, key in (
            ("active orders", self.ordered_active_orders, lambda item: item.local_vt_orderid),
            ("parameters", self.ordered_parameters, lambda item: item.name),
            ("variables", self.ordered_variables, lambda item: item.name),
        ):
            keys = tuple(key(item) for item in items)
            if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
                raise ValueError(f"source facade {name} must be unique and sorted")
        if {item.name for item in self.ordered_parameters} & {item.name for item in self.ordered_variables}:
            raise ValueError("source facade parameter and variable names must be disjoint")
        expected_contract = hash_hex_v1(
            "miniqmt_vnpy_facade_source_contract_projection_v1",
            thaw_json_v1(self.contract_projection),
        )
        if self.contract_projection_sha256 != expected_contract:
            raise ValueError("source facade contract projection hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_source_state_envelope_v1",
            self.canonical_payload_v1(exclude={"state_envelope_sha256"}),
        )
        if self.state_envelope_sha256 != expected:
            raise ValueError("source facade state envelope hash mismatch")
        return self


class VnpyAlgoProjectionObservationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_algo_projection_observation_v1"] = (
        "miniqmt_vnpy_algo_projection_observation_v1"
    )
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_sequence: PositiveIntV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    algo_status: IdentityV1
    ordered_parameters: tuple[VnpyFacadeStateValueV1, ...]
    ordered_variables: tuple[VnpyFacadeStateValueV1, ...]
    observation_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        parameters = tuple(sorted(values["ordered_parameters"], key=lambda item: item.name))
        variables = tuple(sorted(values["ordered_variables"], key=lambda item: item.name))
        payload = {
            "schema_version": "miniqmt_vnpy_algo_projection_observation_v1",
            **{key: value for key, value in values.items() if key not in {"ordered_parameters", "ordered_variables"}},
            "ordered_parameters": parameters,
            "ordered_variables": variables,
        }
        return cls(
            **payload,
            observation_sha256=hash_hex_v1("miniqmt_vnpy_algo_projection_observation_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_observation(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_algo_projection_observation_v1",
            self.canonical_payload_v1(exclude={"observation_sha256"}),
        )
        if self.observation_sha256 != expected:
            raise ValueError("facade projection observation hash mismatch")
        return self


def bound_vnpy_facade_failures_v1(
    failures: tuple[VnpyFacadeConformanceFailureV1, ...],
) -> tuple[VnpyFacadeConformanceFailureV1, ...]:
    if type(failures) is not tuple or any(type(item) is not VnpyFacadeConformanceFailureV1 for item in failures):
        raise TypeError("failures must be a tuple of VnpyFacadeConformanceFailureV1")
    if any(item.field_path == _TRUNCATION_FIELD_PATH for item in failures):
        raise ValueError("caller-supplied facade failure set cannot contain a truncation marker")
    ordered = tuple(sorted(failures, key=lambda item: item.sort_key_v1()))
    identities = tuple((item.field_path, item.reason_code, item.context_sha256) for item in ordered)
    if len(identities) != len(set(identities)):
        raise ValueError("facade failure set must not contain duplicates")
    if len(ordered) <= _MAX_FACADE_FAILURES:
        return ordered
    retained = ordered[:_RETAINED_FAILURES_WHEN_TRUNCATED]
    omitted = ordered[_RETAINED_FAILURES_WHEN_TRUNCATED:]
    marker = VnpyFacadeConformanceFailureV1.create(
        field_path=_TRUNCATION_FIELD_PATH,
        reason_code=_TRUNCATION_REASON,
        context={
            "omitted_count": len(omitted),
            "omitted_failure_set_sha256": hash_hex_v1(
                "miniqmt_vnpy_facade_omitted_failure_set_v1",
                [
                    {
                        "field_path": item.field_path,
                        "reason_code": item.reason_code,
                        "context_sha256": item.context_sha256,
                    }
                    for item in omitted
                ],
            ),
        },
    )
    return (*retained, marker)


def _validate_bounded_failure_set_v1(
    failures: tuple[VnpyFacadeConformanceFailureV1, ...],
) -> None:
    if len(failures) > _MAX_FACADE_FAILURES:
        raise ValueError("facade failure set is unbounded")
    identities = tuple((item.field_path, item.reason_code, item.context_sha256) for item in failures)
    if len(identities) != len(set(identities)):
        raise ValueError("facade failure set contains duplicate identities")
    if failures != tuple(sorted(failures, key=lambda item: item.sort_key_v1())):
        raise ValueError("facade failure set is not canonically ordered")
    markers = tuple(item for item in failures if item.field_path == _TRUNCATION_FIELD_PATH)
    if not markers:
        return
    if len(markers) != 1 or len(failures) != _MAX_FACADE_FAILURES or failures[-1] is not markers[0]:
        raise ValueError("facade truncation marker must be the unique final 256th failure")
    marker = markers[0]
    if marker.reason_code != _TRUNCATION_REASON:
        raise ValueError("facade truncation marker reason is invalid")
    context = thaw_json_v1(marker.context)
    if set(context) != {"omitted_count", "omitted_failure_set_sha256"}:
        raise ValueError("facade truncation marker context schema is invalid")
    if type(context["omitted_count"]) is not int or context["omitted_count"] <= 0:
        raise ValueError("facade truncation marker omitted_count must be positive")
    omitted_hash = context["omitted_failure_set_sha256"]
    if type(omitted_hash) is not str or len(omitted_hash) != 64:
        raise ValueError("facade truncation marker omitted-set hash is invalid")


class VnpyFacadeImplementationBindingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_implementation_binding_v1"] = (
        "miniqmt_vnpy_facade_implementation_binding_v1"
    )
    component_name: IdentityV1
    callable_ref: IdentityV1
    callable_signature_sha256: Sha256V1
    repo_relative_source_path: IdentityV1
    canonical_lf_source_size: NonNegativeIntV1
    canonical_lf_source_sha256: Sha256V1
    binding_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_implementation_binding_v1",
            **values,
        }
        return cls(
            **payload,
            binding_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_implementation_binding_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_implementation_binding_v1",
            self.canonical_payload_v1(exclude={"binding_sha256"}),
        )
        if self.binding_sha256 != expected:
            raise ValueError("facade implementation binding hash mismatch")
        return self


class VnpyFacadeMethodContractV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_method_contract_v1"] = "miniqmt_vnpy_facade_method_contract_v1"
    surface_owner: IdentityV1
    method_name: IdentityV1
    pinned_surface_ref_kind: Literal["K1_METHOD_REQUIREMENT", "PINNED_TEMPLATE_HELPER"]
    pinned_surface_ref_sha256: Sha256V1
    ordered_invocation_phases: tuple[IdentityV1, ...]
    ordered_required_authority_refs: tuple[IdentityV1, ...]
    return_disposition: IdentityV1
    empty_return_disposition: IdentityV1
    ordered_effect_types: tuple[IdentityV1, ...]
    ordered_reason_codes: tuple[IdentityV1, ...]
    implementation_binding_sha256: Sha256V1
    method_contract_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = dict(values)
        for field in (
            "ordered_invocation_phases",
            "ordered_required_authority_refs",
            "ordered_effect_types",
            "ordered_reason_codes",
        ):
            normalized[field] = _sorted_unique_strings(tuple(values[field]), field_name=field)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_method_contract_v1",
            **normalized,
        }
        return cls(
            **payload,
            method_contract_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_method_contract_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        for field in (
            "ordered_invocation_phases",
            "ordered_required_authority_refs",
            "ordered_effect_types",
            "ordered_reason_codes",
        ):
            if getattr(self, field) != _sorted_unique_strings(getattr(self, field), field_name=field):
                raise ValueError(f"{field} is not canonical")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_method_contract_v1",
            self.canonical_payload_v1(exclude={"method_contract_sha256"}),
        )
        if self.method_contract_sha256 != expected:
            raise ValueError("facade method contract hash mismatch")
        return self


class VnpyFacadeDtoMappingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_dto_mapping_v1"] = "miniqmt_vnpy_facade_dto_mapping_v1"
    object_name: IdentityV1
    field_name: IdentityV1
    source_projection_type: IdentityV1
    source_field_path: IdentityV1
    conversion_rule: IdentityV1
    missing_disposition: IdentityV1
    allowed_enum_mapping: FrozenJsonObjectFieldV1
    mapping_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_dto_mapping_v1",
            **values,
        }
        return cls(
            **payload,
            mapping_sha256=hash_hex_v1("miniqmt_vnpy_facade_dto_mapping_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_dto_mapping_v1",
            self.canonical_payload_v1(exclude={"mapping_sha256"}),
        )
        if self.mapping_sha256 != expected:
            raise ValueError("facade DTO mapping hash mismatch")
        return self


class VnpyFacadeStateFieldMappingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_state_field_mapping_v1"] = "miniqmt_vnpy_facade_state_field_mapping_v1"
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    attribute_name: IdentityV1
    state_path: IdentityV1
    field_role: VnpyFacadeFieldRoleV1
    value_type: IdentityV1
    nullable: StrictBool
    mutable_container_disposition: IdentityV1
    constructor_disposition: VnpyFacadeConstructorDispositionV1
    mapping_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "field_role": VnpyFacadeFieldRoleV1(values["field_role"]),
            "constructor_disposition": VnpyFacadeConstructorDispositionV1(values["constructor_disposition"]),
        }
        payload = {
            "schema_version": "miniqmt_vnpy_facade_state_field_mapping_v1",
            **normalized,
        }
        return cls(
            **payload,
            mapping_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_state_field_mapping_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_state_field_mapping_v1",
            self.canonical_payload_v1(exclude={"mapping_sha256"}),
        )
        if self.mapping_sha256 != expected:
            raise ValueError("facade state mapping hash mismatch")
        return self


class VnpyFacadeTerminalMappingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_terminal_mapping_v1"] = "miniqmt_vnpy_facade_terminal_mapping_v1"
    algo_code: IdentityV1
    algo_status_member: IdentityV1
    trigger_event_type: IdentityV1
    traded_relation: Literal["FULL", "RESIDUAL", "ANY"]
    required_active_child_closure: IdentityV1
    terminal_outcome_or_none: IdentityV1 | None
    reason_code: IdentityV1
    mapping_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_terminal_mapping_v1",
            **values,
        }
        return cls(
            **payload,
            mapping_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_terminal_mapping_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_terminal_mapping_v1",
            self.canonical_payload_v1(exclude={"mapping_sha256"}),
        )
        if self.mapping_sha256 != expected:
            raise ValueError("facade terminal mapping hash mismatch")
        return self


class VnpyFacadeIsolatedModuleBindingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_isolated_module_binding_v1"] = (
        "miniqmt_vnpy_facade_isolated_module_binding_v1"
    )
    module_name: IdentityV1
    export_name: IdentityV1
    binding_owner: VnpyFacadeIsolatedBindingOwnerV1
    binding_ref: IdentityV1
    binding_source_identity_sha256_or_implementation_binding_sha256: Sha256V1
    binding_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "binding_owner": VnpyFacadeIsolatedBindingOwnerV1(values["binding_owner"]),
        }
        payload = {
            "schema_version": "miniqmt_vnpy_facade_isolated_module_binding_v1",
            **normalized,
        }
        return cls(
            **payload,
            binding_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_isolated_module_binding_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_isolated_module_binding_v1",
            self.canonical_payload_v1(exclude={"binding_sha256"}),
        )
        if self.binding_sha256 != expected:
            raise ValueError("isolated module binding hash mismatch")
        return self


class VnpyFacadeContractV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_contract_v1"] = "miniqmt_vnpy_facade_contract_v1"
    requirement_sha256: Sha256V1
    surface_sha256: Sha256V1
    method_signature_sha256: Sha256V1
    object_field_sha256: Sha256V1
    ordered_implementation_bindings: tuple[VnpyFacadeImplementationBindingV1, ...]
    implementation_binding_set_sha256: Sha256V1
    ordered_method_contracts: tuple[VnpyFacadeMethodContractV1, ...]
    method_contract_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    facade_contract_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        implementations = tuple(sorted(values["ordered_implementation_bindings"], key=lambda item: item.component_name))
        methods = tuple(
            sorted(
                values["ordered_method_contracts"],
                key=lambda item: (item.surface_owner, item.method_name),
            )
        )
        implementation_set = hash_hex_v1(
            "miniqmt_vnpy_facade_implementation_binding_set_v1",
            [item.canonical_payload_v1() for item in implementations],
        )
        method_set = hash_hex_v1(
            "miniqmt_vnpy_facade_method_contract_set_v1",
            [item.canonical_payload_v1() for item in methods],
        )
        payload = {
            "schema_version": "miniqmt_vnpy_facade_contract_v1",
            **{
                key: value
                for key, value in values.items()
                if key
                not in {
                    "ordered_implementation_bindings",
                    "ordered_method_contracts",
                    "implementation_binding_set_sha256",
                    "method_contract_set_sha256",
                }
            },
            "ordered_implementation_bindings": [item.canonical_payload_v1() for item in implementations],
            "implementation_binding_set_sha256": implementation_set,
            "ordered_method_contracts": [item.canonical_payload_v1() for item in methods],
            "method_contract_set_sha256": method_set,
        }
        return cls(
            **{
                **payload,
                "ordered_implementation_bindings": implementations,
                "ordered_method_contracts": methods,
            },
            facade_contract_sha256=hash_hex_v1("miniqmt_vnpy_facade_contract_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        implementations = tuple(sorted(self.ordered_implementation_bindings, key=lambda item: item.component_name))
        methods = tuple(sorted(self.ordered_method_contracts, key=lambda item: (item.surface_owner, item.method_name)))
        if self.ordered_implementation_bindings != implementations or len(
            {item.component_name for item in implementations}
        ) != len(implementations):
            raise ValueError("implementation bindings must be unique and sorted")
        if self.ordered_method_contracts != methods or len(
            {(item.surface_owner, item.method_name) for item in methods}
        ) != len(methods):
            raise ValueError("method contracts must be unique and sorted")
        expected_implementation_set = hash_hex_v1(
            "miniqmt_vnpy_facade_implementation_binding_set_v1",
            [item.canonical_payload_v1() for item in implementations],
        )
        expected_method_set = hash_hex_v1(
            "miniqmt_vnpy_facade_method_contract_set_v1",
            [item.canonical_payload_v1() for item in methods],
        )
        if self.implementation_binding_set_sha256 != expected_implementation_set:
            raise ValueError("implementation binding set hash mismatch")
        if self.method_contract_set_sha256 != expected_method_set:
            raise ValueError("method contract set hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_contract_v1",
            self.canonical_payload_v1(exclude={"facade_contract_sha256"}),
        )
        if self.facade_contract_sha256 != expected:
            raise ValueError("facade contract hash mismatch")
        return self


class VnpyFacadeUniformDrawV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_uniform_draw_v1"] = "miniqmt_vnpy_facade_uniform_draw_v1"
    ordinal: NonNegativeIntV1
    u53_integer: StrictInt
    draw_sha256: Sha256V1

    @classmethod
    def create(cls, *, ordinal: int, u53_integer: int) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_uniform_draw_v1",
            "ordinal": ordinal,
            "u53_integer": u53_integer,
        }
        return cls(
            **payload,
            draw_sha256=hash_hex_v1("miniqmt_vnpy_facade_uniform_draw_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_draw(self) -> Self:
        if not 0 <= self.u53_integer < 2**53:
            raise ValueError("u53_integer must be inside the exact 53-bit domain")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_uniform_draw_v1",
            self.canonical_payload_v1(exclude={"draw_sha256"}),
        )
        if self.draw_sha256 != expected:
            raise ValueError("uniform draw hash mismatch")
        return self


class VnpyFacadeDeterministicInputsV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_deterministic_inputs_v1"] = (
        "miniqmt_vnpy_facade_deterministic_inputs_v1"
    )
    ordered_uniform_draws: tuple[VnpyFacadeUniformDrawV1, ...]
    inputs_sha256: Sha256V1

    @classmethod
    def create(cls, *, ordered_uniform_draws: tuple[VnpyFacadeUniformDrawV1, ...]) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_deterministic_inputs_v1",
            "ordered_uniform_draws": [item.canonical_payload_v1() for item in ordered_uniform_draws],
        }
        return cls(
            schema_version="miniqmt_vnpy_facade_deterministic_inputs_v1",
            ordered_uniform_draws=ordered_uniform_draws,
            inputs_sha256=hash_hex_v1("miniqmt_vnpy_facade_deterministic_inputs_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_inputs(self) -> Self:
        ordinals = tuple(item.ordinal for item in self.ordered_uniform_draws)
        if ordinals != tuple(range(len(ordinals))):
            raise ValueError("uniform draw ordinals must be contiguous from zero")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_deterministic_inputs_v1",
            self.canonical_payload_v1(exclude={"inputs_sha256"}),
        )
        if self.inputs_sha256 != expected:
            raise ValueError("deterministic input hash mismatch")
        return self


class VnpyFacadeAlgorithmBindingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_algorithm_binding_v1"] = "miniqmt_vnpy_facade_algorithm_binding_v1"
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    class_ref: IdentityV1
    constructor_signature_sha256: Sha256V1
    constructor_body_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    characterization_receipt_sha256: Sha256V1
    adapter_contract_sha256: Sha256V1
    binding_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_algorithm_binding_v1",
            **values,
        }
        return cls(
            **payload,
            binding_sha256=hash_hex_v1("miniqmt_vnpy_facade_algorithm_binding_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_algorithm_binding_v1",
            self.canonical_payload_v1(exclude={"binding_sha256"}),
        )
        if self.binding_sha256 != expected:
            raise ValueError("algorithm binding hash mismatch")
        return self


class VnpyFacadeCharacterizationRequirementV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_requirement_v1"] = (
        "miniqmt_vnpy_facade_characterization_requirement_v1"
    )
    algo_code: IdentityV1
    registration_disposition: VnpyFacadeRegistrationDispositionV1
    source_identity_sha256: Sha256V1
    config_schema_version: IdentityV1
    config_schema: FrozenJsonObjectFieldV1
    config_schema_sha256: Sha256V1
    config_validation_contract_sha256: Sha256V1
    ordered_required_methods: tuple[IdentityV1, ...]
    ordered_required_object_fields: tuple[IdentityV1, ...]
    ordered_required_enum_members: tuple[IdentityV1, ...]
    ordered_event_types: tuple[IdentityV1, ...]
    ordered_market_data_capabilities: tuple[IdentityV1, ...]
    state_mapping_set_sha256: Sha256V1
    requirement_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "registration_disposition": VnpyFacadeRegistrationDispositionV1(values["registration_disposition"]),
        }
        for field in (
            "ordered_required_methods",
            "ordered_required_object_fields",
            "ordered_required_enum_members",
            "ordered_event_types",
            "ordered_market_data_capabilities",
        ):
            normalized[field] = _sorted_unique_strings(tuple(values[field]), field_name=field)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_requirement_v1",
            **normalized,
        }
        return cls(
            **payload,
            requirement_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_characterization_requirement_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_requirement(self) -> Self:
        for field in (
            "ordered_required_methods",
            "ordered_required_object_fields",
            "ordered_required_enum_members",
            "ordered_event_types",
            "ordered_market_data_capabilities",
        ):
            if getattr(self, field) != _sorted_unique_strings(getattr(self, field), field_name=field):
                raise ValueError(f"{field} is not canonical")
        expected_config = hash_hex_v1("miniqmt_plugin_config_schema_v1", thaw_json_v1(self.config_schema))
        if self.config_schema_sha256 != expected_config:
            raise ValueError("characterization config schema hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_requirement_v1",
            self.canonical_payload_v1(exclude={"requirement_sha256"}),
        )
        if self.requirement_sha256 != expected:
            raise ValueError("characterization requirement hash mismatch")
        return self


class VnpyFacadeCharacterizationVectorV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_vector_v1"] = (
        "miniqmt_vnpy_facade_characterization_vector_v1"
    )
    vector_id: IdentityV1
    algo_code: IdentityV1
    side: IdentityV1
    invocation_phase: Literal["INITIALIZE", "TRANSITION"]
    canonical_config: FrozenJsonObjectFieldV1
    before_state_sha256_or_INIT: IdentityV1
    event_type: IdentityV1
    event_payload_sha256: Sha256V1
    projection_set_sha256: Sha256V1
    authority_input_sha256: Sha256V1
    source_market_data_event_id: IdentityV1 | None
    explicit_deterministic_inputs: VnpyFacadeDeterministicInputsV1
    expected_ordered_facade_calls: tuple[FrozenJsonObjectFieldV1, ...]
    expected_ordered_effects: tuple[FrozenJsonObjectFieldV1, ...]
    expected_after_state_sha256: Sha256V1
    expected_terminal_outcome: IdentityV1 | None
    vector_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_vector_v1",
            **values,
        }
        return cls(
            **payload,
            vector_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_vector_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_v1",
            self.canonical_payload_v1(exclude={"vector_sha256"}),
        )
        if self.vector_sha256 != expected:
            raise ValueError("characterization vector hash mismatch")
        return self


class VnpyFacadeAlgorithmCharacterizationReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_algorithm_characterization_receipt_v1"] = (
        "miniqmt_vnpy_facade_algorithm_characterization_receipt_v1"
    )
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    characterization_requirement_sha256: Sha256V1
    canonical_factory_probe_config: FrozenJsonObjectFieldV1
    factory_probe_config_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    implementation_binding_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    ordered_vector_ids: tuple[IdentityV1, ...]
    vector_set_sha256: Sha256V1
    status: VnpyFacadeCompatibilityStatusV1
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        vectors = _sorted_unique_strings(tuple(values["ordered_vector_ids"]), field_name="ordered_vector_ids")
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        normalized = {
            **values,
            "ordered_vector_ids": vectors,
            "ordered_failures": failures,
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
        }
        payload = {
            "schema_version": "miniqmt_vnpy_facade_algorithm_characterization_receipt_v1",
            **normalized,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_algorithm_characterization_receipt_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED and self.ordered_failures:
            raise ValueError("PASSED characterization receipt cannot contain failures")
        if self.status is VnpyFacadeCompatibilityStatusV1.FAILED and not self.ordered_failures:
            raise ValueError("FAILED characterization receipt requires failures")
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED and not self.ordered_vector_ids:
            raise ValueError("PASSED characterization receipt requires executed vectors")
        expected_config = hash_hex_v1(
            "miniqmt_vnpy_facade_factory_probe_config_v1",
            thaw_json_v1(self.canonical_factory_probe_config),
        )
        if self.factory_probe_config_sha256 != expected_config:
            raise ValueError("factory probe config hash mismatch")
        if len(self.ordered_failures) > _MAX_FACADE_FAILURES:
            raise ValueError("characterization receipt failure set is unbounded")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_algorithm_characterization_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("characterization receipt hash mismatch")
        return self


class VnpyFacadeConformanceReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_receipt_v1"] = "miniqmt_vnpy_facade_conformance_receipt_v1"
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    algo_code: IdentityV1
    manifest_sha256: Sha256V1
    runtime_binding_disposition: VnpyFacadeRuntimeBindingDispositionV1
    command_authority_disposition: VnpyFacadeCommandAuthorityDispositionV1
    pinned_compatibility_receipt_sha256: Sha256V1
    requirement_sha256: Sha256V1
    surface_sha256: Sha256V1
    source_lock_sha256: Sha256V1
    method_signature_sha256: Sha256V1
    object_field_sha256: Sha256V1
    characterization_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    implementation_binding_set_sha256: Sha256V1
    method_contract_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    algorithm_characterization_receipt_sha256: Sha256V1
    algorithm_binding_sha256: Sha256V1
    status: VnpyFacadeCompatibilityStatusV1
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        normalized = {
            **values,
            "runtime_binding_disposition": VnpyFacadeRuntimeBindingDispositionV1(values["runtime_binding_disposition"]),
            "command_authority_disposition": VnpyFacadeCommandAuthorityDispositionV1(
                values["command_authority_disposition"]
            ),
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
            "ordered_failures": failures,
        }
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_receipt_v1",
            **normalized,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_conformance_receipt_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        expected_command = (
            VnpyFacadeCommandAuthorityDispositionV1.NOT_APPLICABLE_PURE_PLUGIN
            if self.runtime_binding_disposition is VnpyFacadeRuntimeBindingDispositionV1.PURE_PLUGIN_SHADOW_CONFORMANCE
            else VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1
        )
        if self.command_authority_disposition is not expected_command:
            raise ValueError("runtime and command authority dispositions conflict")
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED and self.ordered_failures:
            raise ValueError("PASSED conformance receipt cannot contain failures")
        if self.status is VnpyFacadeCompatibilityStatusV1.FAILED and not self.ordered_failures:
            raise ValueError("FAILED conformance receipt requires failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("conformance receipt hash mismatch")
        return self


class VnpyFacadeConformanceBuildItemV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_build_item_v1"] = (
        "miniqmt_vnpy_facade_conformance_build_item_v1"
    )
    plugin_key: FrozenJsonObjectFieldV1
    registration_descriptor_full_payload: FrozenJsonObjectFieldV1
    pinned_compatibility_receipt_sha256: Sha256V1
    algorithm_characterization_receipt_sha256: Sha256V1
    algorithm_binding_sha256: Sha256V1
    runtime_binding_disposition: VnpyFacadeRuntimeBindingDispositionV1
    command_authority_disposition: VnpyFacadeCommandAuthorityDispositionV1
    build_item_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
            "runtime_binding_disposition": VnpyFacadeRuntimeBindingDispositionV1(values["runtime_binding_disposition"]),
            "command_authority_disposition": VnpyFacadeCommandAuthorityDispositionV1(
                values["command_authority_disposition"]
            ),
        }
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_build_item_v1",
            **normalized,
        }
        return cls(
            **payload,
            build_item_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_conformance_build_item_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_build_item_v1",
            self.canonical_payload_v1(exclude={"build_item_sha256"}),
        )
        if self.build_item_sha256 != expected:
            raise ValueError("conformance build item hash mismatch")
        return self


class VnpyFacadeConformanceSetV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_set_v1"] = "miniqmt_vnpy_facade_conformance_set_v1"
    plugin_catalog_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    ordered_receipts: tuple[VnpyFacadeConformanceReceiptV1, ...]
    build_input_sha256: Sha256V1
    receipt_set_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        values = dict(values)
        receipts = tuple(
            sorted(
                values["ordered_receipts"],
                key=lambda item: (item.plugin_id, item.plugin_version, item.manifest_sha256),
            )
        )
        build_items = tuple(values.pop("build_items"))
        build_payload = {
            "plugin_catalog_sha256": values["plugin_catalog_sha256"],
            "facade_contract_sha256": values["facade_contract_sha256"],
            "dto_mapping_set_sha256": values["dto_mapping_set_sha256"],
            "state_mapping_set_sha256": values["state_mapping_set_sha256"],
            "terminal_mapping_set_sha256": values["terminal_mapping_set_sha256"],
            "isolated_module_binding_set_sha256": values["isolated_module_binding_set_sha256"],
            "facade_source_manifest_sha256": values["facade_source_manifest_sha256"],
            "ordered_build_items": [item.canonical_payload_v1() for item in build_items],
        }
        build_hash = hash_hex_v1("miniqmt_vnpy_facade_conformance_build_input_v1", build_payload)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_set_v1",
            **values,
            "ordered_receipts": [item.canonical_payload_v1() for item in receipts],
            "build_input_sha256": build_hash,
        }
        return cls(
            **{**payload, "ordered_receipts": receipts},
            receipt_set_sha256=hash_hex_v1("miniqmt_vnpy_facade_conformance_set_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_set(self) -> Self:
        ordered = tuple(
            sorted(
                self.ordered_receipts,
                key=lambda item: (item.plugin_id, item.plugin_version, item.manifest_sha256),
            )
        )
        if self.ordered_receipts != ordered or len(
            {(item.plugin_id, item.plugin_version, item.manifest_sha256) for item in ordered}
        ) != len(ordered):
            raise ValueError("conformance receipts must be unique and sorted")
        if not ordered or any(item.status is not VnpyFacadeCompatibilityStatusV1.PASSED for item in ordered):
            raise ValueError("published conformance set requires complete PASSED receipts")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_set_v1",
            self.canonical_payload_v1(exclude={"receipt_set_sha256"}),
        )
        if self.receipt_set_sha256 != expected:
            raise ValueError("conformance receipt set hash mismatch")
        return self


class VnpyFacadeAuthorityInputV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_authority_input_v1"] = "miniqmt_vnpy_facade_authority_input_v1"
    plugin_catalog_snapshot: PluginCatalogSnapshotV1
    gateway_capability_catalog: GatewayCapabilityCatalogV1
    plugin_key: PluginKeyV1
    manifest: ExecutionAlgoPluginManifestV2
    pinned_compatibility_receipt: VnpyCompatibilityReceiptV2
    route_compatibility_receipt: PluginRouteCompatibilityReceiptV1
    facade_conformance_receipt: VnpyFacadeConformanceReceiptV1
    facade_conformance_set: VnpyFacadeConformanceSetV1
    authority_input_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_authority_input_v1",
            **values,
        }
        return cls(
            **payload,
            authority_input_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_authority_input_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        descriptors = tuple(
            item for item in self.plugin_catalog_snapshot.registration_descriptors if item.plugin_key == self.plugin_key
        )
        if len(descriptors) != 1 or descriptors[0].manifest != self.manifest:
            raise ValueError("facade authority requires one exact catalog descriptor/manifest")
        compatibility = tuple(
            item
            for item in self.plugin_catalog_snapshot.pinned_compatibility_receipts
            if item.plugin_key == self.plugin_key
        )
        if compatibility != (self.pinned_compatibility_receipt,):
            raise ValueError("facade authority pinned compatibility receipt conflicts with catalog")
        if self.facade_conformance_set.plugin_catalog_sha256 != self.plugin_catalog_snapshot.catalog_sha256:
            raise ValueError("facade conformance set conflicts with catalog identity")
        self.route_compatibility_receipt.validate_against_authority_v1(
            catalog_snapshot=self.plugin_catalog_snapshot,
            gateway_catalog=self.gateway_capability_catalog,
        )
        if self.route_compatibility_receipt.status is not CompatibilityStatusV1.PASSED:
            raise ValueError("facade authority requires a PASSED route receipt")
        receipt = self.facade_conformance_receipt
        matches = tuple(
            item
            for item in self.facade_conformance_set.ordered_receipts
            if (item.plugin_id, item.plugin_version, item.manifest_sha256) == self.plugin_key.sort_key_v1()
        )
        if (
            matches != (receipt,)
            or receipt.status is not VnpyFacadeCompatibilityStatusV1.PASSED
            or receipt.algo_code != self.manifest.algo_code
            or receipt.manifest_sha256 != self.manifest.manifest_sha256
        ):
            raise ValueError("facade authority requires one exact PASSED conformance receipt")
        k1 = self.pinned_compatibility_receipt
        component_pairs = (
            (receipt.pinned_compatibility_receipt_sha256, k1.receipt_sha256),
            (receipt.requirement_sha256, k1.requirement_sha256),
            (receipt.surface_sha256, k1.surface_sha256),
            (receipt.source_lock_sha256, k1.source_lock_sha256),
            (receipt.method_signature_sha256, k1.method_signature_sha256),
            (receipt.object_field_sha256, k1.object_field_sha256),
            (receipt.characterization_sha256, k1.characterization_sha256),
            (receipt.facade_contract_sha256, self.facade_conformance_set.facade_contract_sha256),
            (receipt.dto_mapping_set_sha256, self.facade_conformance_set.dto_mapping_set_sha256),
            (receipt.state_mapping_set_sha256, self.facade_conformance_set.state_mapping_set_sha256),
            (receipt.terminal_mapping_set_sha256, self.facade_conformance_set.terminal_mapping_set_sha256),
            (
                receipt.isolated_module_binding_set_sha256,
                self.facade_conformance_set.isolated_module_binding_set_sha256,
            ),
            (
                receipt.facade_source_manifest_sha256,
                self.facade_conformance_set.facade_source_manifest_sha256,
            ),
        )
        if any(actual != expected for actual, expected in component_pairs):
            raise ValueError("facade authority component closure drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_authority_input_v1",
            self.canonical_payload_v1(exclude={"authority_input_sha256"}),
        )
        if self.authority_input_sha256 != expected:
            raise ValueError("facade authority input hash mismatch")
        return self


class VnpyFacadeInitializationInputV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_initialization_input_v1"] = (
        "miniqmt_vnpy_facade_initialization_input_v1"
    )
    start_event: RuntimeEventEnvelopeV2
    start_delivery: AlgoEventDeliveryV1
    start_context: AlgoStartContextV1
    authority_input: VnpyFacadeAuthorityInputV1
    transition_id: IdentityV1
    transition_sequence: Literal[1]
    input_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_initialization_input_v1",
            **values,
            "transition_sequence": 1,
        }
        return cls(
            **payload,
            input_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_initialization_input_v1",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_input(self) -> Self:
        if self.start_event.event_type is not EventTypeV2.ALGO_START:
            raise ValueError("facade initialization requires exact ALGO_START event")
        context = self.start_context
        if (
            self.start_event.event_id != context.start_event_id
            or self.start_delivery.event_id != self.start_event.event_id
            or self.start_delivery.delivery_id != context.start_delivery_id
            or self.start_delivery.runtime_id != context.runtime_id
            or self.start_delivery.algo_instance_id != context.algo_instance_id
            or context.plugin_manifest != self.authority_input.manifest
            or context.deterministic_context.transition_sequence != 1
        ):
            raise ValueError("facade initialization owner/event/delivery context is not closed")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_initialization_input_v1",
            self.canonical_payload_v1(exclude={"input_sha256"}),
        )
        if self.input_sha256 != expected:
            raise ValueError("facade initialization input hash mismatch")
        return self


class VnpyFacadeTransitionInputV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_transition_input_v1"] = "miniqmt_vnpy_facade_transition_input_v1"
    runtime_event: RuntimeEventEnvelopeV2
    delivery: AlgoDeliveryPersistenceV1
    algo_instance: ExecutionAlgoInstancePersistenceV2
    manifest: ExecutionAlgoPluginManifestV2
    authority_input: VnpyFacadeAuthorityInputV1
    before_state: AlgoStateSnapshotV2
    read_only_services: AlgoReadOnlyServicesV1
    command_lifecycle_projection: KernelCommandLifecycleProjectionV1
    ordered_active_mappings: tuple[ExecutionCommandChildMappingV1, ...]
    deterministic_context: DeterministicExecutionContextV1
    transition_sequence: PositiveIntV1
    input_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        mappings = tuple(sorted(values["ordered_active_mappings"], key=lambda item: item.local_vt_orderid))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_transition_input_v1",
            **{key: value for key, value in values.items() if key != "ordered_active_mappings"},
            "ordered_active_mappings": mappings,
        }
        return cls(
            **payload,
            input_sha256=hash_hex_v1("miniqmt_vnpy_facade_transition_input_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_input(self) -> Self:
        mappings = tuple(sorted(self.ordered_active_mappings, key=lambda item: item.local_vt_orderid))
        local_ids = tuple(item.local_vt_orderid for item in mappings)
        lifecycle_ids = tuple(item.local_vt_orderid for item in self.command_lifecycle_projection.ordered_items)
        if self.ordered_active_mappings != mappings or len(local_ids) != len(set(local_ids)):
            raise ValueError("active mappings must be unique and sorted")
        if local_ids != lifecycle_ids:
            raise ValueError("active mappings conflict with command lifecycle projection")
        context = self.deterministic_context
        owners = (
            self.runtime_event.runtime_id,
            self.delivery.runtime_id,
            self.algo_instance.runtime_id,
            self.read_only_services.runtime_id,
            self.command_lifecycle_projection.runtime_id,
            context.runtime_id,
        )
        algos = (
            self.delivery.algo_instance_id,
            self.algo_instance.algo_instance_id,
            self.before_state.algo_instance_id,
            self.read_only_services.algo_instance_id,
            self.command_lifecycle_projection.algo_instance_id,
            context.algo_instance_id,
        )
        if len(set(owners)) != 1 or len(set(algos)) != 1:
            raise ValueError("facade transition runtime/algo owner drift")
        if (
            self.runtime_event.event_id != context.event_id
            or self.delivery.event_id != context.event_id
            or self.delivery.delivery_id != context.delivery_id
            or self.delivery.transition_id is None
            or self.delivery.algo_delivery_sequence != self.transition_sequence
            or self.delivery.previous_delivery_id != self.before_state.last_applied_delivery_id
            or self.manifest != self.authority_input.manifest
            or self.before_state.plugin_manifest_sha256 != self.manifest.manifest_sha256
            or self.transition_sequence != context.transition_sequence
        ):
            raise ValueError("facade transition event/delivery/manifest sequence or predecessor context is not closed")
        previous_sequence = self.transition_sequence - 1
        if (
            self.before_state.transition_sequence != previous_sequence
            or self.before_state.last_applied_delivery_sequence != previous_sequence
            or self.before_state.last_closed_delivery_sequence != previous_sequence
            or self.algo_instance.transition_sequence != previous_sequence
            or self.algo_instance.last_applied_delivery_sequence != previous_sequence
            or self.algo_instance.last_closed_delivery_sequence != previous_sequence
            or self.algo_instance.last_applied_delivery_id != self.before_state.last_applied_delivery_id
            or self.algo_instance.state_sha256 != self.before_state.state_sha256
        ):
            raise ValueError("facade transition sequence or predecessor state closure drifted")
        if (
            self.command_lifecycle_projection.event_id != context.event_id
            or self.command_lifecycle_projection.delivery_id != context.delivery_id
            or self.read_only_services.event_id != context.event_id
            or self.read_only_services.delivery_id != context.delivery_id
        ):
            raise ValueError("facade transition projection event/delivery owner drifted")
        if any(
            (
                item.runtime_id,
                item.algo_instance_id,
                item.parent_intent_id,
                item.strategy_slot_id,
                item.symbol,
                item.side,
            )
            != (
                context.runtime_id,
                context.algo_instance_id,
                self.algo_instance.parent_intent_id,
                self.algo_instance.strategy_slot_id,
                self.algo_instance.symbol,
                self.algo_instance.side,
            )
            for item in mappings
        ):
            raise ValueError("active mapping owner conflicts with transition")
        for mapping, lifecycle in zip(mappings, self.command_lifecycle_projection.ordered_items, strict=True):
            if (
                lifecycle.mapping_id != mapping.mapping_id
                or lifecycle.mapping_version != mapping.mapping_version
                or lifecycle.mapping_payload_sha256 != mapping.payload_sha256
                or lifecycle.local_vt_orderid != mapping.local_vt_orderid
                or lifecycle.submit_command_id != mapping.command_id
                or lifecycle.broker_order_id != mapping.broker_order_id
                or lifecycle.mapping_status is not mapping.mapping_status
            ):
                raise ValueError("active mapping conflicts with command lifecycle facts")
        before = VnpyFacadeStateEnvelopeV1.model_validate_json(
            json.dumps(thaw_json_v1(self.before_state.state), sort_keys=True, separators=(",", ":")),
            strict=True,
        )
        active_by_local = {item.local_vt_orderid: item for item in before.ordered_active_orders}
        if tuple(sorted(active_by_local)) != local_ids:
            raise ValueError("facade before-state active orders conflict with durable mapping identities")
        for mapping in mappings:
            active = active_by_local[mapping.local_vt_orderid]
            immutable_drift = (
                active.command_id != mapping.command_id
                or active.child_order_id != mapping.child_order_id
                or active.symbol != mapping.symbol
                or active.side != mapping.side.value
                or active.price_decimal != mapping.requested_price_decimal
                or active.requested_quantity != mapping.requested_quantity
            )
            mutable_drift = active.broker_order_id != mapping.broker_order_id
            if immutable_drift or (
                self.runtime_event.event_type
                not in {
                    EventTypeV2.COMMAND_OUTCOME,
                    EventTypeV2.ORDER,
                    EventTypeV2.TRADE,
                    EventTypeV2.RECONCILE,
                }
                and mutable_drift
            ):
                raise ValueError("facade before-state active order conflicts with durable mapping facts")
        validate_vnpy_facade_transition_market_authority_v1(
            event=self.runtime_event,
            services=self.read_only_services,
            deterministic_context=self.deterministic_context,
        )
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_transition_input_v1",
            self.canonical_payload_v1(exclude={"input_sha256"}),
        )
        if self.input_sha256 != expected:
            raise ValueError("facade transition input hash mismatch")
        return self


class VnpyFacadeTraceCallV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_trace_call_v1"] = "miniqmt_vnpy_facade_trace_call_v1"
    ordinal: NonNegativeIntV1
    method_name: IdentityV1
    normalized_arguments: FrozenJsonObjectFieldV1
    return_disposition: Literal["VALUE", "NONE", "EMPTY_STRING", "RAISED"]
    normalized_return_or_null: FrozenJsonFieldV1 | None
    ordered_diagnostic_reason_codes: tuple[IdentityV1, ...]
    call_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        reasons = tuple(values["ordered_diagnostic_reason_codes"])
        if any(type(item) is not str or not item or item != item.strip() for item in reasons):
            raise TypeError("trace diagnostic reason codes must be trim-stable strings")
        payload = {
            "schema_version": "miniqmt_vnpy_facade_trace_call_v1",
            **values,
            "normalized_arguments": json_safe_evidence_v1(values["normalized_arguments"]),
            "normalized_return_or_null": json_safe_evidence_v1(values["normalized_return_or_null"]),
            "ordered_diagnostic_reason_codes": reasons,
        }
        return cls(
            **payload,
            call_sha256=hash_hex_v1("miniqmt_vnpy_facade_trace_call_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_trace_call(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_trace_call_v1",
            self.canonical_payload_v1(exclude={"call_sha256"}),
        )
        if self.call_sha256 != expected:
            raise ValueError("trace call hash mismatch")
        return self


class VnpyFacadeTraceEffectV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_trace_effect_v1"] = "miniqmt_vnpy_facade_trace_effect_v1"
    ordinal: NonNegativeIntV1
    effect_kind: Literal["BROKER_COMMAND", "TIMER_MUTATION", "DIAGNOSTIC"]
    carrier_schema_version: IdentityV1
    carrier_full_payload: FrozenJsonObjectFieldV1
    carrier_identity: IdentityV1
    carrier_sha256: Sha256V1
    effect_sha256: Sha256V1

    @classmethod
    def create(cls, *, ordinal: int, carrier: BrokerCommandV2 | TimerMutationV1 | DiagnosticObservationV1) -> Self:
        if isinstance(carrier, BrokerCommandV2):
            effect_kind = "BROKER_COMMAND"
            carrier_identity = carrier.command_id
            carrier_sha256 = carrier.payload_sha256
        elif isinstance(carrier, TimerMutationV1):
            effect_kind = "TIMER_MUTATION"
            carrier_identity = carrier.mutation_identity_v1()
            carrier_sha256 = carrier.payload_sha256
        elif isinstance(carrier, DiagnosticObservationV1):
            effect_kind = "DIAGNOSTIC"
            carrier_identity = carrier.observation_id
            carrier_sha256 = carrier.context_sha256
        else:
            raise TypeError("trace effect carrier must be an existing K1/K2 effect carrier")
        payload = {
            "schema_version": "miniqmt_vnpy_facade_trace_effect_v1",
            "ordinal": ordinal,
            "effect_kind": effect_kind,
            "carrier_schema_version": carrier.schema_version,
            "carrier_full_payload": carrier.canonical_payload_v1(),
            "carrier_identity": carrier_identity,
            "carrier_sha256": carrier_sha256,
        }
        return cls(
            **payload,
            effect_sha256=hash_hex_v1("miniqmt_vnpy_facade_trace_effect_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_trace_effect(self) -> Self:
        payload = thaw_json_v1(self.carrier_full_payload)
        if self.effect_kind == "BROKER_COMMAND":
            carrier = BrokerCommandV2.model_validate_json(
                json.dumps(payload, sort_keys=True, separators=(",", ":")), strict=True
            )
            expected_identity = carrier.command_id
            expected_carrier_sha = carrier.payload_sha256
        elif self.effect_kind == "TIMER_MUTATION":
            carrier = TimerMutationV1.model_validate_json(
                json.dumps(payload, sort_keys=True, separators=(",", ":")), strict=True
            )
            expected_identity = carrier.mutation_identity_v1()
            expected_carrier_sha = carrier.payload_sha256
        else:
            carrier = DiagnosticObservationV1.model_validate_json(
                json.dumps(payload, sort_keys=True, separators=(",", ":")), strict=True
            )
            expected_identity = carrier.observation_id
            expected_carrier_sha = carrier.context_sha256
        if (
            self.carrier_schema_version != carrier.schema_version
            or self.carrier_identity != expected_identity
            or self.carrier_sha256 != expected_carrier_sha
        ):
            raise ValueError("trace effect carrier identity/hash closure mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_trace_effect_v1",
            self.canonical_payload_v1(exclude={"effect_sha256"}),
        )
        if self.effect_sha256 != expected:
            raise ValueError("trace effect hash mismatch")
        return self


class VnpyFacadeExecutedVectorResultV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_executed_vector_result_v1"] = (
        "miniqmt_vnpy_facade_executed_vector_result_v1"
    )
    vector_id: IdentityV1
    vector_sha256: Sha256V1
    scenario_id: IdentityV1
    step_ordinal: NonNegativeIntV1
    source_executor_binding_sha256: Sha256V1
    source_identity_sha256: Sha256V1
    invocation_status: Literal["COMPLETED", "FAILED"]
    actual_ordered_facade_calls: tuple[VnpyFacadeTraceCallV1, ...]
    actual_ordered_effects: tuple[VnpyFacadeTraceEffectV1, ...]
    actual_after_state_or_null: VnpyFacadeSourceStateEnvelopeV1 | None
    actual_terminal_outcome: TerminalOutcomeV1 | None
    consumed_deterministic_inputs: VnpyFacadeDeterministicInputsV1
    ordered_execution_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    result_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        calls = tuple(values["actual_ordered_facade_calls"])
        effects = tuple(values["actual_ordered_effects"])
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_execution_failures"]))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_executed_vector_result_v1",
            **values,
            "actual_ordered_facade_calls": calls,
            "actual_ordered_effects": effects,
            "ordered_execution_failures": failures,
        }
        return cls(
            **payload,
            result_sha256=hash_hex_v1("miniqmt_vnpy_facade_executed_vector_result_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_execution_result(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_execution_failures)
        if tuple(item.ordinal for item in self.actual_ordered_facade_calls) != tuple(
            range(len(self.actual_ordered_facade_calls))
        ):
            raise ValueError("facade trace call ordinals must be contiguous")
        if tuple(item.ordinal for item in self.actual_ordered_effects) != tuple(
            range(len(self.actual_ordered_effects))
        ):
            raise ValueError("facade trace effect ordinals must be contiguous")
        if self.invocation_status == "COMPLETED":
            if self.ordered_execution_failures:
                raise ValueError("COMPLETED execution result cannot contain failures")
            if self.actual_after_state_or_null is None:
                raise ValueError("COMPLETED execution result requires an exact after state")
        elif not self.ordered_execution_failures:
            raise ValueError("FAILED execution result requires failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_executed_vector_result_v1",
            self.canonical_payload_v1(exclude={"result_sha256"}),
        )
        if self.result_sha256 != expected:
            raise ValueError("executed vector result hash mismatch")
        return self


class VnpyFacadeSourceExecutionSetV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_source_execution_set_v1"] = (
        "miniqmt_vnpy_facade_source_execution_set_v1"
    )
    algo_code: IdentityV1
    characterization_requirement_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    vector_set_sha256: Sha256V1
    ordered_results: tuple[VnpyFacadeExecutedVectorResultV1, ...]
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    status: VnpyFacadeCompatibilityStatusV1
    execution_set_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        results = tuple(
            sorted(
                values["ordered_results"],
                key=lambda item: (item.scenario_id, item.step_ordinal, item.vector_id),
            )
        )
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_source_execution_set_v1",
            **values,
            "ordered_results": results,
            "ordered_failures": failures,
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
        }
        return cls(
            **payload,
            execution_set_sha256=hash_hex_v1("miniqmt_vnpy_facade_source_execution_set_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_execution_set(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        ordered = tuple(
            sorted(
                self.ordered_results,
                key=lambda item: (item.scenario_id, item.step_ordinal, item.vector_id),
            )
        )
        vector_ids = tuple(item.vector_id for item in ordered)
        if self.ordered_results != ordered or len(vector_ids) != len(set(vector_ids)):
            raise ValueError("source execution results must be unique and canonically ordered")
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED:
            if self.ordered_failures or not ordered:
                raise ValueError("PASSED source execution set requires complete results without failures")
            if any(item.invocation_status != "COMPLETED" for item in ordered):
                raise ValueError("PASSED source execution set cannot contain a FAILED result")
        elif not self.ordered_failures:
            raise ValueError("FAILED source execution set requires aggregate failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_source_execution_set_v1",
            self.canonical_payload_v1(exclude={"execution_set_sha256"}),
        )
        if self.execution_set_sha256 != expected:
            raise ValueError("source execution set hash mismatch")
        return self


class VnpyFacadeSourceExecutorBindingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_source_executor_binding_v1"] = (
        "miniqmt_vnpy_facade_source_executor_binding_v1"
    )
    executor_ref: IdentityV1
    executor_signature_sha256: Sha256V1
    executor_source_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    implementation_binding_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    vector_artifact_sha256: Sha256V1
    vector_artifact_file_sha256: Sha256V1
    supported_algo_codes: tuple[IdentityV1, ...]
    binding_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        algo_codes = _sorted_unique_strings(tuple(values["supported_algo_codes"]), field_name="supported_algo_codes")
        payload = {
            "schema_version": "miniqmt_vnpy_facade_source_executor_binding_v1",
            **values,
            "supported_algo_codes": algo_codes,
        }
        return cls(
            **payload,
            binding_sha256=hash_hex_v1("miniqmt_vnpy_facade_source_executor_binding_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_source_executor_binding(self) -> Self:
        expected_algos = (
            "BEST_LIMIT_MINIQMT",
            "ICEBERG",
            "SNIPER_MINIQMT",
            "STOP",
            "TWAP_LITE_MINIQMT",
        )
        if self.supported_algo_codes != expected_algos:
            raise ValueError("source executor binding must support the exact five-algorithm set")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_source_executor_binding_v1",
            self.canonical_payload_v1(exclude={"binding_sha256"}),
        )
        if self.binding_sha256 != expected:
            raise ValueError("source executor binding hash mismatch")
        return self


class VnpyFacadeAlgorithmCharacterizationReceiptV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_algorithm_characterization_receipt_v2"] = (
        "miniqmt_vnpy_facade_algorithm_characterization_receipt_v2"
    )
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    characterization_requirement_sha256: Sha256V1
    canonical_factory_probe_config: FrozenJsonObjectFieldV1
    factory_probe_config_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    implementation_binding_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    source_execution_set_sha256: Sha256V1
    ordered_vector_ids: tuple[IdentityV1, ...]
    vector_set_sha256: Sha256V1
    status: VnpyFacadeCompatibilityStatusV1
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        vectors = _sorted_unique_strings(tuple(values["ordered_vector_ids"]), field_name="ordered_vector_ids")
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_algorithm_characterization_receipt_v2",
            **values,
            "ordered_vector_ids": vectors,
            "ordered_failures": failures,
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_algorithm_characterization_receipt_v2",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_characterization_receipt_v2(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED:
            if self.ordered_failures or not self.ordered_vector_ids:
                raise ValueError("PASSED characterization receipt V2 requires vectors without failures")
        elif not self.ordered_failures:
            raise ValueError("FAILED characterization receipt V2 requires failures")
        expected_config = hash_hex_v1(
            "miniqmt_vnpy_facade_factory_probe_config_v1",
            thaw_json_v1(self.canonical_factory_probe_config),
        )
        if self.factory_probe_config_sha256 != expected_config:
            raise ValueError("factory probe config hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_algorithm_characterization_receipt_v2",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("characterization receipt hash mismatch")
        return self


class VnpyFacadeAlgorithmBindingV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_algorithm_binding_v2"] = "miniqmt_vnpy_facade_algorithm_binding_v2"
    algo_code: IdentityV1
    source_identity_sha256: Sha256V1
    class_ref: IdentityV1
    constructor_signature_sha256: Sha256V1
    constructor_body_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    characterization_receipt_sha256: Sha256V1
    adapter_contract_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    source_execution_set_sha256: Sha256V1
    binding_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {"schema_version": "miniqmt_vnpy_facade_algorithm_binding_v2", **values}
        return cls(
            **payload,
            binding_sha256=hash_hex_v1("miniqmt_vnpy_facade_algorithm_binding_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_algorithm_binding_v2(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_algorithm_binding_v2",
            self.canonical_payload_v1(exclude={"binding_sha256"}),
        )
        if self.binding_sha256 != expected:
            raise ValueError("algorithm binding V2 hash mismatch")
        return self


class VnpyFacadeCharacterizationManifestViewV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_manifest_view_v1"] = (
        "miniqmt_vnpy_facade_characterization_manifest_view_v1"
    )
    algo_code: IdentityV1
    registration_disposition: VnpyFacadeRegistrationDispositionV1
    real_plugin_key_or_null: FrozenJsonObjectFieldV1 | None
    real_manifest_sha256_or_null: Sha256V1 | None
    required_facade_methods: tuple[IdentityV1, ...]
    required_object_fields: tuple[IdentityV1, ...]
    required_enum_members: tuple[IdentityV1, ...]
    order_types: tuple[IdentityV1, ...]
    market_data_capabilities: tuple[IdentityV1, ...]
    state_schema_sha256: Sha256V1
    characterization_requirement_sha256: Sha256V1
    view_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = dict(values)
        normalized["registration_disposition"] = VnpyFacadeRegistrationDispositionV1(values["registration_disposition"])
        for field in (
            "required_facade_methods",
            "required_object_fields",
            "required_enum_members",
            "order_types",
            "market_data_capabilities",
        ):
            normalized[field] = _sorted_unique_strings(tuple(values[field]), field_name=field)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_manifest_view_v1",
            **normalized,
        }
        return cls(
            **payload,
            view_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_manifest_view_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_manifest_view(self) -> Self:
        registered = self.registration_disposition is VnpyFacadeRegistrationDispositionV1.REGISTERED_CURRENT_THREE
        if registered != (self.real_plugin_key_or_null is not None and self.real_manifest_sha256_or_null is not None):
            raise ValueError("characterization manifest view real plugin identity conflicts with disposition")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_manifest_view_v1",
            self.canonical_payload_v1(exclude={"view_sha256"}),
        )
        if self.view_sha256 != expected:
            raise ValueError("characterization manifest view hash mismatch")
        return self


class VnpyFacadeCharacterizationStartContextV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_start_context_v2"] = (
        "miniqmt_vnpy_facade_characterization_start_context_v2"
    )
    vector_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    symbol: IdentityV1
    side: SideV1
    limit_price_decimal: IdentityV1
    parent_quantity: PositiveIntV1
    contract_projection: FrozenJsonObjectFieldV1
    deterministic_context: DeterministicExecutionContextV1
    manifest_view: VnpyFacadeCharacterizationManifestViewV1
    canonical_config: FrozenJsonObjectFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_start_context_v2",
            **values,
            "side": SideV1(values["side"]),
            "limit_price_decimal": canonical_decimal_string_v1(
                values["limit_price_decimal"],
                field_name="limit_price_decimal",
                allow_zero=False,
            ),
        }
        return cls(
            **payload,
            context_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_start_context_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_characterization_start_context(self) -> Self:
        context = self.deterministic_context
        if context.runtime_id != self.runtime_id or context.algo_instance_id != self.algo_instance_id:
            raise ValueError("characterization start context deterministic owner closure drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_start_context_v2",
            self.canonical_payload_v1(exclude={"context_sha256"}),
        )
        if self.context_sha256 != expected:
            raise ValueError("characterization start context hash mismatch")
        return self


class VnpyFacadeExpectedTraceAuthorityRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_expected_trace_authority_ref_v1"] = (
        "miniqmt_vnpy_facade_expected_trace_authority_ref_v1"
    )
    authority_kind: Literal["K3_COMMITTED_PARITY", "K4_PINNED_CHARACTERIZATION"]
    authority_identity_sha256: Sha256V1
    source_snapshot_sha256_or_null: Sha256V1 | None
    parity_input_sha256_or_null: Sha256V1 | None
    parity_receipt_sha256_or_null: Sha256V1 | None
    ref_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {"schema_version": "miniqmt_vnpy_facade_expected_trace_authority_ref_v1", **values}
        return cls(
            **payload,
            ref_sha256=hash_hex_v1("miniqmt_vnpy_facade_expected_trace_authority_ref_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_expected_trace_authority(self) -> Self:
        k3_fields = (
            self.source_snapshot_sha256_or_null,
            self.parity_input_sha256_or_null,
            self.parity_receipt_sha256_or_null,
        )
        if self.authority_kind == "K3_COMMITTED_PARITY":
            if any(item is None for item in k3_fields):
                raise ValueError("K3 expected trace authority requires full parity identities")
        elif any(item is not None for item in k3_fields):
            raise ValueError("K4 pinned characterization authority must not fabricate K3 identities")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_expected_trace_authority_ref_v1",
            self.canonical_payload_v1(exclude={"ref_sha256"}),
        )
        if self.ref_sha256 != expected:
            raise ValueError("expected trace authority ref hash mismatch")
        return self


class VnpyFacadeCharacterizationVectorV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_vector_v2"] = (
        "miniqmt_vnpy_facade_characterization_vector_v2"
    )
    vector_id: IdentityV1
    scenario_id: IdentityV1
    step_ordinal: NonNegativeIntV1
    predecessor_vector_id_or_INIT: IdentityV1
    algo_code: IdentityV1
    side: SideV1
    invocation_phase: Literal["INITIALIZE", "TRANSITION"]
    canonical_config: FrozenJsonObjectFieldV1
    deterministic_context: DeterministicExecutionContextV1
    start_context_or_null: VnpyFacadeCharacterizationStartContextV2 | None
    runtime_event_or_null: RuntimeEventEnvelopeV2 | None
    read_only_services_or_null: AlgoReadOnlyServicesV1 | None
    before_state_or_null: VnpyFacadeSourceStateEnvelopeV1 | None
    ordered_active_mappings: tuple[ExecutionCommandChildMappingV1, ...]
    explicit_deterministic_inputs: VnpyFacadeDeterministicInputsV1
    expected_trace_authority_ref: VnpyFacadeExpectedTraceAuthorityRefV1
    expected_ordered_facade_calls: tuple[VnpyFacadeTraceCallV1, ...]
    expected_ordered_effects: tuple[VnpyFacadeTraceEffectV1, ...]
    expected_after_state: VnpyFacadeSourceStateEnvelopeV1
    expected_terminal_outcome: TerminalOutcomeV1 | None
    vector_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        mappings = tuple(sorted(values["ordered_active_mappings"], key=lambda item: item.local_vt_orderid))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_vector_v2",
            **values,
            "side": SideV1(values["side"]),
            "ordered_active_mappings": mappings,
        }
        return cls(
            **payload,
            vector_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_vector_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_characterization_vector_v2(self) -> Self:
        if self.step_ordinal == 0:
            if self.predecessor_vector_id_or_INIT != "INIT":
                raise ValueError("first scenario vector predecessor must be INIT")
        elif self.predecessor_vector_id_or_INIT == "INIT":
            raise ValueError("non-initial scenario vector requires predecessor identity")
        if self.invocation_phase == "INITIALIZE":
            if self.start_context_or_null is None or any(
                item is not None
                for item in (
                    self.runtime_event_or_null,
                    self.read_only_services_or_null,
                    self.before_state_or_null,
                )
            ):
                raise ValueError("INITIALIZE vector must carry only exact start context")
            if self.start_context_or_null.vector_id != self.vector_id:
                raise ValueError("INITIALIZE vector/start context identity drifted")
        else:
            if self.start_context_or_null is not None or any(
                item is None
                for item in (
                    self.runtime_event_or_null,
                    self.read_only_services_or_null,
                    self.before_state_or_null,
                )
            ):
                raise ValueError("TRANSITION vector requires full event/services/before-state")
            assert self.runtime_event_or_null is not None
            assert self.read_only_services_or_null is not None
            if (
                self.runtime_event_or_null.runtime_id != self.deterministic_context.runtime_id
                or self.runtime_event_or_null.event_id != self.deterministic_context.event_id
                or self.read_only_services_or_null.event_id != self.runtime_event_or_null.event_id
                or self.read_only_services_or_null.delivery_id != self.deterministic_context.delivery_id
            ):
                raise ValueError("TRANSITION vector owner/event/service closure drifted")
        if tuple(item.ordinal for item in self.expected_ordered_facade_calls) != tuple(
            range(len(self.expected_ordered_facade_calls))
        ):
            raise ValueError("expected facade call ordinals must be contiguous")
        if tuple(item.ordinal for item in self.expected_ordered_effects) != tuple(
            range(len(self.expected_ordered_effects))
        ):
            raise ValueError("expected facade effect ordinals must be contiguous")
        local_ids = tuple(item.local_vt_orderid for item in self.ordered_active_mappings)
        if len(local_ids) != len(set(local_ids)):
            raise ValueError("characterization vector active mappings must be unique")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_v2",
            self.canonical_payload_v1(exclude={"vector_sha256"}),
        )
        if self.vector_sha256 != expected:
            raise ValueError("characterization vector V2 hash mismatch")
        return self


class VnpyFacadeK3ExpectedTraceMaterialV1(FrozenStrictModel):
    """Full offline K3 committed-fact material; services owns its typed reconstruction."""

    schema_version: Literal["miniqmt_vnpy_facade_k3_expected_trace_material_v1"] = (
        "miniqmt_vnpy_facade_k3_expected_trace_material_v1"
    )
    algo_code: IdentityV1
    side: SideV1
    source_snapshot: FrozenJsonObjectFieldV1
    repository_runtime: FrozenJsonObjectFieldV1
    ordered_repository_events: tuple[FrozenJsonObjectFieldV1, ...]
    ordered_repository_algos: tuple[FrozenJsonObjectFieldV1, ...]
    ordered_repository_children: tuple[FrozenJsonObjectFieldV1, ...]
    parity_input: FrozenJsonObjectFieldV1
    parity_receipt: FrozenJsonObjectFieldV1
    material_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_k3_expected_trace_material_v1",
            **values,
            "side": SideV1(values["side"]),
            "ordered_repository_events": tuple(values["ordered_repository_events"]),
            "ordered_repository_algos": tuple(values["ordered_repository_algos"]),
            "ordered_repository_children": tuple(values["ordered_repository_children"]),
        }
        return cls(
            **payload,
            material_sha256=hash_hex_v1("miniqmt_vnpy_facade_k3_expected_trace_material_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_k3_material(self) -> Self:
        snapshot = thaw_json_v1(self.source_snapshot)
        parity_input = thaw_json_v1(self.parity_input)
        parity_receipt = thaw_json_v1(self.parity_receipt)
        if any(type(item) is not dict for item in (snapshot, parity_input, parity_receipt)):
            raise TypeError("K3 expected trace material core carriers must be strict objects")
        if (
            parity_input.get("algo_code") != self.algo_code
            or parity_input.get("side") != self.side.value
            or parity_receipt.get("algo_code") != self.algo_code
            or parity_receipt.get("parity_input_sha256") != parity_input.get("input_sha256")
            or parity_receipt.get("status") != "PASSED"
            or parity_receipt.get("broker_called") is not False
        ):
            raise ValueError("K3 expected trace material owner/PASSED input closure drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_k3_expected_trace_material_v1",
            self.canonical_payload_v1(exclude={"material_sha256"}),
        )
        if self.material_sha256 != expected:
            raise ValueError("K3 expected trace material hash mismatch")
        return self


class VnpyFacadeCharacterizationVectorArtifactV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_characterization_vector_artifact_v2"] = (
        "miniqmt_vnpy_facade_characterization_vector_artifact_v2"
    )
    k3_source_commit_sha: IdentityV1
    k3_contract_binding_sha256: Sha256V1
    ordered_k3_expected_trace_materials: tuple[VnpyFacadeK3ExpectedTraceMaterialV1, ...]
    ordered_vectors: tuple[VnpyFacadeCharacterizationVectorV2, ...]
    vector_set_sha256: Sha256V1
    artifact_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        materials = tuple(
            sorted(
                values["ordered_k3_expected_trace_materials"],
                key=lambda item: (item.algo_code, item.side.value),
            )
        )
        vectors = tuple(
            sorted(
                values["ordered_vectors"],
                key=lambda item: (item.algo_code, item.scenario_id, item.step_ordinal, item.vector_id),
            )
        )
        vector_set_sha = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_artifact_set_v2",
            [item.canonical_payload_v1() for item in vectors],
        )
        payload = {
            "schema_version": "miniqmt_vnpy_facade_characterization_vector_artifact_v2",
            "k3_source_commit_sha": values["k3_source_commit_sha"],
            "k3_contract_binding_sha256": values["k3_contract_binding_sha256"],
            "ordered_k3_expected_trace_materials": materials,
            "ordered_vectors": vectors,
            "vector_set_sha256": vector_set_sha,
        }
        return cls(
            **payload,
            artifact_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_vector_artifact_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_vector_artifact(self) -> Self:
        if re.fullmatch(r"[0-9a-f]{40}", self.k3_source_commit_sha) is None:
            raise ValueError("K3 source commit must be an exact lowercase git commit")
        materials = tuple(
            sorted(
                self.ordered_k3_expected_trace_materials,
                key=lambda item: (item.algo_code, item.side.value),
            )
        )
        expected_material_keys = tuple(
            (algo_code, side)
            for algo_code in ("BEST_LIMIT_MINIQMT", "SNIPER_MINIQMT", "TWAP_LITE_MINIQMT")
            for side in (SideV1.BUY, SideV1.SELL)
        )
        if (
            materials != self.ordered_k3_expected_trace_materials
            or tuple((item.algo_code, item.side) for item in materials) != expected_material_keys
        ):
            raise ValueError("vector artifact requires the exact current-three BUY/SELL K3 material set")
        vectors = tuple(
            sorted(
                self.ordered_vectors,
                key=lambda item: (item.algo_code, item.scenario_id, item.step_ordinal, item.vector_id),
            )
        )
        if vectors != self.ordered_vectors or len({item.vector_id for item in vectors}) != len(vectors):
            raise ValueError("vector artifact vectors must be unique and canonically ordered")
        if tuple(sorted({item.algo_code for item in vectors})) != (
            "BEST_LIMIT_MINIQMT",
            "ICEBERG",
            "SNIPER_MINIQMT",
            "STOP",
            "TWAP_LITE_MINIQMT",
        ):
            raise ValueError("vector artifact must cover exactly five algorithms")
        by_material = {(item.algo_code, item.side): item for item in materials}
        scenarios: dict[tuple[str, str], list[VnpyFacadeCharacterizationVectorV2]] = {}
        for vector in vectors:
            scenarios.setdefault((vector.algo_code, vector.scenario_id), []).append(vector)
            material = by_material.get((vector.algo_code, vector.side))
            ref = vector.expected_trace_authority_ref
            if material is None:
                if ref.authority_kind != "K4_PINNED_CHARACTERIZATION":
                    raise ValueError("characterization-only algorithm cannot claim K3 parity authority")
            else:
                snapshot = thaw_json_v1(material.source_snapshot)
                parity_input = thaw_json_v1(material.parity_input)
                parity_receipt = thaw_json_v1(material.parity_receipt)
                if (
                    ref.authority_kind != "K3_COMMITTED_PARITY"
                    or ref.authority_identity_sha256 != material.material_sha256
                    or ref.source_snapshot_sha256_or_null != snapshot.get("source_set_sha256")
                    or ref.parity_input_sha256_or_null != parity_input.get("input_sha256")
                    or ref.parity_receipt_sha256_or_null != parity_receipt.get("receipt_sha256")
                ):
                    raise ValueError("current-three vector K3 authority reference drifted")
        for scenario_vectors in scenarios.values():
            if tuple(item.step_ordinal for item in scenario_vectors) != tuple(range(len(scenario_vectors))):
                raise ValueError("vector artifact scenario steps must be contiguous from zero")
            for previous, current in zip(scenario_vectors, scenario_vectors[1:]):
                if current.predecessor_vector_id_or_INIT != previous.vector_id:
                    raise ValueError("vector artifact scenario predecessor chain drifted")
        required_events = {
            "BEST_LIMIT_MINIQMT": {
                "ALGO_START",
                "COMMAND_OUTCOME",
                "EOD",
                "ORDER",
                "RECONCILE",
                "SESSION",
                "TICK",
                "TRADE",
            },
            "SNIPER_MINIQMT": {
                "ALGO_START",
                "COMMAND_OUTCOME",
                "EOD",
                "ORDER",
                "RECONCILE",
                "SESSION",
                "TICK",
                "TRADE",
            },
            "TWAP_LITE_MINIQMT": {
                "ALGO_START",
                "COMMAND_OUTCOME",
                "EOD",
                "ORDER",
                "RECONCILE",
                "SESSION",
                "TICK",
                "TIMER",
                "TRADE",
            },
            "ICEBERG": {"ALGO_START", "TIMER", "ORDER", "TRADE"},
            "STOP": {"ALGO_START", "TICK", "ORDER", "TRADE"},
        }
        for algo_code, expected_events in required_events.items():
            selected = tuple(item for item in vectors if item.algo_code == algo_code)
            actual_events = {
                "ALGO_START" if item.invocation_phase == "INITIALIZE" else item.runtime_event_or_null.event_type.value
                for item in selected
                if item.invocation_phase == "INITIALIZE" or item.runtime_event_or_null is not None
            }
            if actual_events != expected_events or {item.side for item in selected} != {SideV1.BUY, SideV1.SELL}:
                raise ValueError("vector artifact event/side characterization matrix is incomplete")
        iceberg_configs = [thaw_json_v1(item.canonical_config) for item in vectors if item.algo_code == "ICEBERG"]
        iceberg_intervals = {item.get("interval") for item in iceberg_configs}
        iceberg_displays = {item.get("display_volume") for item in iceberg_configs}
        if (
            not {0, 1}.issubset(iceberg_intervals)
            or not any(type(item) is int and item > 1 for item in iceberg_intervals)
            or 0 not in iceberg_displays
            or not any(
                type(item) in (int, float) and not isinstance(item, bool) and item > 0 for item in iceberg_displays
            )
        ):
            raise ValueError("Iceberg interval/display-volume characterization matrix is incomplete")
        stop_adds = {
            thaw_json_v1(item.canonical_config).get("price_add") for item in vectors if item.algo_code == "STOP"
        }
        numeric_stop_adds = {
            float(item) for item in stop_adds if type(item) in (int, float, str) and not isinstance(item, bool)
        }
        if not any(item < 0 for item in numeric_stop_adds) or not any(item > 0 for item in numeric_stop_adds):
            raise ValueError("Stop signed price-add characterization matrix is incomplete")
        expected_set = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_artifact_set_v2",
            [item.canonical_payload_v1() for item in vectors],
        )
        if self.vector_set_sha256 != expected_set:
            raise ValueError("vector artifact set hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_artifact_v2",
            self.canonical_payload_v1(exclude={"artifact_sha256"}),
        )
        if self.artifact_sha256 != expected:
            raise ValueError("vector artifact hash mismatch")
        return self


class VnpyFacadeConformanceReceiptV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_receipt_v2"] = "miniqmt_vnpy_facade_conformance_receipt_v2"
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    algo_code: IdentityV1
    manifest_sha256: Sha256V1
    runtime_binding_disposition: VnpyFacadeRuntimeBindingDispositionV1
    command_authority_disposition: VnpyFacadeCommandAuthorityDispositionV1
    pinned_compatibility_receipt_sha256: Sha256V1
    requirement_sha256: Sha256V1
    surface_sha256: Sha256V1
    source_lock_sha256: Sha256V1
    method_signature_sha256: Sha256V1
    object_field_sha256: Sha256V1
    characterization_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    implementation_binding_set_sha256: Sha256V1
    method_contract_set_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    source_execution_set_sha256: Sha256V1
    algorithm_characterization_receipt_v2_sha256: Sha256V1
    algorithm_binding_sha256: Sha256V1
    status: VnpyFacadeCompatibilityStatusV1
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_receipt_v2",
            **values,
            "runtime_binding_disposition": VnpyFacadeRuntimeBindingDispositionV1(values["runtime_binding_disposition"]),
            "command_authority_disposition": VnpyFacadeCommandAuthorityDispositionV1(
                values["command_authority_disposition"]
            ),
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
            "ordered_failures": failures,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1("miniqmt_vnpy_facade_conformance_receipt_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_conformance_receipt_v2(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        expected_command = (
            VnpyFacadeCommandAuthorityDispositionV1.NOT_APPLICABLE_PURE_PLUGIN
            if self.runtime_binding_disposition is VnpyFacadeRuntimeBindingDispositionV1.PURE_PLUGIN_SHADOW_CONFORMANCE
            else VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1
        )
        if self.command_authority_disposition is not expected_command:
            raise ValueError("runtime and command authority dispositions conflict")
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED:
            if self.ordered_failures:
                raise ValueError("PASSED conformance receipt V2 cannot contain failures")
        elif not self.ordered_failures:
            raise ValueError("FAILED conformance receipt V2 requires failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_receipt_v2",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("conformance receipt V2 hash mismatch")
        return self


class VnpyFacadeConformanceBuildItemV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_build_item_v2"] = (
        "miniqmt_vnpy_facade_conformance_build_item_v2"
    )
    plugin_key: FrozenJsonObjectFieldV1
    registration_descriptor_full_payload: FrozenJsonObjectFieldV1
    pinned_compatibility_receipt_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    source_execution_set_sha256: Sha256V1
    algorithm_characterization_receipt_v2_sha256: Sha256V1
    algorithm_binding_sha256: Sha256V1
    runtime_binding_disposition: VnpyFacadeRuntimeBindingDispositionV1
    command_authority_disposition: VnpyFacadeCommandAuthorityDispositionV1
    build_item_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_build_item_v2",
            **values,
            "runtime_binding_disposition": VnpyFacadeRuntimeBindingDispositionV1(values["runtime_binding_disposition"]),
            "command_authority_disposition": VnpyFacadeCommandAuthorityDispositionV1(
                values["command_authority_disposition"]
            ),
        }
        return cls(
            **payload,
            build_item_sha256=hash_hex_v1("miniqmt_vnpy_facade_conformance_build_item_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_conformance_build_item_v2(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_build_item_v2",
            self.canonical_payload_v1(exclude={"build_item_sha256"}),
        )
        if self.build_item_sha256 != expected:
            raise ValueError("conformance build item V2 hash mismatch")
        return self


class VnpyFacadeConformanceSetV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_set_v2"] = "miniqmt_vnpy_facade_conformance_set_v2"
    plugin_catalog_sha256: Sha256V1
    facade_contract_sha256: Sha256V1
    dto_mapping_set_sha256: Sha256V1
    state_mapping_set_sha256: Sha256V1
    terminal_mapping_set_sha256: Sha256V1
    isolated_module_binding_set_sha256: Sha256V1
    facade_source_manifest_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    ordered_source_execution_set_sha256s: tuple[Sha256V1, ...]
    ordered_receipts: tuple[VnpyFacadeConformanceReceiptV2, ...]
    build_input_sha256: Sha256V1
    receipt_set_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        values = dict(values)
        receipts = tuple(
            sorted(
                values["ordered_receipts"],
                key=lambda item: (item.plugin_id, item.plugin_version, item.manifest_sha256),
            )
        )
        build_items = tuple(values.pop("build_items"))
        execution_sets = tuple(sorted(values["ordered_source_execution_set_sha256s"]))
        build_payload = {
            "plugin_catalog_sha256": values["plugin_catalog_sha256"],
            "facade_contract_sha256": values["facade_contract_sha256"],
            "dto_mapping_set_sha256": values["dto_mapping_set_sha256"],
            "state_mapping_set_sha256": values["state_mapping_set_sha256"],
            "terminal_mapping_set_sha256": values["terminal_mapping_set_sha256"],
            "isolated_module_binding_set_sha256": values["isolated_module_binding_set_sha256"],
            "facade_source_manifest_sha256": values["facade_source_manifest_sha256"],
            "source_executor_binding_sha256": values["source_executor_binding_sha256"],
            "ordered_source_execution_set_sha256s": execution_sets,
            "ordered_build_items": [item.canonical_payload_v1() for item in build_items],
        }
        build_hash = hash_hex_v1("miniqmt_vnpy_facade_conformance_build_input_v2", build_payload)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_set_v2",
            **values,
            "ordered_source_execution_set_sha256s": execution_sets,
            "ordered_receipts": [item.canonical_payload_v1() for item in receipts],
            "build_input_sha256": build_hash,
        }
        return cls(
            **{**payload, "ordered_receipts": receipts},
            receipt_set_sha256=hash_hex_v1("miniqmt_vnpy_facade_conformance_set_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_conformance_set_v2(self) -> Self:
        ordered = tuple(
            sorted(
                self.ordered_receipts,
                key=lambda item: (item.plugin_id, item.plugin_version, item.manifest_sha256),
            )
        )
        keys = tuple((item.plugin_id, item.plugin_version, item.manifest_sha256) for item in ordered)
        if self.ordered_receipts != ordered or len(keys) != len(set(keys)):
            raise ValueError("conformance receipts V2 must be unique and sorted")
        if not ordered or any(item.status is not VnpyFacadeCompatibilityStatusV1.PASSED for item in ordered):
            raise ValueError("published conformance set V2 requires complete PASSED receipts")
        execution_sets = self.ordered_source_execution_set_sha256s
        if execution_sets != tuple(sorted(execution_sets)) or len(execution_sets) != len(set(execution_sets)):
            raise ValueError("source execution set hashes must be unique and sorted")
        if {item.source_execution_set_sha256 for item in ordered} != set(execution_sets):
            raise ValueError("conformance set V2 source execution identities are incomplete")
        if any(item.source_executor_binding_sha256 != self.source_executor_binding_sha256 for item in ordered):
            raise ValueError("conformance set V2 source executor binding drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_set_v2",
            self.canonical_payload_v1(exclude={"receipt_set_sha256"}),
        )
        if self.receipt_set_sha256 != expected:
            raise ValueError("conformance receipt set V2 hash mismatch")
        return self


class VnpyFacadeConformanceAuthorityValidationReceiptV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2"] = (
        "miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2"
    )
    conformance_set_v2_sha256: Sha256V1
    source_executor_binding_sha256: Sha256V1
    ordered_source_execution_set_sha256s: tuple[Sha256V1, ...]
    validation_input_sha256: Sha256V1
    status: VnpyFacadeCompatibilityStatusV1
    ordered_failures: tuple[VnpyFacadeConformanceFailureV1, ...]
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        execution_sets = tuple(sorted(values["ordered_source_execution_set_sha256s"]))
        if len(execution_sets) != len(set(execution_sets)):
            raise ValueError("authority validation execution set identities must be unique")
        failures = bound_vnpy_facade_failures_v1(tuple(values["ordered_failures"]))
        payload = {
            "schema_version": "miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2",
            **values,
            "ordered_source_execution_set_sha256s": execution_sets,
            "status": VnpyFacadeCompatibilityStatusV1(values["status"]),
            "ordered_failures": failures,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1(
                "miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2",
                payload,
            ),
        )

    @model_validator(mode="after")
    def _validate_authority_validation_receipt(self) -> Self:
        _validate_bounded_failure_set_v1(self.ordered_failures)
        if self.status is VnpyFacadeCompatibilityStatusV1.PASSED:
            if self.ordered_failures or not self.ordered_source_execution_set_sha256s:
                raise ValueError("PASSED conformance authority validation requires complete execution sets")
        elif not self.ordered_failures:
            raise ValueError("FAILED conformance authority validation requires failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_authority_validation_receipt_v2",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("conformance authority validation receipt hash mismatch")
        return self


_CONFORMANCE_AUTHORITY_TOKEN_V2 = object()


class VnpyFacadeConformanceAuthorityV2:
    """Process-local sealed result of full V2 writer/readback authority validation."""

    __slots__ = (
        "_algorithm_bindings",
        "_characterization_receipts",
        "_conformance_set",
        "_sealed",
        "_source_execution_sets",
        "_source_executor_binding",
        "_validation_receipt",
    )

    def __init__(
        self,
        *,
        token: object,
        conformance_set: VnpyFacadeConformanceSetV2,
        source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
        source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
        characterization_receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
        algorithm_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...],
        validation_receipt: VnpyFacadeConformanceAuthorityValidationReceiptV2,
    ) -> None:
        if token is not _CONFORMANCE_AUTHORITY_TOKEN_V2:
            raise TypeError("VnpyFacadeConformanceAuthorityV2 can only be created by the V2 authority validator")
        self._conformance_set = conformance_set
        self._source_executor_binding = source_executor_binding
        self._source_execution_sets = source_execution_sets
        self._characterization_receipts = characterization_receipts
        self._algorithm_bindings = algorithm_bindings
        self._validation_receipt = validation_receipt
        self._sealed = True

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("VnpyFacadeConformanceAuthorityV2 is immutable")
        object.__setattr__(self, name, value)

    @property
    def conformance_set(self) -> VnpyFacadeConformanceSetV2:
        return self._conformance_set

    @property
    def source_executor_binding(self) -> VnpyFacadeSourceExecutorBindingV1:
        return self._source_executor_binding

    @property
    def source_execution_sets(self) -> tuple[VnpyFacadeSourceExecutionSetV1, ...]:
        return self._source_execution_sets

    @property
    def characterization_receipts(self) -> tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...]:
        return self._characterization_receipts

    @property
    def algorithm_bindings(self) -> tuple[VnpyFacadeAlgorithmBindingV2, ...]:
        return self._algorithm_bindings

    @property
    def validation_receipt(self) -> VnpyFacadeConformanceAuthorityValidationReceiptV2:
        return self._validation_receipt

    def receipt_for_plugin_v2(self, plugin_key: PluginKeyV1) -> VnpyFacadeConformanceReceiptV2:
        matches = tuple(
            item
            for item in self._conformance_set.ordered_receipts
            if (item.plugin_id, item.plugin_version, item.manifest_sha256) == plugin_key.sort_key_v1()
        )
        if len(matches) != 1:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
                "sealed authority does not contain one exact plugin receipt",
                context={"plugin_key": plugin_key.canonical_payload_v1(), "match_count": len(matches)},
            )
        return matches[0]

    def binding_for_algo_v2(self, algo_code: str) -> VnpyFacadeAlgorithmBindingV2:
        matches = tuple(item for item in self._algorithm_bindings if item.algo_code == algo_code)
        if len(matches) != 1:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
                "sealed authority does not contain one exact algorithm binding",
                context={"algo_code": algo_code, "match_count": len(matches)},
            )
        return matches[0]


def _seal_vnpy_facade_conformance_authority_v2(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
    characterization_receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
    algorithm_bindings: tuple[VnpyFacadeAlgorithmBindingV2, ...],
    validation_receipt: VnpyFacadeConformanceAuthorityValidationReceiptV2,
) -> VnpyFacadeConformanceAuthorityV2:
    return VnpyFacadeConformanceAuthorityV2(
        token=_CONFORMANCE_AUTHORITY_TOKEN_V2,
        conformance_set=conformance_set,
        source_executor_binding=source_executor_binding,
        source_execution_sets=source_execution_sets,
        characterization_receipts=characterization_receipts,
        algorithm_bindings=algorithm_bindings,
        validation_receipt=validation_receipt,
    )


class VnpyFacadeRepositoryReadKindV1(StrEnum):
    ALGO_START = "ALGO_START"
    LATEST_PRIOR_TICK = "LATEST_PRIOR_TICK"


class VnpyFacadeRepositoryReadRequestV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_repository_read_request_v1"] = (
        "miniqmt_vnpy_facade_repository_read_request_v1"
    )
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    current_event_id: IdentityV1
    current_event_sequence: PositiveIntV1
    current_delivery_id: IdentityV1
    current_delivery_sequence: PositiveIntV1
    exchange_trade_date: IdentityV1
    session_epoch: IdentityV1
    session_phase: SessionPhaseV1
    request_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        algo_instance_id: str,
        current_event_id: str,
        current_event_sequence: int,
        current_delivery_id: str,
        current_delivery_sequence: int,
        exchange_trade_date: str,
        session_epoch: str,
        session_phase: SessionPhaseV1,
    ) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_repository_read_request_v1",
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "current_event_id": current_event_id,
            "current_event_sequence": current_event_sequence,
            "current_delivery_id": current_delivery_id,
            "current_delivery_sequence": current_delivery_sequence,
            "exchange_trade_date": exchange_trade_date,
            "session_epoch": session_epoch,
            "session_phase": session_phase,
        }
        return cls(
            **payload,
            request_sha256=hash_hex_v1("miniqmt_vnpy_facade_repository_read_request_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_repository_request(self) -> Self:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.exchange_trade_date) is None:
            raise ValueError("facade repository exchange_trade_date must be YYYY-MM-DD")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_repository_read_request_v1",
            self.canonical_payload_v1(exclude={"request_sha256"}),
        )
        if self.request_sha256 != expected:
            raise ValueError("facade repository read request hash mismatch")
        return self


class VnpyFacadeRepositoryEventReadV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_repository_event_read_v1"] = (
        "miniqmt_vnpy_facade_repository_event_read_v1"
    )
    read_kind: VnpyFacadeRepositoryReadKindV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    cutoff_delivery_sequence_or_null: PositiveIntV1 | None
    cutoff_event_sequence_or_null: PositiveIntV1 | None
    event: RuntimeEventEnvelopeV2
    delivery: AlgoDeliveryPersistenceV1
    read_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_repository_event_read_v1",
            **values,
        }
        return cls(
            **payload,
            read_sha256=hash_hex_v1("miniqmt_vnpy_facade_repository_event_read_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_repository_event_read(self) -> Self:
        if (
            self.event.runtime_id != self.runtime_id
            or self.delivery.runtime_id != self.runtime_id
            or self.delivery.algo_instance_id != self.algo_instance_id
            or self.delivery.event_id != self.event.event_id
            or self.delivery.status is not DeliveryStatusV1.APPLIED
        ):
            raise ValueError("facade repository event/delivery owner or applied closure drifted")
        if self.read_kind is VnpyFacadeRepositoryReadKindV1.ALGO_START:
            if self.cutoff_delivery_sequence_or_null is not None or self.cutoff_event_sequence_or_null is not None:
                raise ValueError("ALGO_START read cannot carry a cutoff")
            if (
                self.delivery.algo_delivery_sequence != 1
                or self.event.event_type is not EventTypeV2.ALGO_START
                or self.event.source is not EventSourceV2.MINIQMT_EXECUTION_KERNEL
                or self.event.payload_schema_version != "miniqmt_algo_start_v1"
            ):
                raise ValueError("facade ALGO_START repository read is not the exact first applied fact")
        else:
            if self.cutoff_delivery_sequence_or_null is None or self.cutoff_event_sequence_or_null is None:
                raise ValueError("LATEST_PRIOR_TICK read requires both immutable cutoffs")
            if (
                self.delivery.algo_delivery_sequence >= self.cutoff_delivery_sequence_or_null
                or self.event.sequence >= self.cutoff_event_sequence_or_null
                or self.event.event_type is not EventTypeV2.TICK
                or self.event.source is not EventSourceV2.B0_QUOTE_V2
                or self.event.payload_schema_version != "miniqmt_market_data_view_v2"
            ):
                raise ValueError("facade prior TICK read violates cutoff or B0 authority")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_repository_event_read_v1",
            self.canonical_payload_v1(exclude={"read_sha256"}),
        )
        if self.read_sha256 != expected:
            raise ValueError("facade repository event read hash mismatch")
        return self


class VnpyFacadeRepositoryReadSetV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_repository_read_set_v1"] = "miniqmt_vnpy_facade_repository_read_set_v1"
    request_sha256: Sha256V1
    algo_start_read: VnpyFacadeRepositoryEventReadV1
    latest_prior_tick_read_or_null: VnpyFacadeRepositoryEventReadV1 | None
    read_set_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        request: VnpyFacadeRepositoryReadRequestV1,
        algo_start_read: VnpyFacadeRepositoryEventReadV1,
        latest_prior_tick_read_or_null: VnpyFacadeRepositoryEventReadV1 | None,
    ) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_repository_read_set_v1",
            "request_sha256": request.request_sha256,
            "algo_start_read": algo_start_read,
            "latest_prior_tick_read_or_null": latest_prior_tick_read_or_null,
        }
        result = cls(
            **payload,
            read_set_sha256=hash_hex_v1("miniqmt_vnpy_facade_repository_read_set_v1", payload),
        )
        result.validate_against_request_v1(request)
        return result

    def validate_against_request_v1(self, request: VnpyFacadeRepositoryReadRequestV1) -> None:
        if not isinstance(request, VnpyFacadeRepositoryReadRequestV1):
            raise TypeError("request must be VnpyFacadeRepositoryReadRequestV1")
        if (
            self.request_sha256 != request.request_sha256
            or self.algo_start_read.runtime_id != request.runtime_id
            or self.algo_start_read.algo_instance_id != request.algo_instance_id
        ):
            raise ValueError("facade repository read set owner/request closure drifted")
        tick = self.latest_prior_tick_read_or_null
        if tick is not None:
            if (
                tick.read_kind is not VnpyFacadeRepositoryReadKindV1.LATEST_PRIOR_TICK
                or tick.runtime_id != request.runtime_id
                or tick.algo_instance_id != request.algo_instance_id
                or tick.cutoff_delivery_sequence_or_null != request.current_delivery_sequence
                or tick.cutoff_event_sequence_or_null != request.current_event_sequence
            ):
                raise ValueError("facade repository prior TICK read does not close to its request")
            correlation = thaw_json_v1(tick.event.correlation)
            if correlation != {
                "exchange_trade_date": request.exchange_trade_date,
                "session_epoch": request.session_epoch,
                "session_phase": request.session_phase.value,
            }:
                raise ValueError("facade repository prior TICK session authority drifted")

    @model_validator(mode="after")
    def _validate_repository_read_set(self) -> Self:
        if self.algo_start_read.read_kind is not VnpyFacadeRepositoryReadKindV1.ALGO_START:
            raise ValueError("facade repository read set requires the exact ALGO_START read")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_repository_read_set_v1",
            self.canonical_payload_v1(exclude={"read_set_sha256"}),
        )
        if self.read_set_sha256 != expected:
            raise ValueError("facade repository read set hash mismatch")
        return self


class VnpyFacadeAuthorityInputV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_authority_input_v2"] = "miniqmt_vnpy_facade_authority_input_v2"
    plugin_catalog_snapshot: PluginCatalogSnapshotV1
    gateway_capability_catalog: GatewayCapabilityCatalogV1
    plugin_key: PluginKeyV1
    manifest: ExecutionAlgoPluginManifestV2
    pinned_compatibility_receipt: VnpyCompatibilityReceiptV2
    route_compatibility_receipt: PluginRouteCompatibilityReceiptV1
    facade_conformance_receipt_v2: VnpyFacadeConformanceReceiptV2
    facade_conformance_set_v2: VnpyFacadeConformanceSetV2
    conformance_authority_validation_receipt_v2: VnpyFacadeConformanceAuthorityValidationReceiptV2
    source_executor_binding_sha256: Sha256V1
    source_execution_set_sha256: Sha256V1
    authority_input_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        conformance_authority: VnpyFacadeConformanceAuthorityV2,
        plugin_catalog_snapshot: PluginCatalogSnapshotV1,
        gateway_capability_catalog: GatewayCapabilityCatalogV1,
        plugin_key: PluginKeyV1,
        manifest: ExecutionAlgoPluginManifestV2,
        pinned_compatibility_receipt: VnpyCompatibilityReceiptV2,
        route_compatibility_receipt: PluginRouteCompatibilityReceiptV1,
    ) -> Self:
        if not isinstance(conformance_authority, VnpyFacadeConformanceAuthorityV2):
            raise TypeError("conformance_authority must be a sealed VnpyFacadeConformanceAuthorityV2")
        receipt = conformance_authority.receipt_for_plugin_v2(plugin_key)
        payload = {
            "schema_version": "miniqmt_vnpy_facade_authority_input_v2",
            "plugin_catalog_snapshot": plugin_catalog_snapshot,
            "gateway_capability_catalog": gateway_capability_catalog,
            "plugin_key": plugin_key,
            "manifest": manifest,
            "pinned_compatibility_receipt": pinned_compatibility_receipt,
            "route_compatibility_receipt": route_compatibility_receipt,
            "facade_conformance_receipt_v2": receipt,
            "facade_conformance_set_v2": conformance_authority.conformance_set,
            "conformance_authority_validation_receipt_v2": conformance_authority.validation_receipt,
            "source_executor_binding_sha256": conformance_authority.source_executor_binding.binding_sha256,
            "source_execution_set_sha256": receipt.source_execution_set_sha256,
        }
        return cls(
            **payload,
            authority_input_sha256=hash_hex_v1("miniqmt_vnpy_facade_authority_input_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_authority_input_v2(self) -> Self:
        descriptors = tuple(
            item for item in self.plugin_catalog_snapshot.registration_descriptors if item.plugin_key == self.plugin_key
        )
        if len(descriptors) != 1 or descriptors[0].manifest != self.manifest:
            raise ValueError("facade authority V2 requires one exact catalog descriptor/manifest")
        compatibility = tuple(
            item
            for item in self.plugin_catalog_snapshot.pinned_compatibility_receipts
            if item.plugin_key == self.plugin_key
        )
        if compatibility != (self.pinned_compatibility_receipt,):
            raise ValueError("facade authority V2 pinned compatibility receipt conflicts with catalog")
        self.route_compatibility_receipt.validate_against_authority_v1(
            catalog_snapshot=self.plugin_catalog_snapshot,
            gateway_catalog=self.gateway_capability_catalog,
        )
        if self.route_compatibility_receipt.status is not CompatibilityStatusV1.PASSED:
            raise ValueError("facade authority V2 requires a PASSED route receipt")
        receipt = self.facade_conformance_receipt_v2
        matches = tuple(
            item
            for item in self.facade_conformance_set_v2.ordered_receipts
            if (item.plugin_id, item.plugin_version, item.manifest_sha256) == self.plugin_key.sort_key_v1()
        )
        if matches != (receipt,) or receipt.status is not VnpyFacadeCompatibilityStatusV1.PASSED:
            raise ValueError("facade authority V2 requires one exact PASSED conformance receipt")
        if (
            self.facade_conformance_set_v2.plugin_catalog_sha256 != self.plugin_catalog_snapshot.catalog_sha256
            or self.source_executor_binding_sha256 != self.facade_conformance_set_v2.source_executor_binding_sha256
            or self.source_executor_binding_sha256 != receipt.source_executor_binding_sha256
            or self.source_execution_set_sha256 != receipt.source_execution_set_sha256
            or self.conformance_authority_validation_receipt_v2.status is not VnpyFacadeCompatibilityStatusV1.PASSED
            or self.conformance_authority_validation_receipt_v2.conformance_set_v2_sha256
            != self.facade_conformance_set_v2.receipt_set_sha256
        ):
            raise ValueError("facade authority V2 component closure drifted")
        k1 = self.pinned_compatibility_receipt
        if (
            receipt.pinned_compatibility_receipt_sha256 != k1.receipt_sha256
            or receipt.requirement_sha256 != k1.requirement_sha256
            or receipt.surface_sha256 != k1.surface_sha256
            or receipt.source_lock_sha256 != k1.source_lock_sha256
            or receipt.method_signature_sha256 != k1.method_signature_sha256
            or receipt.object_field_sha256 != k1.object_field_sha256
            or receipt.characterization_sha256 != k1.characterization_sha256
            or receipt.manifest_sha256 != self.manifest.manifest_sha256
            or receipt.algo_code != self.manifest.algo_code
        ):
            raise ValueError("facade authority V2 K1/plugin component closure drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_authority_input_v2",
            self.canonical_payload_v1(exclude={"authority_input_sha256"}),
        )
        if self.authority_input_sha256 != expected:
            raise ValueError("facade authority input V2 hash mismatch")
        return self


class VnpyFacadeInitializationInputV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_initialization_input_v2"] = (
        "miniqmt_vnpy_facade_initialization_input_v2"
    )
    start_event: RuntimeEventEnvelopeV2
    start_delivery: AlgoEventDeliveryV1
    start_context: AlgoStartContextV1
    authority_input: VnpyFacadeAuthorityInputV2
    transition_id: IdentityV1
    transition_sequence: Literal[1]
    input_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        expected_transition = algo_transition_id_v1(
            delivery_id=values["start_delivery"].delivery_id,
            event_id=values["start_event"].event_id,
            runtime_id=values["start_context"].runtime_id,
            algo_instance_id=values["start_context"].algo_instance_id,
            transition_sequence=1,
        )
        if values.get("transition_id", expected_transition) != expected_transition:
            raise ValueError("facade initialization transition identity conflicts with K2 authority")
        payload = {
            "schema_version": "miniqmt_vnpy_facade_initialization_input_v2",
            **{key: value for key, value in values.items() if key != "transition_id"},
            "transition_id": expected_transition,
            "transition_sequence": 1,
        }
        return cls(
            **payload,
            input_sha256=hash_hex_v1("miniqmt_vnpy_facade_initialization_input_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_initialization_input_v2(self) -> Self:
        if self.start_event.event_type is not EventTypeV2.ALGO_START:
            raise ValueError("facade initialization V2 requires exact ALGO_START event")
        context = self.start_context
        if (
            self.start_event.event_id != context.start_event_id
            or self.start_delivery.event_id != self.start_event.event_id
            or self.start_delivery.delivery_id != context.start_delivery_id
            or self.start_delivery.runtime_id != context.runtime_id
            or self.start_delivery.algo_instance_id != context.algo_instance_id
            or context.plugin_manifest != self.authority_input.manifest
            or context.deterministic_context.transition_sequence != 1
        ):
            raise ValueError("facade initialization V2 owner/event/delivery context is not closed")
        expected_transition = algo_transition_id_v1(
            delivery_id=self.start_delivery.delivery_id,
            event_id=self.start_event.event_id,
            runtime_id=context.runtime_id,
            algo_instance_id=context.algo_instance_id,
            transition_sequence=1,
        )
        if self.transition_id != expected_transition:
            raise ValueError("facade initialization V2 transition identity drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_initialization_input_v2",
            self.canonical_payload_v1(exclude={"input_sha256"}),
        )
        if self.input_sha256 != expected:
            raise ValueError("facade initialization input V2 hash mismatch")
        return self


class VnpyFacadeTransitionInputV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_transition_input_v2"] = "miniqmt_vnpy_facade_transition_input_v2"
    runtime_event: RuntimeEventEnvelopeV2
    claimed_delivery: AlgoDeliveryPersistenceV1
    algo_instance: ExecutionAlgoInstancePersistenceV2
    manifest: ExecutionAlgoPluginManifestV2
    authority_input: VnpyFacadeAuthorityInputV2
    before_state: AlgoStateSnapshotV2
    read_only_services: AlgoReadOnlyServicesV1
    command_lifecycle_projection: KernelCommandLifecycleProjectionV1
    ordered_active_mappings: tuple[ExecutionCommandChildMappingV1, ...]
    deterministic_context: DeterministicExecutionContextV1
    transition_id: IdentityV1
    transition_sequence: PositiveIntV1
    input_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        mappings = tuple(sorted(values["ordered_active_mappings"], key=lambda item: item.local_vt_orderid))
        sequence = values["transition_sequence"]
        expected_transition = algo_transition_id_v1(
            delivery_id=values["claimed_delivery"].delivery_id,
            event_id=values["runtime_event"].event_id,
            runtime_id=values["runtime_event"].runtime_id,
            algo_instance_id=values["claimed_delivery"].algo_instance_id,
            transition_sequence=sequence,
        )
        if values.get("transition_id", expected_transition) != expected_transition:
            raise ValueError("facade transition identity conflicts with K2 authority")
        payload = {
            "schema_version": "miniqmt_vnpy_facade_transition_input_v2",
            **{key: value for key, value in values.items() if key not in {"ordered_active_mappings", "transition_id"}},
            "ordered_active_mappings": mappings,
            "transition_id": expected_transition,
        }
        return cls(
            **payload,
            input_sha256=hash_hex_v1("miniqmt_vnpy_facade_transition_input_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_transition_input_v2(self) -> Self:
        delivery = self.claimed_delivery
        if (
            delivery.status is not DeliveryStatusV1.CLAIMED
            or delivery.transition_id is not None
            or delivery.failure_receipt_id is not None
            or delivery.skip_receipt_id is not None
            or delivery.closed_at_utc is not None
            or delivery.lease_owner is None
            or delivery.lease_fence_token is None
            or delivery.lease_epoch <= 0
        ):
            raise ValueError("facade transition V2 requires an exact unfinished CLAIMED delivery")
        mappings = tuple(sorted(self.ordered_active_mappings, key=lambda item: item.local_vt_orderid))
        local_ids = tuple(item.local_vt_orderid for item in mappings)
        lifecycle_ids = tuple(item.local_vt_orderid for item in self.command_lifecycle_projection.ordered_items)
        if self.ordered_active_mappings != mappings or len(local_ids) != len(set(local_ids)):
            raise ValueError("active mappings must be unique and sorted")
        if local_ids != lifecycle_ids:
            raise ValueError("active mappings conflict with command lifecycle projection")
        context = self.deterministic_context
        owners = (
            self.runtime_event.runtime_id,
            delivery.runtime_id,
            self.algo_instance.runtime_id,
            self.read_only_services.runtime_id,
            self.command_lifecycle_projection.runtime_id,
            context.runtime_id,
        )
        algos = (
            delivery.algo_instance_id,
            self.algo_instance.algo_instance_id,
            self.before_state.algo_instance_id,
            self.read_only_services.algo_instance_id,
            self.command_lifecycle_projection.algo_instance_id,
            context.algo_instance_id,
        )
        if len(set(owners)) != 1 or len(set(algos)) != 1:
            raise ValueError("facade transition V2 runtime/algo owner drift")
        if (
            self.runtime_event.event_id != context.event_id
            or delivery.event_id != context.event_id
            or delivery.delivery_id != context.delivery_id
            or delivery.algo_delivery_sequence != self.transition_sequence
            or delivery.previous_delivery_id != self.before_state.last_applied_delivery_id
            or self.manifest != self.authority_input.manifest
            or self.before_state.plugin_manifest_sha256 != self.manifest.manifest_sha256
            or self.transition_sequence != context.transition_sequence
        ):
            raise ValueError("facade transition V2 event/delivery/manifest sequence closure drifted")
        previous_sequence = self.transition_sequence - 1
        if (
            self.before_state.transition_sequence != previous_sequence
            or self.before_state.last_applied_delivery_sequence != previous_sequence
            or self.before_state.last_closed_delivery_sequence != previous_sequence
            or self.algo_instance.transition_sequence != previous_sequence
            or self.algo_instance.last_applied_delivery_sequence != previous_sequence
            or self.algo_instance.last_closed_delivery_sequence != previous_sequence
            or self.algo_instance.last_applied_delivery_id != self.before_state.last_applied_delivery_id
            or self.algo_instance.state_sha256 != self.before_state.state_sha256
        ):
            raise ValueError("facade transition V2 predecessor state closure drifted")
        if (
            self.command_lifecycle_projection.event_id != context.event_id
            or self.command_lifecycle_projection.delivery_id != context.delivery_id
            or self.read_only_services.event_id != context.event_id
            or self.read_only_services.delivery_id != context.delivery_id
            or self.read_only_services.execution_projection_set.projection_set_sha256 != context.input_projection_sha256
        ):
            raise ValueError("facade transition V2 projection event/delivery owner drifted")
        if any(
            (
                item.runtime_id,
                item.algo_instance_id,
                item.parent_intent_id,
                item.strategy_slot_id,
                item.symbol,
                item.side,
            )
            != (
                context.runtime_id,
                context.algo_instance_id,
                self.algo_instance.parent_intent_id,
                self.algo_instance.strategy_slot_id,
                self.algo_instance.symbol,
                self.algo_instance.side,
            )
            for item in mappings
        ):
            raise ValueError("active mapping owner conflicts with transition V2")
        for mapping, lifecycle in zip(mappings, self.command_lifecycle_projection.ordered_items, strict=True):
            if (
                lifecycle.mapping_id != mapping.mapping_id
                or lifecycle.mapping_version != mapping.mapping_version
                or lifecycle.mapping_payload_sha256 != mapping.payload_sha256
                or lifecycle.local_vt_orderid != mapping.local_vt_orderid
                or lifecycle.submit_command_id != mapping.command_id
                or lifecycle.broker_order_id != mapping.broker_order_id
                or lifecycle.mapping_status is not mapping.mapping_status
            ):
                raise ValueError("active mapping conflicts with command lifecycle facts")
        before = VnpyFacadeStateEnvelopeV1.model_validate_json(
            json.dumps(thaw_json_v1(self.before_state.state), sort_keys=True, separators=(",", ":")),
            strict=True,
        )
        active_by_local = {item.local_vt_orderid: item for item in before.ordered_active_orders}
        if tuple(sorted(active_by_local)) != local_ids:
            raise ValueError("facade before-state active orders conflict with durable mapping identities")
        for mapping in mappings:
            active = active_by_local[mapping.local_vt_orderid]
            immutable_drift = (
                active.command_id != mapping.command_id
                or active.child_order_id != mapping.child_order_id
                or active.symbol != mapping.symbol
                or active.side != mapping.side.value
                or active.price_decimal != mapping.requested_price_decimal
                or active.requested_quantity != mapping.requested_quantity
            )
            mutable_drift = active.broker_order_id != mapping.broker_order_id
            if immutable_drift or (
                self.runtime_event.event_type
                not in {
                    EventTypeV2.COMMAND_OUTCOME,
                    EventTypeV2.ORDER,
                    EventTypeV2.TRADE,
                    EventTypeV2.RECONCILE,
                }
                and mutable_drift
            ):
                raise ValueError("facade before-state active order conflicts with durable mapping facts")
        validate_vnpy_facade_transition_market_authority_v1(
            event=self.runtime_event,
            services=self.read_only_services,
            deterministic_context=self.deterministic_context,
        )
        expected_transition = algo_transition_id_v1(
            delivery_id=delivery.delivery_id,
            event_id=self.runtime_event.event_id,
            runtime_id=self.runtime_event.runtime_id,
            algo_instance_id=delivery.algo_instance_id,
            transition_sequence=self.transition_sequence,
        )
        if self.transition_id != expected_transition:
            raise ValueError("facade transition V2 transition identity drifted")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_transition_input_v2",
            self.canonical_payload_v1(exclude={"input_sha256"}),
        )
        if self.input_sha256 != expected:
            raise ValueError("facade transition input V2 hash mismatch")
        return self


__all__ = [
    "VnpyAlgoProjectionObservationV1",
    "VnpyFacadeActiveOrderV1",
    "VnpyFacadeAlgorithmBindingV1",
    "VnpyFacadeAlgorithmBindingV2",
    "VnpyFacadeAlgorithmCharacterizationReceiptV1",
    "VnpyFacadeAlgorithmCharacterizationReceiptV2",
    "VnpyFacadeCharacterizationRequirementV1",
    "VnpyFacadeCharacterizationManifestViewV1",
    "VnpyFacadeCharacterizationVectorArtifactV2",
    "VnpyFacadeCharacterizationStartContextV2",
    "VnpyFacadeCharacterizationVectorV1",
    "VnpyFacadeCharacterizationVectorV2",
    "VnpyFacadeAuthorityInputV1",
    "VnpyFacadeAuthorityInputV2",
    "VnpyFacadeCommandAuthorityDispositionV1",
    "VnpyFacadeCompatibilityStatusV1",
    "VnpyFacadeConformanceBuildItemV1",
    "VnpyFacadeConformanceBuildItemV2",
    "VnpyFacadeConformanceAuthorityV2",
    "VnpyFacadeConformanceAuthorityValidationReceiptV2",
    "VnpyFacadeConformanceFailureV1",
    "VnpyFacadeConformanceReceiptV1",
    "VnpyFacadeConformanceReceiptV2",
    "VnpyFacadeConformanceSetV1",
    "VnpyFacadeConformanceSetV2",
    "VnpyFacadeConstructorDispositionV1",
    "VnpyFacadeContractError",
    "VnpyFacadeContractViewV1",
    "VnpyFacadeContractV1",
    "VnpyFacadeDeterministicInputsV1",
    "VnpyFacadeDtoMappingV1",
    "VnpyFacadeFieldRoleV1",
    "VnpyFacadeImplementationBindingV1",
    "VnpyFacadeInitializationInputV1",
    "VnpyFacadeInitializationInputV2",
    "VnpyFacadeIsolatedBindingOwnerV1",
    "VnpyFacadeIsolatedModuleBindingV1",
    "VnpyFacadeMethodContractV1",
    "VnpyFacadeK3ExpectedTraceMaterialV1",
    "VnpyFacadeRegistrationDispositionV1",
    "VnpyFacadeRepositoryEventReadV1",
    "VnpyFacadeRepositoryReadKindV1",
    "VnpyFacadeRepositoryReadRequestV1",
    "VnpyFacadeRepositoryReadSetV1",
    "VnpyFacadeRuntimeBindingDispositionV1",
    "VnpyFacadeSourceManifestV1",
    "VnpyFacadeSourceStateEnvelopeV1",
    "VnpyFacadeSourceRoleV1",
    "VnpyFacadeSourceV1",
    "VnpyFacadeStateFieldMappingV1",
    "VnpyFacadeStateEnvelopeV1",
    "read_vnpy_facade_lifecycle_items_v1",
    "VnpyFacadeStateValueV1",
    "VnpyFacadeTerminalMappingV1",
    "VnpyFacadeTransitionInputV1",
    "VnpyFacadeTransitionInputV2",
    "VnpyFacadeExecutedVectorResultV1",
    "VnpyFacadeExpectedTraceAuthorityRefV1",
    "VnpyFacadeSourceExecutionSetV1",
    "VnpyFacadeSourceExecutorBindingV1",
    "VnpyFacadeTraceCallV1",
    "VnpyFacadeTraceEffectV1",
    "VnpyFacadeUniformDrawV1",
    "bound_vnpy_facade_failures_v1",
    "build_vnpy_facade_market_data_lineage_v1",
    "validate_vnpy_facade_market_data_lineage_v1",
    "validate_vnpy_facade_transition_market_authority_v1",
]
