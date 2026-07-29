"""Strict K4 vn.py façade contracts and immutable evidence carriers.

The module is a shadow-only dependency leaf.  It owns no repository, gateway,
broker, event loop, clock, random generator, or product-route decision.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import StrictBool, StrictInt, model_validator

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
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
    DeterministicExecutionContextV1,
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
    NonNegativeIntV1,
    PositiveIntV1,
    RuntimeEventEnvelopeV2,
    Sha256V1,
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
        return value.canonical_payload_v1()
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
    status: IdentityV1
    last_order_event_id: IdentityV1 | None
    last_trade_event_id: IdentityV1 | None
    active_order_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized = {
            **values,
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
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_active_order_v1",
            self.canonical_payload_v1(exclude={"active_order_sha256"}),
        )
        if self.active_order_sha256 != expected:
            raise ValueError("facade active order hash mismatch")
        return self


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
            if (
                active.broker_order_id != mapping.broker_order_id
                or active.command_id != mapping.command_id
                or active.child_order_id != mapping.child_order_id
                or active.symbol != mapping.symbol
                or active.side != mapping.side.value
                or active.price_decimal != mapping.requested_price_decimal
                or active.requested_quantity != mapping.requested_quantity
                or active.last_order_event_id != mapping.last_order_event_id
                or active.last_trade_event_id != mapping.last_trade_event_id
            ):
                raise ValueError("facade before-state active order conflicts with durable mapping facts")
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_transition_input_v1",
            self.canonical_payload_v1(exclude={"input_sha256"}),
        )
        if self.input_sha256 != expected:
            raise ValueError("facade transition input hash mismatch")
        return self


__all__ = [
    "VnpyAlgoProjectionObservationV1",
    "VnpyFacadeActiveOrderV1",
    "VnpyFacadeAlgorithmBindingV1",
    "VnpyFacadeAlgorithmCharacterizationReceiptV1",
    "VnpyFacadeCharacterizationRequirementV1",
    "VnpyFacadeCharacterizationVectorV1",
    "VnpyFacadeAuthorityInputV1",
    "VnpyFacadeCommandAuthorityDispositionV1",
    "VnpyFacadeCompatibilityStatusV1",
    "VnpyFacadeConformanceBuildItemV1",
    "VnpyFacadeConformanceFailureV1",
    "VnpyFacadeConformanceReceiptV1",
    "VnpyFacadeConformanceSetV1",
    "VnpyFacadeConstructorDispositionV1",
    "VnpyFacadeContractError",
    "VnpyFacadeContractViewV1",
    "VnpyFacadeContractV1",
    "VnpyFacadeDeterministicInputsV1",
    "VnpyFacadeDtoMappingV1",
    "VnpyFacadeFieldRoleV1",
    "VnpyFacadeImplementationBindingV1",
    "VnpyFacadeInitializationInputV1",
    "VnpyFacadeIsolatedBindingOwnerV1",
    "VnpyFacadeIsolatedModuleBindingV1",
    "VnpyFacadeMethodContractV1",
    "VnpyFacadeRegistrationDispositionV1",
    "VnpyFacadeRuntimeBindingDispositionV1",
    "VnpyFacadeSourceManifestV1",
    "VnpyFacadeSourceRoleV1",
    "VnpyFacadeSourceV1",
    "VnpyFacadeStateFieldMappingV1",
    "VnpyFacadeStateEnvelopeV1",
    "VnpyFacadeStateValueV1",
    "VnpyFacadeTerminalMappingV1",
    "VnpyFacadeTransitionInputV1",
    "VnpyFacadeUniformDrawV1",
    "bound_vnpy_facade_failures_v1",
]
