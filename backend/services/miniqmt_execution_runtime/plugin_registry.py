"""Immutable K1-B plugin catalog construction and route compatibility."""

from __future__ import annotations

import hashlib
import inspect
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Self

from pydantic import StrictBool, ValidationError, model_validator

from .plugin_canonical import (
    canonical_json_bytes_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from .plugin_contracts import (
    ExecutionAlgoPluginManifestV2,
    FrozenJsonFieldV1,
    FrozenStrictModel,
    GatewayCapabilityCatalogV1,
    IdentityV1,
    MiniQMTPluginReasonCode,
    Sha256V1,
    VnpyCompatibilityRequirementV1,
)

_MAX_BUILD_FAILURES = 256
_CALLABLE_REF_SEPARATOR = ":"
_MANIFEST_BEHAVIOR_KEYS = (
    "plugin_id",
    "algo_code",
    "plugin_version",
    "provider",
    "implementation_ref",
    "config_schema_version",
    "config_schema_sha256",
    "state_schema_version",
    "state_schema_sha256",
    "subscribed_event_types",
    "market_data_requirements",
    "required_facade_methods",
    "required_facade_object_fields",
    "supported_sides",
    "supported_order_types",
    "supported_broker_backends",
    "restart_policy",
    "source_attribution",
    "compatibility_requirement",
    "behavior_characterization_sha256",
)


class CatalogBuildStageV1(StrEnum):
    STRICT_PARSE = "STRICT_PARSE"
    SCHEMA_HASH = "SCHEMA_HASH"
    SOURCE_BEHAVIOR = "SOURCE_BEHAVIOR"
    PROCESS_BINDING = "PROCESS_BINDING"
    PINNED_COMPATIBILITY = "PINNED_COMPATIBILITY"
    REGISTRATION_CREATION = "REGISTRATION_CREATION"
    SNAPSHOT_FREEZE = "SNAPSHOT_FREEZE"


class CompatibilityStatusV1(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"


class PluginKeyV1(FrozenStrictModel):
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    manifest_sha256: Sha256V1

    def sort_key_v1(self) -> tuple[str, str, str]:
        return (self.plugin_id, self.plugin_version, self.manifest_sha256)


class PluginRegistrationDescriptorV2(FrozenStrictModel):
    schema_version: Literal["plugin_registration_descriptor_v2"]
    manifest: ExecutionAlgoPluginManifestV2
    factory_binding_id: IdentityV1
    factory_callable_ref: IdentityV1
    factory_signature_sha256: Sha256V1
    config_validator_binding_id: IdentityV1
    config_validator_callable_ref: IdentityV1
    config_validator_signature_sha256: Sha256V1
    state_codec_binding_id: IdentityV1
    state_codec_callable_ref: IdentityV1
    state_codec_signature_sha256: Sha256V1

    @property
    def plugin_key(self) -> PluginKeyV1:
        return PluginKeyV1(
            plugin_id=self.manifest.plugin_id,
            plugin_version=self.manifest.plugin_version,
            manifest_sha256=self.manifest.manifest_sha256,
        )

    @model_validator(mode="after")
    def _validate_descriptor(self) -> Self:
        if self.factory_callable_ref != self.manifest.implementation_ref:
            raise ValueError("factory_callable_ref must equal manifest implementation_ref")
        binding_ids = (
            self.factory_binding_id,
            self.config_validator_binding_id,
            self.state_codec_binding_id,
        )
        if len(set(binding_ids)) != len(binding_ids):
            raise ValueError("descriptor binding IDs must be unique")
        for field_name in (
            "factory_callable_ref",
            "config_validator_callable_ref",
            "state_codec_callable_ref",
        ):
            value = getattr(self, field_name)
            if _CALLABLE_REF_SEPARATOR not in value:
                raise ValueError(f"{field_name} must be a module:qualname reference")
        return self


class PluginCreationBindingV1(FrozenStrictModel):
    schema_version: Literal["plugin_creation_binding_v1"] = "plugin_creation_binding_v1"
    algo_code: IdentityV1
    plugin_key: PluginKeyV1


class VnpyCompatibilityFailureV1(FrozenStrictModel):
    field_path: IdentityV1
    reason_code: IdentityV1
    context: FrozenJsonFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(cls, *, field_path: str, reason_code: str, context: Any) -> Self:
        safe_context = json_safe_evidence_v1(context)
        return cls(
            field_path=field_path,
            reason_code=reason_code,
            context=safe_context,
            context_sha256=hash_hex_v1("miniqmt_vnpy_compatibility_failure_context_v1", safe_context),
        )

    def sort_key_v1(self) -> tuple[str, str, str]:
        return (self.field_path, self.reason_code, self.context_sha256)

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1("miniqmt_vnpy_compatibility_failure_context_v1", thaw_json_v1(self.context))
        if self.context_sha256 != expected:
            raise ValueError("compatibility failure context hash mismatch")
        return self


class VnpyCompatibilityReceiptV1(FrozenStrictModel):
    schema_version: Literal["vnpy_compatibility_receipt_v1"]
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    manifest_sha256: Sha256V1
    requirement_sha256: Sha256V1
    surface_sha256: Sha256V1
    source_lock_sha256: Sha256V1
    method_signature_sha256: Sha256V1
    object_field_sha256: Sha256V1
    characterization_sha256: Sha256V1
    status: CompatibilityStatusV1
    ordered_failures: tuple[VnpyCompatibilityFailureV1, ...]
    receipt_sha256: Sha256V1

    @property
    def plugin_key(self) -> PluginKeyV1:
        return PluginKeyV1(
            plugin_id=self.plugin_id,
            plugin_version=self.plugin_version,
            manifest_sha256=self.manifest_sha256,
        )

    @classmethod
    def create(
        cls,
        *,
        plugin_id: str,
        plugin_version: str,
        manifest_sha256: str,
        requirement_sha256: str,
        surface_sha256: str,
        source_lock_sha256: str,
        method_signature_sha256: str,
        object_field_sha256: str,
        characterization_sha256: str,
        status: CompatibilityStatusV1,
        ordered_failures: tuple[VnpyCompatibilityFailureV1, ...],
    ) -> Self:
        failures = tuple(sorted(ordered_failures, key=lambda item: item.sort_key_v1()))
        payload = {
            "schema_version": "vnpy_compatibility_receipt_v1",
            "plugin_id": plugin_id,
            "plugin_version": plugin_version,
            "manifest_sha256": manifest_sha256,
            "requirement_sha256": requirement_sha256,
            "surface_sha256": surface_sha256,
            "source_lock_sha256": source_lock_sha256,
            "method_signature_sha256": method_signature_sha256,
            "object_field_sha256": object_field_sha256,
            "characterization_sha256": characterization_sha256,
            "status": status.value,
            "ordered_failures": [item.canonical_payload_v1() for item in failures],
        }
        return cls(
            **{**payload, "status": status, "ordered_failures": failures},
            receipt_sha256=hash_hex_v1("miniqmt_vnpy_compatibility_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        ordered = tuple(sorted(self.ordered_failures, key=lambda item: item.sort_key_v1()))
        if self.ordered_failures != ordered:
            raise ValueError("compatibility receipt failures must be stable sorted")
        if self.status is CompatibilityStatusV1.PASSED and ordered:
            raise ValueError("PASSED compatibility receipt cannot contain failures")
        if self.status is CompatibilityStatusV1.FAILED and not ordered:
            raise ValueError("FAILED compatibility receipt must contain failures")
        expected = hash_hex_v1(
            "miniqmt_vnpy_compatibility_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("compatibility receipt hash mismatch")
        return self


class PluginCatalogBuildFailureV1(FrozenStrictModel):
    stage: CatalogBuildStageV1
    plugin_id: str
    plugin_version: str
    algo_code: str
    manifest_sha256: str
    field_path: IdentityV1
    reason_code: MiniQMTPluginReasonCode
    context: FrozenJsonFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        stage: CatalogBuildStageV1,
        descriptor: PluginRegistrationDescriptorV2 | None,
        field_path: str,
        reason_code: MiniQMTPluginReasonCode,
        context: Any,
    ) -> Self:
        safe = json_safe_evidence_v1(context)
        manifest = descriptor.manifest if descriptor is not None else None
        return cls(
            stage=stage,
            plugin_id=manifest.plugin_id if manifest is not None else "",
            plugin_version=manifest.plugin_version if manifest is not None else "",
            algo_code=manifest.algo_code if manifest is not None else "",
            manifest_sha256=manifest.manifest_sha256 if manifest is not None else "",
            field_path=field_path,
            reason_code=reason_code,
            context=safe,
            context_sha256=hash_hex_v1("miniqmt_plugin_catalog_failure_context_v1", safe),
        )

    def sort_key_v1(self) -> tuple[str, ...]:
        return (
            self.stage.value,
            self.plugin_id,
            self.plugin_version,
            self.algo_code,
            self.manifest_sha256,
            self.field_path,
            self.reason_code.value,
            self.context_sha256,
        )

    @model_validator(mode="after")
    def _validate_context_hash(self) -> Self:
        expected = hash_hex_v1("miniqmt_plugin_catalog_failure_context_v1", thaw_json_v1(self.context))
        if self.context_sha256 != expected:
            raise ValueError("catalog failure context hash mismatch")
        return self


class PluginCatalogBuildFailureReceiptV1(FrozenStrictModel):
    schema_version: Literal["plugin_catalog_build_failure_receipt_v1"]
    build_input_sha256: Sha256V1
    ordered_descriptor_keys: tuple[PluginKeyV1, ...]
    total_failure_count: int
    failures_truncated: StrictBool
    ordered_failures: tuple[PluginCatalogBuildFailureV1, ...]
    omitted_failure_set_sha256: Sha256V1 | None
    failure_set_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        build_input_sha256: str,
        descriptor_keys: tuple[PluginKeyV1, ...],
        failures: list[PluginCatalogBuildFailureV1],
    ) -> Self:
        ordered = sorted(failures, key=lambda item: item.sort_key_v1())
        total = len(ordered)
        omitted_hash: str | None = None
        if total > _MAX_BUILD_FAILURES:
            omitted = ordered[_MAX_BUILD_FAILURES - 1 :]
            omitted_hash = hash_hex_v1(
                "miniqmt_plugin_catalog_omitted_failures_v1",
                [list(item.sort_key_v1()) for item in omitted],
            )
            ordered = ordered[: _MAX_BUILD_FAILURES - 1]
            ordered.append(
                PluginCatalogBuildFailureV1.create(
                    stage=CatalogBuildStageV1.SNAPSHOT_FREEZE,
                    descriptor=None,
                    field_path="__failure_set__",
                    reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
                    context={"omitted_count": len(omitted), "omitted_failure_set_sha256": omitted_hash},
                )
            )
        failure_payload = {
            "total_failure_count": total,
            "failures_truncated": total > _MAX_BUILD_FAILURES,
            "ordered_failures": [item.canonical_payload_v1() for item in ordered],
            "omitted_failure_set_sha256": omitted_hash,
        }
        failure_set_sha256 = hash_hex_v1("miniqmt_plugin_catalog_failure_set_v1", failure_payload)
        payload = {
            "schema_version": "plugin_catalog_build_failure_receipt_v1",
            "build_input_sha256": build_input_sha256,
            "ordered_descriptor_keys": [
                item.canonical_payload_v1() for item in sorted(descriptor_keys, key=lambda x: x.sort_key_v1())
            ],
            **failure_payload,
            "failure_set_sha256": failure_set_sha256,
        }
        return cls(
            **{
                **payload,
                "ordered_descriptor_keys": tuple(sorted(descriptor_keys, key=lambda x: x.sort_key_v1())),
                "ordered_failures": tuple(ordered),
            },
            receipt_sha256=hash_hex_v1("miniqmt_plugin_catalog_build_failure_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        if self.ordered_descriptor_keys != tuple(
            sorted(self.ordered_descriptor_keys, key=lambda item: item.sort_key_v1())
        ):
            raise ValueError("ordered_descriptor_keys must be stable sorted")
        comparable_failures = self.ordered_failures[:-1] if self.failures_truncated else self.ordered_failures
        if comparable_failures != tuple(sorted(comparable_failures, key=lambda item: item.sort_key_v1())):
            raise ValueError("catalog build failures must be stable sorted")
        if self.total_failure_count < len(self.ordered_failures):
            raise ValueError("total_failure_count cannot be smaller than returned failures")
        if self.failures_truncated != (self.total_failure_count > _MAX_BUILD_FAILURES):
            raise ValueError("failures_truncated does not match total_failure_count")
        if self.failures_truncated:
            if len(self.ordered_failures) != _MAX_BUILD_FAILURES or self.omitted_failure_set_sha256 is None:
                raise ValueError("truncated failure receipt must carry 256 entries and omitted hash")
            if self.ordered_failures[-1].field_path != "__failure_set__":
                raise ValueError("truncated failure receipt is missing the bounded marker")
            markers = [item for item in self.ordered_failures if item.field_path == "__failure_set__"]
            marker_context = thaw_json_v1(markers[0].context) if len(markers) == 1 else None
            if (
                len(markers) != 1
                or markers[0].stage is not CatalogBuildStageV1.SNAPSHOT_FREEZE
                or markers[0].reason_code is not MiniQMTPluginReasonCode.REGISTRATION_CONFLICT
                or not isinstance(marker_context, dict)
                or marker_context.get("omitted_count") != self.total_failure_count - (_MAX_BUILD_FAILURES - 1)
                or marker_context.get("omitted_failure_set_sha256") != self.omitted_failure_set_sha256
            ):
                raise ValueError("truncated failure receipt marker does not close over omitted failures")
        else:
            if self.omitted_failure_set_sha256 is not None:
                raise ValueError("non-truncated failure receipt cannot carry omitted hash")
            if self.total_failure_count != len(self.ordered_failures):
                raise ValueError("total_failure_count must equal returned failures when not truncated")
        failure_payload = {
            "total_failure_count": self.total_failure_count,
            "failures_truncated": self.failures_truncated,
            "ordered_failures": [item.canonical_payload_v1() for item in self.ordered_failures],
            "omitted_failure_set_sha256": self.omitted_failure_set_sha256,
        }
        if self.failure_set_sha256 != hash_hex_v1("miniqmt_plugin_catalog_failure_set_v1", failure_payload):
            raise ValueError("failure_set_sha256 mismatch")
        expected_receipt = hash_hex_v1(
            "miniqmt_plugin_catalog_build_failure_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected_receipt:
            raise ValueError("catalog build failure receipt hash mismatch")
        return self


class PluginCatalogSnapshotV1(FrozenStrictModel):
    schema_version: Literal["plugin_catalog_snapshot_v1"]
    registration_descriptors: tuple[PluginRegistrationDescriptorV2, ...]
    pinned_compatibility_receipts: tuple[VnpyCompatibilityReceiptV1, ...]
    creation_bindings: tuple[PluginCreationBindingV1, ...]
    catalog_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        descriptors: tuple[PluginRegistrationDescriptorV2, ...],
        receipts: tuple[VnpyCompatibilityReceiptV1, ...],
        creation_bindings: tuple[PluginCreationBindingV1, ...],
    ) -> Self:
        descriptors = tuple(sorted(descriptors, key=_descriptor_sort_key))
        receipts = tuple(sorted(receipts, key=lambda item: item.plugin_key.sort_key_v1()))
        creation_bindings = tuple(
            sorted(creation_bindings, key=lambda item: (item.algo_code, item.plugin_key.sort_key_v1()))
        )
        payload = {
            "schema_version": "plugin_catalog_snapshot_v1",
            "registration_descriptors": [item.canonical_payload_v1() for item in descriptors],
            "pinned_compatibility_receipts": [item.canonical_payload_v1() for item in receipts],
            "creation_bindings": [item.canonical_payload_v1() for item in creation_bindings],
        }
        return cls(
            **{
                **payload,
                "registration_descriptors": descriptors,
                "pinned_compatibility_receipts": receipts,
                "creation_bindings": creation_bindings,
            },
            catalog_sha256=hash_hex_v1("miniqmt_plugin_catalog_snapshot_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_snapshot(self) -> Self:
        if not self.registration_descriptors:
            raise ValueError("plugin catalog snapshot must not be empty")
        canonical_descriptors = tuple(sorted(self.registration_descriptors, key=_descriptor_sort_key))
        canonical_receipts = tuple(
            sorted(self.pinned_compatibility_receipts, key=lambda item: item.plugin_key.sort_key_v1())
        )
        canonical_bindings = tuple(
            sorted(self.creation_bindings, key=lambda item: (item.algo_code, item.plugin_key.sort_key_v1()))
        )
        if self.registration_descriptors != canonical_descriptors:
            raise ValueError("registration_descriptors must use canonical sorted order")
        if self.pinned_compatibility_receipts != canonical_receipts:
            raise ValueError("pinned_compatibility_receipts must use canonical sorted order")
        if self.creation_bindings != canonical_bindings:
            raise ValueError("creation_bindings must use canonical sorted order")

        descriptor_keys = [item.plugin_key for item in self.registration_descriptors]
        version_keys = [
            (item.manifest.plugin_id, item.manifest.plugin_version) for item in self.registration_descriptors
        ]
        if len(descriptor_keys) != len(set(descriptor_keys)) or len(version_keys) != len(set(version_keys)):
            raise ValueError("registration descriptors must have unique plugin keys and versions")
        receipt_keys = [item.plugin_key for item in self.pinned_compatibility_receipts]
        if len(receipt_keys) != len(set(receipt_keys)) or set(receipt_keys) != set(descriptor_keys):
            raise ValueError("compatibility receipts must map one-to-one to registration descriptors")
        if any(item.status is not CompatibilityStatusV1.PASSED for item in self.pinned_compatibility_receipts):
            raise ValueError("published catalog cannot contain a failed compatibility receipt")

        descriptors_by_key = {item.plugin_key: item for item in self.registration_descriptors}
        creation_counts = Counter(item.algo_code for item in self.creation_bindings)
        descriptor_algo_codes = {item.manifest.algo_code for item in self.registration_descriptors}
        if set(creation_counts) != descriptor_algo_codes or any(count != 1 for count in creation_counts.values()):
            raise ValueError("creation bindings must map every algo code exactly once")
        for binding in self.creation_bindings:
            descriptor = descriptors_by_key.get(binding.plugin_key)
            if descriptor is None or descriptor.manifest.algo_code != binding.algo_code:
                raise ValueError("creation binding must reference the exact registered plugin key and algo code")

        for descriptor in self.registration_descriptors:
            try:
                PluginRegistrationDescriptorV2.model_validate(descriptor.model_dump(mode="python"), strict=True)
            except (ValidationError, TypeError, ValueError) as exc:
                raise ValueError("snapshot descriptor closure is invalid") from exc
        for receipt in self.pinned_compatibility_receipts:
            try:
                VnpyCompatibilityReceiptV1.model_validate(receipt.model_dump(mode="python"), strict=True)
            except (ValidationError, TypeError, ValueError) as exc:
                raise ValueError("snapshot compatibility receipt closure is invalid") from exc
        expected = hash_hex_v1(
            "miniqmt_plugin_catalog_snapshot_v1",
            self.canonical_payload_v1(exclude={"catalog_sha256"}),
        )
        if self.catalog_sha256 != expected:
            raise ValueError("catalog_sha256 does not match snapshot closure")
        return self


class PluginRouteCompatibilityFailureV1(FrozenStrictModel):
    kind: Literal["STATIC_UNSUPPORTED"]
    field_path: IdentityV1
    required: FrozenJsonFieldV1
    supported: FrozenJsonFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(cls, *, field_path: str, required: Any, supported: Any) -> Self:
        context = {"field_path": field_path, "required": required, "supported": supported}
        return cls(
            kind="STATIC_UNSUPPORTED",
            field_path=field_path,
            required=required,
            supported=supported,
            context_sha256=hash_hex_v1("miniqmt_plugin_route_failure_context_v1", context),
        )

    def sort_key_v1(self) -> tuple[str, str]:
        return (self.field_path, self.context_sha256)

    @model_validator(mode="after")
    def _validate_context_hash(self) -> Self:
        context = {
            "field_path": self.field_path,
            "required": thaw_json_v1(self.required),
            "supported": thaw_json_v1(self.supported),
        }
        if self.context_sha256 != hash_hex_v1("miniqmt_plugin_route_failure_context_v1", context):
            raise ValueError("route failure context hash mismatch")
        return self


class PluginRouteCompatibilityReceiptV1(FrozenStrictModel):
    schema_version: Literal["plugin_route_compatibility_receipt_v1"]
    plugin_key: PluginKeyV1
    algo_code: IdentityV1
    plugin_manifest_sha256: Sha256V1
    catalog_sha256: Sha256V1
    gateway_capability_catalog_sha256: Sha256V1
    required_order_types: tuple[str, ...]
    supported_order_types: tuple[str, ...]
    required_market_capabilities: FrozenJsonFieldV1
    supported_market_capabilities: tuple[str, ...]
    status: CompatibilityStatusV1
    ordered_failures: tuple[PluginRouteCompatibilityFailureV1, ...]
    broker_called: Literal[False]
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        descriptor: PluginRegistrationDescriptorV2,
        catalog_sha256: str,
        gateway_catalog: GatewayCapabilityCatalogV1,
        failures: list[PluginRouteCompatibilityFailureV1],
    ) -> Self:
        manifest = descriptor.manifest
        ordered = tuple(sorted(failures, key=lambda item: item.sort_key_v1()))
        payload = {
            "schema_version": "plugin_route_compatibility_receipt_v1",
            "plugin_key": descriptor.plugin_key.canonical_payload_v1(),
            "algo_code": manifest.algo_code,
            "plugin_manifest_sha256": manifest.manifest_sha256,
            "catalog_sha256": catalog_sha256,
            "gateway_capability_catalog_sha256": gateway_catalog.catalog_sha256,
            "required_order_types": [item.value for item in manifest.supported_order_types],
            "supported_order_types": [item.value for item in gateway_catalog.order_types],
            "required_market_capabilities": [item.canonical_payload_v1() for item in manifest.market_data_requirements],
            "supported_market_capabilities": [item.value for item in gateway_catalog.market_data_capabilities],
            "status": (CompatibilityStatusV1.FAILED if ordered else CompatibilityStatusV1.PASSED).value,
            "ordered_failures": [item.canonical_payload_v1() for item in ordered],
            "broker_called": False,
        }
        return cls(
            **{
                **payload,
                "required_order_types": tuple(payload["required_order_types"]),
                "supported_order_types": tuple(payload["supported_order_types"]),
                "supported_market_capabilities": tuple(payload["supported_market_capabilities"]),
                "status": CompatibilityStatusV1.FAILED if ordered else CompatibilityStatusV1.PASSED,
                "ordered_failures": ordered,
            },
            receipt_sha256=hash_hex_v1("miniqmt_plugin_route_compatibility_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        ordered = tuple(sorted(self.ordered_failures, key=lambda item: item.sort_key_v1()))
        if self.ordered_failures != ordered:
            raise ValueError("route failures must be stable sorted")
        expected_status = CompatibilityStatusV1.FAILED if ordered else CompatibilityStatusV1.PASSED
        if self.status is not expected_status:
            raise ValueError("route compatibility status does not match failures")
        if self.plugin_manifest_sha256 != self.plugin_key.manifest_sha256:
            raise ValueError("route receipt plugin manifest identity mismatch")
        expected = hash_hex_v1(
            "miniqmt_plugin_route_compatibility_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("route compatibility receipt hash mismatch")
        return self


class PluginProcessBindingsV2:
    """Sealed process-local callable map; it is never canonicalized."""

    def __init__(self, bindings: Mapping[str, Callable[..., Any]]) -> None:
        if not isinstance(bindings, Mapping):
            raise TypeError("bindings must be a mapping")
        copied: dict[str, Callable[..., Any]] = {}
        for binding_id, binding in bindings.items():
            if type(binding_id) is not str or not binding_id.strip():
                raise TypeError("binding IDs must be non-empty strict strings")
            copied[binding_id] = binding
        self._bindings = MappingProxyType(copied)

    @property
    def binding_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._bindings))

    def resolve(self, binding_id: str) -> Callable[..., Any] | None:
        return self._bindings.get(binding_id)

    def copy_bindings_v1(self) -> dict[str, Callable[..., Any]]:
        return dict(self._bindings)

    def without(self, binding_id: str) -> PluginProcessBindingsV2:
        copied = dict(self._bindings)
        copied.pop(binding_id, None)
        return PluginProcessBindingsV2(copied)


@dataclass(frozen=True)
class PluginCatalogRuntimeV2:
    snapshot: PluginCatalogSnapshotV1
    process_bindings: PluginProcessBindingsV2

    def _descriptors(self) -> dict[PluginKeyV1, PluginRegistrationDescriptorV2]:
        return {item.plugin_key: item for item in self.snapshot.registration_descriptors}

    def plugin_key_for_new_instance(self, algo_code: str) -> PluginKeyV1:
        for binding in self.snapshot.creation_bindings:
            if binding.algo_code == algo_code:
                return binding.plugin_key
        raise KeyError(algo_code)

    def descriptor_for_restore(self, plugin_key: PluginKeyV1) -> PluginRegistrationDescriptorV2:
        try:
            return self._descriptors()[plugin_key]
        except KeyError as exc:
            raise KeyError(plugin_key) from exc


class PluginCatalogBuildError(RuntimeError):
    def __init__(self, receipt: PluginCatalogBuildFailureReceiptV1) -> None:
        self.receipt = receipt
        self.partial_catalog = None
        super().__init__(f"plugin catalog build failed: {receipt.receipt_sha256}")


def callable_ref_v1(value: Callable[..., Any]) -> str:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if type(module) is not str or type(qualname) is not str or "<locals>" in qualname:
        raise TypeError("callable must expose a stable module and qualname")
    return f"{module}:{qualname}"


def _annotation_token(value: Any) -> str | None:
    if value is inspect.Signature.empty:
        return None
    if type(value) is str:
        return value
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if type(module) is str and type(qualname) is str:
        return f"{module}.{qualname}"
    rendered = str(value)
    if "0x" in rendered:
        raise TypeError("annotation has a process-specific representation")
    return rendered


def callable_signature_payload_v1(value: Callable[..., Any]) -> dict[str, Any]:
    signature = inspect.signature(value, eval_str=False)
    parameters = []
    for parameter in signature.parameters.values():
        if parameter.default is inspect.Signature.empty:
            default: Any = {"required": True}
        elif parameter.default is None or type(parameter.default) in (bool, int, str):
            default = {"required": False, "value": parameter.default}
        else:
            raise TypeError(f"unsupported callable default for {parameter.name}")
        parameters.append(
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "default": default,
                "annotation": _annotation_token(parameter.annotation),
            }
        )
    return {"parameters": parameters, "return_annotation": _annotation_token(signature.return_annotation)}


def callable_signature_sha256_v1(value: Callable[..., Any]) -> str:
    return hash_hex_v1("miniqmt_plugin_callable_signature_v1", callable_signature_payload_v1(value))


def compatibility_component_hashes_v1(requirement: VnpyCompatibilityRequirementV1) -> dict[str, str]:
    """Return the exact PASSED component hashes expected from the K1-C seam."""

    source_lock_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_source_lock_v1",
        [item.canonical_payload_v1() for item in requirement.source_files_and_hashes],
    )
    method_signature_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_method_signatures_v1",
        list(requirement.required_method_signatures),
    )
    object_field_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_object_fields_v1",
        {
            "required_object_fields": [item.canonical_payload_v1() for item in requirement.required_object_fields],
            "required_enum_values": [item.canonical_payload_v1() for item in requirement.required_enum_values],
        },
    )
    surface_payload = {
        "source_lock_sha256": source_lock_sha256,
        "method_signature_sha256": method_signature_sha256,
        "object_field_sha256": object_field_sha256,
        "characterization_sha256": requirement.characterization_sha256,
    }
    return {
        **surface_payload,
        "surface_sha256": hash_hex_v1("miniqmt_vnpy_compatibility_surface_v1", surface_payload),
    }


def _descriptor_sort_key(item: PluginRegistrationDescriptorV2) -> tuple[str, str, str, str]:
    manifest = item.manifest
    return (manifest.algo_code, manifest.plugin_id, manifest.plugin_version, manifest.manifest_sha256)


def _build_input_hash(
    descriptors: tuple[Any, ...],
    creation_bindings: tuple[Any, ...],
    process_bindings: PluginProcessBindingsV2,
    receipts: tuple[Any, ...],
) -> str:
    def _ordered_evidence(values: tuple[Any, ...], domain: str) -> list[Any]:
        items = [
            json_safe_evidence_v1(
                item.model_dump(mode="json", warnings=False) if isinstance(item, FrozenStrictModel) else item
            )
            for item in values
        ]
        return sorted(items, key=lambda item: (hash_hex_v1(domain, item), canonical_json_bytes_v1(item)))

    evidence = {
        "descriptors": _ordered_evidence(descriptors, "miniqmt_plugin_catalog_descriptor_input_v1"),
        "creation_bindings": _ordered_evidence(creation_bindings, "miniqmt_plugin_catalog_creation_binding_input_v1"),
        "process_binding_ids": list(process_bindings.binding_ids),
        "pinned_compatibility_receipts": _ordered_evidence(
            receipts, "miniqmt_plugin_catalog_compatibility_receipt_input_v1"
        ),
    }
    return hash_hex_v1("miniqmt_plugin_catalog_build_input_v1", evidence)


def _failure(
    failures: list[PluginCatalogBuildFailureV1],
    *,
    stage: CatalogBuildStageV1,
    descriptor: PluginRegistrationDescriptorV2 | None,
    field_path: str,
    reason_code: MiniQMTPluginReasonCode,
    context: Any,
) -> None:
    failures.append(
        PluginCatalogBuildFailureV1.create(
            stage=stage,
            descriptor=descriptor,
            field_path=field_path,
            reason_code=reason_code,
            context=context,
        )
    )


def _parse_models(
    values: tuple[Any, ...],
    model_type: type[FrozenStrictModel],
    failures: list[PluginCatalogBuildFailureV1],
    field_name: str,
) -> list[Any]:
    parsed = []
    for index, value in enumerate(values):
        try:
            candidate = model_type.model_validate(value, strict=True)
            if model_type is PluginRegistrationDescriptorV2 and not isinstance(
                candidate.manifest, ExecutionAlgoPluginManifestV2
            ):
                raise TypeError("descriptor manifest must be ExecutionAlgoPluginManifestV2")
            if model_type is PluginCreationBindingV1 and not isinstance(candidate.plugin_key, PluginKeyV1):
                raise TypeError("creation binding plugin_key must be PluginKeyV1")
            if model_type is VnpyCompatibilityReceiptV1 and (
                not isinstance(candidate.status, CompatibilityStatusV1)
                or type(candidate.ordered_failures) is not tuple
                or any(not isinstance(item, VnpyCompatibilityFailureV1) for item in candidate.ordered_failures)
            ):
                raise TypeError("compatibility receipt nested closure is invalid")
            parsed.append(candidate)
        except (ValidationError, TypeError, ValueError) as exc:
            _failure(
                failures,
                stage=CatalogBuildStageV1.STRICT_PARSE,
                descriptor=None,
                field_path=f"{field_name}[{index}]",
                reason_code=MiniQMTPluginReasonCode.MANIFEST_SCHEMA_INVALID,
                context=exc,
            )
    return parsed


def _source_path_and_hash(value: Callable[..., Any]) -> tuple[str, str]:
    source = inspect.getsourcefile(value)
    if source is None:
        raise ValueError("callable source file is unavailable")
    path = Path(source).resolve()
    root = Path(__file__).resolve().parents[3]
    relative = path.relative_to(root).as_posix()
    return relative, hashlib.sha256(path.read_bytes()).hexdigest()


def build_plugin_catalog_v2(
    descriptors: tuple[Any, ...],
    creation_bindings: tuple[Any, ...],
    process_bindings: PluginProcessBindingsV2,
    pinned_compatibility_receipts: tuple[Any, ...],
) -> PluginCatalogRuntimeV2:
    if (
        type(descriptors) is not tuple
        or type(creation_bindings) is not tuple
        or type(pinned_compatibility_receipts) is not tuple
    ):
        raise TypeError("catalog sequence inputs must be strict tuples")
    if not isinstance(process_bindings, PluginProcessBindingsV2):
        raise TypeError("process_bindings must be PluginProcessBindingsV2")
    failures: list[PluginCatalogBuildFailureV1] = []
    build_input_sha256 = _build_input_hash(
        descriptors, creation_bindings, process_bindings, pinned_compatibility_receipts
    )
    parsed_descriptors: list[PluginRegistrationDescriptorV2] = _parse_models(
        descriptors, PluginRegistrationDescriptorV2, failures, "descriptors"
    )
    parsed_creation: list[PluginCreationBindingV1] = _parse_models(
        creation_bindings, PluginCreationBindingV1, failures, "creation_bindings"
    )
    parsed_receipts: list[VnpyCompatibilityReceiptV1] = _parse_models(
        pinned_compatibility_receipts,
        VnpyCompatibilityReceiptV1,
        failures,
        "pinned_compatibility_receipts",
    )

    if not parsed_descriptors:
        _failure(
            failures,
            stage=CatalogBuildStageV1.REGISTRATION_CREATION,
            descriptor=None,
            field_path="descriptors",
            reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
            context={"condition": "empty_catalog_forbidden"},
        )

    # Preserve the design's stage boundary even when a deliberately corrupted
    # frozen model was constructed without validation for negative testing.
    for raw_descriptor in parsed_descriptors:
        manifest = raw_descriptor.manifest
        serialized = manifest.canonical_payload_v1()
        expected_config = hash_hex_v1("miniqmt_plugin_config_schema_v1", thaw_json_v1(manifest.config_schema))
        expected_state = hash_hex_v1("miniqmt_plugin_state_schema_v1", thaw_json_v1(manifest.state_schema))
        expected_manifest = hash_hex_v1(
            "execution_algo_plugin_manifest_v2",
            {key: value for key, value in serialized.items() if key != "manifest_sha256"},
        )
        if (
            manifest.config_schema_sha256 != expected_config
            or manifest.state_schema_sha256 != expected_state
            or manifest.manifest_sha256 != expected_manifest
        ):
            _failure(
                failures,
                stage=CatalogBuildStageV1.SCHEMA_HASH,
                descriptor=raw_descriptor,
                field_path="manifest",
                reason_code=MiniQMTPluginReasonCode.MANIFEST_HASH_CONFLICT,
                context={
                    "config_schema_sha256": manifest.config_schema_sha256,
                    "expected_config_schema_sha256": expected_config,
                    "state_schema_sha256": manifest.state_schema_sha256,
                    "expected_state_schema_sha256": expected_state,
                    "manifest_sha256": manifest.manifest_sha256,
                    "expected_manifest_sha256": expected_manifest,
                },
            )
        expected_behavior = hash_hex_v1(
            "miniqmt_plugin_behavior_contract_v2",
            {key: serialized[key] for key in _MANIFEST_BEHAVIOR_KEYS},
        )
        source_payload = manifest.source_attribution.canonical_payload_v1(exclude={"attribution_sha256"})
        expected_source = hash_hex_v1("miniqmt_source_attribution_v1", source_payload)
        requirement_payload = manifest.compatibility_requirement.canonical_payload_v1(exclude={"requirement_sha256"})
        expected_requirement = hash_hex_v1("miniqmt_vnpy_compatibility_requirement_v1", requirement_payload)
        if (
            manifest.behavior_contract_sha256 != expected_behavior
            or manifest.source_attribution.attribution_sha256 != expected_source
            or manifest.compatibility_requirement.requirement_sha256 != expected_requirement
        ):
            _failure(
                failures,
                stage=CatalogBuildStageV1.SOURCE_BEHAVIOR,
                descriptor=raw_descriptor,
                field_path="manifest.source_behavior",
                reason_code=MiniQMTPluginReasonCode.MANIFEST_HASH_CONFLICT,
                context={
                    "behavior_contract_sha256": manifest.behavior_contract_sha256,
                    "expected_behavior_contract_sha256": expected_behavior,
                    "source_attribution_sha256": manifest.source_attribution.attribution_sha256,
                    "expected_source_attribution_sha256": expected_source,
                    "requirement_sha256": manifest.compatibility_requirement.requirement_sha256,
                    "expected_requirement_sha256": expected_requirement,
                },
            )

    for descriptor in parsed_descriptors:
        binding_ids = (
            descriptor.factory_binding_id,
            descriptor.config_validator_binding_id,
            descriptor.state_codec_binding_id,
        )
        callable_refs = (
            descriptor.factory_callable_ref,
            descriptor.config_validator_callable_ref,
            descriptor.state_codec_callable_ref,
        )
        if (
            descriptor.factory_callable_ref != descriptor.manifest.implementation_ref
            or len(binding_ids) != len(set(binding_ids))
            or any(_CALLABLE_REF_SEPARATOR not in item for item in callable_refs)
        ):
            _failure(
                failures,
                stage=CatalogBuildStageV1.PROCESS_BINDING,
                descriptor=descriptor,
                field_path="descriptor",
                reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
                context={"condition": "descriptor_binding_closure_invalid"},
            )
        manifest_files = {item.path: item.sha256 for item in descriptor.manifest.source_attribution.aistock_files}
        for item in descriptor.manifest.source_attribution.aistock_files:
            path = Path(__file__).resolve().parents[3] / item.path
            actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
            if actual != item.sha256:
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.SOURCE_BEHAVIOR,
                    descriptor=descriptor,
                    field_path=f"source_attribution.aistock_files[{item.path}]",
                    reason_code=MiniQMTPluginReasonCode.MANIFEST_HASH_CONFLICT,
                    context={"expected": item.sha256, "actual": actual},
                )
        binding_specs = (
            (
                "factory",
                descriptor.factory_binding_id,
                descriptor.factory_callable_ref,
                descriptor.factory_signature_sha256,
            ),
            (
                "config_validator",
                descriptor.config_validator_binding_id,
                descriptor.config_validator_callable_ref,
                descriptor.config_validator_signature_sha256,
            ),
            (
                "state_codec",
                descriptor.state_codec_binding_id,
                descriptor.state_codec_callable_ref,
                descriptor.state_codec_signature_sha256,
            ),
        )
        for name, binding_id, expected_ref, expected_signature in binding_specs:
            value = process_bindings.resolve(binding_id)
            if value is None or not callable(value):
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.PROCESS_BINDING,
                    descriptor=descriptor,
                    field_path=f"{name}_binding_id",
                    reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
                    context={"binding_id": binding_id, "condition": "missing_or_not_callable"},
                )
                continue
            try:
                actual_ref = callable_ref_v1(value)
                actual_signature = callable_signature_sha256_v1(value)
                source_path, source_hash = _source_path_and_hash(value)
            except (TypeError, ValueError, OSError) as exc:
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.PROCESS_BINDING,
                    descriptor=descriptor,
                    field_path=name,
                    reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
                    context=exc,
                )
                continue
            if actual_ref != expected_ref or actual_signature != expected_signature:
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.PROCESS_BINDING,
                    descriptor=descriptor,
                    field_path=name,
                    reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
                    context={
                        "expected_ref": expected_ref,
                        "actual_ref": actual_ref,
                        "expected_signature": expected_signature,
                        "actual_signature": actual_signature,
                    },
                )
            if manifest_files.get(source_path) != source_hash:
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.PROCESS_BINDING,
                    descriptor=descriptor,
                    field_path=f"{name}.source",
                    reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
                    context={"path": source_path, "expected": manifest_files.get(source_path), "actual": source_hash},
                )

    expected_binding_ids = {
        binding_id
        for descriptor in parsed_descriptors
        for binding_id in (
            descriptor.factory_binding_id,
            descriptor.config_validator_binding_id,
            descriptor.state_codec_binding_id,
        )
    }
    for binding_id in sorted(set(process_bindings.binding_ids) - expected_binding_ids):
        _failure(
            failures,
            stage=CatalogBuildStageV1.PROCESS_BINDING,
            descriptor=None,
            field_path="process_bindings",
            reason_code=MiniQMTPluginReasonCode.BINDING_INVALID,
            context={"condition": "orphan_process_binding", "binding_id": binding_id},
        )

    receipts_by_key: dict[PluginKeyV1, list[VnpyCompatibilityReceiptV1]] = defaultdict(list)
    for receipt in parsed_receipts:
        receipts_by_key[receipt.plugin_key].append(receipt)
    descriptor_keys = {item.plugin_key for item in parsed_descriptors}
    for receipt in parsed_receipts:
        if receipt.plugin_key not in descriptor_keys:
            _failure(
                failures,
                stage=CatalogBuildStageV1.PINNED_COMPATIBILITY,
                descriptor=None,
                field_path="pinned_compatibility_receipts",
                reason_code=MiniQMTPluginReasonCode.VNPY_COMPAT_SURFACE_UNSUPPORTED,
                context={"condition": "orphan_receipt", "plugin_key": receipt.plugin_key.canonical_payload_v1()},
            )
    for descriptor in parsed_descriptors:
        receipts = receipts_by_key.get(descriptor.plugin_key, [])
        if len(receipts) != 1:
            _failure(
                failures,
                stage=CatalogBuildStageV1.PINNED_COMPATIBILITY,
                descriptor=descriptor,
                field_path="pinned_compatibility_receipts",
                reason_code=MiniQMTPluginReasonCode.VNPY_COMPAT_SURFACE_UNSUPPORTED,
                context={"receipt_count": len(receipts)},
            )
            continue
        receipt = receipts[0]
        manifest = descriptor.manifest
        expected_components = compatibility_component_hashes_v1(manifest.compatibility_requirement)
        expected_receipt_sha256 = hash_hex_v1(
            "miniqmt_vnpy_compatibility_receipt_v1",
            receipt.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if (
            receipt.status is not CompatibilityStatusV1.PASSED
            or receipt.receipt_sha256 != expected_receipt_sha256
            or receipt.requirement_sha256 != manifest.compatibility_requirement.requirement_sha256
            or receipt.characterization_sha256 != manifest.compatibility_requirement.characterization_sha256
            or any(getattr(receipt, field) != expected for field, expected in expected_components.items())
        ):
            _failure(
                failures,
                stage=CatalogBuildStageV1.PINNED_COMPATIBILITY,
                descriptor=descriptor,
                field_path="pinned_compatibility_receipt",
                reason_code=MiniQMTPluginReasonCode.VNPY_COMPAT_SURFACE_UNSUPPORTED,
                context={
                    "status": receipt.status.value,
                    "requirement_sha256": receipt.requirement_sha256,
                    "characterization_sha256": receipt.characterization_sha256,
                    "expected_components": expected_components,
                    "actual_components": {field: getattr(receipt, field) for field in expected_components},
                },
            )

    key_counts = Counter(item.plugin_key for item in parsed_descriptors)
    version_counts = Counter((item.manifest.plugin_id, item.manifest.plugin_version) for item in parsed_descriptors)
    for descriptor in parsed_descriptors:
        if (
            key_counts[descriptor.plugin_key] > 1
            or version_counts[(descriptor.manifest.plugin_id, descriptor.manifest.plugin_version)] > 1
        ):
            _failure(
                failures,
                stage=CatalogBuildStageV1.REGISTRATION_CREATION,
                descriptor=descriptor,
                field_path="plugin_key",
                reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
                context={"duplicate_key_count": key_counts[descriptor.plugin_key]},
            )
    binding_id_counts = Counter(
        binding_id
        for descriptor in parsed_descriptors
        for binding_id in (
            descriptor.factory_binding_id,
            descriptor.config_validator_binding_id,
            descriptor.state_codec_binding_id,
        )
    )
    for descriptor in parsed_descriptors:
        for field_name in ("factory_binding_id", "config_validator_binding_id", "state_codec_binding_id"):
            binding_id = getattr(descriptor, field_name)
            if binding_id_counts[binding_id] > 1:
                _failure(
                    failures,
                    stage=CatalogBuildStageV1.REGISTRATION_CREATION,
                    descriptor=descriptor,
                    field_path=field_name,
                    reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
                    context={"binding_id": binding_id, "owner_count": binding_id_counts[binding_id]},
                )
    descriptor_by_key = {item.plugin_key: item for item in parsed_descriptors}
    creation_counts = Counter(item.algo_code for item in parsed_creation)
    descriptor_algo_codes = {item.manifest.algo_code for item in parsed_descriptors}
    for algo_code in descriptor_algo_codes:
        if creation_counts[algo_code] != 1:
            _failure(
                failures,
                stage=CatalogBuildStageV1.REGISTRATION_CREATION,
                descriptor=None,
                field_path=f"creation_bindings[{algo_code}]",
                reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
                context={"binding_count": creation_counts[algo_code]},
            )
    for binding in parsed_creation:
        descriptor = descriptor_by_key.get(binding.plugin_key)
        if descriptor is None or descriptor.manifest.algo_code != binding.algo_code:
            _failure(
                failures,
                stage=CatalogBuildStageV1.REGISTRATION_CREATION,
                descriptor=descriptor,
                field_path=f"creation_bindings[{binding.algo_code}]",
                reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
                context={"plugin_key": binding.plugin_key.canonical_payload_v1()},
            )

    if failures:
        raise PluginCatalogBuildError(
            PluginCatalogBuildFailureReceiptV1.create(
                build_input_sha256=build_input_sha256,
                descriptor_keys=tuple(item.plugin_key for item in parsed_descriptors),
                failures=failures,
            )
        )
    try:
        snapshot = PluginCatalogSnapshotV1.create(
            descriptors=tuple(parsed_descriptors),
            receipts=tuple(parsed_receipts),
            creation_bindings=tuple(parsed_creation),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        _failure(
            failures,
            stage=CatalogBuildStageV1.SNAPSHOT_FREEZE,
            descriptor=None,
            field_path="catalog_snapshot",
            reason_code=MiniQMTPluginReasonCode.REGISTRATION_CONFLICT,
            context=exc,
        )
        raise PluginCatalogBuildError(
            PluginCatalogBuildFailureReceiptV1.create(
                build_input_sha256=build_input_sha256,
                descriptor_keys=tuple(item.plugin_key for item in parsed_descriptors),
                failures=failures,
            )
        ) from exc
    return PluginCatalogRuntimeV2(snapshot=snapshot, process_bindings=process_bindings)


def evaluate_plugin_route_compatibility_v1(
    *,
    catalog_snapshot: PluginCatalogSnapshotV1,
    plugin_key: PluginKeyV1,
    gateway_catalog: GatewayCapabilityCatalogV1,
) -> PluginRouteCompatibilityReceiptV1:
    catalog_snapshot = PluginCatalogSnapshotV1.model_validate(catalog_snapshot.model_dump(mode="python"), strict=True)
    descriptor = next(
        (item for item in catalog_snapshot.registration_descriptors if item.plugin_key == plugin_key),
        None,
    )
    if descriptor is None:
        raise KeyError(plugin_key)
    manifest = descriptor.manifest
    failures: list[PluginRouteCompatibilityFailureV1] = []
    required_orders = {item.value for item in manifest.supported_order_types}
    supported_orders = {item.value for item in gateway_catalog.order_types}
    if not required_orders.issubset(supported_orders):
        failures.append(
            PluginRouteCompatibilityFailureV1.create(
                field_path="order_types", required=sorted(required_orders), supported=sorted(supported_orders)
            )
        )
    if gateway_catalog.gateway_backend not in manifest.supported_broker_backends:
        failures.append(
            PluginRouteCompatibilityFailureV1.create(
                field_path="gateway_backend",
                required=list(manifest.supported_broker_backends),
                supported=gateway_catalog.gateway_backend,
            )
        )
    supported_capabilities = set(gateway_catalog.market_data_capabilities)
    supported_phases = set(gateway_catalog.session_phases)
    for requirement in manifest.market_data_requirements:
        if requirement.capability not in supported_capabilities:
            failures.append(
                PluginRouteCompatibilityFailureV1.create(
                    field_path=f"market_data_capabilities.{requirement.capability.value}",
                    required=requirement.canonical_payload_v1(),
                    supported=[item.value for item in gateway_catalog.market_data_capabilities],
                )
            )
        if not set(requirement.session_phases).issubset(supported_phases):
            failures.append(
                PluginRouteCompatibilityFailureV1.create(
                    field_path=f"session_phases.{requirement.capability.value}",
                    required=[item.value for item in requirement.session_phases],
                    supported=[item.value for item in gateway_catalog.session_phases],
                )
            )
    return PluginRouteCompatibilityReceiptV1.create(
        descriptor=descriptor,
        catalog_sha256=catalog_snapshot.catalog_sha256,
        gateway_catalog=gateway_catalog,
        failures=failures,
    )
