"""Strict, side-effect-free MiniQMT execution plugin contracts (K1-A).

The models in this module are intentionally not imported by the product
runtime yet.  They define the frozen writer/readback schema consumed by later
K1/K2 slices without reaching into repositories, gateways, OMS, or broker SDKs.
"""

from __future__ import annotations

import re
import json
from datetime import date, datetime, time
from enum import StrEnum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Annotated, Any, Literal, Self

from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    MarketCode,
    SessionSegment,
)

from .plugin_canonical import (
    FrozenJsonObjectV1,
    FrozenJsonValueV1,
    canonical_decimal_string_v1,
    canonical_utc_datetime_v1,
    freeze_json_v1,
    hash_hex_v1,
    json_safe_evidence_v1,
    require_identity_v1,
    require_sha256_v1,
    thaw_json_v1,
    validate_json_text_v1,
)

_PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9_]*)+$")
_ALGO_CODE_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SEMVER_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
_IMPLEMENTATION_REF_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_A_SHARE_SYMBOL_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_MAX_SCHEMA_ERROR_PATH_PARTS = 64
_MAX_SCHEMA_VIOLATIONS = 32


def _freeze_json_field(value: Any) -> FrozenJsonValueV1:
    return freeze_json_v1(value)


def _freeze_json_object_field(value: Any) -> FrozenJsonObjectV1:
    frozen = freeze_json_v1(value)
    if not isinstance(frozen, FrozenJsonObjectV1):
        raise TypeError("field must be a JSON object")
    return frozen


FrozenJsonFieldV1 = Annotated[
    Any,
    BeforeValidator(_freeze_json_field),
    PlainSerializer(thaw_json_v1, return_type=Any, when_used="always"),
]
FrozenJsonObjectFieldV1 = Annotated[
    Any,
    BeforeValidator(_freeze_json_object_field),
    PlainSerializer(thaw_json_v1, return_type=dict[str, Any], when_used="always"),
]
IdentityV1 = Annotated[StrictStr, BeforeValidator(lambda value: require_identity_v1(value, field_name="identity"))]
Sha256V1 = Annotated[StrictStr, BeforeValidator(lambda value: require_sha256_v1(value, field_name="sha256"))]
CanonicalDecimalV1 = Annotated[
    StrictStr,
    BeforeValidator(lambda value: canonical_decimal_string_v1(value, field_name="decimal")),
]
PositiveCanonicalDecimalV1 = Annotated[
    StrictStr,
    BeforeValidator(lambda value: canonical_decimal_string_v1(value, field_name="decimal", allow_zero=False)),
]
UtcDateTimeV1 = Annotated[
    StrictStr,
    BeforeValidator(lambda value: canonical_utc_datetime_v1(value, field_name="datetime")),
]
PositiveIntV1 = Annotated[StrictInt, Field(gt=0)]
NonNegativeIntV1 = Annotated[StrictInt, Field(ge=0)]


class FrozenStrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    def canonical_payload_v1(self, *, exclude: set[str] | None = None) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude=exclude or set())

    @classmethod
    def model_validate_json(cls, json_data: str | bytes | bytearray, **kwargs: Any) -> Self:
        validate_json_text_v1(json_data)
        return super().model_validate_json(json_data, **kwargs)


def _json_schema_path_v1(path: Any) -> str:
    parts: list[str] = []
    for index, item in enumerate(path):
        if index >= _MAX_SCHEMA_ERROR_PATH_PARTS:
            parts.append("__path_truncated__")
            break
        parts.append(str(item).replace("~", "~0").replace("/", "~1"))
    return "$" if not parts else "$/" + "/".join(parts)


def _validate_local_schema_reference_v1(*, root: Any, reference: str, field_name: str, path: tuple[Any, ...]) -> None:
    if reference == "#":
        return
    if not reference.startswith("#/"):
        raise ValueError(f"{field_name} schema reference must use a local JSON pointer at {_json_schema_path_v1(path)}")
    target = root
    for raw_token in reference[2:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(target, dict) and token in target:
            target = target[token]
            continue
        if isinstance(target, list) and token.isdigit() and int(token) < len(target):
            target = target[int(token)]
            continue
        raise ValueError(f"{field_name} schema reference target does not exist at {_json_schema_path_v1(path)}")


def _validate_json_schema_reference_closure_v1(
    value: Any,
    *,
    field_name: str,
    path: tuple[Any, ...] = (),
    root: Any | None = None,
) -> None:
    root = value if root is None else root
    if isinstance(value, dict):
        for key, member_value in value.items():
            member_path = (*path, key)
            if key in ("$ref", "$dynamicRef"):
                if type(member_value) is not str or not member_value.startswith("#"):
                    raise ValueError(
                        f"{field_name} external schema reference is forbidden at {_json_schema_path_v1(member_path)}"
                    )
                _validate_local_schema_reference_v1(
                    root=root,
                    reference=member_value,
                    field_name=field_name,
                    path=member_path,
                )
            _validate_json_schema_reference_closure_v1(
                member_value,
                field_name=field_name,
                path=member_path,
                root=root,
            )
    elif isinstance(value, list):
        for index, member_value in enumerate(value):
            _validate_json_schema_reference_closure_v1(
                member_value,
                field_name=field_name,
                path=(*path, index),
                root=root,
            )


def _validate_json_schema_definition_v1(schema: FrozenJsonObjectV1, *, field_name: str) -> None:
    plain_schema = thaw_json_v1(schema)
    _validate_json_schema_reference_closure_v1(plain_schema, field_name=field_name)
    try:
        schema_validator = validator_for(plain_schema)
        schema_validator.check_schema(plain_schema)
    except jsonschema_exceptions.SchemaError as exc:
        raise ValueError(
            f"{field_name} is not a valid JSON schema at {_json_schema_path_v1(exc.absolute_schema_path)}"
        ) from exc


def validate_json_schema_instance_v1(
    *,
    schema: FrozenJsonObjectV1,
    instance: FrozenJsonObjectV1,
    contract_name: str,
    reason_code: MiniQMTPluginReasonCode | None = None,
) -> None:
    plain_schema = thaw_json_v1(schema)
    plain_instance = thaw_json_v1(instance)
    schema_validator = validator_for(plain_schema)
    errors: list[jsonschema_exceptions.ValidationError] = []
    violations_truncated = False
    for error in schema_validator(plain_schema).iter_errors(plain_instance):
        if len(errors) >= _MAX_SCHEMA_VIOLATIONS:
            violations_truncated = True
            break
        errors.append(error)
    errors.sort(
        key=lambda item: (
            tuple(str(part) for part in item.absolute_path),
            str(item.validator),
            item.message,
        )
    )
    if not errors:
        return
    ordered_violations = [
        {
            "path": [str(part) for part in tuple(error.absolute_path)[:_MAX_SCHEMA_ERROR_PATH_PARTS]],
            "validator": str(error.validator),
            "message": json_safe_evidence_v1(error.message),
        }
        for error in errors
    ]
    observed_lower_bound = len(errors) + (1 if violations_truncated else 0)
    context = {
        "schema_version": "miniqmt_json_schema_failure_evidence_v1",
        "contract_name": contract_name,
        "violations_truncated": violations_truncated,
        "retained_violation_count": len(errors),
        "observed_violation_count_lower_bound": observed_lower_bound,
        "ordered_violations": ordered_violations,
    }
    details = f"{contract_name} validation failed"
    if violations_truncated:
        details += f"; additional violations omitted after limit={_MAX_SCHEMA_VIOLATIONS}"
    if reason_code is None:
        reason_code = (
            MiniQMTPluginReasonCode.CONFIG_SCHEMA_INVALID
            if "config" in contract_name.lower()
            else MiniQMTPluginReasonCode.STATE_SCHEMA_INVALID
        )
    raise MiniQMTPluginContractError(reason_code, details, context=context)


def _validate_json_schema_instance_v1(
    *,
    schema: FrozenJsonObjectV1,
    instance: FrozenJsonObjectV1,
    contract_name: str,
) -> None:
    validate_json_schema_instance_v1(schema=schema, instance=instance, contract_name=contract_name)


def _prefixed_identity_v1(*, prefix: str, domain: str, payload: dict[str, Any]) -> str:
    return prefix + hash_hex_v1(domain, payload)


def _delivery_id_v1(*, event_id: str, algo_instance_id: str, plugin_manifest_sha256: str) -> str:
    return _prefixed_identity_v1(
        prefix="mqdelivery_",
        domain="miniqmt_algo_event_delivery_identity_v1",
        payload={
            "event_id": event_id,
            "algo_instance_id": algo_instance_id,
            "plugin_manifest_sha256": plugin_manifest_sha256,
        },
    )


def _algo_instance_id_v2(
    *,
    runtime_id: str,
    parent_intent_id: str,
    strategy_slot_id: str,
    algo_code: str,
    plugin_id: str,
    plugin_version: str,
    plugin_manifest_sha256: str,
    plugin_config_sha256: str,
) -> str:
    return _prefixed_identity_v1(
        prefix="mqalgo_",
        domain="miniqmt_algo_instance_v2",
        payload={
            "runtime_id": runtime_id,
            "parent_intent_id": parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "algo_code": algo_code,
            "plugin_id": plugin_id,
            "plugin_version": plugin_version,
            "plugin_manifest_sha256": plugin_manifest_sha256,
            "plugin_config_sha256": plugin_config_sha256,
        },
    )


def _submit_local_order_id_v1(
    *,
    runtime_id: str,
    algo_instance_id: str,
    parent_intent_id: str,
    transition_id: str,
    ordinal: int,
    symbol: str,
    side: "SideV1",
    order_type: "OrderTypeV1",
) -> str:
    return _prefixed_identity_v1(
        prefix="mqlocalorder_",
        domain="miniqmt_local_order_identity_v1",
        payload={
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "parent_intent_id": parent_intent_id,
            "transition_id": transition_id,
            "ordinal": ordinal,
            "symbol": symbol,
            "side": side.value,
            "order_type": order_type.value,
        },
    )


def _broker_command_id_v2(payload: dict[str, Any]) -> str:
    return _prefixed_identity_v1(
        prefix="mqcommand_",
        domain="miniqmt_broker_command_identity_v2",
        payload=payload,
    )


def _timer_schedule_id_v1(*, algo_instance_id: str, timer_name: str, schedule_epoch: str) -> str:
    return _prefixed_identity_v1(
        prefix="mqtimersched_",
        domain="miniqmt_timer_schedule_identity_v1",
        payload={
            "algo_instance_id": algo_instance_id,
            "timer_name": timer_name,
            "schedule_epoch": schedule_epoch,
        },
    )


def _timer_occurrence_id_v1(*, schedule_id: str, due_at_exchange_utc: str) -> str:
    return _prefixed_identity_v1(
        prefix="mqtimerocc_",
        domain="miniqmt_timer_occurrence_identity_v1",
        payload={"schedule_id": schedule_id, "due_at_exchange_utc": due_at_exchange_utc},
    )


def _diagnostic_observation_id_v1(payload: dict[str, Any]) -> str:
    return _prefixed_identity_v1(
        prefix="mqdiag_",
        domain="miniqmt_diagnostic_observation_identity_v1",
        payload=payload,
    )


class MiniQMTPluginReasonCode(StrEnum):
    MANIFEST_SCHEMA_INVALID = "MINIQMT_PLUGIN_MANIFEST_SCHEMA_INVALID"
    MANIFEST_HASH_CONFLICT = "MINIQMT_PLUGIN_MANIFEST_HASH_CONFLICT"
    REGISTRATION_CONFLICT = "MINIQMT_PLUGIN_REGISTRATION_CONFLICT"
    BINDING_INVALID = "MINIQMT_PLUGIN_BINDING_INVALID"
    CONFIG_SCHEMA_INVALID = "MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID"
    STATE_SCHEMA_INVALID = "MINIQMT_PLUGIN_STATE_SCHEMA_INVALID"
    CAPABILITY_UNSUPPORTED = "MINIQMT_PLUGIN_CAPABILITY_UNSUPPORTED"
    GATEWAY_CAPABILITY_CATALOG_INVALID = "MINIQMT_GATEWAY_CAPABILITY_CATALOG_INVALID"
    ROUTE_COMPATIBILITY_RECEIPT_INVALID = "MINIQMT_PLUGIN_ROUTE_COMPATIBILITY_RECEIPT_INVALID"
    VNPY_COMPAT_SURFACE_UNSUPPORTED = "MINIQMT_VNPY_COMPAT_SURFACE_UNSUPPORTED"
    DETERMINISM_CONFLICT = "MINIQMT_PLUGIN_DETERMINISM_CONFLICT"
    RUNTIME_EVENT_SCHEMA_INVALID = "MINIQMT_RUNTIME_EVENT_SCHEMA_INVALID"


class MiniQMTPluginContractError(ValueError):
    """Typed failure whose context construction cannot mask the primary error."""

    def __init__(
        self,
        reason_code: MiniQMTPluginReasonCode,
        message: str,
        *,
        context: Any,
    ) -> None:
        if not isinstance(reason_code, MiniQMTPluginReasonCode):
            raise TypeError("reason_code must be MiniQMTPluginReasonCode")
        self.reason_code = reason_code
        self.message = require_identity_v1(message, field_name="message")
        self._frozen_context = freeze_json_v1(json_safe_evidence_v1(context))
        super().__init__(self.message)

    @property
    def context(self) -> Any:
        return thaw_json_v1(self._frozen_context)

    def as_dict(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code.value,
            "message": self.message,
            "context": self.context,
        }


class PluginProviderV2(StrEnum):
    AISTOCK_DERIVED = "AISTOCK_DERIVED"
    VNPY_COMPAT = "VNPY_COMPAT"


class SideV1(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderTypeV1(StrEnum):
    LIMIT = "LIMIT"


class EventTypeV2(StrEnum):
    ALGO_START = "ALGO_START"
    TICK = "TICK"
    TIMER = "TIMER"
    ORDER = "ORDER"
    TRADE = "TRADE"
    ACCOUNT = "ACCOUNT"
    SESSION = "SESSION"
    RECONCILE = "RECONCILE"
    EOD = "EOD"
    OPERATOR = "OPERATOR"


class EventSourceV2(StrEnum):
    MINIQMT_EXECUTION_KERNEL = "MINIQMT_EXECUTION_KERNEL"
    B0_QUOTE_V2 = "B0_QUOTE_V2"
    EXCHANGE_SESSION_CLOCK = "EXCHANGE_SESSION_CLOCK"
    QMT_GATEWAY_CALLBACK = "QMT_GATEWAY_CALLBACK"
    QMT_OMS_PROJECTION = "QMT_OMS_PROJECTION"
    QMT_OMS_RECONCILIATION = "QMT_OMS_RECONCILIATION"
    SIMULATION_RUNTIME_OPERATOR = "SIMULATION_RUNTIME_OPERATOR"


class SessionPhaseV1(StrEnum):
    OPEN_AUCTION = "OPEN_AUCTION"
    CONTINUOUS_AM = "CONTINUOUS_AM"
    LUNCH_BREAK = "LUNCH_BREAK"
    CONTINUOUS_PM = "CONTINUOUS_PM"
    CLOSE_AUCTION = "CLOSE_AUCTION"
    CLOSED = "CLOSED"


class MarketDataCapabilityV1(StrEnum):
    L1_BID = "L1_BID"
    L1_ASK = "L1_ASK"
    DEPTH_5_BID = "DEPTH_5_BID"
    DEPTH_5_ASK = "DEPTH_5_ASK"
    LAST_PRICE = "LAST_PRICE"
    LIMIT_UP_DOWN = "LIMIT_UP_DOWN"
    SESSION_PHASE = "SESSION_PHASE"
    TRADE_STATS = "TRADE_STATS"
    AUCTION_NATIVE = "AUCTION_NATIVE"


class AbsenceDispositionV1(StrEnum):
    WAIT_FOR_NEXT_VALID_EVENT = "WAIT_FOR_NEXT_VALID_EVENT"
    TERMINAL_AT_SESSION_BOUNDARY = "TERMINAL_AT_SESSION_BOUNDARY"


class DeliveryStatusV1(StrEnum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    APPLIED = "APPLIED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    SKIPPED_TERMINAL = "SKIPPED_TERMINAL"


class BrokerCommandTypeV2(StrEnum):
    SUBMIT_LIMIT = "SUBMIT_LIMIT"
    CANCEL_ORDER = "CANCEL_ORDER"


class TimerMutationTypeV1(StrEnum):
    UPSERT_ONE_SHOT = "UPSERT_ONE_SHOT"
    CANCEL = "CANCEL"


class DiagnosticSeverityV1(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class TerminalOutcomeV1(StrEnum):
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    EXPIRED_WITH_RESIDUAL = "EXPIRED_WITH_RESIDUAL"


class FileHashV1(FrozenStrictModel):
    path: IdentityV1
    sha256: Sha256V1


class SourceAttributionV1(FrozenStrictModel):
    schema_version: Literal["source_attribution_v1"]
    upstream_repo: IdentityV1
    upstream_commit: IdentityV1
    upstream_files: tuple[FileHashV1, ...]
    upstream_license: IdentityV1
    upstream_copyright: IdentityV1
    aistock_asset_version: IdentityV1
    aistock_files: tuple[FileHashV1, ...]
    derivation_summary: IdentityV1
    attribution_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        if _GIT_SHA_RE.fullmatch(self.upstream_commit) is None:
            raise ValueError("upstream_commit must be a 40-character lowercase git sha")
        for field_name in ("upstream_files", "aistock_files"):
            files = getattr(self, field_name)
            object.__setattr__(self, field_name, _sorted_models(files, "path", field_name))
        expected = hash_hex_v1(
            "miniqmt_source_attribution_v1",
            self.canonical_payload_v1(exclude={"attribution_sha256"}),
        )
        if self.attribution_sha256 != expected:
            raise ValueError("attribution_sha256 does not match source attribution closure")
        return self


class VnpyParameterKindV1(StrEnum):
    POSITIONAL_ONLY = "POSITIONAL_ONLY"
    POSITIONAL_OR_KEYWORD = "POSITIONAL_OR_KEYWORD"
    VAR_POSITIONAL = "VAR_POSITIONAL"
    KEYWORD_ONLY = "KEYWORD_ONLY"
    VAR_KEYWORD = "VAR_KEYWORD"


class VnpyParameterRequirementV1(FrozenStrictModel):
    schema_version: Literal["vnpy_parameter_requirement_v1"] = "vnpy_parameter_requirement_v1"
    name: IdentityV1
    kind: VnpyParameterKindV1
    required: StrictBool
    default_present: StrictBool
    default_value: FrozenJsonFieldV1 | None = None
    annotation: IdentityV1

    @model_validator(mode="after")
    def _validate_default_closure(self) -> Self:
        variadic = self.kind in (VnpyParameterKindV1.VAR_POSITIONAL, VnpyParameterKindV1.VAR_KEYWORD)
        if variadic and (self.required or self.default_present or self.default_value is not None):
            raise ValueError("variadic parameter cannot be required or have a default")
        if not variadic and self.required == self.default_present:
            raise ValueError("non-variadic parameter must be required or have an explicit default")
        if not self.default_present and self.default_value is not None:
            raise ValueError("default_value requires default_present")
        return self


class VnpyObjectFieldKindV1(StrEnum):
    ATTRIBUTE = "ATTRIBUTE"
    CALLABLE = "CALLABLE"


class VnpyObjectFieldV1(FrozenStrictModel):
    schema_version: Literal["vnpy_object_field_v1"] = "vnpy_object_field_v1"
    name: IdentityV1
    kind: VnpyObjectFieldKindV1
    annotation: IdentityV1
    nullable: StrictBool
    return_annotation: IdentityV1 | None = None

    @model_validator(mode="after")
    def _validate_kind_closure(self) -> Self:
        if self.kind is VnpyObjectFieldKindV1.CALLABLE and self.return_annotation is None:
            raise ValueError("callable object field requires return_annotation")
        if self.kind is VnpyObjectFieldKindV1.ATTRIBUTE and self.return_annotation is not None:
            raise ValueError("attribute object field cannot have return_annotation")
        return self


class VnpyMethodRequirementV1(FrozenStrictModel):
    schema_version: Literal["vnpy_method_requirement_v1"] = "vnpy_method_requirement_v1"
    source_path: IdentityV1
    owner: IdentityV1
    name: IdentityV1
    parameters: tuple[VnpyParameterRequirementV1, ...]
    return_annotation: IdentityV1
    return_behavior: IdentityV1
    error_behavior: IdentityV1
    method_requirement_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        names = tuple(item.name for item in self.parameters)
        if len(names) != len(set(names)):
            raise ValueError("method parameter names must be unique")
        expected = hash_hex_v1(
            "miniqmt_vnpy_method_requirement_v1",
            self.canonical_payload_v1(exclude={"method_requirement_sha256"}),
        )
        if self.method_requirement_sha256 != expected:
            raise ValueError("method requirement hash mismatch")
        return self


class ObjectFieldRequirementV1(FrozenStrictModel):
    schema_version: Literal["vnpy_object_requirement_v1"] = "vnpy_object_requirement_v1"
    object_name: IdentityV1
    source_path: IdentityV1
    fields: tuple[VnpyObjectFieldV1, ...]

    @model_validator(mode="after")
    def _validate_fields(self) -> Self:
        if not self.fields:
            raise ValueError("required object fields must not be empty")
        names = tuple(item.name for item in self.fields)
        if len(names) != len(set(names)):
            raise ValueError("required object field names must be unique")
        object.__setattr__(self, "fields", tuple(sorted(self.fields, key=lambda item: item.name)))
        return self


class VnpyEnumMemberRequirementV2(FrozenStrictModel):
    schema_version: Literal["vnpy_enum_member_requirement_v2"] = "vnpy_enum_member_requirement_v2"
    name: IdentityV1
    value_expression: IdentityV1


class EnumValueRequirementV2(FrozenStrictModel):
    schema_version: Literal["vnpy_enum_requirement_v2"] = "vnpy_enum_requirement_v2"
    enum_name: IdentityV1
    source_path: IdentityV1
    enum_kind: IdentityV1
    members: tuple[VnpyEnumMemberRequirementV2, ...]

    @property
    def values(self) -> tuple[str, ...]:
        return tuple(item.name for item in self.members)

    @model_validator(mode="after")
    def _validate_values(self) -> Self:
        if not self.members:
            raise ValueError("required enum values must not be empty")
        names = tuple(item.name for item in self.members)
        if len(names) != len(set(names)):
            raise ValueError("required enum member names must be unique")
        object.__setattr__(self, "members", tuple(sorted(self.members, key=lambda item: item.name)))
        return self


class VnpySourceFileV2(FrozenStrictModel):
    schema_version: Literal["vnpy_source_file_v2"] = "vnpy_source_file_v2"
    path: IdentityV1
    size_bytes: NonNegativeIntV1
    sha256: Sha256V1


class VnpyUpstreamSourceV2(FrozenStrictModel):
    schema_version: Literal["vnpy_upstream_source_v2"] = "vnpy_upstream_source_v2"
    namespace: Literal["VNPY_ALGOTRADING", "VNPY_CORE"]
    upstream_repo: IdentityV1
    release_tag: IdentityV1 | None = None
    upstream_commit: IdentityV1
    files: tuple[VnpySourceFileV2, ...]
    license_file: VnpySourceFileV2
    license: IdentityV1
    copyright: IdentityV1
    authority_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if _GIT_SHA_RE.fullmatch(self.upstream_commit) is None:
            raise ValueError("upstream_commit must be a 40-character lowercase git sha")
        if not self.files:
            raise ValueError("upstream source files must not be empty")
        paths = tuple(item.path for item in self.files)
        if len(paths) != len(set(paths)):
            raise ValueError("upstream source file paths must be unique")
        all_paths = (*paths, self.license_file.path)
        if any(
            path != PurePosixPath(path).as_posix()
            or PurePosixPath(path).is_absolute()
            or PureWindowsPath(path).is_absolute()
            or bool(PureWindowsPath(path).drive)
            or "\\" in path
            or any(part in ("", ".", "..") for part in PurePosixPath(path).parts)
            for path in all_paths
        ):
            raise ValueError("upstream source paths must be normalized relative POSIX paths")
        if self.license_file.path in paths:
            raise ValueError("license file must be distinct from source files")
        namespace_root = "vnpy_algotrading" if self.namespace == "VNPY_ALGOTRADING" else "vnpy_core"
        if any(not path.startswith(namespace_root + "/") for path in paths):
            raise ValueError("upstream source files must remain inside their authority namespace")
        if self.license_file.path != f"{namespace_root}/LICENSE":
            raise ValueError("upstream license file must use the exact authority namespace path")
        object.__setattr__(self, "files", tuple(sorted(self.files, key=lambda item: item.path)))
        expected = hash_hex_v1(
            "miniqmt_vnpy_upstream_source_authority_v2",
            self.canonical_payload_v1(exclude={"authority_sha256"}),
        )
        if self.authority_sha256 != expected:
            raise ValueError("upstream source authority hash mismatch")
        return self


class VnpyCompatibilityRequirementV2(FrozenStrictModel):
    schema_version: Literal["vnpy_compatibility_requirement_v2"]
    mode: Literal["DERIVED_SOURCE_EXACT_CHARACTERIZATION"]
    upstream_sources: tuple[VnpyUpstreamSourceV2, ...]
    source_files_and_hashes: tuple[FileHashV1, ...]
    required_method_signatures: tuple[VnpyMethodRequirementV1, ...]
    required_object_fields: tuple[ObjectFieldRequirementV1, ...]
    required_enum_values: tuple[EnumValueRequirementV2, ...]
    characterization_sha256: Sha256V1
    requirement_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        authorities = tuple(sorted(self.upstream_sources, key=lambda item: item.namespace))
        if tuple(item.namespace for item in authorities) != ("VNPY_ALGOTRADING", "VNPY_CORE"):
            raise ValueError("upstream_sources must contain exactly VNPY_ALGOTRADING and VNPY_CORE")
        object.__setattr__(self, "upstream_sources", authorities)
        object.__setattr__(
            self,
            "source_files_and_hashes",
            _sorted_models(self.source_files_and_hashes, "path", "source_files_and_hashes"),
        )
        source_paths = tuple(item.path for item in self.source_files_and_hashes)
        for path in source_paths:
            normalized = PurePosixPath(path)
            windows_path = PureWindowsPath(path)
            if (
                path != normalized.as_posix()
                or normalized.is_absolute()
                or windows_path.is_absolute()
                or bool(windows_path.drive)
                or "\\" in path
                or any(part in ("", ".", "..") for part in normalized.parts)
            ):
                raise ValueError("source_files_and_hashes paths must be normalized relative POSIX paths")
        authority_files = {item.path: authority.namespace for authority in authorities for item in authority.files}
        if set(source_paths) != set(authority_files):
            raise ValueError("source_files_and_hashes must equal the two upstream authority source set")
        methods = tuple(
            sorted(self.required_method_signatures, key=lambda item: (item.source_path, item.owner, item.name))
        )
        method_keys = tuple((item.source_path, item.owner, item.name) for item in methods)
        if not methods or len(method_keys) != len(set(method_keys)):
            raise ValueError("required_method_signatures must be non-empty with unique method identities")
        if any(
            item.source_path not in source_paths or not item.source_path.startswith("vnpy_algotrading/")
            for item in methods
        ):
            raise ValueError("required method source_path must be an algotrading authority file")
        object.__setattr__(self, "required_method_signatures", methods)

        objects = tuple(sorted(self.required_object_fields, key=lambda item: (item.object_name, item.source_path)))
        object_keys = tuple((item.object_name, item.source_path) for item in objects)
        if not objects or len(object_keys) != len(set(object_keys)):
            raise ValueError("required_object_fields must be non-empty with unique object identities")
        if any(
            item.source_path not in source_paths or not item.source_path.startswith("vnpy_core/") for item in objects
        ):
            raise ValueError("required object source_path must be a vnpy core authority file")
        object.__setattr__(self, "required_object_fields", objects)

        enums = tuple(sorted(self.required_enum_values, key=lambda item: (item.enum_name, item.source_path)))
        enum_keys = tuple((item.enum_name, item.source_path) for item in enums)
        if not enums or len(enum_keys) != len(set(enum_keys)):
            raise ValueError("required_enum_values must be non-empty with unique enum identities")
        if any(
            item.source_path not in source_paths
            or not (
                item.source_path.startswith("vnpy_core/")
                or (item.source_path == "vnpy_algotrading/base.py" and item.enum_name == "AlgoStatus")
            )
            for item in enums
        ):
            raise ValueError("required enum source_path must be an authority file")
        object.__setattr__(self, "required_enum_values", enums)
        expected = hash_hex_v1(
            "miniqmt_vnpy_compatibility_requirement_v2",
            self.canonical_payload_v1(exclude={"requirement_sha256"}),
        )
        if self.requirement_sha256 != expected:
            raise ValueError("requirement_sha256 does not match compatibility closure")
        return self


def compatibility_component_hashes_v2(requirement: VnpyCompatibilityRequirementV2) -> dict[str, str]:
    """Return the single canonical component-hash closure for K1-B/K1-C."""

    source_lock_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_source_lock_v2",
        {
            "upstream_sources": [item.canonical_payload_v1() for item in requirement.upstream_sources],
            "source_files_and_hashes": [item.canonical_payload_v1() for item in requirement.source_files_and_hashes],
        },
    )
    method_signature_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_method_signatures_v2",
        [item.canonical_payload_v1() for item in requirement.required_method_signatures],
    )
    object_field_sha256 = hash_hex_v1(
        "miniqmt_vnpy_compatibility_object_fields_v2",
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
        "surface_sha256": hash_hex_v1("miniqmt_vnpy_compatibility_surface_v2", surface_payload),
    }


# Historical names remain import aliases only; V2 is the sole active schema.
EnumValueRequirementV1 = EnumValueRequirementV2
VnpyCompatibilityRequirementV1 = VnpyCompatibilityRequirementV2
compatibility_component_hashes_v1 = compatibility_component_hashes_v2


def _sorted_unique(values: tuple[Any, ...], *, field_name: str) -> tuple[Any, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{field_name} must be a tuple")
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    comparable: list[str] = []
    for value in values:
        comparable_value = value.value if isinstance(value, StrEnum) else value
        if type(comparable_value) is not str:
            raise TypeError(f"{field_name} values must be strict strings or registered enums")
        comparable.append(comparable_value)
    if len(comparable) != len(set(comparable)):
        raise ValueError(f"{field_name} must not contain duplicates")
    return tuple(value for _, value in sorted(zip(comparable, values, strict=True), key=lambda pair: pair[0]))


def _sorted_models(values: tuple[Any, ...], attribute: str, field_name: str) -> tuple[Any, ...]:
    if not values:
        raise ValueError(f"{field_name} must not be empty")
    keys = [getattr(value, attribute) for value in values]
    if len(keys) != len(set(keys)):
        raise ValueError(f"{field_name} must contain unique keys")
    return tuple(value for _, value in sorted(zip(keys, values, strict=True), key=lambda pair: pair[0]))


class MarketDataRequirementV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_market_data_requirement_v1"]
    capability: MarketDataCapabilityV1
    required_fields: tuple[IdentityV1, ...]
    applicable_sides: tuple[SideV1, ...]
    event_types: tuple[EventTypeV2, ...]
    session_phases: tuple[SessionPhaseV1, ...]
    absence_disposition: AbsenceDispositionV1
    requirement_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        capability: MarketDataCapabilityV1,
        required_fields: tuple[str, ...],
        applicable_sides: tuple[SideV1, ...],
        event_types: tuple[EventTypeV2, ...],
        session_phases: tuple[SessionPhaseV1, ...],
        absence_disposition: AbsenceDispositionV1,
    ) -> Self:
        normalized_required_fields = _sorted_unique(required_fields, field_name="required_fields")
        normalized_sides = _sorted_unique(applicable_sides, field_name="applicable_sides")
        normalized_events = _sorted_unique(event_types, field_name="event_types")
        normalized_phases = _sorted_unique(session_phases, field_name="session_phases")
        payload = {
            "schema_version": "miniqmt_market_data_requirement_v1",
            "capability": capability.value,
            "required_fields": list(normalized_required_fields),
            "applicable_sides": [item.value for item in normalized_sides],
            "event_types": [item.value for item in normalized_events],
            "session_phases": [item.value for item in normalized_phases],
            "absence_disposition": absence_disposition.value,
        }
        return cls(
            schema_version="miniqmt_market_data_requirement_v1",
            capability=capability,
            required_fields=normalized_required_fields,
            applicable_sides=normalized_sides,
            event_types=normalized_events,
            session_phases=normalized_phases,
            absence_disposition=absence_disposition,
            requirement_sha256=hash_hex_v1("miniqmt_market_data_requirement_v1", payload),
        )

    def hash_payload_v1(self) -> dict[str, Any]:
        return self.canonical_payload_v1(exclude={"requirement_sha256"})

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        for name in ("required_fields", "applicable_sides", "event_types", "session_phases"):
            object.__setattr__(self, name, _sorted_unique(getattr(self, name), field_name=name))
        if self.capability in (MarketDataCapabilityV1.L1_BID, MarketDataCapabilityV1.L1_ASK):
            if not set(self.required_fields).issubset({"price", "volume"}):
                raise ValueError("L1 required_fields can only contain price and volume")
        expected = hash_hex_v1("miniqmt_market_data_requirement_v1", self.hash_payload_v1())
        if self.requirement_sha256 != expected:
            raise ValueError("requirement_sha256 does not match requirement closure")
        return self


class GatewayCapabilityCatalogV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_gateway_capability_catalog_v1"]
    route_id: IdentityV1
    quote_source: IdentityV1
    gateway_backend: IdentityV1
    order_types: tuple[OrderTypeV1, ...]
    market_data_capabilities: tuple[MarketDataCapabilityV1, ...]
    session_phases: tuple[SessionPhaseV1, ...]
    idempotent_submit_by_client_ref: StrictBool
    exact_order_id_cancel: StrictBool
    catalog_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        for name in ("order_types", "market_data_capabilities", "session_phases"):
            object.__setattr__(self, name, _sorted_unique(getattr(self, name), field_name=name))
        expected = hash_hex_v1(
            "miniqmt_gateway_capability_catalog_v1",
            self.canonical_payload_v1(exclude={"catalog_sha256"}),
        )
        if self.catalog_sha256 != expected:
            raise ValueError("catalog_sha256 does not match gateway capability closure")
        return self


class ExecutionAlgoPluginManifestV2(FrozenStrictModel):
    schema_version: Literal["execution_algo_plugin_manifest_v2"]
    plugin_id: IdentityV1
    algo_code: IdentityV1
    plugin_version: IdentityV1
    provider: PluginProviderV2
    implementation_ref: IdentityV1
    config_schema_version: IdentityV1
    config_schema: FrozenJsonObjectFieldV1
    config_schema_sha256: Sha256V1
    state_schema_version: IdentityV1
    state_schema: FrozenJsonObjectFieldV1
    state_schema_sha256: Sha256V1
    subscribed_event_types: tuple[EventTypeV2, ...]
    market_data_requirements: tuple[MarketDataRequirementV1, ...]
    required_facade_methods: tuple[IdentityV1, ...]
    required_facade_object_fields: tuple[ObjectFieldRequirementV1, ...]
    supported_sides: tuple[SideV1, ...]
    supported_order_types: tuple[OrderTypeV1, ...]
    supported_broker_backends: tuple[Literal["minqmt_sim"], ...]
    restart_policy: Literal["DURABLE_RESTORE"]
    source_attribution: SourceAttributionV1
    compatibility_requirement: VnpyCompatibilityRequirementV1
    behavior_characterization_sha256: Sha256V1
    behavior_contract_sha256: Sha256V1
    manifest_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_manifest(self) -> Self:
        _validate_json_schema_definition_v1(self.config_schema, field_name="config_schema")
        _validate_json_schema_definition_v1(self.state_schema, field_name="state_schema")
        if _PLUGIN_ID_RE.fullmatch(self.plugin_id) is None:
            raise ValueError("plugin_id must be a lowercase dotted id")
        if _ALGO_CODE_RE.fullmatch(self.algo_code) is None:
            raise ValueError("algo_code must be uppercase snake case")
        if _SEMVER_RE.fullmatch(self.plugin_version) is None:
            raise ValueError("plugin_version must be strict SemVer without leading v")
        if _IMPLEMENTATION_REF_RE.fullmatch(self.implementation_ref) is None:
            raise ValueError("implementation_ref must be python.module:ClassName")
        if EventTypeV2.ALGO_START not in self.subscribed_event_types:
            raise ValueError("subscribed_event_types must include ALGO_START")
        for name in (
            "subscribed_event_types",
            "required_facade_methods",
            "supported_sides",
            "supported_order_types",
            "supported_broker_backends",
        ):
            object.__setattr__(self, name, _sorted_unique(getattr(self, name), field_name=name))
        object.__setattr__(
            self,
            "required_facade_object_fields",
            _sorted_models(self.required_facade_object_fields, "object_name", "required_facade_object_fields"),
        )
        object.__setattr__(
            self,
            "market_data_requirements",
            _sorted_models(self.market_data_requirements, "requirement_sha256", "market_data_requirements"),
        )
        if not set(item for requirement in self.market_data_requirements for item in requirement.event_types).issubset(
            set(self.subscribed_event_types)
        ):
            raise ValueError("market data event_types must be a manifest subscription subset")
        expected_config = hash_hex_v1("miniqmt_plugin_config_schema_v1", thaw_json_v1(self.config_schema))
        expected_state = hash_hex_v1("miniqmt_plugin_state_schema_v1", thaw_json_v1(self.state_schema))
        if self.config_schema_sha256 != expected_config or self.state_schema_sha256 != expected_state:
            raise ValueError("config/state schema hash does not match schema closure")
        serialized = self.canonical_payload_v1()
        behavior_keys = (
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
        behavior_payload = {key: serialized[key] for key in behavior_keys}
        expected_behavior = hash_hex_v1("miniqmt_plugin_behavior_contract_v2", behavior_payload)
        if self.behavior_contract_sha256 != expected_behavior:
            raise ValueError("behavior_contract_sha256 does not match behavior closure")
        expected_manifest = hash_hex_v1(
            "execution_algo_plugin_manifest_v2",
            {key: value for key, value in serialized.items() if key != "manifest_sha256"},
        )
        if self.manifest_sha256 != expected_manifest:
            raise ValueError("manifest_sha256 does not match manifest closure")
        return self


_EVENT_COMPOSITE: dict[EventTypeV2, tuple[EventSourceV2, str, tuple[str, ...]]] = {
    EventTypeV2.ALGO_START: (
        EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        "miniqmt_algo_start_v1",
        (
            "algo_instance_id",
            "runtime_id",
            "parent_intent_id",
            "strategy_slot_id",
            "algo_code",
            "plugin_id",
            "plugin_version",
            "plugin_manifest_sha256",
            "plugin_config_sha256",
        ),
    ),
    EventTypeV2.TICK: (EventSourceV2.B0_QUOTE_V2, "miniqmt_market_data_view_v2", ("market_data_id",)),
    EventTypeV2.TIMER: (EventSourceV2.EXCHANGE_SESSION_CLOCK, "miniqmt_timer_due_v1", ("timer_occurrence_id",)),
    EventTypeV2.SESSION: (EventSourceV2.EXCHANGE_SESSION_CLOCK, "miniqmt_session_event_v1", ("session_event_id",)),
    EventTypeV2.EOD: (
        EventSourceV2.EXCHANGE_SESSION_CLOCK,
        "miniqmt_eod_event_v1",
        ("runtime_id", "trade_date", "session_epoch"),
    ),
    EventTypeV2.ORDER: (EventSourceV2.QMT_GATEWAY_CALLBACK, "miniqmt_order_event_v1", ("order_event_id",)),
    EventTypeV2.TRADE: (EventSourceV2.QMT_GATEWAY_CALLBACK, "miniqmt_trade_fact_v1", ("trade_id",)),
    EventTypeV2.ACCOUNT: (
        EventSourceV2.QMT_OMS_PROJECTION,
        "miniqmt_account_projection_v1",
        ("projection_version", "projection_sha256"),
    ),
    EventTypeV2.RECONCILE: (
        EventSourceV2.QMT_OMS_RECONCILIATION,
        "miniqmt_reconciliation_receipt_v1",
        ("receipt_id", "receipt_sha256"),
    ),
    EventTypeV2.OPERATOR: (
        EventSourceV2.SIMULATION_RUNTIME_OPERATOR,
        "miniqmt_operator_command_v1",
        ("operator_command_id",),
    ),
}


class RuntimeEventEnvelopeV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_runtime_event_envelope_v2"]
    event_id: IdentityV1
    event_key_sha256: Sha256V1
    runtime_id: IdentityV1
    sequence: PositiveIntV1
    event_type: EventTypeV2
    event_time_utc: UtcDateTimeV1
    monotonic_ns: NonNegativeIntV1 | None
    source: EventSourceV2
    symbol: IdentityV1 | None
    payload_schema_version: IdentityV1
    payload: FrozenJsonObjectFieldV1
    payload_sha256: Sha256V1
    source_identity: FrozenJsonObjectFieldV1
    correlation: FrozenJsonObjectFieldV1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        sequence: int,
        event_type: EventTypeV2,
        event_time_utc: Any,
        monotonic_ns: int | None,
        source: EventSourceV2,
        symbol: str | None,
        payload_schema_version: str,
        payload: dict[str, Any],
        source_identity: dict[str, Any],
        correlation: dict[str, Any],
    ) -> Self:
        payload_sha256 = hash_hex_v1("miniqmt_runtime_event_payload_v2", payload)
        event_key_payload = {
            "schema_version": "miniqmt_runtime_event_envelope_v2",
            "runtime_id": runtime_id,
            "event_type": event_type.value,
            "source": source.value,
            "source_identity": source_identity,
        }
        event_key_sha256 = hash_hex_v1("miniqmt_runtime_event_key_v2", event_key_payload)
        return cls(
            schema_version="miniqmt_runtime_event_envelope_v2",
            event_id=f"mqrtevt_{event_key_sha256}",
            event_key_sha256=event_key_sha256,
            runtime_id=runtime_id,
            sequence=sequence,
            event_type=event_type,
            event_time_utc=event_time_utc,
            monotonic_ns=monotonic_ns,
            source=source,
            symbol=symbol,
            payload_schema_version=payload_schema_version,
            payload=payload,
            payload_sha256=payload_sha256,
            source_identity=source_identity,
            correlation=correlation,
        )

    @model_validator(mode="after")
    def _validate_event(self) -> Self:
        expected_source, expected_schema, required_identity_fields = _EVENT_COMPOSITE[self.event_type]
        if self.source is not expected_source or self.payload_schema_version != expected_schema:
            raise ValueError("event/source/payload schema combination is not registered")
        identity = thaw_json_v1(self.source_identity)
        expected_identity_fields = set(required_identity_fields)
        actual_identity_fields = set(identity)
        if actual_identity_fields != expected_identity_fields:
            raise ValueError(
                "source_identity must contain exact registered fields; "
                f"missing={sorted(expected_identity_fields - actual_identity_fields)}, "
                f"extra={sorted(actual_identity_fields - expected_identity_fields)}"
            )
        missing = [
            field for field in required_identity_fields if field not in identity or identity[field] in (None, "")
        ]
        if missing:
            raise ValueError(f"source_identity is missing required fields: {missing}")
        for field in required_identity_fields:
            try:
                require_identity_v1(identity[field], field_name=f"source_identity.{field}")
                if field.endswith("_sha256"):
                    require_sha256_v1(identity[field], field_name=f"source_identity.{field}")
            except (TypeError, ValueError) as exc:
                raise ValueError(f"source_identity.{field} is not a strict identity") from exc
        if "trade_date" in identity and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", identity["trade_date"]) is None:
            raise ValueError("source_identity.trade_date must be YYYY-MM-DD")
        if (
            self.event_type in (EventTypeV2.ALGO_START, EventTypeV2.TICK, EventTypeV2.ORDER, EventTypeV2.TRADE)
            and self.symbol is None
        ):
            raise ValueError("symbol is required for symbol-scoped runtime events")
        if self.event_type is EventTypeV2.TIMER and self.monotonic_ns is None:
            raise ValueError("TIMER event requires monotonic_ns from its process timer")
        if self.event_type is EventTypeV2.EOD and identity["runtime_id"] != self.runtime_id:
            raise ValueError("EOD source_identity runtime_id conflicts with event runtime_id")
        if self.event_type is EventTypeV2.ALGO_START and identity["runtime_id"] != self.runtime_id:
            raise ValueError("ALGO_START source_identity runtime_id conflicts with event runtime_id")
        if self.event_type is EventTypeV2.ALGO_START:
            if _ALGO_CODE_RE.fullmatch(identity["algo_code"]) is None:
                raise ValueError("ALGO_START source_identity.algo_code is invalid")
            if _PLUGIN_ID_RE.fullmatch(identity["plugin_id"]) is None:
                raise ValueError("ALGO_START source_identity.plugin_id is invalid")
            if _SEMVER_RE.fullmatch(identity["plugin_version"]) is None:
                raise ValueError("ALGO_START source_identity.plugin_version is invalid")
            expected_algo_instance_id = _algo_instance_id_v2(
                runtime_id=identity["runtime_id"],
                parent_intent_id=identity["parent_intent_id"],
                strategy_slot_id=identity["strategy_slot_id"],
                algo_code=identity["algo_code"],
                plugin_id=identity["plugin_id"],
                plugin_version=identity["plugin_version"],
                plugin_manifest_sha256=identity["plugin_manifest_sha256"],
                plugin_config_sha256=identity["plugin_config_sha256"],
            )
            if identity["algo_instance_id"] != expected_algo_instance_id:
                raise ValueError("ALGO_START algo_instance_id does not match complete source identity closure")
        expected_payload_hash = hash_hex_v1("miniqmt_runtime_event_payload_v2", thaw_json_v1(self.payload))
        if self.payload_sha256 != expected_payload_hash:
            raise ValueError("payload_sha256 does not match payload closure")
        event_key_payload = {
            "schema_version": self.schema_version,
            "runtime_id": self.runtime_id,
            "event_type": self.event_type.value,
            "source": self.source.value,
            "source_identity": identity,
        }
        expected_key = hash_hex_v1("miniqmt_runtime_event_key_v2", event_key_payload)
        if self.event_key_sha256 != expected_key or self.event_id != f"mqrtevt_{expected_key}":
            raise ValueError("event identity does not match source identity closure")
        return self


class AlgoEventDeliveryV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_event_delivery_v1"]
    delivery_id: IdentityV1
    event_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    plugin_manifest_sha256: Sha256V1
    algo_delivery_sequence: PositiveIntV1
    previous_delivery_id: IdentityV1 | None
    status: DeliveryStatusV1
    attempt_count: NonNegativeIntV1
    lease_owner: IdentityV1 | None
    lease_expires_at: UtcDateTimeV1 | None
    transition_id: IdentityV1 | None
    last_error_json: FrozenJsonObjectFieldV1 | None
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1

    @classmethod
    def create(
        cls,
        *,
        event: RuntimeEventEnvelopeV2,
        algo_instance_id: str,
        plugin_manifest_sha256: str,
        algo_delivery_sequence: int,
        previous_delivery_id: str | None,
        status: DeliveryStatusV1,
        attempt_count: int,
        lease_owner: str | None,
        lease_expires_at: Any | None,
        transition_id: str | None,
        last_error_json: dict[str, Any] | None,
        created_at_utc: Any,
        updated_at_utc: Any,
    ) -> Self:
        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        if event.event_type is EventTypeV2.ALGO_START and algo_delivery_sequence != 1:
            raise ValueError("ALGO_START must be delivery sequence 1")
        return cls(
            schema_version="miniqmt_algo_event_delivery_v1",
            delivery_id=_delivery_id_v1(
                event_id=event.event_id,
                algo_instance_id=algo_instance_id,
                plugin_manifest_sha256=plugin_manifest_sha256,
            ),
            event_id=event.event_id,
            runtime_id=event.runtime_id,
            algo_instance_id=algo_instance_id,
            plugin_manifest_sha256=plugin_manifest_sha256,
            algo_delivery_sequence=algo_delivery_sequence,
            previous_delivery_id=previous_delivery_id,
            status=status,
            attempt_count=attempt_count,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            transition_id=transition_id,
            last_error_json=last_error_json,
            created_at_utc=created_at_utc,
            updated_at_utc=updated_at_utc,
        )

    @model_validator(mode="after")
    def _validate_delivery(self) -> Self:
        expected_delivery_id = _delivery_id_v1(
            event_id=self.event_id,
            algo_instance_id=self.algo_instance_id,
            plugin_manifest_sha256=self.plugin_manifest_sha256,
        )
        if self.delivery_id != expected_delivery_id:
            raise ValueError("delivery_id does not match event/algo/manifest identity closure")
        if self.algo_delivery_sequence == 1 and self.previous_delivery_id is not None:
            raise ValueError("first delivery must not have a predecessor")
        if self.algo_delivery_sequence > 1 and self.previous_delivery_id is None:
            raise ValueError("delivery sequence after 1 requires exact predecessor")
        if (self.lease_owner is None) != (self.lease_expires_at is None):
            raise ValueError("lease_owner and lease_expires_at must be present together")
        if self.status is DeliveryStatusV1.CLAIMED and self.lease_owner is None:
            raise ValueError("CLAIMED delivery requires a lease")
        if self.status is DeliveryStatusV1.APPLIED and self.transition_id is None:
            raise ValueError("APPLIED delivery requires transition_id")
        if (
            self.status in (DeliveryStatusV1.FAILED_RETRYABLE, DeliveryStatusV1.FAILED_TERMINAL)
            and self.last_error_json is None
        ):
            raise ValueError("failed delivery requires last_error_json")
        if self.last_error_json is not None:
            error = thaw_json_v1(self.last_error_json)
            required_error_fields = ("reason_code", "message", "context")
            missing_error_fields = [field for field in required_error_fields if field not in error]
            if missing_error_fields:
                raise ValueError(f"last_error_json is missing structured fields: {missing_error_fields}")
            require_identity_v1(error["reason_code"], field_name="last_error_json.reason_code")
            require_identity_v1(error["message"], field_name="last_error_json.message")
            if not isinstance(error["context"], dict):
                raise ValueError("last_error_json.context must be a JSON object")
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        return self


class AlgoStateSnapshotV2(FrozenStrictModel):
    schema_version: Literal["execution_algo_state_snapshot_v2"]
    algo_instance_id: IdentityV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    state_schema_version: IdentityV1
    transition_sequence: PositiveIntV1
    last_applied_delivery_sequence: PositiveIntV1
    last_applied_delivery_id: IdentityV1
    last_closed_delivery_sequence: PositiveIntV1
    state: FrozenJsonObjectFieldV1
    state_sha256: Sha256V1
    last_applied_event_id: IdentityV1
    updated_at_utc: UtcDateTimeV1

    @classmethod
    def create(
        cls,
        *,
        plugin_manifest: ExecutionAlgoPluginManifestV2,
        deterministic_context: "DeterministicExecutionContextV1",
        transition_sequence: int,
        last_applied_delivery_sequence: int,
        last_applied_delivery_id: str,
        last_closed_delivery_sequence: int,
        state: dict[str, Any],
        last_applied_event_id: str,
    ) -> Self:
        if not isinstance(plugin_manifest, ExecutionAlgoPluginManifestV2):
            raise TypeError("plugin_manifest must be ExecutionAlgoPluginManifestV2")
        if not isinstance(deterministic_context, DeterministicExecutionContextV1):
            raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
        frozen_state = _freeze_json_object_field(state)
        _validate_json_schema_instance_v1(
            schema=plugin_manifest.state_schema,
            instance=frozen_state,
            contract_name="state schema",
        )
        snapshot = cls(
            schema_version="execution_algo_state_snapshot_v2",
            algo_instance_id=deterministic_context.algo_instance_id,
            plugin_id=plugin_manifest.plugin_id,
            plugin_version=plugin_manifest.plugin_version,
            plugin_manifest_sha256=plugin_manifest.manifest_sha256,
            state_schema_version=plugin_manifest.state_schema_version,
            transition_sequence=transition_sequence,
            last_applied_delivery_sequence=last_applied_delivery_sequence,
            last_applied_delivery_id=last_applied_delivery_id,
            last_closed_delivery_sequence=last_closed_delivery_sequence,
            state=frozen_state,
            state_sha256=hash_hex_v1("execution_algo_state_v2", frozen_state),
            last_applied_event_id=last_applied_event_id,
            updated_at_utc=deterministic_context.logical_time_utc,
        )
        return snapshot.validate_against_authority_v1(
            plugin_manifest=plugin_manifest,
            deterministic_context=deterministic_context,
        )

    def validate_against_authority_v1(
        self,
        *,
        plugin_manifest: ExecutionAlgoPluginManifestV2,
        deterministic_context: "DeterministicExecutionContextV1",
    ) -> Self:
        if not isinstance(plugin_manifest, ExecutionAlgoPluginManifestV2):
            raise TypeError("plugin_manifest must be ExecutionAlgoPluginManifestV2")
        if not isinstance(deterministic_context, DeterministicExecutionContextV1):
            raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
        if (
            self.plugin_id != plugin_manifest.plugin_id
            or self.plugin_version != plugin_manifest.plugin_version
            or self.plugin_manifest_sha256 != plugin_manifest.manifest_sha256
            or self.state_schema_version != plugin_manifest.state_schema_version
        ):
            raise ValueError("state snapshot plugin/schema identity conflicts with manifest authority")
        if self.algo_instance_id != deterministic_context.algo_instance_id:
            raise ValueError("state snapshot algo identity conflicts with deterministic context")
        if self.plugin_manifest_sha256 != deterministic_context.plugin_manifest_sha256:
            raise ValueError("state snapshot manifest hash conflicts with deterministic context")
        if self.last_applied_event_id != deterministic_context.event_id:
            raise ValueError("state snapshot last event identity conflicts with deterministic context")
        if self.last_applied_delivery_id != deterministic_context.delivery_id:
            raise ValueError("state snapshot last delivery identity conflicts with deterministic context")
        if self.updated_at_utc != deterministic_context.logical_time_utc:
            raise ValueError("state snapshot updated_at_utc must equal deterministic context logical time")
        _validate_json_schema_instance_v1(
            schema=plugin_manifest.state_schema,
            instance=self.state,
            contract_name="state schema",
        )
        return self

    @model_validator(mode="after")
    def _validate_snapshot(self) -> Self:
        if _PLUGIN_ID_RE.fullmatch(self.plugin_id) is None:
            raise ValueError("plugin_id must be a lowercase dotted id")
        if _SEMVER_RE.fullmatch(self.plugin_version) is None:
            raise ValueError("plugin_version must be strict SemVer")
        if self.transition_sequence != self.last_applied_delivery_sequence:
            raise ValueError("transition_sequence must equal last_applied_delivery_sequence")
        if self.last_closed_delivery_sequence < self.last_applied_delivery_sequence:
            raise ValueError("last_closed_delivery_sequence cannot trail applied sequence")
        expected = hash_hex_v1("execution_algo_state_v2", thaw_json_v1(self.state))
        if self.state_sha256 != expected:
            raise ValueError("state_sha256 does not match frozen state closure")
        return self


class DeterministicExecutionContextV1(FrozenStrictModel):
    schema_version: Literal["deterministic_execution_context_v1"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    plugin_manifest_sha256: Sha256V1
    transition_sequence: NonNegativeIntV1
    logical_time_utc: UtcDateTimeV1
    exchange_trade_date: IdentityV1
    session_epoch: IdentityV1
    session_phase: SessionPhaseV1
    input_projection_sha256: Sha256V1
    context_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        algo_instance_id: str,
        event_id: str,
        delivery_id: str,
        plugin_manifest_sha256: str,
        transition_sequence: int,
        logical_time_utc: Any,
        exchange_trade_date: str,
        session_epoch: str,
        session_phase: SessionPhaseV1,
        input_projection_sha256: str,
    ) -> Self:
        normalized_time = canonical_utc_datetime_v1(logical_time_utc, field_name="logical_time_utc")
        payload = {
            "schema_version": "deterministic_execution_context_v1",
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "event_id": event_id,
            "delivery_id": delivery_id,
            "plugin_manifest_sha256": plugin_manifest_sha256,
            "transition_sequence": transition_sequence,
            "logical_time_utc": normalized_time,
            "exchange_trade_date": exchange_trade_date,
            "session_epoch": session_epoch,
            "session_phase": session_phase.value,
            "input_projection_sha256": input_projection_sha256,
        }
        model_payload = {**payload, "session_phase": session_phase}
        return cls(
            **model_payload,
            context_sha256=hash_hex_v1("miniqmt_deterministic_execution_context_v1", payload),
        )

    def hash_payload_v1(self) -> dict[str, Any]:
        return self.canonical_payload_v1(exclude={"context_sha256"})

    @model_validator(mode="after")
    def _validate_context(self) -> Self:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.exchange_trade_date) is None:
            raise ValueError("exchange_trade_date must be YYYY-MM-DD")
        expected = hash_hex_v1("miniqmt_deterministic_execution_context_v1", self.hash_payload_v1())
        if self.context_sha256 != expected:
            raise ValueError("context_sha256 does not match deterministic context closure")
        return self


class AlgoStartContextV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_start_context_v1"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    symbol: IdentityV1
    side: SideV1
    limit_price_decimal: PositiveCanonicalDecimalV1
    parent_quantity: PositiveIntV1
    min_volume: PositiveIntV1
    volume_increment: PositiveIntV1
    plugin_manifest: ExecutionAlgoPluginManifestV2
    plugin_config: FrozenJsonObjectFieldV1
    plugin_config_sha256: Sha256V1
    start_event_id: IdentityV1
    start_delivery_id: IdentityV1
    deterministic_context: DeterministicExecutionContextV1
    contract_projection: FrozenJsonObjectFieldV1
    contract_projection_sha256: Sha256V1
    account_projection: FrozenJsonObjectFieldV1
    account_projection_sha256: Sha256V1
    market_capability_projection: FrozenJsonObjectFieldV1
    market_capability_projection_sha256: Sha256V1
    execution_plan_id: IdentityV1
    execution_plan_sha256: Sha256V1
    release_id: IdentityV1
    release_sha256: Sha256V1
    policy_id: IdentityV1
    policy_sha256: Sha256V1

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        if _A_SHARE_SYMBOL_RE.fullmatch(value) is None:
            raise ValueError("symbol must be a recognized normalized A-share symbol")
        return value

    @model_validator(mode="after")
    def _validate_start(self) -> Self:
        _validate_json_schema_instance_v1(
            schema=self.plugin_manifest.config_schema,
            instance=self.plugin_config,
            contract_name="config schema",
        )
        if self.parent_quantity % self.volume_increment != 0:
            raise ValueError("parent_quantity must close under frozen volume_increment")
        if self.min_volume % self.volume_increment != 0:
            raise ValueError("min_volume must close under frozen volume_increment")
        if self.plugin_manifest.manifest_sha256 != self.deterministic_context.plugin_manifest_sha256:
            raise ValueError("manifest hash conflicts with deterministic context")
        if self.runtime_id != self.deterministic_context.runtime_id:
            raise ValueError("runtime_id conflicts with deterministic context")
        if self.algo_instance_id != self.deterministic_context.algo_instance_id:
            raise ValueError("algo_instance_id conflicts with deterministic context")
        if self.start_event_id != self.deterministic_context.event_id:
            raise ValueError("start_event_id conflicts with deterministic context")
        if self.start_delivery_id != self.deterministic_context.delivery_id:
            raise ValueError("start_delivery_id conflicts with deterministic context")
        expected_algo_instance_id = _algo_instance_id_v2(
            runtime_id=self.runtime_id,
            parent_intent_id=self.parent_intent_id,
            strategy_slot_id=self.strategy_slot_id,
            algo_code=self.plugin_manifest.algo_code,
            plugin_id=self.plugin_manifest.plugin_id,
            plugin_version=self.plugin_manifest.plugin_version,
            plugin_manifest_sha256=self.plugin_manifest.manifest_sha256,
            plugin_config_sha256=self.plugin_config_sha256,
        )
        if self.algo_instance_id != expected_algo_instance_id:
            raise ValueError("algo_instance_id does not match frozen parent/plugin/config identity closure")
        if self.side not in self.plugin_manifest.supported_sides:
            raise ValueError("side is not supported by the frozen plugin manifest")
        projections = (
            ("miniqmt_plugin_config_v2", self.plugin_config, self.plugin_config_sha256),
            ("miniqmt_contract_projection_v1", self.contract_projection, self.contract_projection_sha256),
            ("miniqmt_account_projection_v1", self.account_projection, self.account_projection_sha256),
            (
                "miniqmt_market_capability_projection_v1",
                self.market_capability_projection,
                self.market_capability_projection_sha256,
            ),
        )
        for domain, projection, supplied_hash in projections:
            if hash_hex_v1(domain, thaw_json_v1(projection)) != supplied_hash:
                raise ValueError(f"{domain} hash does not match frozen projection")
        return self


class BrokerCommandV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_command_v2"]
    command_type: BrokerCommandTypeV2
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    transition_id: IdentityV1
    ordinal: NonNegativeIntV1
    local_vt_orderid: IdentityV1
    symbol: IdentityV1
    side: SideV1
    order_type: OrderTypeV1
    price_decimal: PositiveCanonicalDecimalV1
    quantity: PositiveIntV1
    owned_broker_order_id: IdentityV1 | None
    reason_code: IdentityV1
    metadata: FrozenJsonObjectFieldV1
    payload_sha256: Sha256V1
    command_id: IdentityV1

    @classmethod
    def create(
        cls,
        *,
        command_type: BrokerCommandTypeV2,
        runtime_id: str,
        algo_instance_id: str,
        parent_intent_id: str,
        transition_id: str,
        ordinal: int,
        local_vt_orderid: str | None,
        symbol: str,
        side: SideV1,
        order_type: OrderTypeV1,
        price_decimal: Any,
        quantity: int,
        owned_broker_order_id: str | None,
        reason_code: str,
        metadata: dict[str, Any],
    ) -> Self:
        normalized_price = canonical_decimal_string_v1(price_decimal, field_name="price_decimal", allow_zero=False)
        if command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            expected_local_vt_orderid = _submit_local_order_id_v1(
                runtime_id=runtime_id,
                algo_instance_id=algo_instance_id,
                parent_intent_id=parent_intent_id,
                transition_id=transition_id,
                ordinal=ordinal,
                symbol=symbol,
                side=side,
                order_type=order_type,
            )
            if local_vt_orderid is not None and local_vt_orderid != expected_local_vt_orderid:
                raise ValueError("SUBMIT_LIMIT local_vt_orderid conflicts with deterministic identity closure")
            normalized_local_vt_orderid = expected_local_vt_orderid
        else:
            normalized_local_vt_orderid = require_identity_v1(
                local_vt_orderid,
                field_name="local_vt_orderid",
            )
        payload = {
            "schema_version": "miniqmt_broker_command_v2",
            "command_type": command_type.value,
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "parent_intent_id": parent_intent_id,
            "transition_id": transition_id,
            "ordinal": ordinal,
            "local_vt_orderid": normalized_local_vt_orderid,
            "symbol": symbol,
            "side": side.value,
            "order_type": order_type.value,
            "price_decimal": normalized_price,
            "quantity": quantity,
            "owned_broker_order_id": owned_broker_order_id,
            "reason_code": reason_code,
            "metadata": metadata,
        }
        model_payload = {
            **payload,
            "command_type": command_type,
            "side": side,
            "order_type": order_type,
        }
        return cls(
            **model_payload,
            payload_sha256=hash_hex_v1("miniqmt_broker_command_payload_v2", payload),
            command_id=_broker_command_id_v2(payload),
        )

    def business_payload_v1(self) -> dict[str, Any]:
        return self.canonical_payload_v1(exclude={"payload_sha256", "command_id"})

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        if _A_SHARE_SYMBOL_RE.fullmatch(value) is None:
            raise ValueError("symbol must be a recognized normalized A-share symbol")
        return value

    @model_validator(mode="after")
    def _validate_command(self) -> Self:
        if self.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT and self.owned_broker_order_id is not None:
            raise ValueError("SUBMIT_LIMIT must not carry broker order ID")
        if self.command_type is BrokerCommandTypeV2.CANCEL_ORDER and self.owned_broker_order_id is None:
            raise ValueError("CANCEL_ORDER requires exact durable-owned broker order ID")
        if self.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            expected_local_vt_orderid = _submit_local_order_id_v1(
                runtime_id=self.runtime_id,
                algo_instance_id=self.algo_instance_id,
                parent_intent_id=self.parent_intent_id,
                transition_id=self.transition_id,
                ordinal=self.ordinal,
                symbol=self.symbol,
                side=self.side,
                order_type=self.order_type,
            )
            if self.local_vt_orderid != expected_local_vt_orderid:
                raise ValueError("SUBMIT_LIMIT local_vt_orderid does not match deterministic identity closure")
        expected = hash_hex_v1("miniqmt_broker_command_payload_v2", self.business_payload_v1())
        if self.payload_sha256 != expected:
            raise ValueError("payload_sha256 does not match broker command closure")
        expected_command_id = _broker_command_id_v2(self.business_payload_v1())
        if self.command_id != expected_command_id:
            raise ValueError("command_id does not match deterministic business payload closure")
        return self


class TimerMutationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_timer_mutation_v1"]
    mutation_type: TimerMutationTypeV1
    algo_instance_id: IdentityV1
    transition_id: IdentityV1
    ordinal: NonNegativeIntV1
    timer_name: IdentityV1
    schedule_epoch: IdentityV1
    due_at_exchange_utc: UtcDateTimeV1 | None
    catch_up_policy: IdentityV1
    payload: FrozenJsonObjectFieldV1
    payload_sha256: Sha256V1
    schedule_id: IdentityV1
    timer_occurrence_id: IdentityV1 | None

    @classmethod
    def create(
        cls,
        *,
        mutation_type: TimerMutationTypeV1,
        algo_instance_id: str,
        transition_id: str,
        ordinal: int,
        timer_name: str,
        schedule_epoch: str,
        due_at_exchange_utc: Any | None,
        catch_up_policy: str,
        payload: dict[str, Any],
    ) -> Self:
        schedule_id = _timer_schedule_id_v1(
            algo_instance_id=algo_instance_id,
            timer_name=timer_name,
            schedule_epoch=schedule_epoch,
        )
        normalized_due = (
            None
            if due_at_exchange_utc is None
            else canonical_utc_datetime_v1(due_at_exchange_utc, field_name="due_at_exchange_utc")
        )
        occurrence_id = (
            None
            if normalized_due is None
            else _timer_occurrence_id_v1(
                schedule_id=schedule_id,
                due_at_exchange_utc=normalized_due,
            )
        )
        return cls(
            schema_version="miniqmt_timer_mutation_v1",
            mutation_type=mutation_type,
            algo_instance_id=algo_instance_id,
            transition_id=transition_id,
            ordinal=ordinal,
            timer_name=timer_name,
            schedule_epoch=schedule_epoch,
            due_at_exchange_utc=normalized_due,
            catch_up_policy=catch_up_policy,
            payload=payload,
            payload_sha256=hash_hex_v1("miniqmt_timer_mutation_payload_v1", payload),
            schedule_id=schedule_id,
            timer_occurrence_id=occurrence_id,
        )

    def mutation_identity_v1(self) -> str:
        return _prefixed_identity_v1(
            prefix="mqtimermut_",
            domain="miniqmt_timer_mutation_identity_v1",
            payload=self.canonical_payload_v1(),
        )

    @model_validator(mode="after")
    def _validate_timer(self) -> Self:
        expected_schedule_id = _timer_schedule_id_v1(
            algo_instance_id=self.algo_instance_id,
            timer_name=self.timer_name,
            schedule_epoch=self.schedule_epoch,
        )
        if self.schedule_id != expected_schedule_id:
            raise ValueError("schedule_id does not match algo/timer/epoch identity closure")
        if self.mutation_type is TimerMutationTypeV1.CANCEL:
            if self.due_at_exchange_utc is not None or self.timer_occurrence_id is not None:
                raise ValueError("CANCEL timer mutation must not fabricate due/occurrence identity")
        elif self.due_at_exchange_utc is None or self.timer_occurrence_id is None:
            raise ValueError("UPSERT_ONE_SHOT requires due and timer occurrence identity")
        else:
            expected_occurrence_id = _timer_occurrence_id_v1(
                schedule_id=self.schedule_id,
                due_at_exchange_utc=self.due_at_exchange_utc,
            )
            if self.timer_occurrence_id != expected_occurrence_id:
                raise ValueError("timer_occurrence_id does not match schedule/due identity closure")
        expected = hash_hex_v1("miniqmt_timer_mutation_payload_v1", thaw_json_v1(self.payload))
        if self.payload_sha256 != expected:
            raise ValueError("payload_sha256 does not match timer payload closure")
        return self


class DiagnosticObservationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_diagnostic_observation_v1"]
    observation_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    transition_id: IdentityV1
    ordinal: NonNegativeIntV1
    severity: DiagnosticSeverityV1
    reason_code: IdentityV1
    message: IdentityV1
    context: FrozenJsonObjectFieldV1
    context_sha256: Sha256V1
    observed_at_logical_utc: UtcDateTimeV1

    @classmethod
    def create(
        cls,
        *,
        deterministic_context: DeterministicExecutionContextV1,
        transition_id: str,
        ordinal: int,
        severity: DiagnosticSeverityV1 | str,
        reason_code: str,
        message: str,
        context: dict[str, Any],
    ) -> Self:
        if not isinstance(deterministic_context, DeterministicExecutionContextV1):
            raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
        if type(severity) is str:
            severity = DiagnosticSeverityV1(severity)
        if not isinstance(severity, DiagnosticSeverityV1):
            raise TypeError("severity must be DiagnosticSeverityV1 or its exact value")
        context_sha256 = hash_hex_v1("miniqmt_diagnostic_context_v1", context)
        identity_payload = {
            "schema_version": "miniqmt_diagnostic_observation_v1",
            "runtime_id": deterministic_context.runtime_id,
            "algo_instance_id": deterministic_context.algo_instance_id,
            "event_id": deterministic_context.event_id,
            "transition_id": transition_id,
            "ordinal": ordinal,
            "severity": severity.value,
            "reason_code": reason_code,
            "message": message,
            "context": context,
            "context_sha256": context_sha256,
            "observed_at_logical_utc": deterministic_context.logical_time_utc,
        }
        observation = cls(
            **{**identity_payload, "severity": severity},
            observation_id=_diagnostic_observation_id_v1(identity_payload),
        )
        return observation.validate_against_context_v1(deterministic_context)

    def identity_payload_v1(self) -> dict[str, Any]:
        return self.canonical_payload_v1(exclude={"observation_id"})

    def validate_against_context_v1(self, context: DeterministicExecutionContextV1) -> Self:
        if not isinstance(context, DeterministicExecutionContextV1):
            raise TypeError("context must be DeterministicExecutionContextV1")
        if self.runtime_id != context.runtime_id:
            raise ValueError("diagnostic runtime_id conflicts with deterministic context")
        if self.algo_instance_id != context.algo_instance_id:
            raise ValueError("diagnostic algo_instance_id conflicts with deterministic context")
        if self.event_id != context.event_id:
            raise ValueError("diagnostic event_id conflicts with deterministic context")
        if self.observed_at_logical_utc != context.logical_time_utc:
            raise ValueError("diagnostic observed_at_logical_utc must equal deterministic context logical time")
        return self

    @model_validator(mode="after")
    def _validate_observation(self) -> Self:
        expected = hash_hex_v1("miniqmt_diagnostic_context_v1", thaw_json_v1(self.context))
        if self.context_sha256 != expected:
            raise ValueError("context_sha256 does not match diagnostic context closure")
        expected_observation_id = _diagnostic_observation_id_v1(self.identity_payload_v1())
        if self.observation_id != expected_observation_id:
            raise ValueError("observation_id does not match diagnostic identity closure")
        return self


class _AlgoEffectBundleV1(FrozenStrictModel):
    next_state: AlgoStateSnapshotV2
    broker_commands: tuple[BrokerCommandV2, ...]
    timer_mutations: tuple[TimerMutationV1, ...]
    diagnostic_observations: tuple[DiagnosticObservationV1, ...]
    terminal_outcome: TerminalOutcomeV1 | None
    effect_set_sha256: Sha256V1

    def effect_hash_payload_v1(self) -> dict[str, Any]:
        return {
            "next_state_sha256": self.next_state.state_sha256,
            "ordered_command_ids": [item.command_id for item in self.broker_commands],
            "ordered_timer_mutation_ids": [item.mutation_identity_v1() for item in self.timer_mutations],
            "ordered_diagnostic_observation_ids": [item.observation_id for item in self.diagnostic_observations],
            "terminal_outcome": self.terminal_outcome.value if self.terminal_outcome is not None else None,
        }

    @model_validator(mode="after")
    def _validate_effects(self) -> Self:
        for name, effects in (
            ("broker_commands", self.broker_commands),
            ("timer_mutations", self.timer_mutations),
            ("diagnostic_observations", self.diagnostic_observations),
        ):
            typed_ordinals = [item.ordinal for item in effects]
            if typed_ordinals != sorted(typed_ordinals):
                raise ValueError(f"{name} must preserve ascending effect ordinal order")
        all_effects: list[Any] = [*self.broker_commands, *self.timer_mutations, *self.diagnostic_observations]
        ordinals = [item.ordinal for item in all_effects]
        if len(ordinals) != len(set(ordinals)):
            raise ValueError("effect ordinals contain duplicate values")
        if sorted(ordinals) != list(range(len(ordinals))):
            raise ValueError("effect ordinals must be contiguous from zero without gaps")
        if any(item.algo_instance_id != self.next_state.algo_instance_id for item in all_effects):
            raise ValueError("effect algo identity conflicts with next state")
        transition_ids = {item.transition_id for item in all_effects}
        if len(transition_ids) > 1:
            raise ValueError("effect transition identities conflict")
        expected = hash_hex_v1("miniqmt_algo_effect_set_v1", self.effect_hash_payload_v1())
        if self.effect_set_sha256 != expected:
            raise ValueError("effect_set_sha256 does not match ordered effect closure")
        return self


class AlgoInitializationV1(_AlgoEffectBundleV1):
    schema_version: Literal["miniqmt_algo_initialization_v1"]
    start_event_id: IdentityV1
    start_delivery_id: IdentityV1

    @model_validator(mode="after")
    def _validate_initialization_identity(self) -> Self:
        if self.next_state.transition_sequence != 1 or self.next_state.last_applied_delivery_sequence != 1:
            raise ValueError("initialization next state must close transition/delivery sequence 1")
        if self.next_state.last_applied_event_id != self.start_event_id:
            raise ValueError("initialization start_event_id conflicts with next state")
        if self.next_state.last_applied_delivery_id != self.start_delivery_id:
            raise ValueError("initialization start_delivery_id conflicts with next state")
        return self


class AlgoTransitionV1(_AlgoEffectBundleV1):
    schema_version: Literal["miniqmt_algo_transition_v1"]


class ConsumedLineageTypeV1(StrEnum):
    EVENT = "EVENT"
    MARKET_DATA = "MARKET_DATA"
    ORDER = "ORDER"
    TRADE = "TRADE"
    ACCOUNT = "ACCOUNT"
    RECONCILIATION = "RECONCILIATION"
    OPERATOR = "OPERATOR"


class KernelProjectionTypeV1(StrEnum):
    CONTRACT = "CONTRACT"
    ACCOUNT = "ACCOUNT"
    MARKET_CAPABILITY = "MARKET_CAPABILITY"
    OMS_PREFLIGHT = "OMS_PREFLIGHT"
    RISK_DECISION = "RISK_DECISION"
    ROUTE_COMPATIBILITY = "ROUTE_COMPATIBILITY"
    KILL_SWITCH_STATE = "KILL_SWITCH_STATE"


class RiskDecisionStageV1(StrEnum):
    EVENT = "EVENT"
    PRE_SUBMIT = "PRE_SUBMIT"


class RiskDecisionActionV1(StrEnum):
    PASS = "PASS"
    KILL_SWITCH = "KILL_SWITCH"


class ConsumedLineageRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_consumed_lineage_ref_v1"]
    lineage_type: ConsumedLineageTypeV1
    identity: IdentityV1
    payload_sha256: Sha256V1
    lineage_ref_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        lineage_type: ConsumedLineageTypeV1 | str,
        identity: str,
        payload_sha256: str,
    ) -> Self:
        normalized_type = ConsumedLineageTypeV1(lineage_type)
        payload = {
            "schema_version": "miniqmt_consumed_lineage_ref_v1",
            "lineage_type": normalized_type.value,
            "identity": identity,
            "payload_sha256": payload_sha256,
        }
        return cls(
            schema_version="miniqmt_consumed_lineage_ref_v1",
            lineage_type=normalized_type,
            identity=identity,
            payload_sha256=payload_sha256,
            lineage_ref_sha256=hash_hex_v1("miniqmt_consumed_lineage_ref_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_lineage_ref(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_consumed_lineage_ref_v1",
            self.canonical_payload_v1(exclude={"lineage_ref_sha256"}),
        )
        if self.lineage_ref_sha256 != expected:
            raise ValueError("lineage_ref_sha256 does not match consumed lineage closure")
        return self


class ExecutionProjectionRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_execution_projection_ref_v1"]
    projection_type: KernelProjectionTypeV1
    projection_id: IdentityV1
    projection_version: IdentityV1
    payload_sha256: Sha256V1
    source_event_id: IdentityV1 | None
    logical_at_utc: UtcDateTimeV1
    ref_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        projection_type: KernelProjectionTypeV1,
        projection_id: str,
        projection_version: str,
        payload_sha256: str,
        source_event_id: str | None,
        logical_at_utc: Any,
    ) -> Self:
        normalized_time = canonical_utc_datetime_v1(logical_at_utc, field_name="logical_at_utc")
        payload = {
            "schema_version": "miniqmt_execution_projection_ref_v1",
            "projection_type": projection_type.value,
            "projection_id": projection_id,
            "projection_version": projection_version,
            "payload_sha256": payload_sha256,
            "source_event_id": source_event_id,
            "logical_at_utc": normalized_time,
        }
        return cls(
            schema_version="miniqmt_execution_projection_ref_v1",
            projection_type=projection_type,
            projection_id=projection_id,
            projection_version=projection_version,
            payload_sha256=payload_sha256,
            source_event_id=source_event_id,
            logical_at_utc=normalized_time,
            ref_sha256=hash_hex_v1("miniqmt_execution_projection_ref_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_projection_ref(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_execution_projection_ref_v1",
            self.canonical_payload_v1(exclude={"ref_sha256"}),
        )
        if self.ref_sha256 != expected:
            raise ValueError("ref_sha256 does not match execution projection closure")
        return self


class ExecutionProjectionSetV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_execution_projection_set_v1"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    ordered_projection_refs: tuple[ExecutionProjectionRefV1, ...]
    projection_set_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        algo_instance_id: str,
        event_id: str,
        delivery_id: str,
        projection_refs: tuple[ExecutionProjectionRefV1, ...],
    ) -> Self:
        payload = {
            "schema_version": "miniqmt_execution_projection_set_v1",
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "event_id": event_id,
            "delivery_id": delivery_id,
            "ordered_projection_refs": [item.model_dump(mode="json") for item in projection_refs],
        }
        return cls(
            schema_version="miniqmt_execution_projection_set_v1",
            runtime_id=runtime_id,
            algo_instance_id=algo_instance_id,
            event_id=event_id,
            delivery_id=delivery_id,
            ordered_projection_refs=projection_refs,
            projection_set_sha256=hash_hex_v1("miniqmt_execution_projection_set_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_projection_set(self) -> Self:
        keys = [(item.projection_type.value, item.projection_id) for item in self.ordered_projection_refs]
        if keys != sorted(keys):
            raise ValueError("ordered_projection_refs must be sorted by projection_type and projection_id")
        projection_types = [item.projection_type for item in self.ordered_projection_refs]
        if len(projection_types) != len(set(projection_types)):
            raise ValueError("ordered_projection_refs contain duplicate projection_type authority")
        expected = hash_hex_v1(
            "miniqmt_execution_projection_set_v1",
            self.canonical_payload_v1(exclude={"projection_set_sha256"}),
        )
        if self.projection_set_sha256 != expected:
            raise ValueError("projection_set_sha256 does not match execution projection set closure")
        return self


class MiniQMTRiskDecisionReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_risk_decision_receipt_v1"]
    decision_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    child_order_id: IdentityV1 | None
    decision_stage: RiskDecisionStageV1
    action: RiskDecisionActionV1
    reason_code: IdentityV1
    reason: IdentityV1
    metadata: FrozenJsonObjectFieldV1
    metadata_sha256: Sha256V1
    logical_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        algo_instance_id: str,
        event_id: str,
        child_order_id: str | None,
        decision_stage: RiskDecisionStageV1 | str,
        action: RiskDecisionActionV1 | str,
        reason_code: str,
        reason: str,
        metadata: dict[str, Any],
        logical_at_utc: Any,
    ) -> Self:
        normalized_stage = RiskDecisionStageV1(decision_stage)
        normalized_action = RiskDecisionActionV1(action)
        normalized_metadata = thaw_json_v1(_freeze_json_object_field(metadata))
        normalized_time = canonical_utc_datetime_v1(logical_at_utc, field_name="logical_at_utc")
        decision_identity_payload = {
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "event_id": event_id,
            "child_order_id": child_order_id,
            "decision_stage": normalized_stage.value,
        }
        decision_id = "mqriskdecision_" + hash_hex_v1(
            "miniqmt_risk_decision_identity_v1",
            decision_identity_payload,
        )
        payload = {
            "schema_version": "miniqmt_risk_decision_receipt_v1",
            "decision_id": decision_id,
            **decision_identity_payload,
            "action": normalized_action.value,
            "reason_code": reason_code,
            "reason": reason,
            "metadata": normalized_metadata,
            "metadata_sha256": hash_hex_v1("miniqmt_risk_decision_metadata_v1", normalized_metadata),
            "logical_at_utc": normalized_time,
        }
        return cls(
            schema_version="miniqmt_risk_decision_receipt_v1",
            decision_id=decision_id,
            runtime_id=runtime_id,
            algo_instance_id=algo_instance_id,
            event_id=event_id,
            child_order_id=child_order_id,
            decision_stage=normalized_stage,
            action=normalized_action,
            reason_code=reason_code,
            reason=reason,
            metadata=normalized_metadata,
            metadata_sha256=payload["metadata_sha256"],
            logical_at_utc=normalized_time,
            receipt_sha256=hash_hex_v1("miniqmt_risk_decision_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_risk_decision(self) -> Self:
        identity_payload = {
            "runtime_id": self.runtime_id,
            "algo_instance_id": self.algo_instance_id,
            "event_id": self.event_id,
            "child_order_id": self.child_order_id,
            "decision_stage": self.decision_stage.value,
        }
        expected_id = "mqriskdecision_" + hash_hex_v1(
            "miniqmt_risk_decision_identity_v1",
            identity_payload,
        )
        if self.decision_id != expected_id:
            raise ValueError("decision_id does not match risk decision identity closure")
        metadata_payload = thaw_json_v1(self.metadata)
        if self.metadata_sha256 != hash_hex_v1("miniqmt_risk_decision_metadata_v1", metadata_payload):
            raise ValueError("metadata_sha256 does not match risk decision metadata closure")
        expected_receipt = hash_hex_v1(
            "miniqmt_risk_decision_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected_receipt:
            raise ValueError("receipt_sha256 does not match risk decision receipt closure")
        return self


class RuntimeEventIngressReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_runtime_event_ingress_receipt_v1"]
    ingress_receipt_id: IdentityV1
    runtime_id: IdentityV1
    event_id: IdentityV1
    event_key_sha256: Sha256V1
    runtime_sequence: PositiveIntV1
    routing_rule_version: Literal["miniqmt_event_routing_v1"]
    ordered_target_algo_instance_ids: tuple[IdentityV1, ...]
    ordered_delivery_ids: tuple[IdentityV1, ...]
    delivery_set_sha256: Sha256V1
    transaction_commit_identity: IdentityV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        event_id: str,
        event_key_sha256: str,
        runtime_sequence: int,
        ordered_target_algo_instance_ids: tuple[str, ...],
        ordered_delivery_ids: tuple[str, ...],
        transaction_commit_identity: str,
    ) -> Self:
        routing_rule_version = "miniqmt_event_routing_v1"
        ingress_receipt_id = "mqingress_" + hash_hex_v1(
            "miniqmt_runtime_event_ingress_identity_v1",
            {
                "runtime_id": runtime_id,
                "event_id": event_id,
                "runtime_sequence": runtime_sequence,
                "routing_rule_version": routing_rule_version,
            },
        )
        delivery_payload = {
            "event_id": event_id,
            "routing_rule_version": routing_rule_version,
            "ordered_target_algo_instance_ids": list(ordered_target_algo_instance_ids),
            "ordered_delivery_ids": list(ordered_delivery_ids),
        }
        receipt_payload = {
            "schema_version": "miniqmt_runtime_event_ingress_receipt_v1",
            "ingress_receipt_id": ingress_receipt_id,
            "runtime_id": runtime_id,
            "event_id": event_id,
            "event_key_sha256": event_key_sha256,
            "runtime_sequence": runtime_sequence,
            "routing_rule_version": routing_rule_version,
            "ordered_target_algo_instance_ids": list(ordered_target_algo_instance_ids),
            "ordered_delivery_ids": list(ordered_delivery_ids),
            "delivery_set_sha256": hash_hex_v1("miniqmt_event_delivery_set_v1", delivery_payload),
            "transaction_commit_identity": transaction_commit_identity,
        }
        return cls(
            schema_version="miniqmt_runtime_event_ingress_receipt_v1",
            ingress_receipt_id=ingress_receipt_id,
            runtime_id=runtime_id,
            event_id=event_id,
            event_key_sha256=event_key_sha256,
            runtime_sequence=runtime_sequence,
            routing_rule_version=routing_rule_version,
            ordered_target_algo_instance_ids=ordered_target_algo_instance_ids,
            ordered_delivery_ids=ordered_delivery_ids,
            delivery_set_sha256=receipt_payload["delivery_set_sha256"],
            transaction_commit_identity=transaction_commit_identity,
            receipt_sha256=hash_hex_v1("miniqmt_runtime_event_ingress_receipt_v1", receipt_payload),
        )

    @model_validator(mode="after")
    def _validate_ingress_receipt(self) -> Self:
        targets = list(self.ordered_target_algo_instance_ids)
        deliveries = list(self.ordered_delivery_ids)
        if len(targets) != len(deliveries):
            raise ValueError("ingress target and delivery cardinality must match")
        if len(targets) != len(set(targets)) or len(deliveries) != len(set(deliveries)):
            raise ValueError("ingress target or delivery identities contain duplicate values")
        if targets != sorted(targets):
            raise ValueError("ordered_target_algo_instance_ids must be sorted")
        expected_id = "mqingress_" + hash_hex_v1(
            "miniqmt_runtime_event_ingress_identity_v1",
            {
                "runtime_id": self.runtime_id,
                "event_id": self.event_id,
                "runtime_sequence": self.runtime_sequence,
                "routing_rule_version": self.routing_rule_version,
            },
        )
        if self.ingress_receipt_id != expected_id:
            raise ValueError("ingress_receipt_id does not match event ingress identity closure")
        expected_set = hash_hex_v1(
            "miniqmt_event_delivery_set_v1",
            {
                "event_id": self.event_id,
                "routing_rule_version": self.routing_rule_version,
                "ordered_target_algo_instance_ids": targets,
                "ordered_delivery_ids": deliveries,
            },
        )
        if self.delivery_set_sha256 != expected_set:
            raise ValueError("delivery_set_sha256 does not match event delivery closure")
        expected_receipt = hash_hex_v1(
            "miniqmt_runtime_event_ingress_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected_receipt:
            raise ValueError("receipt_sha256 does not match event ingress receipt closure")
        return self


def _qualified_exception_type_v1(error: BaseException) -> str:
    error_type = type(error)
    return f"{error_type.__module__}.{error_type.__qualname__}"


def _safe_error_object_v1(value: Any) -> dict[str, Any]:
    rendered = json_safe_evidence_v1(value)
    if isinstance(rendered, dict):
        return rendered
    return {"value": rendered}


class KernelErrorEvidenceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_kernel_error_evidence_v1"]
    stage: IdentityV1
    stable_reason_code: IdentityV1
    exception_type: IdentityV1
    message: IdentityV1
    retryable: StrictBool
    terminal: StrictBool
    broker_called: StrictBool | None
    primary_context: FrozenJsonObjectFieldV1
    bounded_secondary_errors: tuple[FrozenJsonObjectFieldV1, ...]
    context_sha256: Sha256V1
    evidence_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        stage: str,
        stable_reason_code: str,
        exception: BaseException,
        message: str,
        retryable: bool,
        terminal: bool,
        broker_called: bool | None,
        primary_context: dict[str, Any],
        secondary_errors: list[Any],
    ) -> Self:
        if retryable and terminal:
            raise ValueError("kernel error evidence cannot be retryable and terminal")
        safe_primary = _safe_error_object_v1(primary_context)
        renderer_secondary: list[dict[str, Any]] = []
        try:
            str(exception)
        except Exception as render_error:
            renderer_secondary.append(
                {
                    "reason_code": "MINIQMT_KERNEL_EXCEPTION_RENDER_FAILED",
                    "primary_exception_type": _qualified_exception_type_v1(exception),
                    "renderer_exception_type": _qualified_exception_type_v1(render_error),
                    "renderer_error": _safe_error_object_v1(render_error),
                }
            )
        safe_secondary = [*renderer_secondary, *[_safe_error_object_v1(item) for item in secondary_errors]]
        if len(safe_secondary) > 16:
            omitted = safe_secondary[15:]
            safe_secondary = [
                *safe_secondary[:15],
                {
                    "reason_code": "MINIQMT_KERNEL_SECONDARY_ERRORS_TRUNCATED",
                    "omitted_count": len(omitted),
                    "omitted_set_sha256": hash_hex_v1("miniqmt_kernel_error_omitted_set_v1", omitted),
                },
            ]
        context_payload = {
            "primary_context": safe_primary,
            "bounded_secondary_errors": safe_secondary,
        }
        payload = {
            "schema_version": "miniqmt_kernel_error_evidence_v1",
            "stage": stage,
            "stable_reason_code": stable_reason_code,
            "exception_type": _qualified_exception_type_v1(exception),
            "message": message,
            "retryable": retryable,
            "terminal": terminal,
            "broker_called": broker_called,
            **context_payload,
            "context_sha256": hash_hex_v1("miniqmt_kernel_error_context_v1", context_payload),
        }
        return cls(
            schema_version="miniqmt_kernel_error_evidence_v1",
            stage=stage,
            stable_reason_code=stable_reason_code,
            exception_type=payload["exception_type"],
            message=message,
            retryable=retryable,
            terminal=terminal,
            broker_called=broker_called,
            primary_context=safe_primary,
            bounded_secondary_errors=tuple(safe_secondary),
            context_sha256=payload["context_sha256"],
            evidence_sha256=hash_hex_v1("miniqmt_kernel_error_evidence_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_kernel_error_evidence(self) -> Self:
        if self.retryable and self.terminal:
            raise ValueError("kernel error evidence cannot be retryable and terminal")
        if len(self.bounded_secondary_errors) > 16:
            raise ValueError("bounded_secondary_errors exceeds the retained evidence limit")
        context_payload = {
            "primary_context": thaw_json_v1(self.primary_context),
            "bounded_secondary_errors": [thaw_json_v1(item) for item in self.bounded_secondary_errors],
        }
        expected_context = hash_hex_v1("miniqmt_kernel_error_context_v1", context_payload)
        if self.context_sha256 != expected_context:
            raise ValueError("context_sha256 does not match kernel error context closure")
        expected_evidence = hash_hex_v1(
            "miniqmt_kernel_error_evidence_v1",
            self.canonical_payload_v1(exclude={"evidence_sha256"}),
        )
        if self.evidence_sha256 != expected_evidence:
            raise ValueError("evidence_sha256 does not match kernel error evidence closure")
        return self


def _require_unique_identities_v1(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(require_identity_v1(value, field_name=field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} contains duplicate identities")
    return normalized


def _require_unique_hashes_v1(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(require_sha256_v1(value, field_name=field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} contains duplicate hashes")
    return normalized


def _canonical_constructor_payload_v1(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {key: _canonical_constructor_payload_v1(member) for key, member in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_constructor_payload_v1(member) for member in value]
    return value


def _bounded_context_v1(context: dict[str, Any]) -> dict[str, Any]:
    safe = _safe_error_object_v1(context)
    flattened: list[dict[str, Any]] = []

    def visit(value: Any, path: str) -> None:
        if isinstance(value, dict):
            if not value:
                flattened.append({"path": path, "value": {}})
                return
            for key in sorted(value):
                visit(value[key], f"{path}/{key.replace('~', '~0').replace('/', '~1')}")
            return
        if isinstance(value, list):
            if not value:
                flattened.append({"path": path, "value": []})
                return
            for index, item in enumerate(value):
                visit(item, f"{path}/{index}")
            return
        flattened.append({"path": path, "value": value})

    visit(safe, "$")
    flattened.sort(key=lambda item: item["path"])
    if len(flattened) <= 32:
        return {"items": flattened}
    omitted = flattened[31:]
    return {
        "items": [
            *flattened[:31],
            {
                "path": "$__truncated__",
                "value": {
                    "omitted_count": len(omitted),
                    "omitted_set_sha256": hash_hex_v1("miniqmt_algo_failure_omitted_set_v1", omitted),
                },
            },
        ]
    }


def transaction_commit_identity_v1(
    *,
    operation: str,
    owner_identities: tuple[str, ...],
    input_hashes: tuple[str, ...],
    output_identities: tuple[str, ...],
) -> str:
    payload = {
        "operation": require_identity_v1(operation, field_name="operation"),
        "owner_identities": list(_require_unique_identities_v1(owner_identities, field_name="owner_identities")),
        "input_hashes": list(_require_unique_hashes_v1(input_hashes, field_name="input_hashes")),
        "ordered_output_identities": list(
            _require_unique_identities_v1(output_identities, field_name="output_identities")
        ),
    }
    return "mqtx_" + hash_hex_v1("miniqmt_kernel_transaction_v1", payload)


class ExecutionAlgoPersistenceStatusV2(StrEnum):
    INITIALIZING = "INITIALIZING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    EXPIRED_WITH_RESIDUAL = "EXPIRED_WITH_RESIDUAL"


class ActiveChildClosureStatusV1(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    CLEAN = "CLEAN"
    CANCEL_PENDING = "CANCEL_PENDING"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class ExecutionAlgoInstancePersistenceV2(FrozenStrictModel):
    kernel_contract_version: Literal["KERNEL_V2"]
    algo_instance_id: IdentityV1
    runtime_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    symbol: IdentityV1
    side: SideV1
    target_quantity: PositiveIntV1
    traded_quantity: NonNegativeIntV1
    remaining_quantity: NonNegativeIntV1
    algo_code: IdentityV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    plugin_config_json: FrozenJsonObjectFieldV1
    plugin_config_sha256: Sha256V1
    compatibility_receipt_sha256: Sha256V1
    state_schema_version: IdentityV1 | None
    state_json: FrozenJsonObjectFieldV1 | None
    state_sha256: Sha256V1 | None
    transition_sequence: NonNegativeIntV1
    last_applied_delivery_sequence: NonNegativeIntV1
    last_applied_delivery_id: IdentityV1 | None
    last_closed_delivery_sequence: NonNegativeIntV1
    terminal_delivery_sequence: PositiveIntV1 | None
    status: ExecutionAlgoPersistenceStatusV2
    failure_receipt_id: IdentityV1 | None
    active_child_closure_status: ActiveChildClosureStatusV1
    active_child_count: NonNegativeIntV1
    row_version: PositiveIntV1
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    terminal_at_utc: UtcDateTimeV1 | None
    archived_at_utc: UtcDateTimeV1 | None

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "kernel_contract_version": "KERNEL_V2",
            **values,
            "created_at_utc": canonical_utc_datetime_v1(values["created_at_utc"], field_name="created_at_utc"),
            "updated_at_utc": canonical_utc_datetime_v1(values["updated_at_utc"], field_name="updated_at_utc"),
            "terminal_at_utc": None
            if values.get("terminal_at_utc") is None
            else canonical_utc_datetime_v1(values["terminal_at_utc"], field_name="terminal_at_utc"),
            "archived_at_utc": None
            if values.get("archived_at_utc") is None
            else canonical_utc_datetime_v1(values["archived_at_utc"], field_name="archived_at_utc"),
        }
        return cls(**payload)

    def immutable_business_payload_v1(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in self.canonical_payload_v1().items()
            if key
            in {
                "kernel_contract_version",
                "algo_instance_id",
                "runtime_id",
                "parent_intent_id",
                "strategy_slot_id",
                "symbol",
                "side",
                "target_quantity",
                "algo_code",
                "plugin_id",
                "plugin_version",
                "plugin_manifest_sha256",
                "plugin_config_json",
                "plugin_config_sha256",
                "compatibility_receipt_sha256",
                "created_at_utc",
            }
        }

    def validate_successor_v1(self, previous: "ExecutionAlgoInstancePersistenceV2") -> Self:
        if not isinstance(previous, ExecutionAlgoInstancePersistenceV2):
            raise TypeError("previous must be ExecutionAlgoInstancePersistenceV2")
        if self.algo_instance_id != previous.algo_instance_id:
            raise ValueError("algo successor identity conflicts with previous row")
        if self.immutable_business_payload_v1() != previous.immutable_business_payload_v1():
            raise ValueError("algo immutable business payload changed")
        if self.row_version != previous.row_version + 1:
            raise ValueError("algo row_version must increment exactly once")
        allowed = {
            ExecutionAlgoPersistenceStatusV2.INITIALIZING: {
                ExecutionAlgoPersistenceStatusV2.ACTIVE,
                ExecutionAlgoPersistenceStatusV2.FAILED,
            },
            ExecutionAlgoPersistenceStatusV2.ACTIVE: {
                ExecutionAlgoPersistenceStatusV2.ACTIVE,
                ExecutionAlgoPersistenceStatusV2.PAUSED,
                ExecutionAlgoPersistenceStatusV2.COMPLETED,
                ExecutionAlgoPersistenceStatusV2.CANCELLED,
                ExecutionAlgoPersistenceStatusV2.FAILED,
                ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
            },
            ExecutionAlgoPersistenceStatusV2.PAUSED: {
                ExecutionAlgoPersistenceStatusV2.PAUSED,
                ExecutionAlgoPersistenceStatusV2.ACTIVE,
                ExecutionAlgoPersistenceStatusV2.CANCELLED,
                ExecutionAlgoPersistenceStatusV2.FAILED,
                ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
            },
            ExecutionAlgoPersistenceStatusV2.FAILED: {ExecutionAlgoPersistenceStatusV2.FAILED},
            ExecutionAlgoPersistenceStatusV2.COMPLETED: {ExecutionAlgoPersistenceStatusV2.COMPLETED},
            ExecutionAlgoPersistenceStatusV2.CANCELLED: {ExecutionAlgoPersistenceStatusV2.CANCELLED},
            ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL: {
                ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL
            },
        }
        if self.status not in allowed[previous.status]:
            raise ValueError("illegal algo status transition")
        if self.transition_sequence < previous.transition_sequence:
            raise ValueError("transition_sequence cannot decrease")
        if self.last_closed_delivery_sequence < previous.last_closed_delivery_sequence:
            raise ValueError("last_closed_delivery_sequence cannot decrease")
        return self

    @model_validator(mode="after")
    def _validate_persistence(self) -> Self:
        if _A_SHARE_SYMBOL_RE.fullmatch(self.symbol) is None:
            raise ValueError("symbol must be a recognized normalized A-share symbol")
        expected_id = _algo_instance_id_v2(
            runtime_id=self.runtime_id,
            parent_intent_id=self.parent_intent_id,
            strategy_slot_id=self.strategy_slot_id,
            algo_code=self.algo_code,
            plugin_id=self.plugin_id,
            plugin_version=self.plugin_version,
            plugin_manifest_sha256=self.plugin_manifest_sha256,
            plugin_config_sha256=self.plugin_config_sha256,
        )
        if self.algo_instance_id != expected_id:
            raise ValueError("algo_instance_id does not match complete source identity closure")
        if self.plugin_config_sha256 != hash_hex_v1("miniqmt_plugin_config_v2", thaw_json_v1(self.plugin_config_json)):
            raise ValueError("plugin_config_sha256 does not match frozen config")
        if self.traded_quantity + self.remaining_quantity != self.target_quantity:
            raise ValueError("traded_quantity + remaining_quantity must equal target_quantity")
        state_fields = (self.state_schema_version, self.state_json, self.state_sha256)
        if any(value is None for value in state_fields) != all(value is None for value in state_fields):
            raise ValueError("state schema, JSON and hash must be present together")
        if self.state_json is not None and self.state_sha256 != hash_hex_v1(
            "execution_algo_state_v2", thaw_json_v1(self.state_json)
        ):
            raise ValueError("state_sha256 does not match frozen state")
        if self.status is ExecutionAlgoPersistenceStatusV2.FAILED:
            if self.transition_sequence not in {
                self.last_applied_delivery_sequence,
                self.last_applied_delivery_sequence + 1,
            }:
                raise ValueError("FAILED transition sequence must close the last applied or failing delivery")
        elif self.transition_sequence != self.last_applied_delivery_sequence:
            raise ValueError("non-failed transition_sequence must equal last_applied_delivery_sequence")
        if (self.last_applied_delivery_sequence == 0) != (self.last_applied_delivery_id is None):
            raise ValueError("last applied delivery identity must match its sequence")
        if self.last_closed_delivery_sequence < self.last_applied_delivery_sequence:
            raise ValueError("last_closed_delivery_sequence cannot trail applied delivery sequence")
        terminal_statuses = {
            ExecutionAlgoPersistenceStatusV2.COMPLETED,
            ExecutionAlgoPersistenceStatusV2.CANCELLED,
            ExecutionAlgoPersistenceStatusV2.FAILED,
            ExecutionAlgoPersistenceStatusV2.EXPIRED_WITH_RESIDUAL,
        }
        if self.status in terminal_statuses:
            if self.terminal_delivery_sequence is None or self.terminal_at_utc is None:
                raise ValueError("terminal algo requires terminal delivery sequence and timestamp")
        elif self.terminal_delivery_sequence is not None or self.terminal_at_utc is not None:
            raise ValueError("non-terminal algo cannot carry terminal closure")
        if self.status is ExecutionAlgoPersistenceStatusV2.FAILED:
            if self.failure_receipt_id is None:
                raise ValueError("FAILED algo requires failure receipt")
            if self.active_child_closure_status is ActiveChildClosureStatusV1.NOT_APPLICABLE:
                raise ValueError("FAILED algo requires active-child closure diagnosis")
        else:
            if self.failure_receipt_id is not None:
                raise ValueError("only FAILED algo may carry failure receipt")
            expected_closure = (
                ActiveChildClosureStatusV1.CLEAN
                if self.status in terminal_statuses
                else ActiveChildClosureStatusV1.NOT_APPLICABLE
            )
            if self.active_child_closure_status is not expected_closure:
                raise ValueError("active-child closure conflicts with algo status")
        if self.active_child_closure_status is ActiveChildClosureStatusV1.CLEAN and self.active_child_count != 0:
            raise ValueError("clean closure requires zero active children")
        if (
            self.active_child_closure_status
            in {
                ActiveChildClosureStatusV1.CANCEL_PENDING,
                ActiveChildClosureStatusV1.OUTCOME_UNKNOWN,
            }
            and self.active_child_count == 0
        ):
            raise ValueError("pending/unknown closure requires active children")
        if self.status is ExecutionAlgoPersistenceStatusV2.INITIALIZING:
            raise ValueError("INITIALIZING cannot be committed as an independently visible K2 row")
        if self.state_json is None and not (
            self.status is ExecutionAlgoPersistenceStatusV2.FAILED and self.transition_sequence == 0
        ):
            raise ValueError("only deterministic initialization failure may omit strict state")
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        if self.archived_at_utc is not None and self.terminal_at_utc is None:
            raise ValueError("archived algo must already be terminal")
        return self


class AlgoDeliveryPersistenceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_delivery_persistence_v1"]
    delivery_id: IdentityV1
    event_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    plugin_manifest_sha256: Sha256V1
    algo_delivery_sequence: PositiveIntV1
    previous_delivery_id: IdentityV1 | None
    status: DeliveryStatusV1
    attempt_count: NonNegativeIntV1
    lease_owner: IdentityV1 | None
    lease_expires_at: UtcDateTimeV1 | None
    transition_id: IdentityV1 | None
    last_error_json: FrozenJsonObjectFieldV1 | None
    lease_epoch: NonNegativeIntV1
    lease_fence_token: IdentityV1 | None
    row_version: PositiveIntV1
    next_attempt_at_utc: UtcDateTimeV1 | None
    failure_receipt_id: IdentityV1 | None
    skip_receipt_id: IdentityV1 | None
    closed_at_utc: UtcDateTimeV1 | None
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1

    @classmethod
    def create(cls, *, delivery: AlgoEventDeliveryV1, **values: Any) -> Self:
        if not isinstance(delivery, AlgoEventDeliveryV1):
            raise TypeError("delivery must be AlgoEventDeliveryV1")
        payload = delivery.canonical_payload_v1(exclude={"schema_version"})
        payload.update(values)
        payload["schema_version"] = "miniqmt_algo_delivery_persistence_v1"
        return cls(**{**payload, "status": delivery.status})

    def validate_successor_v1(self, previous: "AlgoDeliveryPersistenceV1") -> Self:
        if not isinstance(previous, AlgoDeliveryPersistenceV1):
            raise TypeError("previous must be AlgoDeliveryPersistenceV1")
        immutable = {
            "delivery_id",
            "event_id",
            "runtime_id",
            "algo_instance_id",
            "plugin_manifest_sha256",
            "algo_delivery_sequence",
            "previous_delivery_id",
            "created_at_utc",
        }
        if any(getattr(self, name) != getattr(previous, name) for name in immutable):
            raise ValueError("delivery immutable business payload changed")
        if self.row_version != previous.row_version + 1:
            raise ValueError("delivery row_version must increment exactly once")
        allowed = {
            DeliveryStatusV1.PENDING: {DeliveryStatusV1.CLAIMED},
            DeliveryStatusV1.CLAIMED: {
                DeliveryStatusV1.APPLIED,
                DeliveryStatusV1.FAILED_RETRYABLE,
                DeliveryStatusV1.FAILED_TERMINAL,
                DeliveryStatusV1.SKIPPED_TERMINAL,
            },
            DeliveryStatusV1.FAILED_RETRYABLE: {DeliveryStatusV1.CLAIMED},
            DeliveryStatusV1.APPLIED: set(),
            DeliveryStatusV1.FAILED_TERMINAL: set(),
            DeliveryStatusV1.SKIPPED_TERMINAL: set(),
        }
        if self.status not in allowed[previous.status]:
            raise ValueError("illegal delivery status transition")
        if self.status is DeliveryStatusV1.CLAIMED:
            if self.lease_epoch != previous.lease_epoch + 1:
                raise ValueError("delivery lease_epoch must advance from its durable predecessor")
        elif self.lease_epoch != previous.lease_epoch:
            raise ValueError("delivery completion must preserve the claimed lease epoch")
        return self

    def validate_initial_v1(self) -> Self:
        if self.status is not DeliveryStatusV1.PENDING:
            raise ValueError("initial delivery must be PENDING")
        if self.attempt_count != 0 or self.lease_epoch != 0 or self.row_version != 1:
            raise ValueError("initial PENDING delivery must start at attempt=0, lease_epoch=0 and row_version=1")
        if any(
            value is not None
            for value in (
                self.lease_owner,
                self.lease_expires_at,
                self.lease_fence_token,
                self.transition_id,
                self.last_error_json,
                self.next_attempt_at_utc,
                self.failure_receipt_id,
                self.skip_receipt_id,
                self.closed_at_utc,
            )
        ):
            raise ValueError("initial PENDING delivery cannot carry lease or outcome history")
        if self.created_at_utc != self.updated_at_utc:
            raise ValueError("initial PENDING delivery timestamps must be equal")
        return self

    @model_validator(mode="after")
    def _validate_persistence(self) -> Self:
        expected_id = _delivery_id_v1(
            event_id=self.event_id,
            algo_instance_id=self.algo_instance_id,
            plugin_manifest_sha256=self.plugin_manifest_sha256,
        )
        if self.delivery_id != expected_id:
            raise ValueError("delivery_id does not match event/algo/manifest identity closure")
        if (self.algo_delivery_sequence == 1) != (self.previous_delivery_id is None):
            raise ValueError("delivery predecessor must match delivery sequence")
        lease_values = (self.lease_owner, self.lease_expires_at, self.lease_fence_token)
        if any(value is None for value in lease_values) != all(value is None for value in lease_values):
            raise ValueError("lease owner, expiry and fence must be present together")
        terminal = self.status in {
            DeliveryStatusV1.APPLIED,
            DeliveryStatusV1.FAILED_TERMINAL,
            DeliveryStatusV1.SKIPPED_TERMINAL,
        }
        if self.status is DeliveryStatusV1.PENDING:
            if any(
                value is not None
                for value in (
                    *lease_values,
                    self.transition_id,
                    self.last_error_json,
                    self.failure_receipt_id,
                    self.skip_receipt_id,
                    self.closed_at_utc,
                )
            ):
                raise ValueError("PENDING delivery cannot carry lease or outcome")
        elif self.status is DeliveryStatusV1.CLAIMED:
            if self.lease_owner is None or self.lease_epoch <= 0:
                raise ValueError("CLAIMED delivery requires durable lease and positive epoch")
        elif self.status is DeliveryStatusV1.APPLIED:
            if self.transition_id is None or any(
                value is not None for value in (self.last_error_json, self.failure_receipt_id, self.skip_receipt_id)
            ):
                raise ValueError("APPLIED delivery requires only transition outcome")
        elif self.status is DeliveryStatusV1.FAILED_RETRYABLE:
            if self.last_error_json is None or self.next_attempt_at_utc is None:
                raise ValueError("FAILED_RETRYABLE delivery requires error evidence and next attempt")
            if any(value is not None for value in (self.transition_id, self.failure_receipt_id, self.skip_receipt_id)):
                raise ValueError("retryable failure cannot commit state/effect or terminal receipt")
        elif self.status is DeliveryStatusV1.FAILED_TERMINAL:
            if self.failure_receipt_id is None or self.last_error_json is None:
                raise ValueError("FAILED_TERMINAL delivery requires failure receipt and error evidence")
        elif self.skip_receipt_id is None or self.transition_id is not None:
            raise ValueError("SKIPPED_TERMINAL delivery requires skip receipt without transition")
        if terminal:
            if self.closed_at_utc is None or any(value is not None for value in lease_values):
                raise ValueError("terminal delivery must close and clear its active lease")
        elif self.closed_at_utc is not None:
            raise ValueError("non-terminal delivery cannot carry closed_at_utc")
        _validate_kernel_lease_fence_v1(
            owner_type="DELIVERY",
            owner_id=self.delivery_id,
            lease_owner=self.lease_owner,
            lease_epoch=self.lease_epoch,
            lease_fence_token=self.lease_fence_token,
        )
        if self.last_error_json is not None:
            KernelErrorEvidenceV1.model_validate_json(
                json.dumps(thaw_json_v1(self.last_error_json), sort_keys=True, separators=(",", ":"))
            )
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        if self.status is DeliveryStatusV1.PENDING:
            self.validate_initial_v1()
        return self


class TransactionCommitIdentityV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_kernel_transaction_commit_identity_v1"]
    operation: IdentityV1
    owner_identities: tuple[IdentityV1, ...]
    input_hashes: tuple[Sha256V1, ...]
    ordered_output_identities: tuple[IdentityV1, ...]
    transaction_commit_identity: IdentityV1

    @classmethod
    def create(
        cls,
        *,
        operation: str,
        owner_identities: tuple[str, ...],
        input_hashes: tuple[str, ...],
        output_identities: tuple[str, ...],
    ) -> Self:
        return cls(
            schema_version="miniqmt_kernel_transaction_commit_identity_v1",
            operation=operation,
            owner_identities=owner_identities,
            input_hashes=input_hashes,
            ordered_output_identities=output_identities,
            transaction_commit_identity=transaction_commit_identity_v1(
                operation=operation,
                owner_identities=owner_identities,
                input_hashes=input_hashes,
                output_identities=output_identities,
            ),
        )

    @model_validator(mode="after")
    def _validate_identity(self) -> Self:
        _require_unique_identities_v1(self.owner_identities, field_name="owner_identities")
        _require_unique_hashes_v1(self.input_hashes, field_name="input_hashes")
        _require_unique_identities_v1(self.ordered_output_identities, field_name="ordered_output_identities")
        expected = transaction_commit_identity_v1(
            operation=self.operation,
            owner_identities=self.owner_identities,
            input_hashes=self.input_hashes,
            output_identities=self.ordered_output_identities,
        )
        if self.transaction_commit_identity != expected:
            raise ValueError("transaction_commit_identity does not match transaction closure")
        return self


class OMSPreflightDecisionV1(StrEnum):
    PASS = "PASS"
    REJECT = "REJECT"


class OMSPreflightProjectionReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_oms_preflight_projection_receipt_v1"]
    receipt_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    child_order_id: IdentityV1
    order_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    account_projection_sha256: Sha256V1
    cash_fact_sha256: Sha256V1
    lot_fact_sha256: Sha256V1
    open_order_fact_sha256: Sha256V1
    decision: OMSPreflightDecisionV1
    reason_code: IdentityV1
    logical_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        decision = OMSPreflightDecisionV1(values["decision"])
        normalized_time = canonical_utc_datetime_v1(values["logical_at_utc"], field_name="logical_at_utc")
        identity_keys = (
            "runtime_id",
            "algo_instance_id",
            "parent_intent_id",
            "child_order_id",
            "account_projection_sha256",
            "cash_fact_sha256",
            "lot_fact_sha256",
            "open_order_fact_sha256",
        )
        identity_payload = {key: values[key] for key in identity_keys}
        receipt_id = "mqomspreflight_" + hash_hex_v1("miniqmt_oms_preflight_identity_v1", identity_payload)
        payload = {
            "schema_version": "miniqmt_oms_preflight_projection_receipt_v1",
            "receipt_id": receipt_id,
            **{key: values[key] for key in values if key != "logical_at_utc"},
            "decision": decision.value,
            "logical_at_utc": normalized_time,
        }
        return cls(
            **{**payload, "decision": decision},
            receipt_sha256=hash_hex_v1("miniqmt_oms_preflight_projection_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        identity_payload = {
            key: getattr(self, key)
            for key in (
                "runtime_id",
                "algo_instance_id",
                "parent_intent_id",
                "child_order_id",
                "account_projection_sha256",
                "cash_fact_sha256",
                "lot_fact_sha256",
                "open_order_fact_sha256",
            )
        }
        if self.receipt_id != "mqomspreflight_" + hash_hex_v1("miniqmt_oms_preflight_identity_v1", identity_payload):
            raise ValueError("receipt_id does not match OMS preflight identity closure")
        expected = hash_hex_v1(
            "miniqmt_oms_preflight_projection_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match OMS preflight closure")
        return self


class AlgoTransitionReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_transition_receipt_v1"]
    transition_id: IdentityV1
    delivery_id: IdentityV1
    event_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    transition_sequence: PositiveIntV1
    before_state_sha256_or_INIT: IdentityV1
    after_state_sha256: Sha256V1
    ordered_command_ids: tuple[IdentityV1, ...]
    command_set_sha256: Sha256V1
    ordered_timer_mutation_ids: tuple[IdentityV1, ...]
    timer_set_sha256: Sha256V1
    ordered_diagnostic_observation_ids: tuple[IdentityV1, ...]
    diagnostic_set_sha256: Sha256V1
    ordered_consumed_lineage_refs: tuple[ConsumedLineageRefV1, ...]
    consumed_lineage_set_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    effect_set_sha256: Sha256V1
    terminal_outcome: TerminalOutcomeV1 | None
    logical_applied_at_utc: UtcDateTimeV1
    transaction_commit_identity: IdentityV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        identity_payload = {
            key: values[key]
            for key in ("delivery_id", "event_id", "runtime_id", "algo_instance_id", "transition_sequence")
        }
        transition_id = "mqtransition_" + hash_hex_v1("miniqmt_algo_transition_identity_v1", identity_payload)
        command_ids = _require_unique_identities_v1(
            tuple(values["ordered_command_ids"]), field_name="ordered_command_ids"
        )
        timer_ids = _require_unique_identities_v1(
            tuple(values["ordered_timer_mutation_ids"]), field_name="ordered_timer_mutation_ids"
        )
        diagnostic_ids = _require_unique_identities_v1(
            tuple(values["ordered_diagnostic_observation_ids"]), field_name="ordered_diagnostic_observation_ids"
        )
        lineage_refs = tuple(values["ordered_consumed_lineage_refs"])
        lineage_ids = [item.identity for item in lineage_refs]
        if len(lineage_ids) != len(set(lineage_ids)):
            raise ValueError("ordered_consumed_lineage_refs contain duplicate identity")
        normalized_time = canonical_utc_datetime_v1(
            values["logical_applied_at_utc"], field_name="logical_applied_at_utc"
        )
        payload = {
            "schema_version": "miniqmt_algo_transition_receipt_v1",
            "transition_id": transition_id,
            **{
                key: values[key] for key in values if not key.startswith("ordered_") and key != "logical_applied_at_utc"
            },
            "ordered_command_ids": list(command_ids),
            "command_set_sha256": hash_hex_v1(
                "miniqmt_transition_command_set_v1",
                {
                    "transition_id": transition_id,
                    "algo_instance_id": values["algo_instance_id"],
                    "ordered_command_ids": list(command_ids),
                },
            ),
            "ordered_timer_mutation_ids": list(timer_ids),
            "timer_set_sha256": hash_hex_v1(
                "miniqmt_transition_timer_set_v1",
                {
                    "transition_id": transition_id,
                    "algo_instance_id": values["algo_instance_id"],
                    "ordered_timer_mutation_ids": list(timer_ids),
                },
            ),
            "ordered_diagnostic_observation_ids": list(diagnostic_ids),
            "diagnostic_set_sha256": hash_hex_v1(
                "miniqmt_transition_diagnostic_set_v1",
                {
                    "transition_id": transition_id,
                    "algo_instance_id": values["algo_instance_id"],
                    "ordered_diagnostic_observation_ids": list(diagnostic_ids),
                },
            ),
            "ordered_consumed_lineage_refs": [item.model_dump(mode="json") for item in lineage_refs],
            "consumed_lineage_set_sha256": hash_hex_v1(
                "miniqmt_consumed_lineage_set_v1",
                {
                    "transition_id": transition_id,
                    "algo_instance_id": values["algo_instance_id"],
                    "ordered_consumed_lineage_refs": [item.model_dump(mode="json") for item in lineage_refs],
                },
            ),
            "logical_applied_at_utc": normalized_time,
        }
        return cls(
            **{
                **payload,
                "ordered_command_ids": command_ids,
                "ordered_timer_mutation_ids": timer_ids,
                "ordered_diagnostic_observation_ids": diagnostic_ids,
                "ordered_consumed_lineage_refs": lineage_refs,
            },
            receipt_sha256=hash_hex_v1("miniqmt_algo_transition_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        expected_id = "mqtransition_" + hash_hex_v1(
            "miniqmt_algo_transition_identity_v1",
            {
                "delivery_id": self.delivery_id,
                "event_id": self.event_id,
                "runtime_id": self.runtime_id,
                "algo_instance_id": self.algo_instance_id,
                "transition_sequence": self.transition_sequence,
            },
        )
        if self.transition_id != expected_id:
            raise ValueError("transition_id does not match transition identity closure")
        expected_receipt = hash_hex_v1(
            "miniqmt_algo_transition_receipt_v1", self.canonical_payload_v1(exclude={"receipt_sha256"})
        )
        if self.receipt_sha256 != expected_receipt:
            raise ValueError("receipt_sha256 does not match transition receipt closure")
        command_ids = _require_unique_identities_v1(self.ordered_command_ids, field_name="ordered_command_ids")
        timer_ids = _require_unique_identities_v1(
            self.ordered_timer_mutation_ids, field_name="ordered_timer_mutation_ids"
        )
        diagnostic_ids = _require_unique_identities_v1(
            self.ordered_diagnostic_observation_ids,
            field_name="ordered_diagnostic_observation_ids",
        )
        lineage_payload = [item.model_dump(mode="json") for item in self.ordered_consumed_lineage_refs]
        lineage_ids = [item.identity for item in self.ordered_consumed_lineage_refs]
        expected_set_hashes = (
            hash_hex_v1(
                "miniqmt_transition_command_set_v1",
                {
                    "transition_id": self.transition_id,
                    "algo_instance_id": self.algo_instance_id,
                    "ordered_command_ids": list(command_ids),
                },
            ),
            hash_hex_v1(
                "miniqmt_transition_timer_set_v1",
                {
                    "transition_id": self.transition_id,
                    "algo_instance_id": self.algo_instance_id,
                    "ordered_timer_mutation_ids": list(timer_ids),
                },
            ),
            hash_hex_v1(
                "miniqmt_transition_diagnostic_set_v1",
                {
                    "transition_id": self.transition_id,
                    "algo_instance_id": self.algo_instance_id,
                    "ordered_diagnostic_observation_ids": list(diagnostic_ids),
                },
            ),
            hash_hex_v1(
                "miniqmt_consumed_lineage_set_v1",
                {
                    "transition_id": self.transition_id,
                    "algo_instance_id": self.algo_instance_id,
                    "ordered_consumed_lineage_refs": lineage_payload,
                },
            ),
        )
        if len(lineage_ids) != len(set(lineage_ids)) or expected_set_hashes != (
            self.command_set_sha256,
            self.timer_set_sha256,
            self.diagnostic_set_sha256,
            self.consumed_lineage_set_sha256,
        ):
            raise ValueError("transition receipt set hashes do not match ordered effects")
        return self


class AlgoFailureReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_failure_receipt_v1"]
    failure_receipt_id: IdentityV1
    delivery_id: IdentityV1
    event_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    transition_sequence: PositiveIntV1
    stable_reason_code: IdentityV1
    exception_type: IdentityV1
    message: IdentityV1
    bounded_context: FrozenJsonObjectFieldV1
    context_sha256: Sha256V1
    last_good_state_sha256_or_ABSENT_INITIAL_STATE: IdentityV1
    ordered_cancel_command_ids: tuple[IdentityV1, ...]
    ordered_active_child_ids: tuple[IdentityV1, ...]
    active_child_closure_status: IdentityV1
    transaction_commit_identity: IdentityV1
    failure_receipt_sha256: Sha256V1

    @classmethod
    def create(cls, *, context: dict[str, Any], **values: Any) -> Self:
        bounded_context = _bounded_context_v1(context)
        cancel_ids = _require_unique_identities_v1(
            tuple(values["ordered_cancel_command_ids"]), field_name="ordered_cancel_command_ids"
        )
        active_child_ids = _require_unique_identities_v1(
            tuple(values["ordered_active_child_ids"]), field_name="ordered_active_child_ids"
        )
        if active_child_ids != tuple(sorted(active_child_ids)):
            raise ValueError("ordered_active_child_ids must be sorted")
        identity_payload = {
            "delivery_id": values["delivery_id"],
            "event_id": values["event_id"],
            "algo_instance_id": values["algo_instance_id"],
            "transition_sequence": values["transition_sequence"],
            "stable_reason_code": values["stable_reason_code"],
        }
        failure_receipt_id = "mqalgofailure_" + hash_hex_v1("miniqmt_algo_failure_identity_v1", identity_payload)
        payload = {
            "schema_version": "miniqmt_algo_failure_receipt_v1",
            "failure_receipt_id": failure_receipt_id,
            **{key: values[key] for key in values if not key.startswith("ordered_")},
            "bounded_context": bounded_context,
            "context_sha256": hash_hex_v1("miniqmt_algo_failure_context_v1", bounded_context),
            "ordered_cancel_command_ids": list(cancel_ids),
            "ordered_active_child_ids": list(active_child_ids),
        }
        return cls(
            **{
                **payload,
                "ordered_cancel_command_ids": cancel_ids,
                "ordered_active_child_ids": active_child_ids,
            },
            failure_receipt_sha256=hash_hex_v1("miniqmt_algo_failure_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        bounded_context = thaw_json_v1(self.bounded_context)
        if self.context_sha256 != hash_hex_v1("miniqmt_algo_failure_context_v1", bounded_context):
            raise ValueError("context_sha256 does not match failure context closure")
        expected_id = "mqalgofailure_" + hash_hex_v1(
            "miniqmt_algo_failure_identity_v1",
            {
                "delivery_id": self.delivery_id,
                "event_id": self.event_id,
                "algo_instance_id": self.algo_instance_id,
                "transition_sequence": self.transition_sequence,
                "stable_reason_code": self.stable_reason_code,
            },
        )
        if self.failure_receipt_id != expected_id:
            raise ValueError("failure_receipt_id does not match failure identity closure")
        expected = hash_hex_v1(
            "miniqmt_algo_failure_receipt_v1",
            self.canonical_payload_v1(exclude={"failure_receipt_sha256"}),
        )
        if self.failure_receipt_sha256 != expected:
            raise ValueError("failure_receipt_sha256 does not match failure receipt closure")
        return self


class AlgoSkipReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_algo_skip_receipt_v1"]
    skip_receipt_id: IdentityV1
    delivery_id: IdentityV1
    event_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    previous_delivery_id: IdentityV1
    terminal_failure_receipt_id: IdentityV1
    reason_code: Literal["MINIQMT_ALGO_ALREADY_TERMINAL"]
    logical_skipped_at_utc: UtcDateTimeV1
    transaction_commit_identity: IdentityV1
    skip_receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        normalized_time = canonical_utc_datetime_v1(
            values["logical_skipped_at_utc"], field_name="logical_skipped_at_utc"
        )
        skip_receipt_id = "mqalgoskip_" + hash_hex_v1(
            "miniqmt_algo_skip_identity_v1",
            {
                "delivery_id": values["delivery_id"],
                "event_id": values["event_id"],
                "algo_instance_id": values["algo_instance_id"],
                "terminal_failure_receipt_id": values["terminal_failure_receipt_id"],
            },
        )
        payload = {
            "schema_version": "miniqmt_algo_skip_receipt_v1",
            "skip_receipt_id": skip_receipt_id,
            **{key: values[key] for key in values if key != "logical_skipped_at_utc"},
            "reason_code": "MINIQMT_ALGO_ALREADY_TERMINAL",
            "logical_skipped_at_utc": normalized_time,
        }
        return cls(**payload, skip_receipt_sha256=hash_hex_v1("miniqmt_algo_skip_receipt_v1", payload))

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        expected_id = "mqalgoskip_" + hash_hex_v1(
            "miniqmt_algo_skip_identity_v1",
            {
                "delivery_id": self.delivery_id,
                "event_id": self.event_id,
                "algo_instance_id": self.algo_instance_id,
                "terminal_failure_receipt_id": self.terminal_failure_receipt_id,
            },
        )
        if self.skip_receipt_id != expected_id:
            raise ValueError("skip_receipt_id does not match skip identity closure")
        expected = hash_hex_v1(
            "miniqmt_algo_skip_receipt_v1", self.canonical_payload_v1(exclude={"skip_receipt_sha256"})
        )
        if self.skip_receipt_sha256 != expected:
            raise ValueError("skip_receipt_sha256 does not match skip receipt closure")
        return self


class KernelWorkerStartupReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_kernel_worker_startup_receipt_v1"]
    worker_id: IdentityV1
    process_role: IdentityV1
    incarnation_sequence: PositiveIntV1
    source_revision: IdentityV1
    process_incarnation_id: IdentityV1
    started_at_utc: UtcDateTimeV1
    startup_transaction_commit_identity: IdentityV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        process_incarnation_id = "mqinc_" + hash_hex_v1(
            "miniqmt_kernel_worker_incarnation_v1",
            {
                "worker_id": values["worker_id"],
                "process_role": values["process_role"],
                "incarnation_sequence": values["incarnation_sequence"],
                "source_revision": values["source_revision"],
            },
        )
        payload = {
            "schema_version": "miniqmt_kernel_worker_startup_receipt_v1",
            **values,
            "process_incarnation_id": process_incarnation_id,
            "started_at_utc": canonical_utc_datetime_v1(values["started_at_utc"], field_name="started_at_utc"),
        }
        return cls(**payload, receipt_sha256=hash_hex_v1("miniqmt_kernel_worker_startup_receipt_v1", payload))

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        expected_id = "mqinc_" + hash_hex_v1(
            "miniqmt_kernel_worker_incarnation_v1",
            {
                "worker_id": self.worker_id,
                "process_role": self.process_role,
                "incarnation_sequence": self.incarnation_sequence,
                "source_revision": self.source_revision,
            },
        )
        if self.process_incarnation_id != expected_id:
            raise ValueError("process_incarnation_id does not match durable DB incarnation closure")
        expected = hash_hex_v1(
            "miniqmt_kernel_worker_startup_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match worker startup closure")
        return self


class BrokerDispatchAttemptStageV1(StrEnum):
    CLAIMED = "CLAIMED"
    PRE_CALL = "PRE_CALL"
    DISPATCHING_COMMITTED = "DISPATCHING_COMMITTED"
    GATEWAY_RETURNED = "GATEWAY_RETURNED"
    CALLBACK_OBSERVED = "CALLBACK_OBSERVED"
    COMPLETION_COMMITTED = "COMPLETION_COMMITTED"
    RECONCILING = "RECONCILING"
    CLOSED = "CLOSED"


class BrokerDispatchAttemptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_dispatch_attempt_v1"]
    dispatch_attempt_id: IdentityV1
    command_id: IdentityV1
    attempt_count: PositiveIntV1
    lease_epoch: PositiveIntV1
    lease_fence_token: IdentityV1
    process_incarnation_id: IdentityV1
    stage: BrokerDispatchAttemptStageV1
    started_at_utc: UtcDateTimeV1
    finished_at_utc: UtcDateTimeV1 | None
    pre_call_complete: StrictBool
    broker_called: StrictBool | None
    outcome: IdentityV1 | None
    error_reason_code: IdentityV1 | None
    error_context_sha256: Sha256V1 | None
    authority_receipt_sha256: Sha256V1 | None
    attempt_receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        stage = BrokerDispatchAttemptStageV1(values["stage"])
        dispatch_attempt_id = "mqdispatch_" + hash_hex_v1(
            "miniqmt_command_dispatch_attempt_v1",
            {
                "command_id": values["command_id"],
                "attempt_count": values["attempt_count"],
                "lease_epoch": values["lease_epoch"],
                "lease_fence_token": values["lease_fence_token"],
            },
        )
        payload = {
            "schema_version": "miniqmt_broker_dispatch_attempt_v1",
            "dispatch_attempt_id": dispatch_attempt_id,
            **values,
            "stage": stage.value,
            "started_at_utc": canonical_utc_datetime_v1(values["started_at_utc"], field_name="started_at_utc"),
            "finished_at_utc": None
            if values.get("finished_at_utc") is None
            else canonical_utc_datetime_v1(values["finished_at_utc"], field_name="finished_at_utc"),
        }
        return cls(
            **{**payload, "stage": stage},
            attempt_receipt_sha256=hash_hex_v1(
                "miniqmt_command_dispatch_attempt_v1", _canonical_constructor_payload_v1(payload)
            ),
        )

    @model_validator(mode="after")
    def _validate_attempt(self) -> Self:
        expected_id = "mqdispatch_" + hash_hex_v1(
            "miniqmt_command_dispatch_attempt_v1",
            {
                "command_id": self.command_id,
                "attempt_count": self.attempt_count,
                "lease_epoch": self.lease_epoch,
                "lease_fence_token": self.lease_fence_token,
            },
        )
        if self.dispatch_attempt_id != expected_id:
            raise ValueError("dispatch_attempt_id does not match attempt identity closure")
        if self.broker_called is True and not self.pre_call_complete:
            raise ValueError("broker_called=true requires completed pre-call stage")
        if (self.error_reason_code is None) != (self.error_context_sha256 is None):
            raise ValueError("attempt error reason and context hash must be present together")
        expected = hash_hex_v1(
            "miniqmt_command_dispatch_attempt_v1",
            self.canonical_payload_v1(exclude={"attempt_receipt_sha256"}),
        )
        if self.attempt_receipt_sha256 != expected:
            raise ValueError("attempt_receipt_sha256 does not match dispatch attempt closure")
        return self


class BrokerAckSourceV1(StrEnum):
    SYNCHRONOUS_RETURN = "SYNCHRONOUS_RETURN"
    CALLBACK = "CALLBACK"
    RECONCILIATION = "RECONCILIATION"


class BrokerCommandAckReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_command_ack_receipt_v1"]
    command_id: IdentityV1
    mapping_id: IdentityV1
    deterministic_client_order_ref: IdentityV1
    gateway_route_id: IdentityV1
    gateway_catalog_sha256: Sha256V1
    source: BrokerAckSourceV1
    accepted: StrictBool
    broker_called: Literal[True]
    broker_order_id: IdentityV1 | None
    reason_code: IdentityV1
    ack_payload_sha256: Sha256V1
    observed_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        source = BrokerAckSourceV1(values["source"])
        payload = {
            "schema_version": "miniqmt_broker_command_ack_receipt_v1",
            **values,
            "source": source.value,
            "broker_called": True,
            "observed_at_utc": canonical_utc_datetime_v1(values["observed_at_utc"], field_name="observed_at_utc"),
        }
        return cls(
            **{**payload, "source": source},
            receipt_sha256=hash_hex_v1("miniqmt_broker_command_ack_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        if self.accepted != (self.broker_order_id is not None):
            raise ValueError("accepted ACK must have broker order id and rejected ACK must not")
        expected = hash_hex_v1(
            "miniqmt_broker_command_ack_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match broker ACK closure")
        return self


class BrokerUncertainStageV1(StrEnum):
    GATEWAY_CALL = "GATEWAY_CALL"
    GATEWAY_RETURN = "GATEWAY_RETURN"
    ACK_PERSIST = "ACK_PERSIST"
    CALLBACK_CORRELATION = "CALLBACK_CORRELATION"


class BrokerUnknownOutcomeReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_unknown_outcome_receipt_v1"]
    command_id: IdentityV1
    dispatch_attempt_id: IdentityV1
    mapping_id: IdentityV1
    lease_fence_token: IdentityV1
    uncertain_stage: BrokerUncertainStageV1
    callback_watermark: IdentityV1
    reason_code: IdentityV1
    broker_called: None
    observed_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        uncertain_stage = BrokerUncertainStageV1(values["uncertain_stage"])
        payload = {
            "schema_version": "miniqmt_broker_unknown_outcome_receipt_v1",
            **values,
            "uncertain_stage": uncertain_stage.value,
            "broker_called": None,
            "observed_at_utc": canonical_utc_datetime_v1(values["observed_at_utc"], field_name="observed_at_utc"),
        }
        return cls(
            **{**payload, "uncertain_stage": uncertain_stage},
            receipt_sha256=hash_hex_v1("miniqmt_broker_unknown_outcome_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_broker_unknown_outcome_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match unknown outcome closure")
        return self


class BrokerNonAcceptanceReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_non_acceptance_receipt_v1"]
    command_id: IdentityV1
    deterministic_client_order_ref: IdentityV1
    gateway_route_id: IdentityV1
    gateway_catalog_sha256: Sha256V1
    query_criteria_sha256: Sha256V1
    callback_watermark_before: IdentityV1
    callback_watermark_after: IdentityV1
    order_snapshot_sha256: Sha256V1
    trade_snapshot_sha256: Sha256V1
    observed_at_utc: UtcDateTimeV1
    reason_code: IdentityV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_broker_non_acceptance_receipt_v1",
            **values,
            "observed_at_utc": canonical_utc_datetime_v1(values["observed_at_utc"], field_name="observed_at_utc"),
        }
        return cls(**payload, receipt_sha256=hash_hex_v1("miniqmt_broker_non_acceptance_receipt_v1", payload))

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        if self.callback_watermark_before == self.callback_watermark_after:
            raise ValueError("non-acceptance receipt requires a bounded callback watermark interval")
        expected = hash_hex_v1(
            "miniqmt_broker_non_acceptance_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match non-acceptance closure")
        return self


class BrokerReconciliationOutcomeV1(StrEnum):
    NOT_FOUND = "NOT_FOUND"
    UNIQUE_ACCEPTED = "UNIQUE_ACCEPTED"
    UNIQUE_REJECTED = "UNIQUE_REJECTED"
    CONFLICT = "CONFLICT"


class BrokerOutcomeReconciliationReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_outcome_reconciliation_receipt_v1"]
    command_id: IdentityV1
    reconcile_attempt: PositiveIntV1
    query_criteria_sha256: Sha256V1
    callback_watermark: IdentityV1
    ordered_matched_order_ids: tuple[IdentityV1, ...]
    ordered_matched_trade_ids: tuple[IdentityV1, ...]
    order_snapshot_sha256: Sha256V1
    trade_snapshot_sha256: Sha256V1
    outcome: BrokerReconciliationOutcomeV1
    broker_called: StrictBool | None
    broker_order_id: IdentityV1 | None
    reason_code: IdentityV1
    observed_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        outcome = BrokerReconciliationOutcomeV1(values["outcome"])
        order_ids = _require_unique_identities_v1(
            tuple(values["ordered_matched_order_ids"]), field_name="ordered_matched_order_ids"
        )
        trade_ids = _require_unique_identities_v1(
            tuple(values["ordered_matched_trade_ids"]), field_name="ordered_matched_trade_ids"
        )
        if order_ids != tuple(sorted(order_ids)) or trade_ids != tuple(sorted(trade_ids)):
            raise ValueError("matched broker identities must be sorted")
        payload = {
            "schema_version": "miniqmt_broker_outcome_reconciliation_receipt_v1",
            **{key: values[key] for key in values if not key.startswith("ordered_")},
            "outcome": outcome.value,
            "ordered_matched_order_ids": list(order_ids),
            "ordered_matched_trade_ids": list(trade_ids),
            "observed_at_utc": canonical_utc_datetime_v1(values["observed_at_utc"], field_name="observed_at_utc"),
        }
        return cls(
            **{
                **payload,
                "outcome": outcome,
                "ordered_matched_order_ids": order_ids,
                "ordered_matched_trade_ids": trade_ids,
            },
            receipt_sha256=hash_hex_v1("miniqmt_broker_outcome_reconciliation_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        if self.outcome is BrokerReconciliationOutcomeV1.NOT_FOUND:
            if self.broker_called is not None or self.broker_order_id is not None:
                raise ValueError("NOT_FOUND cannot prove broker_called or broker order identity")
        elif self.outcome is BrokerReconciliationOutcomeV1.UNIQUE_ACCEPTED:
            if (
                self.broker_called is not True
                or self.broker_order_id is None
                or len(self.ordered_matched_order_ids) != 1
            ):
                raise ValueError("UNIQUE_ACCEPTED requires one broker order and broker_called=true")
        elif self.outcome is BrokerReconciliationOutcomeV1.UNIQUE_REJECTED:
            if self.broker_called is not True or self.broker_order_id is not None:
                raise ValueError("UNIQUE_REJECTED requires broker_called=true without accepted broker id")
        elif self.broker_order_id is not None:
            raise ValueError("CONFLICT cannot select one broker order id")
        expected = hash_hex_v1(
            "miniqmt_broker_outcome_reconciliation_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match reconciliation closure")
        return self


def deterministic_client_order_ref_v1(*, command_id: str, mapping_id: str) -> str:
    return "mqclientref_" + hash_hex_v1(
        "miniqmt_command_client_ref_v1",
        {
            "command_id": require_identity_v1(command_id, field_name="command_id"),
            "mapping_id": require_identity_v1(mapping_id, field_name="mapping_id"),
        },
    )


def execution_child_order_id_v1(*, command_id: str, local_vt_orderid: str) -> str:
    return "mqchild_" + hash_hex_v1(
        "miniqmt_kernel_child_order_identity_v1",
        {
            "command_id": require_identity_v1(command_id, field_name="command_id"),
            "local_vt_orderid": require_identity_v1(local_vt_orderid, field_name="local_vt_orderid"),
        },
    )


def command_child_mapping_id_v1(*, command_id: str, local_vt_orderid: str, child_order_id: str) -> str:
    return "mqcmdchild_" + hash_hex_v1(
        "miniqmt_command_child_mapping_identity_v1",
        {
            "command_id": require_identity_v1(command_id, field_name="command_id"),
            "local_vt_orderid": require_identity_v1(local_vt_orderid, field_name="local_vt_orderid"),
            "child_order_id": require_identity_v1(child_order_id, field_name="child_order_id"),
        },
    )


def kernel_lease_fence_token_v1(*, owner_type: str, owner_id: str, lease_epoch: int, lease_owner: str) -> str:
    if type(lease_epoch) is not int or lease_epoch <= 0:
        raise TypeError("lease_epoch must be a positive strict integer")
    return "mqfence_" + hash_hex_v1(
        "miniqmt_kernel_lease_fence_v1",
        {
            "owner_type": require_identity_v1(owner_type, field_name="owner_type"),
            "owner_id": require_identity_v1(owner_id, field_name="owner_id"),
            "lease_epoch": lease_epoch,
            "lease_owner": require_identity_v1(lease_owner, field_name="lease_owner"),
        },
    )


def _validate_kernel_lease_fence_v1(
    *,
    owner_type: str,
    owner_id: str,
    lease_owner: str | None,
    lease_epoch: int,
    lease_fence_token: str | None,
) -> None:
    if lease_owner is None:
        if lease_fence_token is not None:
            raise ValueError("lease fence cannot exist without a lease owner")
        return
    expected = kernel_lease_fence_token_v1(
        owner_type=owner_type,
        owner_id=owner_id,
        lease_epoch=lease_epoch,
        lease_owner=lease_owner,
    )
    if lease_fence_token != expected:
        raise ValueError("lease_fence_token does not match exact kernel fence authority")


class CommandChildMappingStatusV1(StrEnum):
    RESERVED = "RESERVED"
    DISPATCHING = "DISPATCHING"
    BROKER_ACCEPTED = "BROKER_ACCEPTED"
    BROKER_REJECTED = "BROKER_REJECTED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    TERMINAL = "TERMINAL"


class ExecutionCommandChildMappingV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_command_child_mapping_v1"]
    mapping_id: IdentityV1
    command_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    local_vt_orderid: IdentityV1
    child_order_id: IdentityV1
    deterministic_client_order_ref: IdentityV1
    order_remark: IdentityV1
    symbol: IdentityV1
    side: SideV1
    requested_price_decimal: PositiveCanonicalDecimalV1
    requested_quantity: PositiveIntV1
    broker_order_id: IdentityV1 | None
    broker_identity_source_event_id: IdentityV1 | None
    mapping_status: CommandChildMappingStatusV1
    mapping_version: PositiveIntV1
    payload_sha256: Sha256V1
    last_order_event_id: IdentityV1 | None
    last_trade_event_id: IdentityV1 | None
    created_transition_id: IdentityV1
    updated_by_event_id: IdentityV1 | None
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    mapping_receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        command: BrokerCommandV2,
        strategy_slot_id: str,
        mapping_status: CommandChildMappingStatusV1 | str,
        mapping_version: int,
        broker_order_id: str | None,
        broker_identity_source_event_id: str | None,
        last_order_event_id: str | None,
        last_trade_event_id: str | None,
        updated_by_event_id: str | None,
        created_at_utc: Any,
        updated_at_utc: Any,
    ) -> Self:
        if not isinstance(command, BrokerCommandV2):
            raise TypeError("command must be BrokerCommandV2")
        if command.command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT:
            raise ValueError("a durable child mapping can only be created by SUBMIT_LIMIT")
        normalized_status = CommandChildMappingStatusV1(mapping_status)
        child_order_id = execution_child_order_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
        )
        mapping_id = command_child_mapping_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
            child_order_id=child_order_id,
        )
        client_ref = deterministic_client_order_ref_v1(command_id=command.command_id, mapping_id=mapping_id)
        payload_fields = {
            "command_id": command.command_id,
            "runtime_id": command.runtime_id,
            "algo_instance_id": command.algo_instance_id,
            "parent_intent_id": command.parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "local_vt_orderid": command.local_vt_orderid,
            "child_order_id": child_order_id,
            "deterministic_client_order_ref": client_ref,
            "order_remark": client_ref,
            "symbol": command.symbol,
            "side": command.side.value,
            "requested_price_decimal": command.price_decimal,
            "requested_quantity": command.quantity,
            "created_transition_id": command.transition_id,
        }
        payload = {
            "schema_version": "miniqmt_command_child_mapping_v1",
            "mapping_id": mapping_id,
            **payload_fields,
            "broker_order_id": broker_order_id,
            "broker_identity_source_event_id": broker_identity_source_event_id,
            "mapping_status": normalized_status.value,
            "mapping_version": mapping_version,
            "payload_sha256": hash_hex_v1("miniqmt_command_child_mapping_payload_v1", payload_fields),
            "last_order_event_id": last_order_event_id,
            "last_trade_event_id": last_trade_event_id,
            "updated_by_event_id": updated_by_event_id,
            "created_at_utc": canonical_utc_datetime_v1(created_at_utc, field_name="created_at_utc"),
            "updated_at_utc": canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc"),
        }
        return cls(
            **{**payload, "side": command.side, "mapping_status": normalized_status},
            mapping_receipt_sha256=hash_hex_v1("miniqmt_command_child_mapping_receipt_v1", payload),
        )

    def mapping_payload_v1(self) -> dict[str, Any]:
        return {
            key: self.canonical_payload_v1()[key]
            for key in (
                "command_id",
                "runtime_id",
                "algo_instance_id",
                "parent_intent_id",
                "strategy_slot_id",
                "local_vt_orderid",
                "child_order_id",
                "deterministic_client_order_ref",
                "order_remark",
                "symbol",
                "side",
                "requested_price_decimal",
                "requested_quantity",
                "created_transition_id",
            )
        }

    def validate_successor_v1(self, previous: "ExecutionCommandChildMappingV1") -> Self:
        if not isinstance(previous, ExecutionCommandChildMappingV1):
            raise TypeError("previous must be ExecutionCommandChildMappingV1")
        if self.mapping_id != previous.mapping_id or self.mapping_payload_v1() != previous.mapping_payload_v1():
            raise ValueError("mapping immutable business payload changed")
        if self.mapping_version != previous.mapping_version + 1:
            raise ValueError("mapping_version must increment exactly once")
        allowed = {
            CommandChildMappingStatusV1.RESERVED: {CommandChildMappingStatusV1.DISPATCHING},
            CommandChildMappingStatusV1.DISPATCHING: {
                CommandChildMappingStatusV1.BROKER_ACCEPTED,
                CommandChildMappingStatusV1.BROKER_REJECTED,
                CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
            },
            CommandChildMappingStatusV1.OUTCOME_UNKNOWN: {
                CommandChildMappingStatusV1.BROKER_ACCEPTED,
                CommandChildMappingStatusV1.BROKER_REJECTED,
                CommandChildMappingStatusV1.TERMINAL,
            },
            CommandChildMappingStatusV1.BROKER_ACCEPTED: {CommandChildMappingStatusV1.TERMINAL},
            CommandChildMappingStatusV1.BROKER_REJECTED: {CommandChildMappingStatusV1.TERMINAL},
            CommandChildMappingStatusV1.TERMINAL: set(),
        }
        if self.mapping_status not in allowed[previous.mapping_status]:
            raise ValueError("illegal command-child mapping status transition")
        return self

    def validate_initial_v1(self) -> Self:
        if self.mapping_status is not CommandChildMappingStatusV1.RESERVED:
            raise ValueError("initial mapping must be RESERVED")
        if self.mapping_version != 1:
            raise ValueError("initial RESERVED mapping must start at mapping_version=1")
        if any(
            value is not None
            for value in (
                self.broker_order_id,
                self.broker_identity_source_event_id,
                self.last_order_event_id,
                self.last_trade_event_id,
                self.updated_by_event_id,
            )
        ):
            raise ValueError("initial RESERVED mapping cannot carry broker or event lineage")
        if self.created_at_utc != self.updated_at_utc:
            raise ValueError("initial RESERVED mapping timestamps must be equal")
        return self

    @model_validator(mode="after")
    def _validate_mapping(self) -> Self:
        expected_child = execution_child_order_id_v1(command_id=self.command_id, local_vt_orderid=self.local_vt_orderid)
        if self.child_order_id != expected_child:
            raise ValueError("child_order_id does not match command/local identity closure")
        expected_mapping = command_child_mapping_id_v1(
            command_id=self.command_id,
            local_vt_orderid=self.local_vt_orderid,
            child_order_id=self.child_order_id,
        )
        if self.mapping_id != expected_mapping:
            raise ValueError("mapping_id does not match command-child identity closure")
        expected_ref = deterministic_client_order_ref_v1(command_id=self.command_id, mapping_id=self.mapping_id)
        if self.deterministic_client_order_ref != expected_ref or self.order_remark != expected_ref:
            raise ValueError("client ref/order remark does not match deterministic mapping closure")
        if _A_SHARE_SYMBOL_RE.fullmatch(self.symbol) is None:
            raise ValueError("symbol must be a recognized normalized A-share symbol")
        if self.payload_sha256 != hash_hex_v1("miniqmt_command_child_mapping_payload_v1", self.mapping_payload_v1()):
            raise ValueError("payload_sha256 does not match immutable mapping payload")
        broker_status = self.mapping_status is CommandChildMappingStatusV1.BROKER_ACCEPTED
        if broker_status != (self.broker_order_id is not None):
            if not (self.mapping_status is CommandChildMappingStatusV1.TERMINAL and self.broker_order_id is not None):
                raise ValueError("broker order identity conflicts with mapping status")
        if (self.broker_order_id is None) != (self.broker_identity_source_event_id is None):
            raise ValueError("broker order identity and source event must be present together")
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        expected_receipt = hash_hex_v1(
            "miniqmt_command_child_mapping_receipt_v1",
            self.canonical_payload_v1(exclude={"mapping_receipt_sha256"}),
        )
        if self.mapping_receipt_sha256 != expected_receipt:
            raise ValueError("mapping_receipt_sha256 does not match mapping closure")
        if self.mapping_status is CommandChildMappingStatusV1.RESERVED:
            self.validate_initial_v1()
        return self


class BrokerCommandOutboxStatusV1(StrEnum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    DISPATCHING = "DISPATCHING"
    ACKED = "ACKED"
    ACKED_REJECTED = "ACKED_REJECTED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    RECONCILING = "RECONCILING"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class BrokerCommandOutboxV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_broker_command_outbox_v1"]
    command_id: IdentityV1
    transition_id: IdentityV1
    ordinal: NonNegativeIntV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    mapping_id: IdentityV1
    command_type: BrokerCommandTypeV2
    local_vt_orderid: IdentityV1
    payload_json: FrozenJsonObjectFieldV1
    payload_sha256: Sha256V1
    status: BrokerCommandOutboxStatusV1
    attempt_count: NonNegativeIntV1
    lease_owner: IdentityV1 | None
    lease_epoch: NonNegativeIntV1
    lease_fence_token: IdentityV1 | None
    lease_expires_at: UtcDateTimeV1 | None
    dispatch_attempt_id: IdentityV1 | None
    deterministic_client_order_ref: IdentityV1
    next_attempt_at_utc: UtcDateTimeV1 | None
    broker_called: StrictBool | None
    broker_order_id: IdentityV1 | None
    ack_receipt_json: BrokerCommandAckReceiptV1 | None
    ack_receipt_sha256: Sha256V1 | None
    non_acceptance_receipt: BrokerNonAcceptanceReceiptV1 | None
    unknown_outcome_receipt: BrokerUnknownOutcomeReceiptV1 | None
    reconcile_receipt: BrokerOutcomeReconciliationReceiptV1 | None
    last_error_json: FrozenJsonObjectFieldV1 | None
    row_version: PositiveIntV1
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    closed_at_utc: UtcDateTimeV1 | None
    outbox_row_sha256: Sha256V1

    @classmethod
    def create(cls, *, command: BrokerCommandV2, mapping_id: str, **values: Any) -> Self:
        if not isinstance(command, BrokerCommandV2):
            raise TypeError("command must be BrokerCommandV2")
        client_ref = deterministic_client_order_ref_v1(command_id=command.command_id, mapping_id=mapping_id)
        normalized_status = BrokerCommandOutboxStatusV1(values["status"])
        payload = {
            "schema_version": "miniqmt_broker_command_outbox_v1",
            "command_id": command.command_id,
            "transition_id": command.transition_id,
            "ordinal": command.ordinal,
            "runtime_id": command.runtime_id,
            "algo_instance_id": command.algo_instance_id,
            "parent_intent_id": command.parent_intent_id,
            "mapping_id": mapping_id,
            "command_type": command.command_type.value,
            "local_vt_orderid": command.local_vt_orderid,
            "payload_json": command.model_dump(mode="json"),
            "payload_sha256": command.payload_sha256,
            "deterministic_client_order_ref": client_ref,
            **values,
            "status": normalized_status.value,
        }
        for field_name in (
            "lease_expires_at",
            "next_attempt_at_utc",
            "created_at_utc",
            "updated_at_utc",
            "closed_at_utc",
        ):
            if payload.get(field_name) is not None:
                payload[field_name] = canonical_utc_datetime_v1(payload[field_name], field_name=field_name)
        model_payload = {
            **payload,
            "command_type": command.command_type,
            "status": normalized_status,
        }
        return cls(
            **model_payload,
            outbox_row_sha256=hash_hex_v1(
                "miniqmt_broker_command_outbox_row_v1", _canonical_constructor_payload_v1(payload)
            ),
        )

    def validate_successor_v1(self, previous: "BrokerCommandOutboxV1") -> Self:
        if not isinstance(previous, BrokerCommandOutboxV1):
            raise TypeError("previous must be BrokerCommandOutboxV1")
        immutable = {
            "command_id",
            "transition_id",
            "ordinal",
            "runtime_id",
            "algo_instance_id",
            "parent_intent_id",
            "mapping_id",
            "command_type",
            "local_vt_orderid",
            "payload_json",
            "payload_sha256",
            "deterministic_client_order_ref",
            "created_at_utc",
        }
        if any(getattr(self, name) != getattr(previous, name) for name in immutable):
            raise ValueError("outbox immutable business payload changed")
        if self.row_version != previous.row_version + 1:
            raise ValueError("outbox row_version must increment exactly once")
        allowed = {
            BrokerCommandOutboxStatusV1.PENDING: {BrokerCommandOutboxStatusV1.CLAIMED},
            BrokerCommandOutboxStatusV1.CLAIMED: {
                BrokerCommandOutboxStatusV1.DISPATCHING,
                BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            },
            BrokerCommandOutboxStatusV1.DISPATCHING: {
                BrokerCommandOutboxStatusV1.ACKED,
                BrokerCommandOutboxStatusV1.ACKED_REJECTED,
                BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            },
            BrokerCommandOutboxStatusV1.FAILED_RETRYABLE: {BrokerCommandOutboxStatusV1.CLAIMED},
            BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN: {BrokerCommandOutboxStatusV1.RECONCILING},
            BrokerCommandOutboxStatusV1.RECONCILING: {
                BrokerCommandOutboxStatusV1.ACKED,
                BrokerCommandOutboxStatusV1.ACKED_REJECTED,
                BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
            },
            BrokerCommandOutboxStatusV1.ACKED: set(),
            BrokerCommandOutboxStatusV1.ACKED_REJECTED: set(),
            BrokerCommandOutboxStatusV1.FAILED_TERMINAL: set(),
        }
        if self.status not in allowed[previous.status]:
            raise ValueError("illegal outbox status transition")
        if self.status is BrokerCommandOutboxStatusV1.CLAIMED:
            if self.lease_epoch != previous.lease_epoch + 1:
                raise ValueError("outbox lease_epoch must advance from its durable predecessor")
        elif self.lease_epoch != previous.lease_epoch:
            raise ValueError("outbox completion must preserve the claimed lease epoch")
        return self

    def validate_initial_v1(self) -> Self:
        if self.status is not BrokerCommandOutboxStatusV1.PENDING:
            raise ValueError("initial outbox must be PENDING")
        if self.attempt_count != 0 or self.lease_epoch != 0 or self.row_version != 1:
            raise ValueError("initial PENDING outbox must start at attempt=0, lease_epoch=0 and row_version=1")
        if any(
            value is not None
            for value in (
                self.lease_owner,
                self.lease_fence_token,
                self.lease_expires_at,
                self.dispatch_attempt_id,
                self.next_attempt_at_utc,
                self.broker_called,
                self.broker_order_id,
                self.ack_receipt_json,
                self.ack_receipt_sha256,
                self.non_acceptance_receipt,
                self.unknown_outcome_receipt,
                self.reconcile_receipt,
                self.last_error_json,
                self.closed_at_utc,
            )
        ):
            raise ValueError("initial PENDING outbox cannot carry lease, dispatch or broker outcome history")
        if self.created_at_utc != self.updated_at_utc:
            raise ValueError("initial PENDING outbox timestamps must be equal")
        return self

    @model_validator(mode="after")
    def _validate_outbox(self) -> Self:
        command = BrokerCommandV2.model_validate_json(
            json.dumps(thaw_json_v1(self.payload_json), sort_keys=True, separators=(",", ":"))
        )
        if (
            command.command_id != self.command_id
            or command.transition_id != self.transition_id
            or command.ordinal != self.ordinal
            or command.runtime_id != self.runtime_id
            or command.algo_instance_id != self.algo_instance_id
            or command.parent_intent_id != self.parent_intent_id
            or command.command_type is not self.command_type
            or command.local_vt_orderid != self.local_vt_orderid
            or command.payload_sha256 != self.payload_sha256
        ):
            raise ValueError("outbox row conflicts with strict BrokerCommandV2 readback")
        expected_ref = deterministic_client_order_ref_v1(command_id=self.command_id, mapping_id=self.mapping_id)
        if self.deterministic_client_order_ref != expected_ref:
            raise ValueError("deterministic client ref conflicts with command/mapping closure")
        lease_values = (self.lease_owner, self.lease_fence_token, self.lease_expires_at)
        if any(value is None for value in lease_values) != all(value is None for value in lease_values):
            raise ValueError("outbox lease owner, fence and expiry must be present together")
        if self.lease_owner is not None and self.lease_epoch <= 0:
            raise ValueError("leased outbox row requires positive lease epoch")
        _validate_kernel_lease_fence_v1(
            owner_type="OUTBOX_COMMAND",
            owner_id=self.command_id,
            lease_owner=self.lease_owner,
            lease_epoch=self.lease_epoch,
            lease_fence_token=self.lease_fence_token,
        )
        if self.dispatch_attempt_id is not None and self.attempt_count <= 0:
            raise ValueError("dispatch attempt identity requires positive attempt count")
        if self.ack_receipt_json is not None:
            if self.ack_receipt_sha256 != self.ack_receipt_json.receipt_sha256:
                raise ValueError("ack_receipt_sha256 conflicts with strict ACK receipt")
            if self.ack_receipt_json.command_id != self.command_id:
                raise ValueError("ACK receipt command identity conflicts with outbox")
        elif self.ack_receipt_sha256 is not None:
            raise ValueError("ack_receipt_sha256 requires strict ACK receipt")
        authority_receipts = (
            self.ack_receipt_json,
            self.non_acceptance_receipt,
            self.unknown_outcome_receipt,
            self.reconcile_receipt,
        )
        if any(receipt is not None and receipt.command_id != self.command_id for receipt in authority_receipts):
            raise ValueError("broker authority receipt command identity conflicts with outbox")
        if self.status in {BrokerCommandOutboxStatusV1.PENDING, BrokerCommandOutboxStatusV1.CLAIMED}:
            if self.broker_called is not None or any(receipt is not None for receipt in authority_receipts):
                raise ValueError("pre-dispatch outbox cannot claim broker outcome")
        elif self.status is BrokerCommandOutboxStatusV1.DISPATCHING:
            if self.broker_called is not None or self.dispatch_attempt_id is None:
                raise ValueError("DISPATCHING requires committed attempt with broker_called unknown")
        elif self.status in {BrokerCommandOutboxStatusV1.ACKED, BrokerCommandOutboxStatusV1.ACKED_REJECTED}:
            if self.broker_called is not True or self.ack_receipt_json is None:
                raise ValueError("ACKED outcome requires strict ACK and broker_called=true")
            if (self.status is BrokerCommandOutboxStatusV1.ACKED) != self.ack_receipt_json.accepted:
                raise ValueError("outbox ACK status conflicts with ACK acceptance")
            if self.broker_order_id != self.ack_receipt_json.broker_order_id:
                raise ValueError("outbox broker identity conflicts with ACK receipt")
        elif self.status is BrokerCommandOutboxStatusV1.FAILED_RETRYABLE:
            if self.broker_called is not False or self.last_error_json is None or self.next_attempt_at_utc is None:
                raise ValueError("retryable outbox failure must prove pre-call failure")
        elif self.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN:
            if self.broker_called is not None or self.unknown_outcome_receipt is None:
                raise ValueError("OUTCOME_UNKNOWN requires strict unknown-outcome receipt")
        elif self.status is BrokerCommandOutboxStatusV1.RECONCILING:
            if self.broker_called is not None or self.unknown_outcome_receipt is None:
                raise ValueError("RECONCILING requires unresolved unknown-outcome authority")
        elif not any(receipt is not None for receipt in authority_receipts) or self.broker_called not in (
            False,
            None,
            True,
        ):
            raise ValueError("terminal outbox failure requires explicit broker outcome authority")
        if self.non_acceptance_receipt is not None and self.broker_called is not False:
            raise ValueError("non-acceptance authority requires broker_called=false")
        if self.last_error_json is not None:
            KernelErrorEvidenceV1.model_validate_json(
                json.dumps(thaw_json_v1(self.last_error_json), sort_keys=True, separators=(",", ":"))
            )
        closed = self.status in {
            BrokerCommandOutboxStatusV1.ACKED,
            BrokerCommandOutboxStatusV1.ACKED_REJECTED,
            BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
        }
        if closed != (self.closed_at_utc is not None):
            raise ValueError("outbox closed timestamp conflicts with terminal status")
        if closed and any(value is not None for value in lease_values):
            raise ValueError("terminal outbox must clear active lease")
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        expected_hash = hash_hex_v1(
            "miniqmt_broker_command_outbox_row_v1",
            self.canonical_payload_v1(exclude={"outbox_row_sha256"}),
        )
        if self.outbox_row_sha256 != expected_hash:
            raise ValueError("outbox_row_sha256 does not match current strict row")
        if self.status is BrokerCommandOutboxStatusV1.PENDING:
            self.validate_initial_v1()
        return self


class ExecutionAlgoTimerScheduleStatusV1(StrEnum):
    SCHEDULED = "SCHEDULED"
    EMITTING = "EMITTING"
    EMITTED = "EMITTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class ExecutionAlgoTimerOccurrenceStatusV1(StrEnum):
    CLAIMED = "CLAIMED"
    EVENT_COMMITTED = "EVENT_COMMITTED"
    SKIPPED = "SKIPPED"
    EXPIRED = "EXPIRED"


class ExecutionAlgoTimerScheduleV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_execution_algo_timer_schedule_v1"]
    schedule_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    timer_name: IdentityV1
    schedule_epoch: IdentityV1
    due_at_exchange_utc: UtcDateTimeV1
    catch_up_policy: IdentityV1
    payload: FrozenJsonObjectFieldV1
    payload_sha256: Sha256V1
    status: ExecutionAlgoTimerScheduleStatusV1
    timer_occurrence_id: IdentityV1
    emitted_event_id: IdentityV1 | None
    lease_owner: IdentityV1 | None
    lease_epoch: NonNegativeIntV1
    lease_fence_token: IdentityV1 | None
    lease_expires_at_utc: UtcDateTimeV1 | None
    row_version: PositiveIntV1
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    closed_at_utc: UtcDateTimeV1 | None
    schedule_receipt_sha256: Sha256V1

    @classmethod
    def create(cls, *, runtime_id: str, mutation: TimerMutationV1, **values: Any) -> Self:
        if not isinstance(mutation, TimerMutationV1):
            raise TypeError("mutation must be TimerMutationV1")
        if mutation.mutation_type is not TimerMutationTypeV1.UPSERT_ONE_SHOT:
            raise ValueError("timer schedule persistence requires UPSERT_ONE_SHOT mutation")
        normalized_status = ExecutionAlgoTimerScheduleStatusV1(values["status"])
        payload = {
            "schema_version": "miniqmt_execution_algo_timer_schedule_v1",
            "schedule_id": mutation.schedule_id,
            "runtime_id": runtime_id,
            "algo_instance_id": mutation.algo_instance_id,
            "timer_name": mutation.timer_name,
            "schedule_epoch": mutation.schedule_epoch,
            "due_at_exchange_utc": mutation.due_at_exchange_utc,
            "catch_up_policy": mutation.catch_up_policy,
            "payload": thaw_json_v1(mutation.payload),
            "payload_sha256": mutation.payload_sha256,
            "timer_occurrence_id": mutation.timer_occurrence_id,
            **values,
            "status": normalized_status.value,
        }
        for field_name in ("lease_expires_at_utc", "created_at_utc", "updated_at_utc", "closed_at_utc"):
            if payload.get(field_name) is not None:
                payload[field_name] = canonical_utc_datetime_v1(payload[field_name], field_name=field_name)
        return cls(
            **{**payload, "status": normalized_status},
            schedule_receipt_sha256=hash_hex_v1(
                "miniqmt_timer_schedule_receipt_v1", _canonical_constructor_payload_v1(payload)
            ),
        )

    def immutable_schedule_payload_v1(self) -> dict[str, Any]:
        return {
            key: self.canonical_payload_v1()[key]
            for key in (
                "schedule_id",
                "runtime_id",
                "algo_instance_id",
                "timer_name",
                "schedule_epoch",
                "due_at_exchange_utc",
                "catch_up_policy",
                "payload",
                "payload_sha256",
                "timer_occurrence_id",
                "created_at_utc",
            )
        }

    def validate_successor_v1(self, previous: "ExecutionAlgoTimerScheduleV1") -> Self:
        if not isinstance(previous, ExecutionAlgoTimerScheduleV1):
            raise TypeError("previous must be ExecutionAlgoTimerScheduleV1")
        if self.immutable_schedule_payload_v1() != previous.immutable_schedule_payload_v1():
            raise ValueError("timer schedule immutable payload changed")
        if self.row_version != previous.row_version + 1:
            raise ValueError("timer schedule row_version must increment exactly once")
        allowed = {
            ExecutionAlgoTimerScheduleStatusV1.SCHEDULED: {
                ExecutionAlgoTimerScheduleStatusV1.EMITTING,
                ExecutionAlgoTimerScheduleStatusV1.CANCELLED,
                ExecutionAlgoTimerScheduleStatusV1.EXPIRED,
            },
            ExecutionAlgoTimerScheduleStatusV1.EMITTING: {
                ExecutionAlgoTimerScheduleStatusV1.EMITTED,
                ExecutionAlgoTimerScheduleStatusV1.EXPIRED,
            },
            ExecutionAlgoTimerScheduleStatusV1.EMITTED: set(),
            ExecutionAlgoTimerScheduleStatusV1.CANCELLED: set(),
            ExecutionAlgoTimerScheduleStatusV1.EXPIRED: set(),
        }
        if self.status not in allowed[previous.status]:
            raise ValueError("illegal timer schedule status transition")
        if self.status is ExecutionAlgoTimerScheduleStatusV1.EMITTING:
            if self.lease_epoch != previous.lease_epoch + 1:
                raise ValueError("timer schedule lease_epoch must advance from its durable predecessor")
        elif self.lease_epoch != previous.lease_epoch:
            raise ValueError("timer schedule completion must preserve the claimed lease epoch")
        return self

    def validate_initial_v1(self) -> Self:
        if self.status is not ExecutionAlgoTimerScheduleStatusV1.SCHEDULED:
            raise ValueError("initial timer schedule must be SCHEDULED")
        if self.lease_epoch != 0 or self.row_version != 1:
            raise ValueError("initial SCHEDULED timer must start at lease_epoch=0 and row_version=1")
        if any(
            value is not None
            for value in (
                self.emitted_event_id,
                self.lease_owner,
                self.lease_fence_token,
                self.lease_expires_at_utc,
                self.closed_at_utc,
            )
        ):
            raise ValueError("initial SCHEDULED timer cannot carry lease or outcome history")
        if self.created_at_utc != self.updated_at_utc:
            raise ValueError("initial SCHEDULED timer timestamps must be equal")
        return self

    @model_validator(mode="after")
    def _validate_schedule(self) -> Self:
        expected_schedule = _timer_schedule_id_v1(
            algo_instance_id=self.algo_instance_id,
            timer_name=self.timer_name,
            schedule_epoch=self.schedule_epoch,
        )
        if self.schedule_id != expected_schedule:
            raise ValueError("schedule_id does not match timer identity closure")
        expected_occurrence = _timer_occurrence_id_v1(
            schedule_id=self.schedule_id,
            due_at_exchange_utc=self.due_at_exchange_utc,
        )
        if self.timer_occurrence_id != expected_occurrence:
            raise ValueError("timer_occurrence_id does not match schedule/due closure")
        if self.payload_sha256 != hash_hex_v1("miniqmt_timer_mutation_payload_v1", thaw_json_v1(self.payload)):
            raise ValueError("payload_sha256 does not match timer payload")
        lease_values = (self.lease_owner, self.lease_fence_token, self.lease_expires_at_utc)
        if any(value is None for value in lease_values) != all(value is None for value in lease_values):
            raise ValueError("timer lease owner, fence and expiry must be present together")
        if self.lease_owner is not None and self.lease_epoch <= 0:
            raise ValueError("leased timer schedule requires positive lease epoch")
        if self.status is ExecutionAlgoTimerScheduleStatusV1.EMITTING and self.lease_owner is None:
            raise ValueError("EMITTING timer schedule requires durable lease")
        _validate_kernel_lease_fence_v1(
            owner_type="TIMER_SCHEDULE",
            owner_id=self.schedule_id,
            lease_owner=self.lease_owner,
            lease_epoch=self.lease_epoch,
            lease_fence_token=self.lease_fence_token,
        )
        if self.status is ExecutionAlgoTimerScheduleStatusV1.EMITTED:
            if self.emitted_event_id is None:
                raise ValueError("EMITTED timer schedule requires committed event identity")
        elif self.emitted_event_id is not None:
            raise ValueError("only EMITTED timer schedule may carry event identity")
        terminal = self.status in {
            ExecutionAlgoTimerScheduleStatusV1.EMITTED,
            ExecutionAlgoTimerScheduleStatusV1.CANCELLED,
            ExecutionAlgoTimerScheduleStatusV1.EXPIRED,
        }
        if terminal != (self.closed_at_utc is not None):
            raise ValueError("timer closed timestamp conflicts with schedule status")
        if terminal and any(value is not None for value in lease_values):
            raise ValueError("terminal timer schedule must clear lease")
        if self.updated_at_utc < self.created_at_utc:
            raise ValueError("updated_at_utc cannot precede created_at_utc")
        expected_hash = hash_hex_v1(
            "miniqmt_timer_schedule_receipt_v1",
            self.canonical_payload_v1(exclude={"schedule_receipt_sha256"}),
        )
        if self.schedule_receipt_sha256 != expected_hash:
            raise ValueError("schedule_receipt_sha256 does not match timer schedule closure")
        if self.status is ExecutionAlgoTimerScheduleStatusV1.SCHEDULED:
            self.validate_initial_v1()
        return self


class ExecutionAlgoTimerOccurrenceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_execution_algo_timer_occurrence_v1"]
    timer_occurrence_id: IdentityV1
    schedule_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    due_at_exchange_utc: UtcDateTimeV1
    exchange_session_authority_sha256: Sha256V1
    status: ExecutionAlgoTimerOccurrenceStatusV1
    emitted_event_id: IdentityV1 | None
    catch_up_receipt_sha256: Sha256V1 | None
    lease_owner: IdentityV1 | None
    lease_epoch: NonNegativeIntV1
    lease_fence_token: IdentityV1 | None
    lease_expires_at_utc: UtcDateTimeV1 | None
    row_version: PositiveIntV1
    created_at_utc: UtcDateTimeV1
    closed_at_utc: UtcDateTimeV1 | None
    occurrence_receipt_sha256: Sha256V1

    @classmethod
    def create(cls, *, schedule: ExecutionAlgoTimerScheduleV1, **values: Any) -> Self:
        if not isinstance(schedule, ExecutionAlgoTimerScheduleV1):
            raise TypeError("schedule must be ExecutionAlgoTimerScheduleV1")
        normalized_status = ExecutionAlgoTimerOccurrenceStatusV1(values["status"])
        payload = {
            "schema_version": "miniqmt_execution_algo_timer_occurrence_v1",
            "timer_occurrence_id": schedule.timer_occurrence_id,
            "schedule_id": schedule.schedule_id,
            "runtime_id": schedule.runtime_id,
            "algo_instance_id": schedule.algo_instance_id,
            "due_at_exchange_utc": schedule.due_at_exchange_utc,
            **values,
            "status": normalized_status.value,
        }
        for field_name in ("lease_expires_at_utc", "created_at_utc", "closed_at_utc"):
            if payload.get(field_name) is not None:
                payload[field_name] = canonical_utc_datetime_v1(payload[field_name], field_name=field_name)
        return cls(
            **{**payload, "status": normalized_status},
            occurrence_receipt_sha256=hash_hex_v1(
                "miniqmt_timer_occurrence_receipt_v1", _canonical_constructor_payload_v1(payload)
            ),
        )

    def validate_successor_v1(self, previous: "ExecutionAlgoTimerOccurrenceV1") -> Self:
        if not isinstance(previous, ExecutionAlgoTimerOccurrenceV1):
            raise TypeError("previous must be ExecutionAlgoTimerOccurrenceV1")
        immutable = {
            "timer_occurrence_id",
            "schedule_id",
            "runtime_id",
            "algo_instance_id",
            "due_at_exchange_utc",
            "exchange_session_authority_sha256",
            "created_at_utc",
        }
        if any(getattr(self, name) != getattr(previous, name) for name in immutable):
            raise ValueError("timer occurrence immutable payload changed")
        if self.row_version != previous.row_version + 1:
            raise ValueError("timer occurrence row_version must increment exactly once")
        if previous.status is not ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED:
            raise ValueError("closed timer occurrence cannot transition")
        if self.status not in {
            ExecutionAlgoTimerOccurrenceStatusV1.EVENT_COMMITTED,
            ExecutionAlgoTimerOccurrenceStatusV1.SKIPPED,
            ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED,
        }:
            raise ValueError("illegal timer occurrence status transition")
        if self.lease_epoch != previous.lease_epoch:
            raise ValueError("timer occurrence completion must preserve the claimed lease epoch")
        return self

    def validate_initial_v1(self) -> Self:
        if self.status is not ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED:
            raise ValueError("initial timer occurrence must be CLAIMED")
        if self.lease_epoch != 1 or self.row_version != 1:
            raise ValueError("initial CLAIMED occurrence must start at lease_epoch=1 and row_version=1")
        if self.lease_owner is None or self.lease_fence_token is None or self.lease_expires_at_utc is None:
            raise ValueError("initial CLAIMED occurrence requires an exact active fence")
        return self

    @model_validator(mode="after")
    def _validate_occurrence(self) -> Self:
        expected = _timer_occurrence_id_v1(
            schedule_id=self.schedule_id,
            due_at_exchange_utc=self.due_at_exchange_utc,
        )
        if self.timer_occurrence_id != expected:
            raise ValueError("timer occurrence identity does not match schedule/due closure")
        lease_values = (self.lease_owner, self.lease_fence_token, self.lease_expires_at_utc)
        if any(value is None for value in lease_values) != all(value is None for value in lease_values):
            raise ValueError("timer occurrence lease fields must be present together")
        if self.status is ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED:
            if self.lease_owner is None or self.lease_epoch != 1 or self.closed_at_utc is not None:
                raise ValueError("CLAIMED occurrence requires active durable lease")
            if self.emitted_event_id is not None or self.catch_up_receipt_sha256 is not None:
                raise ValueError("CLAIMED occurrence cannot carry terminal emission outcome")
        else:
            if self.closed_at_utc is None or any(value is not None for value in lease_values):
                raise ValueError("closed occurrence requires timestamp and cleared lease")
            if self.status is ExecutionAlgoTimerOccurrenceStatusV1.EVENT_COMMITTED:
                if self.emitted_event_id is None:
                    raise ValueError("EVENT_COMMITTED occurrence requires emitted event identity")
            elif self.emitted_event_id is not None:
                raise ValueError("skipped/expired occurrence cannot fabricate emitted event")
        _validate_kernel_lease_fence_v1(
            owner_type="TIMER_OCCURRENCE",
            owner_id=self.timer_occurrence_id,
            lease_owner=self.lease_owner,
            lease_epoch=self.lease_epoch,
            lease_fence_token=self.lease_fence_token,
        )
        expected_hash = hash_hex_v1(
            "miniqmt_timer_occurrence_receipt_v1",
            self.canonical_payload_v1(exclude={"occurrence_receipt_sha256"}),
        )
        if self.occurrence_receipt_sha256 != expected_hash:
            raise ValueError("occurrence_receipt_sha256 does not match timer occurrence closure")
        if self.status is ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED:
            self.validate_initial_v1()
        return self


class ExchangeSessionAuthorityV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_exchange_session_authority_v1"]
    runtime_id: IdentityV1
    exchange_trade_date: IdentityV1
    calendar_snapshot_set_id: IdentityV1
    calendar_snapshot_set_json: FrozenJsonObjectFieldV1
    calendar_snapshot_set_sha256: Sha256V1
    ordered_market_calendar_sha256s: tuple[Sha256V1, ...]
    timezone: Literal["Asia/Shanghai"]
    session_definition_version: IdentityV1
    ordered_session_segments: tuple[FrozenJsonObjectFieldV1, ...]
    source_effective_at_utc: UtcDateTimeV1
    authority_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        market_hashes = _require_unique_hashes_v1(
            tuple(values["ordered_market_calendar_sha256s"]),
            field_name="ordered_market_calendar_sha256s",
        )
        segments = tuple(values["ordered_session_segments"])
        segment_payload = [thaw_json_v1(_freeze_json_object_field(segment)) for segment in segments]
        session_definition_version = "mqsessiondef_" + hash_hex_v1(
            "miniqmt_exchange_session_definition_v1",
            {"timezone": "Asia/Shanghai", "ordered_session_segments": segment_payload},
        )
        payload = {
            "schema_version": "miniqmt_exchange_session_authority_v1",
            **{
                key: values[key]
                for key in values
                if key not in {"ordered_market_calendar_sha256s", "ordered_session_segments", "source_effective_at_utc"}
            },
            "ordered_market_calendar_sha256s": list(market_hashes),
            "timezone": "Asia/Shanghai",
            "session_definition_version": session_definition_version,
            "ordered_session_segments": segment_payload,
            "source_effective_at_utc": canonical_utc_datetime_v1(
                values["source_effective_at_utc"], field_name="source_effective_at_utc"
            ),
        }
        return cls(
            **{
                **payload,
                "ordered_market_calendar_sha256s": market_hashes,
                "ordered_session_segments": tuple(segments),
            },
            authority_sha256=hash_hex_v1("miniqmt_exchange_session_authority_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.exchange_trade_date) is None:
            raise ValueError("exchange_trade_date must be YYYY-MM-DD")
        if len(self.ordered_market_calendar_sha256s) != 3:
            raise ValueError("exchange session authority requires exact SH/SZ/BJ calendar set")
        if len(self.ordered_session_segments) == 0:
            raise ValueError("exchange session authority requires non-empty ordered session segments")
        snapshot_set = _calendar_snapshot_set_from_json_v1(thaw_json_v1(self.calendar_snapshot_set_json))
        if snapshot_set.snapshot_set_id != self.calendar_snapshot_set_id:
            raise ValueError("calendar snapshot set identity conflicts with canonical JSON")
        if snapshot_set.set_sha256 != self.calendar_snapshot_set_sha256:
            raise ValueError("calendar snapshot set hash conflicts with canonical JSON")
        market_order = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
        snapshots = tuple(snapshot_set.snapshot_by_market[market] for market in market_order)
        if tuple(snapshot.calendar_sha256 for snapshot in snapshots) != self.ordered_market_calendar_sha256s:
            raise ValueError("ordered market calendar hashes conflict with shared CalendarSnapshotSet")
        if any(snapshot.trade_date.isoformat() != self.exchange_trade_date for snapshot in snapshots):
            raise ValueError("calendar snapshot trade date conflicts with exchange authority")
        if any(snapshot.timezone != self.timezone for snapshot in snapshots):
            raise ValueError("calendar snapshot timezone conflicts with exchange authority")
        if len({snapshot.source_version for snapshot in snapshots}) != 1:
            raise ValueError("calendar snapshot set must use one exact source version")
        if len({snapshot.effective_at_utc for snapshot in snapshots}) != 1:
            raise ValueError("calendar snapshot set must use one exact effective time")
        if (
            canonical_utc_datetime_v1(snapshots[0].effective_at_utc, field_name="calendar.source_effective_at_utc")
            != self.source_effective_at_utc
        ):
            raise ValueError("calendar source effective time conflicts with exchange authority")
        segment_payload = [thaw_json_v1(item) for item in self.ordered_session_segments]
        shared_segments = tuple(segment.canonical_payload() for segment in snapshots[0].session_segments)
        if any(
            tuple(segment.canonical_payload() for segment in snapshot.session_segments) != shared_segments
            for snapshot in snapshots
        ):
            raise ValueError("SH/SZ/BJ session segments must close to one exact shared definition")
        if tuple(segment_payload) != shared_segments:
            raise ValueError("ordered session segments conflict with shared CalendarSnapshotSet")
        expected_definition = "mqsessiondef_" + hash_hex_v1(
            "miniqmt_exchange_session_definition_v1",
            {"timezone": self.timezone, "ordered_session_segments": segment_payload},
        )
        if self.session_definition_version != expected_definition:
            raise ValueError("session definition version does not match ordered segments")
        expected_hash = hash_hex_v1(
            "miniqmt_exchange_session_authority_v1",
            self.canonical_payload_v1(exclude={"authority_sha256"}),
        )
        if self.authority_sha256 != expected_hash:
            raise ValueError("authority_sha256 does not match exchange-session closure")
        return self


def _calendar_snapshot_set_from_json_v1(payload: dict[str, Any]) -> CalendarSnapshotSet:
    if set(payload) != {"snapshot_set_id", "snapshot_by_market", "set_sha256"}:
        raise ValueError("calendar_snapshot_set_json must be the exact shared CalendarSnapshotSet payload")
    raw_snapshots = payload["snapshot_by_market"]
    if not isinstance(raw_snapshots, dict):
        raise TypeError("calendar snapshot_by_market must be a JSON object")
    expected_keys = {f"MarketCode.{market.value}" for market in MarketCode}
    if set(raw_snapshots) != expected_keys:
        raise ValueError("calendar snapshot set must contain exact SH/SZ/BJ market keys")
    snapshots: dict[MarketCode, CalendarSnapshot] = {}
    for market in MarketCode:
        raw = raw_snapshots[f"MarketCode.{market.value}"]
        if not isinstance(raw, dict) or set(raw) != {
            "calendar_id",
            "market",
            "trade_date",
            "timezone",
            "session_segments",
            "effective_at_utc",
            "source_version",
        }:
            raise ValueError("calendar snapshot must use the exact shared CalendarSnapshot fields")
        raw_segments = raw["session_segments"]
        if not isinstance(raw_segments, list) or not raw_segments:
            raise ValueError("calendar snapshot requires non-empty session segments")
        segments = tuple(
            SessionSegment(
                start_local=time.fromisoformat(segment["start_local"]),
                end_local=time.fromisoformat(segment["end_local"]),
            )
            for segment in raw_segments
            if isinstance(segment, dict) and set(segment) == {"start_local", "end_local"}
        )
        if len(segments) != len(raw_segments):
            raise ValueError("calendar session segment must use exact start_local/end_local fields")
        snapshots[market] = CalendarSnapshot(
            calendar_id=raw["calendar_id"],
            market=raw["market"],
            trade_date=date.fromisoformat(raw["trade_date"]),
            timezone=raw["timezone"],
            session_segments=segments,
            effective_at_utc=datetime.fromisoformat(raw["effective_at_utc"].replace("Z", "+00:00")),
            source_version=raw["source_version"],
        )
    snapshot_set = CalendarSnapshotSet(snapshot_set_id=payload["snapshot_set_id"], snapshot_by_market=snapshots)
    if payload["set_sha256"] != snapshot_set.set_sha256:
        raise ValueError("calendar snapshot set SHA-256 conflicts with shared authority")
    return snapshot_set


__all__ = [
    "AbsenceDispositionV1",
    "ActiveChildClosureStatusV1",
    "AlgoDeliveryPersistenceV1",
    "AlgoEventDeliveryV1",
    "AlgoFailureReceiptV1",
    "AlgoInitializationV1",
    "AlgoSkipReceiptV1",
    "AlgoStartContextV1",
    "AlgoStateSnapshotV2",
    "AlgoTransitionReceiptV1",
    "AlgoTransitionV1",
    "BrokerAckSourceV1",
    "BrokerCommandAckReceiptV1",
    "BrokerCommandOutboxStatusV1",
    "BrokerCommandOutboxV1",
    "BrokerCommandTypeV2",
    "BrokerCommandV2",
    "BrokerDispatchAttemptStageV1",
    "BrokerDispatchAttemptV1",
    "BrokerNonAcceptanceReceiptV1",
    "BrokerOutcomeReconciliationReceiptV1",
    "BrokerReconciliationOutcomeV1",
    "BrokerUncertainStageV1",
    "BrokerUnknownOutcomeReceiptV1",
    "CommandChildMappingStatusV1",
    "ConsumedLineageRefV1",
    "ConsumedLineageTypeV1",
    "DeliveryStatusV1",
    "DeterministicExecutionContextV1",
    "DiagnosticObservationV1",
    "DiagnosticSeverityV1",
    "EnumValueRequirementV2",
    "EnumValueRequirementV1",
    "EventSourceV2",
    "EventTypeV2",
    "ExchangeSessionAuthorityV1",
    "ExecutionAlgoInstancePersistenceV2",
    "ExecutionAlgoPersistenceStatusV2",
    "ExecutionAlgoPluginManifestV2",
    "ExecutionAlgoTimerOccurrenceStatusV1",
    "ExecutionAlgoTimerOccurrenceV1",
    "ExecutionAlgoTimerScheduleStatusV1",
    "ExecutionAlgoTimerScheduleV1",
    "ExecutionCommandChildMappingV1",
    "ExecutionProjectionRefV1",
    "ExecutionProjectionSetV1",
    "FileHashV1",
    "FrozenJsonFieldV1",
    "FrozenJsonObjectFieldV1",
    "FrozenStrictModel",
    "GatewayCapabilityCatalogV1",
    "KernelErrorEvidenceV1",
    "KernelProjectionTypeV1",
    "KernelWorkerStartupReceiptV1",
    "MarketDataCapabilityV1",
    "MarketDataRequirementV1",
    "MiniQMTPluginContractError",
    "MiniQMTPluginReasonCode",
    "MiniQMTRiskDecisionReceiptV1",
    "OMSPreflightDecisionV1",
    "OMSPreflightProjectionReceiptV1",
    "ObjectFieldRequirementV1",
    "OrderTypeV1",
    "PluginProviderV2",
    "RuntimeEventEnvelopeV2",
    "RuntimeEventIngressReceiptV1",
    "RiskDecisionActionV1",
    "RiskDecisionStageV1",
    "SessionPhaseV1",
    "SideV1",
    "SourceAttributionV1",
    "TerminalOutcomeV1",
    "TimerMutationTypeV1",
    "TimerMutationV1",
    "TransactionCommitIdentityV1",
    "VnpyCompatibilityRequirementV1",
    "VnpyCompatibilityRequirementV2",
    "VnpyEnumMemberRequirementV2",
    "VnpyMethodRequirementV1",
    "VnpyParameterKindV1",
    "VnpyParameterRequirementV1",
    "VnpyObjectFieldKindV1",
    "VnpyObjectFieldV1",
    "VnpySourceFileV2",
    "VnpyUpstreamSourceV2",
    "compatibility_component_hashes_v2",
    "compatibility_component_hashes_v1",
    "command_child_mapping_id_v1",
    "deterministic_client_order_ref_v1",
    "execution_child_order_id_v1",
    "kernel_lease_fence_token_v1",
    "transaction_commit_identity_v1",
    "validate_json_schema_instance_v1",
]
