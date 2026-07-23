"""Strict, side-effect-free MiniQMT execution plugin contracts (K1-A).

The models in this module are intentionally not imported by the product
runtime yet.  They define the frozen writer/readback schema consumed by later
K1/K2 slices without reaching into repositories, gateways, OMS, or broker SDKs.
"""

from __future__ import annotations

import re
from enum import StrEnum
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


class ObjectFieldRequirementV1(FrozenStrictModel):
    object_name: IdentityV1
    fields: tuple[IdentityV1, ...]

    @model_validator(mode="after")
    def _validate_fields(self) -> Self:
        object.__setattr__(self, "fields", _sorted_unique(self.fields, field_name="required object fields"))
        return self


class EnumValueRequirementV1(FrozenStrictModel):
    enum_name: IdentityV1
    values: tuple[IdentityV1, ...]

    @model_validator(mode="after")
    def _validate_values(self) -> Self:
        object.__setattr__(self, "values", _sorted_unique(self.values, field_name="required enum values"))
        return self


class VnpyCompatibilityRequirementV1(FrozenStrictModel):
    schema_version: Literal["vnpy_compatibility_requirement_v1"]
    mode: IdentityV1
    upstream_repo: IdentityV1
    upstream_commit: IdentityV1
    source_files_and_hashes: tuple[FileHashV1, ...]
    required_method_signatures: tuple[IdentityV1, ...]
    required_object_fields: tuple[ObjectFieldRequirementV1, ...]
    required_enum_values: tuple[EnumValueRequirementV1, ...]
    characterization_sha256: Sha256V1
    requirement_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        if _GIT_SHA_RE.fullmatch(self.upstream_commit) is None:
            raise ValueError("upstream_commit must be a 40-character lowercase git sha")
        object.__setattr__(
            self,
            "source_files_and_hashes",
            _sorted_models(self.source_files_and_hashes, "path", "source_files_and_hashes"),
        )
        object.__setattr__(
            self,
            "required_method_signatures",
            _sorted_unique(self.required_method_signatures, field_name="required_method_signatures"),
        )
        object.__setattr__(
            self,
            "required_object_fields",
            _sorted_models(self.required_object_fields, "object_name", "required_object_fields"),
        )
        object.__setattr__(
            self,
            "required_enum_values",
            _sorted_models(self.required_enum_values, "enum_name", "required_enum_values"),
        )
        expected = hash_hex_v1(
            "miniqmt_vnpy_compatibility_requirement_v1",
            self.canonical_payload_v1(exclude={"requirement_sha256"}),
        )
        if self.requirement_sha256 != expected:
            raise ValueError("requirement_sha256 does not match compatibility closure")
        return self


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


__all__ = [
    "AbsenceDispositionV1",
    "AlgoEventDeliveryV1",
    "AlgoInitializationV1",
    "AlgoStartContextV1",
    "AlgoStateSnapshotV2",
    "AlgoTransitionV1",
    "BrokerCommandTypeV2",
    "BrokerCommandV2",
    "DeliveryStatusV1",
    "DeterministicExecutionContextV1",
    "DiagnosticObservationV1",
    "DiagnosticSeverityV1",
    "EnumValueRequirementV1",
    "EventSourceV2",
    "EventTypeV2",
    "ExecutionAlgoPluginManifestV2",
    "FileHashV1",
    "FrozenJsonFieldV1",
    "FrozenJsonObjectFieldV1",
    "FrozenStrictModel",
    "GatewayCapabilityCatalogV1",
    "MarketDataCapabilityV1",
    "MarketDataRequirementV1",
    "MiniQMTPluginContractError",
    "MiniQMTPluginReasonCode",
    "ObjectFieldRequirementV1",
    "OrderTypeV1",
    "PluginProviderV2",
    "RuntimeEventEnvelopeV2",
    "SessionPhaseV1",
    "SideV1",
    "SourceAttributionV1",
    "TerminalOutcomeV1",
    "TimerMutationTypeV1",
    "TimerMutationV1",
    "VnpyCompatibilityRequirementV1",
    "validate_json_schema_instance_v1",
]
