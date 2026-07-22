"""Strict, side-effect-free MiniQMT execution plugin contracts (K1-A).

The models in this module are intentionally not imported by the product
runtime yet.  They define the frozen writer/readback schema consumed by later
K1/K2 slices without reaching into repositories, gateways, OMS, or broker SDKs.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

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
    )

    def canonical_payload_v1(self, *, exclude: set[str] | None = None) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude=exclude or set())

    @classmethod
    def model_validate_json(cls, json_data: str | bytes | bytearray, **kwargs: Any) -> Self:
        validate_json_text_v1(json_data)
        return super().model_validate_json(json_data, **kwargs)


class MiniQMTPluginReasonCode(StrEnum):
    MANIFEST_SCHEMA_INVALID = "MINIQMT_PLUGIN_MANIFEST_SCHEMA_INVALID"
    MANIFEST_HASH_CONFLICT = "MINIQMT_PLUGIN_MANIFEST_HASH_CONFLICT"
    REGISTRATION_CONFLICT = "MINIQMT_PLUGIN_REGISTRATION_CONFLICT"
    BINDING_INVALID = "MINIQMT_PLUGIN_BINDING_INVALID"
    CONFIG_SCHEMA_INVALID = "MINIQMT_PLUGIN_CONFIG_SCHEMA_INVALID"
    STATE_SCHEMA_INVALID = "MINIQMT_PLUGIN_STATE_SCHEMA_INVALID"
    CAPABILITY_UNSUPPORTED = "MINIQMT_PLUGIN_CAPABILITY_UNSUPPORTED"
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
        ("algo_instance_id", "plugin_manifest_sha256", "plugin_config_sha256", "parent_intent_id"),
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

    @model_validator(mode="after")
    def _validate_delivery(self) -> Self:
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
        algo_instance_id: str,
        plugin_id: str,
        plugin_version: str,
        plugin_manifest_sha256: str,
        state_schema_version: str,
        transition_sequence: int,
        last_applied_delivery_sequence: int,
        last_applied_delivery_id: str,
        last_closed_delivery_sequence: int,
        state: dict[str, Any],
        last_applied_event_id: str,
        updated_at_utc: Any,
    ) -> Self:
        return cls(
            schema_version="execution_algo_state_snapshot_v2",
            algo_instance_id=algo_instance_id,
            plugin_id=plugin_id,
            plugin_version=plugin_version,
            plugin_manifest_sha256=plugin_manifest_sha256,
            state_schema_version=state_schema_version,
            transition_sequence=transition_sequence,
            last_applied_delivery_sequence=last_applied_delivery_sequence,
            last_applied_delivery_id=last_applied_delivery_id,
            last_closed_delivery_sequence=last_closed_delivery_sequence,
            state=state,
            state_sha256=hash_hex_v1("execution_algo_state_v2", state),
            last_applied_event_id=last_applied_event_id,
            updated_at_utc=updated_at_utc,
        )

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
        local_vt_orderid: str,
        symbol: str,
        side: SideV1,
        order_type: OrderTypeV1,
        price_decimal: Any,
        quantity: int,
        owned_broker_order_id: str | None,
        reason_code: str,
        metadata: dict[str, Any],
        command_id: str,
    ) -> Self:
        normalized_price = canonical_decimal_string_v1(price_decimal, field_name="price_decimal", allow_zero=False)
        payload = {
            "schema_version": "miniqmt_broker_command_v2",
            "command_type": command_type.value,
            "runtime_id": runtime_id,
            "algo_instance_id": algo_instance_id,
            "parent_intent_id": parent_intent_id,
            "transition_id": transition_id,
            "ordinal": ordinal,
            "local_vt_orderid": local_vt_orderid,
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
            command_id=command_id,
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
        expected = hash_hex_v1("miniqmt_broker_command_payload_v2", self.business_payload_v1())
        if self.payload_sha256 != expected:
            raise ValueError("payload_sha256 does not match broker command closure")
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

    @model_validator(mode="after")
    def _validate_timer(self) -> Self:
        if self.mutation_type is TimerMutationTypeV1.CANCEL:
            if self.due_at_exchange_utc is not None or self.timer_occurrence_id is not None:
                raise ValueError("CANCEL timer mutation must not fabricate due/occurrence identity")
        elif self.due_at_exchange_utc is None or self.timer_occurrence_id is None:
            raise ValueError("UPSERT_ONE_SHOT requires due and timer occurrence identity")
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
        observation_id: str,
        runtime_id: str,
        algo_instance_id: str,
        event_id: str,
        transition_id: str,
        ordinal: int,
        severity: DiagnosticSeverityV1 | str,
        reason_code: str,
        message: str,
        context: dict[str, Any],
        observed_at_logical_utc: Any,
    ) -> Self:
        if type(severity) is str:
            severity = DiagnosticSeverityV1(severity)
        if not isinstance(severity, DiagnosticSeverityV1):
            raise TypeError("severity must be DiagnosticSeverityV1 or its exact value")
        return cls(
            schema_version="miniqmt_diagnostic_observation_v1",
            observation_id=observation_id,
            runtime_id=runtime_id,
            algo_instance_id=algo_instance_id,
            event_id=event_id,
            transition_id=transition_id,
            ordinal=ordinal,
            severity=severity,
            reason_code=reason_code,
            message=message,
            context=context,
            context_sha256=hash_hex_v1("miniqmt_diagnostic_context_v1", context),
            observed_at_logical_utc=observed_at_logical_utc,
        )

    @model_validator(mode="after")
    def _validate_observation(self) -> Self:
        expected = hash_hex_v1("miniqmt_diagnostic_context_v1", thaw_json_v1(self.context))
        if self.context_sha256 != expected:
            raise ValueError("context_sha256 does not match diagnostic context closure")
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
            "ordered_timer_mutation_ids": [
                item.timer_occurrence_id or item.schedule_id for item in self.timer_mutations
            ],
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
]
