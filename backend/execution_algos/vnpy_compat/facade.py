"""Transition-scoped, broker-neutral vn.py AlgoEngine facade for K4."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Self, TypeVar

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_decimal_string_v1,
    canonical_utc_datetime_v1,
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoInitializationV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DiagnosticSeverityV1,
    ExecutionAlgoPluginManifestV2,
    ExecutionCommandChildMappingV1,
    OrderTypeV1,
    SideV1,
    TerminalOutcomeV1,
)

from .facade_contracts import (
    VnpyFacadeCharacterizationManifestViewV1,
    VnpyFacadeCharacterizationStartContextV2,
    VnpyFacadeContractError,
    VnpyFacadeInitializationInputV1,
    VnpyFacadeInitializationInputV2,
    VnpyFacadeTransitionInputV1,
    VnpyFacadeTransitionInputV2,
    VnpyFacadeTraceCallV1,
    VnpyFacadeTraceEffectV1,
)
from .facade_projection import (
    ContractData,
    Direction,
    Offset,
    OrderData,
    OrderType,
    TickData,
    TradeData,
    build_pinned_round_to_v1,
    project_contract_data_v1,
    project_tick_data_v1,
)


def _facade_error(reason_code: str, message: str, **context: Any) -> VnpyFacadeContractError:
    return VnpyFacadeContractError(reason_code, message, context=context)


def _effect_set_sha256_v1(
    *,
    next_state: AlgoStateSnapshotV2,
    commands: tuple[BrokerCommandV2, ...],
    diagnostics: tuple[DiagnosticObservationV1, ...],
    terminal_outcome: TerminalOutcomeV1 | None,
) -> str:
    return hash_hex_v1(
        "miniqmt_algo_effect_set_v1",
        {
            "next_state_sha256": next_state.state_sha256,
            "ordered_command_ids": [item.command_id for item in commands],
            "ordered_timer_mutation_ids": [],
            "ordered_diagnostic_observation_ids": [item.observation_id for item in diagnostics],
            "terminal_outcome": terminal_outcome.value if terminal_outcome is not None else None,
        },
    )


def _diagnostic_json_v1(value: Any, *, depth: int = 0) -> Any:
    if depth >= 8:
        raise ValueError("facade diagnostic context exceeds maximum depth 8")
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("facade diagnostic context contains non-finite float")
        normalized = format(Decimal(str(value)), "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return "0" if normalized in {"", "-0"} else normalized
    if isinstance(value, Enum):
        return {
            "enum_owner": type(value).__name__,
            "member": value.name,
            "pinned_value": value.value,
        }
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise TypeError("facade diagnostic context keys must be strict strings")
        return {key: _diagnostic_json_v1(item, depth=depth + 1) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [_diagnostic_json_v1(item, depth=depth + 1) for item in value]
    raise TypeError(f"unsupported facade diagnostic value: {type(value).__name__}")


def _trace_json_v1(value: Any) -> Any:
    if isinstance(value, TickData):
        return {
            "object_type": "TickData",
            "vt_symbol": value.vt_symbol,
            "datetime": canonical_utc_datetime_v1(value.datetime, field_name="trace.tick.datetime"),
            "bid_price_1": canonical_decimal_string_v1(
                str(value.bid_price_1), field_name="trace.tick.bid_price_1", allow_zero=True
            ),
            "bid_volume_1": canonical_decimal_string_v1(
                str(value.bid_volume_1), field_name="trace.tick.bid_volume_1", allow_zero=True
            ),
            "ask_price_1": canonical_decimal_string_v1(
                str(value.ask_price_1), field_name="trace.tick.ask_price_1", allow_zero=True
            ),
            "ask_volume_1": canonical_decimal_string_v1(
                str(value.ask_volume_1), field_name="trace.tick.ask_volume_1", allow_zero=True
            ),
            "last_price": canonical_decimal_string_v1(
                str(value.last_price), field_name="trace.tick.last_price", allow_zero=True
            ),
            "limit_up": canonical_decimal_string_v1(
                str(value.limit_up), field_name="trace.tick.limit_up", allow_zero=True
            ),
            "limit_down": canonical_decimal_string_v1(
                str(value.limit_down), field_name="trace.tick.limit_down", allow_zero=True
            ),
        }
    if isinstance(value, ContractData):
        return {
            "object_type": "ContractData",
            "symbol": value.symbol,
            "exchange": _diagnostic_json_v1(value.exchange),
            "gateway_name": value.gateway_name,
            "min_volume": canonical_decimal_string_v1(
                str(value.min_volume), field_name="trace.contract.min_volume", allow_zero=False
            ),
            "pricetick": canonical_decimal_string_v1(
                str(value.pricetick), field_name="trace.contract.pricetick", allow_zero=False
            ),
        }
    if isinstance(value, OrderData):
        return {
            "object_type": "OrderData",
            "vt_orderid": value.vt_orderid,
            "status": _diagnostic_json_v1(value.status),
            "traded": canonical_decimal_string_v1(str(value.traded), field_name="trace.order.traded", allow_zero=True),
            "price": canonical_decimal_string_v1(str(value.price), field_name="trace.order.price", allow_zero=False),
        }
    if isinstance(value, TradeData):
        return {
            "object_type": "TradeData",
            "vt_orderid": value.vt_orderid,
            "vt_tradeid": value.vt_tradeid,
            "price": canonical_decimal_string_v1(str(value.price), field_name="trace.trade.price", allow_zero=False),
            "volume": canonical_decimal_string_v1(str(value.volume), field_name="trace.trade.volume", allow_zero=False),
            "datetime": (
                None
                if value.datetime is None
                else canonical_utc_datetime_v1(value.datetime, field_name="trace.trade.datetime")
            ),
        }
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise TypeError("facade trace object keys must be strict strings")
        return {key: _trace_json_v1(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [_trace_json_v1(item) for item in value]
    return _diagnostic_json_v1(value)


_TraceReturnT = TypeVar("_TraceReturnT")


class VnpyFacadeEffectCollectorV1:
    """Single-use ordinal owner for existing K2 effect constructors."""

    __slots__ = (
        "_bound_manifest",
        "_commands",
        "_deterministic_context",
        "_diagnostics",
        "_frozen",
        "_parent_intent_id",
        "_transition_id",
    )

    def __init__(
        self,
        *,
        deterministic_context: DeterministicExecutionContextV1,
        parent_intent_id: str,
        transition_id: str,
    ) -> None:
        self._deterministic_context = deterministic_context
        self._parent_intent_id = parent_intent_id
        self._transition_id = transition_id
        self._commands: list[BrokerCommandV2] = []
        self._diagnostics: list[DiagnosticObservationV1] = []
        self._frozen = False

    @classmethod
    def create(
        cls,
        deterministic_context: DeterministicExecutionContextV1,
        parent_intent_id: str,
        transition_id: str,
    ) -> Self:
        if not isinstance(deterministic_context, DeterministicExecutionContextV1):
            raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
        for field_name, value in (
            ("parent_intent_id", parent_intent_id),
            ("transition_id", transition_id),
        ):
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{field_name} must be a trim-stable strict string")
        return cls(
            deterministic_context=deterministic_context,
            parent_intent_id=parent_intent_id,
            transition_id=transition_id,
        )

    @property
    def deterministic_context(self) -> DeterministicExecutionContextV1:
        return self._deterministic_context

    @property
    def parent_intent_id(self) -> str:
        return self._parent_intent_id

    @property
    def transition_id(self) -> str:
        return self._transition_id

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    @property
    def broker_commands(self) -> tuple[BrokerCommandV2, ...]:
        return tuple(self._commands)

    @property
    def diagnostic_observations(self) -> tuple[DiagnosticObservationV1, ...]:
        return tuple(self._diagnostics)

    def _next_ordinal(self) -> int:
        if self._frozen:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "effect collector is already frozen",
                transition_id=self._transition_id,
            )
        return len(self._commands) + len(self._diagnostics)

    def append_submit(
        self,
        *,
        symbol: str,
        side: SideV1,
        price_decimal: str,
        quantity: int,
    ) -> BrokerCommandV2:
        ordinal = self._next_ordinal()
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            runtime_id=self._deterministic_context.runtime_id,
            algo_instance_id=self._deterministic_context.algo_instance_id,
            parent_intent_id=self._parent_intent_id,
            transition_id=self._transition_id,
            ordinal=ordinal,
            local_vt_orderid=None,
            symbol=symbol,
            side=side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=price_decimal,
            quantity=quantity,
            owned_broker_order_id=None,
            reason_code="MINIQMT_VNPY_FACADE_SUBMIT_LIMIT",
            metadata={"facade_schema_version": "miniqmt_vnpy_algo_engine_facade_v1"},
        )
        self._commands.append(command)
        return command

    def append_cancel(
        self,
        *,
        symbol: str,
        side: SideV1,
        price_decimal: str,
        quantity: int,
        local_vt_orderid: str,
        broker_order_id: str,
    ) -> BrokerCommandV2:
        ordinal = self._next_ordinal()
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.CANCEL_ORDER,
            runtime_id=self._deterministic_context.runtime_id,
            algo_instance_id=self._deterministic_context.algo_instance_id,
            parent_intent_id=self._parent_intent_id,
            transition_id=self._transition_id,
            ordinal=ordinal,
            local_vt_orderid=local_vt_orderid,
            symbol=symbol,
            side=side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=price_decimal,
            quantity=quantity,
            owned_broker_order_id=broker_order_id,
            reason_code="MINIQMT_VNPY_FACADE_CANCEL_ORDER",
            metadata={"facade_schema_version": "miniqmt_vnpy_algo_engine_facade_v1"},
        )
        self._commands.append(command)
        return command

    def append_diagnostic(
        self,
        *,
        severity: DiagnosticSeverityV1,
        reason_code: str,
        message: str,
        context: dict[str, Any],
    ) -> DiagnosticObservationV1:
        ordinal = self._next_ordinal()
        normalized_context = _diagnostic_json_v1(context)
        observation = DiagnosticObservationV1.create(
            deterministic_context=self._deterministic_context,
            transition_id=self._transition_id,
            ordinal=ordinal,
            severity=severity,
            reason_code=reason_code,
            message=message,
            context=normalized_context,
        )
        self._diagnostics.append(observation)
        return observation

    def freeze(
        self,
        next_state: AlgoStateSnapshotV2,
        terminal_outcome: TerminalOutcomeV1 | None,
    ) -> AlgoTransitionV1:
        if self._frozen:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "effect collector cannot be frozen twice",
                transition_id=self._transition_id,
            )
        if not isinstance(next_state, AlgoStateSnapshotV2):
            raise TypeError("next_state must be AlgoStateSnapshotV2")
        if terminal_outcome is not None and not isinstance(terminal_outcome, TerminalOutcomeV1):
            raise TypeError("terminal_outcome must be TerminalOutcomeV1 or None")
        next_state.validate_against_authority_v1(
            plugin_manifest=self._manifest_for_state_v1(next_state),
            deterministic_context=self._deterministic_context,
        )
        commands = tuple(self._commands)
        diagnostics = tuple(self._diagnostics)
        transition = AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=next_state,
            broker_commands=commands,
            timer_mutations=(),
            diagnostic_observations=diagnostics,
            terminal_outcome=terminal_outcome,
            effect_set_sha256=_effect_set_sha256_v1(
                next_state=next_state,
                commands=commands,
                diagnostics=diagnostics,
                terminal_outcome=terminal_outcome,
            ),
        )
        self._frozen = True
        return transition

    def freeze_initialization(self, next_state: AlgoStateSnapshotV2) -> AlgoInitializationV1:
        if self._frozen:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "effect collector cannot be frozen twice",
                transition_id=self._transition_id,
            )
        if not isinstance(next_state, AlgoStateSnapshotV2):
            raise TypeError("next_state must be AlgoStateSnapshotV2")
        next_state.validate_against_authority_v1(
            plugin_manifest=self._manifest_for_state_v1(next_state),
            deterministic_context=self._deterministic_context,
        )
        commands = tuple(self._commands)
        diagnostics = tuple(self._diagnostics)
        initialization = AlgoInitializationV1(
            schema_version="miniqmt_algo_initialization_v1",
            start_event_id=self._deterministic_context.event_id,
            start_delivery_id=self._deterministic_context.delivery_id,
            next_state=next_state,
            broker_commands=commands,
            timer_mutations=(),
            diagnostic_observations=diagnostics,
            terminal_outcome=None,
            effect_set_sha256=_effect_set_sha256_v1(
                next_state=next_state,
                commands=commands,
                diagnostics=diagnostics,
                terminal_outcome=None,
            ),
        )
        self._frozen = True
        return initialization

    def _manifest_for_state_v1(self, state: AlgoStateSnapshotV2) -> Any:
        """Resolve only the exact manifest carried by the bound facade."""

        manifest = getattr(self, "_bound_manifest", None)
        if manifest is None:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
                "collector was not bound to an invocation manifest",
                state_manifest_sha256=state.plugin_manifest_sha256,
            )
        return manifest

    def bind_manifest_v1(self, manifest: Any) -> None:
        if self._frozen or hasattr(self, "_bound_manifest"):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "collector manifest binding is single-use",
                transition_id=self._transition_id,
            )
        object.__setattr__(self, "_bound_manifest", manifest)


class VnpyFacadeTraceCollectorV2:
    """Transition-local full call/effect trace owner for pinned-source characterization."""

    __slots__ = ("_calls", "_effect_collector", "_frozen", "_vector_id")

    def __init__(
        self,
        *,
        vector_id: str,
        effect_collector: VnpyFacadeEffectCollectorV1,
    ) -> None:
        if type(vector_id) is not str or not vector_id or vector_id != vector_id.strip():
            raise TypeError("vector_id must be a trim-stable strict string")
        if not isinstance(effect_collector, VnpyFacadeEffectCollectorV1):
            raise TypeError("effect_collector must be VnpyFacadeEffectCollectorV1")
        if effect_collector.is_frozen:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "trace collector cannot bind an already frozen effect collector",
                context={"vector_id": vector_id},
            )
        self._vector_id = vector_id
        self._effect_collector = effect_collector
        self._calls: list[VnpyFacadeTraceCallV1 | None] = []
        self._frozen = False

    @property
    def vector_id(self) -> str:
        return self._vector_id

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    @property
    def ordered_calls(self) -> tuple[VnpyFacadeTraceCallV1, ...]:
        if any(item is None for item in self._calls):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "trace collector contains an unfinished call",
                context={"vector_id": self._vector_id},
            )
        return tuple(item for item in self._calls if item is not None)

    def invoke_v1(
        self,
        *,
        method_name: str,
        normalized_arguments: dict[str, Any],
        operation: Callable[[], _TraceReturnT],
    ) -> _TraceReturnT:
        if self._frozen:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "trace collector is already frozen",
                context={"vector_id": self._vector_id, "method_name": method_name},
            )
        normalized_args = _trace_json_v1(normalized_arguments)
        ordinal = len(self._calls)
        self._calls.append(None)
        diagnostic_start = len(self._effect_collector.diagnostic_observations)
        try:
            result = operation()
        except Exception as exc:
            diagnostics = self._effect_collector.diagnostic_observations[diagnostic_start:]
            self._calls[ordinal] = VnpyFacadeTraceCallV1.create(
                ordinal=ordinal,
                method_name=method_name,
                normalized_arguments=normalized_args,
                return_disposition="RAISED",
                normalized_return_or_null={
                    "exception_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
                    "reason_code": getattr(exc, "reason_code", None),
                },
                ordered_diagnostic_reason_codes=tuple(item.reason_code for item in diagnostics),
            )
            raise
        diagnostics = self._effect_collector.diagnostic_observations[diagnostic_start:]
        if result is None:
            disposition = "NONE"
            normalized_result = None
        elif result == "":
            disposition = "EMPTY_STRING"
            normalized_result = ""
        else:
            disposition = "VALUE"
            normalized_result = _trace_json_v1(result)
        self._calls[ordinal] = VnpyFacadeTraceCallV1.create(
            ordinal=ordinal,
            method_name=method_name,
            normalized_arguments=normalized_args,
            return_disposition=disposition,
            normalized_return_or_null=normalized_result,
            ordered_diagnostic_reason_codes=tuple(item.reason_code for item in diagnostics),
        )
        return result

    def freeze_v1(self) -> tuple[tuple[VnpyFacadeTraceCallV1, ...], tuple[VnpyFacadeTraceEffectV1, ...]]:
        if self._frozen:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "trace collector cannot be frozen twice",
                context={"vector_id": self._vector_id},
            )
        snapshot = self.snapshot_v1()
        self._frozen = True
        return snapshot

    def snapshot_v1(
        self,
    ) -> tuple[tuple[VnpyFacadeTraceCallV1, ...], tuple[VnpyFacadeTraceEffectV1, ...]]:
        """Read the exact current trace without weakening single-use freeze semantics."""

        carriers: list[BrokerCommandV2 | DiagnosticObservationV1] = [
            *self._effect_collector.broker_commands,
            *self._effect_collector.diagnostic_observations,
        ]
        carriers.sort(key=lambda item: item.ordinal)
        if tuple(item.ordinal for item in carriers) != tuple(range(len(carriers))):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "characterization effect ordinals are not contiguous",
                context={"vector_id": self._vector_id},
            )
        effects = tuple(VnpyFacadeTraceEffectV1.create(ordinal=item.ordinal, carrier=item) for item in carriers)
        return self.ordered_calls, effects


class VnpyAlgoEngineFacadeV1:
    """Exact six-method facade; it never owns a Gateway or repository."""

    __slots__ = (
        "_active_mappings",
        "_collector",
        "_contract",
        "_input",
        "_parent_intent_id",
        "_round_to",
        "_side",
        "_symbol",
        "_tick",
        "_trace_collector",
        "_trace_depth",
    )

    def __init__(self) -> None:
        raise TypeError("use VnpyAlgoEngineFacadeV1.create")

    @classmethod
    def create(
        cls,
        invocation_input: (
            VnpyFacadeInitializationInputV1
            | VnpyFacadeTransitionInputV1
            | VnpyFacadeInitializationInputV2
            | VnpyFacadeTransitionInputV2
        ),
        effect_collector: VnpyFacadeEffectCollectorV1,
    ) -> Self:
        if not isinstance(
            invocation_input,
            (
                VnpyFacadeInitializationInputV1,
                VnpyFacadeTransitionInputV1,
                VnpyFacadeInitializationInputV2,
                VnpyFacadeTransitionInputV2,
            ),
        ):
            raise TypeError("invocation_input must be an exact K4 facade input")
        if not isinstance(effect_collector, VnpyFacadeEffectCollectorV1):
            raise TypeError("effect_collector must be VnpyFacadeEffectCollectorV1")
        if effect_collector.is_frozen:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
                "cannot construct facade with a frozen collector",
            )
        if isinstance(invocation_input, (VnpyFacadeInitializationInputV1, VnpyFacadeInitializationInputV2)):
            context = invocation_input.start_context
            deterministic = context.deterministic_context
            parent_intent_id = context.parent_intent_id
            symbol = context.symbol
            side = context.side
            contract_payload = thaw_json_v1(context.contract_projection)
            tick_payload = None
            mappings: tuple[Any, ...] = ()
            manifest = context.plugin_manifest
        else:
            context = invocation_input.algo_instance
            deterministic = invocation_input.deterministic_context
            parent_intent_id = context.parent_intent_id
            symbol = context.symbol
            side = context.side
            contract_payload = (
                None
                if invocation_input.read_only_services.contract_projection is None
                else thaw_json_v1(invocation_input.read_only_services.contract_projection)
            )
            tick_payload = (
                None
                if invocation_input.read_only_services.market_data_projection is None
                else thaw_json_v1(invocation_input.read_only_services.market_data_projection)
            )
            mappings = invocation_input.ordered_active_mappings
            manifest = invocation_input.manifest
        if (
            effect_collector.deterministic_context != deterministic
            or effect_collector.parent_intent_id != parent_intent_id
            or (
                isinstance(invocation_input, (VnpyFacadeInitializationInputV1, VnpyFacadeInitializationInputV2))
                and effect_collector.transition_id != invocation_input.transition_id
            )
            or (
                isinstance(invocation_input, VnpyFacadeTransitionInputV1)
                and effect_collector.transition_id != invocation_input.delivery.transition_id
            )
            or (
                isinstance(invocation_input, VnpyFacadeTransitionInputV2)
                and effect_collector.transition_id != invocation_input.transition_id
            )
        ):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
                "collector owner conflicts with invocation input",
                transition_id=effect_collector.transition_id,
            )
        instance = object.__new__(cls)
        instance._input = invocation_input
        instance._collector = effect_collector
        instance._parent_intent_id = parent_intent_id
        instance._symbol = symbol
        instance._side = side
        instance._contract = instance._project_contract_v1(contract_payload)
        instance._tick = instance._project_tick_v1(tick_payload)
        instance._active_mappings = {item.local_vt_orderid: item for item in mappings}
        instance._round_to = build_pinned_round_to_v1()
        instance._trace_collector = None
        instance._trace_depth = 0
        effect_collector.bind_manifest_v1(manifest)
        return instance

    @classmethod
    def _create_characterization_v1(
        cls,
        *,
        deterministic_context: DeterministicExecutionContextV1,
        parent_intent_id: str,
        symbol: str,
        side: SideV1,
        contract: ContractData | None,
        tick: TickData | None,
        active_mappings: tuple[ExecutionCommandChildMappingV1, ...],
        manifest: ExecutionAlgoPluginManifestV2,
        effect_collector: VnpyFacadeEffectCollectorV1,
    ) -> Self:
        """Closed offline seam used only by the source characterization runner."""

        if not isinstance(deterministic_context, DeterministicExecutionContextV1):
            raise TypeError("deterministic_context must be DeterministicExecutionContextV1")
        if not isinstance(side, SideV1):
            raise TypeError("side must be SideV1")
        if contract is not None and not isinstance(contract, ContractData):
            raise TypeError("contract must be ContractData or None")
        if tick is not None and not isinstance(tick, TickData):
            raise TypeError("tick must be TickData or None")
        if not isinstance(manifest, ExecutionAlgoPluginManifestV2):
            raise TypeError("manifest must be ExecutionAlgoPluginManifestV2")
        if not isinstance(effect_collector, VnpyFacadeEffectCollectorV1):
            raise TypeError("effect_collector must be VnpyFacadeEffectCollectorV1")
        if type(active_mappings) is not tuple or any(
            not isinstance(item, ExecutionCommandChildMappingV1) for item in active_mappings
        ):
            raise TypeError("active_mappings must be a tuple of ExecutionCommandChildMappingV1")
        if (
            effect_collector.deterministic_context != deterministic_context
            or effect_collector.parent_intent_id != parent_intent_id
            or effect_collector.is_frozen
        ):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
                "characterization collector owner is not exact",
            )
        ordered = tuple(sorted(active_mappings, key=lambda item: item.local_vt_orderid))
        if active_mappings != ordered or len({item.local_vt_orderid for item in ordered}) != len(ordered):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
                "characterization active mappings must be unique and sorted",
            )
        if any(
            (item.runtime_id, item.algo_instance_id, item.parent_intent_id, item.symbol, item.side)
            != (
                deterministic_context.runtime_id,
                deterministic_context.algo_instance_id,
                parent_intent_id,
                symbol,
                side,
            )
            for item in ordered
        ):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
                "characterization mapping owner drifted",
            )
        instance = object.__new__(cls)
        instance._input = None
        instance._collector = effect_collector
        instance._parent_intent_id = parent_intent_id
        instance._symbol = symbol
        instance._side = side
        instance._contract = contract
        instance._tick = tick
        instance._active_mappings = {item.local_vt_orderid: item for item in ordered}
        instance._round_to = build_pinned_round_to_v1()
        instance._trace_collector = None
        instance._trace_depth = 0
        effect_collector.bind_manifest_v1(manifest)
        return instance

    @classmethod
    def _create_characterization_v2(
        cls,
        *,
        manifest_view: VnpyFacadeCharacterizationManifestViewV1,
        characterization_context: VnpyFacadeCharacterizationStartContextV2,
        trace_collector: VnpyFacadeTraceCollectorV2,
        contract: ContractData | None,
        tick: TickData | None,
        active_mappings: tuple[ExecutionCommandChildMappingV1, ...],
    ) -> Self:
        if not isinstance(manifest_view, VnpyFacadeCharacterizationManifestViewV1):
            raise TypeError("manifest_view must be VnpyFacadeCharacterizationManifestViewV1")
        if not isinstance(characterization_context, VnpyFacadeCharacterizationStartContextV2):
            raise TypeError("characterization_context must be VnpyFacadeCharacterizationStartContextV2")
        if not isinstance(trace_collector, VnpyFacadeTraceCollectorV2):
            raise TypeError("trace_collector must be VnpyFacadeTraceCollectorV2")
        if characterization_context.manifest_view != manifest_view:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "characterization manifest view differs from the start authority",
                context={"vector_id": characterization_context.vector_id},
            )
        effect_collector = trace_collector._effect_collector
        if (
            effect_collector.deterministic_context != characterization_context.deterministic_context
            or effect_collector.parent_intent_id != characterization_context.parent_intent_id
            or trace_collector.vector_id != characterization_context.vector_id
            or trace_collector.is_frozen
        ):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "characterization trace/effect/start owners do not close",
                context={"vector_id": characterization_context.vector_id},
            )
        ordered = tuple(sorted(active_mappings, key=lambda item: item.local_vt_orderid))
        if active_mappings != ordered or len({item.local_vt_orderid for item in ordered}) != len(ordered):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
                "characterization active mappings must be unique and sorted",
                context={"vector_id": characterization_context.vector_id},
            )
        if any(
            (
                item.runtime_id,
                item.algo_instance_id,
                item.parent_intent_id,
                item.symbol,
                item.side,
            )
            != (
                characterization_context.runtime_id,
                characterization_context.algo_instance_id,
                characterization_context.parent_intent_id,
                characterization_context.symbol,
                characterization_context.side,
            )
            for item in ordered
        ):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
                "characterization active mapping owner drifted",
                context={"vector_id": characterization_context.vector_id},
            )
        instance = object.__new__(cls)
        instance._input = None
        instance._collector = effect_collector
        instance._parent_intent_id = characterization_context.parent_intent_id
        instance._symbol = characterization_context.symbol
        instance._side = characterization_context.side
        instance._contract = contract
        instance._tick = tick
        instance._active_mappings = {item.local_vt_orderid: item for item in ordered}
        instance._round_to = build_pinned_round_to_v1()
        instance._trace_collector = trace_collector
        instance._trace_depth = 0
        effect_collector.bind_manifest_v1(manifest_view)
        return instance

    def _invoke_traced_v2(
        self,
        *,
        method_name: str,
        normalized_arguments: dict[str, Any],
        operation: Callable[[], _TraceReturnT],
    ) -> _TraceReturnT:
        trace = self._trace_collector
        if trace is None or self._trace_depth > 0:
            return operation()
        self._trace_depth += 1
        try:
            return trace.invoke_v1(
                method_name=method_name,
                normalized_arguments=normalized_arguments,
                operation=operation,
            )
        finally:
            self._trace_depth -= 1

    def _project_contract_v1(self, payload: Mapping[str, Any] | None) -> ContractData | None:
        if payload is None:
            return None
        required = {"gateway_name", "min_volume", "pricetick_decimal"}
        if not required.issubset(payload):
            return None
        return project_contract_data_v1(
            symbol=self._symbol,
            gateway_name=payload["gateway_name"],
            min_volume=payload["min_volume"],
            pricetick_decimal=payload["pricetick_decimal"],
        )

    def _project_tick_v1(self, payload: Mapping[str, Any] | None) -> TickData | None:
        if payload is None:
            return None
        return project_tick_data_v1(symbol=self._symbol, payload=dict(payload))

    def send_order(
        self,
        algo: Any,
        direction: Direction,
        price: float,
        volume: float,
        order_type: OrderType,
        offset: Offset,
    ) -> str:
        return self._invoke_traced_v2(
            method_name="send_order",
            normalized_arguments={
                "algo_instance_id": getattr(algo, "algo_name", None),
                "direction": direction,
                "price": price,
                "volume": volume,
                "order_type": order_type,
                "offset": offset,
            },
            operation=lambda: self._send_order_impl_v1(algo, direction, price, volume, order_type, offset),
        )

    def _send_order_impl_v1(
        self,
        algo: Any,
        direction: Direction,
        price: float,
        volume: float,
        order_type: OrderType,
        offset: Offset,
    ) -> str:
        self._assert_algo_owner_v1(algo)
        if (
            not isinstance(direction, Direction)
            or not isinstance(order_type, OrderType)
            or not isinstance(offset, Offset)
        ):
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
                "send_order requires exact pinned enum projections",
            )
        price_value = self._finite_number_v1(price, field_name="price", positive=True)
        volume_value = self._finite_number_v1(volume, field_name="volume", positive=False)
        if volume_value < 0:
            raise ValueError("volume must be finite non-negative")
        contract = self.get_contract(algo)
        if contract is None:
            return ""
        rounded = self._round_to(volume_value, contract.min_volume)
        if rounded == 0:
            self._collector.append_diagnostic(
                severity=DiagnosticSeverityV1.WARNING,
                reason_code="MINIQMT_VNPY_FACADE_ROUNDED_VOLUME_ZERO",
                message="pinned round_to produced zero volume",
                context={"symbol": self._symbol, "volume": volume_value, "min_volume": contract.min_volume},
            )
            return ""
        if not rounded.is_integer():
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
                "rounded volume is not integral shares",
                rounded_volume=rounded,
            )
        command = self._collector.append_submit(
            symbol=self._symbol,
            side=SideV1.BUY if direction is Direction.LONG else SideV1.SELL,
            price_decimal=canonical_decimal_string_v1(str(price_value), field_name="price", allow_zero=False),
            quantity=int(rounded),
        )
        return command.local_vt_orderid

    def cancel_order(self, algo: Any, vt_orderid: str) -> None:
        return self._invoke_traced_v2(
            method_name="cancel_order",
            normalized_arguments={
                "algo_instance_id": getattr(algo, "algo_name", None),
                "vt_orderid": vt_orderid,
            },
            operation=lambda: self._cancel_order_impl_v1(algo, vt_orderid),
        )

    def _cancel_order_impl_v1(self, algo: Any, vt_orderid: str) -> None:
        self._assert_algo_owner_v1(algo)
        if type(vt_orderid) is not str or not vt_orderid or vt_orderid != vt_orderid.strip():
            raise TypeError("vt_orderid must be a trim-stable strict string")
        mapping = self._active_mappings.get(vt_orderid)
        if mapping is None or mapping.broker_order_id is None:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
                "cancel target is not an active durable-owned broker order",
                local_vt_orderid=vt_orderid,
            )
        self._collector.append_cancel(
            symbol=mapping.symbol,
            side=mapping.side,
            price_decimal=mapping.requested_price_decimal,
            quantity=mapping.requested_quantity,
            local_vt_orderid=mapping.local_vt_orderid,
            broker_order_id=mapping.broker_order_id,
        )

    def get_tick(self, algo: Any) -> TickData | None:
        return self._invoke_traced_v2(
            method_name="get_tick",
            normalized_arguments={"algo_instance_id": getattr(algo, "algo_name", None)},
            operation=lambda: self._get_tick_impl_v1(algo),
        )

    def _get_tick_impl_v1(self, algo: Any) -> TickData | None:
        self._assert_algo_owner_v1(algo)
        if self._tick is None:
            self._collector.append_diagnostic(
                severity=DiagnosticSeverityV1.WARNING,
                reason_code="MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE",
                message="immutable market-data projection is unavailable",
                context={"symbol": self._symbol},
            )
        return self._tick

    def get_contract(self, algo: Any) -> ContractData | None:
        return self._invoke_traced_v2(
            method_name="get_contract",
            normalized_arguments={"algo_instance_id": getattr(algo, "algo_name", None)},
            operation=lambda: self._get_contract_impl_v1(algo),
        )

    def _get_contract_impl_v1(self, algo: Any) -> ContractData | None:
        self._assert_algo_owner_v1(algo)
        if self._contract is None:
            self._collector.append_diagnostic(
                severity=DiagnosticSeverityV1.WARNING,
                reason_code="MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE",
                message="immutable contract projection is unavailable",
                context={"symbol": self._symbol},
            )
        return self._contract

    def write_log(self, msg: str, algo: Any | None = None) -> None:
        return self._invoke_traced_v2(
            method_name="write_log",
            normalized_arguments={
                "msg": msg,
                "algo_instance_id": None if algo is None else getattr(algo, "algo_name", None),
            },
            operation=lambda: self._write_log_impl_v1(msg, algo),
        )

    def _write_log_impl_v1(self, msg: str, algo: Any | None = None) -> None:
        if algo is not None:
            self._assert_algo_owner_v1(algo)
        if type(msg) is not str:
            raise TypeError("msg must be a strict string")
        original = msg
        truncated = original[:2048]
        self._collector.append_diagnostic(
            severity=DiagnosticSeverityV1.INFO,
            reason_code="MINIQMT_VNPY_FACADE_ALGO_LOG",
            message=truncated,
            context={
                "message_truncated": len(original) > 2048,
                "original_length": len(original),
                "full_message_sha256": hashlib.sha256(original.encode("utf-8")).hexdigest(),
            },
        )

    def put_algo_event(self, algo: Any, data: dict[str, Any]) -> None:
        return self._invoke_traced_v2(
            method_name="put_algo_event",
            normalized_arguments={
                "algo_instance_id": getattr(algo, "algo_name", None),
                "data": data,
            },
            operation=lambda: self._put_algo_event_impl_v1(algo, data),
        )

    def _put_algo_event_impl_v1(self, algo: Any, data: dict[str, Any]) -> None:
        self._assert_algo_owner_v1(algo)
        if type(data) is not dict or any(type(key) is not str for key in data):
            raise TypeError("algo event data must be a strict string-keyed dict")
        self._collector.append_diagnostic(
            severity=DiagnosticSeverityV1.INFO,
            reason_code="MINIQMT_VNPY_FACADE_ALGO_PROJECTION",
            message="algorithm parameter/variable projection",
            context={"projection": data},
        )

    def _assert_algo_owner_v1(self, algo: Any) -> None:
        if algo is None or getattr(algo, "algo_name", None) != self._collector.deterministic_context.algo_instance_id:
            raise _facade_error(
                "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
                "algorithm object does not match transition owner",
                expected_algo_instance_id=self._collector.deterministic_context.algo_instance_id,
                actual_algo_name=getattr(algo, "algo_name", None),
            )

    @staticmethod
    def _finite_number_v1(value: Any, *, field_name: str, positive: bool) -> float:
        if type(value) not in (int, float):
            raise TypeError(f"{field_name} must be a strict number and not bool")
        normalized = float(value)
        if not math.isfinite(normalized) or (positive and normalized <= 0):
            raise ValueError(f"{field_name} must be finite positive")
        return normalized


__all__ = ["VnpyAlgoEngineFacadeV1", "VnpyFacadeEffectCollectorV1", "VnpyFacadeTraceCollectorV2"]
