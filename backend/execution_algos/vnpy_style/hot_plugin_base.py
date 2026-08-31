"""V4 current-three base: process-local market decisions, durable economics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoInitializationV1,
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CurrentThreeActiveOrderStatusV3,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    RuntimeEventEnvelopeV2,
    TerminalOutcomeV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    SideV1,
    algo_transition_id_v1,
)

from .hot_plugin_manifests import CurrentThreeHotActiveOrderStateV4
from .plugin_base import CurrentThreePluginBaseV3, CurrentThreePluginError, EffectCollectorV3, _effect_hash
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
)


class CurrentThreeHotPluginBaseV4(CurrentThreePluginBaseV3):
    """Reuse callback/effect closure while replacing the durable market plane."""

    def __init__(self, *, manifest: ExecutionAlgoPluginManifestV2, canonical_config: dict[str, Any]) -> None:
        if manifest.algo_code != self.ALGO_CODE or manifest.plugin_version != "4.0.0":
            raise CurrentThreePluginError("hot plugin class and exact V4 manifest do not close")
        self.manifest = manifest
        self.config = dict(canonical_config)

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1:
        if context.plugin_manifest != self.manifest or thaw_json_v1(context.plugin_config) != self.config:
            raise CurrentThreePluginError("initialize context conflicts with V4 factory manifest/config")
        state = {
            "algo_name": self.ALGO_NAME,
            "algo_code": self.ALGO_CODE,
            "parent_intent_id": context.parent_intent_id,
            "symbol": context.symbol,
            "side": context.side.value,
            "offset": "NONE",
            "limit_price_decimal": context.limit_price_decimal,
            "parent_quantity": context.parent_quantity,
            "min_volume": context.min_volume,
            "volume_increment": context.volume_increment,
            "status": "RUNNING",
            "traded_quantity": 0,
            "traded_price_decimal": "0",
            "active_orders": [],
            "parameters": {},
            "variables": {},
            "finished_reason": None,
            **self._specific_initial_state(context),
        }
        transition_id = algo_transition_id_v1(
            delivery_id=context.start_delivery_id,
            event_id=context.start_event_id,
            runtime_id=context.runtime_id,
            algo_instance_id=context.algo_instance_id,
            transition_sequence=1,
        )
        collector = EffectCollectorV3(
            context=context.deterministic_context,
            parent_intent_id=context.parent_intent_id,
            transition_id=transition_id,
        )
        self._initialize_effects(context=context, collector=collector)
        next_state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=context.deterministic_context,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=context.start_delivery_id,
            last_closed_delivery_sequence=1,
            state=state,
            last_applied_event_id=context.start_event_id,
        )
        commands, timers, diagnostics = tuple(collector.commands), tuple(collector.timers), tuple(collector.diagnostics)
        return AlgoInitializationV1(
            schema_version="miniqmt_algo_initialization_v1",
            start_event_id=context.start_event_id,
            start_delivery_id=context.start_delivery_id,
            next_state=next_state,
            broker_commands=commands,
            timer_mutations=timers,
            diagnostic_observations=diagnostics,
            terminal_outcome=None,
            effect_set_sha256=_effect_hash(
                next_state=next_state, commands=commands, timers=timers, diagnostics=diagnostics, terminal_outcome=None
            ),
        )

    @staticmethod
    def _active_items(state: dict[str, Any]) -> list[CurrentThreeHotActiveOrderStateV4]:
        return [
            CurrentThreeHotActiveOrderStateV4.model_validate_json(
                json.dumps(item, sort_keys=True, separators=(",", ":"))
            )
            for item in state["active_orders"]
        ]

    @staticmethod
    def _write_active_items(state: dict[str, Any], items: list[CurrentThreeHotActiveOrderStateV4]) -> None:
        items.sort(key=lambda item: item.local_vt_orderid)
        state["active_orders"] = [item.model_dump(mode="json") for item in items]

    @staticmethod
    def _replace_item(item: CurrentThreeHotActiveOrderStateV4, **changes: Any) -> CurrentThreeHotActiveOrderStateV4:
        payload = item.model_dump(mode="python", exclude={"active_order_state_sha256"})
        payload.update(changes)
        return CurrentThreeHotActiveOrderStateV4.create(**payload)

    @staticmethod
    def _pending_submit_item(
        *, command: BrokerCommandV2, lineage: dict[str, Any] | None = None
    ) -> CurrentThreeHotActiveOrderStateV4:
        if lineage:
            raise CurrentThreePluginError("V4 active child forbids market-data lineage")
        return CurrentThreeHotActiveOrderStateV4.create(
            local_vt_orderid=command.local_vt_orderid,
            submit_command_id=command.command_id,
            broker_order_id=None,
            symbol=command.symbol,
            side=command.side,
            status=CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
            pending_command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            pending_command_id=command.command_id,
            requested_price_decimal=command.price_decimal,
            requested_quantity=command.quantity,
            cumulative_filled_quantity=0,
            remaining_quantity=command.quantity,
            last_order_event_id=None,
            last_trade_event_id=None,
            last_command_outcome_event_id=None,
            last_oms_reconcile_event_id=None,
            terminal_order_status=None,
            terminal_observed_cumulative_filled_quantity=None,
        )

    def transition(
        self, *, state: AlgoStateSnapshotV2, event: RuntimeEventEnvelopeV2, services: AlgoReadOnlyServicesV1
    ) -> AlgoTransitionV1:
        if event.event_type is not EventTypeV2.OPERATOR:
            if event.event_type is EventTypeV2.TICK:
                raise CurrentThreePluginError("V4 plugin cannot receive durable TICK")
            return super().transition(state=state, event=event, services=services)
        plain = thaw_json_v1(state.state)
        payload = thaw_json_v1(event.payload)
        if (
            payload.get("schema_version") != "miniqmt_hot_market_economic_action_v1"
            or payload.get("algo_instance_id") != state.algo_instance_id
        ):
            raise CurrentThreePluginError("hot economic action owner/schema conflict")
        context = self._transition_context(state=state, event=event, services=services)
        transition_id = algo_transition_id_v1(
            delivery_id=services.delivery_id,
            event_id=event.event_id,
            runtime_id=event.runtime_id,
            algo_instance_id=state.algo_instance_id,
            transition_sequence=context.transition_sequence,
        )
        collector = EffectCollectorV3(
            context=context, parent_intent_id=plain["parent_intent_id"], transition_id=transition_id
        )
        terminal = self._handle_hot_economic_action(plain, payload["economic_effect"], collector)
        next_state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=context,
            transition_sequence=context.transition_sequence,
            last_applied_delivery_sequence=context.transition_sequence,
            last_applied_delivery_id=services.delivery_id,
            last_closed_delivery_sequence=context.transition_sequence,
            state=plain,
            last_applied_event_id=event.event_id,
        )
        commands, timers, diagnostics = tuple(collector.commands), tuple(collector.timers), tuple(collector.diagnostics)
        return AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=next_state,
            broker_commands=commands,
            timer_mutations=timers,
            diagnostic_observations=diagnostics,
            terminal_outcome=terminal,
            effect_set_sha256=_effect_hash(
                next_state=next_state,
                commands=commands,
                timers=timers,
                diagnostics=diagnostics,
                terminal_outcome=terminal,
            ),
        )

    def _handle_hot_economic_action(
        self, state: dict[str, Any], effect: dict[str, Any], collector: EffectCollectorV3
    ) -> TerminalOutcomeV1 | None:
        action = effect.get("action")
        items = self._active_items(state)
        if action == "SUBMIT_LIMIT":
            if items:
                raise CurrentThreePluginError("hot SUBMIT requires no active child")
            command = collector.submit(
                symbol=effect["symbol"],
                side=SideV1(effect["side"]),
                price_decimal=effect["price_decimal"],
                quantity=effect["quantity"],
                reason_code=effect["reason_code"],
                metadata={},
            )
            items.append(self._pending_submit_item(command=command))
        elif action == "CANCEL_ORDER":
            if len(items) != 1:
                raise CurrentThreePluginError("hot CANCEL requires one exact active child")
            item = items[0]
            command = collector.cancel(item=item, reason_code=effect["reason_code"])
            items[0] = self._replace_item(
                item,
                status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                pending_command_type=command.command_type,
                pending_command_id=command.command_id,
            )
        else:
            raise CurrentThreePluginError("hot economic action is unsupported")
        self._write_active_items(state, items)
        self._after_hot_economic_action(state, effect, items)
        return None

    def _after_hot_economic_action(
        self, state: dict[str, Any], effect: dict[str, Any], items: list[CurrentThreeHotActiveOrderStateV4]
    ) -> None:
        self._sync_specific_active_state(state, items)


def _effect_v1(
    *, algo: ExecutionAlgoInstancePersistenceV2, view: HotMarketDataViewV1, payload: dict[str, Any]
) -> HotMarketDataEconomicEffectV1:
    economic = {
        **payload,
        "exchange_trade_date": view.exchange_trade_date,
        "session_epoch": view.session_epoch,
        "session_phase": view.session_phase,
        "action_time_utc": view.exchange_time_utc.isoformat().replace("+00:00", "Z"),
    }
    identity = "mqhoteffect_" + hash_hex_v1(
        "miniqmt_hot_market_economic_effect_v1",
        {
            "runtime_id": algo.runtime_id,
            "algo_instance_id": algo.algo_instance_id,
            "expected_algo_row_version": algo.row_version,
            "economic_effect": economic,
        },
    )
    return HotMarketDataEconomicEffectV1(
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        expected_algo_row_version=algo.row_version,
        effect_identity=identity,
        economic_payload=economic,
    )


@dataclass
class CurrentThreeHotTargetV4:
    algo: ExecutionAlgoInstancePersistenceV2

    @property
    def runtime_id(self) -> str:
        return self.algo.runtime_id

    @property
    def algo_instance_id(self) -> str:
        return self.algo.algo_instance_id

    @property
    def symbol(self) -> str:
        return self.algo.symbol

    def _state(self) -> dict[str, Any]:
        if self.algo.state_json is None:
            raise CurrentThreePluginError("hot target requires committed V4 state")
        return thaw_json_v1(self.algo.state_json)

    @staticmethod
    def _is_continuous_market_v1(view: HotMarketDataViewV1) -> bool:
        return view.session_phase in {"CONTINUOUS_AM", "CONTINUOUS_PM"}

    def accept_committed_effect_v1(self, effect: HotMarketDataEconomicEffectV1, readback: Any) -> None:
        if (
            effect.runtime_id != self.runtime_id
            or effect.algo_instance_id != self.algo_instance_id
            or effect.expected_algo_row_version != self.algo.row_version
        ):
            raise CurrentThreePluginError("hot effect does not close to the target predecessor")
        if not isinstance(readback, ExecutionAlgoInstancePersistenceV2):
            raise TypeError("hot effect readback must be ExecutionAlgoInstancePersistenceV2")
        try:
            readback.validate_successor_v1(self.algo)
        except (TypeError, ValueError) as exc:
            raise CurrentThreePluginError("hot effect readback is not a valid target successor") from exc
        if (
            readback.runtime_id != self.runtime_id
            or readback.algo_instance_id != self.algo_instance_id
            or readback.row_version != effect.expected_algo_row_version + 1
            or readback.status is not ExecutionAlgoPersistenceStatusV2.ACTIVE
        ):
            raise CurrentThreePluginError("hot effect readback does not close its exact algo successor")
        event_key = hash_hex_v1(
            "miniqmt_runtime_event_key_v2",
            {
                "schema_version": "miniqmt_runtime_event_envelope_v2",
                "runtime_id": effect.runtime_id,
                "event_type": EventTypeV2.OPERATOR.value,
                "source": "SIMULATION_RUNTIME_OPERATOR",
                "source_identity": {"operator_command_id": effect.effect_identity},
            },
        )
        expected_delivery_id = "mqdelivery_" + hash_hex_v1(
            "miniqmt_algo_event_delivery_identity_v1",
            {
                "event_id": f"mqrtevt_{event_key}",
                "algo_instance_id": effect.algo_instance_id,
                "plugin_manifest_sha256": readback.plugin_manifest_sha256,
            },
        )
        if readback.last_applied_delivery_id != expected_delivery_id:
            raise CurrentThreePluginError("hot effect readback does not close its exact applied delivery")
        self.algo = readback

    def evaluate_hot_market_data_v1(self, view: HotMarketDataViewV1) -> HotMarketDataEconomicEffectV1 | None:
        raise NotImplementedError

    @staticmethod
    def _active(state: dict[str, Any]) -> list[CurrentThreeHotActiveOrderStateV4]:
        return [CurrentThreeHotActiveOrderStateV4.model_validate(item, strict=True) for item in state["active_orders"]]

    def _cancel_or_wait(
        self, *, view: HotMarketDataViewV1, state: dict[str, Any], reason_code: str
    ) -> HotMarketDataEconomicEffectV1 | None:
        items = self._active(state)
        if not items:
            return None
        item = items[0]
        if item.status in {
            CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
            CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
            CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
            CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
        }:
            return None
        return _effect_v1(algo=self.algo, view=view, payload={"action": "CANCEL_ORDER", "reason_code": reason_code})

    @staticmethod
    def _price(view: HotMarketDataViewV1, side: SideV1, *, opposite: bool = False) -> Decimal:
        selected = SideV1.SELL if opposite and side is SideV1.BUY else SideV1.BUY if opposite else side
        return view.ask_price_1 if selected is SideV1.BUY else view.bid_price_1

    @staticmethod
    def _volume(view: HotMarketDataViewV1, side: SideV1) -> int:
        return view.ask_volume_1 if side is SideV1.BUY else view.bid_volume_1


__all__ = ["CurrentThreeHotPluginBaseV4", "CurrentThreeHotTargetV4", "_effect_v1"]
