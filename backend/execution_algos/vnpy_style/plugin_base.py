"""Shared pure state/effect machinery for the current-three v3 plugins."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_decimal_string_v1,
    canonical_utc_datetime_v1,
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoInitializationV1,
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CurrentThreeActiveOrderStateV3,
    CurrentThreeActiveOrderStatusV3,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DiagnosticSeverityV1,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    KernelCommandOutcomeEventPayloadV1,
    KernelCommandOutcomeV1,
    KernelOrderEventPayloadV1,
    KernelOrderReconcileEventPayloadV1,
    KernelTradeEventPayloadV1,
    NormalizedOrderStatusV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    TerminalOutcomeV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    algo_transition_id_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    strict_readback_kernel_event_payload_v1,
)


class CurrentThreePluginError(ValueError):
    pass


def plus_one_second_v1(value: str) -> str:
    instant = datetime.fromisoformat(canonical_utc_datetime_v1(value).replace("Z", "+00:00"))
    return canonical_utc_datetime_v1(instant + timedelta(seconds=1))


def _effect_hash(
    *,
    next_state: AlgoStateSnapshotV2,
    commands: tuple[BrokerCommandV2, ...],
    timers: tuple[TimerMutationV1, ...],
    diagnostics: tuple[DiagnosticObservationV1, ...],
    terminal_outcome: TerminalOutcomeV1 | None,
) -> str:
    return hash_hex_v1(
        "miniqmt_algo_effect_set_v1",
        {
            "next_state_sha256": next_state.state_sha256,
            "ordered_command_ids": [item.command_id for item in commands],
            "ordered_timer_mutation_ids": [item.mutation_identity_v1() for item in timers],
            "ordered_diagnostic_observation_ids": [item.observation_id for item in diagnostics],
            "terminal_outcome": None if terminal_outcome is None else terminal_outcome.value,
        },
    )


class EffectCollectorV3:
    def __init__(self, *, context: DeterministicExecutionContextV1, parent_intent_id: str, transition_id: str) -> None:
        self.context = context
        self.parent_intent_id = parent_intent_id
        self.transition_id = transition_id
        self.commands: list[BrokerCommandV2] = []
        self.timers: list[TimerMutationV1] = []
        self.diagnostics: list[DiagnosticObservationV1] = []
        self._ordinal = 0

    def submit(
        self,
        *,
        symbol: str,
        side: SideV1,
        price_decimal: str,
        quantity: int,
        reason_code: str,
        metadata: dict[str, Any],
    ) -> BrokerCommandV2:
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            runtime_id=self.context.runtime_id,
            algo_instance_id=self.context.algo_instance_id,
            parent_intent_id=self.parent_intent_id,
            transition_id=self.transition_id,
            ordinal=self._ordinal,
            local_vt_orderid=None,
            symbol=symbol,
            side=side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=price_decimal,
            quantity=quantity,
            owned_broker_order_id=None,
            reason_code=reason_code,
            metadata=metadata,
        )
        self.commands.append(command)
        self._ordinal += 1
        return command

    def cancel(self, *, item: CurrentThreeActiveOrderStateV3, reason_code: str) -> BrokerCommandV2:
        if item.broker_order_id is None:
            raise CurrentThreePluginError("cannot cancel an active order without durable broker identity")
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.CANCEL_ORDER,
            runtime_id=self.context.runtime_id,
            algo_instance_id=self.context.algo_instance_id,
            parent_intent_id=self.parent_intent_id,
            transition_id=self.transition_id,
            ordinal=self._ordinal,
            local_vt_orderid=item.local_vt_orderid,
            symbol=item.symbol,
            side=item.side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal=item.requested_price_decimal,
            quantity=item.requested_quantity,
            owned_broker_order_id=item.broker_order_id,
            reason_code=reason_code,
            metadata={"submit_command_id": item.submit_command_id},
        )
        self.commands.append(command)
        self._ordinal += 1
        return command

    def timer(self, *, timer_name: str, schedule_epoch: str, raw_due_at_utc: str) -> TimerMutationV1:
        mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
            algo_instance_id=self.context.algo_instance_id,
            transition_id=self.transition_id,
            ordinal=self._ordinal,
            timer_name=timer_name,
            schedule_epoch=schedule_epoch,
            due_at_exchange_utc=raw_due_at_utc,
            catch_up_policy="SKIP_MISSED",
            payload={"timer_name": timer_name, "raw_due_at_utc": raw_due_at_utc},
        )
        self.timers.append(mutation)
        self._ordinal += 1
        return mutation

    def cancel_timer(self, *, timer_name: str, schedule_epoch: str, reason_code: str) -> TimerMutationV1:
        mutation = TimerMutationV1.create(
            mutation_type=TimerMutationTypeV1.CANCEL,
            algo_instance_id=self.context.algo_instance_id,
            transition_id=self.transition_id,
            ordinal=self._ordinal,
            timer_name=timer_name,
            schedule_epoch=schedule_epoch,
            due_at_exchange_utc=None,
            catch_up_policy="SKIP_MISSED",
            payload={"reason_code": reason_code},
        )
        self.timers.append(mutation)
        self._ordinal += 1
        return mutation

    def diagnostic(
        self,
        reason_code: str,
        message: str,
        *,
        severity: DiagnosticSeverityV1 = DiagnosticSeverityV1.INFO,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.diagnostics.append(
            DiagnosticObservationV1.create(
                deterministic_context=self.context,
                transition_id=self.transition_id,
                ordinal=self._ordinal,
                severity=severity,
                reason_code=reason_code,
                message=message,
                context=context or {},
            )
        )
        self._ordinal += 1


class CurrentThreePluginBaseV3:
    ALGO_CODE: str
    ALGO_NAME: str

    def __init__(
        self,
        *,
        manifest: ExecutionAlgoPluginManifestV2,
        canonical_config: dict[str, Any],
    ) -> None:
        if manifest.algo_code != self.ALGO_CODE or manifest.plugin_version != "3.0.0":
            raise CurrentThreePluginError("plugin class and exact v3 manifest do not close")
        self.manifest = manifest
        self.config = dict(canonical_config)

    def restore_state(self, snapshot: AlgoStateSnapshotV2) -> AlgoStateSnapshotV2:
        if not isinstance(snapshot, AlgoStateSnapshotV2):
            raise TypeError("snapshot must be AlgoStateSnapshotV2")
        if (
            snapshot.plugin_id != self.manifest.plugin_id
            or snapshot.plugin_version != self.manifest.plugin_version
            or snapshot.plugin_manifest_sha256 != self.manifest.manifest_sha256
            or snapshot.state_schema_version != self.manifest.state_schema_version
        ):
            raise CurrentThreePluginError("state snapshot conflicts with the exact plugin manifest")
        return snapshot

    def _specific_initial_state(self, context: AlgoStartContextV1) -> dict[str, Any]:
        raise NotImplementedError

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1:
        if context.plugin_manifest != self.manifest or thaw_json_v1(context.plugin_config) != self.config:
            raise CurrentThreePluginError("initialize context conflicts with factory manifest/config")
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
            "last_tick_lineage": None,
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
        commands = tuple(collector.commands)
        timers = tuple(collector.timers)
        diagnostics = tuple(collector.diagnostics)
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
                next_state=next_state,
                commands=commands,
                timers=timers,
                diagnostics=diagnostics,
                terminal_outcome=None,
            ),
        )

    def _initialize_effects(self, *, context: AlgoStartContextV1, collector: EffectCollectorV3) -> None:
        return None

    def _transition_context(
        self,
        *,
        state: AlgoStateSnapshotV2,
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
    ) -> DeterministicExecutionContextV1:
        correlation = thaw_json_v1(event.correlation)
        session_phase = correlation.get("session_phase")
        exchange_trade_date = correlation.get("exchange_trade_date")
        session_epoch = correlation.get("session_epoch")
        if event.event_type is EventTypeV2.TIMER:
            timer_payload = thaw_json_v1(event.payload)
            session_epoch = session_epoch or timer_payload.get("schedule_epoch")
        if not all(type(value) is str and value for value in (session_phase, exchange_trade_date, session_epoch)):
            raise CurrentThreePluginError("event correlation is missing deterministic session identity")
        return DeterministicExecutionContextV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=state.algo_instance_id,
            event_id=event.event_id,
            delivery_id=services.delivery_id,
            plugin_manifest_sha256=self.manifest.manifest_sha256,
            transition_sequence=state.transition_sequence + 1,
            logical_time_utc=event.event_time_utc,
            exchange_trade_date=exchange_trade_date,
            session_epoch=session_epoch,
            session_phase=SessionPhaseV1(session_phase),
            input_projection_sha256=services.execution_projection_set.projection_set_sha256,
        )

    def transition(
        self,
        *,
        state: AlgoStateSnapshotV2,
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
    ) -> AlgoTransitionV1:
        plain = thaw_json_v1(state.state)
        if plain["algo_code"] != self.ALGO_CODE:
            raise CurrentThreePluginError("state algorithm identity conflicts with plugin class")
        context = self._transition_context(state=state, event=event, services=services)
        transition_id = algo_transition_id_v1(
            delivery_id=services.delivery_id,
            event_id=event.event_id,
            runtime_id=event.runtime_id,
            algo_instance_id=state.algo_instance_id,
            transition_sequence=context.transition_sequence,
        )
        collector = EffectCollectorV3(
            context=context,
            parent_intent_id=plain["parent_intent_id"],
            transition_id=transition_id,
        )
        terminal_outcome: TerminalOutcomeV1 | None = None
        if event.event_type is EventTypeV2.TICK:
            terminal_outcome = self._handle_tick(plain, event, services, collector)
        elif event.event_type is EventTypeV2.ORDER:
            terminal_outcome = self._handle_order(plain, event, collector)
        elif event.event_type is EventTypeV2.TRADE:
            terminal_outcome = self._handle_trade(plain, event, collector)
        elif event.event_type is EventTypeV2.COMMAND_OUTCOME:
            terminal_outcome = self._handle_command_outcome(plain, event, collector)
        elif event.event_type is EventTypeV2.RECONCILE:
            terminal_outcome = self._handle_reconcile(plain, event, collector)
        elif event.event_type is EventTypeV2.TIMER:
            terminal_outcome = self._handle_timer(plain, event, services, collector)
        elif event.event_type is EventTypeV2.EOD:
            terminal_outcome = self._handle_eod(plain, collector)
        elif event.event_type is EventTypeV2.SESSION:
            collector.diagnostic("K3_SESSION_OBSERVED", "session event produced no direct broker effect")
        else:
            raise CurrentThreePluginError(
                f"{self.ALGO_CODE} transition does not accept {event.event_type.value} events"
            )
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
        commands = tuple(collector.commands)
        timers = tuple(collector.timers)
        diagnostics = tuple(collector.diagnostics)
        return AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=next_state,
            broker_commands=commands,
            timer_mutations=timers,
            diagnostic_observations=diagnostics,
            terminal_outcome=terminal_outcome,
            effect_set_sha256=_effect_hash(
                next_state=next_state,
                commands=commands,
                timers=timers,
                diagnostics=diagnostics,
                terminal_outcome=terminal_outcome,
            ),
        )

    def _handle_tick(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        raise NotImplementedError

    def _handle_timer(
        self,
        state: dict[str, Any],
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
        collector: EffectCollectorV3,
    ) -> TerminalOutcomeV1 | None:
        raise CurrentThreePluginError(f"{self.ALGO_CODE} does not accept TIMER events")

    @staticmethod
    def _active_items(state: dict[str, Any]) -> list[CurrentThreeActiveOrderStateV3]:
        return [
            CurrentThreeActiveOrderStateV3.model_validate_json(json.dumps(item, sort_keys=True, separators=(",", ":")))
            for item in state["active_orders"]
        ]

    @staticmethod
    def _write_active_items(state: dict[str, Any], items: list[CurrentThreeActiveOrderStateV3]) -> None:
        items.sort(key=lambda item: item.local_vt_orderid)
        state["active_orders"] = [item.model_dump(mode="json") for item in items]

    @staticmethod
    def _find_item(
        items: list[CurrentThreeActiveOrderStateV3], local_vt_orderid: str
    ) -> tuple[int, CurrentThreeActiveOrderStateV3] | None:
        matches = [(index, item) for index, item in enumerate(items) if item.local_vt_orderid == local_vt_orderid]
        if len(matches) > 1:
            raise CurrentThreePluginError("duplicate active local order identity")
        return None if not matches else matches[0]

    @staticmethod
    def _tick_lineage(event: RuntimeEventEnvelopeV2) -> dict[str, Any]:
        payload = thaw_json_v1(event.payload)
        correlation = thaw_json_v1(event.correlation)
        generation = payload.get("generation")
        session_phase = payload.get("session_phase", correlation.get("session_phase"))
        exchange_time = payload.get("exchange_time_utc", event.event_time_utc)
        if type(generation) is not int or generation < 0:
            raise CurrentThreePluginError("TICK payload is missing strict generation")
        if session_phase not in {SessionPhaseV1.CONTINUOUS_AM.value, SessionPhaseV1.CONTINUOUS_PM.value}:
            raise CurrentThreePluginError("TICK is not a continuous-session market observation")
        return {
            "market_data_id": thaw_json_v1(event.source_identity)["market_data_id"],
            "event_id": event.event_id,
            "payload_sha256": event.payload_sha256,
            "generation": generation,
            "sequence": event.sequence,
            "exchange_time_utc": canonical_utc_datetime_v1(exchange_time),
            "session_phase": session_phase,
        }

    @staticmethod
    def _quote(event: RuntimeEventEnvelopeV2, *, side: SideV1, need_volume: bool) -> tuple[str, int | None]:
        return CurrentThreePluginBaseV3._quote_payload(thaw_json_v1(event.payload), side=side, need_volume=need_volume)

    @staticmethod
    def _quote_or_wait(
        event: RuntimeEventEnvelopeV2,
        *,
        side: SideV1,
        need_volume: bool,
        collector: EffectCollectorV3,
    ) -> tuple[str, int | None] | None:
        payload = thaw_json_v1(event.payload)
        price_key = "ask_price_1" if side is SideV1.BUY else "bid_price_1"
        volume_key = "ask_volume_1" if side is SideV1.BUY else "bid_volume_1"
        required = (price_key, volume_key) if need_volume else (price_key,)
        missing = [field for field in required if field not in payload]
        if missing:
            collector.diagnostic(
                "WAITING_FOR_MARKET_DATA",
                "current native quote observation is incomplete",
                context={"missing_fields": missing, "event_id": event.event_id},
            )
            return None
        return CurrentThreePluginBaseV3._quote_payload(payload, side=side, need_volume=need_volume)

    @staticmethod
    def _quote_payload(payload: dict[str, Any], *, side: SideV1, need_volume: bool) -> tuple[str, int | None]:
        price_key = "ask_price_1" if side is SideV1.BUY else "bid_price_1"
        volume_key = "ask_volume_1" if side is SideV1.BUY else "bid_volume_1"
        price = canonical_decimal_string_v1(payload[price_key], field_name=price_key, allow_zero=False)
        if not need_volume:
            return price, None
        volume = payload[volume_key]
        if type(volume) is not int or volume < 0:
            raise CurrentThreePluginError(f"{volume_key} must be a nonnegative strict integer")
        return price, volume

    @staticmethod
    def _pending_submit_item(*, command: BrokerCommandV2, lineage: dict[str, Any]) -> CurrentThreeActiveOrderStateV3:
        return CurrentThreeActiveOrderStateV3.create(
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
            market_data_lineage=lineage,
        )

    @staticmethod
    def _replace_item(item: CurrentThreeActiveOrderStateV3, **changes: Any) -> CurrentThreeActiveOrderStateV3:
        payload = item.model_dump(mode="python", exclude={"active_order_state_sha256"})
        payload.update(changes)
        return CurrentThreeActiveOrderStateV3.create(**payload)

    def _handle_order(
        self, state: dict[str, Any], event: RuntimeEventEnvelopeV2, collector: EffectCollectorV3
    ) -> TerminalOutcomeV1 | None:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelOrderEventPayloadV1):
            raise CurrentThreePluginError("ORDER event did not read back as the strict ORDER payload")
        items = self._active_items(state)
        found = self._find_item(items, payload.local_vt_orderid)
        if found is None:
            collector.diagnostic("K3_ORDER_CALLBACK_PRECEDED", "ORDER arrived after active state was already closed")
            return None
        index, item = found
        if payload.command_id != item.submit_command_id or (
            item.broker_order_id is not None and payload.broker_order_id != item.broker_order_id
        ):
            raise CurrentThreePluginError("ORDER command/broker lineage conflicts with active state")
        if payload.terminal:
            observed = payload.observed_cumulative_filled_quantity
            if observed is None or observed > item.cumulative_filled_quantity:
                items[index] = self._replace_item(
                    item,
                    broker_order_id=payload.broker_order_id,
                    status=CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
                    pending_command_type=None,
                    pending_command_id=None,
                    last_order_event_id=event.event_id,
                    terminal_order_status=payload.normalized_order_status,
                    terminal_observed_cumulative_filled_quantity=observed,
                )
                collector.diagnostic(
                    "K3_ORDER_AHEAD_OF_TRADE_PENDING" if observed is not None else "K3_ORDER_CUMULATIVE_UNAVAILABLE",
                    "terminal ORDER waits for exact TRADE and OMS reconciliation facts",
                )
            else:
                items.pop(index)
                if observed < item.cumulative_filled_quantity:
                    collector.diagnostic("K3_ORDER_CUMULATIVE_STALE", "ORDER cumulative trails exact TRADE facts")
        else:
            status = (
                CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                if payload.normalized_order_status is NormalizedOrderStatusV1.PARTIALLY_FILLED
                or item.cumulative_filled_quantity > 0
                else CurrentThreeActiveOrderStatusV3.SUBMITTED
            )
            items[index] = self._replace_item(
                item,
                broker_order_id=payload.broker_order_id,
                status=status,
                pending_command_type=None,
                pending_command_id=None,
                last_order_event_id=event.event_id,
            )
        self._write_active_items(state, items)
        self._sync_specific_active_state(state, items)
        return self._terminal_if_filled(state, items)

    def _handle_trade(
        self, state: dict[str, Any], event: RuntimeEventEnvelopeV2, collector: EffectCollectorV3
    ) -> TerminalOutcomeV1 | None:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelTradeEventPayloadV1):
            raise CurrentThreePluginError("TRADE event did not read back as the strict TRADE payload")
        items = self._active_items(state)
        found = self._find_item(items, payload.local_vt_orderid)
        if found is None:
            raise CurrentThreePluginError("TRADE has no exact active or terminal-trade-pending order")
        index, item = found
        if item.last_trade_event_id == event.event_id:
            collector.diagnostic("K3_TRADE_DUPLICATE", "duplicate TRADE event produced no quantity change")
            return None
        if payload.command_id != item.submit_command_id or payload.broker_order_id != item.broker_order_id:
            raise CurrentThreePluginError("TRADE command/broker lineage conflicts with active state")
        new_total = state["traded_quantity"] + payload.trade_quantity
        if new_total > state["parent_quantity"]:
            raise CurrentThreePluginError("TRADE quantity exceeds parent target")
        old_total = state["traded_quantity"]
        old_price = Decimal(state["traded_price_decimal"])
        trade_price = Decimal(payload.trade_price_decimal)
        state["traded_quantity"] = new_total
        state["traded_price_decimal"] = canonical_decimal_string_v1(
            (old_price * old_total + trade_price * payload.trade_quantity) / new_total
        )
        child_cumulative = item.cumulative_filled_quantity + payload.trade_quantity
        if child_cumulative > item.requested_quantity:
            raise CurrentThreePluginError("TRADE quantity exceeds child requested quantity")
        observed = item.terminal_observed_cumulative_filled_quantity
        if (
            item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
            and observed is not None
            and child_cumulative >= observed
        ):
            items.pop(index)
        else:
            items[index] = self._replace_item(
                item,
                cumulative_filled_quantity=child_cumulative,
                remaining_quantity=item.requested_quantity - child_cumulative,
                last_trade_event_id=event.event_id,
                status=(
                    CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
                    if item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
                    else CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                ),
            )
        self._write_active_items(state, items)
        self._sync_specific_active_state(state, items)
        if new_total < state["parent_quantity"]:
            collector.diagnostic(f"{self.ALGO_CODE}_TRADE_PARTIAL", "exact TRADE advanced partial fill state")
        return self._terminal_if_filled(state, items)

    def _handle_command_outcome(
        self, state: dict[str, Any], event: RuntimeEventEnvelopeV2, collector: EffectCollectorV3
    ) -> TerminalOutcomeV1 | None:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelCommandOutcomeEventPayloadV1):
            raise CurrentThreePluginError("COMMAND_OUTCOME event did not read back as its strict payload")
        if payload.outcome is KernelCommandOutcomeV1.CONFLICT:
            raise CurrentThreePluginError("COMMAND_OUTCOME reported a durable identity conflict")
        items = self._active_items(state)
        found = self._find_item(items, payload.local_vt_orderid)
        if found is None:
            collector.diagnostic(
                "K3_COMMAND_OUTCOME_CALLBACK_PRECEDED",
                "preceding callback already advanced or closed active state",
            )
            return None
        index, item = found
        if item.pending_command_id != payload.command_id:
            if item.last_command_outcome_event_id == event.event_id:
                collector.diagnostic("K3_COMMAND_OUTCOME_DUPLICATE", "duplicate outcome produced no state change")
                return None
            raise CurrentThreePluginError("COMMAND_OUTCOME does not own the pending command")
        if payload.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            if payload.outcome is KernelCommandOutcomeV1.ACCEPTED:
                if payload.broker_order_id is None:
                    raise CurrentThreePluginError("accepted SUBMIT outcome requires broker identity")
                accepted_item = self._replace_item(
                    item,
                    broker_order_id=payload.broker_order_id,
                    status=CurrentThreeActiveOrderStatusV3.SUBMITTED,
                    pending_command_type=None,
                    pending_command_id=None,
                    last_command_outcome_event_id=event.event_id,
                )
                if state["status"] == "STOPPED":
                    cancel = collector.cancel(
                        item=accepted_item,
                        reason_code="K3_STOPPED_LATE_SUBMIT_ACCEPTED_CANCEL",
                    )
                    accepted_item = self._replace_item(
                        accepted_item,
                        status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                        pending_command_type=BrokerCommandTypeV2.CANCEL_ORDER,
                        pending_command_id=cancel.command_id,
                    )
                items[index] = accepted_item
            elif payload.outcome in {KernelCommandOutcomeV1.REJECTED, KernelCommandOutcomeV1.PRE_CALL_TERMINAL}:
                items.pop(index)
                collector.diagnostic("K3_SUBMIT_NOT_ACCEPTED", "SUBMIT outcome closed the pending child")
            else:
                items[index] = self._replace_item(
                    item,
                    status=CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    last_command_outcome_event_id=event.event_id,
                )
        else:
            if payload.broker_order_id != item.broker_order_id:
                raise CurrentThreePluginError("CANCEL outcome broker identity conflicts with active order")
            if payload.outcome is KernelCommandOutcomeV1.ACCEPTED:
                items[index] = self._replace_item(
                    item,
                    status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                    last_command_outcome_event_id=event.event_id,
                )
            elif payload.outcome in {KernelCommandOutcomeV1.REJECTED, KernelCommandOutcomeV1.PRE_CALL_TERMINAL}:
                active_item = self._replace_item(
                    item,
                    status=(
                        CurrentThreeActiveOrderStatusV3.PARTIALLY_FILLED
                        if item.cumulative_filled_quantity > 0
                        else CurrentThreeActiveOrderStatusV3.SUBMITTED
                    ),
                    pending_command_type=None,
                    pending_command_id=None,
                    last_command_outcome_event_id=event.event_id,
                )
                if state["status"] == "STOPPED":
                    retry_cancel = collector.cancel(
                        item=active_item,
                        reason_code="K3_STOPPED_CANCEL_NOT_ACCEPTED_RETRY",
                    )
                    active_item = self._replace_item(
                        active_item,
                        status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                        pending_command_type=BrokerCommandTypeV2.CANCEL_ORDER,
                        pending_command_id=retry_cancel.command_id,
                    )
                items[index] = active_item
            else:
                items[index] = self._replace_item(
                    item,
                    status=CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
                    last_command_outcome_event_id=event.event_id,
                )
        self._write_active_items(state, items)
        self._sync_specific_active_state(state, items)
        return self._terminal_if_filled(state, items)

    def _handle_reconcile(
        self, state: dict[str, Any], event: RuntimeEventEnvelopeV2, collector: EffectCollectorV3
    ) -> TerminalOutcomeV1 | None:
        payload = strict_readback_kernel_event_payload_v1(event)
        if not isinstance(payload, KernelOrderReconcileEventPayloadV1):
            raise CurrentThreePluginError("RECONCILE event did not read back as its strict payload")
        items = self._active_items(state)
        found = self._find_item(items, payload.local_vt_orderid)
        if found is None:
            collector.diagnostic("K3_RECONCILE_CALLBACK_PRECEDED", "reconcile observed an already closed order")
            return None
        index, item = found
        if payload.broker_order_id != item.broker_order_id:
            raise CurrentThreePluginError("RECONCILE broker identity conflicts with active order")
        if payload.authoritative_cumulative_filled_quantity != item.cumulative_filled_quantity:
            collector.diagnostic(
                "K3_RECONCILE_TRADE_SET_INCOMPLETE",
                "OMS cumulative cannot advance quantity without exact TRADE events",
            )
            return None
        if payload.authoritative_remaining_quantity != item.remaining_quantity:
            raise CurrentThreePluginError("RECONCILE remaining quantity conflicts with exact TRADE-applied state")
        successor = self._replace_item(item, last_oms_reconcile_event_id=event.event_id)
        if payload.terminal and item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING:
            items.pop(index)
        else:
            items[index] = successor
        self._write_active_items(state, items)
        self._sync_specific_active_state(state, items)
        return self._terminal_if_filled(state, items)

    def _handle_eod(self, state: dict[str, Any], collector: EffectCollectorV3) -> TerminalOutcomeV1 | None:
        items = self._active_items(state)
        if not items:
            state["status"] = "FINISHED"
            state["finished_reason"] = "K3_EOD_EXPIRED_WITH_RESIDUAL"
            return TerminalOutcomeV1.EXPIRED_WITH_RESIDUAL
        state["status"] = "STOPPED"
        for index, item in enumerate(items):
            if item.status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING:
                continue
            if item.status in {
                CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
                CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
            }:
                collector.diagnostic("K3_EOD_COMMAND_PENDING", "EOD waits for the durable command lifecycle")
                continue
            command = collector.cancel(item=item, reason_code="K3_EOD_CANCEL_ACTIVE_CHILD")
            items[index] = self._replace_item(
                item,
                status=CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
                pending_command_type=BrokerCommandTypeV2.CANCEL_ORDER,
                pending_command_id=command.command_id,
            )
        self._write_active_items(state, items)
        self._sync_specific_active_state(state, items)
        return None

    @staticmethod
    def _terminal_if_filled(
        state: dict[str, Any], items: list[CurrentThreeActiveOrderStateV3]
    ) -> TerminalOutcomeV1 | None:
        if state["traded_quantity"] == state["parent_quantity"] and not items:
            state["status"] = "FINISHED"
            state["finished_reason"] = "K3_TARGET_QUANTITY_FILLED"
            return TerminalOutcomeV1.FILLED
        if state["status"] == "STOPPED" and not items:
            state["status"] = "FINISHED"
            state["finished_reason"] = "K3_STOPPED_CHILDREN_CLOSED_WITH_RESIDUAL"
            return TerminalOutcomeV1.EXPIRED_WITH_RESIDUAL
        return None

    def _sync_specific_active_state(self, state: dict[str, Any], items: list[CurrentThreeActiveOrderStateV3]) -> None:
        return None


__all__ = ["CurrentThreePluginBaseV3", "CurrentThreePluginError", "EffectCollectorV3", "plus_one_second_v1"]
