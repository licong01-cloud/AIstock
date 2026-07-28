"""Read-only K3-B legacy policy, state and dependent-BUY inventory."""

from __future__ import annotations

from collections import Counter
from decimal import InvalidOperation
from typing import Any

from backend.execution_algos.vnpy_style.plugin_manifests import (
    LegacyProjectionDriftV1,
    current_three_manifests_v3,
    project_legacy_vnpy_policy_v1,
)

from .kernel_current_three_contracts import (
    CurrentThreeDependentBuyCompletenessV1,
    CurrentThreeDependentBuyInventoryV1,
    CurrentThreeDependentBuyStatusV1,
    CurrentThreeFailureV1,
    CurrentThreeInventoryDispositionV1,
    CurrentThreeLegacyEvidenceRefV1,
    CurrentThreeLegacyInventorySetV1,
    CurrentThreeLegacyStateInventoryV1,
    bounded_failures_v1,
    legacy_evidence_set_sha256_v1,
)
from .kernel_current_three_shadow_source import CurrentThreeShadowRepositoryReadV1, _canonical_legacy_fact
from .models import MiniQMTAlgoInstanceStatus, MiniQMTChildOrderStatus, MiniQMTExecutionEventType
from .plugin_canonical import canonical_decimal_string_v1, hash_hex_v1, thaw_json_v1
from .plugin_contracts import SideV1


def _hash(domain: str, value: Any) -> str:
    return hash_hex_v1(domain, _canonical_legacy_fact(value))


def _legacy_ref(*, identity: str, payload_hash: str, logical_time: Any) -> CurrentThreeLegacyEvidenceRefV1:
    return CurrentThreeLegacyEvidenceRefV1.create(
        identity=identity,
        payload_sha256=payload_hash,
        logical_time_utc=logical_time,
    )


def _raw_config(metadata: dict[str, Any], failures: list[CurrentThreeFailureV1]) -> dict[str, Any]:
    aliases = [(name, metadata.get(name)) for name in ("config", "setting", "algo_setting") if name in metadata]
    if not aliases:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.config",
                reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                context={"reason": "legacy config is missing"},
            )
        )
        return {}
    if any(not isinstance(value, dict) for _, value in aliases):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.config",
                reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                context={"alias_types": {name: type(value).__name__ for name, value in aliases}},
            )
        )
        return {}
    canonical = aliases[0][1]
    conflicts = [name for name, value in aliases[1:] if value != canonical]
    if conflicts:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.config",
                reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                context={"reason": "legacy config aliases conflict", "conflicting_aliases": conflicts},
            )
        )
        return {}
    return dict(canonical)


def _event_owner_id(read: CurrentThreeShadowRepositoryReadV1, event: Any) -> str | None:
    payload = event.payload
    candidates: set[str] = set()
    direct = payload.get("algo_instance_id")
    if type(direct) is str and direct:
        candidates.add(direct)
    child_id = payload.get("child_order_id")
    if type(child_id) is str and child_id:
        candidates.update(item.algo_instance_id for item in read.children if item.child_order_id == child_id)
    broker_id = payload.get("broker_order_id")
    if type(broker_id) is str and broker_id:
        candidates.update(item.algo_instance_id for item in read.children if item.broker_order_id == broker_id)
    parent_id = payload.get("parent_intent_id")
    if type(parent_id) is str and parent_id:
        candidates.update(item.algo_instance_id for item in read.algos if item.parent_intent_id == parent_id)
    return next(iter(candidates)) if len(candidates) == 1 else None


def _event_refs_for_algo(
    read: CurrentThreeShadowRepositoryReadV1,
    *,
    algo_instance_id: str,
    event_type: MiniQMTExecutionEventType,
) -> tuple[CurrentThreeLegacyEvidenceRefV1, ...]:
    snapshot_by_id = {item.event_id: item for item in read.snapshot.ordered_legacy_event_refs}
    refs = [
        _legacy_ref(
            identity=event.event_id,
            payload_hash=snapshot_by_id[event.event_id].payload_sha256,
            logical_time=event.event_time,
        )
        for event in read.events
        if event.event_type is event_type and _event_owner_id(read, event) == algo_instance_id
    ]
    return tuple(sorted(refs, key=lambda item: item.identity))


def _dependent_buy_inventory(
    read: CurrentThreeShadowRepositoryReadV1,
    *,
    algo: Any,
    metadata: dict[str, Any],
) -> CurrentThreeDependentBuyInventoryV1 | None:
    present = any(
        key in metadata
        for key in (
            "dependent_buy",
            "dependent_buy_status",
            "dependent_buy_contract",
            "dependent_buy_action",
            "dependent_buy_history",
        )
    )
    if not present:
        return None
    failures: list[CurrentThreeFailureV1] = []
    raw_status = metadata.get("dependent_buy_status")
    status_map = {
        "DEFERRED": CurrentThreeDependentBuyStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        "RELEASED": CurrentThreeDependentBuyStatusV1.RELEASED_SUBMITTED,
        "BLOCKED": CurrentThreeDependentBuyStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
        "EOD_RESIDUAL": CurrentThreeDependentBuyStatusV1.EOD_RESIDUAL,
    }
    normalized = status_map.get(raw_status, CurrentThreeDependentBuyStatusV1.INVALID_VISIBLE)
    if normalized is CurrentThreeDependentBuyStatusV1.INVALID_VISIBLE:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_status",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual": raw_status},
            )
        )
    reason_code = metadata.get("dependent_buy_reason_code")
    if type(reason_code) is not str or not reason_code.strip():
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_reason_code",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual_type": type(reason_code).__name__},
            )
        )
        reason_code = None
    contract = metadata.get("dependent_buy_contract")
    action = metadata.get("dependent_buy_action")
    if contract is not None and not isinstance(contract, dict):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_contract",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual_type": type(contract).__name__},
            )
        )
        contract = None
    if action is not None and not isinstance(action, dict):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_action",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual_type": type(action).__name__},
            )
        )
        action = None
    raw_sell_ids = (contract or {}).get("sell_parent_intent_ids", [])
    if not isinstance(raw_sell_ids, list) or any(type(item) is not str or not item.strip() for item in raw_sell_ids):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_contract.sell_parent_intent_ids",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual_type": type(raw_sell_ids).__name__},
            )
        )
        sell_ids: tuple[str, ...] = ()
    else:
        sell_ids = tuple(sorted(set(raw_sell_ids)))
    strategy_id = metadata.get("dependent_buy_strategy_id")
    if type(strategy_id) is not str or not strategy_id.strip():
        strategy_id = None
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_strategy_id",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"reason": "strategy identity is missing"},
            )
        )
    required_cash: str | None = None
    raw_cash = metadata.get("dependent_buy_required_cash")
    try:
        if raw_cash is not None:
            required_cash = canonical_decimal_string_v1(str(raw_cash), field_name="required_cash", allow_zero=False)
    except (TypeError, ValueError, InvalidOperation):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_required_cash",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"actual": raw_cash},
            )
        )
    if required_cash is None:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_required_cash",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"reason": "positive canonical required cash is missing"},
            )
        )
    if not sell_ids:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_contract.sell_parent_intent_ids",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"reason": "at least one sell parent identity is required"},
            )
        )
    ledger_source = metadata.get("dependent_buy_ledger_authority_source")
    ledger_context = metadata.get("dependent_buy_ledger_observation_context")
    ledger_hash = (
        _hash("miniqmt_current_three_dependent_buy_ledger_observation_v1", ledger_context)
        if ledger_context is not None
        else None
    )
    released_child = metadata.get("dependent_buy_released_child_order_id")
    if type(released_child) is not str or not released_child.strip():
        released_child = None
    if normalized is CurrentThreeDependentBuyStatusV1.RELEASED_SUBMITTED and (released_child is None or action is None):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_release",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={
                    "released_child_present": released_child is not None,
                    "action_present": action is not None,
                },
            )
        )
    if normalized is not CurrentThreeDependentBuyStatusV1.RELEASED_SUBMITTED and released_child is not None:
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_released_child_order_id",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_INVENTORY_INVALID",
                context={"normalized_status": normalized.value},
            )
        )
        released_child = None
    semantic_failure_count = len(failures)
    if normalized in {
        CurrentThreeDependentBuyStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        CurrentThreeDependentBuyStatusV1.RELEASED_SUBMITTED,
    } and (ledger_hash is None or ledger_source != "qmt_strategy_ledger.virtual_account.cash"):
        failures.append(
            CurrentThreeFailureV1.create(
                field_path="metadata.dependent_buy_ledger_observation_context",
                reason_code="MINIQMT_K3_DEPENDENT_BUY_COORDINATOR_NOT_MIGRATED",
                context={"reason": "historical ledger identity is unavailable"},
            )
        )
        completeness = (
            CurrentThreeDependentBuyCompletenessV1.HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE
            if semantic_failure_count == 0
            else CurrentThreeDependentBuyCompletenessV1.INVALID_VISIBLE
        )
        if semantic_failure_count:
            normalized = CurrentThreeDependentBuyStatusV1.INVALID_VISIBLE
    elif failures:
        completeness = CurrentThreeDependentBuyCompletenessV1.INVALID_VISIBLE
        normalized = CurrentThreeDependentBuyStatusV1.INVALID_VISIBLE
    else:
        completeness = CurrentThreeDependentBuyCompletenessV1.COMPLETE
    trigger_refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...] = ()
    trigger_hash = legacy_evidence_set_sha256_v1(
        "miniqmt_current_three_dependent_buy_trigger_event_set_v1", trigger_refs
    )
    ordered_failures = bounded_failures_v1(failures)
    payload = {
        "schema_version": "miniqmt_current_three_dependent_buy_inventory_v1",
        "runtime_id": read.snapshot.runtime_id,
        "buy_algo_instance_id": algo.algo_instance_id,
        "buy_parent_intent_id": algo.parent_intent_id,
        "strategy_id": strategy_id,
        "ordered_sell_parent_intent_ids": sell_ids,
        "required_cash_decimal": required_cash,
        "observed_status": raw_status if type(raw_status) is str and raw_status else None,
        "observed_reason_code": reason_code,
        "normalized_status": normalized,
        "raw_metadata_sha256": _hash("miniqmt_current_three_dependent_buy_raw_metadata_v1", metadata),
        "dependent_buy_contract_sha256": _hash("miniqmt_current_three_dependent_buy_contract_v1", contract)
        if contract is not None
        else None,
        "dependent_buy_action_sha256": _hash("miniqmt_current_three_dependent_buy_action_v1", action)
        if action is not None
        else None,
        "ledger_authority_source": ledger_source if type(ledger_source) is str and ledger_source else None,
        "ledger_observation_context_sha256": ledger_hash,
        "released_child_order_id": released_child,
        "ordered_trigger_event_refs": trigger_refs,
        "trigger_event_set_sha256": trigger_hash,
        "ordered_failures": ordered_failures,
        "evidence_completeness": completeness,
        "observation_only": True,
        "runtime_effect_applied": False,
    }
    coordination_hash = hash_hex_v1(
        "miniqmt_current_three_dependent_buy_inventory_v1",
        CurrentThreeDependentBuyInventoryV1.model_construct(
            **payload, coordination_ref_sha256="0" * 64
        ).canonical_payload_v1(exclude={"coordination_ref_sha256"}),
    )
    return CurrentThreeDependentBuyInventoryV1(**payload, coordination_ref_sha256=coordination_hash)


def build_current_three_legacy_inventory_set_v1(
    read: CurrentThreeShadowRepositoryReadV1,
) -> tuple[CurrentThreeLegacyInventorySetV1, tuple[CurrentThreeDependentBuyInventoryV1, ...]]:
    if not isinstance(read, CurrentThreeShadowRepositoryReadV1):
        raise TypeError("read must be CurrentThreeShadowRepositoryReadV1")
    read.strict_readback_v1()
    manifests = {item.algo_code: item for item in current_three_manifests_v3()}
    child_ref_by_id = {item.identity: item for item in read.snapshot.ordered_child_fact_refs}
    items: list[CurrentThreeLegacyStateInventoryV1] = []
    dependent_items: list[CurrentThreeDependentBuyInventoryV1] = []
    terminal_statuses = {
        MiniQMTAlgoInstanceStatus.COMPLETED,
        MiniQMTAlgoInstanceStatus.CANCELLED,
        MiniQMTAlgoInstanceStatus.FAILED,
    }
    active_child_statuses = {
        MiniQMTChildOrderStatus.SUBMITTING,
        MiniQMTChildOrderStatus.SUBMITTED,
        MiniQMTChildOrderStatus.PARTIALLY_FILLED,
    }
    for algo in sorted(read.algos, key=lambda item: item.algo_instance_id):
        metadata = dict(algo.metadata)
        failures: list[CurrentThreeFailureV1] = []
        raw_config = _raw_config(metadata, failures)
        projection = None
        try:
            projection = project_legacy_vnpy_policy_v1(algo.algo_code, raw_config)
        except (TypeError, ValueError) as exc:
            failures.append(
                CurrentThreeFailureV1.create(
                    field_path="metadata.config",
                    reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                    context={"error_type": type(exc).__name__, "message": str(exc)},
                )
            )
        candidate_manifest = manifests.get(algo.algo_code)
        candidate_key: str | None = None
        candidate_config_hash: str | None = None
        projection_receipt = "0" * 64
        if projection is not None:
            projection_receipt = projection.receipt_sha256
            if (
                projection.drift_classification
                in {
                    LegacyProjectionDriftV1.NO_DRIFT,
                    LegacyProjectionDriftV1.ALIAS_EQUIVALENT,
                }
                and candidate_manifest is not None
            ):
                candidate_key = f"{candidate_manifest.plugin_id}@{candidate_manifest.plugin_version}"
                candidate_config_hash = hash_hex_v1(
                    "miniqmt_plugin_config_v2", thaw_json_v1(projection.candidate_canonical_config)
                )
            else:
                failures.append(
                    CurrentThreeFailureV1.create(
                        field_path="legacy_policy_projection",
                        reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                        context={"drift_classification": projection.drift_classification.value},
                    )
                )
        legacy_state = metadata.get("legacy_state", metadata.get("state"))
        if legacy_state is None and algo.status not in terminal_statuses:
            failures.append(
                CurrentThreeFailureV1.create(
                    field_path="metadata.legacy_state",
                    reason_code="MINIQMT_K3_LEGACY_STATE_INVENTORY_INVALID",
                    context={"reason": "active legacy state evidence is missing"},
                )
            )
        child_rows = [item for item in read.children if item.algo_instance_id == algo.algo_instance_id]
        child_refs = tuple(
            _legacy_ref(
                identity=item.child_order_id,
                payload_hash=child_ref_by_id[item.child_order_id].payload_sha256,
                logical_time=item.updated_at,
            )
            for item in sorted(child_rows, key=lambda item: item.child_order_id)
        )
        order_refs = _event_refs_for_algo(
            read, algo_instance_id=algo.algo_instance_id, event_type=MiniQMTExecutionEventType.ORDER_EVENT
        )
        trade_refs = _event_refs_for_algo(
            read, algo_instance_id=algo.algo_instance_id, event_type=MiniQMTExecutionEventType.TRADE_EVENT
        )
        dependent = _dependent_buy_inventory(read, algo=algo, metadata=metadata)
        if dependent is not None:
            dependent_items.append(dependent)
        if failures:
            disposition = CurrentThreeInventoryDispositionV1.INVALID_VISIBLE
            candidate_key = None
            candidate_config_hash = None
        elif algo.status in terminal_statuses:
            disposition = CurrentThreeInventoryDispositionV1.TERMINAL_NO_WRITE
        elif any(item.status in active_child_statuses for item in child_rows):
            disposition = CurrentThreeInventoryDispositionV1.ACTIVE_LEGACY_OWNER
        else:
            disposition = CurrentThreeInventoryDispositionV1.SESSION_BOUNDARY_ELIGIBLE
        ordered_failures = bounded_failures_v1(failures)
        payload = {
            "schema_version": "miniqmt_current_three_legacy_state_inventory_v1",
            "runtime_id": read.snapshot.runtime_id,
            "trade_date": read.snapshot.trade_date,
            "legacy_algo_instance_id": algo.algo_instance_id,
            "parent_intent_id": algo.parent_intent_id,
            "strategy_slot_id": algo.strategy_slot_id,
            "symbol": algo.symbol,
            "side": SideV1(algo.side.value),
            "target_quantity": algo.target_quantity,
            "algo_code": algo.algo_code,
            "legacy_metadata_sha256": _hash("miniqmt_current_three_legacy_metadata_v1", metadata),
            "legacy_state_sha256": _hash("miniqmt_current_three_legacy_state_v1", legacy_state)
            if legacy_state is not None
            else None,
            "ordered_child_fact_refs": child_refs,
            "child_fact_set_sha256": legacy_evidence_set_sha256_v1(
                "miniqmt_current_three_legacy_child_fact_set_v1", child_refs
            ),
            "ordered_order_event_refs": order_refs,
            "order_event_set_sha256": legacy_evidence_set_sha256_v1(
                "miniqmt_current_three_legacy_order_event_set_v1", order_refs
            ),
            "ordered_trade_event_refs": trade_refs,
            "trade_event_set_sha256": legacy_evidence_set_sha256_v1(
                "miniqmt_current_three_legacy_trade_event_set_v1", trade_refs
            ),
            "legacy_policy_projection_receipt_sha256": projection_receipt,
            "candidate_plugin_key": candidate_key,
            "candidate_plugin_config_sha256": candidate_config_hash,
            "candidate_state_schema_version": None,
            "candidate_state_sha256": None,
            "dependent_buy_coordination_ref": dependent.coordination_ref_sha256 if dependent is not None else None,
            "ordered_failures": ordered_failures,
            "disposition": disposition,
            "observation_only": True,
            "runtime_effect_applied": False,
        }
        inventory_hash = hash_hex_v1(
            "miniqmt_current_three_legacy_state_inventory_v1",
            CurrentThreeLegacyStateInventoryV1.model_construct(
                **payload, inventory_sha256="0" * 64
            ).canonical_payload_v1(exclude={"inventory_sha256"}),
        )
        items.append(CurrentThreeLegacyStateInventoryV1(**payload, inventory_sha256=inventory_hash))
    ordered_items = tuple(sorted(items, key=lambda item: (item.runtime_id, item.legacy_algo_instance_id)))
    item_set_hash = hash_hex_v1(
        "miniqmt_current_three_legacy_inventory_item_set_v1",
        [
            {
                "runtime_id": item.runtime_id,
                "legacy_algo_instance_id": item.legacy_algo_instance_id,
                "inventory_sha256": item.inventory_sha256,
            }
            for item in ordered_items
        ],
    )
    counts = dict(sorted(Counter(item.disposition.value for item in ordered_items).items()))
    set_payload = {
        "schema_version": "miniqmt_current_three_legacy_inventory_set_v1",
        "repository_commit_sha": read.snapshot.repository_commit_sha,
        "trade_date": read.snapshot.trade_date,
        "observed_at_database_utc": read.snapshot.database_snapshot_at_utc,
        "ordered_inventory_items": ordered_items,
        "inventory_item_set_sha256": item_set_hash,
        "total_count": len(ordered_items),
        "counts_by_disposition": counts,
    }
    set_hash = hash_hex_v1(
        "miniqmt_current_three_legacy_inventory_set_v1",
        {
            **set_payload,
            "ordered_inventory_items": [item.canonical_payload_v1() for item in ordered_items],
        },
    )
    inventory_set = CurrentThreeLegacyInventorySetV1(**set_payload, set_sha256=set_hash)
    return inventory_set, tuple(sorted(dependent_items, key=lambda item: item.buy_algo_instance_id))


__all__ = ["build_current_three_legacy_inventory_set_v1"]
