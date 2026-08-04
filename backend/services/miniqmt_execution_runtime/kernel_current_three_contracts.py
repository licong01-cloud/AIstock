"""Strict K3-B inventory, shadow-source and parity carriers.

The module is deliberately pure.  It owns canonical identities and readback
validation, but never reads a repository, invokes a plugin or calls a broker.
"""

from __future__ import annotations

from collections import Counter
from enum import StrEnum
import re
from typing import Any, Literal, Self

from pydantic import model_validator

from .plugin_canonical import hash_hex_v1, json_safe_evidence_v1, thaw_json_v1
from .plugin_contracts import (
    CanonicalDecimalV1,
    FrozenJsonFieldV1,
    FrozenStrictModel,
    IdentityV1,
    NonNegativeIntV1,
    PositiveIntV1,
    Sha256V1,
    SideV1,
    UtcDateTimeV1,
)

MAX_K3_FAILURES = 256


class CurrentThreeContractError(ValueError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


class CurrentThreeInventoryDispositionV1(StrEnum):
    TERMINAL_NO_WRITE = "TERMINAL_NO_WRITE"
    ACTIVE_LEGACY_OWNER = "ACTIVE_LEGACY_OWNER"
    SESSION_BOUNDARY_ELIGIBLE = "SESSION_BOUNDARY_ELIGIBLE"
    INVALID_VISIBLE = "INVALID_VISIBLE"


class CurrentThreeDependentBuyCompletenessV1(StrEnum):
    COMPLETE = "COMPLETE"
    HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE = "HISTORICAL_LEDGER_IDENTITY_UNAVAILABLE"
    INVALID_VISIBLE = "INVALID_VISIBLE"


class CurrentThreeDependentBuyStatusV1(StrEnum):
    DEFERRED_WAITING_SELL_PROCEEDS = "DEFERRED_WAITING_SELL_PROCEEDS"
    RELEASED_SUBMITTED = "RELEASED_SUBMITTED"
    BLOCKED_SELL_PROCEEDS_UNAVAILABLE = "BLOCKED_SELL_PROCEEDS_UNAVAILABLE"
    EOD_RESIDUAL = "EOD_RESIDUAL"
    INVALID_VISIBLE = "INVALID_VISIBLE"


class CurrentThreeParityStatusV1(StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"


class CurrentThreeFailureV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_failure_v1"]
    field_path: IdentityV1
    reason_code: IdentityV1
    context: FrozenJsonFieldV1
    context_sha256: Sha256V1

    @classmethod
    def create(cls, *, field_path: str, reason_code: str, context: Any) -> Self:
        safe = json_safe_evidence_v1(context)
        return cls(
            schema_version="miniqmt_current_three_failure_v1",
            field_path=field_path,
            reason_code=reason_code,
            context=safe,
            context_sha256=hash_hex_v1("miniqmt_current_three_failure_context_v1", safe),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        if self.context_sha256 != hash_hex_v1("miniqmt_current_three_failure_context_v1", thaw_json_v1(self.context)):
            raise ValueError("current-three failure context hash mismatch")
        return self

    def sort_key_v1(self) -> tuple[str, str, str]:
        return (self.field_path, self.reason_code, self.context_sha256)


def bounded_failures_v1(items: list[CurrentThreeFailureV1]) -> tuple[CurrentThreeFailureV1, ...]:
    ordered = sorted(items, key=lambda item: item.sort_key_v1())
    if len(ordered) <= MAX_K3_FAILURES:
        return tuple(ordered)
    omitted = ordered[MAX_K3_FAILURES - 1 :]
    omitted_payload = [item.canonical_payload_v1() for item in omitted]
    marker = CurrentThreeFailureV1.create(
        field_path="__truncated__",
        reason_code="MINIQMT_K3_FAILURE_SET_TRUNCATED",
        context={
            "omitted_count": len(omitted),
            "omitted_failure_set_sha256": hash_hex_v1("miniqmt_current_three_omitted_failure_set_v1", omitted_payload),
        },
    )
    return (*ordered[: MAX_K3_FAILURES - 1], marker)


def _validate_failure_order(items: tuple[CurrentThreeFailureV1, ...]) -> None:
    if len(items) > MAX_K3_FAILURES:
        raise ValueError("ordered_failures exceeds the bounded evidence limit")
    markers = [index for index, item in enumerate(items) if item.field_path == "__truncated__"]
    if markers:
        if markers != [len(items) - 1] or len(items) != MAX_K3_FAILURES:
            raise ValueError("failure truncation marker must be the unique final bounded item")
        ordered_prefix = tuple(sorted(items[:-1], key=lambda item: item.sort_key_v1()))
        if items[:-1] != ordered_prefix:
            raise ValueError("ordered_failures prefix must be stable sorted")
    elif items != tuple(sorted(items, key=lambda item: item.sort_key_v1())):
        raise ValueError("ordered_failures must be stable sorted")


class CurrentThreeLegacyEvidenceRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_legacy_evidence_ref_v1"]
    identity: IdentityV1
    payload_sha256: Sha256V1
    logical_time_utc: UtcDateTimeV1
    ref_sha256: Sha256V1

    @classmethod
    def create(cls, *, identity: str, payload_sha256: str, logical_time_utc: Any) -> Self:
        from .plugin_canonical import canonical_utc_datetime_v1

        payload = {
            "schema_version": "miniqmt_current_three_legacy_evidence_ref_v1",
            "identity": identity,
            "payload_sha256": payload_sha256,
            "logical_time_utc": canonical_utc_datetime_v1(logical_time_utc, field_name="logical_time_utc"),
        }
        return cls(**payload, ref_sha256=hash_hex_v1("miniqmt_current_three_legacy_evidence_ref_v1", payload))

    @model_validator(mode="after")
    def _validate_ref(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_current_three_legacy_evidence_ref_v1",
            self.canonical_payload_v1(exclude={"ref_sha256"}),
        )
        if self.ref_sha256 != expected:
            raise ValueError("legacy evidence ref hash mismatch")
        return self


def legacy_evidence_set_sha256_v1(domain: str, refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...]) -> str:
    if refs != tuple(sorted(refs, key=lambda item: item.identity)):
        raise ValueError("legacy evidence refs must be ordered by identity")
    identities = [item.identity for item in refs]
    if any(count != 1 for count in Counter(identities).values()):
        raise ValueError("legacy evidence refs contain duplicate identity")
    return hash_hex_v1(domain, [{"identity": item.identity, "ref_sha256": item.ref_sha256} for item in refs])


class CurrentThreeDependentBuyInventoryV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_dependent_buy_inventory_v1"]
    runtime_id: IdentityV1
    buy_algo_instance_id: IdentityV1
    buy_parent_intent_id: IdentityV1
    strategy_id: IdentityV1 | None
    ordered_sell_parent_intent_ids: tuple[IdentityV1, ...]
    required_cash_decimal: CanonicalDecimalV1 | None
    observed_status: IdentityV1 | None
    observed_reason_code: IdentityV1 | None
    normalized_status: CurrentThreeDependentBuyStatusV1
    raw_metadata_sha256: Sha256V1
    dependent_buy_contract_sha256: Sha256V1 | None
    dependent_buy_action_sha256: Sha256V1 | None
    ledger_authority_source: IdentityV1 | None
    ledger_observation_context_sha256: Sha256V1 | None
    released_child_order_id: IdentityV1 | None
    ordered_trigger_event_refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...]
    trigger_event_set_sha256: Sha256V1
    ordered_failures: tuple[CurrentThreeFailureV1, ...]
    evidence_completeness: CurrentThreeDependentBuyCompletenessV1
    observation_only: Literal[True]
    runtime_effect_applied: Literal[False]
    coordination_ref_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if self.ordered_sell_parent_intent_ids != tuple(sorted(set(self.ordered_sell_parent_intent_ids))):
            raise ValueError("sell parent intent identities must be sorted and unique")
        expected_triggers = legacy_evidence_set_sha256_v1(
            "miniqmt_current_three_dependent_buy_trigger_event_set_v1", self.ordered_trigger_event_refs
        )
        if self.trigger_event_set_sha256 != expected_triggers:
            raise ValueError("dependent-buy trigger event set hash mismatch")
        _validate_failure_order(self.ordered_failures)
        if self.evidence_completeness is CurrentThreeDependentBuyCompletenessV1.COMPLETE:
            if self.ordered_failures:
                raise ValueError("complete dependent-buy inventory cannot carry failures")
            if (
                self.strategy_id is None
                or not self.ordered_sell_parent_intent_ids
                or self.required_cash_decimal is None
            ):
                raise ValueError("complete dependent-buy inventory lacks strategy, sell parent or cash authority")
        elif not self.ordered_failures:
            raise ValueError("incomplete dependent-buy inventory must retain at least one failure")
        released = self.normalized_status is CurrentThreeDependentBuyStatusV1.RELEASED_SUBMITTED
        if released:
            if (
                self.released_child_order_id is None
                or self.dependent_buy_action_sha256 is None
                or self.ledger_authority_source != "qmt_strategy_ledger.virtual_account.cash"
            ):
                raise ValueError("released dependent-buy inventory lacks child, action or ledger authority")
        elif self.released_child_order_id is not None:
            raise ValueError("non-released dependent-buy inventory cannot carry a released child")
        expected = hash_hex_v1(
            "miniqmt_current_three_dependent_buy_inventory_v1",
            self.canonical_payload_v1(exclude={"coordination_ref_sha256"}),
        )
        if self.coordination_ref_sha256 != expected:
            raise ValueError("dependent-buy coordination hash mismatch")
        return self


class CurrentThreeLegacyStateInventoryV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_legacy_state_inventory_v1"]
    runtime_id: IdentityV1
    trade_date: IdentityV1
    legacy_algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    symbol: IdentityV1
    side: SideV1
    target_quantity: PositiveIntV1
    algo_code: IdentityV1
    legacy_metadata_sha256: Sha256V1
    legacy_state_sha256: Sha256V1 | None
    ordered_child_fact_refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...]
    child_fact_set_sha256: Sha256V1
    ordered_order_event_refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...]
    order_event_set_sha256: Sha256V1
    ordered_trade_event_refs: tuple[CurrentThreeLegacyEvidenceRefV1, ...]
    trade_event_set_sha256: Sha256V1
    legacy_policy_projection_receipt_sha256: Sha256V1
    candidate_plugin_key: IdentityV1 | None
    candidate_plugin_config_sha256: Sha256V1 | None
    candidate_state_schema_version: IdentityV1 | None
    candidate_state_sha256: Sha256V1 | None
    dependent_buy_coordination_ref: Sha256V1 | None
    ordered_failures: tuple[CurrentThreeFailureV1, ...]
    disposition: CurrentThreeInventoryDispositionV1
    observation_only: Literal[True]
    runtime_effect_applied: Literal[False]
    inventory_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.trade_date) is None:
            raise ValueError("legacy inventory trade_date must be YYYY-MM-DD")
        sets = (
            (
                self.ordered_child_fact_refs,
                self.child_fact_set_sha256,
                "miniqmt_current_three_legacy_child_fact_set_v1",
            ),
            (
                self.ordered_order_event_refs,
                self.order_event_set_sha256,
                "miniqmt_current_three_legacy_order_event_set_v1",
            ),
            (
                self.ordered_trade_event_refs,
                self.trade_event_set_sha256,
                "miniqmt_current_three_legacy_trade_event_set_v1",
            ),
        )
        for refs, supplied, domain in sets:
            if supplied != legacy_evidence_set_sha256_v1(domain, refs):
                raise ValueError(f"{domain} hash mismatch")
        _validate_failure_order(self.ordered_failures)
        if self.disposition is CurrentThreeInventoryDispositionV1.INVALID_VISIBLE and not self.ordered_failures:
            raise ValueError("invalid legacy inventory must retain at least one failure")
        if self.disposition is not CurrentThreeInventoryDispositionV1.INVALID_VISIBLE and self.ordered_failures:
            raise ValueError("valid legacy inventory cannot carry failures")
        expected = hash_hex_v1(
            "miniqmt_current_three_legacy_state_inventory_v1",
            self.canonical_payload_v1(exclude={"inventory_sha256"}),
        )
        if self.inventory_sha256 != expected:
            raise ValueError("legacy inventory hash mismatch")
        return self


class CurrentThreeLegacyInventorySetV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_legacy_inventory_set_v1"]
    repository_commit_sha: IdentityV1
    trade_date: IdentityV1
    observed_at_database_utc: UtcDateTimeV1
    ordered_inventory_items: tuple[CurrentThreeLegacyStateInventoryV1, ...]
    inventory_item_set_sha256: Sha256V1
    total_count: NonNegativeIntV1
    counts_by_disposition: FrozenJsonFieldV1
    set_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_set(self) -> Self:
        if re.fullmatch(r"[0-9a-f]{40}", self.repository_commit_sha) is None:
            raise ValueError("repository_commit_sha must be an exact lowercase git commit")
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.trade_date) is None:
            raise ValueError("inventory trade_date must be YYYY-MM-DD")
        ordered = tuple(
            sorted(self.ordered_inventory_items, key=lambda item: (item.runtime_id, item.legacy_algo_instance_id))
        )
        if self.ordered_inventory_items != ordered:
            raise ValueError("inventory items must be sorted by runtime and legacy algo identity")
        identities = [(item.runtime_id, item.legacy_algo_instance_id) for item in ordered]
        if len(set(identities)) != len(identities):
            raise ValueError("inventory items contain duplicate identity")
        expected_items = hash_hex_v1(
            "miniqmt_current_three_legacy_inventory_item_set_v1",
            [
                {
                    "runtime_id": item.runtime_id,
                    "legacy_algo_instance_id": item.legacy_algo_instance_id,
                    "inventory_sha256": item.inventory_sha256,
                }
                for item in ordered
            ],
        )
        expected_counts = dict(Counter(item.disposition.value for item in ordered))
        if (
            self.inventory_item_set_sha256 != expected_items
            or self.total_count != len(ordered)
            or thaw_json_v1(self.counts_by_disposition) != dict(sorted(expected_counts.items()))
        ):
            raise ValueError("legacy inventory set count or item authority mismatch")
        expected = hash_hex_v1(
            "miniqmt_current_three_legacy_inventory_set_v1",
            self.canonical_payload_v1(exclude={"set_sha256"}),
        )
        if self.set_sha256 != expected:
            raise ValueError("legacy inventory set hash mismatch")
        return self


class CurrentThreeShadowEventRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_shadow_event_ref_v1"]
    event_id: IdentityV1
    sequence: PositiveIntV1
    event_type: IdentityV1
    event_source: IdentityV1
    payload_sha256: Sha256V1
    event_time_utc: UtcDateTimeV1
    ref_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_current_three_shadow_event_ref_v1", self.canonical_payload_v1(exclude={"ref_sha256"})
        )
        if self.ref_sha256 != expected:
            raise ValueError("shadow event ref hash mismatch")
        return self


class CurrentThreeShadowFactRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_shadow_fact_ref_v1"]
    identity: IdentityV1
    owner_identity: IdentityV1
    payload: FrozenJsonFieldV1
    payload_sha256: Sha256V1
    logical_time_utc: UtcDateTimeV1
    ref_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        if self.payload_sha256 != hash_hex_v1(
            "miniqmt_current_three_shadow_fact_payload_v1", thaw_json_v1(self.payload)
        ):
            raise ValueError("shadow fact payload hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_current_three_shadow_fact_ref_v1", self.canonical_payload_v1(exclude={"ref_sha256"})
        )
        if self.ref_sha256 != expected:
            raise ValueError("shadow fact ref hash mismatch")
        return self


class CurrentThreeShadowSourceSnapshotV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_shadow_source_snapshot_v1"]
    repository_commit_sha: IdentityV1
    runtime_id: IdentityV1
    trade_date: IdentityV1
    database_snapshot_at_utc: UtcDateTimeV1
    ordered_legacy_event_refs: tuple[CurrentThreeShadowEventRefV1, ...]
    event_count: NonNegativeIntV1
    event_set_sha256: Sha256V1
    ordered_child_fact_refs: tuple[CurrentThreeShadowFactRefV1, ...]
    child_count: NonNegativeIntV1
    child_set_sha256: Sha256V1
    ordered_algo_instance_refs: tuple[CurrentThreeShadowFactRefV1, ...]
    algo_count: NonNegativeIntV1
    algo_set_sha256: Sha256V1
    source_set_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_snapshot(self) -> Self:
        if re.fullmatch(r"[0-9a-f]{40}", self.repository_commit_sha) is None:
            raise ValueError("repository_commit_sha must be an exact lowercase git commit")
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.trade_date) is None:
            raise ValueError("shadow trade_date must be YYYY-MM-DD")
        events = tuple(sorted(self.ordered_legacy_event_refs, key=lambda item: (item.sequence, item.event_id)))
        if events != self.ordered_legacy_event_refs:
            raise ValueError("shadow events must be ordered by sequence and identity")
        if events:
            expected_sequences = tuple(range(events[0].sequence, events[0].sequence + len(events)))
            if tuple(item.sequence for item in events) != expected_sequences:
                raise ValueError("shadow event sequences must be strictly contiguous")
        if len({item.event_id for item in events}) != len(events):
            raise ValueError("shadow event identity must be unique")
        children = tuple(sorted(self.ordered_child_fact_refs, key=lambda item: item.identity))
        algos = tuple(sorted(self.ordered_algo_instance_refs, key=lambda item: item.identity))
        if children != self.ordered_child_fact_refs or algos != self.ordered_algo_instance_refs:
            raise ValueError("shadow fact refs must be sorted by identity")
        for label, refs, count, supplied, domain in (
            ("event", events, self.event_count, self.event_set_sha256, "miniqmt_current_three_shadow_event_set_v1"),
            ("child", children, self.child_count, self.child_set_sha256, "miniqmt_current_three_shadow_child_set_v1"),
            ("algo", algos, self.algo_count, self.algo_set_sha256, "miniqmt_current_three_shadow_algo_set_v1"),
        ):
            if count != len(refs):
                raise ValueError(f"shadow {label} count mismatch")
            if len({item.event_id if label == "event" else item.identity for item in refs}) != len(refs):
                raise ValueError(f"shadow {label} identities are not unique")
            expected = hash_hex_v1(domain, [item.canonical_payload_v1() for item in refs])
            if supplied != expected:
                raise ValueError(f"shadow {label} set hash mismatch")
        expected_source = hash_hex_v1(
            "miniqmt_current_three_shadow_source_snapshot_v1",
            self.canonical_payload_v1(exclude={"source_set_sha256"}),
        )
        if self.source_set_sha256 != expected_source:
            raise ValueError("shadow source set hash mismatch")
        return self


class CurrentThreeParityEventRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_event_ref_v1"]
    step_ordinal: NonNegativeIntV1
    event_id: IdentityV1
    event_type: IdentityV1
    event_source: IdentityV1
    event_payload_sha256: Sha256V1
    logical_time_utc: UtcDateTimeV1
    market_data_projection_id: IdentityV1 | None
    market_data_projection_sha256: Sha256V1 | None
    account_projection_id: IdentityV1 | None
    account_projection_sha256: Sha256V1 | None
    contract_projection_id: IdentityV1 | None
    contract_projection_sha256: Sha256V1 | None
    event_ref_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_event_ref(self) -> Self:
        for identity, digest in (
            (self.market_data_projection_id, self.market_data_projection_sha256),
            (self.account_projection_id, self.account_projection_sha256),
            (self.contract_projection_id, self.contract_projection_sha256),
        ):
            if (identity is None) != (digest is None):
                raise ValueError("parity projection identity and hash must be jointly present or absent")
        expected = hash_hex_v1(
            "miniqmt_current_three_parity_event_ref_v1",
            self.canonical_payload_v1(exclude={"event_ref_sha256"}),
        )
        if self.event_ref_sha256 != expected:
            raise ValueError("parity event ref hash mismatch")
        return self


class CurrentThreeParityInputV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_input_v1"]
    algo_code: IdentityV1
    runtime_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    symbol: IdentityV1
    side: SideV1
    target_quantity: PositiveIntV1
    limit_price_decimal: CanonicalDecimalV1
    pricetick_decimal: CanonicalDecimalV1
    min_volume: PositiveIntV1
    volume_increment: PositiveIntV1
    plugin_config: FrozenJsonFieldV1
    plugin_config_sha256: Sha256V1
    legacy_policy_projection_receipt_sha256: Sha256V1
    execution_coordination_scope: Literal["ALGO_LOCAL_ONLY"]
    ordered_event_refs: tuple[CurrentThreeParityEventRefV1, ...]
    event_set_sha256: Sha256V1
    input_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_input(self) -> Self:
        if self.plugin_config_sha256 != hash_hex_v1("miniqmt_plugin_config_v2", thaw_json_v1(self.plugin_config)):
            raise ValueError("parity plugin config hash mismatch")
        if tuple(item.step_ordinal for item in self.ordered_event_refs) != tuple(range(len(self.ordered_event_refs))):
            raise ValueError("parity event ordinals must be contiguous from zero")
        if len({item.event_id for item in self.ordered_event_refs}) != len(self.ordered_event_refs):
            raise ValueError("parity event identities must be unique")
        expected_set = hash_hex_v1(
            "miniqmt_current_three_parity_event_set_v1",
            [
                {"event_id": item.event_id, "event_ref_sha256": item.event_ref_sha256}
                for item in self.ordered_event_refs
            ],
        )
        if self.event_set_sha256 != expected_set:
            raise ValueError("parity event set hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_current_three_parity_input_v1", self.canonical_payload_v1(exclude={"input_sha256"})
        )
        if self.input_sha256 != expected:
            raise ValueError("parity input hash mismatch")
        return self


class CurrentThreeParityBusinessEffectV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_business_effect_v1"]
    kind: Literal["SUBMIT_LIMIT", "CANCEL_ORDER"]
    side: SideV1
    symbol: IdentityV1
    canonical_price: CanonicalDecimalV1
    quantity: PositiveIntV1
    cancel_target_ordinal: NonNegativeIntV1 | None
    reason_code: IdentityV1
    market_data_lineage_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_effect(self) -> Self:
        if (self.kind == "SUBMIT_LIMIT") != (self.cancel_target_ordinal is None):
            raise ValueError("only CANCEL_ORDER business effects require a cancel target ordinal")
        return self


class CurrentThreeTransportDuplicateObservationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_transport_duplicate_observation_v1"]
    suppression_kind: Literal["PENDING_CANCEL_DUPLICATE", "TERMINAL_FILLED_CANCEL"]
    legacy_step_ordinal: NonNegativeIntV1
    legacy_event_id: IdentityV1
    original_cancel_ordinal: NonNegativeIntV1
    pending_command_id: IdentityV1 | None = None
    pending_command_status: Literal["PENDING", "DISPATCHING", "OUTCOME_UNKNOWN"] | None = None
    pending_command_payload_sha256: Sha256V1 | None = None
    terminal_trade_event_id: IdentityV1 | None = None
    terminal_traded_quantity: PositiveIntV1 | None = None
    terminal_target_quantity: PositiveIntV1 | None = None
    reason_code: IdentityV1

    @model_validator(mode="after")
    def _validate_suppression(self) -> Self:
        pending = (
            self.pending_command_id,
            self.pending_command_status,
            self.pending_command_payload_sha256,
        )
        terminal = (
            self.terminal_trade_event_id,
            self.terminal_traded_quantity,
            self.terminal_target_quantity,
        )
        if self.suppression_kind == "PENDING_CANCEL_DUPLICATE":
            if any(value is None for value in pending) or any(value is not None for value in terminal):
                raise ValueError("pending duplicate suppression requires only exact pending-command authority")
        else:
            if any(value is not None for value in pending) or any(value is None for value in terminal):
                raise ValueError("terminal-filled suppression requires only exact trade/target authority")
            if self.terminal_traded_quantity != self.terminal_target_quantity:
                raise ValueError("terminal-filled suppression requires traded quantity to equal target")
        return self


class CurrentThreeParityTimerEffectV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_timer_effect_v1"]
    mutation_type: Literal["UPSERT_ONE_SHOT", "CANCEL"]
    timer_name: IdentityV1
    schedule_epoch: IdentityV1
    due_at_exchange_utc: UtcDateTimeV1 | None
    catch_up_policy: Literal["SKIP_MISSED"]

    @model_validator(mode="after")
    def _validate_timer(self) -> Self:
        if (self.mutation_type == "CANCEL") != (self.due_at_exchange_utc is None):
            raise ValueError("cancel timer must have no due time and upsert timer must have one")
        return self


class CurrentThreeParityTraceStepV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_trace_step_v1"]
    step_ordinal: NonNegativeIntV1
    event_type: IdentityV1
    event_payload_sha256: Sha256V1
    logical_time_utc: UtcDateTimeV1
    state_status: IdentityV1
    traded_quantity: NonNegativeIntV1
    remaining_quantity: NonNegativeIntV1
    algo_specific_state_projection: FrozenJsonFieldV1
    ordered_business_effects: tuple[CurrentThreeParityBusinessEffectV1, ...]
    ordered_transport_duplicate_observations: tuple[CurrentThreeTransportDuplicateObservationV1, ...]
    ordered_timer_effects: tuple[CurrentThreeParityTimerEffectV1, ...]
    ordered_diagnostic_reason_codes: tuple[IdentityV1, ...]
    terminal_outcome: FrozenJsonFieldV1 | None
    step_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_step(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_current_three_parity_trace_step_v1", self.canonical_payload_v1(exclude={"step_sha256"})
        )
        if self.step_sha256 != expected:
            raise ValueError("parity trace step hash mismatch")
        return self


class CurrentThreeParityTraceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_trace_v1"]
    algo_code: IdentityV1
    side: SideV1
    ordered_steps: tuple[CurrentThreeParityTraceStepV1, ...]
    trace_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_trace(self) -> Self:
        if tuple(item.step_ordinal for item in self.ordered_steps) != tuple(range(len(self.ordered_steps))):
            raise ValueError("trace step ordinals must be contiguous from zero")
        expected = hash_hex_v1(
            "miniqmt_current_three_parity_trace_v1",
            {
                "algo_code": self.algo_code,
                "side": self.side.value,
                "ordered_steps": [
                    {"step_ordinal": item.step_ordinal, "step_sha256": item.step_sha256} for item in self.ordered_steps
                ],
            },
        )
        if self.trace_sha256 != expected:
            raise ValueError("parity trace hash mismatch")
        return self


class CurrentThreeParityDifferenceV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_difference_v1"]
    step_ordinal: NonNegativeIntV1
    field_path: IdentityV1
    legacy_value_sha256: Sha256V1
    kernel_value_sha256: Sha256V1
    reason_code: IdentityV1
    context_sha256: Sha256V1

    def sort_key_v1(self) -> tuple[int, str, str, str, str]:
        return (
            self.step_ordinal,
            self.field_path,
            self.reason_code,
            self.legacy_value_sha256,
            self.kernel_value_sha256,
        )


class CurrentThreeParityReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_parity_receipt_v1"]
    algo_code: IdentityV1
    legacy_source_attribution_sha256: Sha256V1
    plugin_id: IdentityV1
    plugin_version: IdentityV1
    plugin_manifest_sha256: Sha256V1
    plugin_config_sha256: Sha256V1
    parity_input_sha256: Sha256V1
    execution_coordination_scope: Literal["ALGO_LOCAL_ONLY"]
    ordered_event_refs: tuple[CurrentThreeParityEventRefV1, ...]
    event_set_sha256: Sha256V1
    legacy_trace_sha256: Sha256V1
    kernel_trace_sha256: Sha256V1
    ordered_differences: tuple[CurrentThreeParityDifferenceV1, ...]
    status: CurrentThreeParityStatusV1
    broker_called: Literal[False]
    receipt_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_receipt(self) -> Self:
        if tuple(item.step_ordinal for item in self.ordered_event_refs) != tuple(range(len(self.ordered_event_refs))):
            raise ValueError("receipt event ordinals must be contiguous from zero")
        if len({item.event_id for item in self.ordered_event_refs}) != len(self.ordered_event_refs):
            raise ValueError("receipt event identities must be unique")
        expected_event_set = hash_hex_v1(
            "miniqmt_current_three_parity_event_set_v1",
            [
                {"event_id": item.event_id, "event_ref_sha256": item.event_ref_sha256}
                for item in self.ordered_event_refs
            ],
        )
        if self.event_set_sha256 != expected_event_set:
            raise ValueError("receipt event set hash mismatch")
        if len(self.ordered_differences) > MAX_K3_FAILURES:
            raise ValueError("parity differences exceed bounded limit")
        if self.ordered_differences != tuple(sorted(self.ordered_differences, key=lambda item: item.sort_key_v1())):
            raise ValueError("parity differences must be stable sorted")
        if (self.status is CurrentThreeParityStatusV1.PASSED) != (not self.ordered_differences):
            raise ValueError("parity status and difference set disagree")
        expected = hash_hex_v1(
            "miniqmt_current_three_parity_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("parity receipt hash mismatch")
        return self


class CurrentThreeShadowCommandAssociationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_current_three_shadow_command_association_v1"]
    parity_input_sha256: Sha256V1
    step_ordinal: NonNegativeIntV1
    business_effect_ordinal: NonNegativeIntV1
    legacy_algo_instance_id: IdentityV1
    legacy_child_order_id: IdentityV1
    legacy_broker_order_id: IdentityV1
    legacy_child_payload_sha256: Sha256V1
    kernel_runtime_id: IdentityV1
    kernel_algo_instance_id: IdentityV1
    transition_id: IdentityV1
    kernel_command_id: IdentityV1
    mapping_id: IdentityV1
    local_vt_orderid: IdentityV1
    symbol: IdentityV1
    side: SideV1
    canonical_price: CanonicalDecimalV1
    quantity: PositiveIntV1
    reason_code: IdentityV1
    association_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_association(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_current_three_shadow_command_association_v1",
            self.canonical_payload_v1(exclude={"association_sha256"}),
        )
        if self.association_sha256 != expected:
            raise ValueError("shadow command association hash mismatch")
        return self


__all__ = [name for name in globals() if name.startswith("CurrentThree") or name in {"bounded_failures_v1"}]
