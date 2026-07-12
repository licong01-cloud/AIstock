"""Application service for the Phase 0A MiniQMT TCA read-only contract.

This module deliberately contains no scheduler, broker, rebuild, or mutation
dependency.  It translates immutable ledger evidence into the public SIM
projection while preserving the repository's result-series authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping

from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.qmt_strategy_ledger.tca_read_repository import (
    ExecutionTcaDetail,
    ExecutionTcaParentPage,
    ExecutionTcaReadRepository,
    ExecutionTcaSelection,
)
from backend.services.qmt_strategy_ledger.tca_read_service import (
    AccountPseudonymizer,
    TcaKeysetCursorCodec,
    TcaReadError,
    TcaReadRuntimeConfig,
    TCA_READ_SCHEMA_VERSION,
)
from backend.services.miniqmt_execution_runtime.models import MiniQMTExecutionEventType
from backend.services.miniqmt_execution_runtime.repository import (
    MiniQMTExecutionRuntimeRepository,
    PostgresMiniQMTExecutionRuntimeRepository,
)


_PARENT_LINEAGE_FIELDS = (
    "run_id",
    "execution_plan_id",
    "execution_plan_hash",
    "binding_id",
    "binding_hash",
    "strategy_id",
    "portfolio_id",
    "package_id",
    "release_id",
    "selection_evidence_id",
    "runtime_id",
    "logical_tca_scope_hash",
    "qmt_order_intent_id",
)
_PARENT_QUANTITY_FIELDS = (
    "planning_requested_quantity",
    "trading_rule_legal_quantity",
    "emitted_parent_quantity",
    "managed_request_quantity_before_cash",
    "managed_request_quantity_after_cash",
    "eligible_now_quantity",
    "conditional_eligible_quantity",
    "eligible_quantity",
    "execution_ineligible_quantity",
    "planning_excluded_quantity",
)
_PARENT_BENCHMARK_FIELDS = (
    "decision_benchmark_type",
    "decision_capture_fetch_started_at",
    "decision_event_at",
    "decision_market_time",
    "decision_received_at",
    "decision_persisted_at",
    "decision_bid_price_1",
    "decision_ask_price_1",
    "decision_mid_price",
    "decision_quote_source",
    "decision_quote_age_ms",
    "decision_transport_latency_ms",
    "decision_quality",
    "decision_raw_quote_sha256",
    "strategy_decision_price",
    "strategy_decision_time",
    "strategy_decision_source",
    "strategy_decision_quality",
    "arrival_time",
    "arrival_benchmark_type",
    "arrival_quote_market_time",
    "arrival_quote_received_at",
    "arrival_persisted_at",
    "arrival_bid_price_1",
    "arrival_ask_price_1",
    "arrival_mid_price",
    "arrival_quote_source",
    "arrival_quote_offset_ms",
    "arrival_transport_latency_ms",
    "arrival_quality",
    "arrival_raw_quote_sha256",
)
_PARENT_ELIGIBILITY_FIELDS = (
    "eligibility_as_of",
    "eligibility_class",
    "eligibility_quality",
    "eligibility_rule_version",
    "trading_rule_decision_id",
    "preflight_result_hash",
    "dependency_parent_ids",
)
_PARENT_POLICY_FIELDS = (
    "deadline",
    "calendar_version",
    "deadline_mark_policy_version",
    "deadline_mark_max_age_ms",
    "arrival_forward_window_ms",
    "clock_skew_tolerance_ms",
    "benchmark_max_transport_latency_ms",
    "tail_sweep_time",
    "continuous_cancel_cutoff",
    "benchmark_schema_version",
    "benchmark_policy_version",
    "capture_code_version",
    "execution_policy_id",
    "execution_policy_sha256",
    "runtime_config_sha256",
    "time_parser_version",
    "unit_mapping_version",
    "hard_cost_limit_bps",
    "hard_cost_benchmark_type",
    "hard_cost_benchmark_price",
    "evidence_sha256",
)
_RESULT_SCALAR_FIELDS = (
    "tca_result_id",
    "result_series_key",
    "result_generation",
    "supersedes_tca_result_id",
    "parent_intent_id",
    "parent_revision",
    "snapshot_kind",
    "result_status",
    "as_of_time",
    "source_snapshot_started_at",
    "source_snapshot_completed_at",
    "deadline",
    "terminal_as_of",
    "reconciliation_run_id",
    "eligible_quantity",
    "deadline_filled_quantity",
    "terminal_filled_quantity",
    "post_deadline_filled_quantity",
    "deadline_residual_quantity",
    "terminal_residual_quantity",
    "deadline_fill_count",
    "deadline_fill_notional_cny",
    "deadline_fill_vwap",
    "terminal_fill_count",
    "terminal_fill_notional_cny",
    "terminal_fill_vwap",
    "delay_cost_cny",
    "execution_cost_cny",
    "opportunity_cost_cny",
    "decision_calculation_mode",
    "decision_is_direct_check_gross_cny",
    "decision_is_gross_cny",
    "decision_is_net_actual_cny",
    "decision_is_net_estimated_cny",
    "decision_is_gross_bps",
    "decision_is_net_actual_bps",
    "decision_is_net_estimated_bps",
    "arrival_is_gross_cny",
    "arrival_is_net_actual_cny",
    "arrival_is_net_estimated_cny",
    "arrival_is_gross_bps",
    "arrival_is_net_actual_bps",
    "arrival_is_net_estimated_bps",
    "deadline_fee_actual_cny",
    "deadline_fee_estimated_cny",
    "post_deadline_fee_actual_cny",
    "post_deadline_fee_estimated_cny",
    "deadline_fee_quality",
    "post_deadline_fee_quality",
    "fee_schedule_version",
    "account_fee_profile_version",
    "fee_allocation_version",
    "completion_by_deadline_quantity",
    "terminal_completion_quantity",
    "completion_by_deadline_notional",
    "effective_spread_bps",
    "effective_spread_partial_bps",
    "effective_spread_coverage_notional_ratio",
    "cost_markout_60s_bps",
    "cost_markout_300s_bps",
    "cost_markout_900s_bps",
    "post_deadline_execution_cost_cny",
    "residual_reason",
    "residual_executability_class",
    "formula_version",
    "calculator_version",
    "schema_version",
    "query_version",
    "benchmark_policy_version",
    "mark_policy_version",
    "fee_policy_version",
    "trade_provenance_policy_version",
    "canonical_input_sha256",
    "canonical_output_sha256",
)
_RESULT_SAFE_MAPPING_FIELDS = (
    "fee_breakdown",
    "markout_partial_metrics",
    "markout_coverage",
    "metric_validity",
    "join_coverage",
    "benchmark_coverage",
    "mark_coverage",
    "fee_coverage",
    "finality_evidence",
    "invariant_results",
)
_BLOCKED_MAPPING_KEY_TOKENS = (
    "raw_payload",
    "normalized_payload",
    "raw_evidence",
    "secret",
    "credential",
    "password",
)
_ACCOUNT_MAPPING_KEYS = frozenset({"account_id", "trade_account_id"})
TCA_EVIDENCE_EXPORT_VERSION = "miniqmt_execution_tca_evidence_v1"
TCA_EVIDENCE_MANIFEST_SCHEMA_VERSION = "miniqmt_execution_tca_evidence_manifest_v1"
TCA_EVIDENCE_EXPORT_VERSION_V2 = "miniqmt_execution_tca_evidence_v2"
TCA_EVIDENCE_MANIFEST_SCHEMA_VERSION_V2 = "miniqmt_execution_tca_evidence_manifest_v2"
TCA_EVIDENCE_RECORD_SCHEMA_VERSION_V2 = "miniqmt_execution_tca_evidence_record_v2"


@dataclass(frozen=True, slots=True)
class ExecutionTcaEvidenceExport:
    """Deterministic, pseudonymized evidence export with its own manifest."""

    manifest: Mapping[str, Any]
    records: tuple[Mapping[str, Any], ...]


class ExecutionTcaReadService:
    """Public read-only service; all database access stays inside its repository."""

    def __init__(
        self,
        *,
        repository: ExecutionTcaReadRepository | None = None,
        config_provider: Callable[[], TcaReadRuntimeConfig] | None = None,
        runtime_repository: MiniQMTExecutionRuntimeRepository | None = None,
    ) -> None:
        self._repository = repository or ExecutionTcaReadRepository()
        self._config_provider = config_provider or TcaReadRuntimeConfig.from_environ
        self._runtime_repository = runtime_repository or PostgresMiniQMTExecutionRuntimeRepository()

    def get_execution_parent(
        self, *, parent_intent_id: str, parent_revision: int | str | None = None
    ) -> dict[str, Any]:
        config = self._runtime_config()
        pseudonymizer = config.require_pseudonymizer()
        active_version = config.require_active_read_version()
        with self._repository.read_snapshot() as cursor:
            parent = self._repository.get_parent(
                parent_intent_id=parent_intent_id,
                parent_revision=parent_revision,
                cursor=cursor,
            )
            if parent is None:
                raise _parent_not_found(parent_intent_id, parent_revision)
            summaries: dict[str, dict[str, Any] | None] = {}
            for snapshot_kind in ("RECONCILED_FINAL", "DEADLINE"):
                try:
                    selection = self._repository.get_tca(
                        parent_intent_id=str(parent["parent_intent_id"]),
                        parent_revision=int(parent["parent_revision"]),
                        snapshot_kind=snapshot_kind,
                        active_version=active_version,
                        cursor=cursor,
                    )
                    summaries[snapshot_kind] = _project_result_summary(selection)
                except TcaReadError as exc:
                    if exc.reason_code != "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND":
                        raise
                    summaries[snapshot_kind] = None
        latest_tca = summaries["RECONCILED_FINAL"] or summaries["DEADLINE"]
        return {
            "schema_version": TCA_READ_SCHEMA_VERSION,
            "active_read_version": _version_payload(active_version),
            "account_pseudonym_key_version": pseudonymizer.key_version,
            "parent": _project_parent(parent, pseudonymizer),
            "latest_tca": latest_tca,
            "tca_by_snapshot": summaries,
        }

    def list_execution_parents(
        self,
        *,
        binding_id: str,
        trade_date: date | str,
        terminal_state: str | None = None,
        limit: int | str = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        parsed_trade_date = _parse_trade_date(trade_date)
        config = self._runtime_config()
        pseudonymizer = config.require_pseudonymizer()
        active_version = config.require_active_read_version()
        filter_sha256 = canonical_json_sha256(
            {
                "binding_id": _required_text(binding_id, "binding_id"),
                "trade_date": parsed_trade_date.isoformat(),
                "terminal_state": _optional_text(terminal_state, "terminal_state"),
                "active_read_version_sha256": active_version.config_sha256,
                "schema_version": TCA_READ_SCHEMA_VERSION,
            }
        )
        codec = TcaKeysetCursorCodec(pseudonymizer)
        after_key = _decode_cursor(cursor, codec=codec, filter_sha256=filter_sha256)
        with self._repository.read_snapshot() as snapshot_cursor:
            page = self._repository.list_parents(
                binding_id=binding_id,
                trade_date=parsed_trade_date,
                terminal_state=terminal_state,
                limit=limit,
                after_key=after_key,
                active_version=active_version,
                cursor=snapshot_cursor,
            )
        return {
            "schema_version": TCA_READ_SCHEMA_VERSION,
            "active_read_version": _version_payload(active_version),
            "account_pseudonym_key_version": pseudonymizer.key_version,
            "binding_id": _required_text(binding_id, "binding_id"),
            "trade_date": parsed_trade_date.isoformat(),
            "terminal_state": _optional_text(terminal_state, "terminal_state"),
            "parents": [_project_parent(row, pseudonymizer) for row in page.parents],
            "next_cursor": _encode_next_cursor(page, codec=codec, filter_sha256=filter_sha256),
        }

    def get_execution_tca(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int | str,
        snapshot_kind: str,
        tca_version: str | None = None,
        receipt_id: str | None = None,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any]:
        config = self._runtime_config()
        pseudonymizer = config.require_pseudonymizer()
        explicit_version = _optional_text(tca_version, "tca_version")
        active_version = None if explicit_version is not None else config.require_active_read_version()
        parsed_as_of = _parse_as_of(as_of)
        with self._repository.read_snapshot() as cursor:
            detail = self._repository.get_tca_detail(
                parent_intent_id=parent_intent_id,
                parent_revision=parent_revision,
                snapshot_kind=snapshot_kind,
                active_version=active_version,
                tca_version=explicit_version,
                receipt_id=receipt_id,
                as_of=parsed_as_of,
                cursor=cursor,
            )
        return {
            "schema_version": TCA_READ_SCHEMA_VERSION,
            "active_read_version": _version_payload(active_version) if active_version is not None else None,
            "account_pseudonym_key_version": pseudonymizer.key_version,
            **_project_tca_detail(detail, pseudonymizer),
        }

    def export_execution_evidence(
        self,
        *,
        binding_id: str,
        trade_date: date | str,
        evidence_version: str = TCA_EVIDENCE_EXPORT_VERSION,
    ) -> ExecutionTcaEvidenceExport:
        """Materialize a canonical SIM evidence export from one read snapshot.

        The export has no REST adapter in Phase 0A.  It keeps all records in one
        repeatable-read cursor and projects only the same pseudonymized public
        evidence that the read service permits.
        """

        if evidence_version == TCA_EVIDENCE_EXPORT_VERSION_V2:
            return self._export_quote_control_evidence_v2(binding_id=binding_id, trade_date=trade_date)
        if evidence_version != TCA_EVIDENCE_EXPORT_VERSION:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_EVIDENCE_VERSION_UNSUPPORTED",
                "evidence_version is unsupported",
                http_status=400,
                stage="TCA_EXPORT",
                context={"evidence_version": evidence_version},
            )
        binding = _required_text(binding_id, "binding_id")
        parsed_trade_date = _parse_trade_date(trade_date)
        config = self._runtime_config()
        pseudonymizer = config.require_pseudonymizer()
        active_version = config.require_active_read_version()
        records: list[Mapping[str, Any]] = []
        after_key: tuple[date, str, int] | None = None
        with self._repository.read_snapshot() as cursor:
            while True:
                page = self._repository.list_parents(
                    binding_id=binding,
                    trade_date=parsed_trade_date,
                    limit=200,
                    after_key=after_key,
                    active_version=None,
                    cursor=cursor,
                )
                for parent in page.parents:
                    tca_by_snapshot: dict[str, Any] = {}
                    for snapshot_kind in ("DEADLINE", "RECONCILED_FINAL"):
                        try:
                            detail = self._repository.get_tca_detail(
                                parent_intent_id=str(parent["parent_intent_id"]),
                                parent_revision=int(parent["parent_revision"]),
                                snapshot_kind=snapshot_kind,
                                active_version=active_version,
                                cursor=cursor,
                            )
                            tca_by_snapshot[snapshot_kind] = _project_tca_detail(detail, pseudonymizer)
                        except TcaReadError as exc:
                            if exc.reason_code != "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND":
                                raise
                            tca_by_snapshot[snapshot_kind] = None
                    records.append(
                        {
                            "record_type": "execution_parent_evidence",
                            "schema_version": TCA_READ_SCHEMA_VERSION,
                            "parent": _project_parent(parent, pseudonymizer),
                            "tca_by_snapshot": tca_by_snapshot,
                        }
                    )
                if page.next_key is None:
                    break
                after_key = page.next_key
        ordered_records = tuple(sorted(records, key=_evidence_record_sort_key))
        manifest_base = {
            "manifest_schema_version": TCA_EVIDENCE_MANIFEST_SCHEMA_VERSION,
            "evidence_version": evidence_version,
            "read_schema_version": TCA_READ_SCHEMA_VERSION,
            "scope": {
                "environment": "SIM",
                "binding_id": binding,
                "trade_date": parsed_trade_date.isoformat(),
            },
            "active_read_version": _version_payload(active_version),
            "account_pseudonym_key_version": pseudonymizer.key_version,
            "record_count": len(ordered_records),
            "records_sha256": canonical_json_sha256(ordered_records),
        }
        manifest = {**manifest_base, "manifest_sha256": canonical_json_sha256(manifest_base)}
        return ExecutionTcaEvidenceExport(manifest=manifest, records=ordered_records)

    def _export_quote_control_evidence_v2(
        self,
        *,
        binding_id: str,
        trade_date: date | str,
    ) -> ExecutionTcaEvidenceExport:
        binding = _required_text(binding_id, "binding_id")
        parsed_trade_date = _parse_trade_date(trade_date)
        config = self._runtime_config()
        pseudonymizer = config.require_pseudonymizer()
        active_version = config.require_active_read_version()
        records: list[dict[str, Any]] = []
        runtime_ids: set[str] = set()
        binding_hashes: set[str] = set()
        parent_ids: set[str] = set()
        after_key: tuple[date, str, int] | None = None
        with self._repository.read_snapshot() as cursor:
            while True:
                page = self._repository.list_parents(
                    binding_id=binding,
                    trade_date=parsed_trade_date,
                    limit=200,
                    after_key=after_key,
                    active_version=None,
                    cursor=cursor,
                )
                for parent in page.parents:
                    projected_parent = _project_parent(parent, pseudonymizer)
                    parent_id = str(projected_parent["parent_intent_id"])
                    parent_ids.add(parent_id)
                    lineage = projected_parent.get("lineage")
                    if not isinstance(lineage, Mapping):
                        raise TcaReadError(
                            "ADAPTIVE_IS_TCA_EVIDENCE_LINEAGE_MISSING",
                            "Phase 0B parent projection is missing its bounded lineage object",
                            http_status=409,
                            stage="TCA_EXPORT",
                            context={"parent_intent_id": parent_id},
                        )
                    runtime_id = str(lineage.get("runtime_id") or "").strip()
                    if runtime_id:
                        runtime_ids.add(runtime_id)
                    binding_hash = str(lineage.get("binding_hash") or "").strip()
                    if binding_hash:
                        binding_hashes.add(binding_hash)
                    records.append(
                        _v2_record(
                            record_kind="TCA_PARENT",
                            runtime_id=runtime_id,
                            sequence=0,
                            event_id="",
                            parent_intent_id=parent_id,
                            payload=projected_parent,
                        )
                    )
                    for snapshot_kind in ("DEADLINE", "RECONCILED_FINAL"):
                        try:
                            detail = self._repository.get_tca_detail(
                                parent_intent_id=parent_id,
                                parent_revision=int(parent["parent_revision"]),
                                snapshot_kind=snapshot_kind,
                                active_version=active_version,
                                cursor=cursor,
                            )
                        except TcaReadError as exc:
                            if exc.reason_code != "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND":
                                raise
                            continue
                        projected = _project_tca_detail(detail, pseudonymizer)
                        records.append(
                            _v2_record(
                                record_kind="TCA_RESULT",
                                runtime_id=runtime_id,
                                sequence=0,
                                event_id=str(projected["result"].get("tca_result_id") or ""),
                                parent_intent_id=parent_id,
                                payload={"snapshot_kind": snapshot_kind, **projected["result"]},
                            )
                        )
                        for mark in projected["marks"]:
                            records.append(
                                _v2_record(
                                    record_kind="TCA_MARK",
                                    runtime_id=runtime_id,
                                    sequence=0,
                                    event_id=str(mark.get("mark_id") or ""),
                                    parent_intent_id=parent_id,
                                    payload=mark,
                                )
                            )
                        for trade in projected["trade_observations"]:
                            records.append(
                                _v2_record(
                                    record_kind="TCA_TRADE",
                                    runtime_id=runtime_id,
                                    sequence=0,
                                    event_id=str(trade.get("trade_observation_id") or ""),
                                    parent_intent_id=parent_id,
                                    payload=trade,
                                )
                            )
                if page.next_key is None:
                    break
                after_key = page.next_key
            runtime_rows, runtime_events = self._runtime_repository.read_quote_control_snapshot(
                cursor=cursor,
                runtime_ids=tuple(sorted(runtime_ids)),
                include_archived=True,
            )
        revision_ids: set[str] = set()
        assignments_by_parent: dict[str, list[Mapping[str, Any]]] = {}
        hash_sets = {name: set() for name in ("policy", "config", "adapter", "code", "schema")}
        for runtime in runtime_rows:
            quote_control = runtime.metadata.get("quote_control") if isinstance(runtime.metadata, dict) else None
            if not isinstance(quote_control, Mapping):
                continue
            revision = quote_control.get("revision")
            if isinstance(revision, Mapping):
                revision_id = str(revision.get("revision_id") or "")
                if revision_id:
                    revision_ids.add(revision_id)
                records.append(
                    _v2_record(
                        record_kind="CONTROL_REVISION",
                        runtime_id=runtime.runtime_id,
                        sequence=0,
                        event_id=revision_id,
                        parent_intent_id="",
                        payload=dict(revision),
                    )
                )
            assignments = quote_control.get("assignments")
            if isinstance(assignments, list):
                for assignment in assignments:
                    if not isinstance(assignment, Mapping):
                        continue
                    parent_id = str(assignment.get("parent_intent_id") or "")
                    assignments_by_parent.setdefault(parent_id, []).append(assignment)
                    records.append(
                        _v2_record(
                            record_kind="PARENT_ASSIGNMENT",
                            runtime_id=runtime.runtime_id,
                            sequence=0,
                            event_id=str(assignment.get("assignment_id") or ""),
                            parent_intent_id=parent_id,
                            payload=dict(assignment),
                        )
                    )
                    for key, field_name in (
                        ("policy", "quote_policy_sha256"),
                        ("adapter", "adapter_sha256"),
                        ("code", "code_sha256"),
                        ("schema", "evidence_schema_sha256"),
                    ):
                        value = str(assignment.get(field_name) or "")
                        if value:
                            hash_sets[key].add(value)
        child_events: list[Mapping[str, Any]] = []
        receipt_events: list[Mapping[str, Any]] = []
        trade_anchors: list[Mapping[str, Any]] = []
        action_events: list[Mapping[str, Any]] = []
        action_input_events: list[Mapping[str, Any]] = []
        markout_by_trade: dict[str, set[int]] = {}
        capture_counts: dict[str, int] = {}
        for event in runtime_events:
            evidence = event.payload.get("evidence") if isinstance(event.payload, dict) else None
            record_kind = None
            payload: Mapping[str, Any] | None = None
            parent_id = ""
            if isinstance(evidence, Mapping):
                capture_type = str(evidence.get("capture_type") or "")
                record_kind = {
                    "ACTION_INPUT": "ACTION_INPUT",
                    "ACTION_REJECT": "ACTION_REJECT",
                    "CHILD_RECEIPT": "CHILD_RECEIPT",
                    "MARKOUT_60S": "MARKOUT",
                    "MARKOUT_300S": "MARKOUT",
                    "MARKOUT_900S": "MARKOUT",
                    "CADENCE_AGGREGATE": "CADENCE_AGGREGATE",
                }.get(capture_type)
                payload = evidence
                parent_id = str(evidence.get("parent_intent_id") or "")
                capture_counts[capture_type] = capture_counts.get(capture_type, 0) + 1
                for key, field_name in (
                    ("policy", "policy_sha256"),
                    ("config", "config_sha256"),
                    ("adapter", "adapter_sha256"),
                    ("code", "code_sha256"),
                    ("schema", "schema_sha256"),
                ):
                    value = str(evidence.get(field_name) or "")
                    if value:
                        hash_sets[key].add(value)
                if capture_type == "CHILD_RECEIPT":
                    receipt_events.append(evidence)
                if capture_type == "ACTION_INPUT":
                    action_input_events.append(evidence)
                if capture_type.startswith("MARKOUT_"):
                    trade_id = str(evidence.get("trade_id") or "")
                    horizon = evidence.get("horizon_seconds")
                    if trade_id and isinstance(horizon, int):
                        markout_by_trade.setdefault(trade_id, set()).add(horizon)
            elif event.event_type == MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH:
                record_kind = "INGRESS_HEALTH"
                payload = event.payload
            elif event.event_type in {
                MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
                MiniQMTExecutionEventType.CHILD_ORDER_REJECTED,
            }:
                child_events.append(event.payload)
                record_kind = "CHILD_EVENT"
                payload = event.payload
                parent_id = str(event.payload.get("parent_intent_id") or "")
            elif event.event_type == MiniQMTExecutionEventType.TRADE_EVENT:
                anchor = event.payload.get("quote_evidence_markout_anchor_v1")
                if isinstance(anchor, Mapping):
                    trade_anchors.append(anchor)
                    record_kind = "TRADE_ANCHOR"
                    payload = anchor
                    parent_id = str(anchor.get("parent_intent_id") or "")
            elif event.event_type == MiniQMTExecutionEventType.ALGO_ACTION_EMITTED:
                action = event.payload.get("b0_quote_v2_action")
                if isinstance(action, Mapping):
                    action_events.append(action)
                    record_kind = "ACTION_EVENT"
                    payload = action
                    parent_id = str(action.get("parent_intent_id") or "")
            if record_kind is not None and payload is not None:
                records.append(
                    _v2_record(
                        record_kind=record_kind,
                        runtime_id=event.runtime_id,
                        sequence=event.sequence,
                        event_id=event.event_id,
                        parent_intent_id=parent_id,
                        payload=payload,
                    )
                )
        assignment_missing = sum(len(assignments_by_parent.get(parent_id, ())) != 1 for parent_id in parent_ids)
        revision_conflict_count = sum(
            len({str(item.get("revision_id") or "") for item in items}) > 1 for items in assignments_by_parent.values()
        )
        action_by_id = {str(item.get("action_id") or ""): item for item in action_events}
        action_input_by_id = {str(item.get("evidence_id") or ""): item for item in action_input_events}
        receipt_child_ids = {str(item.get("child_order_id") or "") for item in receipt_events}
        missing_action_links = sum(
            not action.get("action_evidence_id")
            or str(action.get("action_evidence_id") or "") not in action_input_by_id
            or not action.get("action_market_data_id")
            or str(action.get("parent_intent_id") or "") not in parent_ids
            for action in action_events
        )
        missing_child_links = sum(
            not payload.get("action_evidence_id")
            or not payload.get("action_market_data_id")
            or str(payload.get("action_id") or "") not in action_by_id
            or str(payload.get("child_order_id") or "") not in receipt_child_ids
            for payload in child_events
        )
        children_by_action: dict[str, set[str]] = {}
        for payload in child_events:
            action_id = str(payload.get("action_id") or "")
            child_id = str(payload.get("child_order_id") or "")
            if action_id:
                children_by_action.setdefault(action_id, set()).add(child_id)
        duplicate_child_count = sum(max(0, len(children) - 1) for children in children_by_action.values())
        missing_trade_marks = sum(
            markout_by_trade.get(str(anchor.get("trade_id") or ""), set()) != {60, 300, 900} for anchor in trade_anchors
        )
        hash_conflict_count = sum(len(values) != 1 for values in hash_sets.values())
        identity_conflict_count = (
            int(len(binding_hashes) != 1)
            + int(len(revision_ids) != 1)
            + int(bool(action_events) and len(action_input_events) != len(action_events))
        )
        missing_link_count = assignment_missing + missing_action_links + missing_child_links + missing_trade_marks
        ordered_records = tuple(
            sorted(
                records,
                key=lambda item: (
                    str(item["record_kind"]),
                    str(item["runtime_id"]),
                    int(item["sequence"]),
                    str(item["event_id"]),
                    str(item["parent_intent_id"]),
                ),
            )
        )
        record_counts: dict[str, int] = {}
        for record in ordered_records:
            kind = str(record["record_kind"])
            record_counts[kind] = record_counts.get(kind, 0) + 1
        quote_control_complete = bool(parent_ids) and not any(
            (
                missing_link_count,
                duplicate_child_count,
                revision_conflict_count,
                hash_conflict_count,
                identity_conflict_count,
            )
        )
        five_level_action_count = sum(_has_complete_five_level_depth(evidence) for evidence in action_input_events)
        manifest_base = {
            "manifest_schema_version": TCA_EVIDENCE_MANIFEST_SCHEMA_VERSION_V2,
            "evidence_version": TCA_EVIDENCE_EXPORT_VERSION_V2,
            "read_schema_version": TCA_READ_SCHEMA_VERSION,
            "scope": {"environment": "SIM", "binding_id": binding, "trade_date": parsed_trade_date.isoformat()},
            "binding_hash": next(iter(binding_hashes)) if len(binding_hashes) == 1 else None,
            "trade_date": parsed_trade_date.isoformat(),
            "control_revision": "B0_QUOTE_V2" if assignments_by_parent else None,
            "revision_ids": sorted(revision_ids),
            "policy_sha256_set": sorted(hash_sets["policy"]),
            "config_sha256_set": sorted(hash_sets["config"]),
            "adapter_sha256_set": sorted(hash_sets["adapter"]),
            "code_sha256_set": sorted(hash_sets["code"]),
            "schema_sha256_set": sorted(hash_sets["schema"]),
            "record_counts": record_counts,
            "action_ready_count": capture_counts.get("ACTION_INPUT", 0),
            "action_reject_count": capture_counts.get("ACTION_REJECT", 0),
            "five_level_coverage": _coverage(five_level_action_count, capture_counts.get("ACTION_INPUT", 0)),
            "age_coverage": _coverage(
                sum(
                    bool(record.get("payload", {}).get("quote_age_ms") is not None)
                    for record in ordered_records
                    if record["record_kind"] in {"ACTION_INPUT", "ACTION_REJECT"}
                ),
                capture_counts.get("ACTION_INPUT", 0) + capture_counts.get("ACTION_REJECT", 0),
            ),
            "cadence_aggregate_count": capture_counts.get("CADENCE_AGGREGATE", 0),
            "markout_coverage": {
                str(horizon): _coverage(capture_counts.get(f"MARKOUT_{horizon}S", 0), len(trade_anchors))
                for horizon in (60, 300, 900)
            },
            "missing_link_count": missing_link_count,
            "assignment_missing_count": assignment_missing,
            "missing_action_link_count": missing_action_links,
            "missing_child_link_count": missing_child_links,
            "missing_trade_mark_count": missing_trade_marks,
            "duplicate_child_count": duplicate_child_count,
            "revision_conflict_count": revision_conflict_count,
            "hash_conflict_count": hash_conflict_count,
            "identity_conflict_count": identity_conflict_count,
            "quote_control_complete": quote_control_complete,
            "active_read_version": _version_payload(active_version),
            "account_pseudonym_key_version": pseudonymizer.key_version,
            "record_count": len(ordered_records),
            "records_sha256": canonical_json_sha256(ordered_records),
        }
        manifest = {**manifest_base, "manifest_sha256": canonical_json_sha256(manifest_base)}
        return ExecutionTcaEvidenceExport(manifest=manifest, records=ordered_records)

    def _runtime_config(self) -> TcaReadRuntimeConfig:
        try:
            return self._config_provider()
        except TcaReadError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_READ_CONFIG_UNAVAILABLE",
                "TCA read configuration could not be loaded",
                http_status=503,
                stage="TCA_READ_CONFIG",
                context={"error_type": type(exc).__name__},
            ) from exc


def _project_parent(parent: Mapping[str, Any], pseudonymizer: AccountPseudonymizer) -> dict[str, Any]:
    account_id = _required_text(parent.get("account_id"), "account_id")
    payload = {
        "parent_intent_id": _required_text(parent.get("parent_intent_id"), "parent_intent_id"),
        "parent_revision": _positive_int(parent.get("parent_revision"), "parent_revision"),
        "account_pseudonym": pseudonymizer.pseudonymize(account_id),
        "trade_date": _json_value(parent.get("trade_date")),
        "environment": _required_text(parent.get("environment"), "environment"),
        "symbol": _required_text(parent.get("symbol"), "symbol"),
        "side": _required_text(parent.get("side"), "side"),
        "currency": _required_text(parent.get("currency"), "currency"),
        "lineage": _project_fields(parent, _PARENT_LINEAGE_FIELDS),
        "quantities": _project_fields(parent, _PARENT_QUANTITY_FIELDS),
        "benchmark": _project_fields(parent, _PARENT_BENCHMARK_FIELDS),
        "eligibility": _project_fields(parent, _PARENT_ELIGIBILITY_FIELDS),
        "policy": _project_fields(parent, _PARENT_POLICY_FIELDS),
    }
    if "terminal_state" in parent:
        payload["terminal_state"] = _json_value(parent.get("terminal_state"))
        payload["latest_tca_result_id"] = _json_value(parent.get("latest_tca_result_id"))
        payload["latest_tca_snapshot_kind"] = _json_value(parent.get("latest_tca_snapshot_kind"))
    return payload


def _v2_record(
    *,
    record_kind: str,
    runtime_id: str,
    sequence: int,
    event_id: str,
    parent_intent_id: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    base = {
        "schema_version": TCA_EVIDENCE_RECORD_SCHEMA_VERSION_V2,
        "record_kind": record_kind,
        "runtime_id": runtime_id,
        "sequence": int(sequence),
        "event_id": event_id,
        "parent_intent_id": parent_intent_id,
        "payload": dict(payload),
    }
    return {**base, "record_sha256": canonical_json_sha256(base)}


def _coverage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _has_complete_five_level_depth(evidence: Mapping[str, Any]) -> bool:
    for field_name in ("bid_prices", "bid_quantities", "ask_prices", "ask_quantities"):
        values = evidence.get(field_name)
        if not isinstance(values, list) or len(values) != 5:
            return False
    return True


def _project_tca_detail(detail: ExecutionTcaDetail, pseudonymizer: AccountPseudonymizer) -> dict[str, Any]:
    result = detail.selection.result
    valid_results = [
        row
        for row in detail.selection.result_series
        if str(row.get("result_status") or "") != "INVALID" and row.get("completed_receipt_ids")
    ]
    return {
        "selection_mode": detail.selection.selection_mode,
        "result": {
            **_project_fields(result, _RESULT_SCALAR_FIELDS),
            "completed_receipt_ids": list(_completed_receipt_ids(result)),
            "metrics": _project_safe_mappings(result, pseudonymizer),
        },
        "latest_valid_result_id": (_json_value(valid_results[-1].get("tca_result_id")) if valid_results else None),
        "supersedes_chain": [_project_result_summary_row(row) for row in detail.selection.result_series],
        "marks": [_project_mark(row, pseudonymizer) for row in detail.marks],
        "trade_observations": [_project_trade_observation(row, pseudonymizer) for row in detail.trade_observations],
    }


def _project_result_summary(selection: ExecutionTcaSelection) -> dict[str, Any]:
    return {
        "selection_mode": selection.selection_mode,
        **_project_result_summary_row(selection.result),
    }


def _project_result_summary_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "tca_result_id": _json_value(row.get("tca_result_id")),
        "result_series_key": _json_value(row.get("result_series_key")),
        "result_generation": _json_value(row.get("result_generation")),
        "supersedes_tca_result_id": _json_value(row.get("supersedes_tca_result_id")),
        "snapshot_kind": _json_value(row.get("snapshot_kind")),
        "result_status": _json_value(row.get("result_status")),
        "source_snapshot_started_at": _json_value(row.get("source_snapshot_started_at")),
        "source_snapshot_completed_at": _json_value(row.get("source_snapshot_completed_at")),
        "completed_receipt_ids": list(_completed_receipt_ids(row)),
        "versions": {
            field: _json_value(row.get(field))
            for field in (
                "calculator_version",
                "formula_version",
                "schema_version",
                "query_version",
                "benchmark_policy_version",
                "mark_policy_version",
                "fee_policy_version",
                "trade_provenance_policy_version",
            )
        },
    }


def _project_mark(row: Mapping[str, Any], pseudonymizer: AccountPseudonymizer) -> dict[str, Any]:
    fields = (
        "tca_result_id",
        "mark_id",
        "mark_role",
        "membership_hash",
        "parent_intent_id",
        "parent_revision",
        "mark_type",
        "trade_date",
        "trade_id",
        "child_order_id",
        "horizon_ms",
        "target_time",
        "market_time",
        "received_at",
        "bid_price_1",
        "ask_price_1",
        "mid_price",
        "last_price",
        "quote_source",
        "age_or_lag_ms",
        "quality",
        "market_phase",
        "stock_status",
        "raw_quote_sha256",
        "market_data_id",
        "mark_policy_version",
        "source_input_sha256",
        "evidence_sha256",
    )
    payload = _project_fields(row, fields)
    account_id = _optional_text(row.get("trade_account_id"), "trade_account_id")
    payload["trade_account_pseudonym"] = pseudonymizer.pseudonymize(account_id) if account_id else None
    return payload


def _project_trade_observation(row: Mapping[str, Any], pseudonymizer: AccountPseudonymizer) -> dict[str, Any]:
    fields = (
        "tca_result_id",
        "trade_observation_id",
        "parent_intent_id",
        "parent_revision",
        "trade_date",
        "trade_id",
        "observation_role",
        "selected_content_sha256",
        "membership_hash",
        "intent_id",
        "qmt_order_id",
        "child_order_id",
        "symbol",
        "side",
        "ingest_source",
        "observed_at",
        "broker_trade_time",
        "price",
        "quantity",
        "amount",
        "commission",
        "fee_evidence_level",
        "canonical_trade_fact_sha256",
        "timing_observation_sha256",
        "attribution_sha256",
        "fee_observation_sha256",
        "reconciliation_run_id",
        "normalization_version",
        "broker_time_parser_version",
    )
    payload = _project_fields(row, fields)
    payload["trade_account_pseudonym"] = pseudonymizer.pseudonymize(
        _required_text(row.get("trade_account_id"), "trade_account_id")
    )
    return payload


def _project_fields(row: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: _json_value(row.get(field)) for field in fields}


def _project_safe_mappings(row: Mapping[str, Any], pseudonymizer: AccountPseudonymizer) -> dict[str, Any]:
    return {field: _sanitize_mapping_value(row.get(field), pseudonymizer) for field in _RESULT_SAFE_MAPPING_FIELDS}


def _sanitize_mapping_value(value: Any, pseudonymizer: AccountPseudonymizer) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key)
            lowered = normalized_key.lower()
            if any(token in lowered for token in _BLOCKED_MAPPING_KEY_TOKENS):
                continue
            if lowered in _ACCOUNT_MAPPING_KEYS:
                sanitized[f"{normalized_key}_pseudonym"] = pseudonymizer.pseudonymize(
                    _required_text(item, normalized_key)
                )
            else:
                sanitized[normalized_key] = _sanitize_mapping_value(item, pseudonymizer)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_mapping_value(item, pseudonymizer) for item in value]
    return _json_value(value)


def _version_payload(active_version: Any) -> dict[str, Any]:
    return {**active_version.as_mapping(), "config_sha256": active_version.config_sha256}


def _decode_cursor(
    value: str | None, *, codec: TcaKeysetCursorCodec, filter_sha256: str
) -> tuple[date, str, int] | None:
    raw = _optional_text(value, "cursor")
    if raw is None:
        return None
    trade_date_text, parent_intent_id, parent_revision = codec.decode(cursor=raw, expected_filter_sha256=filter_sha256)
    try:
        parsed_trade_date = date.fromisoformat(trade_date_text)
    except ValueError as exc:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_CURSOR_INVALID",
            "cursor trade date is invalid",
            http_status=400,
            stage="TCA_READ_CURSOR",
        ) from exc
    return parsed_trade_date, parent_intent_id, parent_revision


def _encode_next_cursor(page: ExecutionTcaParentPage, *, codec: TcaKeysetCursorCodec, filter_sha256: str) -> str | None:
    if page.next_key is None:
        return None
    trade_date, parent_intent_id, parent_revision = page.next_key
    return codec.encode(
        last_key=(trade_date.isoformat(), parent_intent_id, parent_revision),
        filter_sha256=filter_sha256,
    )


def _parse_trade_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(_required_text(value, "trade_date"))
    except ValueError as exc:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "trade_date must be ISO YYYY-MM-DD",
            http_status=400,
            stage="TCA_READ_API",
            context={"field": "trade_date"},
        ) from exc


def _parse_as_of(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_required_text(value, "as_of").replace("Z", "+00:00"))
        except ValueError as exc:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_REQUEST_INVALID",
                "as_of must be an ISO timestamp with UTC offset",
                http_status=400,
                stage="TCA_READ_API",
                context={"field": "as_of"},
            ) from exc
    if parsed.tzinfo is None:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "as_of must include a UTC offset",
            http_status=400,
            stage="TCA_READ_API",
            context={"field": "as_of"},
        )
    return parsed.astimezone(UTC)


def _parent_not_found(parent_intent_id: str, parent_revision: int | str | None) -> TcaReadError:
    return TcaReadError(
        "ADAPTIVE_IS_TCA_PARENT_NOT_FOUND",
        "no SIM parent benchmark matched the requested parent identity",
        http_status=404,
        stage="TCA_READ_API",
        context={"parent_intent_id": str(parent_intent_id or ""), "parent_revision": parent_revision},
    )


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if text:
        return text
    raise TcaReadError(
        "ADAPTIVE_IS_TCA_REQUEST_INVALID",
        f"{field} must not be empty",
        http_status=400,
        stage="TCA_READ_API",
        context={"field": field},
    )


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            f"{field} must be a positive integer",
            http_status=400,
            stage="TCA_READ_API",
            context={"field": field},
        ) from exc
    if parsed <= 0:
        raise TcaReadError(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            f"{field} must be a positive integer",
            http_status=400,
            stage="TCA_READ_API",
            context={"field": field},
        )
    return parsed


def _completed_receipt_ids(row: Mapping[str, Any]) -> tuple[str, ...]:
    raw = row.get("completed_receipt_ids") or ()
    if isinstance(raw, str):
        raw = (raw,)
    return tuple(sorted({str(item) for item in raw if str(item).strip()}))


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, datetime):
        return (value if value.tzinfo is not None else value.replace(tzinfo=UTC)).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def render_canonical_evidence_export(export: ExecutionTcaEvidenceExport, *, output_format: str) -> str:
    """Render deterministic JSON or NDJSON without a noncanonical timestamp."""

    if output_format == "json":
        return _canonical_json({"manifest": export.manifest, "records": export.records}) + "\n"
    if output_format == "ndjson":
        lines = [_canonical_json({"record_type": "manifest", "manifest": export.manifest})]
        lines.extend(_canonical_json(record) for record in export.records)
        return "\n".join(lines) + "\n"
    raise TcaReadError(
        "ADAPTIVE_IS_TCA_EXPORT_FORMAT_UNSUPPORTED",
        "output format must be json or ndjson",
        http_status=400,
        stage="TCA_EXPORT",
        context={"output_format": output_format},
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(_json_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _evidence_record_sort_key(record: Mapping[str, Any]) -> tuple[str, str, int]:
    parent = record["parent"]
    return (
        str(parent["trade_date"]),
        str(parent["parent_intent_id"]),
        int(parent["parent_revision"]),
    )
