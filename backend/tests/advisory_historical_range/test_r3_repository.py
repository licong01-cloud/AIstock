from datetime import UTC, date, datetime
from decimal import Decimal
import inspect
from types import SimpleNamespace

import pytest
from psycopg2 import errors as postgres_errors

from backend.services.advisory_historical_range.executor import (
    _classify_failure,
    _decision_cutoff,
    _list_all_execution_runs,
)
from backend.services.advisory_historical_range.catalog_planner import HistoricalRangeSourceInputUnavailable
from backend.services.advisory_historical_range.models import (
    HistoricalRangeContractError,
    HistoricalRangeCandidateFactV1,
    HistoricalRangeDayStatus,
    HistoricalRangeExecutionOperationV1,
    HistoricalRangeListItemFactV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeRunExecutionReceiptV1,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.tests.advisory_historical_range.conftest import digest


def test_decision_cutoff_is_shanghai_close_normalized_to_utc() -> None:
    assert _decision_cutoff(date(2026, 7, 22)) == datetime(2026, 7, 22, 7, tzinfo=UTC)


def test_deterministic_contract_errors_are_terminal_not_retryable() -> None:
    assert _classify_failure(ValueError("bad contract")) == (
        HistoricalRangeDayStatus.FAILED,
        ("ADVISORY_HR_DETERMINISTIC_CONTRACT_FAILURE",),
    )
    assert _classify_failure(HistoricalRangeContractError("EXACT_REASON", "bad evidence")) == (
        HistoricalRangeDayStatus.FAILED,
        ("EXACT_REASON",),
    )
    assert _classify_failure(
        DataUnavailableError(
            "package input unavailable",
            context={"reason_code": "STRATEGY_PACKAGE_RUNTIME_ASSETS_INCOMPLETE"},
        )
    ) == (
        HistoricalRangeDayStatus.WAITING_INPUT,
        ("STRATEGY_PACKAGE_RUNTIME_ASSETS_INCOMPLETE",),
    )
    assert _classify_failure(
        HistoricalRangeSourceInputUnavailable(
            "ADVISORY_HR_SOURCE_REVISION_MISMATCH",
            "sealed historical source is no longer available",
        )
    ) == (
        HistoricalRangeDayStatus.WAITING_INPUT,
        ("ADVISORY_HR_SOURCE_REVISION_MISMATCH",),
    )
    assert _classify_failure(RuntimeConfigInvalidError("invalid frozen runtime")) == (
        HistoricalRangeDayStatus.FAILED,
        ("ADVISORY_HR_RUNTIME_CONFIG_INVALID",),
    )
    assert _classify_failure(postgres_errors.DiskFull("shared memory exhausted")) == (
        HistoricalRangeDayStatus.RETRYABLE_FAILED,
        ("ADVISORY_HR_DATABASE_CAPACITY_EXHAUSTED",),
    )


def test_repository_sql_closes_waiting_stage_and_operation_worker_lease_identity() -> None:
    transition_batch_source = inspect.getsource(PostgresHistoricalRangeRepository.transition_batch)
    transition_operation_source = inspect.getsource(PostgresHistoricalRangeRepository.transition_operation)

    assert "waiting_stage = %s" in transition_batch_source
    assert '"DAY_INPUT" if target_status is HistoricalRangeBatchStatus.WAITING_INPUT else None' in (
        transition_batch_source
    )
    assert "worker_id = %s" in transition_operation_source
    assert "lease_token = %s" in transition_operation_source
    assert "lease_expires_at = %s" in transition_operation_source
    assert "fencing_token = COALESCE(%s, fencing_token)" in transition_operation_source


def test_batch_aggregate_refresh_locks_batch_before_reading_child_counts() -> None:
    source = inspect.getsource(PostgresHistoricalRangeRepository._sync_batch_aggregate)

    lock_position = source.index("FOR UPDATE")
    aggregate_position = source.index("self._batch_aggregate")
    update_position = source.index("UPDATE app.advisory_historical_range_batch")
    assert lock_position < aggregate_position < update_position


def test_retryable_day_claim_refreshes_run_aggregate_before_returning_claim() -> None:
    source = inspect.getsource(PostgresHistoricalRangeRepository.claim_next_day)

    running_update_position = source.index("SET status = 'RUNNING'")
    aggregate_refresh_position = source.index("self._sync_run_aggregate")
    return_position = source.index("return self._claimed_day_from_row")
    assert running_update_position < aggregate_refresh_position < return_position


def test_successful_day_readback_allows_only_its_typed_direct_predecessor_for_decision_marks() -> None:
    source = inspect.getsource(PostgresHistoricalRangeRepository.full_readback_successful_day)
    mark_readback = source[source.index("mark_envelope = self._load_artifact(") :]

    assert "expected_kind=HistoricalRangeArtifactKind.DECISION_MARK_SET" in mark_readback
    assert "allow_direct_predecessor_day_run_id=(" in mark_readback
    assert 'str(day["previous_day_run_id"]) if day["previous_day_run_id"] is not None else None' in mark_readback
    assert "validate_recursive_upstream=False" in mark_readback
    assert "mark_set.predecessor_day_receipt_ref != receipt.previous_day_receipt_ref" in mark_readback
    assert "decision mark upstream set differs from its typed direct lineage" in mark_readback


def test_non_running_operation_retains_historical_fencing_without_active_lease() -> None:
    operation = HistoricalRangeExecutionOperationV1(
        operation_id="operation-1",
        batch_id="batch-1",
        operation_type="RESUME",
        operation_idempotency_key="resume-1",
        idempotency_payload_hash=digest("operation-input"),
        resolved_request_hash=digest("request"),
        expected_row_version=1,
        status=HistoricalRangeOperationStatus.RETRYABLE_FAILED,
        row_version=3,
        attempt_no=1,
        fencing_token=1,
    )

    assert operation.worker_id is None
    assert operation.lease_token is None
    assert operation.lease_expires_at is None
    assert operation.fencing_token == 1


def test_list_item_score_is_quantized_to_persisted_numeric_scale_before_hashing() -> None:
    item = HistoricalRangeListItemFactV1(
        list_item_id="item-1",
        list_version_id="list-1",
        symbol="000001.SZ",
        action="WATCH",
        rank=1,
        score=Decimal("0.7823946475982666"),
        reason_codes=("RANKED",),
        rule_guidance_json={
            "schema_version": "advisory_historical_range_rule_guidance_v2",
            "action": "WATCH",
            "intended_execution_trade_date": None,
            "intended_execution_basis": None,
            "execution_status": "NOT_APPLICABLE",
            "market_state_reason": None,
            "requested_execution_basis": None,
            "range_end_reason": None,
        },
        execution_status="NOT_APPLICABLE",
    )

    assert item.score == Decimal("0.782394647598")
    assert HistoricalRangeListItemFactV1.model_validate(item.model_dump(mode="json")) == item


def test_candidate_scores_are_quantized_to_persisted_numeric_scale_before_hashing() -> None:
    lineage = {"source": "selection", "rank": 1}
    candidate = HistoricalRangeCandidateFactV1(
        candidate_id=derive_prefixed_id("ahc", {"day_run_id": "day-1", "symbol": "000001.SZ"}),
        day_run_id="day-1",
        symbol="000001.SZ",
        membership_status="INCLUDED",
        alpha_raw_rank=1,
        alpha_raw_score=Decimal("4.257508715025424"),
        hmm_adjusted_rank=1,
        hmm_adjusted_score=Decimal("4.257508715025424"),
        risk_policy_adjusted_rank=1,
        risk_policy_adjusted_score=Decimal("4.257508715025424"),
        selection_effective_rank=1,
        selection_effective_score=Decimal("4.257508715025424"),
        advisory_model_rank=1,
        advisory_model_score=Decimal("4.257508715025424"),
        component_lineage_json=lineage,
        component_lineage_hash=digest(lineage),
    )

    assert candidate.alpha_raw_score == Decimal("4.257508715025")
    assert candidate.selection_effective_score == Decimal("4.257508715025")
    assert HistoricalRangeCandidateFactV1.model_validate(candidate.model_dump(mode="json")) == candidate


def test_completed_run_receipt_rejects_unexecuted_or_blocking_state() -> None:
    with pytest.raises(ValueError, match="completed run receipt"):
        HistoricalRangeRunExecutionReceiptV1(
            range_run_id="range-1",
            research_program_id="program-1",
            status="COMPLETED",
            resolved_request_hash=digest("request"),
            successful_day_count=0,
            failed_day_count=0,
            unexecuted_day_count=1,
        )


def test_execution_run_readback_paginates_beyond_repository_page_limit() -> None:
    rows = tuple(SimpleNamespace(research_program_id=f"program-{index:04d}") for index in range(501))

    class _PagedRepository:
        def __init__(self) -> None:
            self.cursors: list[str | None] = []

        def list_execution_runs(self, *, stable_after_research_program_id=None, limit, **_kwargs):
            self.cursors.append(stable_after_research_program_id)
            start = 0
            if stable_after_research_program_id is not None:
                start = next(
                    index
                    for index, row in enumerate(rows)
                    if row.research_program_id > stable_after_research_program_id
                )
            return rows[start : start + limit]

    repository = _PagedRepository()
    result = _list_all_execution_runs(repository=repository, batch_id="batch-1")

    assert result == rows
    assert repository.cursors == [None, "program-0499"]
