from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeWorkItemV1,
)
from backend.services.advisory_historical_range.outcome_evaluator import (
    FrozenHistoricalRangeOutcomeInputFactory,
    HistoricalRangeAggregateOutcomeEvaluator,
    HistoricalRangeOutcomeSubjectInputV1,
    HistoricalRangeValuationPolicyBundleV1,
    HistoricalRangeValuationPolicySetV1,
    PostgresHistoricalRangeOutcomeEvaluator,
    PostgresHistoricalRangeOutcomeSubjectInputProvider,
)
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeSymbolPathReceiptV1,
)
from backend.services.advisory_historical_range.outcome_projection import (
    HistoricalRangeAggregateCalculationResultV1,
    HistoricalRangeOutcomeProjectionBuilder,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.outcome_engine import (
    OutcomeOwner,
    OwnerType,
    TerminalDisposition,
    TerminalResolution,
)
from backend.tests.advisory_phase1.test_outcome_engine import (
    AS_OF,
    _path,
    _policies,
    _source_binding,
    _source_revision_set,
)
from backend.tests.advisory_historical_range.test_r4_outcome_service import (
    _Evaluator as _ChildEvaluator,
    _work_item as _child_work_item,
)


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{digest}.json",
        producer_contract_version="r4_test",
        payload_schema_version="r4_test",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _range_policy(policy_hash: str) -> HistoricalRangeValuationPolicySetV1:
    formal = _policies()
    component_hashes = {
        role: str(index) * 64
        for index, role in enumerate(
            (
                "CALENDAR",
                "MARKET_DATA",
                "EXECUTION",
                "COST",
                "BENCHMARK",
                "CASH_RETURN",
                "TERMINAL",
                "BARRIER",
                "CORPORATE_ACTION",
            ),
            start=1,
        )
    }
    return HistoricalRangeValuationPolicySetV1(
        bundle=HistoricalRangeValuationPolicyBundleV1(
            policy_bundle_hash=policy_hash,
            calendar_version=formal.calendar.calendar_version,
            calendar_hash=str(formal.calendar.calendar_hash),
            component_hashes=component_hashes,
            horizons=(1, 2),
            projections_by_horizon={
                1: (
                    Projection.RETURN_GROSS,
                    Projection.RETURN_NET_ABSOLUTE,
                    Projection.RETURN_NET_EXCESS,
                    Projection.PATH_MFE,
                    Projection.PATH_MAE,
                    Projection.EXECUTABLE_MFE,
                    Projection.EXECUTABLE_MAE,
                ),
                2: (Projection.RETURN_GROSS,),
            },
            candidate_reference_notional=formal.bundle.candidate_reference_notional,
            benchmark_portfolio_notional=formal.bundle.benchmark_portfolio_notional,
        ),
        calendar=formal.calendar,
        market_data=formal.market_data,
        execution=formal.execution,
        cost=formal.cost,
        benchmark=formal.benchmark,
        cash_return=formal.cash_return,
        barrier=formal.barrier,
        terminal=formal.terminal,
    )


class _PolicyProvider:
    def __init__(self, policy: HistoricalRangeValuationPolicySetV1) -> None:
        self.policy = policy

    def load(self, policy_bundle_hash: str) -> HistoricalRangeValuationPolicySetV1:
        assert policy_bundle_hash == self.policy.bundle.policy_bundle_hash
        return self.policy


class _SubjectProvider:
    def __init__(self, subject: HistoricalRangeOutcomeSubjectInputV1) -> None:
        self.subject = subject

    def load(self, work_item: HistoricalRangeOutcomeWorkItemV1) -> HistoricalRangeOutcomeSubjectInputV1:
        return self.subject


class _SourceProvider:
    def __init__(self, receipt: HistoricalRangeSymbolPathReceiptV1) -> None:
        self.receipt = receipt

    def load_symbol_path(self, request):
        assert canonical_json_sha256(request.model_dump(mode="json")) == self.receipt.request_hash
        return self.receipt


class _MetadataCursor:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.rows = rows
        self.query = ""
        self.params: tuple[object, ...] = ()

    def __enter__(self) -> "_MetadataCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.query = query
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)


class _MetadataConnection:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.cursor_instance = _MetadataCursor(rows)

    def __enter__(self) -> "_MetadataConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_session(self, **_kwargs: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _MetadataCursor:
        return self.cursor_instance

    def rollback(self) -> None:
        return None


def _work_item(
    *,
    subject_type: HistoricalRangeOutcomeSubjectType = HistoricalRangeOutcomeSubjectType.CANDIDATE,
    projection: HistoricalRangeOutcomeProjection = HistoricalRangeOutcomeProjection.RECOMMENDATION,
    window: HistoricalRangeEvaluationWindowType = HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
    horizon: int = 1,
    exit_trade_date: date = date(2026, 7, 8),
) -> HistoricalRangeOutcomeWorkItemV1:
    ref = _ref(
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT
        if subject_type is HistoricalRangeOutcomeSubjectType.CANDIDATE
        else HistoricalRangeArtifactKind.DAY_RECEIPT,
        "a",
    )
    source_hash = canonical_json_sha256([ref.model_dump(mode="json")])
    return HistoricalRangeOutcomeWorkItemV1(
        range_run_id="run-1",
        subject_type=subject_type,
        subject_id="candidate-1"
        if subject_type is HistoricalRangeOutcomeSubjectType.CANDIDATE
        else "episode-1",
        subject_ref=ref,
        policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "f"),
        projection=projection,
        evaluation_window_type=window,
        horizon_trade_days=horizon,
        policy_bundle_hash="f" * 64,
        decision_trade_date=date(2026, 7, 3),
        intended_entry_trade_date=date(2026, 7, 6),
        earliest_sell_trade_date=date(2026, 7, 7),
        exit_trade_date=exit_trade_date,
        label_as_of_trade_date=date(2026, 7, 10),
        source_revision_refs=(ref,),
        source_revision_set_hash=source_hash,
        producer_code_hash="e" * 64,
        outcome_contract_version="r4_v1",
        revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
    )


def test_postgres_subject_metadata_binds_episode_to_exact_day_receipt() -> None:
    item = _work_item(
        subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
        projection=HistoricalRangeOutcomeProjection.RECOMMENDATION,
        window=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
        horizon=0,
    )
    connection = _MetadataConnection(
        (
            {
                "symbol": "000001.SZ",
                "end_decision_trade_date": date(2026, 7, 8),
                "episode_closed": True,
            },
        )
    )
    provider = object.__new__(PostgresHistoricalRangeOutcomeSubjectInputProvider)
    provider._conn_factory = lambda: connection

    metadata = provider._metadata(item)

    assert metadata["episode_closed"] is True
    assert "day.day_receipt_ref = %s" in connection.cursor_instance.query
    assert "LIMIT 1" not in connection.cursor_instance.query.upper()
    assert connection.cursor_instance.params[-1].adapted == item.subject_ref.model_dump(
        mode="json"
    )


def test_postgres_subject_metadata_rejects_ambiguous_exact_ref_rows() -> None:
    item = _work_item()
    row = {
        "symbol": "000001.SZ",
        "end_decision_trade_date": date(2026, 7, 3),
        "episode_closed": False,
    }
    connection = _MetadataConnection((row, row))
    provider = object.__new__(PostgresHistoricalRangeOutcomeSubjectInputProvider)
    provider._conn_factory = lambda: connection

    with pytest.raises(ValueError, match="unavailable or ambiguous"):
        provider._metadata(item)

    assert "candidate.artifact_ref = %s" in connection.cursor_instance.query


def _subject(
    item: HistoricalRangeOutcomeWorkItemV1,
    *,
    terminal: TerminalResolution | None = None,
    episode_closed: bool = False,
) -> HistoricalRangeOutcomeSubjectInputV1:
    return HistoricalRangeOutcomeSubjectInputV1(
        owner=OutcomeOwner(
            owner_type=OwnerType.CANDIDATE,
            owner_key=item.subject_id,
            canonical_signal_id=f"range-signal:{item.subject_id}",
            observation_version_id=f"range-observation:{item.subject_id}",
            candidate_stage_evidence_id=f"range-stage:{item.subject_id}",
            symbol="000001.SZ",
            decision_as_of_trade_date=item.decision_trade_date,
        ),
        label_as_of_ts=AS_OF,
        source_artifact_ref_set_hash=item.source_revision_set_hash,
        source_revision_set=_source_revision_set(),
        price_source=_source_binding(),
        adjustment_source=_source_binding(),
        tradability_source=_source_binding(),
        terminal=terminal or TerminalResolution(disposition=TerminalDisposition.NONE),
        lifecycle_timeline=(
            item.decision_trade_date,
            item.intended_entry_trade_date,
            item.earliest_sell_trade_date,
            item.exit_trade_date,
        )
        if item.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE
        else None,
        episode_closed=episode_closed,
    )


def test_range_native_factory_uses_t_mark_without_phase0a_policy_identity() -> None:
    item = _work_item()
    policy = _range_policy(item.policy_bundle_hash)
    subject = _subject(item)
    factory = FrozenHistoricalRangeOutcomeInputFactory(
        subject_provider=_SubjectProvider(subject),
        policy_provider=_PolicyProvider(policy),
    )
    source_request = factory.source_request(item)
    receipt = HistoricalRangeSymbolPathReceiptV1(
        request_hash=canonical_json_sha256(source_request.model_dump(mode="json")),
        source_revision_set_hash=item.source_revision_set_hash,
        price_path=_path(),
        row_count=len(_path().bars),
    )
    evaluator = PostgresHistoricalRangeOutcomeEvaluator(
        source_provider=_SourceProvider(receipt),
        input_factory=factory,
    )

    result = evaluator.evaluate(item)

    assert result.calculation_results[0].entry_price_raw_yuan == 10
    assert result.calculation_results[0].calculation_evidence.evidence_payload["policy_bundle_hash"] == "f" * 64
    assert "phase1_handoff_bundle_hash" not in policy.bundle.model_dump(mode="json")
    assert "admission_scope_id" not in policy.bundle.model_dump(mode="json")


def test_range_native_episode_stays_open_and_zero_sentinel() -> None:
    item = _work_item(
        subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
        projection=HistoricalRangeOutcomeProjection.EXECUTABLE,
        window=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
        horizon=0,
    )
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=date(2026, 7, 8),
        event_closed_at=AS_OF,
        source=_source_binding(),
        censor_reason_code="RANGE_END_ACTIVE",
    )
    subject = _subject(item, terminal=terminal, episode_closed=False)
    factory = FrozenHistoricalRangeOutcomeInputFactory(
        subject_provider=_SubjectProvider(subject),
        policy_provider=_PolicyProvider(_range_policy(item.policy_bundle_hash)),
    )
    source_request = factory.source_request(item)
    receipt = HistoricalRangeSymbolPathReceiptV1(
        request_hash=canonical_json_sha256(source_request.model_dump(mode="json")),
        source_revision_set_hash=item.source_revision_set_hash,
        price_path=_path(),
        row_count=len(_path().bars),
    )

    result = PostgresHistoricalRangeOutcomeEvaluator(
        source_provider=_SourceProvider(receipt),
        input_factory=factory,
    ).evaluate(item)

    assert result.maturity_status.value == "CENSORED"
    assert result.horizon_trade_days == 0
    assert {entry.horizon_trading_days for entry in result.calculation_results} == {0}


def test_range_native_episode_censored_before_entry_preserves_range_end() -> None:
    range_end = date(2026, 7, 3)
    item = _work_item(
        subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
        projection=HistoricalRangeOutcomeProjection.RECOMMENDATION,
        window=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
        horizon=0,
        exit_trade_date=range_end,
    )
    terminal = TerminalResolution(
        disposition=TerminalDisposition.RIGHT_CENSORED,
        symbol="000001.SZ",
        event_trade_date=range_end,
        event_closed_at=AS_OF,
        source=_source_binding(),
        censor_reason_code="RANGE_END_ACTIVE",
    )
    subject = _subject(item, terminal=terminal, episode_closed=False)
    factory = FrozenHistoricalRangeOutcomeInputFactory(
        subject_provider=_SubjectProvider(subject),
        policy_provider=_PolicyProvider(_range_policy(item.policy_bundle_hash)),
    )
    source_request = factory.source_request(item)
    receipt = HistoricalRangeSymbolPathReceiptV1(
        request_hash=canonical_json_sha256(source_request.model_dump(mode="json")),
        source_revision_set_hash=item.source_revision_set_hash,
        price_path=_path(),
        row_count=len(_path().bars),
    )

    result = PostgresHistoricalRangeOutcomeEvaluator(
        source_provider=_SourceProvider(receipt),
        input_factory=factory,
    ).evaluate(item)

    calculation = result.calculation_results[0]
    assert result.maturity_status.value == "CENSORED"
    assert "EPISODE_RIGHT_CENSORED_BEFORE_EARLIEST_SELL" in result.reason_codes
    assert calculation.projection_value_decimal is None
    assert calculation.entry_status.value == "UNAVAILABLE"
    assert calculation.exit_trade_date == range_end
    assert calculation.observed_holding_trading_days == 0
    assert calculation.event_closed_at == AS_OF


class _AggregateProvider:
    def __init__(self, children):
        self.children = children

    def list_child_outcomes_for_aggregate(self, *, work_item):
        return self.children


def test_list_aggregate_uses_child_outcome_artifacts_without_price_read(
    tmp_path: Path,
) -> None:
    child_item = _child_work_item()
    child_result = _ChildEvaluator().evaluate(child_item)
    builder = HistoricalRangeOutcomeProjectionBuilder()
    artifact = builder.build_artifact(
        work_item=child_item,
        result=child_result,
        outcome_version=1,
    )
    store = HistoricalRangeArtifactStore(root=tmp_path)
    stored = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.OUTCOME,
        producer_contract_version="r4_v1",
        payload_schema_version=artifact.schema_version,
        resolved_request_hash="9" * 64,
        payload=artifact.model_dump(mode="json"),
        range_run_id="run-1",
        upstream_refs=artifact.direct_upstream_refs,
    )
    child_fact = builder.build_fact(
        work_item=child_item,
        result=child_result,
        artifact=artifact,
        outcome_artifact_ref=stored.ref,
        outcome_version=1,
    )
    list_item = _work_item(
        subject_type=HistoricalRangeOutcomeSubjectType.LIST_VERSION,
        projection=HistoricalRangeOutcomeProjection.EXECUTABLE,
    )
    list_payload = list_item.model_dump(
        mode="python", exclude={"outcome_logical_id", "outcome_input_hash"}
    )
    list_payload.update(
        source_revision_refs=(
            *list_item.source_revision_refs,
            child_fact.outcome_artifact_ref,
        ),
        source_artifact_ref_set_hash=None,
        source_revision_set_hash=canonical_json_sha256(
            [child_fact.outcome_artifact_ref.model_dump(mode="json")]
        ),
    )
    list_item = HistoricalRangeOutcomeWorkItemV1.model_validate(list_payload)

    result = HistoricalRangeAggregateOutcomeEvaluator(
        provider=_AggregateProvider((child_fact,)),
        artifact_store=store,
    ).evaluate(list_item)

    aggregate = result.calculation_results[0]
    assert isinstance(aggregate, HistoricalRangeAggregateCalculationResultV1)
    assert aggregate.projection_value_decimal == Decimal("0.089108910891")
    assert aggregate.child_outcome_refs == (child_fact.outcome_artifact_ref,)
