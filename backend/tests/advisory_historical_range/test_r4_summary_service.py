from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.composition import (
    build_historical_range_summary_coordinator,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore

from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeSummaryArtifactV2,
    HistoricalRangeSummaryPolicyV1,
)
from backend.services.advisory_historical_range.summary_service import (
    HistoricalRangeRecallDenominatorV1,
    HistoricalRangeRecallDenominatorSetV1,
    HistoricalRangeSummaryCoordinatorService,
    HistoricalRangeSummaryInputRowV1,
    HistoricalRangeSummaryOutcomeSetV1,
    HistoricalRangeSummaryService,
    HistoricalRangeSummaryRowContextV1,
    Phase1UniverseOutcomeEvidenceRefV1,
    Phase1WinnerDefinitionV1,
    PostgresHistoricalRangeSummaryContextProvider,
    PostgresHistoricalRangeSummaryOutcomeSetLoader,
    PostgresHistoricalRangeRecallDenominatorProvider,
)
from backend.services.advisory_historical_range.outcome_projection import (
    HistoricalRangeOutcomeProjectionBuilder,
)
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
)
from backend.tests.advisory_historical_range.test_r4_outcome_service import (
    _Evaluator,
    _work_item,
)


def _ref(char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    return HistoricalRangeArtifactRefV1(
        artifact_kind=HistoricalRangeArtifactKind.OUTCOME,
        relative_path=f"outcomes/{digest}.json",
        producer_contract_version="r4_v1",
        payload_schema_version="outcome_v2",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _policy() -> HistoricalRangeSummaryPolicyV1:
    return HistoricalRangeSummaryPolicyV1(
        subject_types=(HistoricalRangeOutcomeSubjectType.CANDIDATE,),
        projection_groups=(HistoricalRangeOutcomeProjection.EXECUTABLE,),
        evaluation_window_types=(HistoricalRangeEvaluationWindowType.FIXED_HORIZON,),
        horizons=(5,),
        outcome_policy_bundle_hash="f" * 64,
        recall_k_values=(2,),
    )


def _winner(*, projection: str = "RETURN_NET_ABSOLUTE", horizon: int = 5):
    return Phase1WinnerDefinitionV1(
        winner_definition_id="positive-return-v1",
        projection=projection,
        comparison_operator="GT",
        threshold=Decimal("0"),
        ranking_direction="DESC",
        horizon_trade_days=horizon,
        label_policy_hash="3" * 64,
        denominator_universe_layer="pit-universe-v1",
    )


def _universe_ref(
    char: str,
    *,
    symbol: str,
    projection: str = "RETURN_NET_ABSOLUTE",
    horizon: int = 5,
    value: str = "0.10",
    industry: str | None = None,
    regime: str | None = None,
) -> Phase1UniverseOutcomeEvidenceRefV1:
    payload = {
        "snapshot_id": "snapshot-1",
        "snapshot_content_hash": "4" * 64,
        "manifest_sha256": "5" * 64,
        "snapshot_source_revision_set_hash": "6" * 64,
        "logical_path": f"universe_outcomes/horizon={horizon}/year=2026/month=07/part-{char}.parquet",
        "file_sha256": char * 64,
        "file_size_bytes": 100,
        "file_row_count": 1,
        "partition_content_hash": "7" * 64,
        "raw_outcome_id": f"uor-{char}",
        "raw_outcome_hash": "8" * 64,
        "decision_trade_date": date(2026, 7, 1),
        "symbol": symbol,
        "projection": projection,
        "horizon_trade_days": horizon,
        "projection_value_decimal": value,
        "maturity_status": "MATURED",
        "outcome_event_status": "NONE",
        "label_policy_bundle_hash": "9" * 64,
        "label_policy_hash": "3" * 64,
        "universe_policy_hash": "1" * 64,
        "universe_layer": "pit-universe-v1",
        "calculation_evidence_sha256": "a" * 64,
        "calculation_evidence_size_bytes": 100,
        "calculation_evidence_store_backend_hash": "b" * 64,
        "label_source_revision_set_hash": "2" * 64,
        "industry_at_t": industry,
        "industry_evidence_hash": "c" * 64 if industry is not None else None,
        "market_regime_at_t": regime,
        "market_regime_evidence_hash": "d" * 64 if regime is not None else None,
    }
    payload["canonical_row_hash"] = canonical_json_sha256(payload)
    return Phase1UniverseOutcomeEvidenceRefV1.model_validate(payload)


def _row(
    char: str,
    *,
    signal: str,
    symbol: str,
    day: date,
    list_id: str,
    value: str | None,
    status: HistoricalRangeOutcomeStatus = HistoricalRangeOutcomeStatus.COMPLETE,
    industry: str = "I1",
    regime: str = "R1",
) -> HistoricalRangeSummaryInputRowV1:
    return HistoricalRangeSummaryInputRowV1(
        outcome_ref=_ref(char),
        outcome_logical_id=f"logical-{char}",
        canonical_signal_id=signal,
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id=f"candidate-{char}",
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        projection="RETURN_NET_ABSOLUTE",
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=5,
        decision_trade_date=day,
        list_version_id=list_id,
        symbol=symbol,
        maturity_status=status,
        value=Decimal(value) if value is not None else None,
        industry_at_t=industry,
        market_regime_at_t=regime,
    )


def test_summary_formulas_keep_coverage_and_missing_recall_visible() -> None:
    rows = (
        _row("a", signal="s1", symbol="000001.SZ", day=date(2026, 7, 1), list_id="l1", value="0.10"),
        _row("b", signal="s2", symbol="000002.SZ", day=date(2026, 7, 1), list_id="l1", value="-0.05", industry="I2"),
        _row("c", signal="s3", symbol="000001.SZ", day=date(2026, 7, 2), list_id="l2", value="0.00"),
        _row(
            "d",
            signal="s4",
            symbol="000003.SZ",
            day=date(2026, 7, 2),
            list_id="l2",
            value=None,
            status=HistoricalRangeOutcomeStatus.CENSORED,
        ),
    )
    artifact = HistoricalRangeSummaryService().calculate(
        range_run_id="run-1",
        rows=rows,
        policy=_policy(),
        producer_code_hash="f" * 64,
    )
    metric_by_suffix = {item["metric_key"].split(":")[-1]: item for item in artifact.metrics}
    assert metric_by_suffix["win_rate"]["value"] == "0.333333333333"
    assert metric_by_suffix["odds"]["value"] == "2.000000000000"
    assert metric_by_suffix["max_drawdown"]["value"] == "0.000000000000"
    assert metric_by_suffix["turnover"]["coverage"] == {"adjacent_pair_count": 1}
    coverage = next(iter(artifact.maturity_coverage.values()))
    assert coverage["eligible_total"] == 4
    assert coverage["censored"] == 1
    assert any(
        item["reason_code"] == "PIT_ELIGIBLE_DENOMINATOR_UNAVAILABLE"
        for item in artifact.unavailable_metrics
    )
    assert any(
        item.get("group_key") == "REGIME:R1"
        for item in artifact.metrics
    )


def test_closed_censored_episode_does_not_invent_holding_days() -> None:
    row = HistoricalRangeSummaryInputRowV1(
        outcome_ref=_ref("e"),
        outcome_logical_id="episode-logical",
        canonical_signal_id="range-subject:EPISODE:episode-1",
        subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
        subject_id="episode-1",
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        projection="RETURN_GROSS",
        evaluation_window_type=HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE,
        horizon_trade_days=0,
        decision_trade_date=date(2026, 7, 1),
        maturity_status=HistoricalRangeOutcomeStatus.CENSORED,
        episode_closed=True,
        observed_holding_trading_days=None,
    )

    assert row.observed_holding_trading_days is None


def test_recall_uses_exact_positive_target_denominator() -> None:
    rows = (
        _row("a", signal="s1", symbol="000001.SZ", day=date(2026, 7, 1), list_id="l1", value="0.10"),
        _row("b", signal="s2", symbol="000002.SZ", day=date(2026, 7, 1), list_id="l1", value="-0.05"),
    )
    refs = (
        _universe_ref("e", symbol="000001.SZ", value="0.20"),
        _universe_ref("f", symbol="000002.SZ", value="-0.05"),
        _universe_ref("1", symbol="000003.SZ", value="0.10"),
    )
    denominator = HistoricalRangeRecallDenominatorV1(
        decision_trade_date=date(2026, 7, 1),
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        projection="RETURN_NET_ABSOLUTE",
        horizon_trade_days=5,
        k=2,
        strategy_symbols=("000001.SZ", "000002.SZ"),
        positive_target_symbols=("000001.SZ", "000003.SZ"),
        eligible_universe_symbols=("000001.SZ", "000002.SZ", "000003.SZ"),
        eligible_universe_refs=refs,
        eligible_universe_set_hash=canonical_json_sha256(
            {
                "decision_trade_date": date(2026, 7, 1),
                "eligible_symbols": ("000001.SZ", "000002.SZ", "000003.SZ"),
                "eligible_outcome_refs": [
                    item.model_dump(mode="json") for item in refs
                ],
            }
        ),
        eligible_universe_source_hash="9" * 64,
        label_policy_bundle_hash="9" * 64,
        label_policy_hash="3" * 64,
        universe_policy_hash="1" * 64,
        universe_layer="pit-universe-v1",
        source_revision_set_hash="2" * 64,
        winner_definition=_winner(),
    )
    artifact = HistoricalRangeSummaryService().calculate(
        range_run_id="run-1",
        rows=rows,
        policy=_policy(),
        producer_code_hash="f" * 64,
        recall_denominator_set=HistoricalRangeRecallDenominatorSetV1(
            availability="AVAILABLE",
            denominators=(denominator,),
        ),
    )
    recall = next(item for item in artifact.metrics if item["metric_key"].startswith("strategy_recall@2"))
    assert recall["value"] == "0.500000000000"
    assert recall["coverage"]["target_set_size"] == 2


def test_recall_top_k_preserves_strategy_rank_instead_of_symbol_order() -> None:
    refs = (
        _universe_ref("e", symbol="000001.SZ", value="-0.10"),
        _universe_ref("f", symbol="000002.SZ", value="0.10"),
    )
    denominator = HistoricalRangeRecallDenominatorV1(
        decision_trade_date=date(2026, 7, 1),
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        projection="RETURN_NET_ABSOLUTE",
        horizon_trade_days=5,
        k=1,
        strategy_symbols=("000002.SZ", "000001.SZ"),
        positive_target_symbols=("000002.SZ",),
        eligible_universe_symbols=("000001.SZ", "000002.SZ"),
        eligible_universe_refs=refs,
        eligible_universe_set_hash=canonical_json_sha256(
            {
                "decision_trade_date": date(2026, 7, 1),
                "eligible_symbols": ("000001.SZ", "000002.SZ"),
                "eligible_outcome_refs": [
                    item.model_dump(mode="json") for item in refs
                ],
            }
        ),
        eligible_universe_source_hash="9" * 64,
        label_policy_bundle_hash="9" * 64,
        label_policy_hash="3" * 64,
        universe_policy_hash="1" * 64,
        universe_layer="pit-universe-v1",
        source_revision_set_hash="2" * 64,
        winner_definition=_winner(),
    )

    artifact = HistoricalRangeSummaryService().calculate(
        range_run_id="run-1",
        rows=(),
        policy=_policy(),
        producer_code_hash="f" * 64,
        recall_denominator_set=HistoricalRangeRecallDenominatorSetV1(
            availability="AVAILABLE",
            denominators=(denominator,),
        ),
    )

    recall = next(
        item
        for item in artifact.metrics
        if item["metric_key"].startswith("strategy_recall@1")
    )
    assert recall["value"] == "1.000000000000"


def test_recall_denominator_revision_changes_summary_input_identity() -> None:
    rows = (
        _row(
            "a",
            signal="s1",
            symbol="000001.SZ",
            day=date(2026, 7, 1),
            list_id="l1",
            value="0.10",
        ),
    )

    def evidence(file_char: str) -> HistoricalRangeRecallDenominatorSetV1:
        refs = (
            _universe_ref(file_char, symbol="000001.SZ", value="0.10"),
        )
        denominator = HistoricalRangeRecallDenominatorV1(
            decision_trade_date=date(2026, 7, 1),
            projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
            projection="RETURN_NET_ABSOLUTE",
            horizon_trade_days=5,
            k=2,
            strategy_symbols=("000001.SZ",),
            positive_target_symbols=("000001.SZ",),
            eligible_universe_symbols=("000001.SZ",),
            eligible_universe_refs=refs,
            eligible_universe_set_hash=canonical_json_sha256(
                {
                    "decision_trade_date": date(2026, 7, 1),
                    "eligible_symbols": ("000001.SZ",),
                    "eligible_outcome_refs": [
                        item.model_dump(mode="json") for item in refs
                    ],
                }
            ),
            eligible_universe_source_hash="9" * 64,
            label_policy_bundle_hash="9" * 64,
            label_policy_hash="3" * 64,
            universe_policy_hash="1" * 64,
            universe_layer="pit-universe-v1",
            source_revision_set_hash="2" * 64,
            winner_definition=_winner(),
        )
        return HistoricalRangeRecallDenominatorSetV1(
            availability="AVAILABLE",
            denominators=(denominator,),
        )

    service = HistoricalRangeSummaryService()
    first = service.calculate(
        range_run_id="run-1",
        rows=rows,
        policy=_policy(),
        producer_code_hash="f" * 64,
        recall_denominator_set=evidence("e"),
    )
    second = service.calculate(
        range_run_id="run-1",
        rows=rows,
        policy=_policy(),
        producer_code_hash="f" * 64,
        recall_denominator_set=evidence("f"),
    )

    assert first.covered_outcome_set_hash == second.covered_outcome_set_hash
    assert first.summary_policy_hash == second.summary_policy_hash
    assert first.recall_denominator_set_hash != second.recall_denominator_set_hash
    assert first.summary_input_hash != second.summary_input_hash


def test_postgres_recall_provider_requires_full_pit_outcome_coverage() -> None:
    item = _work_item()
    universe_ref = _universe_ref(
        "e",
        symbol="000001.SZ",
        value="0.20",
        projection="RETURN_GROSS",
        horizon=1,
    )
    provider = PostgresHistoricalRangeRecallDenominatorProvider(
        conn_factory=lambda: None,
        dataset_store=SimpleNamespace(),
        calculation_evidence_reader=SimpleNamespace(),
        winner_definitions=(
            Phase1WinnerDefinitionV1(
                winner_definition_id="winner-return-gross-1d",
                projection="RETURN_GROSS",
                comparison_operator="GT",
                threshold=Decimal("0"),
                ranking_direction="DESC",
                horizon_trade_days=1,
                label_policy_hash=universe_ref.label_policy_hash,
                denominator_universe_layer=universe_ref.universe_layer,
            ),
        ),
    )
    provider._load_snapshot_evidence = lambda **_kwargs: (universe_ref,)
    outcome_ref = _ref("f")
    row = HistoricalRangeSummaryInputRowV1(
        outcome_ref=outcome_ref,
        outcome_logical_id="logical-1",
        canonical_signal_id="signal-1",
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id="candidate-1",
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        projection="RETURN_GROSS",
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
        decision_trade_date=date(2026, 7, 1),
        symbol="000001.SZ",
        strategy_rank=1,
        maturity_status=HistoricalRangeOutcomeStatus.COMPLETE,
        value=Decimal("0.01"),
    )
    policy = HistoricalRangeSummaryPolicyV1(
        subject_types=(HistoricalRangeOutcomeSubjectType.CANDIDATE,),
        projection_groups=(HistoricalRangeOutcomeProjection.EXECUTABLE,),
        evaluation_window_types=(
            HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        ),
        horizons=(1,),
        outcome_policy_bundle_hash=item.policy_bundle_hash,
        recall_k_values=(1,),
    )

    denominators = provider.load(
        range_run_id="run-1",
        rows=(row,),
        policy=policy,
    )
    assert denominators.availability == "AVAILABLE"
    assert len(denominators.denominators) == 1
    assert denominators.denominators[0].strategy_symbols == ("000001.SZ",)
    assert denominators.denominators[0].eligible_universe_refs == (universe_ref,)

    provider._load_snapshot_evidence = lambda **_kwargs: ()
    unavailable = provider.load(range_run_id="run-1", rows=(row,), policy=policy)
    assert unavailable.availability == "UNAVAILABLE"
    assert unavailable.denominators == ()
    assert unavailable.reason_codes == ("PIT_ELIGIBLE_UNIVERSE_INCOMPLETE",)


def test_summary_composition_wires_exact_phase1_snapshot_recall_provider(
    tmp_path: Path,
) -> None:
    store_identity = {
        "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
    }
    coordinator = build_historical_range_summary_coordinator(
        conn_factory=lambda: None,
        artifact_root=tmp_path / "artifacts",
        policy=_policy(),
        label_as_of_trade_date=date(2026, 7, 21),
        producer_code_hash="f" * 64,
        repository_root=Path.cwd().resolve(),
        calculation_evidence_root=(tmp_path / "evidence").resolve(),
        calculation_evidence_store_identity=store_identity,
        dataset_store_root=(tmp_path / "dataset").resolve(),
        dataset_store_identity=store_identity,
        winner_definitions=(_winner(),),
    )

    assert isinstance(
        coordinator._outcome_set_loader._denominator_provider,
        PostgresHistoricalRangeRecallDenominatorProvider,
    )


class _SummaryRepository:
    def __init__(self) -> None:
        self.summaries = []

    def append_summary(self, fact):
        self.summaries.append(fact)
        return False

    def load_latest_summary(self, *, range_run_id):
        matching = [item for item in self.summaries if item.range_run_id == range_run_id]
        return matching[-1] if matching else None

    def find_summary_by_input(self, *, range_run_id, summary_input_hash):
        return next(
            (
                item
                for item in self.summaries
                if item.range_run_id == range_run_id
                and item.summary_input_hash == summary_input_hash
            ),
            None,
        )


class _OutcomeSetLoader:
    def __init__(self, rows):
        self.rows = rows

    def load(self, *, range_run_id, label_as_of_trade_date, policy):
        return HistoricalRangeSummaryOutcomeSetV1(
            range_run_id=range_run_id,
            label_as_of_trade_date=label_as_of_trade_date,
            resolved_request_hash="9" * 64,
            rows=self.rows,
        )


def test_summary_coordinator_appends_once_and_exact_retry_reads_existing(
    tmp_path: Path,
) -> None:
    rows = (
        _row(
            "a",
            signal="s1",
            symbol="000001.SZ",
            day=date(2026, 7, 1),
            list_id="l1",
            value="0.10",
        ),
    )
    repository = _SummaryRepository()
    coordinator = HistoricalRangeSummaryCoordinatorService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path),
        outcome_set_loader=_OutcomeSetLoader(rows),
        policy=_policy(),
        label_as_of_trade_date=date(2026, 7, 21),
        producer_code_hash="f" * 64,
    )

    first = coordinator.refresh(range_run_id="run-1")
    second = coordinator.refresh(range_run_id="run-1")

    assert first == second
    assert len(repository.summaries) == 1


def test_summary_coordinator_version_two_exact_retry_preserves_predecessor(
    tmp_path: Path,
) -> None:
    first_rows = (
        _row(
            "a",
            signal="s1",
            symbol="000001.SZ",
            day=date(2026, 7, 1),
            list_id="l1",
            value="0.10",
        ),
    )
    second_rows = (
        _row(
            "b",
            signal="s1",
            symbol="000001.SZ",
            day=date(2026, 7, 1),
            list_id="l1",
            value="0.20",
        ),
    )
    repository = _SummaryRepository()
    loader = _OutcomeSetLoader(first_rows)
    coordinator = HistoricalRangeSummaryCoordinatorService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path),
        outcome_set_loader=loader,
        policy=_policy(),
        label_as_of_trade_date=date(2026, 7, 21),
        producer_code_hash="f" * 64,
    )

    first = coordinator.refresh(range_run_id="run-1")
    loader.rows = second_rows
    second = coordinator.refresh(range_run_id="run-1")
    retry = coordinator.refresh(range_run_id="run-1")

    assert first != second
    assert retry == second
    assert len(repository.summaries) == 2
    assert repository.summaries[1].predecessor_summary_id == repository.summaries[0].summary_id
    assert (
        HistoricalRangeSummaryArtifactV2.model_validate(
            repository.summaries[1].summary_json
        ).predecessor_summary_ref
        == first
    )


class _OutcomeRepository:
    def __init__(self, fact):
        self.fact = fact

    def list_outcomes_for_summary(self, *, range_run_id, label_as_of_trade_date):
        return (self.fact,)

    def load_run_resolved_request_hash(self, *, range_run_id):
        return "9" * 64


class _ContextProvider:
    def load(self, *, fact, artifact, calculation):
        return HistoricalRangeSummaryRowContextV1(
            canonical_signal_id="signal-1",
            decision_trade_date=date(2026, 7, 3),
            list_version_id="list-1",
            symbol="000001.SZ",
            industry_at_t="I1",
            market_regime_at_t="R1",
        )


def test_outcome_set_loader_selects_typed_calculation_rows(tmp_path: Path) -> None:
    item = _work_item()
    result = _Evaluator().evaluate(item)
    builder = HistoricalRangeOutcomeProjectionBuilder()
    artifact = builder.build_artifact(
        work_item=item,
        result=result,
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
    fact = builder.build_fact(
        work_item=item,
        result=result,
        artifact=artifact,
        outcome_artifact_ref=stored.ref,
        outcome_version=1,
    )
    policy = HistoricalRangeSummaryPolicyV1(
        subject_types=(HistoricalRangeOutcomeSubjectType.CANDIDATE,),
        projection_groups=(HistoricalRangeOutcomeProjection.EXECUTABLE,),
        evaluation_window_types=(HistoricalRangeEvaluationWindowType.FIXED_HORIZON,),
        horizons=(1,),
        outcome_policy_bundle_hash=item.policy_bundle_hash,
        recall_k_values=(2,),
    )

    frozen = PostgresHistoricalRangeSummaryOutcomeSetLoader(
        repository=_OutcomeRepository(fact),
        artifact_store=store,
        context_provider=_ContextProvider(),
    ).load(
        range_run_id="run-1",
        label_as_of_trade_date=date(2026, 7, 10),
        policy=policy,
    )

    assert len(frozen.rows) == 1
    assert frozen.rows[0].projection == "RETURN_GROSS"
    assert frozen.rows[0].industry_at_t == "I1"
    assert frozen.rows[0].market_regime_at_t == "R1"


class _ContextCursor:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.rows = rows
        self.query = ""
        self.params: tuple[object, ...] = ()

    def __enter__(self) -> "_ContextCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.query = query
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return list(self.rows)


class _ContextConnection:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.cursor_instance = _ContextCursor(rows)

    def __enter__(self) -> "_ContextConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def set_session(self, **_kwargs: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _ContextCursor:
        return self.cursor_instance

    def rollback(self) -> None:
        return None


def _industry_membership(*, code: str, in_date: date) -> dict[str, object]:
    return {
        "l1_code": "L1",
        "l1_name": "L1_NAME",
        "l2_code": "L2",
        "l2_name": "L2_NAME",
        "l3_code": code,
        "l3_name": code,
        "in_date": in_date,
        "out_date": None,
    }


def test_summary_industry_uses_latest_effective_membership() -> None:
    provider = object.__new__(PostgresHistoricalRangeSummaryContextProvider)
    provider._conn_factory = lambda: _ContextConnection(
        (
            _industry_membership(code="NEW", in_date=date(2026, 7, 1)),
            _industry_membership(code="OLD", in_date=date(2000, 1, 1)),
        )
    )

    assert provider._load_summary_industry_at_t(
        symbol="000001.SZ",
        decision_trade_date=date(2026, 7, 1),
    ) == "NEW"


def test_summary_industry_rejects_conflicts_on_same_latest_effective_date() -> None:
    provider = object.__new__(PostgresHistoricalRangeSummaryContextProvider)
    provider._conn_factory = lambda: _ContextConnection(
        (
            _industry_membership(code="A", in_date=date(2026, 7, 1)),
            _industry_membership(code="B", in_date=date(2026, 7, 1)),
        )
    )

    with pytest.raises(HistoricalRangeOutcomeSourceError) as exc_info:
        provider._load_summary_industry_at_t(
            symbol="000001.SZ",
            decision_trade_date=date(2026, 7, 1),
        )

    assert (
        exc_info.value.reason_code
        == "ADVISORY_HR_OUTCOME_INDUSTRY_MEMBERSHIP_CONFLICT"
    )


def test_summary_episode_context_binds_to_exact_subject_day_receipt() -> None:
    digest = "d" * 64
    subject_ref = HistoricalRangeArtifactRefV1(
        artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
        relative_path=f"day-receipts/{digest}.json",
        producer_contract_version="r4_v1",
        payload_schema_version="day_receipt_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )
    connection = _ContextConnection(
        (
            {
                "symbol": "000001.SZ",
                "decision_trade_date": date(2026, 7, 1),
                "list_version_id": "list-2",
                "episode_closed": True,
            },
        )
    )
    provider = object.__new__(PostgresHistoricalRangeSummaryContextProvider)
    provider._conn_factory = lambda: connection

    metadata = provider._subject_metadata(
        SimpleNamespace(
            subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
            subject_id="episode-1",
        ),
        subject_ref=subject_ref,
    )

    assert metadata["list_version_id"] == "list-2"
    assert "day.day_receipt_ref = %s" in connection.cursor_instance.query
    assert "LIMIT 1" not in connection.cursor_instance.query.upper()
    assert connection.cursor_instance.params[-1].adapted == subject_ref.model_dump(
        mode="json"
    )


def test_summary_signal_identity_distinguishes_candidate_and_range_native_owner() -> None:
    candidate_calculation = _Evaluator().evaluate(_work_item()).calculation_results[0]
    candidate_fact = SimpleNamespace(
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id="candidate-1",
    )
    episode_fact = SimpleNamespace(
        subject_type=HistoricalRangeOutcomeSubjectType.EPISODE,
        subject_id="episode-1",
    )

    assert PostgresHistoricalRangeSummaryContextProvider._canonical_signal_id(
        fact=candidate_fact,
        calculation=candidate_calculation,
    ) == candidate_calculation.owner.canonical_signal_id
    assert PostgresHistoricalRangeSummaryContextProvider._canonical_signal_id(
        fact=episode_fact,
        calculation=SimpleNamespace(owner=SimpleNamespace()),
    ) == "range-subject:EPISODE:episode-1"
