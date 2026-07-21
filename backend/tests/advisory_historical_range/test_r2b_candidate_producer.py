from __future__ import annotations

from datetime import UTC, date, datetime

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.candidate_producer import HistoricalRangeCandidateProducer
from backend.services.advisory_historical_range.candidate_projector import _receipt_payload
from backend.services.advisory_historical_range.models import (
    HistoricalRangeResearchBatchRequestV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeResolvedRequestArtifactPayloadV1,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRevisionCatalogV1,
    HistoricalRangeSourceRevisionMemberV1,
    ResolvedHistoricalRangeRequestV1,
)
from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    RiskAdjustmentResult,
    StageReceiptStatus,
    TradabilityResult,
    build_stage_receipt,
    canonical_evidence_json_sha256,
)
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.strategy_package.models import AlphaMode, SelectionScoreArtifactStatus
from backend.services.strategy_package.selection_artifact import SelectionScoreArtifact
from backend.services.strategy_package.selection_computation import (
    PreparedPackageComponentLineageV1,
    PreparedPackageSignalV1,
    SelectionArtifactHeaderV1,
    StrategyPackageSelectionComputation,
    StrategyPackageSelectionReadOnlyProvidersV1,
    selection_runtime_profile_sha256,
)
from backend.services.strategy_package.selection_signal_preparation import (
    PreparedRawSelectionArtifactV2,
    StrategyPackageSignalPreparationResultV1,
)
from backend.tests.advisory_historical_range.conftest import (
    date_plan,
    digest,
    frozen_program,
    research_spec,
)


TRADE_DATE = date(2026, 6, 2)


class _Risk:
    def evaluate(self, **_kwargs):  # noqa: ANN003, ANN202
        return {}

    def apply_to_candidates_with_receipt(self, *, candidates, package_id, manifest_sha256, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        return RiskAdjustmentResult(
            candidates=list(candidates),
            exclusions=[],
            receipt=build_stage_receipt(
                stage=CandidateStageName.RISK_POLICY_ADJUSTED,
                status=StageReceiptStatus.COMPLETE,
                input_count=len(candidates),
                candidates=list(candidates),
                semantic_payload={"enabled": False, "package_id": package_id, "manifest_sha256": manifest_sha256},
            ),
            risk_metadata={"enabled": False},
        )


class _RejectAllRisk(_Risk):
    def apply_to_candidates_with_receipt(self, *, candidates, package_id, manifest_sha256, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        exclusions = [
            SelectionExclusion(
                symbol=item.symbol,
                score=item.score,
                rank=item.rank,
                reason="historical_test_risk_exclusion",
                source="runtime_profile.risk_policy",
            )
            for item in candidates
        ]
        return RiskAdjustmentResult(
            candidates=[],
            exclusions=exclusions,
            receipt=build_stage_receipt(
                stage=CandidateStageName.RISK_POLICY_ADJUSTED,
                status=StageReceiptStatus.COMPLETE,
                input_count=len(candidates),
                candidates=[],
                exclusions=exclusions,
                semantic_payload={"package_id": package_id, "manifest_sha256": manifest_sha256},
            ),
            risk_metadata={"enabled": True, "provider": "test"},
        )


class _Tradability:
    def select_top_k_with_receipt(self, *, candidates, top_k, package_id, manifest_sha256, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        selected = list(candidates[:top_k])
        exclusions = [
            SelectionExclusion(
                symbol=item.symbol,
                score=item.score,
                rank=item.rank,
                reason="outside_selection_top_k",
                source="runtime_profile.selection.top_k",
            )
            for item in candidates[top_k:]
        ]
        return TradabilityResult(
            candidates=selected,
            exclusions=exclusions,
            receipt=build_stage_receipt(
                stage=CandidateStageName.SELECTION_EFFECTIVE,
                status=StageReceiptStatus.COMPLETE,
                input_count=len(candidates),
                candidates=selected,
                exclusions=exclusions,
                semantic_payload={"top_k": top_k, "package_id": package_id, "manifest_sha256": manifest_sha256},
            ),
            universe_metadata={"top_k": top_k},
        )

    def filter_candidates_with_receipt(self, **_kwargs):  # noqa: ANN003, ANN202
        raise AssertionError("test runtime disables suspend and industry filtering")


class _SignalPreparation:
    def __init__(self, result: StrategyPackageSignalPreparationResultV1) -> None:
        self.result = result
        self.calls = []

    def prepare_historical(self, **kwargs):  # noqa: ANN003, ANN202
        self.calls.append(kwargs)
        return self.result


class _Verifier:
    def __init__(self) -> None:
        self.calls = 0

    def verify_program_day(self, *, catalog, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        self.calls += 1
        return catalog.source_revision_refs()


def _fixture(tmp_path):  # noqa: ANN001, ANN202
    spec = research_spec(package_id="pkg-r2b")
    runtime_config = {
        "runtime_profile": {
            "selection": {"top_k": 2},
            "hmm": {"enabled": False},
            "tradability": {"exclude_suspended": False},
            "risk_policy": {"enabled": False},
        }
    }
    base_program = frozen_program(spec, alpha_mode=HistoricalRangeAlphaMode.SINGLE_ALPHA)
    program_payload = base_program.model_dump(mode="python", exclude={"frozen_program_hash"})
    program_payload.update({"runtime_config": runtime_config, "runtime_config_hash": digest(runtime_config)})
    program = type(base_program).model_validate(program_payload)
    plan = date_plan(
        trade_dates=(TRADE_DATE,),
        research_program_ids=(spec.research_program_id,),
        component_ids=("alpha",),
    )
    request = HistoricalRangeResearchBatchRequestV1(
        request_id="request-r2b-candidate",
        client_idempotency_key="candidate-producer-key",
        program_specs=(spec,),
        start_trade_date=TRADE_DATE,
        end_trade_date=TRADE_DATE,
        requested_at=datetime(2026, 7, 20, tzinfo=UTC),
        requested_by="test",
    )
    parameters = {"content_hash": digest("code-release"), "row_count": 1}
    member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="code-release",
        source_role="code_release",
        dataset_id="aistock.source_closure",
        partition_ref="code-release:test",
        query_template_id="frozen_artifact_identity",
        query_template_version="v1",
        query_template_hash=digest("frozen-query"),
        bound_parameters=parameters,
        parameter_hash=digest(parameters),
        row_count=1,
        content_hash=digest("code-release"),
        admissibility=HistoricalRangeRevisionAdmissibility.FROZEN_ARTIFACT,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    universe_parameters = {
        "trade_date": TRADE_DATE.isoformat(),
        "universe_key": "shsz_st_pit_active_v1",
        "ensure": False,
    }
    universe_member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="pit-universe",
        source_role="pit_universe",
        dataset_id="market.stock_universe_pit",
        partition_ref=f"shsz_st_pit_active_v1:{TRADE_DATE.isoformat()}",
        decision_trade_date=TRADE_DATE,
        query_template_id="historical_pit_universe_existing_readonly",
        query_template_version="v1",
        query_template_hash=digest("universe-query"),
        bound_parameters=universe_parameters,
        parameter_hash=digest(universe_parameters),
        row_count=5000,
        content_hash="4" * 64,
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    window_start = "2026-03-20"
    market_parameters = {
        "start_date": window_start,
        "trade_date": TRADE_DATE.isoformat(),
        "universe_key": "shsz_st_pit_active_v1",
        "required_window": 61,
        "buffer_trading_days": 5,
        "window_resolution": "trading_calendar",
    }
    market_member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="market-history-alpha",
        source_role="market_history",
        dataset_id="market.kline_daily_raw",
        partition_ref=f"market-history:{program.package_id}:alpha:{window_start}:{TRADE_DATE.isoformat()}",
        package_id=program.package_id,
        component_id="alpha",
        decision_trade_date=TRADE_DATE,
        query_template_id="historical_market_history_window",
        query_template_version="v1",
        query_template_hash=digest("market-query"),
        bound_parameters=market_parameters,
        parameter_hash=digest(market_parameters),
        row_count=1000,
        content_hash=digest("market-content"),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    fundamental_member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="fundamental-alpha",
        source_role="fundamental_moneyflow",
        dataset_id="timescaledb.fundamental_moneyflow",
        partition_ref=f"fundamental:{program.package_id}:alpha:{window_start}:{TRADE_DATE.isoformat()}",
        package_id=program.package_id,
        component_id="alpha",
        decision_trade_date=TRADE_DATE,
        query_template_id="historical_fundamental_moneyflow_window",
        query_template_version="v1",
        query_template_hash=digest("fundamental-query"),
        bound_parameters=market_parameters,
        parameter_hash=digest(market_parameters),
        row_count=1000,
        content_hash=digest("fundamental-content"),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    calendar_parameters = {
        "range_start": window_start,
        "trade_date": TRADE_DATE.isoformat(),
    }
    calendar_member = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="calendar-requirement",
        source_role="trading_calendar",
        dataset_id="market.trading_calendar",
        partition_ref=f"trading-calendar:{window_start}:{TRADE_DATE.isoformat()}",
        decision_trade_date=TRADE_DATE,
        query_template_id="historical_trading_calendar_window",
        query_template_version="v1",
        query_template_hash=digest("calendar-query"),
        bound_parameters=calendar_parameters,
        parameter_hash=digest(calendar_parameters),
        row_count=1,
        content_hash=digest("calendar-content"),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    catalog = HistoricalRangeSourceRevisionCatalogV1(
        requirement_plan_hash=digest("plan"),
        catalog_generation=1,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        members=(member, universe_member, market_member, fundamental_member, calendar_member),
    )
    resolved = ResolvedHistoricalRangeRequestV1(
        batch_id="ahrb_candidate_test",
        request=request,
        frozen_programs=(program,),
        date_plan=plan,
        source_revision_catalog_hash=str(catalog.catalog_hash),
        selection_semantics_version=program.selection_semantics_version,
        selection_semantics_hash=program.selection_semantics_hash,
        list_semantics_version=program.list_semantics_version,
        list_semantics_hash=program.list_semantics_hash,
    )
    request_payload = HistoricalRangeResolvedRequestArtifactPayloadV1(
        resolved_request=resolved,
        source_revision_catalog=catalog,
    )
    scores = [
        {"symbol": f"00000{index}.SZ", "score": score, "rank": index, "component_scores": {}}
        for index, score in ((1, 0.3), (2, 0.2), (3, 0.1))
    ]
    calendar_identity_hash = digest(
        {
            "dataset_id": "market.trading_calendar",
            "effective_trade_date": TRADE_DATE.isoformat(),
            "calendar_version": "market.trading_calendar.v1",
            "calendar_source": "market.trading_calendar",
        }
    )
    calendar_hash = digest(
        {
            "calendar_identity_hash": calendar_identity_hash,
            "window_start_date": window_start,
            "required_window": 61,
            "window_resolution": "trading_calendar",
        }
    )
    input_context = {
        "calendar_version": "market.trading_calendar.v1",
        "calendar_source": "market.trading_calendar",
        "calendar_identity_hash": calendar_identity_hash,
        "calendar_hash": calendar_hash,
        "universe_input_hash": "4" * 64,
        "effective_trade_date": TRADE_DATE.isoformat(),
        "window_start_date": window_start,
        "required_window": 61,
        "window_resolution": "trading_calendar",
    }
    artifact = SelectionScoreArtifact(
        artifact_id="ssa_ephemeral",
        package_id=program.package_id,
        manifest_sha256=program.manifest_sha256,
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        runtime_config_hash="5" * 64,
        scores_json=scores,
        score_count=3,
        universe_count=5000,
        top_score_symbol="000001.SZ",
        status=SelectionScoreArtifactStatus.SUCCEEDED,
        metadata={
            "authority_scope": "authoritative_selection",
            "candidate_outcome": "CANDIDATES_PRESENT",
            "provider_semantics_id": "strategy_package_live_inference_v2",
            "provider_semantics_hash": "6" * 64,
            "artifact_input_context": input_context,
            "source_read_receipts": [
                {
                    "source_role": "pit_universe",
                    "dataset_id": "market.stock_universe_pit",
                    "content_hash": "4" * 64,
                    "row_count": 5000,
                    "partition_ref": TRADE_DATE.isoformat(),
                    "admissibility": "RETROSPECTIVE_DB_CONTENT_HASH",
                },
                {
                    "source_role": "market_history",
                    "dataset_id": "market.kline_daily_raw",
                    "content_hash": digest("raw-market"),
                    "row_count": 1000,
                    "partition_ref": f"{window_start}:{TRADE_DATE.isoformat()}",
                    "admissibility": "RETROSPECTIVE_DB_CONTENT_HASH",
                },
                {
                    "source_role": "fundamental_moneyflow",
                    "dataset_id": "timescaledb.fundamental_moneyflow",
                    "content_hash": digest("raw-fundamental"),
                    "row_count": 1000,
                    "partition_ref": f"{window_start}:{TRADE_DATE.isoformat()}",
                    "admissibility": "RETROSPECTIVE_DB_CONTENT_HASH",
                },
                {
                    "source_role": "trading_calendar",
                    "dataset_id": "market.trading_calendar",
                    "content_hash": calendar_hash,
                    "row_count": 2,
                    "partition_ref": f"{window_start}:{TRADE_DATE.isoformat()}",
                    "admissibility": "RETROSPECTIVE_DB_CONTENT_HASH",
                },
            ],
            "topk": 3,
        },
        artifact_contract_version="selection_score_artifact_v2",
        artifact_input_context_hash=canonical_evidence_json_sha256(input_context),
        source_revision_set_hash="8" * 64,
        asset_closure_hash="9" * 64,
    )
    score_hash = digest(scores)
    artifact = artifact.model_copy(update={"artifact_sha256": score_hash})
    payload_hash = canonical_evidence_json_sha256(artifact.canonical_v2_header(score_hash=score_hash))
    artifact = artifact.model_copy(update={"artifact_payload_sha256": payload_hash})
    candidates = tuple(SelectionCandidate.model_validate(item) for item in scores)
    hmm_receipt = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.NOT_APPLICABLE,
        input_count=len(candidates),
        candidates=[],
        semantic_payload={"enabled": False, "generation_mode": "NOT_APPLICABLE"},
    )
    profile = parse_selection_runtime_profile(runtime_config)
    runtime_profile_hash = selection_runtime_profile_sha256(profile)
    raw_header = {
        "runtime_profile_hash": runtime_profile_hash,
        "artifact_input_context_hash": artifact.artifact_input_context_hash,
        "universe_identity_hash": input_context["universe_input_hash"],
    }
    raw = PreparedRawSelectionArtifactV2(
        artifact=artifact,
        semantic_header=raw_header,
        raw_inference_receipt={"status": "COMPLETE", "score_count": 3, "universe_count": 5000},
        source_read_receipts=tuple(artifact.metadata["source_read_receipts"]),
    )
    prepared = PreparedPackageSignalV1(
        package_id=program.package_id,
        package_version=program.package_version,
        manifest_sha256=program.manifest_sha256,
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        component_lineage=(
            PreparedPackageComponentLineageV1(
                component_id="alpha",
                component_weight=1.0,
                factor_ids=("factor_alpha_a", "factor_alpha_b"),
                score_normalization="none",
            ),
        ),
        alpha_raw_candidates=candidates,
        hmm_adjusted_candidates=candidates,
        hmm_receipt=hmm_receipt,
        hmm_metadata={"enabled": False, "status": "NOT_APPLICABLE", "generation_mode": "NOT_APPLICABLE"},
        artifact_header=SelectionArtifactHeaderV1(
            artifact_id=raw.signal_id,
            artifact_sha256=score_hash,
            package_id=program.package_id,
            manifest_sha256=program.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config_hash=artifact.runtime_config_hash,
            artifact_payload_sha256=payload_hash,
            artifact_contract_version="selection_score_artifact_v2",
            artifact_input_context_hash=artifact.artifact_input_context_hash,
            source_revision_set_hash=artifact.source_revision_set_hash,
            asset_closure_hash=artifact.asset_closure_hash,
            universe_identity_hash=input_context["universe_input_hash"],
        ),
        input_context_hash=artifact.artifact_input_context_hash,
        source_revision_set_hash=artifact.source_revision_set_hash,
        universe_identity_hash=input_context["universe_input_hash"],
    )
    signal = _SignalPreparation(StrategyPackageSignalPreparationResultV1(raw=raw, prepared_signal=prepared))
    verifier = _Verifier()
    producer = HistoricalRangeCandidateProducer(
        signal_preparation=signal,
        computation=StrategyPackageSelectionComputation(),
        providers=StrategyPackageSelectionReadOnlyProvidersV1(risk_policy=_Risk(), tradability=_Tradability()),
        source_verifier=verifier,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "candidate-cas"),
    )
    return producer, request_payload, verifier, program


def test_candidate_producer_projects_all_stages_and_exact_cas_rerun(tmp_path) -> None:  # noqa: ANN001
    producer, request_payload, verifier, program = _fixture(tmp_path)

    first = producer.produce(
        request_payload=request_payload,
        research_program_id=program.research_program_id,
        decision_trade_date=TRADE_DATE,
    )
    second = producer.produce(
        request_payload=request_payload,
        research_program_id=program.research_program_id,
        decision_trade_date=TRADE_DATE,
    )

    assert first.candidate_artifact_ref == second.candidate_artifact_ref
    assert first.candidate_outcome == "CANDIDATES_AVAILABLE"
    assert [item.membership_status for item in first.candidates] == ["INCLUDED", "INCLUDED", "EXCLUDED"]
    assert first.candidates[2].component_lineage_json["stage_exclusions"][0]["reason"] == "outside_selection_top_k"
    assert first.candidates[0].advisory_model_rank is None
    assert verifier.calls == 4


def test_stage_receipt_identity_excludes_operational_time_and_local_paths() -> None:
    candidate = SelectionCandidate(
        symbol="000001.SZ",
        score=1.0,
        rank=1,
        reason="hmm adjusted",
        component_scores={
            "hmm": {
                "coefficient": 1.1,
                "model_path": "C:/runtime-a/model.pkl",
                "coefficients_path": "C:/runtime-a/coefficients.json",
            }
        },
    )
    first = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[candidate],
        semantic_payload={"coefficient_sha256": "a" * 64},
    )
    second = first.model_copy(
        update={
            "candidates": [
                {
                    **first.candidates[0],
                    "component_scores": {
                        "hmm": {
                            "coefficient": 1.1,
                            "model_path": "/mnt/runtime-b/model.pkl",
                            "coefficients_path": "/mnt/runtime-b/coefficients.json",
                        }
                    },
                }
            ]
        }
    )

    assert _receipt_payload(first) == _receipt_payload(second)


def test_candidate_producer_preserves_valid_empty_stage_evidence(tmp_path) -> None:  # noqa: ANN001
    producer, request_payload, verifier, program = _fixture(tmp_path)
    empty_producer = HistoricalRangeCandidateProducer(
        signal_preparation=producer._signal_preparation,
        computation=StrategyPackageSelectionComputation(),
        providers=StrategyPackageSelectionReadOnlyProvidersV1(
            risk_policy=_RejectAllRisk(),
            tradability=_Tradability(),
        ),
        source_verifier=verifier,
        artifact_store=producer._artifact_store,
    )

    result = empty_producer.produce(
        request_payload=request_payload,
        research_program_id=program.research_program_id,
        decision_trade_date=TRADE_DATE,
    )

    assert result.candidate_outcome == "VALID_NO_CANDIDATE"
    assert result.no_candidate_reason_codes == ("historical_test_risk_exclusion",)
    assert result.candidates
    assert {item.membership_status for item in result.candidates} == {"EXCLUDED"}
    assert result.stage_trace["risk_policy_adjusted"]["excluded_count"] == len(result.candidates)
