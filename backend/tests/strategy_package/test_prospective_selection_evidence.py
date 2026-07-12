from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.selection_center.models import SelectionCandidate, SelectionExclusion
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    EvidenceCaptureMode,
    HISTORICAL_RESEARCH_DATA_SOURCE,
    ProspectiveSelectionContext,
    REASON_HISTORICAL_RESEARCH_ONLY,
    SelectionStageTrace,
    SourceReadReceipt,
    StageReceiptStatus,
    build_stage_receipt,
    canonical_evidence_json_sha256,
    require_historical_research_data_source,
)
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.selection_artifact import (
    SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
    build_selection_artifact_v2_provenance,
    selection_artifact_runtime_hash,
    selection_artifact_runtime_hash_v2,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _v2_artifact(*, score: float = 1.0) -> SelectionScoreArtifact:
    return SelectionScoreArtifact(
        artifact_id="ssa_v2_unit",
        package_id="pkg_v2_unit",
        manifest_sha256="a" * 64,
        trade_date=date(2026, 7, 10),
        data_source="DB_HISTORICAL",
        runtime_config_hash=selection_artifact_runtime_hash_v2({"selection_artifact_config": {"cutoff_date": "2026-07-09"}}),
        scores_json=[{"symbol": "000001.SZ", "score": score, "rank": 1}],
        score_count=1,
        universe_count=3,
        top_score_symbol="000001.SZ",
        metadata={
            "authority_scope": "authoritative_selection",
            "candidate_outcome": "CANDIDATES_PRESENT",
            "provider_semantics_id": "live_inference_v1",
            "provider_semantics_hash": "b" * 64,
        },
        artifact_contract_version=SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
        artifact_input_context_hash="c" * 64,
        source_revision_set_hash="d" * 64,
        asset_closure_hash="e" * 64,
    )


def test_v2_artifact_hash_is_immutable_and_retry_is_idempotent() -> None:
    repository = InMemorySelectionScoreArtifactRepository()
    first = repository.save(_v2_artifact())
    retry = repository.save(_v2_artifact())

    assert first.artifact_payload_sha256
    assert retry.artifact_id == first.artifact_id
    assert retry.artifact_payload_sha256 == first.artifact_payload_sha256


def test_v2_artifact_retry_ignores_diagnostic_metadata() -> None:
    repository = InMemorySelectionScoreArtifactRepository()
    first = repository.save(_v2_artifact())
    retry = _v2_artifact().model_copy(
        update={"metadata": {**_v2_artifact().metadata, "diagnostic_workspace_path": "C:/different-host/worktree"}}
    )

    stored = repository.save(retry)

    assert stored.artifact_id == first.artifact_id
    assert stored.artifact_payload_sha256 == first.artifact_payload_sha256


def test_v2_artifact_rejects_same_business_key_with_different_payload() -> None:
    repository = InMemorySelectionScoreArtifactRepository()
    repository.save(_v2_artifact(score=1.0))

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repository.save(_v2_artifact(score=2.0))

    assert exc_info.value.context["reason_code"] == "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT"


def test_v2_artifact_rejects_nonsemantic_count_or_hash_mismatch() -> None:
    payload = _v2_artifact().model_dump(mode="python")
    with pytest.raises(ValueError, match="score_count"):
        SelectionScoreArtifact.model_validate({**payload, "score_count": 2})

    with pytest.raises(ValueError, match="invalid SHA256"):
        SelectionScoreArtifact.model_validate({**payload, "asset_closure_hash": "invalid"})


def test_v2_additive_migration_has_no_data_mutation_and_matches_bootstrap_contract() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    migration = (repo_root / "backend" / "db" / "migrations" / "add_selection_score_artifact_v2_evidence_20260712.sql").read_text(
        encoding="utf-8"
    )
    bootstrap = (repo_root / "backend" / "db" / "init_trading_core_v2_schema.py").read_text(encoding="utf-8")

    assert "ALTER TABLE strategy_pkg.selection_score_artifact" in migration
    assert "CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_pkg_selection_artifact_v2_payload" in migration
    assert "WHERE artifact_payload_sha256 IS NOT NULL" in migration
    assert not any(token in migration.upper() for token in (" DROP ", " INSERT ", " UPDATE ", " DELETE ", " TRUNCATE "))
    for column in (
        "artifact_contract_version",
        "artifact_payload_sha256",
        "artifact_input_context_hash",
        "source_revision_set_hash",
        "asset_closure_hash",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in migration
        assert f"COMMENT ON COLUMN strategy_pkg.selection_score_artifact.{column}" in migration
        assert column in bootstrap


def test_v2_provenance_rejects_missing_actual_universe_count() -> None:
    result = SimpleNamespace(
        scores=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1}],
        universe_count=None,
        source_read_receipts=[],
        input_context={},
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        build_selection_artifact_v2_provenance(
            result=result,
            requested_trade_date=date(2026, 7, 10),
            cutoff_date=None,
            include_reference_price=False,
            asset_closure=[],
            asset_closure_status="COMPLETE",
            asset_reason_codes=[],
            provider_semantics={"provider_semantics_id": "unit_provider_v2"},
        )

    assert exc_info.value.context["reason_code"] == "ADVISORY_PHASE0A2C_UNIVERSE_RECEIPT_INCOMPLETE"


def test_v2_source_revision_hash_ignores_retry_observation_timestamp() -> None:
    def build_provenance(observed_at: datetime):
        receipts = [
            SourceReadReceipt(
                source_role=role,
                dataset_id=f"unit.{role}",
                row_count=3,
                content_hash=f"{index + 1:x}" * 64,
                first_observed_at=observed_at,
            ).model_dump(mode="json")
            for index, role in enumerate(
                ("pit_universe", "market_history", "fundamental_moneyflow", "trading_calendar")
            )
        ]
        result = SimpleNamespace(
            scores=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1}],
            universe_count=3,
            source_read_receipts=receipts,
            input_context={
                "effective_trade_date": "2026-07-10",
                "score_trade_date": "2026-07-10",
                "pit_mode": "stock_universe_pit_v1",
                "calendar_version": "market.trading_calendar.v1",
                "calendar_hash": "a" * 64,
                "calendar_source": "market.trading_calendar",
                "universe_input_hash": "b" * 64,
            },
        )
        return build_selection_artifact_v2_provenance(
            result=result,
            requested_trade_date=date(2026, 7, 10),
            cutoff_date=None,
            include_reference_price=False,
            asset_closure=[{"asset_role": "unit", "asset_id": "unit", "sha256": "c" * 64}],
            asset_closure_status="COMPLETE",
            asset_reason_codes=[],
            provider_semantics={"provider_semantics_id": "unit_live_inference"},
        )

    first = build_provenance(datetime(2026, 7, 10, 15, 0, tzinfo=UTC))
    retry = build_provenance(datetime(2026, 7, 10, 15, 1, tzinfo=UTC))

    assert first.source_read_receipts != retry.source_read_receipts
    assert first.source_revision_set_hash == retry.source_revision_set_hash


def test_force_regenerate_requires_explicit_diagnostic_identity() -> None:
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        StrategyPackageSelectionArtifactService._validate_v2_generation_mode(
            {"selection_artifact_config": {"force_regenerate": True}}
        )

    assert exc_info.value.context["reason_code"] == "ADVISORY_PHASE0A2C_ARTIFACT_IDEMPOTENCY_CONFLICT"


def test_v2_runtime_hash_is_distinct_from_legacy_and_ignores_orchestration_flags() -> None:
    base = {"selection_artifact_config": {"cutoff_date": "2026-07-09", "auto_generate": True, "force_regenerate": False}}
    switched = {"selection_artifact_config": {"cutoff_date": "2026-07-09", "auto_generate": False, "force_regenerate": True}}

    assert selection_artifact_runtime_hash(base) == selection_artifact_runtime_hash(switched)
    assert selection_artifact_runtime_hash_v2(base) == selection_artifact_runtime_hash_v2(switched)
    assert selection_artifact_runtime_hash_v2(base) != selection_artifact_runtime_hash(base)


def test_prospective_context_binds_run_id_at_selection_center_and_source_receipt_requires_time() -> None:
    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        execution_origin="ADVISORY_RUN",
    )
    assert context.selection_run_id is None
    with pytest.raises(ValueError, match="available_at or first_observed_at"):
        SourceReadReceipt(source_role="market", dataset_id="daily", row_count=1)

    receipt = SourceReadReceipt(
        source_role="market",
        dataset_id="daily",
        row_count=1,
        first_observed_at=datetime(2026, 7, 10, 15, 0, tzinfo=UTC),
    )
    assert receipt.first_observed_at is not None


def test_prospective_capture_is_limited_to_historical_advisory_research() -> None:
    with pytest.raises(ValueError, match="historical ADVISORY_RUN research"):
        ProspectiveSelectionContext(
            capture_mode=EvidenceCaptureMode.PROSPECTIVE,
            execution_origin="PAPER",
        )

    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        execution_origin="ADVISORY_RUN",
    )
    require_historical_research_data_source(
        context=context,
        data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
    )
    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        require_historical_research_data_source(
            context=context,
            data_source="MINIQMT_REALTIME",
        )

    assert excinfo.value.context["reason_code"] == REASON_HISTORICAL_RESEARCH_ONLY


def _prospective_capture_fixture() -> tuple[
    ProspectiveSelectionContext,
    object,
    SelectionScoreArtifact,
    SelectionStageTrace,
    dict,
    list[SelectionCandidate],
]:
    manifest = freeze_manifest(make_manifest())
    decision_date = date(2026, 7, 10)
    target_date = date(2026, 7, 13)
    shanghai = timezone(timedelta(hours=8))
    observed_at = datetime(2026, 7, 10, 14, 30, tzinfo=shanghai)
    candidate = SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1)
    source_receipts = [
        {
            "source_role": "pit_universe",
            "dataset_id": "market.stock_universe_pit",
            "row_count": 1,
            "content_hash": "1" * 64,
            "first_observed_at": observed_at,
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        },
        {
            "source_role": "market_history",
            "dataset_id": "market.kline_daily_raw",
            "row_count": 10,
            "content_hash": "2" * 64,
            "first_observed_at": observed_at,
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        },
    ]
    source_hash = canonical_evidence_json_sha256(
        [SourceReadReceipt.model_validate(item).model_dump(mode="json") for item in source_receipts]
    )
    universe_hash = "3" * 64
    asset_closure = [
        {
            "asset_role": "strategy_package_manifest",
            "asset_id": manifest.package_id,
            "asset_ref": None,
            "sha256": manifest.manifest_sha256,
            "first_observed_at": observed_at.isoformat(),
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        }
    ]
    artifact = InMemorySelectionScoreArtifactRepository().save(
        SelectionScoreArtifact(
            artifact_id="ssa_prospective",
            package_id=manifest.package_id,
            manifest_sha256=str(manifest.manifest_sha256),
            trade_date=decision_date,
            data_source="DB_HISTORICAL",
            runtime_config_hash="4" * 64,
            scores_json=[candidate.model_dump(mode="json")],
            score_count=1,
            universe_count=1,
            top_score_symbol=candidate.symbol,
            metadata={
                "source_type": "live_qe_model_inference_v1",
                "authority_scope": "authoritative_selection",
                "candidate_outcome": "CANDIDATES_PRESENT",
                "provider_semantics_id": "unit_prospective_provider_v2",
                "provider_semantics_hash": "5" * 64,
                "provider_semantics": {"provider_semantics_id": "unit_prospective_provider_v2"},
                "artifact_input_context": {
                    "requested_trade_date": decision_date.isoformat(),
                    "effective_trade_date": decision_date.isoformat(),
                    "cutoff_date": decision_date.isoformat(),
                    "score_trade_date": decision_date.isoformat(),
                    "reference_price_trade_date": decision_date.isoformat(),
                    "pit_mode": "stock_universe_pit_v1",
                    "calendar_version": "market.trading_calendar.v1",
                    "calendar_hash": "6" * 64,
                    "calendar_source": "market.trading_calendar",
                    "universe_input_hash": universe_hash,
                },
                "source_read_receipts": source_receipts,
                "asset_closure": asset_closure,
                "asset_closure_status": "COMPLETE",
                "capture_prerequisite_reason_codes": [],
            },
            artifact_contract_version=SELECTION_SCORE_ARTIFACT_CONTRACT_V2,
            artifact_input_context_hash="7" * 64,
            source_revision_set_hash=source_hash,
            asset_closure_hash=canonical_evidence_json_sha256(
                [{key: value for key, value in asset_closure[0].items() if key != "first_observed_at"}]
            ),
        )
    )
    alpha = build_stage_receipt(
        stage=CandidateStageName.ALPHA_RAW,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[candidate],
        semantic_payload={"artifact_id": artifact.artifact_id},
    )
    hmm = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.NOT_APPLICABLE,
        input_count=1,
        candidates=[],
        semantic_payload={"enabled": False, "generation_mode": "NOT_APPLICABLE"},
    )
    risk = build_stage_receipt(
        stage=CandidateStageName.RISK_POLICY_ADJUSTED,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[candidate],
        semantic_payload={"enabled": False},
    )
    effective = build_stage_receipt(
        stage=CandidateStageName.SELECTION_EFFECTIVE,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[candidate],
        semantic_payload={"enabled": False, "candidate_pool_count": 1},
    )
    trace = SelectionStageTrace(
        alpha_raw=alpha,
        hmm_adjusted=hmm,
        risk_policy_adjusted=risk,
        selection_effective=effective,
        hmm_metadata={"enabled": False, "status": "NOT_APPLICABLE", "generation_mode": "NOT_APPLICABLE"},
        risk_metadata={"enabled": False, "status": "COMPLETE"},
        universe_metadata={"enabled": False, "status": "COMPLETE"},
    )
    runtime_profile = {
        "hmm": {"enabled": False},
        "risk_policy": {"enabled": False},
        "tradability": {"exclude_suspended": False},
        "selection": {"top_k": 1},
        "industry_blacklist": [],
    }
    runtime_config = {
        "runtime_profile": runtime_profile,
        "runtime_profile_binding": {
            "source": "unit_prospective",
            "profile_version_id": "rprof_unit",
            "config_sha256": "8" * 64,
            "trade_enabled": True,
        },
        "selection_artifact_config": {"cutoff_date": decision_date.isoformat()},
    }
    def _hash(payload: dict) -> str:
        return canonical_evidence_json_sha256(payload)

    universe_layers = []
    for name in (
        "listed_universe",
        "seasoned_universe",
        "pit_st_delist_risk_universe",
        "package_eligible_universe",
        "risk_can_buy_universe",
        "tradability_industry_universe",
    ):
        universe_layers.append(
            {
                "layer": name,
                "status": "PARTIAL",
                "policy_id": f"policy_{name}",
                "policy_version": "v1",
                "policy_hash": "9" * 64,
                "policy_available_at": observed_at,
                "input_count": 1,
                "output_count": 1,
                "excluded_count": 0,
                "exclusion_reason_counts": {},
                "input_symbol_set_hash": universe_hash,
                "output_symbol_set_hash": universe_hash,
                "source_revision_refs": [{"dataset_id": "unit"}],
                "source_revision_set_hash": source_hash,
                "available_at": observed_at,
                "reason_codes": [],
            }
        )
    base_config = {"binding": "unit"}
    request_override = {}
    date_enforced = {"trade_date": decision_date.isoformat()}
    context = ProspectiveSelectionContext(
        capture_mode=EvidenceCaptureMode.PROSPECTIVE,
        selection_run_id="sel_prospective",
        execution_origin="ADVISORY_RUN",
        decision_clock_seed={
            "decision_as_of_trade_date": decision_date,
            "selection_as_of_trade_date": decision_date,
            "target_trade_date": target_date,
            "effective_entry_trade_date": target_date,
            "score_trade_date": decision_date,
            "reference_price_trade_date": decision_date,
            "requested_selection_as_of_trade_date": decision_date,
            "requested_cutoff_date": decision_date,
            "effective_cutoff_date": decision_date,
            "decision_cutoff_ts": observed_at,
            "data_available_at": observed_at,
            "decision_generated_at": observed_at,
            "timezone": "Asia/Shanghai",
            "calendar_version": "market.trading_calendar.v1",
            "calendar_hash": "6" * 64,
            "calendar_source": "market.trading_calendar",
            "is_immediately_previous_trade_date": True,
            "immediate_after_data_refresh": True,
        },
        effective_config_seed={
            "binding_base_config": base_config,
            "binding_base_config_hash": _hash(base_config),
            "binding_base_source_id": "binding_unit",
            "binding_base_source_version": "v1",
            "binding_base_source_hash": "a" * 64,
            "binding_base_available_at": observed_at,
            "binding_base_effective_from_trade_date": decision_date,
            "request_override_config": request_override,
            "request_override_hash": _hash(request_override),
            "date_enforced_config": date_enforced,
            "date_enforced_version": "v1",
            "date_enforced_hash": _hash(date_enforced),
            "selection_normalized_config": runtime_profile,
            "selection_normalized_config_hash": _hash(runtime_profile),
            "package_effective_config": runtime_config,
            "package_effective_config_hash": _hash(runtime_config),
            "runtime_profile_version_id": "rprof_unit",
            "runtime_profile_hash": "8" * 64,
            "selection_adapter_version": "selection_adapter_v1",
            "query_template_version": "query_v1",
            "provider_version": "provider_v1",
            "code_release_id": "unit_release",
            "code_release_hash": "b" * 64,
            "overridden_field_paths_by_layer": {"request_override": []},
            "final_effective_config_hash": _hash(runtime_config),
        },
        policy_registry_ref={"policy_registry_id": "policy_unit", "registry_hash": "c" * 64},
        binding_ref={"binding_id": "binding_unit", "binding_hash": "d" * 64},
        source_watermark_seed={"universe_evidence": {"layers": universe_layers, "package_cohort": {"status": "PARTIAL"}}},
    )
    return context, manifest, artifact, trace, runtime_config, [candidate]


def test_prospective_assembler_emits_v2_and_repository_is_insert_or_compare() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    assembler = ProspectiveSelectionEvidenceAssembler()
    evidence = assembler.assemble(
        context=context,
        manifest=manifest,
        selection_run_id="sel_prospective",
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=selected,
        excluded=[],
        created_by="test",
    )
    retry_evidence = assembler.assemble(
        context=context,
        manifest=manifest,
        selection_run_id="sel_prospective",
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=selected,
        excluded=[],
        created_by="test",
    )
    repository = InMemorySimulationRuntimeRepository()
    first = repository.save_daily_selection_evidence(evidence)
    retry = repository.save_daily_selection_evidence(retry_evidence)

    assert first.evidence_id == retry.evidence_id
    assert first.artifact_hash == retry.artifact_hash
    assert first.evidence_payload_json["schema_version"] == "daily_selection_evidence_v2"
    assert first.evidence_payload_json["evidence_contract"]["capture_status"] == "COMPLETE"
    assert first.evidence_payload_json["evidence_contract"]["research_scope"] == "HISTORICAL_RESEARCH_ONLY"
    assert first.evidence_payload_json["evidence_contract"]["execution_prohibited"] is True
    assert first.evidence_payload_json["evidence_contract"]["market_data_scope"] == "DB_HISTORICAL"
    assert first.evidence_payload_json["phase0a_candidate_lineage"]["selection_run_id"] == "sel_prospective"


def test_prospective_assembler_rejects_non_historical_artifact() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    non_historical_artifact = artifact.model_copy(update={"data_source": "MINIQMT_REALTIME"})

    with pytest.raises(ValueError) as excinfo:
        ProspectiveSelectionEvidenceAssembler().assemble(
            context=context,
            manifest=manifest,
            selection_run_id="sel_prospective",
            artifact=non_historical_artifact,
            stage_trace=trace,
            runtime_config=runtime_config,
            selected=selected,
            excluded=[],
            created_by="test",
        )

    assert getattr(excinfo.value, "reason_code", None) == REASON_HISTORICAL_RESEARCH_ONLY


def test_prospective_assembler_rejects_missing_six_layer_universe_without_fallback() -> None:
    context, manifest, artifact, trace, runtime_config, selected = _prospective_capture_fixture()
    invalid_context = context.model_copy(
        update={"source_watermark_seed": {"universe_evidence": {"layers": [], "package_cohort": {"status": "PARTIAL"}}}}
    )

    with pytest.raises(ValueError, match="six-layer universe"):
        ProspectiveSelectionEvidenceAssembler().assemble(
            context=invalid_context,
            manifest=manifest,
            selection_run_id="sel_prospective",
            artifact=artifact,
            stage_trace=trace,
            runtime_config=runtime_config,
            selected=selected,
            excluded=[],
            created_by="test",
        )


def _v2_artifact_with_payload(payload: dict) -> SelectionScoreArtifact:
    """Recalculate both immutable artifact hashes after a fixture mutation."""

    score_hash = canonical_evidence_json_sha256(payload["scores_json"])
    provisional = SelectionScoreArtifact.model_validate(
        {
            **payload,
            "artifact_sha256": score_hash,
            "artifact_payload_sha256": None,
        }
    )
    return SelectionScoreArtifact.model_validate(
        {
            **provisional.model_dump(mode="python"),
            "artifact_payload_sha256": canonical_evidence_json_sha256(provisional.canonical_v2_header()),
        }
    )


def _context_with_universe_stage_counts(
    context: ProspectiveSelectionContext,
    *,
    package_universe_count: int,
    risk_input_count: int,
    risk_output_count: int,
    risk_excluded_count: int,
    effective_input_count: int,
    effective_output_count: int,
    effective_excluded_count: int,
) -> ProspectiveSelectionContext:
    seed = dict(context.source_watermark_seed)
    source = dict(seed["universe_evidence"])
    layers = []
    for raw in source["layers"]:
        item = dict(raw)
        if item["layer"] == "package_eligible_universe":
            item.update(input_count=package_universe_count, output_count=package_universe_count, excluded_count=0)
        elif item["layer"] == "risk_can_buy_universe":
            item.update(
                input_count=risk_input_count,
                output_count=risk_output_count,
                excluded_count=risk_excluded_count,
                exclusion_reason_counts={"formal_filter": risk_excluded_count} if risk_excluded_count else {},
            )
        elif item["layer"] == "tradability_industry_universe":
            item.update(
                input_count=effective_input_count,
                output_count=effective_output_count,
                excluded_count=effective_excluded_count,
                exclusion_reason_counts={"formal_filter": effective_excluded_count}
                if effective_excluded_count
                else {},
            )
        layers.append(item)
    seed["universe_evidence"] = {**source, "layers": layers}
    return context.model_copy(update={"source_watermark_seed": seed})


def test_prospective_assembler_accepts_proof_backed_raw_empty() -> None:
    context, manifest, artifact, _trace, runtime_config, _selected = _prospective_capture_fixture()
    metadata = {**artifact.metadata, "candidate_outcome": "VALID_NO_CANDIDATE", "empty_stage": "alpha_raw"}
    raw_empty_artifact = _v2_artifact_with_payload(
        {
            **artifact.model_dump(mode="python"),
            "scores_json": [],
            "score_count": 0,
            "universe_count": 3,
            "top_score_symbol": None,
            "metadata": metadata,
        }
    )
    alpha = build_stage_receipt(
        stage=CandidateStageName.ALPHA_RAW,
        status=StageReceiptStatus.COMPLETE,
        input_count=0,
        candidates=[],
        semantic_payload={"artifact_id": raw_empty_artifact.artifact_id},
    )
    hmm = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.NOT_APPLICABLE,
        input_count=0,
        candidates=[],
        semantic_payload={"enabled": True, "generation_mode": "NO_ALPHA_CANDIDATES"},
    )
    risk = build_stage_receipt(
        stage=CandidateStageName.RISK_POLICY_ADJUSTED,
        status=StageReceiptStatus.COMPLETE,
        input_count=0,
        candidates=[],
        semantic_payload={"enabled": False},
    )
    effective = build_stage_receipt(
        stage=CandidateStageName.SELECTION_EFFECTIVE,
        status=StageReceiptStatus.COMPLETE,
        input_count=0,
        candidates=[],
        semantic_payload={"enabled": False, "candidate_pool_count": 0},
    )
    trace = SelectionStageTrace(
        alpha_raw=alpha,
        hmm_adjusted=hmm,
        risk_policy_adjusted=risk,
        selection_effective=effective,
        hmm_metadata={"enabled": True, "status": "NOT_APPLICABLE", "generation_mode": "NO_ALPHA_CANDIDATES"},
        risk_metadata={"enabled": False, "status": "COMPLETE"},
        universe_metadata={"enabled": False, "status": "COMPLETE"},
    )
    raw_empty_context = _context_with_universe_stage_counts(
        context,
        package_universe_count=3,
        risk_input_count=0,
        risk_output_count=0,
        risk_excluded_count=0,
        effective_input_count=0,
        effective_output_count=0,
        effective_excluded_count=0,
    )

    evidence = ProspectiveSelectionEvidenceAssembler().assemble(
        context=raw_empty_context,
        manifest=manifest,
        selection_run_id="sel_prospective",
        artifact=raw_empty_artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=[],
        excluded=[],
        created_by="test",
        candidate_outcome="VALID_NO_CANDIDATE",
    )

    assert evidence.candidate_count == 0
    assert evidence.evidence_payload_json["candidate_outcome"] == "VALID_NO_CANDIDATE"
    assert evidence.evidence_payload_json["phase0a_stage_evidence"]["alpha_raw"]["output_count"] == 0


def test_prospective_assembler_accepts_filtered_empty_and_rejects_manual_declaration() -> None:
    context, manifest, artifact, _trace, runtime_config, selected = _prospective_capture_fixture()
    candidate = selected[0]
    exclusion = SelectionExclusion(
        symbol=candidate.symbol,
        score=candidate.score,
        rank=candidate.rank,
        reason="risk_policy_block_buy",
        source="runtime_profile.risk_policy",
        context={"unit": True},
    )
    alpha = build_stage_receipt(
        stage=CandidateStageName.ALPHA_RAW,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[candidate],
        semantic_payload={"artifact_id": artifact.artifact_id},
    )
    hmm = build_stage_receipt(
        stage=CandidateStageName.HMM_ADJUSTED,
        status=StageReceiptStatus.NOT_APPLICABLE,
        input_count=1,
        candidates=[],
        semantic_payload={"enabled": False, "generation_mode": "NOT_APPLICABLE"},
    )
    risk = build_stage_receipt(
        stage=CandidateStageName.RISK_POLICY_ADJUSTED,
        status=StageReceiptStatus.COMPLETE,
        input_count=1,
        candidates=[],
        exclusions=[exclusion],
        semantic_payload={"enabled": True},
    )
    effective = build_stage_receipt(
        stage=CandidateStageName.SELECTION_EFFECTIVE,
        status=StageReceiptStatus.COMPLETE,
        input_count=0,
        candidates=[],
        semantic_payload={"enabled": False, "candidate_pool_count": 0},
    )
    trace = SelectionStageTrace(
        alpha_raw=alpha,
        hmm_adjusted=hmm,
        risk_policy_adjusted=risk,
        selection_effective=effective,
        hmm_metadata={"enabled": False, "status": "NOT_APPLICABLE", "generation_mode": "NOT_APPLICABLE"},
        risk_metadata={"enabled": True, "status": "COMPLETE"},
        universe_metadata={"enabled": False, "status": "COMPLETE"},
    )
    filtered_context = _context_with_universe_stage_counts(
        context,
        package_universe_count=1,
        risk_input_count=1,
        risk_output_count=0,
        risk_excluded_count=1,
        effective_input_count=0,
        effective_output_count=0,
        effective_excluded_count=0,
    )
    assembler = ProspectiveSelectionEvidenceAssembler()
    evidence = assembler.assemble(
        context=filtered_context,
        manifest=manifest,
        selection_run_id="sel_prospective",
        artifact=artifact,
        stage_trace=trace,
        runtime_config=runtime_config,
        selected=[],
        excluded=[exclusion],
        created_by="test",
        candidate_outcome="VALID_NO_CANDIDATE",
    )
    assert evidence.evidence_payload_json["candidate_outcome"] == "VALID_NO_CANDIDATE"

    with pytest.raises(ValueError, match="runtime no-candidate declaration"):
        assembler.assemble(
            context=filtered_context,
            manifest=manifest,
            selection_run_id="sel_prospective",
            artifact=artifact,
            stage_trace=trace,
            runtime_config={**runtime_config, "valid_no_candidate": True},
            selected=[],
            excluded=[exclusion],
            created_by="test",
            candidate_outcome="VALID_NO_CANDIDATE",
        )


def test_stage_receipt_and_canonical_hash_are_order_deterministic() -> None:
    candidates = [
        SelectionCandidate(symbol="000002.SZ", score=0.8, rank=2),
        SelectionCandidate(symbol="000001.SZ", score=1.0, rank=1),
    ]
    receipt = build_stage_receipt(
        stage=CandidateStageName.ALPHA_RAW,
        status=StageReceiptStatus.COMPLETE,
        input_count=2,
        candidates=candidates,
        semantic_payload={"provider": "live"},
    )

    assert [row["symbol"] for row in receipt.candidates] == ["000001.SZ", "000002.SZ"]
    assert receipt.receipt_hash == canonical_evidence_json_sha256(
        {
            "stage": "alpha_raw",
            "status": "COMPLETE",
            "input_count": 2,
            "output_count": 2,
            "excluded_count": 0,
            "content_hash": receipt.content_hash,
            "semantic_hash": receipt.semantic_hash,
            "exclusions": [],
            "reason_codes": [],
        }
    )
