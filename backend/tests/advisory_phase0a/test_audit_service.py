from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta

import pytest

from backend.services.advisory_phase0a.audit_service import (
    AdvisoryPhase0AAuditService,
    write_receipt_artifacts,
)
from backend.services.advisory_phase0a.handoff import (
    Phase0AHandoffNormalizer,
    audit_manifest_base,
    build_handoff_readiness_report,
    build_phase1_handoff_bundle,
)
from backend.services.advisory_phase0a.models import (
    AuditDateRange,
    AuditRequest,
    AuditTarget,
    CandidateAuthorityStatus,
    ExpectedAlphaMode,
    FormalOOSStatus,
    LabelMaturityStatus,
    Phase0AAuditError,
    Phase0APolicyRegistry,
    SourceAvailability,
    SourceAvailabilityStatus,
)
from backend.services.advisory_phase0a.resolvers import AuditReaders
from backend.services.advisory_phase0a.resolvers import resolve_as_of_binding
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_program import (
    BINDING_STATUS_ACTIVE,
    BINDING_STATUS_RETIRED,
    PACKAGE_MODE_SINGLE,
    AdvisoryProgram,
    AdvisoryStrategyBindingVersion,
)
from backend.services.selection_center.models import SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.selection_center.models import SelectionCandidate
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    PackageStatus,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from backend.services.strategy_package.repository import StrategyPackageRecord
from backend.services.strategy_package.selection_artifact import SelectionScoreArtifact


@dataclass(frozen=True)
class _Evidence:
    evidence_id: str
    target_trade_date: date
    cutoff_date: date
    package_id: str
    manifest_sha256: str
    runtime_profile_version_id: str
    runtime_profile_hash: str
    source_type: str
    data_source: str
    candidate_count: int
    artifact_hash: str
    evidence_payload_json: dict
    excluded_count: int = 0


class _AdvisoryReader:
    def __init__(self, program: AdvisoryProgram, binding: AdvisoryStrategyBindingVersion) -> None:
        self.program = program
        self.binding = binding
        self.calls: list[str] = []

    def get_program(self, program_id: str) -> AdvisoryProgram:
        self.calls.append("get_program")
        assert program_id == self.program.program_id
        return self.program

    def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]:
        self.calls.append("list_binding_versions")
        assert program_id == self.program.program_id
        return [self.binding]


class _PackageReader:
    def __init__(self, record: StrategyPackageRecord, assets: list[StrategyPackageAssetRecord]) -> None:
        self.record = record
        self.assets = assets
        self.calls: list[str] = []

    def get(self, package_id: str) -> StrategyPackageRecord:
        self.calls.append("get")
        assert package_id == self.record.package_id
        return self.record

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[StrategyPackageAssetRecord]:
        self.calls.append("list_package_assets")
        assert package_id == self.record.package_id
        assert protected_only is True
        return self.assets


class _EvidenceReader:
    def __init__(self, evidence: _Evidence) -> None:
        self.evidence = evidence
        self.calls: list[str] = []

    def get_daily_selection_evidence(self, evidence_id: str) -> _Evidence:
        self.calls.append("get_daily_selection_evidence")
        assert evidence_id == self.evidence.evidence_id
        return self.evidence


class _ScoreReader:
    def __init__(self, artifact: SelectionScoreArtifact) -> None:
        self.artifact = artifact
        self.calls: list[str] = []

    def list(self, *, package_id: str, manifest_sha256: str | None = None, limit: int = 100) -> list[SelectionScoreArtifact]:
        self.calls.append("list")
        assert package_id == self.artifact.package_id
        assert manifest_sha256 == self.artifact.manifest_sha256
        assert limit == 1000
        return [self.artifact]


class _RunReader:
    def __init__(self, run: SelectionRun) -> None:
        self.run = run
        self.calls: list[str] = []

    def get_run(self, run_id: str) -> SelectionRun:
        self.calls.append("get_run")
        assert run_id == self.run.run_id
        return self.run


def _manifest(*, alpha_mode: AlphaMode = AlphaMode.SINGLE_ALPHA) -> StrategyPackageManifest:
    if alpha_mode == AlphaMode.MULTI_ALPHA:
        components = [
            AlphaComponent(
                alpha_id="alpha_1",
                alpha_name="alpha_1",
                component_weight=0.5,
                factor_ids=["factor_1"],
                model_id="model_1",
                holding_period="5day",
                rebalance_frequency="1day",
                score_direction="higher_better",
            ),
            AlphaComponent(
                alpha_id="alpha_2",
                alpha_name="alpha_2",
                component_weight=0.5,
                factor_ids=["factor_2"],
                model_id="model_2",
                holding_period="5day",
                rebalance_frequency="1day",
                score_direction="higher_better",
            ),
        ]
        factor_set = [
            FactorAsset(factor_id="factor_1", factor_name="factor_1", asset_ref="cas://factor_1", sha256="f" * 64),
            FactorAsset(factor_id="factor_2", factor_name="factor_2", asset_ref="cas://factor_2", sha256="e" * 64),
        ]
        model_asset: ModelAsset | list[ModelAsset] = [
            ModelAsset(model_id="model_1", model_type="LGB", asset_ref="cas://model_1", sha256="a" * 64),
            ModelAsset(model_id="model_2", model_type="LGB", asset_ref="cas://model_2", sha256="b" * 64),
        ]
        combination = AlphaCombinationPolicy(method="weighted_sum", weights={"alpha_1": 0.5, "alpha_2": 0.5})
    else:
        components = [
            AlphaComponent(
                alpha_id="alpha_1",
                alpha_name="alpha_1",
                component_weight=1.0,
                factor_ids=["factor_1"],
                model_id="model_1",
                holding_period="5day",
                rebalance_frequency="1day",
                score_direction="higher_better",
            )
        ]
        factor_set = [
            FactorAsset(
                factor_id="factor_1",
                factor_name="factor_1",
                asset_ref="cas://factor_1",
                sha256="f" * 64,
            )
        ]
        model_asset = ModelAsset(
            model_id="model_1",
            model_type="LGB",
            asset_ref="cas://model_1",
            sha256="a" * 64,
        )
        combination = AlphaCombinationPolicy(method="identity", weights={"alpha_1": 1.0})
    return freeze_manifest(
        StrategyPackageManifest(
            package_id="pkg_phase0a",
            package_name="Phase 0A test package",
            source=StrategyPackageSource(
                source_type=SourceType.QE_EXPERIMENT,
                source_id="qe_phase0a",
                created_at=datetime(2025, 12, 1, tzinfo=UTC),
            ),
            alpha_mode=alpha_mode,
            alpha_components=components,
            alpha_combination_policy=combination,
            factor_set=factor_set,
            model_asset=model_asset,
            source_evidence={
                "phase0a_parent_vintage": {
                    "available_at": "2025-12-20T00:00:00+00:00",
                    "information_cutoff": "2026-01-05",
                },
                "phase0a_alpha_leg_vintages": {
                    component.alpha_id: {
                        "available_at": "2025-12-20T00:00:00+00:00",
                        "information_cutoff": "2026-01-05",
                    }
                    for component in components
                },
                "phase0a_multi_alpha_weight_vintage": {
                    "available_at": "2025-12-20T00:00:00+00:00",
                    "information_cutoff": "2026-01-05",
                },
            },
            backtest_context={
                "daily_strategy": {
                    "topk": 20,
                    "topk_variants": [5, 20],
                }
            },
            backtest_summary=BacktestSummary(ic=0.05, raw_metrics={"IC": 0.05}),
            package_status=PackageStatus.SELECTION_ENABLED,
        )
    )


def _record(manifest: StrategyPackageManifest) -> StrategyPackageRecord:
    assert manifest.manifest_sha256
    return StrategyPackageRecord(
        package_id=manifest.package_id,
        package_name=manifest.package_name,
        package_version=manifest.package_version,
        source_type=manifest.source.source_type.value,
        source_id=manifest.source.source_id,
        package_status=PackageStatus.SELECTION_ENABLED,
        manifest=manifest,
        manifest_sha256=manifest.manifest_sha256,
        alpha_mode=manifest.alpha_mode,
    )


def _program_and_binding() -> tuple[AdvisoryProgram, AdvisoryStrategyBindingVersion]:
    program = AdvisoryProgram(
        program_id="adv_phase0a",
        program_name="Phase 0A",
        status="ENABLED",
        target_count=5,
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_phase0a"],
        package_weights={"pkg_phase0a": 1.0},
        fusion_method=None,
        package_set_hash="package_set_hash",
        fusion_policy_sha256=None,
        review_policy={},
        review_policy_sha256="review_policy_hash",
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
        review_schedule={},
    )
    binding = AdvisoryStrategyBindingVersion(
        binding_version_id="bind_phase0a",
        program_id=program.program_id,
        program_version=1,
        package_mode=PACKAGE_MODE_SINGLE,
        package_ids=["pkg_phase0a"],
        package_weights={"pkg_phase0a": 1.0},
        fusion_method=None,
        package_set_hash="package_set_hash",
        fusion_policy_sha256=None,
        effective_from_trade_date=date(2025, 12, 1),
        activation_status=BINDING_STATUS_ACTIVE,
    )
    return program, binding


def _target(*, expected_alpha_mode: ExpectedAlphaMode = ExpectedAlphaMode.SINGLE_ALPHA) -> AuditTarget:
    alpha_mode = AlphaMode.MULTI_ALPHA if expected_alpha_mode == ExpectedAlphaMode.MULTI_ALPHA else AlphaMode.SINGLE_ALPHA
    manifest = _manifest(alpha_mode=alpha_mode)
    assert manifest.manifest_sha256
    return AuditTarget(
        audit_target_id="target_phase0a",
        program_id="adv_phase0a",
        package_id="pkg_phase0a",
        manifest_sha256=manifest.manifest_sha256,
        expected_alpha_mode=expected_alpha_mode,
        decision_date_range=AuditDateRange(start_date=date(2026, 2, 5), end_date=date(2026, 2, 5)),
        decision_dates=[date(2026, 2, 5)],
        selection_evidence_ids_by_decision_date={date(2026, 2, 5): "dse_phase0a"},
        style_family="mean_reversion",
        audit_policy_version="phase0a_policy_v1",
    )


def _policy() -> Phase0APolicyRegistry:
    return Phase0APolicyRegistry(
        policy_registry_id="phase0a_test_registry",
        policy_version="phase0a_policy_v1",
        frozen_at="2025-12-01T00:00:00+00:00",
        effective_from_trade_date=date(2025, 1, 1),
        registry_content_hash="e" * 64,
        benchmark_policy={
            "policy_id": "PIT_ELIGIBLE_UNIVERSE_EQ_WEIGHT_TOTAL_RETURN_V1",
            "policy_hash": "1" * 64,
            "universe_layer": "tradability_industry_universe",
            "entry_basis": "target_open",
            "effective_range": "2025-01-01/",
        },
        cost_policy={"policy_id": "cost_v1", "policy_hash": "2" * 64, "effective_range": "2025-01-01/"},
        label_policy={
            "policy_id": "label_v1",
            "policy_hash": "3" * 64,
            "horizons": [1, 5, 20],
            "entry_basis": "target_open",
            "censor_rule": "pre_registered_v1",
            "barrier_event_order_policy_id": "BARRIER_EVENT_ORDER_V1",
            "barrier_event_order_policy_hash": "4" * 64,
            "maturity_status_by_decision_date": {"2026-02-05": "PENDING"},
        },
        prior_policy={"registry_id": "prior_v1", "registry_hash": "5" * 64, "frozen_at": "2025-12-01T00:00:00+00:00"},
        multiple_testing_policy={"registry_id": "multiple_v1", "registry_hash": "6" * 64, "frozen_at": "2025-12-01T00:00:00+00:00"},
        universe_policy={"policy_id": "pit_universe_v1", "policy_hash": "7" * 64},
        embargo_policy_id="ADVISORY_RESEARCH_EMBARGO_V1",
        embargo_policy_version="v1",
        embargo_policy_hash="c" * 64,
        cutoff_timestamp_normalization="ASIA_SHANGHAI_AVAILABLE_TRADE_DATE_V1",
        training_label_information_end_rule="MAX_INFORMATION_CONSUMED",
        calendar_version="market_calendar_v1",
        calendar_hash="d" * 64,
    )


def _readers(
    *,
    no_candidate: bool = False,
    alpha_mode: AlphaMode = AlphaMode.SINGLE_ALPHA,
) -> tuple[AuditReaders, tuple[object, ...]]:
    manifest = _manifest(alpha_mode=alpha_mode)
    record = _record(manifest)
    model_asset = StrategyPackageAssetRecord(
        package_id=record.package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="cas://model_1",
        asset_sha256="a" * 64,
        metadata={"available_at": "2025-12-20T00:00:00+00:00", "data_cutoff": "2026-01-05"},
    )
    factor_asset = StrategyPackageAssetRecord(
        package_id=record.package_id,
        asset_type=StrategyPackageAssetType.FACTOR_CODE,
        asset_ref="cas://factor_1",
        asset_sha256="f" * 64,
        metadata={"available_at": "2025-12-20T00:00:00+00:00", "data_cutoff": "2026-01-05"},
    )
    assets = [model_asset, factor_asset]
    if alpha_mode == AlphaMode.MULTI_ALPHA:
        assets.extend(
            [
                StrategyPackageAssetRecord(
                    package_id=record.package_id,
                    asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
                    asset_ref="cas://model_2",
                    asset_sha256="b" * 64,
                    metadata={"available_at": "2025-12-20T00:00:00+00:00", "data_cutoff": "2026-01-05"},
                ),
                StrategyPackageAssetRecord(
                    package_id=record.package_id,
                    asset_type=StrategyPackageAssetType.FACTOR_CODE,
                    asset_ref="cas://factor_2",
                    asset_sha256="e" * 64,
                    metadata={"available_at": "2025-12-20T00:00:00+00:00", "data_cutoff": "2026-01-05"},
                ),
            ]
        )
    artifact = SelectionScoreArtifact(
        artifact_id="ssa_phase0a",
        package_id=record.package_id,
        manifest_sha256=record.manifest_sha256,
        trade_date=date(2026, 2, 5),
        data_source="DB_HISTORICAL",
        runtime_config_hash="runtime_hash",
        scores_json=[] if no_candidate else [{"symbol": "000001.SZ", "score": 1.0, "rank": 1}],
        artifact_sha256="b" * 64,
        score_count=0 if no_candidate else 1,
        universe_count=1,
        metadata={
            "source_type": "live_multi_alpha_inference_v1" if alpha_mode == AlphaMode.MULTI_ALPHA else "live_qe_model_inference_v1",
            "authority_scope": "authoritative_selection",
            "top_k": 20,
            "effective_top_k": 20,
        },
    )
    candidate = SelectionCandidate(symbol="000001.SZ", score=1.0, rank=1)
    run = SelectionRun(
        run_id="sel_phase0a",
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=date(2026, 2, 6),
        data_source="DB_HISTORICAL",
        package_ids=[record.package_id],
        status=SelectionRunStatus.SUCCEEDED,
        manifest_sha256_by_package={record.package_id: record.manifest_sha256},
        aggregate_results=[] if no_candidate else [candidate],
    )
    payload = {
        "runtime_profile_binding": {
            "source": "runtime_config_activation",
            "profile_version_id": "runtime_v1",
            "config_sha256": "runtime_hash",
            "available_at": "2025-12-20T00:00:00+00:00",
        },
        "runtime_profile": {"hmm": {"enabled": False}},
        "point_in_time_context": {
            "pit_mode": "PREVIOUS_TRADE_DATE",
            "cutoff_date": "2026-02-05",
            "selection_as_of_trade_date": "2026-02-05",
            "score_trade_date": "2026-02-05",
            "reference_price_trade_date": "2026-02-05",
            "effective_trade_date": "2026-02-06",
            "is_immediately_previous_trade_date": True,
            "decision_cutoff_ts": "2026-02-05T15:00:00+08:00",
            "data_available_at": "2026-02-05T14:30:00+08:00",
            "timezone": "Asia/Shanghai",
            "calendar_version": "market_calendar_v1",
            "calendar_hash": "d" * 64,
            "calendar_source": "market.trading_calendar",
        },
        "selection_artifact_config": {"top_k": 20, "display_top_n": 20, "effective_selection_top_k": 20},
        "phase0a_effective_config_chain": {
            "binding_base_config": {},
            "request_override_config": {},
            "date_enforced_config": {"cutoff_date": "2026-02-05"},
            "selection_normalized_config": {"hmm": {"enabled": False}},
            "package_effective_config": {"package_id": record.package_id},
            "runtime_variant_id": "default",
            "selection_adapter_version": "selection_v1",
            "query_template_version": "query_v1",
        },
        "selected_candidates": [] if no_candidate else [candidate.model_dump(mode="json")],
        "phase0a_universe_evidence": {
            "layers": [
                {
                    "layer": name,
                    "status": "FORMAL_READY",
                    "policy_hash": "u" * 64,
                    "input_count": 1,
                    "output_count": 1,
                    "excluded_count": 0,
                    "symbol_set_hash": "v" * 64,
                    "available_at": "2026-02-05T14:30:00+08:00",
                    "policy_available_at": "2025-12-20T00:00:00+00:00",
                }
                for name in (
                    "listed_universe",
                    "seasoned_universe",
                    "pit_st_delist_risk_universe",
                    "package_eligible_universe",
                    "risk_can_buy_universe",
                    "tradability_industry_universe",
                )
            ],
            "package_cohort": {"status": "FORMAL_READY"},
        },
        "phase0a_candidate_lineage": {
            "selection_run_id": run.run_id,
            "selection_score_artifact_id": artifact.artifact_id,
            "selection_score_artifact_sha256": artifact.artifact_sha256,
        },
    }
    evidence = _Evidence(
        evidence_id="dse_phase0a",
        target_trade_date=date(2026, 2, 6),
        cutoff_date=date(2026, 2, 5),
        package_id=record.package_id,
        manifest_sha256=record.manifest_sha256,
        runtime_profile_version_id="runtime_v1",
        runtime_profile_hash="runtime_hash",
        source_type="live_multi_alpha_inference_v1" if alpha_mode == AlphaMode.MULTI_ALPHA else "live_qe_model_inference_v1",
        data_source="DB_HISTORICAL",
        candidate_count=0 if no_candidate else 1,
        artifact_hash="d" * 64,
        evidence_payload_json=payload,
    )
    program, binding = _program_and_binding()
    advisory = _AdvisoryReader(program, binding)
    package = _PackageReader(record, assets)
    evidence_reader = _EvidenceReader(evidence)
    score = _ScoreReader(artifact)
    run_reader = _RunReader(run)
    class _Calendar:
        def list_trading_days(self, *, start_date: date, end_date: date) -> list[date]:
            rows: list[date] = []
            current = start_date
            while current <= end_date:
                if current.weekday() < 5:
                    rows.append(current)
                current += timedelta(days=1)
            return rows

    return (
        AuditReaders(
            advisory=advisory,
            package=package,
            evidence=evidence_reader,
            score_artifact=score,
            selection_run=run_reader,
            calendar=_Calendar(),
        ),
        (advisory, package, evidence_reader, score, run_reader),
    )


def _request(*, expected_alpha_mode: ExpectedAlphaMode = ExpectedAlphaMode.SINGLE_ALPHA) -> AuditRequest:
    return AuditRequest(
        audit_id="audit_phase0a",
        policy_registry_id="phase0a_test_registry",
        audit_policy_version="phase0a_policy_v1",
        policy_registry_content_hash="e" * 64,
        targets=[_target(expected_alpha_mode=expected_alpha_mode)],
    )


def test_formal_audit_rejects_a_scratch_policy_registry() -> None:
    readers, _fakes = _readers()
    scratch_policy = Phase0APolicyRegistry(policy_version="phase0a_policy_v1")

    with pytest.raises(Phase0AAuditError, match="ADVISORY_PHASE0A_POLICY_REGISTRY_NOT_FROZEN"):
        AdvisoryPhase0AAuditService(readers=readers, policy=scratch_policy).audit(_request())


def test_formal_audit_rejects_request_policy_hash_mismatch() -> None:
    readers, _fakes = _readers()
    mismatched_request = _request().model_copy(update={"policy_registry_content_hash": "f" * 64})

    with pytest.raises(Phase0AAuditError, match="ADVISORY_PHASE0A_POLICY_REGISTRY_HASH_MISMATCH"):
        AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(mismatched_request)


def test_audit_builds_formal_receipt_and_only_uses_reader_methods(tmp_path) -> None:
    readers, fakes = _readers()
    service = AdvisoryPhase0AAuditService(readers=readers, policy=_policy())

    receipt = service.audit(_request())

    result = receipt.results[0]
    assert result.candidate_authority[0].status == CandidateAuthorityStatus.FORMAL
    assert result.candidate_authority[0].signal_context_hash
    assert result.oos_classifications[0].formal_oos_status == FormalOOSStatus.FORMAL_OOS
    assert result.oos_classifications[0].label_maturity_status == LabelMaturityStatus.PENDING
    assert result.oos_intervals[0].start_date == date(2026, 2, 5)
    assert all(getattr(fake, "calls") for fake in fakes)

    destination = write_receipt_artifacts(
        receipt=receipt,
        request=_request(),
        policy=_policy(),
        output_root=tmp_path,
    )
    assert {path.name for path in destination.iterdir()} == {
        "target_scope_registry.json",
        "package_asset_vintage_ledger.json",
        "runtime_semantics_ledger.json",
        "hmm_vintage_ledger.json",
        "source_availability_matrix.json",
        "universe_survivorship_report.json",
        "oos_interval_report.json",
        "metric_label_policy_registry.json",
        "prior_registry.json",
        "multiple_testing_registry.json",
        "audit_manifest.json",
        "audit_summary.md",
        "candidate_authority_stage_capability_report.json",
        "prior_cohort_report.json",
        "handoff_readiness_report.json",
        "phase1_handoff_bundle.json",
    }
    manifest = json.loads((destination / "audit_manifest.json").read_text(encoding="utf-8"))
    manifest_hash = manifest.pop("audit_manifest_hash")
    handoff_readiness_hash = manifest.pop("handoff_readiness_report_hash")
    assert manifest_hash == receipt.audit_manifest_hash == canonical_json_sha256(manifest)
    readiness = json.loads((destination / "handoff_readiness_report.json").read_text(encoding="utf-8"))
    bundle = json.loads((destination / "phase1_handoff_bundle.json").read_text(encoding="utf-8"))
    assert readiness["schema_version"] == "advisory_phase0a_handoff_readiness_v1"
    assert readiness["readiness"] == "READY"
    assert readiness["handoff_readiness_hash"] == handoff_readiness_hash
    assert bundle["schema_version"] == "advisory_phase0a_handoff_bundle_v2"
    assert bundle["handoff_readiness_report_hash"] == handoff_readiness_hash
    assert len(bundle["sorted_target_handoffs"][0]["admission_scopes"]) == 1
    assert not any("approval" in path.name for path in destination.iterdir())
    with pytest.raises(Phase0AAuditError, match="already exists"):
        write_receipt_artifacts(receipt=receipt, request=_request(), policy=_policy(), output_root=tmp_path)


def test_no_candidate_never_becomes_formal_authority_or_consumable_handoff(tmp_path) -> None:
    readers, _fakes = _readers(no_candidate=True)
    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(_request())

    report = receipt.results[0].candidate_authority[0]
    classification = receipt.results[0].oos_classifications[0]
    assert report.status == CandidateAuthorityStatus.NONE
    assert classification.formal_oos_status == FormalOOSStatus.NONE
    assert "ADVISORY_PHASE0A_NO_CANDIDATE_AUTHORITY_MISSING" in report.phase0a_reason_codes
    destination = write_receipt_artifacts(
        receipt=receipt,
        request=_request(),
        policy=_policy(),
        output_root=tmp_path,
    )
    readiness = json.loads((destination / "handoff_readiness_report.json").read_text(encoding="utf-8"))
    assert readiness["readiness"] == "BLOCKED"
    assert not (destination / "phase1_handoff_bundle.json").exists()


def _receipt_with_results(*, receipt, request: AuditRequest, policy: Phase0APolicyRegistry, results):
    result_hash = canonical_json_sha256([result.model_dump(mode="python") for result in results])
    refreshed = receipt.model_copy(update={"results": results, "result_hash": result_hash})
    manifest_hash = canonical_json_sha256(
        audit_manifest_base(
            audit_id=refreshed.audit_id,
            audit_policy_version=refreshed.audit_policy_version,
            request_hash=refreshed.request_hash,
            result_hash=refreshed.result_hash,
            results=refreshed.results,
            policy=policy,
        )
    )
    return refreshed.model_copy(update={"audit_manifest_hash": manifest_hash})


def test_native_multi_alpha_parent_uses_one_program_binding_and_formal_source_type(tmp_path) -> None:
    readers, _fakes = _readers(alpha_mode=AlphaMode.MULTI_ALPHA)
    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(
        _request(expected_alpha_mode=ExpectedAlphaMode.MULTI_ALPHA)
    )

    result = receipt.results[0]
    assert result.candidate_authority[0].status == CandidateAuthorityStatus.FORMAL
    assert result.oos_classifications[0].formal_oos_status == FormalOOSStatus.FORMAL_OOS
    assert len(result.asset_ledger) == 8
    destination = write_receipt_artifacts(
        receipt=receipt,
        request=_request(expected_alpha_mode=ExpectedAlphaMode.MULTI_ALPHA),
        policy=_policy(),
        output_root=tmp_path,
    )
    bundle = json.loads((destination / "phase1_handoff_bundle.json").read_text(encoding="utf-8"))
    scopes = bundle["sorted_target_handoffs"][0]["admission_scopes"]
    assert len(scopes) == 1
    assert scopes[0]["stable_signal_semantics_payload_v1"]["package_id"] == "pkg_phase0a"
    assert scopes[0]["stable_signal_semantics_payload_v1"]["manifest_sha256"] == result.manifest_sha256


def test_handoff_hashes_are_stable_and_tampered_receipts_fail_closed() -> None:
    readers, _fakes = _readers()
    request = _request()
    policy = _policy()
    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=policy).audit(request)

    normalizer = Phase0AHandoffNormalizer(policy=policy)
    first, first_bundle = normalizer.normalize(
        receipt=receipt,
        request=request,
        created_at=datetime(2026, 2, 6, tzinfo=UTC),
    )
    second, second_bundle = normalizer.normalize(
        receipt=receipt,
        request=request,
        created_at=datetime(2026, 2, 6, tzinfo=UTC),
    )
    assert first.handoff_readiness_hash == second.handoff_readiness_hash
    assert first_bundle is not None and second_bundle is not None
    assert first_bundle.phase1_handoff_bundle_hash == second_bundle.phase1_handoff_bundle_hash

    tampered = receipt.model_copy(update={"result_hash": "0" * 64})
    blocked = build_handoff_readiness_report(receipt=tampered, request=request, policy=policy)
    assert blocked.readiness.value == "BLOCKED"
    assert "ADVISORY_PHASE0A_HANDOFF_RESULT_HASH_MISMATCH" in blocked.blocking_reason_codes
    assert build_phase1_handoff_bundle(report=blocked, policy=policy) is None


def test_current_watermark_without_historical_available_at_is_not_formal_oos() -> None:
    readers, _fakes = _readers()

    class _WatermarkOnlyProbe:
        def probe(self, *, decision_date: date) -> list[SourceAvailability]:
            return [
                SourceAvailability(
                    source_id="market_kline_daily_raw",
                    capability="daily_market",
                    decision_date=decision_date,
                    status=SourceAvailabilityStatus.PARTIAL,
                    watermark_date=decision_date,
                    data_cutoff=decision_date,
                    reason_codes=["ADVISORY_PHASE0A_SOURCE_AVAILABLE_AT_MISSING"],
                )
            ]

    audited_readers = AuditReaders(
        advisory=readers.advisory,
        package=readers.package,
        evidence=readers.evidence,
        score_artifact=readers.score_artifact,
        selection_run=readers.selection_run,
        source_probe=_WatermarkOnlyProbe(),
        calendar=readers.calendar,
    )
    receipt = AdvisoryPhase0AAuditService(readers=audited_readers, policy=_policy()).audit(_request())

    classification = receipt.results[0].oos_classifications[0]
    assert classification.formal_oos_status == FormalOOSStatus.RETROSPECTIVE_RESEARCH_ONLY
    assert classification.availability_status.value == "UNAVAILABLE"
    handoff = build_handoff_readiness_report(receipt=receipt, request=_request(), policy=_policy())
    scope = handoff.sorted_target_handoffs[0].admission_scopes[0]
    assert handoff.readiness.value == "PARTIAL"
    assert scope.evidence_scope.value == "RETROSPECTIVE_RESEARCH_ONLY"
    assert scope.readiness.value == "PARTIAL"
    assert build_phase1_handoff_bundle(report=handoff, policy=_policy()) is not None


def test_handoff_blocks_target_scope_drift_without_blocking_receipt_serialization() -> None:
    readers, _fakes = _readers()
    request = _request()
    policy = _policy()
    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=policy).audit(request)
    result = receipt.results[0].model_copy(update={"package_id": "pkg_other"})
    drifted = _receipt_with_results(receipt=receipt, request=request, policy=policy, results=[result])

    report = build_handoff_readiness_report(receipt=drifted, request=request, policy=policy)

    assert report.readiness.value == "BLOCKED"
    assert "ADVISORY_PHASE0A_HANDOFF_TARGET_SCOPE_MISMATCH" in report.blocking_reason_codes


def test_handoff_blocks_incomplete_runtime_config_and_receipt_identity_drift() -> None:
    readers, _fakes = _readers()
    request = _request()
    policy = _policy()
    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=policy).audit(request)
    result = receipt.results[0]
    runtime = result.runtime_semantics[0].model_copy(update={"effective_config_chain_complete": False})
    invalid_runtime_result = result.model_copy(update={"runtime_semantics": [runtime]})
    invalid_runtime = _receipt_with_results(
        receipt=receipt,
        request=request,
        policy=policy,
        results=[invalid_runtime_result],
    )

    report = build_handoff_readiness_report(receipt=invalid_runtime, request=request, policy=policy)
    assert report.readiness.value == "BLOCKED"
    assert "ADVISORY_PHASE0A_HANDOFF_EFFECTIVE_CONFIG_MISSING" in report.blocking_reason_codes

    identity_drift = receipt.model_copy(
        update={
            "audit_id": "audit_phase0a_drift",
            "audit_policy_version": "phase0a_policy_drift",
            "request_hash": "0" * 64,
        }
    )
    blocked = build_handoff_readiness_report(receipt=identity_drift, request=request, policy=policy)
    assert blocked.readiness.value == "BLOCKED"
    assert "ADVISORY_PHASE0A_HANDOFF_AUDIT_ID_MISMATCH" in blocked.blocking_reason_codes
    assert "ADVISORY_PHASE0A_HANDOFF_AUDIT_POLICY_MISMATCH" in blocked.blocking_reason_codes
    assert "ADVISORY_PHASE0A_HANDOFF_REQUEST_HASH_MISMATCH" in blocked.blocking_reason_codes


def test_equivalent_programs_share_signal_context_but_keep_lineage(tmp_path) -> None:
    readers, fakes = _readers()
    base_advisory = fakes[0]
    assert isinstance(base_advisory, _AdvisoryReader)
    second_program = replace(base_advisory.program, program_id="adv_phase0a_second", program_name="Phase 0A second")
    second_binding = replace(
        base_advisory.binding,
        binding_version_id="bind_phase0a_second",
        program_id=second_program.program_id,
    )

    class _TwoProgramReader:
        def get_program(self, program_id: str) -> AdvisoryProgram:
            return {base_advisory.program.program_id: base_advisory.program, second_program.program_id: second_program}[program_id]

        def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]:
            return {base_advisory.program.program_id: [base_advisory.binding], second_program.program_id: [second_binding]}[program_id]

    target_one = _target()
    target_two = _target().model_copy(update={"audit_target_id": "target_phase0a_second", "program_id": second_program.program_id})
    request = AuditRequest(
        audit_id="audit_phase0a_two_programs",
        policy_registry_id="phase0a_test_registry",
        audit_policy_version="phase0a_policy_v1",
        policy_registry_content_hash="e" * 64,
        targets=[target_one, target_two],
    )
    audited_readers = AuditReaders(
        advisory=_TwoProgramReader(),
        package=readers.package,
        evidence=readers.evidence,
        score_artifact=readers.score_artifact,
        selection_run=readers.selection_run,
        calendar=readers.calendar,
    )
    receipt = AdvisoryPhase0AAuditService(readers=audited_readers, policy=_policy()).audit(request)

    reports = [result.candidate_authority[0] for result in receipt.results]
    assert len({report.signal_context_hash for report in reports}) == 1
    destination = write_receipt_artifacts(receipt=receipt, request=request, policy=_policy(), output_root=tmp_path)
    capability = json.loads(
        (destination / "candidate_authority_stage_capability_report.json").read_text(encoding="utf-8")
    )
    assert len(capability["canonical_observation_groups"]) == 1
    assert len(capability["canonical_observation_groups"][0]["lineage"]) == 2


def test_hmm_config_id_without_historical_snapshot_is_unavailable() -> None:
    readers, fakes = _readers()
    evidence_reader = fakes[2]
    assert isinstance(evidence_reader, _EvidenceReader)
    payload = dict(evidence_reader.evidence.evidence_payload_json)
    payload["runtime_profile"] = {"hmm": {"enabled": True, "model_config_id": "hmm_config_only"}}
    evidence_reader.evidence = replace(evidence_reader.evidence, evidence_payload_json=payload)

    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(_request())

    hmm = receipt.results[0].hmm_vintages[0]
    assert hmm.status == "UNAVAILABLE"
    assert "ADVISORY_PHASE0A_HMM_DYNAMIC_LATEST_FORBIDDEN" in hmm.reason_codes
    assert receipt.results[0].oos_classifications[0].formal_oos_status != FormalOOSStatus.FORMAL_OOS


def test_explicit_hmm_snapshot_metadata_can_be_formal_without_generation() -> None:
    readers, fakes = _readers()
    evidence_reader = fakes[2]
    assert isinstance(evidence_reader, _EvidenceReader)
    payload = dict(evidence_reader.evidence.evidence_payload_json)
    payload["runtime_profile"] = {
        "hmm": {
            "enabled": True,
            "model_snapshot_id": "hmm_snapshot_1",
            "model_config_id": "hmm_config_1",
            "signal_preset": "baseline",
        }
    }
    config_chain = dict(payload["phase0a_effective_config_chain"])
    config_chain["selection_normalized_config"] = payload["runtime_profile"]
    payload["phase0a_effective_config_chain"] = config_chain
    payload["phase0a_hmm_metadata"] = {
        "model_snapshot_id": "hmm_snapshot_1",
        "model_config_id": "hmm_config_1",
        "signal_preset": "baseline",
        "model_artifact_sha256": "8" * 64,
        "coefficient_sha256": "9" * 64,
        "snapshot_trained_at": "2025-12-15T00:00:00+00:00",
        "available_at": "2025-12-20T00:00:00+00:00",
        "training_information_cutoff": "2026-01-05",
        "as_of_trade_date": "2026-02-05",
        "effective_trade_date": "2026-02-06",
        "generation_mode": "historical_snapshot",
        "input_data_max_dates": {"sector_data": "2026-01-05"},
        "freshness_lag": 1,
        "data_cutoff": "2026-01-05",
    }
    evidence_reader.evidence = replace(evidence_reader.evidence, evidence_payload_json=payload)

    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(_request())

    hmm = receipt.results[0].hmm_vintages[0]
    assert hmm.status == "FORMAL"
    assert receipt.results[0].oos_classifications[0].formal_oos_status == FormalOOSStatus.FORMAL_OOS


def test_multi_alpha_topk_outside_frozen_variants_is_not_formal() -> None:
    readers, fakes = _readers(alpha_mode=AlphaMode.MULTI_ALPHA)
    evidence_reader = fakes[2]
    assert isinstance(evidence_reader, _EvidenceReader)
    payload = dict(evidence_reader.evidence.evidence_payload_json)
    payload["selection_artifact_config"] = {"top_k": 50, "effective_selection_top_k": 50}
    evidence_reader.evidence = replace(evidence_reader.evidence, evidence_payload_json=payload)

    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(
        _request(expected_alpha_mode=ExpectedAlphaMode.MULTI_ALPHA)
    )

    report = receipt.results[0].candidate_authority[0]
    assert report.status == CandidateAuthorityStatus.RETROSPECTIVE
    assert "multi_alpha_topk_runtime_mismatch" in report.phase0a_reason_codes


def test_enabled_risk_policy_without_intermediate_stage_is_partial() -> None:
    readers, fakes = _readers()
    evidence_reader = fakes[2]
    assert isinstance(evidence_reader, _EvidenceReader)
    payload = dict(evidence_reader.evidence.evidence_payload_json)
    payload["runtime_profile"] = {
        "hmm": {"enabled": False},
        "risk_policy": {"enabled": True},
        "industry_blacklist": ["Bank"],
    }
    evidence_reader.evidence = replace(evidence_reader.evidence, evidence_payload_json=payload)

    receipt = AdvisoryPhase0AAuditService(readers=readers, policy=_policy()).audit(_request())

    report = receipt.results[0].candidate_authority[0]
    risk_stage = next(item for item in report.stage_capabilities if item.stage.value == "risk_policy_adjusted")
    assert risk_stage.status.value == "PARTIAL"
    assert "ADVISORY_PHASE0A_RISK_POLICY_EVIDENCE_PARTIAL" in report.risk_policy.reason_codes


def test_historical_binding_rollover_uses_retired_record_before_right_open_end() -> None:
    _program, binding = _program_and_binding()
    retired = replace(
        binding,
        binding_version_id="binding_retired",
        effective_from_trade_date=date(2026, 1, 1),
        effective_to_trade_date=date(2026, 2, 5),
        activation_status=BINDING_STATUS_RETIRED,
    )
    active = replace(
        binding,
        binding_version_id="binding_active",
        effective_from_trade_date=date(2026, 2, 5),
        effective_to_trade_date=None,
        activation_status=BINDING_STATUS_ACTIVE,
    )

    before = resolve_as_of_binding(bindings=[active, retired], decision_date=date(2026, 2, 4), target=_target())
    after = resolve_as_of_binding(bindings=[active, retired], decision_date=date(2026, 2, 5), target=_target())

    assert before.binding is not None and before.binding.binding_version_id == "binding_retired"
    assert after.binding is not None and after.binding.binding_version_id == "binding_active"
