from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from uuid import uuid4

import psycopg2
import pytest

from backend.services.advisory_dev_input_onboarding.contracts import AlphaMode, HistoricalProgramSpec
from backend.services.advisory_dev_input_onboarding.historical_onboarding import (
    HistoricalResearchExecutionProhibitedPortfolioService,
    RealDevHistoricalOnboardingService,
)
from backend.services.advisory_phase0a.historical_research import (
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchBatchRequest,
    HistoricalResearchRunStatus,
)
from backend.services.advisory_phase0a.historical_research_postgres import (
    PersistedHistoricalSelectionEvidenceAdapter,
    PostgresHistoricalResearchProgramResolver,
    PostgresHistoricalResearchRepository,
    PostgresHistoricalResearchTradingDateResolver,
)
from backend.services.advisory_phase1.stage_trace import Phase1TraceCaptureService
from backend.services.advisory_program import AdvisoryProgramPGRepository, AdvisoryProgramService
from backend.services.selection_center.prospective_evidence import canonical_evidence_json_sha256
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.simulation_runtime.selection import DailySelectionSignalService, StrategyPackageSelectionService
from backend.services.strategy_package.components import StrategyPackageComponentService
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.multi_alpha_live import LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactRepository,
    selection_artifact_runtime_hash_v2_for_manifest,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_multi_alpha_base_schema import _multi_manifest, _single_manifest
from backend.tests.strategy_package.test_prospective_selection_evidence_dev_db import _dynamic_fixture


REPO_ROOT = Path(__file__).resolve().parents[3]
O3_MIGRATION_CHAIN = (
    REPO_ROOT / "backend/migrations/data_sync_targets_20260519.sql",
    REPO_ROOT / "backend/migrations/trading_core_v2_schema.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_package_asset_20260509.sql",
    REPO_ROOT / "backend/migrations/qe_phase4_master_seed_contract_20260509.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_candidate_strategy_package_20260513.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_multi_alpha_base_20260619.sql",
    REPO_ROOT / "backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.sql",
    REPO_ROOT / "backend/db/migrations/add_price_guard_stage1_advisory_20260602.sql",
    REPO_ROOT / "backend/db/migrations/add_advisory_program_lifecycle_20260604.sql",
    REPO_ROOT / "backend/db/migrations/add_advisory_recommendation_list_lifecycle_20260608.sql",
    REPO_ROOT / "backend/db/migrations/add_selection_score_artifact_v2_evidence_20260712.sql",
    REPO_ROOT / "backend/db/migrations/add_advisory_historical_research_runner_20260712.sql",
)


@pytest.fixture
def postgres_dsn() -> str:
    dsn = os.getenv("ADVISORY_O3_TEST_DSN")
    if not dsn:
        pytest.skip("ADVISORY_O3_TEST_DSN is not configured for disposable PostgreSQL")
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            for schema in ("app", "strategy_pkg", "selection", "paper_v2", "trading_core", "market"):
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cursor.execute("CREATE SCHEMA app")
            cursor.execute("CREATE TABLE app.watchlist_items (id BIGSERIAL PRIMARY KEY)")
            cursor.execute("CREATE SCHEMA market")
            cursor.execute(
                "CREATE TABLE market.trading_calendar (cal_date DATE PRIMARY KEY, is_trading BOOLEAN NOT NULL)"
            )
            for migration in O3_MIGRATION_CHAIN:
                cursor.execute(migration.read_text(encoding="utf-8-sig"))
        connection.commit()
    finally:
        connection.close()
    yield dsn
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for schema in ("app", "strategy_pkg", "selection", "paper_v2", "trading_core", "market"):
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    finally:
        connection.close()


def _conn_factory(dsn: str):
    @contextmanager
    def connect() -> Iterator[Any]:
        connection = psycopg2.connect(dsn)
        connection.autocommit = False
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return connect


class _Calendar:
    @staticmethod
    def next_trading_day(anchor_date: date, *, inclusive: bool = False) -> date:
        return anchor_date if inclusive else date.fromordinal(anchor_date.toordinal() + 1)


class _ForbiddenSelection:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected Selection dependency during Program provisioning: {name}")


def _program_spec(*, program_id: str, package_id: str, alpha_mode: AlphaMode, runtime_config: dict[str, Any]) -> HistoricalProgramSpec:
    return HistoricalProgramSpec(
        program_id=program_id,
        program_name=f"O3 L2 {program_id}",
        package_id=package_id,
        alpha_mode=alpha_mode,
        style="postgres_l2",
        target_count=5,
        review_policy={},
        runtime_config=runtime_config,
        created_by="o3_postgres_l2",
    )


class _PreparedArtifactService:
    """Replace only external model execution; Selection and DSE production stay real."""

    def __init__(
        self,
        *,
        package_repository: StrategyPackageRepository,
        artifact_repository: StrategyPackageSelectionArtifactRepository,
    ) -> None:
        self.package_repository = package_repository
        self.artifact_repository = artifact_repository

    def generate_from_live_inference(
        self,
        *,
        package_id: str,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        cutoff_date: date | None = None,
        **_kwargs: Any,
    ) -> Any:
        manifest = self.package_repository.get(package_id).current_manifest()
        runtime_hash = selection_artifact_runtime_hash_v2_for_manifest(manifest, runtime_config)
        try:
            return self.artifact_repository.get(
                package_id=package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
            )
        except DataUnavailableError:
            pass

        _context, _base_manifest, artifact, _trace, _base_runtime_config, _selected = _dynamic_fixture()
        decision_date = cutoff_date or trade_date
        observed_at = artifact.metadata["asset_closure"][0]["first_observed_at"]
        source_receipts = list(artifact.metadata["source_read_receipts"])
        calendar_payload = [
            {"cal_date": decision_date, "is_trading": True},
            {"cal_date": trade_date, "is_trading": True},
        ]
        input_context = {
            **artifact.metadata["artifact_input_context"],
            "requested_trade_date": trade_date.isoformat(),
            "effective_trade_date": decision_date.isoformat(),
            "cutoff_date": decision_date.isoformat(),
            "score_trade_date": decision_date.isoformat(),
            "reference_price_trade_date": decision_date.isoformat(),
            "calendar_hash": canonical_evidence_json_sha256(calendar_payload),
        }
        asset_closure = [
            {
                "asset_role": "strategy_package_manifest",
                "asset_id": manifest.package_id,
                "asset_ref": None,
                "sha256": manifest.manifest_sha256,
                "first_observed_at": observed_at,
                "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
            }
        ]
        metadata = {
            **artifact.metadata,
            "source_type": (
                LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
                if manifest.alpha_mode.value == "multi_alpha"
                else AUTHORITATIVE_SELECTION_SOURCE_TYPE
            ),
            "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            "artifact_input_context": input_context,
            "source_read_receipts": source_receipts,
            "asset_closure": asset_closure,
        }
        if manifest.alpha_mode.value == "multi_alpha":
            component_ids = {
                component.alpha_id: f"ssa_leg_{component.alpha_id}_{uuid4().hex}"
                for component in manifest.alpha_components
            }
            component_hashes = {
                component.alpha_id: canonical_evidence_json_sha256(
                    {"alpha_id": component.alpha_id, "manifest_sha256": manifest.manifest_sha256}
                )
                for component in manifest.alpha_components
            }
            metadata.update(
                {
                    "component_score_artifact_ids": component_ids,
                    "component_score_artifact_sha256": component_hashes,
                    "weight_artifact_id": f"weight_{uuid4().hex}",
                    "weight_artifact_sha256": canonical_evidence_json_sha256(
                        manifest.alpha_combination_policy.weights
                    ),
                    "combined_score_artifact_sha256": canonical_evidence_json_sha256(
                        {"component_hashes": component_hashes, "weights": manifest.alpha_combination_policy.weights}
                    ),
                    "multi_alpha_parent_parity_hash": canonical_evidence_json_sha256(
                        {"package_id": manifest.package_id, "component_ids": component_ids}
                    ),
                    "multi_alpha_parent_parity": {
                        "package_id": manifest.package_id,
                        "component_count": len(manifest.alpha_components),
                        "deterministic_replay": True,
                    },
                    "component_artifacts": {
                        component.alpha_id: {
                            "artifact_id": component_ids[component.alpha_id],
                            "artifact_sha256": component_hashes[component.alpha_id],
                            "model_ref": component.model_ref,
                        }
                        for component in manifest.alpha_components
                    },
                    "weights": manifest.alpha_combination_policy.weights,
                }
            )
        isolated = artifact.model_copy(
            update={
                "artifact_id": f"ssa_o3_l2_{uuid4().hex}",
                "package_id": manifest.package_id,
                "manifest_sha256": manifest.manifest_sha256,
                "trade_date": trade_date,
                "data_source": data_source,
                "runtime_config_hash": runtime_hash,
                "metadata": metadata,
                "artifact_input_context_hash": canonical_evidence_json_sha256(input_context),
                "source_revision_set_hash": artifact.source_revision_set_hash,
                "asset_closure_hash": canonical_evidence_json_sha256(
                    [{key: value for key, value in asset_closure[0].items() if key != "first_observed_at"}]
                ),
                "artifact_payload_sha256": None,
            }
        )
        return self.artifact_repository.save(isolated)


class _PackageHealth:
    @staticmethod
    def summarize(_record: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"status": "READY", "source": "o3_postgres_l2"}


class _ResultEnrichment:
    @staticmethod
    def enrich_candidates(candidates: list[Any], **_kwargs: Any) -> list[Any]:
        return list(candidates)


def _selection_components(
    *,
    conn_factory: Any,
    package_repository: StrategyPackageRepository,
    program_repository: AdvisoryProgramPGRepository,
    program_service: AdvisoryProgramService,
) -> SimpleNamespace:
    artifact_repository = StrategyPackageSelectionArtifactRepository(conn_factory=conn_factory)
    artifact_service = _PreparedArtifactService(
        package_repository=package_repository,
        artifact_repository=artifact_repository,
    )
    runtime = StrategyPackageRuntime(artifact_repository=artifact_repository)
    tradability = TradabilityFilter()
    risk_policy = StockRiskPolicyService()
    health = _PackageHealth()
    signal_service = DailySelectionSignalService(runtime=runtime, selection_artifact_service=artifact_service)
    selection_repository = SimulationRuntimeRepository(conn_factory=conn_factory)
    strategy_selection = StrategyPackageSelectionService(
        package_repository=package_repository,
        runtime=runtime,
        tradability_filter=tradability,
        refresh_audit=SimpleNamespace(require_success=lambda **_kwargs: None),
        selection_artifact_service=artifact_service,
        calendar_provider=_Calendar(),
        risk_policy_service=risk_policy,
        package_health_service=health,
        repository=selection_repository,
        signal_service=signal_service,
        phase1_trace_capture_service=Phase1TraceCaptureService(),
    )
    selection_center = SelectionCenterService(
        package_repository=package_repository,
        repository=SelectionCenterRepository(conn_factory=conn_factory),
        runtime=runtime,
        tradability_filter=tradability,
        refresh_audit=SimpleNamespace(require_success=lambda **_kwargs: None),
        paper_portfolio_service=HistoricalResearchExecutionProhibitedPortfolioService(),
        selection_artifact_service=artifact_service,
        calendar_provider=_Calendar(),
        risk_policy_service=risk_policy,
        package_health_service=health,
        strategy_selection_service=strategy_selection,
        result_enrichment_service=_ResultEnrichment(),
    )
    return SimpleNamespace(
        conn_factory=conn_factory,
        program_repository=program_repository,
        program_service=program_service,
        selection_center=selection_center,
        selection_service=strategy_selection,
        artifact_service=artifact_service,
        artifact_repository=artifact_repository,
        program_resolver=PostgresHistoricalResearchProgramResolver(conn_factory=conn_factory),
        evidence_adapter=PersistedHistoricalSelectionEvidenceAdapter(conn_factory=conn_factory),
        calendar_service=_Calendar(),
    )


def test_o3_program_dse_and_dual_track_historical_retry_use_real_postgres(postgres_dsn: str) -> None:
    conn_factory = _conn_factory(postgres_dsn)
    decision_date = date(2026, 7, 10)
    requested_at = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
    with conn_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO market.trading_calendar (cal_date, is_trading) VALUES (%s, TRUE), (%s, TRUE)",
                (decision_date, date.fromordinal(decision_date.toordinal() + 1)),
            )

    package_repository = StrategyPackageRepository(conn_factory=conn_factory)
    single_manifest = freeze_manifest(_single_manifest(f"o3_single_{uuid4().hex}"))
    child_a = freeze_manifest(_single_manifest(f"o3_child_a_{uuid4().hex}"))
    child_b = freeze_manifest(_single_manifest(f"o3_child_b_{uuid4().hex}"))
    package_repository.save_manifest(single_manifest)
    package_repository.save_manifest(child_a)
    package_repository.save_manifest(child_b)
    multi_source_manifest = _multi_manifest(
        f"o3_multi_{uuid4().hex}",
        child_a.alpha_components[0],
        child_b.alpha_components[0],
    )
    multi_source_manifest = multi_source_manifest.model_copy(
        update={
            "backtest_context": {
                **(multi_source_manifest.backtest_context or {}),
                "daily_strategy": {"topk": 5},
            }
        }
    )
    multi_record, _components = StrategyPackageComponentService(repository=package_repository).create_multi_alpha_package(
        manifest=multi_source_manifest,
        components=[
            {"child_package_id": child_a.package_id, "component_weight": 0.6, "score_normalization": "rank", "position": 1},
            {"child_package_id": child_b.package_id, "component_weight": 0.4, "score_normalization": "zscore", "position": 2},
        ],
    )
    multi_manifest = multi_record.current_manifest()

    runtime_config = {
        "runtime_profile": {
            "selection": {"top_k": 5},
            "hmm": {"enabled": False},
            "risk_policy": {"enabled": False},
            "tradability": {"exclude_suspended": False},
            "industry_blacklist": [],
        },
        "selection_artifact_config": {
            "auto_generate": True,
            "inference_backend": "wsl",
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
        },
    }
    program_repository = AdvisoryProgramPGRepository(conn_factory=conn_factory)
    program_service = AdvisoryProgramService(
        repository=program_repository,
        selection_service=_ForbiddenSelection(),
        calendar_provider=_Calendar(),
        symbol_name_resolver=SimpleNamespace(resolve=lambda symbol: symbol),
        now_provider=lambda: datetime(2026, 7, 1, 8, 0, tzinfo=UTC),
    )
    onboarding_service = RealDevHistoricalOnboardingService(now_provider=lambda: datetime(2026, 7, 1, 8, 0, tzinfo=UTC))
    components = SimpleNamespace(program_repository=program_repository, program_service=program_service)
    single_spec = _program_spec(
        program_id=f"advp_o3_single_{uuid4().hex}",
        package_id=single_manifest.package_id,
        alpha_mode=AlphaMode.SINGLE,
        runtime_config=runtime_config,
    )
    multi_spec = _program_spec(
        program_id=f"advp_o3_multi_{uuid4().hex}",
        package_id=multi_manifest.package_id,
        alpha_mode=AlphaMode.MULTI,
        runtime_config=runtime_config,
    )
    single_program, single_binding = onboarding_service._ensure_program(
        spec=single_spec,
        effective_from=decision_date,
        components=components,
    )
    multi_program, multi_binding = onboarding_service._ensure_program(
        spec=multi_spec,
        effective_from=decision_date,
        components=components,
    )
    single_retry, single_binding_retry = onboarding_service._ensure_program(
        spec=single_spec,
        effective_from=decision_date,
        components=components,
    )
    assert single_retry.program_id == single_program.program_id
    assert single_binding_retry.binding_version_id == single_binding.binding_version_id

    producer_components = _selection_components(
        conn_factory=conn_factory,
        package_repository=package_repository,
        program_repository=program_repository,
        program_service=program_service,
    )
    exact_request = SimpleNamespace(
        binding_effective_from_trade_date=decision_date,
        decision_trade_date=decision_date,
        policy_registry_id="o3_l2_policy",
        policy_registry_version="v1",
        policy_registry_hash="9" * 64,
        code_release_id="c" * 40,
        code_release_hash="d" * 64,
    )
    evidence_service = RealDevHistoricalOnboardingService(now_provider=lambda: requested_at)
    single_evidence, single_selection_run_id = evidence_service._ensure_prospective_evidence(  # noqa: SLF001
        request=exact_request,
        spec=single_spec,
        program=single_program,
        binding=single_binding,
        components=producer_components,
    )
    assert single_evidence.evidence_id
    assert single_selection_run_id and single_selection_run_id.startswith("sel_")
    runner = HistoricalAdvisoryResearchRunner(
        repository=PostgresHistoricalResearchRepository(conn_factory=conn_factory),
        trading_date_resolver=PostgresHistoricalResearchTradingDateResolver(conn_factory=conn_factory),
        program_resolver=PostgresHistoricalResearchProgramResolver(conn_factory=conn_factory),
        evidence_adapter=PersistedHistoricalSelectionEvidenceAdapter(conn_factory=conn_factory),
        now_provider=lambda: requested_at,
    )
    request = HistoricalResearchBatchRequest(
        decision_trade_date=decision_date,
        program_ids=[single_program.program_id, multi_program.program_id],
        requested_at=requested_at,
    )
    first = runner.run(request)
    first_by_program = {item.program_id: item for item in first.program_runs}
    assert first_by_program[single_program.program_id].status is HistoricalResearchRunStatus.COMPLETE
    assert first_by_program[multi_program.program_id].status is HistoricalResearchRunStatus.WAITING_INPUT

    multi_evidence, multi_selection_run_id = evidence_service._ensure_prospective_evidence(  # noqa: SLF001
        request=exact_request,
        spec=multi_spec,
        program=multi_program,
        binding=multi_binding,
        components=producer_components,
    )
    assert multi_evidence.evidence_id
    assert multi_selection_run_id and multi_selection_run_id.startswith("sel_")
    completed = runner.run(request)
    completed_by_program = {item.program_id: item for item in completed.program_runs}
    assert completed.status is HistoricalResearchRunStatus.COMPLETE
    assert {item.status for item in completed.program_runs} == {HistoricalResearchRunStatus.COMPLETE}
    assert completed_by_program[single_program.program_id].program_run_id == first_by_program[single_program.program_id].program_run_id
    assert completed_by_program[multi_program.program_id].program_run_id == first_by_program[multi_program.program_id].program_run_id

    exact_retry = runner.run(request)
    assert exact_retry.receipt_id == completed.receipt_id
    assert exact_retry.receipt_hash == completed.receipt_hash
    assert [item.program_run_id for item in exact_retry.program_runs] == [
        item.program_run_id for item in completed.program_runs
    ]
