from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest

from backend.services.advisory_dev_input_onboarding.contracts import AlphaMode, HistoricalProgramSpec
from backend.services.advisory_dev_input_onboarding.historical_onboarding import (
    ExactDevSymbolNameResolver,
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
from backend.services.selection_center.package_health import SelectionPackageHealthService
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.selection_center.result_enrichment import SelectionResultEnrichmentService
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.simulation_runtime.selection import DailySelectionSignalService, StrategyPackageSelectionService
from backend.services.strategy_package.live_inference import (
    LiveInferenceResult,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import ModelAsset, RuntimeAssetManifest
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
    StrategyPackageSelectionArtifactRepository,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.tests.strategy_package.test_multi_alpha_base_schema import _multi_manifest, _single_manifest


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
    admin_dsn = os.getenv("ADVISORY_O3_TEST_DSN")
    if not admin_dsn:
        pytest.skip("ADVISORY_O3_TEST_DSN is not configured for disposable PostgreSQL")
    database_name = f"aistock_o3_l2_{uuid4().hex}"
    admin_connection = psycopg2.connect(admin_dsn)
    admin_connection.autocommit = True
    try:
        if admin_connection.server_version < 160000:
            pytest.fail(f"Advisory O3 L2 requires PostgreSQL 16+, got {admin_connection.server_version}")
        with admin_connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        dsn = make_dsn(admin_dsn, dbname=database_name)
        connection = psycopg2.connect(dsn)
        try:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA app")
                cursor.execute("CREATE TABLE app.watchlist_items (id BIGSERIAL PRIMARY KEY)")
                cursor.execute("CREATE SCHEMA market")
                cursor.execute(
                    "CREATE TABLE market.trading_calendar (cal_date DATE PRIMARY KEY, is_trading BOOLEAN NOT NULL)"
                )
                cursor.execute(
                    """
                    CREATE TABLE market.kline_daily_raw (
                        ts_code TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        close_li BIGINT NOT NULL,
                        volume_hand BIGINT NOT NULL,
                        PRIMARY KEY (ts_code, trade_date)
                    )
                    """
                )
                cursor.execute("CREATE TABLE market.stock_basic (ts_code TEXT PRIMARY KEY, name TEXT NOT NULL)")
                for migration in O3_MIGRATION_CHAIN:
                    cursor.execute(migration.read_text(encoding="utf-8-sig"))
            connection.commit()
        finally:
            connection.close()
        yield dsn
    finally:
        with admin_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (database_name,),
            )
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))
        admin_connection.close()


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


class _DeterministicRuntimeAssetResolver:
    """Stand in only for the external immutable model-asset workspace boundary."""

    def __init__(self, root: Path) -> None:
        self.root = root

    @staticmethod
    def load_source_for_strategy_package(
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None = None,
        run_id: str | None = None,
        manifest: Any | None = None,
        package_id: str | None = None,
    ) -> Any:
        resolved_package_id = package_id or getattr(manifest, "package_id", None) or source_id
        return SimpleNamespace(
            experiment_id=f"o3:{resolved_package_id}",
            qe_task_id="o3_l2",
            qe_loop_id=loop_id or run_id or "single",
            source_type=source_type,
        )

    @staticmethod
    def load_source_for_strategy_package_leg(**kwargs: Any) -> Any:
        return SimpleNamespace(
            experiment_id=f"o3:{kwargs['package_id']}:{kwargs['leg_id']}",
            qe_task_id="o3_l2",
            qe_loop_id=str(kwargs["leg_id"]),
            leg_id=str(kwargs["leg_id"]),
        )

    def prepare_workspace(self, **kwargs: Any) -> Any:
        artifact_config = dict(kwargs.get("runtime_config", {}).get("selection_artifact_config") or {})
        leg_id = str(artifact_config.get("multi_alpha_leg_id") or "single")
        workspace = self.root / leg_id
        return SimpleNamespace(
            workspace_path=workspace,
            factor_order_path=workspace / "factor_order.json",
            factor_entry_path=workspace / "factor_entry.py",
            model_params_path=workspace / "params.pkl",
            model_source_path=workspace / "source_params.pkl",
            factor_source_dir=workspace / "factors",
            factor_order=["factor_a", "factor_b"],
            alpha158_factors=[],
            dynamic_factors=["factor_a", "factor_b"],
            model_candidate_count=1,
            model_params_origin="package_asset",
            leg_id=leg_id,
            seed_run_id=str(artifact_config.get("multi_alpha_seed_run_id") or "single_seed"),
        )


class _DeterministicLiveInferenceProvider:
    """Return deterministic model output while preserving the real artifact producer."""

    backend_name = "o3_l2_deterministic"

    def __init__(self, *, observed_at: datetime) -> None:
        self.observed_at = observed_at

    def run(self, **kwargs: Any) -> LiveInferenceResult:
        requested_trade_date = kwargs["trade_date"]
        effective_trade_date = kwargs.get("cutoff_date") or requested_trade_date
        leg_id = str(getattr(kwargs["workspace"], "leg_id", "single"))
        symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
        reverse = leg_id not in {"single", "alpha_1"} and leg_id.endswith("2")
        values = list(range(1, 6)) if reverse else list(range(5, 0, -1))
        calendar_source = "market.trading_calendar"
        calendar_identity_hash = canonical_evidence_json_sha256(
            {
                "dataset_id": calendar_source,
                "effective_trade_date": effective_trade_date.isoformat(),
                "calendar_version": "market.trading_calendar.v1",
                "calendar_source": calendar_source,
            }
        )
        window = {
            "window_start_date": date.fromordinal(effective_trade_date.toordinal() - 120).isoformat(),
            "required_window": 60,
            "window_resolution": "model_required_window",
        }
        window_hash = canonical_evidence_json_sha256({"calendar_identity_hash": calendar_identity_hash, **window})
        input_context = {
            "requested_trade_date": requested_trade_date.isoformat(),
            "effective_trade_date": effective_trade_date.isoformat(),
            "cutoff_date": effective_trade_date.isoformat(),
            "score_trade_date": effective_trade_date.isoformat(),
            "reference_price_trade_date": effective_trade_date.isoformat(),
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "calendar_version": "market.trading_calendar.v1",
            "calendar_identity_hash": calendar_identity_hash,
            "calendar_hash": window_hash,
            "calendar_source": calendar_source,
            "window_lineage_hash": window_hash,
            "universe_input_hash": canonical_evidence_json_sha256(symbols),
            **window,
        }
        source_specs = (
            ("pit_universe", "market.stock_universe_pit", "1" * 64),
            ("market_history", "market.kline_daily_raw", "2" * 64),
            ("fundamental_moneyflow", "timescaledb.fundamental_moneyflow", "3" * 64),
            ("trading_calendar", calendar_source, window_hash),
        )
        return LiveInferenceResult(
            scores=[
                {"symbol": symbol, "score": float(score), "rank": rank}
                for rank, (symbol, score) in enumerate(zip(symbols, values, strict=True), start=1)
            ],
            metadata={"provider": self.backend_name, "leg_id": leg_id},
            universe_count=len(symbols),
            source_read_receipts=[
                {
                    "source_role": role,
                    "dataset_id": dataset_id,
                    "row_count": len(symbols),
                    "content_hash": content_hash,
                    "first_observed_at": self.observed_at,
                }
                for role, dataset_id, content_hash in source_specs
            ],
            input_context=input_context,
        )


class _UnexpectedRefreshAudit:
    @staticmethod
    def require_success(**kwargs: Any) -> None:
        raise AssertionError(f"unexpected data refresh audit in O3 L2: {kwargs}")


def _with_complete_asset_closure(manifest: Any) -> Any:
    raw_models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    models = [
        model.model_copy(
            update={
                "asset_ref": f"o3-l2://model/{model.model_id}",
                "sha256": canonical_evidence_json_sha256({"package_id": manifest.package_id, "model_id": model.model_id}),
            }
        )
        for model in raw_models
    ]
    factors = [
        factor.model_copy(
            update={
                "asset_ref": f"o3-l2://factor/{factor.factor_id}",
                "sha256": canonical_evidence_json_sha256(
                    {"package_id": manifest.package_id, "factor_id": factor.factor_id}
                ),
            }
        )
        for factor in manifest.factor_set
    ]
    return manifest.model_copy(
        update={
            "model_asset": models if isinstance(manifest.model_asset, list) else models[0],
            "factor_set": factors,
        }
    )


def _native_multi_manifest(manifest: Any) -> Any:
    completed = _with_complete_asset_closure(manifest)
    first, second = completed.alpha_components
    first = first.model_copy(
        update={
            "model_id": "model_a",
            "model_ref": "model_a",
            "lineage": first.lineage.model_copy(update={"model_artifact_ref": "parent_package_asset:model_id:model_a"}),
        }
    )
    second = second.model_copy(
        update={
            "model_id": "model_b",
            "model_ref": "model_b",
            "lineage": second.lineage.model_copy(update={"model_artifact_ref": "parent_package_asset:model_id:model_b"}),
        }
    )
    model_assets = [
        ModelAsset(
            model_id=model_id,
            model_ref=model_id,
            asset_ref=f"o3-l2://model/{model_id}",
            sha256=canonical_evidence_json_sha256({"package_id": completed.package_id, "model_id": model_id}),
        )
        for model_id in ("model_a", "model_b")
    ]
    weights = {first.alpha_id: float(first.component_weight), second.alpha_id: float(second.component_weight)}
    return freeze_manifest(
        completed.model_copy(
            update={
                "alpha_components": [first, second],
                "model_asset": model_assets,
                "runtime_assets": RuntimeAssetManifest(),
                "source_evidence": {
                    "authority": "parent_package_asset_runtime_authority",
                    "multi_alpha": {
                        "combine_backtest_run_id": f"o3-l2:{completed.package_id}",
                        "legs": [
                            {"leg_id": first.alpha_id, "seed_run_ids": ["seed_a"], "terminal_weight": weights[first.alpha_id]},
                            {"leg_id": second.alpha_id, "seed_run_ids": ["seed_b"], "terminal_weight": weights[second.alpha_id]},
                        ],
                        "terminal_weights": weights,
                        "weight_policy": {"mode": "frozen_backtest_terminal_weights"},
                    },
                },
                "backtest_context": {
                    **(completed.backtest_context or {}),
                    "daily_strategy": {"topk": 5},
                    "weight_policy": {"mode": "frozen_backtest_terminal_weights"},
                },
                "manifest_sha256": None,
            }
        )
    )


def _selection_components(
    *,
    conn_factory: Any,
    package_repository: StrategyPackageRepository,
    program_repository: AdvisoryProgramPGRepository,
    program_service: AdvisoryProgramService,
    runtime_workspace_root: Path,
    observed_at: datetime,
) -> SimpleNamespace:
    artifact_repository = StrategyPackageSelectionArtifactRepository(conn_factory=conn_factory)
    runtime_asset_resolver = _DeterministicRuntimeAssetResolver(runtime_workspace_root)
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repository,
        artifact_repository=artifact_repository,
        runtime_asset_resolver=runtime_asset_resolver,
        live_inference_provider=_DeterministicLiveInferenceProvider(observed_at=observed_at),
        conn_factory=conn_factory,
    )
    runtime = StrategyPackageRuntime(artifact_repository=artifact_repository)
    tradability = TradabilityFilter()
    risk_policy = StockRiskPolicyService()
    health = SelectionPackageHealthService(
        artifact_repository=artifact_repository,
        runtime_source_resolver=runtime_asset_resolver,
    )
    calendar_service = TradingCalendarStatusService(conn_factory=conn_factory)
    refresh_audit = _UnexpectedRefreshAudit()
    signal_service = DailySelectionSignalService(runtime=runtime, selection_artifact_service=artifact_service)
    selection_repository = SimulationRuntimeRepository(conn_factory=conn_factory)
    strategy_selection = StrategyPackageSelectionService(
        package_repository=package_repository,
        runtime=runtime,
        tradability_filter=tradability,
        refresh_audit=refresh_audit,
        selection_artifact_service=artifact_service,
        calendar_provider=calendar_service,
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
        refresh_audit=refresh_audit,
        paper_portfolio_service=HistoricalResearchExecutionProhibitedPortfolioService(),
        selection_artifact_service=artifact_service,
        calendar_provider=calendar_service,
        risk_policy_service=risk_policy,
        package_health_service=health,
        strategy_selection_service=strategy_selection,
        result_enrichment_service=SelectionResultEnrichmentService(
            conn_factory=conn_factory,
            symbol_name_resolver=ExactDevSymbolNameResolver(conn_factory),
            quote_fetcher=lambda _symbol: None,
            today_provider=lambda: date.max,
        ),
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
        calendar_service=calendar_service,
    )


def test_o3_program_dse_and_dual_track_historical_retry_use_real_postgres(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    conn_factory = _conn_factory(postgres_dsn)
    wall_clock = datetime.now(UTC)
    decision_date = wall_clock.astimezone(ZoneInfo("Asia/Shanghai")).date()
    target_date = date.fromordinal(decision_date.toordinal() + 1)
    observed_at = wall_clock - timedelta(minutes=1)
    generated_at = wall_clock + timedelta(minutes=2)
    requested_at = generated_at + timedelta(days=2)
    symbols = [f"{index:06d}.SZ" for index in range(1, 6)]
    with conn_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO market.trading_calendar (cal_date, is_trading) VALUES (%s, TRUE), (%s, TRUE)",
                (decision_date, target_date),
            )
            cursor.executemany(
                "INSERT INTO market.kline_daily_raw (ts_code, trade_date, close_li, volume_hand) VALUES (%s, %s, %s, %s)",
                [(symbol, decision_date, 10_000 + index * 100, 1_000 + index) for index, symbol in enumerate(symbols)],
            )
            cursor.executemany(
                "INSERT INTO market.stock_basic (ts_code, name) VALUES (%s, %s)",
                [(symbol, f"O3 Stock {index}") for index, symbol in enumerate(symbols, start=1)],
            )

    package_repository = StrategyPackageRepository(conn_factory=conn_factory)
    single_manifest = freeze_manifest(_with_complete_asset_closure(_single_manifest(f"o3_single_{uuid4().hex}")))
    child_a = freeze_manifest(_with_complete_asset_closure(_single_manifest(f"o3_child_a_{uuid4().hex}")))
    child_b = freeze_manifest(_with_complete_asset_closure(_single_manifest(f"o3_child_b_{uuid4().hex}")))
    package_repository.save_manifest(single_manifest)
    package_repository.save_manifest(child_a)
    package_repository.save_manifest(child_b)
    multi_source_manifest = _multi_manifest(
        f"o3_multi_{uuid4().hex}",
        child_a.alpha_components[0],
        child_b.alpha_components[0],
    )
    multi_manifest = _native_multi_manifest(multi_source_manifest)
    package_repository.save_manifest(multi_manifest)

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
        now_provider=lambda: generated_at - timedelta(days=7),
    )
    onboarding_service = RealDevHistoricalOnboardingService(now_provider=lambda: generated_at - timedelta(days=7))
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
        runtime_workspace_root=tmp_path / "runtime-workspaces",
        observed_at=observed_at,
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
    evidence_service = RealDevHistoricalOnboardingService(now_provider=lambda: generated_at)
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
