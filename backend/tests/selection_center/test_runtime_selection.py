from __future__ import annotations

import io
import json
import runpy
import tarfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.industry_provider import IndustryInfo
from backend.services.selection_center.models import SelectionMode, SelectionRunStatus
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.risk_policy import RiskDecision, StockRiskPolicyService
from backend.services.selection_center.runtime_profile import runtime_profile_config_sha256, validate_runtime_profile_binding
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy, SourceType, StrategyPackageSource
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime, TargetPositionEngine
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind, RuntimeVariantValidationStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    QEExperimentRuntimeAssetResolver,
    QEExperimentRuntimeSource,
    LiveInferenceResult,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)
from backend.services.trading_core.errors import (
    DataUnavailableError,
    HMMRuntimeUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    UnsupportedFeatureError,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class NoopRefreshAudit:
    def require_success(self, **_kwargs):
        return None


class NoopSelectionResultEnrichment:
    def enrich_candidates(self, candidates, *, trade_date, runtime_config=None):
        return list(candidates)


class FixedEntryPriceEnrichment:
    def __init__(self, entry_prices: dict[str, float]) -> None:
        self.entry_prices = entry_prices

    def enrich_candidates(self, candidates, *, trade_date, runtime_config=None):
        enriched = []
        for candidate in candidates:
            entry_price = self.entry_prices.get(candidate.symbol, candidate.reference_price)
            enriched.append(
                candidate.model_copy(
                    update={
                        "selection_entry_price": entry_price,
                        "current_price": 999.0,
                        "reference_price": entry_price,
                    }
                )
            )
        return enriched


class RecordingRefreshAudit:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def require_success(self, **kwargs):
        self.calls.append(kwargs)
        return None


class FakeSuspendLookup:
    def __init__(self, suspended: set[str] | None = None) -> None:
        self.suspended = suspended or set()

    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict]:
        return {
            symbol: {"source": "market.suspend_d", "suspend_type": "S", "suspend_timing": None}
            for symbol in symbols
            if symbol in self.suspended
        }


class FakeIndustryLookup:
    def __init__(self, industries: dict[str, IndustryInfo] | None = None, *, fail: bool = False) -> None:
        self.industries = industries or {}
        self.fail = fail

    def get_industries(self, symbols: list[str], trade_date: date) -> dict[str, IndustryInfo]:
        if self.fail:
            raise DataUnavailableError("industry provider failed")
        return {symbol: self.industries[symbol] for symbol in symbols if symbol in self.industries}


class RecordingRiskPolicyService(StockRiskPolicyService):
    def __init__(self, decisions: dict[str, RiskDecision] | None = None) -> None:
        self.decisions = decisions or {}
        self.profile_seen = None

    def evaluate(self, *, symbols, trade_date, profile, current_positions=None):  # type: ignore[override]
        self.profile_seen = profile
        return {symbol: self.decisions.get(symbol, RiskDecision(symbol=symbol)) for symbol in symbols}


class FakeHMMSnapshotProvider:
    def __init__(self, snapshots: dict[str, dict] | None = None) -> None:
        self.snapshots = snapshots or {}

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        return self.snapshots.get(snapshot_id)


TEST_ARTIFACT_REPO = InMemorySelectionScoreArtifactRepository()


def versioned_selection_runtime_config(
    config: dict | None = None,
    *,
    version_id: str | None = None,
) -> dict:
    payload = dict(config or {})
    if version_id is None:
        version_id = f"unit_runtime_profile_{runtime_profile_config_sha256(payload)[:12]}"
    payload["runtime_profile_binding"] = {
        "source": "selection_runtime_profile_version",
        "profile_version_id": version_id,
        "config_sha256": runtime_profile_config_sha256(payload),
        "trade_enabled": True,
    }
    return payload


def non_trading_preview_runtime_config(config: dict | None = None) -> dict:
    payload = dict(config or {})
    payload["runtime_config_scope"] = "non_trading_preview"
    return payload


@pytest.fixture(autouse=True)
def use_authoritative_artifact_repo(monkeypatch):
    """Unit tests seed authoritative artifacts instead of using inline scores."""

    global TEST_ARTIFACT_REPO
    TEST_ARTIFACT_REPO = InMemorySelectionScoreArtifactRepository()
    original_init = StrategyPackageRuntime.__init__

    def patched_init(self, validator=None, hmm_runtime=None, artifact_repository=None):
        original_init(
            self,
            validator=validator,
            hmm_runtime=hmm_runtime,
            artifact_repository=artifact_repository or TEST_ARTIFACT_REPO,
        )

    monkeypatch.setattr(StrategyPackageRuntime, "__init__", patched_init)
    yield


def seed_test_authoritative_artifact(
    manifest,
    rows: list[dict],
    *,
    trade_dates: list[date] | None = None,
    data_source: str = "DB_HISTORICAL",
    runtime_config: dict | None = None,
) -> None:
    for trade_date in trade_dates or [date(2024, 1, 2), date(2024, 1, 3)]:
        sorted_rows = sorted(rows, key=lambda item: int(item["rank"]))
        TEST_ARTIFACT_REPO.save(
            SelectionScoreArtifact(
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or "",
                trade_date=trade_date,
                data_source=data_source,
                runtime_config_hash=selection_artifact_runtime_hash(runtime_config or {}),
                scores_json=sorted_rows,
                score_count=len(sorted_rows),
                universe_count=len(sorted_rows),
                top_score_symbol=sorted_rows[0]["symbol"] if sorted_rows else None,
                metadata={
                    "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                    "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                    "test_seeded": True,
                },
            )
        )


def ready_manifest_with_scores(package_name: str, symbol: str, score: float, rank: int):
    rows = [
        {
            "symbol": symbol,
            "score": score,
            "rank": rank,
            "target_weight": 0.03,
            "reference_price": 10.0,
        }
    ]
    manifest = make_manifest().model_copy(
        update={
            "package_name": package_name,
            "package_status": PackageStatus.SELECTION_ENABLED,
            "strategy_config": {
                "strategy_id": package_name,
                "selection_runtime": {"scores": rows},
            },
        }
    )
    frozen = freeze_manifest(manifest)
    seed_test_authoritative_artifact(frozen, rows)
    return frozen


def ready_manifest_with_score_rows(package_name: str, rows: list[dict]):
    manifest = make_manifest().model_copy(
        update={
            "package_name": package_name,
            "package_status": PackageStatus.SELECTION_ENABLED,
            "strategy_config": {
                "strategy_id": package_name,
                "selection_runtime": {"scores": rows},
            },
        }
    )
    frozen = freeze_manifest(manifest)
    seed_test_authoritative_artifact(frozen, rows)
    return frozen


def st_pit_manifest_with_score_rows(
    package_name: str,
    rows: list[dict],
    *,
    topk: int = 2,
    hmm_custom_params: dict | None = None,
):
    custom_params = {
        "strategy_id": "score_weighted_topk_v2",
        "topk": topk,
        "n_drop": 1,
        "risk_policy": {
            "enabled": True,
            "providers": ["st_pit"],
            "st_universe_key": "shsz_st_pit_active_v1",
            "hard_actions": ["block_buy", "force_exit"],
            "strict_data_ready": True,
        },
    }
    if hmm_custom_params:
        custom_params.update(hmm_custom_params)
    manifest = make_manifest().model_copy(
        update={
            "package_name": package_name,
            "package_status": PackageStatus.SELECTION_ENABLED,
            "portfolio_policy": PortfolioPolicy(topk=topk, n_drop=1),
            "strategy_config": {
                "strategy_id": "score_weighted_topk_v2",
                "selection_runtime": {"scores": rows},
                "custom_params": custom_params,
            },
        }
    )
    frozen = freeze_manifest(manifest)
    seed_test_authoritative_artifact(frozen, rows)
    return frozen


def test_strategy_package_runtime_builds_signal_and_targets() -> None:
    manifest = ready_manifest_with_scores("pkg_a", "000001.SZ", 0.9, 1)
    snapshot = StrategyPackageRuntime().build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )
    targets = TargetPositionEngine().build_targets(snapshot=snapshot, total_equity=100_000, top_k=1)

    assert snapshot.candidates[0].symbol == "000001.SZ"
    assert targets[0].target_quantity == 300


def test_strategy_package_runtime_rejects_runtime_config_selection_scores() -> None:
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.SELECTION_ENABLED}))

    with pytest.raises(RuntimeConfigInvalidError, match="selection_scores cannot be used"):
        StrategyPackageRuntime().build_signal_snapshot(
            manifest=manifest,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={"selection_scores": [{"symbol": "000001.SZ", "rank": 1}]},
        )


def test_strategy_package_runtime_rejects_manifest_embedded_scores_without_artifact() -> None:
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {
                    "strategy_id": "pkg_manifest_scores",
                    "selection_runtime": {
                        "scores": [
                            {
                                "symbol": "000001.SZ",
                                "score": 0.9,
                                "rank": 1,
                                "target_weight": 0.03,
                                "reference_price": 10.0,
                            }
                        ]
                    },
                },
            }
        )
    )

    with pytest.raises(RuntimeConfigInvalidError, match="not authoritative"):
        StrategyPackageRuntime().build_signal_snapshot(
            manifest=manifest,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
        )


def test_strategy_package_runtime_loads_persisted_selection_artifact() -> None:
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_artifact_only"},
            }
        )
    )
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash({}),
            scores_json=[
                {
                    "symbol": "000001.SZ",
                    "score": 0.9,
                    "rank": 1,
                    "target_weight": 0.03,
                    "reference_price": 10.0,
                }
            ],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
        )
    )

    snapshot = StrategyPackageRuntime(artifact_repository=artifact_repo).build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    assert snapshot.candidates[0].symbol == "000001.SZ"
    assert snapshot.candidates[0].rank == 1


def test_selection_artifact_service_generates_qe_prediction_as_diagnostic_only(tmp_path) -> None:
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-02"), "000002.SZ"),
            (pd.Timestamp("2024-01-02"), "000001.SZ"),
            (pd.Timestamp("2024-01-02"), "000003.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    pred_path = tmp_path / "pred.pkl"
    pd.DataFrame({"score": [0.8, 0.9, 0.7]}, index=idx).to_pickle(pred_path)

    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_generated_artifact"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
    )

    artifact = artifact_service.generate_from_qe_prediction(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        source_path=str(pred_path),
        include_reference_price=False,
    )

    assert artifact.top_score_symbol == "000001.SZ"
    assert [row["symbol"] for row in artifact.scores_json[:3]] == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert artifact.scores_json[0]["target_weight"] == pytest.approx(1.0 / manifest.portfolio_policy.topk)
    assert artifact.metadata["authority_scope"] == "diagnostic_backtest_only"

    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(DataUnavailableError, match="not authoritative"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"runtime_profile": {"selection": {"top_k": 2}}}),
        )


def test_selection_artifact_service_generates_live_inference_artifact_without_execution_algo(tmp_path) -> None:
    class FakeResolver:
        def load_source(self, experiment_id: str):
            return {"experiment_id": experiment_id}

        def prepare_workspace(self, **_kwargs):
            class Prepared:
                workspace_path = tmp_path
                factor_order_path = tmp_path / "factor_order.json"
                factor_entry_path = tmp_path / "factor_entry.py"
                model_params_path = tmp_path / "params.pkl"
                model_source_path = tmp_path / "source_params.pkl"
                factor_source_dir = tmp_path / "factors"
                factor_order = ["f1", "f2"]
                alpha158_factors = []
                dynamic_factors = ["f1", "f2"]
                model_candidate_count = 1

            return Prepared()

    class FakeProvider:
        backend_name = "fake_live"

        def __init__(self) -> None:
            self.calls = []

        def run(self, **kwargs):
            self.calls.append(kwargs)
            return LiveInferenceResult(
                scores=[
                    {"symbol": "000002.SZ", "score": 0.8, "rank": 2},
                    {"symbol": "000001.SZ", "score": 0.9, "rank": 1},
                    {"symbol": "000003.SZ", "score": 0.7, "rank": 3},
                ],
                metadata={"provider": "fake"},
            )

    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_live_artifact"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    live_provider = FakeProvider()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=FakeResolver(),
        live_inference_provider=live_provider,
    )

    artifact = artifact_service.generate_from_live_inference(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        include_reference_price=False,
    )

    assert artifact.top_score_symbol == "000001.SZ"
    assert artifact.metadata["source_type"] == AUTHORITATIVE_SELECTION_SOURCE_TYPE
    assert artifact.metadata["authority_scope"] == AUTHORITATIVE_SELECTION_SCOPE
    assert [row["symbol"] for row in artifact.scores_json[:3]] == ["000001.SZ", "000002.SZ", "000003.SZ"]

    cutoff_artifact = artifact_service.generate_from_live_inference(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        cutoff_date=date(2024, 1, 2),
        include_reference_price=False,
    )
    assert cutoff_artifact.trade_date == date(2024, 1, 3)
    assert cutoff_artifact.metadata["cutoff_date"] == "2024-01-02"
    assert cutoff_artifact.metadata["score_trade_date"] == "2024-01-02"
    assert live_provider.calls[-1]["trade_date"] == date(2024, 1, 3)
    assert live_provider.calls[-1]["cutoff_date"] == date(2024, 1, 2)

    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"runtime_profile": {"selection": {"top_k": 2}}}),
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ", "000002.SZ"]


def test_selection_artifact_service_resolves_qe_evolution_loop_source(tmp_path) -> None:
    class FakeResolver:
        def __init__(self) -> None:
            self.source_calls: list[dict] = []

        def load_source_for_strategy_package(self, **kwargs):
            self.source_calls.append(kwargs)
            return {"experiment_id": "qe_task_L1"}

        def prepare_workspace(self, **_kwargs):
            class Prepared:
                workspace_path = tmp_path
                factor_order_path = tmp_path / "factor_order.json"
                factor_entry_path = tmp_path / "factor_entry.py"
                model_params_path = tmp_path / "params.pkl"
                model_source_path = tmp_path / "source_params.pkl"
                factor_source_dir = tmp_path / "factors"
                factor_order = ["factor_a"]
                alpha158_factors = []
                dynamic_factors = ["factor_a"]
                model_candidate_count = 1

            return Prepared()

    class FakeProvider:
        backend_name = "fake_live"

        def run(self, **_kwargs):
            return LiveInferenceResult(
                scores=[{"symbol": "000001.SZ", "score": 1.0, "rank": 1}],
                metadata={},
            )

    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "source": StrategyPackageSource(
                    source_type=SourceType.QE_EVOLUTION_LOOP,
                    source_id="qe_task",
                    loop_id="Loop1",
                    run_id="qe_task_L1",
                ),
            }
        )
    )
    package_repo.save_manifest(manifest)
    resolver = FakeResolver()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
        runtime_asset_resolver=resolver,
        live_inference_provider=FakeProvider(),
    )

    artifact = artifact_service.generate_from_live_inference(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        include_reference_price=False,
    )

    assert artifact.status.value == "SUCCEEDED"
    assert resolver.source_calls == [
        {
            "source_type": "qe_evolution_loop",
            "source_id": "qe_task",
            "loop_id": "Loop1",
            "run_id": "qe_task_L1",
        }
    ]


def test_selection_center_generates_binding_for_ad_hoc_runtime_config() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_scores("pkg_runtime_boundary", "000001.SZ", 0.9, 1)
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={"top_k": 1},
    )
    binding = validate_runtime_profile_binding(run.runtime_config)

    assert binding["source"] == "generated_effective_runtime_config"
    assert binding["config_sha256"] == runtime_profile_config_sha256(run.runtime_config)
    assert run.status == SelectionRunStatus.SUCCEEDED


def test_selection_center_non_trading_preview_is_marked_and_cannot_create_paper_portfolio() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = ready_manifest_with_scores("pkg_preview_boundary", "000001.SZ", 0.9, 1)
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        paper_portfolio_service=PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=paper_repo,
        ),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=non_trading_preview_runtime_config({"top_k": 1}),
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert run.runtime_config["runtime_config_scope"] == "non_trading_preview"
    assert run.runtime_config["runtime_profile_binding"]["source"] == "ad_hoc_non_trading_preview"
    assert run.runtime_config["runtime_profile_binding"]["trade_enabled"] is False
    with pytest.raises(InvalidStateTransitionError, match="non-trading"):
        service.create_paper_portfolio_from_run(
            run_id=run.run_id,
            portfolio_name="preview should not trade",
            initial_cash=100_000,
            start_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )


def test_selection_center_pit_mode_resolves_previous_trading_day_and_passes_cutoff(tmp_path) -> None:
    class FakeCalendar:
        def __init__(self) -> None:
            self.ensure_calls: list[date] = []

        def ensure_trading_day(self, trade_date: date) -> None:
            self.ensure_calls.append(trade_date)

        def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
            assert end_date == date(2024, 1, 2)
            return [date(2024, 1, 2)]

    class FakeResolver:
        def load_source(self, experiment_id: str):
            return {"experiment_id": experiment_id}

        def prepare_workspace(self, **_kwargs):
            class Prepared:
                workspace_path = tmp_path
                factor_order_path = tmp_path / "factor_order.json"
                factor_entry_path = tmp_path / "factor_entry.py"
                model_params_path = tmp_path / "params.pkl"
                model_source_path = tmp_path / "source_params.pkl"
                factor_source_dir = tmp_path / "factors"
                factor_order = ["f1"]
                alpha158_factors = []
                dynamic_factors = ["f1"]
                model_candidate_count = 1

            return Prepared()

        def require_preflight_or_raise(self, **_kwargs):
            # P0-F preflight stub — fake resolver always passes (test fixture
            # exercises happy path, not the cold-start failure surface).
            from backend.services.strategy_package.live_inference import (
                LiveInferencePreflightCheck,
                LiveInferencePreflightResult,
                PREFLIGHT_CHECK_NAMES,
                PREFLIGHT_STATUS_PASS,
            )

            return LiveInferencePreflightResult(
                passed=True,
                checks=[
                    LiveInferencePreflightCheck(
                        name=name,
                        status=PREFLIGHT_STATUS_PASS,
                        message="fake resolver preflight stub",
                    )
                    for name in PREFLIGHT_CHECK_NAMES
                ],
            )

    class FakeProvider:
        backend_name = "fake_live"

        def __init__(self) -> None:
            self.calls = []

        def run(self, **kwargs):
            self.calls.append(kwargs)
            return LiveInferenceResult(
                scores=[
                    {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "reference_price": 10.0},
                    {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "reference_price": 11.0},
                ],
                metadata={"provider": "fake"},
            )

    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_pit"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    provider = FakeProvider()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=FakeResolver(),
        live_inference_provider=provider,
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        selection_artifact_service=artifact_service,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        calendar_provider=FakeCalendar(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config(
            {
                "selection_artifact_config": {
                    "auto_generate": True,
                    "inference_backend": "local",
                    "include_reference_price": False,
                    "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
                },
                "runtime_profile": {"selection": {"top_k": 2}},
            }
        ),
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert run.runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-02"
    assert run.runtime_config["point_in_time_context"]["score_trade_date"] == "2024-01-02"
    assert provider.calls[-1]["trade_date"] == date(2024, 1, 3)
    assert provider.calls[-1]["cutoff_date"] == date(2024, 1, 2)
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ", "000002.SZ"]


def test_selection_center_pit_mode_maps_non_trading_date_to_latest_completed_day() -> None:
    class FakeCalendar:
        def ensure_trading_day(self, trade_date: date) -> None:
            raise DataUnavailableError("trade_date is not a trading day", context={"trade_date": trade_date.isoformat()})

        def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
            if end_date == date(2024, 1, 6):
                return [date(2024, 1, 4), date(2024, 1, 5)]
            if end_date == date(2024, 1, 4):
                return [date(2024, 1, 4)]
            raise AssertionError(f"unexpected calendar range end: {end_date}")

    service = SelectionCenterService(
        package_repository=InMemoryStrategyPackageRepository(),
        repository=InMemorySelectionCenterRepository(),
        calendar_provider=FakeCalendar(),
    )

    context = service.resolve_point_in_time_context(
        trade_date=date(2024, 1, 6),
        pit_mode="PREVIOUS_TRADING_DAY_CLOSE",
    )

    assert context["requested_trade_date"] == "2024-01-06"
    assert context["effective_trade_date"] == "2024-01-05"
    assert context["cutoff_date"] == "2024-01-04"


def test_selection_center_uses_frozen_artifact_when_node_preflight_fails() -> None:
    class FailingResolver:
        def __init__(self) -> None:
            self.preflight_calls = 0
            self.source_calls = 0

        def load_source(self, _experiment_id: str):
            self.source_calls += 1
            raise DataUnavailableError("node mlruns params endpoint returned 404")

        def require_preflight_or_raise(self, **_kwargs):
            self.preflight_calls += 1
            raise DataUnavailableError("node mlruns params endpoint returned 404")

    class ProviderShouldNotRun:
        backend_name = "should_not_run"

        def run(self, **_kwargs):
            raise AssertionError("daily selection must reuse frozen artifact instead of live inference")

    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {"strategy_id": "pkg_frozen_artifact"},
            }
        )
    )
    package_repo.save_manifest(manifest)
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    runtime_config = versioned_selection_runtime_config(
        {
            "selection_artifact_config": {"auto_generate": True, "cutoff_date": "2024-01-01"},
            "runtime_profile": {"selection": {"top_k": 1}, "tradability": {"exclude_suspended": False}},
        }
    )
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config),
            scores_json=[
                {
                    "symbol": "000001.SZ",
                    "score": 0.9,
                    "rank": 1,
                    "target_weight": 0.03,
                    "reference_price": 10.0,
                }
            ],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "runtime_workspace": "F:/AIstock/runtime_cache/pkg_frozen_artifact/sha",
            },
        )
    )
    resolver = FailingResolver()
    artifact_service = StrategyPackageSelectionArtifactService(
        package_repository=package_repo,
        artifact_repository=artifact_repo,
        runtime_asset_resolver=resolver,
        live_inference_provider=ProviderShouldNotRun(),
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(artifact_repository=artifact_repo),
        selection_artifact_service=artifact_service,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ"]
    assert resolver.preflight_calls == 0
    assert resolver.source_calls == 0
    health_checks = run.runtime_config["package_health"][manifest.package_id]["checks"]
    source_check = next(item for item in health_checks if item["name"] == "source_resolves")
    assert source_check["status"] == "PASS"
    assert source_check["context"]["asset_authority"] == "frozen_selection_score_artifact"


def test_live_inference_factor_order_uses_static_dataloader_schema(tmp_path) -> None:
    workspace = tmp_path / "safe_assets" / "qe_static_schema"
    factors_dir = workspace / "factors"
    artifacts_dir = workspace / "mlruns" / "1" / "artifacts"
    factors_dir.mkdir(parents=True)
    artifacts_dir.mkdir(parents=True)
    (artifacts_dir / "params.pkl").write_bytes(b"test model placeholder")
    for factor_name in ["factor_b", "factor_a", "factor_c"]:
        (factors_dir / f"{factor_name}.py").write_text(
            "import pandas as pd\n"
            f"def calculate():\n    return pd.DataFrame({{{factor_name!r}: [1.0]}})\n",
            encoding="utf-8",
        )
    (workspace / "model.py").write_text(
        "class CustomModel:\n    pass\n",
        encoding="utf-8",
    )
    static_columns = pd.MultiIndex.from_tuples(
        [("feature", "factor_b"), ("feature", "factor_a"), ("feature", "factor_c")]
    )
    pd.DataFrame([[1.0, 2.0, 3.0]], columns=static_columns).to_parquet(workspace / "combined_factors_df.parquet")
    (workspace / "conf.yaml").write_text(
        """
data_handler_config:
  data_loader:
    class: NestedDataLoader
    kwargs:
      dataloader_l:
        - class: qlib.contrib.data.loader.Alpha158DL
          kwargs:
            config:
              feature:
                - ["Ref($close, 1) / $close - 1"]
                - ["ROC1"]
        - class: qlib.data.dataset.loader.StaticDataLoader
          kwargs:
            config: combined_factors_df.parquet
model:
  kwargs:
    "num_features": {{ num_features }}
""",
        encoding="utf-8",
    )

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_cache")
    source = QEExperimentRuntimeSource(
        experiment_id="qe_static_schema",
        db_workspace_path=workspace,
        asset_workspace_path=workspace,
        factor_names=["factor_a"],
        custom_params={},
        data_split={},
    )

    prepared = resolver.prepare_workspace(
        package_id="pkg_static_schema",
        manifest_sha256="a" * 64,
        source=source,
    )

    assert prepared.alpha158_factors == ["ROC1"]
    assert prepared.dynamic_factors == ["factor_b", "factor_a", "factor_c"]
    assert prepared.factor_order == ["ROC1", "factor_b", "factor_a", "factor_c"]
    payload = json.loads(prepared.factor_order_path.read_text(encoding="utf-8"))
    assert payload["dynamic_factor_source"] == "qe_static_dataloader"
    assert payload["qe_experiment_factor_name_count"] == 1
    assert (prepared.workspace_path / "model" / "model.py").exists()
    entry_namespace = runpy.run_path(str(prepared.factor_entry_path))
    assert entry_namespace["_FACTOR_FILES"]["factor_a"] == str((factors_dir / "factor_a.py").resolve(strict=False))


def test_live_inference_factor_order_falls_back_to_qe_factor_names_when_static_schema_missing(tmp_path) -> None:
    workspace = tmp_path / "safe_assets" / "qe_missing_static_schema"
    factors_dir = workspace / "factors"
    artifacts_dir = workspace / "mlruns" / "1" / "artifacts"
    factors_dir.mkdir(parents=True)
    artifacts_dir.mkdir(parents=True)
    (artifacts_dir / "params.pkl").write_bytes(b"test model placeholder")
    for factor_name in ["factor_c", "factor_a"]:
        (factors_dir / f"{factor_name}.py").write_text(
            "import pandas as pd\n"
            f"def calculate():\n    return pd.DataFrame({{{factor_name!r}: [1.0]}})\n",
            encoding="utf-8",
        )
    (workspace / "conf.yaml").write_text(
        """
data_handler_config:
  data_loader:
    class: NestedDataLoader
    kwargs:
      dataloader_l:
        - class: qlib.contrib.data.loader.Alpha158DL
          kwargs:
            config:
              feature:
                - ["Ref($close, 1) / $close - 1"]
                - ["ROC1"]
        - class: qlib.data.dataset.loader.StaticDataLoader
          kwargs:
            config: combined_factors_df.parquet
""",
        encoding="utf-8",
    )

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_cache")
    source = QEExperimentRuntimeSource(
        experiment_id="qe_missing_static_schema",
        db_workspace_path=workspace,
        asset_workspace_path=workspace,
        factor_names=["factor_c", "factor_a"],
        custom_params={},
        data_split={},
    )

    prepared = resolver.prepare_workspace(
        package_id="pkg_missing_static_schema",
        manifest_sha256="b" * 64,
        source=source,
    )

    assert prepared.alpha158_factors == ["ROC1"]
    assert prepared.dynamic_factors == ["factor_c", "factor_a"]
    assert prepared.factor_order == ["ROC1", "factor_c", "factor_a"]
    payload = json.loads(prepared.factor_order_path.read_text(encoding="utf-8"))
    assert payload["dynamic_factor_source"] == "qe_experiments.factor_names_after_missing_static_loader"
    assert payload["static_loader_schema_available"] is False
    assert payload["static_loader_missing_configs"] == ["combined_factors_df.parquet"]
    assert payload["static_loader_unreadable_configs"] == []
    assert payload["warnings"]


def test_live_inference_materialize_continues_when_node_static_loader_file_is_404(tmp_path, monkeypatch) -> None:
    conf = """
data_handler_config:
  data_loader:
    class: NestedDataLoader
    kwargs:
      dataloader_l:
        - class: qlib.data.dataset.loader.StaticDataLoader
          kwargs:
            config: combined_factors_df.parquet
model:
  kwargs:
    "num_features": {{ num_features }}
"""

    def params_archive() -> bytes:
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode="w:gz") as archive:
            data = b"test model placeholder"
            info = tarfile.TarInfo("mlruns/1/artifacts/params.pkl")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        return payload.getvalue()

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
            if file_path == "conf.yaml":
                return conf.encode("utf-8")
            if file_path == "combined_factors_df.parquet":
                raise QEWorkspaceFileNotFound(
                    task_id,
                    loop_id,
                    file_path,
                    "http://node/files/combined_factors_df.parquet",
                )
            if file_path == "factors/factor_a.py":
                return b"def calculate():\n    return None\n"
            if file_path == "model.py":
                return b"class CustomModel:\n    pass\n"
            raise AssertionError(f"unexpected workspace file request: {file_path}")

        async def download_mlruns_params(self, task_id, loop_id):
            return params_archive()

    monkeypatch.setattr(QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: FakeClient()))

    resolver = QEExperimentRuntimeAssetResolver(cache_root=tmp_path / "runtime_cache")
    source_dir, model_params_origin = resolver._materialize_runtime_source_from_node(
        experiment_id="qe_node_missing_static",
        qe_task_id="qe_task_node",
        qe_loop_id="Loop1",
        execution_node_id="node-1",
        factor_names=["factor_a"],
        custom_params={"disable_alpha158": True},
        data_split={},
    )

    assert model_params_origin == "node"
    assert (source_dir / "conf.yaml").exists()
    assert not (source_dir / "combined_factors_df.parquet").exists()
    assert (source_dir / "factors" / "factor_a.py").exists()
    assert (source_dir / "model.py").exists()
    assert list(source_dir.glob("**/artifacts/params.pkl"))


def test_live_inference_materialize_uses_cached_params_when_node_mlruns_params_404(tmp_path, monkeypatch) -> None:
    cache_root = tmp_path / "runtime_cache"
    package_cache = cache_root / "pkg_cached" / "manifest_hash"
    (package_cache / "model").mkdir(parents=True)
    (package_cache / "model" / "params.pkl").write_bytes(b"cached model params")
    (package_cache / "manifest.json").write_text(
        json.dumps({"diagnostics": {"qe_experiment_id": "qe_cached_params"}}),
        encoding="utf-8",
    )

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
            if file_path == "conf.yaml":
                return b"data_handler_config: {}\n"
            if file_path == "factors/factor_a.py":
                return b"def calculate():\n    return None\n"
            if file_path == "model.py":
                raise QEWorkspaceFileNotFound(
                    task_id,
                    loop_id,
                    file_path,
                    "http://node/files/model.py",
                )
            raise AssertionError(f"unexpected workspace file request: {file_path}")

        async def download_mlruns_params(self, task_id, loop_id):
            raise RuntimeError("node mlruns params endpoint returned 404")

    monkeypatch.setattr(QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: FakeClient()))

    resolver = QEExperimentRuntimeAssetResolver(cache_root=cache_root)
    # Cache fallback now requires explicit opt-in (feedback_no_silent_errors).
    source_dir, model_params_origin = resolver._materialize_runtime_source_from_node(
        experiment_id="qe_cached_params",
        qe_task_id="qe_task_node",
        qe_loop_id="Loop1",
        execution_node_id="node-1",
        factor_names=["factor_a"],
        custom_params={"disable_alpha158": True},
        data_split={},
        allow_cache_fallback=True,
    )

    assert model_params_origin == "cache"
    copied = list(source_dir.glob("**/artifacts/params.pkl"))
    assert len(copied) == 1
    assert copied[0].read_bytes() == b"cached model params"


def test_live_inference_materialize_requires_explicit_cache_opt_in(tmp_path, monkeypatch) -> None:
    cache_root = tmp_path / "runtime_cache"
    package_cache = cache_root / "pkg_cached" / "manifest_hash"
    (package_cache / "model").mkdir(parents=True)
    (package_cache / "model" / "params.pkl").write_bytes(b"cached model params")
    (package_cache / "manifest.json").write_text(
        json.dumps({"diagnostics": {"qe_experiment_id": "qe_no_fallback"}}),
        encoding="utf-8",
    )

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def download_workspace_file_bytes(self, task_id, loop_id, file_path):
            if file_path == "conf.yaml":
                return b"data_handler_config: {}\n"
            if file_path == "factors/factor_a.py":
                return b"def calculate():\n    return None\n"
            if file_path == "model.py":
                raise QEWorkspaceFileNotFound(
                    task_id,
                    loop_id,
                    file_path,
                    "http://node/files/model.py",
                )
            raise AssertionError(f"unexpected workspace file request: {file_path}")

        async def download_mlruns_params(self, task_id, loop_id):
            raise RuntimeError("node mlruns params endpoint returned 404")

    monkeypatch.setattr(QEWorkspaceClient, "for_node", staticmethod(lambda _node_id: FakeClient()))

    resolver = QEExperimentRuntimeAssetResolver(cache_root=cache_root)
    with pytest.raises(DataUnavailableError, match="failed to materialize QE runtime assets") as exc_info:
        resolver._materialize_runtime_source_from_node(
            experiment_id="qe_no_fallback",
            qe_task_id="qe_task_node",
            qe_loop_id="Loop1",
            execution_node_id="node-1",
            factor_names=["factor_a"],
            custom_params={"disable_alpha158": True},
            data_split={},
        )

    assert "node mlruns params endpoint returned 404" in exc_info.value.context["error"]


def test_live_inference_load_source_materializes_via_node_api_not_db_workspace(tmp_path, monkeypatch) -> None:
    class Cursor:
        description = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return {
                "experiment_id": "qe_node_only",
                "status": "completed",
                "qe_task_id": "qe_task_node",
                "qe_loop_id": "Loop3",
                "factor_names": ["factor_a"],
                "custom_params": {"execution_node_id": "node-1"},
                "data_split": {},
            }

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    def fake_materialize(self, **kwargs):
        assert kwargs["execution_node_id"] == "node-1"
        return tmp_path / "node_api_cache", "node"

    monkeypatch.setattr(QEExperimentRuntimeAssetResolver, "_materialize_runtime_source_from_node", fake_materialize)

    resolver = QEExperimentRuntimeAssetResolver(conn_factory=lambda: Conn(), cache_root=tmp_path / "runtime")
    source = resolver.load_source("qe_node_only")

    assert source.asset_workspace_path == tmp_path / "node_api_cache"
    assert source.db_workspace_path == Path()
    assert source.qe_task_id == "qe_task_node"
    assert source.qe_loop_id == "Loop3"
    assert source.execution_node_id == "node-1"


def test_live_inference_load_source_for_qe_evolution_loop_uses_task_loop(tmp_path, monkeypatch) -> None:
    class Cursor:
        description = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query, params=None, **_kwargs):
            self.query = query
            self.params = params

        def fetchone(self):
            assert "WHERE qe_task_id = %s" in self.query
            assert self.params == ("qe_task", "Loop1")
            return {
                "experiment_id": "qe_task_L1",
                "status": "completed",
                "qe_task_id": "qe_task",
                "qe_loop_id": "Loop1",
                "factor_names": ["factor_a"],
                "custom_params": {},
                "data_split": {},
                "result_metrics": {"execution_trace": {"node_id": "node-1"}},
            }

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    def fake_materialize(self, **kwargs):
        assert kwargs["experiment_id"] == "qe_task_L1"
        assert kwargs["qe_task_id"] == "qe_task"
        assert kwargs["qe_loop_id"] == "Loop1"
        return tmp_path / "node_api_cache", "node"

    monkeypatch.setattr(QEExperimentRuntimeAssetResolver, "_materialize_runtime_source_from_node", fake_materialize)

    resolver = QEExperimentRuntimeAssetResolver(conn_factory=lambda: Conn(), cache_root=tmp_path / "runtime")
    source = resolver.load_source_for_strategy_package(
        source_type="qe_evolution_loop",
        source_id="qe_task",
        loop_id="Loop1",
        run_id="qe_task_L1",
    )

    assert source.experiment_id == "qe_task_L1"
    assert source.qe_task_id == "qe_task"
    assert source.qe_loop_id == "Loop1"
    assert source.execution_node_id == "node-1"


def test_selection_artifact_diagnostic_requires_explicit_source_path_without_workspace_scan() -> None:
    service = StrategyPackageSelectionArtifactService(
        package_repository=InMemoryStrategyPackageRepository(),
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
    )

    with pytest.raises(DataUnavailableError, match="source_path"):
        service._resolve_prediction_path("qe_no_direct_workspace", source_path=None)


def test_selection_center_intersection() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_scores("pkg_a", "000001.SZ", 0.9, 1)
    manifest_b = ready_manifest_with_scores("pkg_b", "000001.SZ", 0.8, 2)
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    selection_repo = InMemorySelectionCenterRepository()
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=selection_repo,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_packages(
        package_ids=[manifest_a.package_id, manifest_b.package_id],
        mode=SelectionMode.INTERSECTION,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ"]
    assert service.list_runs(limit=10)[0].run_id == run.run_id


def test_selection_center_lists_selectable_packages_with_metrics_and_latest_run() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_scores("pkg_list", "000001.SZ", 0.9, 1)
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(),
        ),
        refresh_audit=NoopRefreshAudit(),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    packages = service.list_selectable_packages(limit=10)

    assert packages[0]["package_id"] == manifest.package_id
    assert packages[0]["manifest_sha256"] == manifest.manifest_sha256
    assert packages[0]["metrics_summary"]["ic"] == pytest.approx(0.05)
    assert packages[0]["metrics_summary"]["rank_ic"] == pytest.approx(0.04)
    assert packages[0]["model_state"]["package_id"] == manifest.package_id
    assert "selection_health" in packages[0]
    assert packages[0]["latest_selection_run"]["run_id"] == run.run_id


def test_selection_center_authoritative_mode_uses_platform_risk_policy_and_display_top_n() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = st_pit_manifest_with_score_rows(
        "pkg_st_pit_contract",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.98, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
        topk=2,
    )
    package_repo.save_manifest(manifest)
    risk_policy = RecordingRiskPolicyService(
        {
            "000001.SZ": RiskDecision(
                symbol="000001.SZ",
                can_buy=False,
                reason_codes=["unit_st_pit_not_eligible"],
            )
        }
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=risk_policy,
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config(
            {
                "st_pit_authoritative": True,
                "top_k": 1,
                "runtime_profile": {
                    "selection": {"top_k": 1},
                    "risk_policy": {"enabled": True},
                },
            }
        ),
    )

    package_config = run.runtime_config["package_runtime_configs"][manifest.package_id]
    assert package_config["display_top_n"] == 1
    assert package_config["runtime_profile"]["selection"]["top_k"] is None
    assert package_config["runtime_profile"]["risk_policy"]["enabled"] is True
    assert package_config["qe_backtest_runtime_contract"]["runtime_features"]["risk_policy"]["package_bound"] is False
    assert risk_policy.profile_seen is not None
    assert risk_policy.profile_seen.enabled is True
    assert [item.symbol for item in run.package_results[manifest.package_id]] == ["000002.SZ"]
    assert run.excluded_results[manifest.package_id][0].reason == "risk_policy_block_buy"


def test_selection_center_consumes_validated_runtime_variant_candidate() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    rows = [
        {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
        {"symbol": "000002.SZ", "score": 0.98, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
    ]
    manifest = st_pit_manifest_with_score_rows("pkg_variant_selection", rows, topk=2)
    package_repo.save_manifest(manifest)
    package_service = StrategyPackageService(repository=package_repo)
    variant = package_service.create_runtime_variant(
        manifest.package_id,
        variant_name="selection top1 variant",
        variant_kind=RuntimeVariantKind.STRATEGY_CONFIG,
        variant_config={"strategy_config": {"custom_params": {"strategy_id": "score_weighted_topk_v2", "topk": 1}}},
        created_by="unit_test",
    )
    variant = package_service.mark_runtime_variant_validation(
        manifest.package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        paper_candidate=True,
        validation_evidence={"validation_run_id": "vr_selection_variant", "status": "passed"},
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=RecordingRiskPolicyService(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"runtime_variant_id": variant.variant_id}),
    )

    package_config = run.runtime_config["package_runtime_configs"][manifest.package_id]
    assert package_config["runtime_variant"]["variant_id"] == variant.variant_id
    assert package_config["runtime_variant"]["paper_candidate"] is False
    assert package_config["runtime_variant"]["variant_config"]["strategy_config"]["custom_params"]["topk"] == 1
    assert [item.symbol for item in run.package_results[manifest.package_id]] == ["000001.SZ"]

    st_pit_run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config(
            {
                "runtime_variant_id": variant.variant_id,
                "st_pit_authoritative": True,
                "runtime_profile": {"risk_policy": {"enabled": True}},
            }
        ),
    )
    st_pit_config = st_pit_run.runtime_config["package_runtime_configs"][manifest.package_id]
    assert st_pit_config["qe_backtest_runtime_contract"]["runtime_features"]["variant"]["variant_id"] == variant.variant_id
    assert st_pit_config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]["topk"] == 1


def test_selection_center_authoritative_mode_allows_runtime_st_pit_warning() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_name": "pkg_legacy_contract",
                "package_status": PackageStatus.SELECTION_ENABLED,
                "strategy_config": {
                    "strategy_id": "score_weighted_topk_v2",
                    "custom_params": {"strategy_id": "score_weighted_topk_v2"},
                },
            }
        )
    )
    package_repo.save_manifest(manifest)
    seed_test_authoritative_artifact(
        manifest,
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"st_pit_authoritative": True}),
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert run.package_results[manifest.package_id][0].symbol == "000001.SZ"


def test_selection_center_health_blocks_hmm_missing_stock_sector_map_before_inference(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")
    (tmp_path / "coefficients_preset_A_2024-01-01_2024-01-31.json").write_text(
        json.dumps(
            {
                "preset_key": "preset_A",
                "daily_coefficients": {"2024-01-02": {"801780.SI": 0.95}},
                "stock_sector_map": {},
            }
        ),
        encoding="utf-8",
    )
    package_repo = InMemoryStrategyPackageRepository()
    manifest = st_pit_manifest_with_score_rows(
        "pkg_st_pit_hmm_missing_map",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
        hmm_custom_params={
            "enable_sector_hmm": True,
            "hmm_model_snapshot_id": "hmm_001",
            "hmm_signal_preset": "preset_A",
        },
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(
            hmm_runtime=SectorHMMRuntime(
                snapshot_provider=FakeHMMSnapshotProvider(
                    {
                        "hmm_001": {
                            "snapshot_id": "hmm_001",
                            "model_path": str(model_path),
                            "status": "completed",
                        }
                    }
                )
            )
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(HMMRuntimeUnavailableError, match="stock sector mapping") as exc_info:
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config(
                {
                    "st_pit_authoritative": True,
                    "runtime_profile": {
                        "risk_policy": {"enabled": True},
                        "hmm": {
                            "enabled": True,
                            "model_snapshot_id": "hmm_001",
                            "signal_preset": "preset_A",
                        }
                    },
                }
            ),
        )

    assert exc_info.value.context["symbol"] == "000001.SZ"


def test_selection_center_health_passes_hmm_artifact_preflight(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")
    (tmp_path / "coefficients_preset_A_2024-01-01_2024-01-31.json").write_text(
        json.dumps(
            {
                "preset_key": "preset_A",
                "daily_coefficients": {"2024-01-02": {"801780.SI": 0.95}},
                "stock_sector_map": {"000001.SZ": "801780.SI"},
            }
        ),
        encoding="utf-8",
    )
    package_repo = InMemoryStrategyPackageRepository()
    manifest = st_pit_manifest_with_score_rows(
        "pkg_st_pit_hmm_preflight_ok",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
        hmm_custom_params={
            "enable_sector_hmm": True,
            "hmm_model_snapshot_id": "hmm_001",
            "hmm_signal_preset": "preset_A",
        },
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(
            hmm_runtime=SectorHMMRuntime(
                snapshot_provider=FakeHMMSnapshotProvider(
                    {
                        "hmm_001": {
                            "snapshot_id": "hmm_001",
                            "model_path": str(model_path),
                            "status": "completed",
                        }
                    }
                )
            )
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=RecordingRiskPolicyService(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config(
            {
                "st_pit_authoritative": True,
                "runtime_profile": {
                    "risk_policy": {"enabled": True},
                    "hmm": {
                        "enabled": True,
                        "model_snapshot_id": "hmm_001",
                        "signal_preset": "preset_A",
                    }
                },
            }
        ),
    )

    hmm_check = next(
        item
        for item in run.runtime_config["package_health"][manifest.package_id]["checks"]
        if item["name"] == "hmm_artifact_status"
    )
    assert hmm_check["status"] == "PASS"
    assert hmm_check["context"]["stock_sector_map_count"] == 1


def test_selection_center_weighted_fusion_uses_rank_normalized_scores() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_score_rows(
        "pkg_a",
        [
            {"symbol": "000001.SZ", "score": 100.0, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 90.0, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    manifest_b = ready_manifest_with_score_rows(
        "pkg_b",
        [
            {"symbol": "000002.SZ", "score": 0.1, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000003.SZ", "score": 0.9, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(),
        ),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_packages(
        package_ids=[manifest_a.package_id, manifest_b.package_id],
        mode=SelectionMode.WEIGHTED_FUSION,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"package_weights": {manifest_a.package_id: 0.25, manifest_b.package_id: 0.75}}),
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert [item.symbol for item in run.aggregate_results] == ["000002.SZ", "000001.SZ", "000003.SZ"]
    top = run.aggregate_results[0]
    assert top.score == pytest.approx(0.75)
    assert top.component_scores["fusion_method"] == "weighted_rank_fusion"
    assert top.component_scores["package_ranks"] == {
        manifest_a.package_id: 2,
        manifest_b.package_id: 1,
    }
    assert top.component_scores["package_raw_scores"] == {
        manifest_a.package_id: 90.0,
        manifest_b.package_id: 0.1,
    }
    assert top.component_scores["normalized_package_weights"] == {
        manifest_a.package_id: 0.25,
        manifest_b.package_id: 0.75,
    }
    assert top.component_scores["package_presence"] == {
        manifest_a.package_id: "selected_topK",
        manifest_b.package_id: "selected_topK",
    }
    assert top.component_scores["support_count"] == 2
    assert top.component_scores["rank_dispersion"] == 1
    assert top.component_scores["fusion_policy_sha256"]
    single_support = next(item for item in run.aggregate_results if item.symbol == "000001.SZ")
    assert single_support.component_scores["package_rank_scores"][manifest_b.package_id] == 0.0
    assert single_support.component_scores["package_presence"][manifest_b.package_id] == "not_selected_in_full_evidence"


def test_selection_center_aggregates_existing_single_package_runs() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_score_rows(
        "pkg_a",
        [
            {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.8, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    manifest_b = ready_manifest_with_score_rows(
        "pkg_b",
        [
            {"symbol": "000002.SZ", "score": 0.7, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000003.SZ", "score": 0.6, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(),
        ),
        refresh_audit=NoopRefreshAudit(),
    )
    run_a = service.run_single_package(
        package_id=manifest_a.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )
    run_b = service.run_single_package(
        package_id=manifest_b.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    aggregate = service.aggregate_existing_runs(
        source_run_ids=[run_a.run_id, run_b.run_id],
        mode=SelectionMode.WEIGHTED_FUSION,
        runtime_config=versioned_selection_runtime_config({"package_weights": {manifest_a.package_id: 0.4, manifest_b.package_id: 0.6}}),
    )

    assert aggregate.status == SelectionRunStatus.SUCCEEDED
    assert aggregate.runtime_config["aggregation_source"] == "existing_selection_runs"
    assert aggregate.runtime_config["source_run_ids"] == [run_a.run_id, run_b.run_id]
    assert [item.symbol for item in aggregate.aggregate_results] == ["000002.SZ", "000001.SZ", "000003.SZ"]
    assert aggregate.aggregate_results[0].component_scores["source_run_ids"] == [run_a.run_id, run_b.run_id]
    assert aggregate.package_results[manifest_a.package_id][0].component_scores["source_selection_run_id"] == run_a.run_id


def test_selection_center_aggregate_existing_runs_requires_same_date() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_scores("pkg_a", "000001.SZ", 0.9, 1)
    manifest_b = ready_manifest_with_scores("pkg_b", "000002.SZ", 0.8, 1)
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )
    run_a = service.run_single_package(
        package_id=manifest_a.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )
    run_b = service.run_single_package(
        package_id=manifest_b.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
    )

    with pytest.raises(RuntimeConfigInvalidError, match="same trade_date"):
        service.aggregate_existing_runs(
            source_run_ids=[run_a.run_id, run_b.run_id],
            mode=SelectionMode.UNION,
        )


def test_selection_center_weighted_fusion_requires_exact_positive_weights() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_scores("pkg_a", "000001.SZ", 0.9, 1)
    manifest_b = ready_manifest_with_scores("pkg_b", "000002.SZ", 0.8, 1)
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="package_weights"):
        service.run_packages(
            package_ids=[manifest_a.package_id, manifest_b.package_id],
            mode=SelectionMode.WEIGHTED_FUSION,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
        )
    with pytest.raises(RuntimeConfigInvalidError, match="positive finite"):
        service.run_packages(
            package_ids=[manifest_a.package_id, manifest_b.package_id],
            mode=SelectionMode.WEIGHTED_FUSION,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"package_weights": {manifest_a.package_id: 1.0, manifest_b.package_id: 0.0}}),
        )


def test_selection_center_filters_suspended_and_backfills_by_raw_rank() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_filter",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000003.SZ", "score": 0.77, "rank": 3, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    package_repo.save_manifest(manifest)
    selection_repo = InMemorySelectionCenterRepository()
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=selection_repo,
        tradability_filter=TradabilityFilter(FakeSuspendLookup({"000001.SZ"})),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"top_k": 2}),
    )

    assert [item.symbol for item in run.aggregate_results] == ["000002.SZ", "000003.SZ"]
    assert [item.rank for item in run.aggregate_results] == [1, 2]
    assert run.excluded_results[manifest.package_id][0].symbol == "000001.SZ"
    assert run.excluded_results[manifest.package_id][0].reason == "suspended_by_suspend_d"


def test_selection_center_preopen_readiness_does_not_require_daily_basic_gate() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_preopen_data_gate",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    runtime_config = versioned_selection_runtime_config(
        {
            "selection_artifact_config": {
                "cutoff_date": "2024-01-02",
                "required_cutoff_audit_datasets": ["stk_limit"],
            },
            "runtime_profile": {"selection": {"top_k": 1}},
        }
    )
    seed_test_authoritative_artifact(
        manifest,
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
        trade_dates=[date(2024, 1, 3)],
        runtime_config=runtime_config,
    )
    package_repo.save_manifest(manifest)
    refresh_audit = RecordingRefreshAudit()
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        runtime=StrategyPackageRuntime(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=refresh_audit,
    )

    service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config=runtime_config,
    )

    assert [call["dataset"] for call in refresh_audit.calls] == ["suspend_d", "stk_limit"]


def test_selection_center_runtime_profile_industry_blacklist_backfills() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_industry_filter",
        [
            {
                "symbol": "000001.SZ",
                "score": 0.99,
                "rank": 1,
                "target_weight": 0.03,
                "reference_price": 10.0,
                "component_scores": {"industry": "Bank"},
            },
            {
                "symbol": "000002.SZ",
                "score": 0.88,
                "rank": 2,
                "target_weight": 0.03,
                "reference_price": 10.0,
                "component_scores": {"industry": "Computer"},
            },
            {
                "symbol": "000003.SZ",
                "score": 0.77,
                "rank": 3,
                "target_weight": 0.03,
                "reference_price": 10.0,
                "component_scores": {"industry": "Medicine"},
            },
        ],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(),
        ),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"runtime_profile": {"industry_blacklist": ["Bank"], "selection": {"top_k": 2}}}),
    )

    assert [item.symbol for item in run.aggregate_results] == ["000002.SZ", "000003.SZ"]
    assert run.aggregate_results[0].component_scores["raw_rank"] == 2
    assert run.excluded_results[manifest.package_id][0].reason == "industry_blacklisted"
    assert run.runtime_config["runtime_profile"]["industry_blacklist"] == ["Bank"]


def test_selection_center_industry_blacklist_requires_industry_metadata() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_missing_industry",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(),
        ),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(DataUnavailableError, match="PIT industry metadata"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"runtime_profile": {"industry_blacklist": ["Bank"]}}),
        )


def test_selection_center_industry_blacklist_matches_pit_l2_code() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_db_industry_filter",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(
                {
                    "000001.SZ": IndustryInfo(
                        symbol="000001.SZ",
                        l1_code="801780.SI",
                        l1_name="银行",
                        l2_code="801783.SI",
                        l2_name="股份制银行II",
                    ),
                    "000002.SZ": IndustryInfo(
                        symbol="000002.SZ",
                        l1_code="801750.SI",
                        l1_name="计算机",
                        l2_code="801755.SI",
                        l2_name="软件开发",
                    ),
                }
            ),
        ),
        refresh_audit=NoopRefreshAudit(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config=versioned_selection_runtime_config({"runtime_profile": {"industry_blacklist": ["801783.SI"], "selection": {"top_k": 1}}}),
    )

    assert [item.symbol for item in run.aggregate_results] == ["000002.SZ"]
    exclusion = run.excluded_results[manifest.package_id][0]
    assert exclusion.context["matched_blacklist"] == "801783.SI"
    assert exclusion.context["matched_level"] == "l2_code"
    assert exclusion.context["l1_name"] == "银行"


def test_selection_center_industry_provider_failure_propagates() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_industry_provider_failure",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(
            FakeSuspendLookup(),
            industry_provider=FakeIndustryLookup(fail=True),
        ),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(DataUnavailableError, match="industry provider failed"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"runtime_profile": {"industry_blacklist": ["银行"]}}),
        )


def test_selection_center_hmm_runtime_profile_fails_fast() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_scores("pkg_hmm", "000001.SZ", 0.9, 1)
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    )

    with pytest.raises(HMMRuntimeUnavailableError, match="model_snapshot_id"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"runtime_profile": {"hmm": {"enabled": True}}}),
        )
    with pytest.raises(HMMRuntimeUnavailableError, match="signal_preset"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config=versioned_selection_runtime_config({"runtime_profile": {"hmm": {"enabled": True, "model_snapshot_id": "hmm_001"}}}),
        )


def test_strategy_package_runtime_applies_hmm_coefficients(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")
    coeff_path = tmp_path / "coefficients_preset_A_2024-01-01_2024-01-31.json"
    coeff_path.write_text(
        json.dumps(
            {
                "preset_key": "preset_A",
                "daily_coefficients": {
                    "2024-01-02": {
                        "801780.SI": 0.95,
                        "801750.SI": 1.10,
                    }
                },
                "stock_sector_map": {
                    "000001.SZ": "801780.SI",
                    "000002.SZ": "801750.SI",
                },
                "sector_count": 2,
            }
        ),
        encoding="utf-8",
    )
    manifest = ready_manifest_with_score_rows(
        "pkg_hmm_applied",
        [
            {"symbol": "000001.SZ", "score": 1.0, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
            {"symbol": "000002.SZ", "score": 0.98, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
        ],
    )
    runtime = StrategyPackageRuntime(
        hmm_runtime=SectorHMMRuntime(
            snapshot_provider=FakeHMMSnapshotProvider(
                {
                    "hmm_001": {
                        "snapshot_id": "hmm_001",
                        "model_path": str(model_path),
                        "status": "completed",
                    }
                }
            )
        )
    )

    snapshot = runtime.build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "hmm_001",
                    "signal_preset": "preset_A",
                }
            }
        },
    )

    assert [candidate.symbol for candidate in snapshot.candidates] == ["000002.SZ", "000001.SZ"]
    assert snapshot.candidates[0].score == pytest.approx(1.078)
    assert snapshot.candidates[0].component_scores["raw_rank"] == 2
    assert snapshot.candidates[0].component_scores["hmm"]["coefficient"] == pytest.approx(1.10)
    assert snapshot.candidates[0].component_scores["hmm"]["coefficients_path"] == str(coeff_path)


def test_strategy_package_runtime_auto_generates_hmm_coefficients_on_miss(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")

    class AutoGenerateHMMSnapshotProvider(FakeHMMSnapshotProvider):
        def __init__(self) -> None:
            super().__init__({"hmm_001": {"snapshot_id": "hmm_001", "model_path": str(model_path), "status": "completed"}})
            self.calls = []

        def _list_trading_days(self, start_date, end_date):
            return [date(2024, 1, 1)]

        def generate_daily_coefficients(self, snapshot_id, *, signal_preset, confirm_generate=False, confirm_text=None, as_of_date=None, effective_trade_date=None):
            self.calls.append({
                "snapshot_id": snapshot_id,
                "signal_preset": signal_preset,
                "confirm_generate": confirm_generate,
                "as_of_date": as_of_date,
                "effective_trade_date": effective_trade_date,
            })
            output = tmp_path / "coefficients_preset_A_2024-01-02_2024-01-02.json"
            output.write_text(
                json.dumps(
                    {
                        "generation_mode": "daily_asof_prediction_v1",
                        "snapshot_id": snapshot_id,
                        "config_id": "cfg_001",
                        "preset_key": signal_preset,
                        "as_of_trade_date": as_of_date.isoformat(),
                        "effective_trade_date": effective_trade_date.isoformat(),
                        "daily_coefficients": {"2024-01-02": {"801780.SI": 1.20}},
                        "stock_sector_map": {"000001.SZ": "801780.SI"},
                    }
                ),
                encoding="utf-8",
            )
            return {"status": "CREATED", "output_path": str(output)}

    provider = AutoGenerateHMMSnapshotProvider()
    manifest = ready_manifest_with_scores("pkg_hmm_auto_cache", "000001.SZ", 1.0, 1)
    runtime = StrategyPackageRuntime(hmm_runtime=SectorHMMRuntime(snapshot_provider=provider))

    snapshot = runtime.build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "hmm_001",
                    "signal_preset": "preset_A",
                }
            }
        },
    )

    assert provider.calls == [
        {
            "snapshot_id": "hmm_001",
            "signal_preset": "preset_A",
            "confirm_generate": True,
            "as_of_date": date(2024, 1, 1),
            "effective_trade_date": date(2024, 1, 2),
        }
    ]
    assert snapshot.candidates[0].score == pytest.approx(1.2)
    assert snapshot.candidates[0].component_scores["hmm"]["coefficient"] == pytest.approx(1.2)

    cached = runtime.build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "hmm_001",
                    "signal_preset": "preset_A",
                }
            }
        },
    )

    assert len(provider.calls) == 1
    assert cached.candidates[0].component_scores["hmm"]["coefficients_path"] == snapshot.candidates[0].component_scores["hmm"]["coefficients_path"]


def test_strategy_package_runtime_auto_generation_accepts_metadata_only_builtin_preset(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")

    class MetadataOnlyPresetProvider(FakeHMMSnapshotProvider):
        def __init__(self) -> None:
            super().__init__({"hmm_001": {"snapshot_id": "hmm_001", "model_path": str(model_path), "status": "completed"}})
            self.calls = []

        def _list_trading_days(self, start_date, end_date):
            return [date(2024, 1, 1)]

        def generate_daily_coefficients(self, snapshot_id, *, signal_preset, confirm_generate=False, as_of_date=None, effective_trade_date=None):
            self.calls.append({"signal_preset": signal_preset, "as_of_date": as_of_date, "effective_trade_date": effective_trade_date})
            output = tmp_path / "coefficients_preset_A_2024-01-02_2024-01-02.json"
            output.write_text(
                json.dumps(
                    {
                        "generation_mode": "daily_asof_prediction_v1",
                        "snapshot_id": snapshot_id,
                        "config_id": "cfg_001",
                        "preset_key": signal_preset,
                        "preset_coeffs": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
                        "as_of_trade_date": as_of_date.isoformat(),
                        "effective_trade_date": effective_trade_date.isoformat(),
                        "daily_coefficients": {"2024-01-02": {"801780.SI": 1.05}},
                        "stock_sector_map": {"000001.SZ": "801780.SI"},
                    }
                ),
                encoding="utf-8",
            )
            return {"status": "CREATED", "output_path": str(output)}

    provider = MetadataOnlyPresetProvider()
    manifest = ready_manifest_with_scores("pkg_hmm_metadata_preset", "000001.SZ", 1.0, 1)
    runtime = StrategyPackageRuntime(hmm_runtime=SectorHMMRuntime(snapshot_provider=provider))

    snapshot = runtime.build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "hmm_001",
                    "signal_preset": "preset_A",
                }
            }
        },
    )

    assert provider.calls == [{"signal_preset": "preset_A", "as_of_date": date(2024, 1, 1), "effective_trade_date": date(2024, 1, 2)}]
    assert snapshot.candidates[0].component_scores["hmm"]["coefficient"] == pytest.approx(1.05)


def test_strategy_package_runtime_resolves_latest_ready_hmm_snapshot_from_model_config(tmp_path) -> None:
    old_model_path = tmp_path / "old_models.json"
    new_model_path = tmp_path / "new_models.json"
    old_model_path.write_text("{}", encoding="utf-8")
    new_model_path.write_text("{}", encoding="utf-8")
    (tmp_path / "coefficients_preset_A_latest_2024-01-02.json").write_text(
        json.dumps(
            {
                "preset_key": "preset_A",
                "daily_coefficients": {"2024-01-02": {"801780.SI": 1.15}},
                "stock_sector_map": {"000001.SZ": "801780.SI"},
            }
        ),
        encoding="utf-8",
    )

    class ConfigResolvingProvider(FakeHMMSnapshotProvider):
        def __init__(self) -> None:
            super().__init__(
                {
                    "hmm_old": {"snapshot_id": "hmm_old", "model_path": str(old_model_path), "status": "completed", "trained_at": "2024-01-01T00:00:00"},
                    "hmm_latest": {"snapshot_id": "hmm_latest", "model_path": str(new_model_path), "status": "completed", "trained_at": "2024-01-03T00:00:00"},
                    "hmm_failed": {"snapshot_id": "hmm_failed", "model_path": str(new_model_path), "status": "failed", "trained_at": "2024-01-04T00:00:00"},
                }
            )
            self.list_calls: list[str] = []

        def list_snapshots(self, config_id: str):
            self.list_calls.append(config_id)
            return list(self.snapshots.values())

    provider = ConfigResolvingProvider()
    manifest = ready_manifest_with_scores("pkg_hmm_config", "000001.SZ", 1.0, 1)
    runtime = StrategyPackageRuntime(hmm_runtime=SectorHMMRuntime(snapshot_provider=provider))

    snapshot = runtime.build_signal_snapshot(
        manifest=manifest,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
        runtime_config={
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_config_id": "cfg_hmm",
                    "signal_preset": "preset_A",
                }
            }
        },
    )

    assert provider.list_calls == ["cfg_hmm"]
    assert snapshot.candidates[0].score == pytest.approx(1.15)
    assert snapshot.candidates[0].component_scores["hmm"]["model_snapshot_id"] == "hmm_latest"


def test_strategy_package_runtime_hmm_requires_stock_sector_map(tmp_path) -> None:
    model_path = tmp_path / "models.json"
    model_path.write_text("{}", encoding="utf-8")
    (tmp_path / "coefficients_preset_A_2024-01-01_2024-01-31.json").write_text(
        json.dumps(
            {
                "preset_key": "preset_A",
                "daily_coefficients": {"2024-01-02": {"801780.SI": 0.95}},
                "stock_sector_map": {},
            }
        ),
        encoding="utf-8",
    )
    manifest = ready_manifest_with_scores("pkg_hmm_missing_map", "000001.SZ", 0.9, 1)
    runtime = StrategyPackageRuntime(
        hmm_runtime=SectorHMMRuntime(
            snapshot_provider=FakeHMMSnapshotProvider(
                {
                    "hmm_001": {
                        "snapshot_id": "hmm_001",
                        "model_path": str(model_path),
                        "status": "completed",
                    }
                }
            )
        )
    )

    with pytest.raises(HMMRuntimeUnavailableError, match="stock sector mapping"):
        runtime.build_signal_snapshot(
            manifest=manifest,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={
                "runtime_profile": {
                    "risk_policy": {"enabled": True},
                    "hmm": {
                        "enabled": True,
                        "model_snapshot_id": "hmm_001",
                        "signal_preset": "preset_A",
                    }
                }
            },
        )


def test_selection_center_creates_single_package_paper_portfolio_with_trace_link() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = ready_manifest_with_score_rows(
        "pkg_paper",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    package_repo.save_manifest(manifest)
    StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="selection_link_default_policy",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="selection_link_backtest",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )
    selection_repo = InMemorySelectionCenterRepository()
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=selection_repo,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        paper_portfolio_service=PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=paper_repo,
        ),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    result = service.create_paper_portfolio_from_run(
        run_id=run.run_id,
        portfolio_name="from selection",
        initial_cash=100_000,
        start_date=date(2024, 1, 3),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )

    portfolio = result["portfolio"]
    link = result["link"]
    assert portfolio.package_id == manifest.package_id
    assert link.run_id == run.run_id
    assert link.portfolio_id == portfolio.portfolio_id
    assert result["paper_runtime_config"]["selection_source"]["run_id"] == run.run_id
    assert result["paper_runtime_config"]["selection_source"]["candidate_count"] == 1
    assert "selection_scores" not in result["paper_runtime_config"]
    assert package_repo.get(manifest.package_id).paper_portfolio_count == 1
    assert service.list_paper_portfolio_links(run.run_id)[0].portfolio_id == portfolio.portfolio_id


def test_selection_center_creates_paper_portfolio_after_default_pit_binding_finalization() -> None:
    class FakeCalendar:
        def ensure_trading_day(self, trade_date: date) -> None:
            return None

        def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
            assert end_date == date(2024, 1, 2)
            return [date(2024, 1, 2)]

    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = ready_manifest_with_score_rows(
        "pkg_paper_pit_default",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    package_repo.save_manifest(manifest)
    StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="selection_link_pit_default_policy",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="selection_link_pit_default_backtest",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        paper_portfolio_service=PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=paper_repo,
        ),
        calendar_provider=FakeCalendar(),
    )

    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 3),
        data_source="DB_HISTORICAL",
        runtime_config={"selection_artifact_config": {"pit_mode": "PREVIOUS_TRADING_DAY_CLOSE"}},
    )
    binding = validate_runtime_profile_binding(run.runtime_config)

    result = service.create_paper_portfolio_from_run(
        run_id=run.run_id,
        portfolio_name="from PIT default selection",
        initial_cash=100_000,
        start_date=date(2024, 1, 4),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )

    assert run.runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-02"
    assert binding["source"] == "generated_effective_runtime_config"
    assert binding["config_sha256"] == runtime_profile_config_sha256(run.runtime_config)
    assert result["paper_runtime_config"]["selection_runtime_profile_binding"]["config_sha256"] == binding["config_sha256"]


def test_selection_center_rejects_multi_package_paper_portfolio_without_combined_package() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest_a = ready_manifest_with_scores("pkg_a", "000001.SZ", 0.9, 1)
    manifest_b = ready_manifest_with_scores("pkg_b", "000001.SZ", 0.8, 2)
    package_repo.save_manifest(manifest_a)
    package_repo.save_manifest(manifest_b)
    selection_repo = InMemorySelectionCenterRepository()
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=selection_repo,
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        paper_portfolio_service=PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=InMemoryPaperTradingV2Repository(),
        ),
    )
    run = service.run_packages(
        package_ids=[manifest_a.package_id, manifest_b.package_id],
        mode=SelectionMode.INTERSECTION,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    with pytest.raises(UnsupportedFeatureError, match="combined StrategyPackage"):
        service.create_paper_portfolio_from_run(
            run_id=run.run_id,
            portfolio_name="multi package",
            initial_cash=100_000,
            start_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )


def test_selection_center_adds_selection_run_to_watchlist_with_trace(monkeypatch) -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "qe_watchlist_pkg",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.5},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 11.5},
            {"symbol": "000003.SZ", "score": 0.77, "rank": 3, "target_weight": 0.03, "reference_price": 12.5},
        ],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        result_enrichment_service=NoopSelectionResultEnrichment(),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.list_categories",
        lambda: [],
    )
    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.create_category",
        lambda name, description: captured.setdefault("created_category", (name, description)) and 901,
    )

    def fake_add_items_bulk_from_task_selection(**kwargs):
        captured["watchlist_call"] = kwargs
        items = kwargs["items"]
        return {
            "ok": True,
            "added": len(items),
            "skipped": 0,
            "moved": 0,
            "errors": [],
            "item_ids_by_code": {item["code"]: idx + 1 for idx, item in enumerate(items)},
        }

    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.add_items_bulk_from_task_selection",
        fake_add_items_bulk_from_task_selection,
    )

    result = service.add_run_to_watchlist(
        run_id=run.run_id,
        category_name="PaperV2-E2E-Watchlist",
        top_k=2,
        on_conflict="move",
    )

    assert result["ok"] is True
    assert result["category_id"] == 901
    assert result["entry_source"] == "qe_watchlist_pkg"
    assert result["entry_as_of"] == "2024-01-02"
    assert result["imported_symbols"] == ["000001.SZ", "000002.SZ"]
    assert captured["created_category"][0] == "PaperV2-E2E-Watchlist"

    call = captured["watchlist_call"]
    assert call["category_id"] == 901
    assert call["on_conflict"] == "move"
    assert call["entry_source"] == "qe_watchlist_pkg"
    items = call["items"]
    assert [item["code"] for item in items] == ["000001.SZ", "000002.SZ"]
    assert [item["entry_price"] for item in items] == [10.5, 11.5]
    assert [item["rank"] for item in items] == [1, 2]
    assert all(item["task_id"] == run.run_id for item in items)
    assert all(item["as_of"] == "2024-01-02" for item in items)
    assert all(item["entry_source"] == "qe_watchlist_pkg" for item in items)
    assert "Selection Center single_package" in items[0]["note"]


def test_selection_center_watchlist_import_uses_selection_entry_price_not_current_price(monkeypatch) -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "qe_watchlist_entry_pkg",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.5},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 11.5},
        ],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        result_enrichment_service=FixedEntryPriceEnrichment({"000001.SZ": 21.5, "000002.SZ": 22.5}),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr("backend.services.selection_center.service.watchlist_service.list_categories", lambda: [])
    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.create_category",
        lambda name, description: 901,
    )

    def fake_add_items_bulk_from_task_selection(**kwargs):
        captured["watchlist_call"] = kwargs
        return {
            "ok": True,
            "added": len(kwargs["items"]),
            "skipped": 0,
            "moved": 0,
            "errors": [],
            "item_ids_by_code": {},
        }

    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.add_items_bulk_from_task_selection",
        fake_add_items_bulk_from_task_selection,
    )

    result = service.add_run_to_watchlist(run_id=run.run_id, category_name="EntryPrice", top_k=2)

    assert result["ok"] is True
    items = captured["watchlist_call"]["items"]
    assert [item["entry_price"] for item in items] == [21.5, 22.5]


def test_selection_center_watchlist_import_uses_entry_price_basis_date(monkeypatch) -> None:
    class DatedEntryPriceEnrichment:
        def enrich_candidates(self, candidates, *, trade_date, runtime_config=None):
            return [
                candidate.model_copy(
                    update={
                        "selection_entry_price": 133.08,
                        "selection_entry_price_time": "2024-01-01",
                        "reference_price": 133.08,
                    }
                )
                for candidate in candidates
            ]

    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "qe_watchlist_entry_date_pkg",
        [{"symbol": "301312.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 133.08}],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        result_enrichment_service=DatedEntryPriceEnrichment(),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr("backend.services.selection_center.service.watchlist_service.list_categories", lambda: [])
    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.create_category",
        lambda name, description: 901,
    )

    def fake_add_items_bulk_from_task_selection(**kwargs):
        captured["watchlist_call"] = kwargs
        return {
            "ok": True,
            "added": len(kwargs["items"]),
            "skipped": 0,
            "moved": 0,
            "errors": [],
            "item_ids_by_code": {},
        }

    monkeypatch.setattr(
        "backend.services.selection_center.service.watchlist_service.add_items_bulk_from_task_selection",
        fake_add_items_bulk_from_task_selection,
    )

    result = service.add_run_to_watchlist(run_id=run.run_id, category_name="EntryPriceDate", top_k=1)

    assert result["ok"] is True
    item = captured["watchlist_call"]["items"][0]
    assert item["entry_price"] == 133.08
    assert item["as_of"] == "2024-01-01"


def test_selection_center_watchlist_import_rejects_missing_reference_price() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = ready_manifest_with_score_rows(
        "pkg_watchlist_missing_price",
        [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03},
            {"symbol": "000002.SZ", "score": 0.88, "rank": 2, "target_weight": 0.03, "reference_price": 11.5},
        ],
    )
    package_repo.save_manifest(manifest)
    service = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        result_enrichment_service=NoopSelectionResultEnrichment(),
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    with pytest.raises(DataUnavailableError, match="requires reference_price"):
        service.add_run_to_watchlist(run_id=run.run_id, top_k=2)
