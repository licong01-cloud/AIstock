from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.industry_provider import IndustryInfo
from backend.services.selection_center.models import SelectionMode, SelectionRunStatus
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime, TargetPositionEngine
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    StrategyPackageSelectionArtifactService,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    QEExperimentRuntimeAssetResolver,
    QEExperimentRuntimeSource,
    LiveInferenceResult,
)
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, UnsupportedFeatureError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class NoopRefreshAudit:
    def require_success(self, **_kwargs):
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


class FakeHMMSnapshotProvider:
    def __init__(self, snapshots: dict[str, dict] | None = None) -> None:
        self.snapshots = snapshots or {}

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        return self.snapshots.get(snapshot_id)


TEST_ARTIFACT_REPO = InMemorySelectionScoreArtifactRepository()


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

    with pytest.raises(StrategyPackageValidationError, match="selection_scores cannot be used"):
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

    with pytest.raises(StrategyPackageValidationError, match="not authoritative"):
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
            runtime_config={"runtime_profile": {"selection": {"top_k": 2}}},
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
        runtime_config={"runtime_profile": {"selection": {"top_k": 2}}},
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ", "000002.SZ"]


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
        runtime_config={
            "selection_artifact_config": {
                "auto_generate": True,
                "inference_backend": "local",
                "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            },
            "runtime_profile": {"selection": {"top_k": 2}},
        },
    )

    assert run.status == SelectionRunStatus.SUCCEEDED
    assert run.runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-02"
    assert run.runtime_config["point_in_time_context"]["score_trade_date"] == "2024-01-02"
    assert provider.calls[-1]["trade_date"] == date(2024, 1, 3)
    assert provider.calls[-1]["cutoff_date"] == date(2024, 1, 2)
    assert [item.symbol for item in run.aggregate_results] == ["000001.SZ", "000002.SZ"]


def test_live_inference_factor_order_uses_static_dataloader_schema(tmp_path) -> None:
    workspace = tmp_path / "qe_workspace" / "qe_static_schema"
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
    assert packages[0]["latest_selection_run"]["run_id"] == run.run_id


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
        runtime_config={"package_weights": {manifest_a.package_id: 0.25, manifest_b.package_id: 0.75}},
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
        runtime_config={"package_weights": {manifest_a.package_id: 0.4, manifest_b.package_id: 0.6}},
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

    with pytest.raises(StrategyPackageValidationError, match="same trade_date"):
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

    with pytest.raises(StrategyPackageValidationError, match="package_weights"):
        service.run_packages(
            package_ids=[manifest_a.package_id, manifest_b.package_id],
            mode=SelectionMode.WEIGHTED_FUSION,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
        )
    with pytest.raises(StrategyPackageValidationError, match="positive finite"):
        service.run_packages(
            package_ids=[manifest_a.package_id, manifest_b.package_id],
            mode=SelectionMode.WEIGHTED_FUSION,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={"package_weights": {manifest_a.package_id: 1.0, manifest_b.package_id: 0.0}},
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
        runtime_config={"top_k": 2},
    )

    assert [item.symbol for item in run.aggregate_results] == ["000002.SZ", "000003.SZ"]
    assert [item.rank for item in run.aggregate_results] == [1, 2]
    assert run.excluded_results[manifest.package_id][0].symbol == "000001.SZ"
    assert run.excluded_results[manifest.package_id][0].reason == "suspended_by_suspend_d"


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
        runtime_config={"runtime_profile": {"industry_blacklist": ["Bank"], "selection": {"top_k": 2}}},
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

    with pytest.raises(StrategyPackageValidationError, match="PIT industry metadata"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={"runtime_profile": {"industry_blacklist": ["Bank"]}},
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
        runtime_config={"runtime_profile": {"industry_blacklist": ["801783.SI"], "selection": {"top_k": 1}}},
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
            runtime_config={"runtime_profile": {"industry_blacklist": ["银行"]}},
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

    with pytest.raises(StrategyPackageValidationError, match="model_snapshot_id"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={"runtime_profile": {"hmm": {"enabled": True}}},
        )
    with pytest.raises(StrategyPackageValidationError, match="signal_preset"):
        service.run_single_package(
            package_id=manifest.package_id,
            trade_date=date(2024, 1, 2),
            data_source="DB_HISTORICAL",
            runtime_config={"runtime_profile": {"hmm": {"enabled": True, "model_snapshot_id": "hmm_001"}}},
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

    with pytest.raises(DataUnavailableError, match="stock sector mapping"):
        runtime.build_signal_snapshot(
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


def test_selection_center_creates_single_package_paper_portfolio_with_trace_link() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = ready_manifest_with_score_rows(
        "pkg_paper",
        [{"symbol": "000001.SZ", "score": 0.99, "rank": 1, "target_weight": 0.03, "reference_price": 10.0}],
    )
    package_repo.save_manifest(manifest)
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
    )
    run = service.run_single_package(
        package_id=manifest.package_id,
        trade_date=date(2024, 1, 2),
        data_source="DB_HISTORICAL",
    )

    with pytest.raises(StrategyPackageValidationError, match="requires reference_price"):
        service.add_run_to_watchlist(run_id=run.run_id, top_k=2)
