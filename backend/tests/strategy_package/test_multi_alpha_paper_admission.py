from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.package_health import SelectionPackageHealthService
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.asset_eligibility import (
    MULTI_ALPHA_PAPER_ADMISSION_BLOCKER,
    MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE,
    MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
    MULTI_ALPHA_SIGNAL_ADMISSION_PASSED,
    MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING,
    MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
    MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC,
    MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE,
    MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER,
    StrategyPackageAssetEligibilityService,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import AlphaMode, PackageStatus
from backend.services.strategy_package.multi_alpha_live import LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.multi_alpha_paper_admission import (
    InMemoryMultiAlphaPaperAdmissionRepository,
)
from backend.services.strategy_package.multi_alpha_paper_dry_run import (
    MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION,
    REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE,
    REASON_MULTI_ALPHA_DRY_RUN_UNSUPPORTED_BROKER,
    MultiAlphaPaperDryRunResult,
    MultiAlphaPaperDryRunValidator,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash_for_manifest,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError, StrategyPackageValidationError
from backend.tests.paper_trading_v2.test_day_runner import (
    FakeCalendar,
    FakeLimitProvider,
    FakeSuspendLookup,
    FakeSuspendProvider,
    RecordingRefreshAudit,
    make_paper_enabled_manifest,
    make_raw_bars,
    save_manifest_with_default_execution_policy,
)
from backend.tests.strategy_package.test_multi_alpha_base_schema import _single_manifest
from backend.tests.strategy_package.test_multi_alpha_live_selection import (
    ChildFailingRepository,
    TRADE_DATE,
    _artifact_service,
    _live_weight_history,
    _make_parent,
    _runtime_config,
)
from backend.tests.strategy_package.test_multi_alpha_promotion import (
    A1_LEG,
    FUND_LEG,
)
from backend.tests.strategy_package.test_manifest_v1 import admit_manifest_for_test


def _reason(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


class RaiseStrategyPackageRevalidation(StrategyPackageValidator):
    def validate_manifest(self, manifest) -> None:  # noqa: ANN001
        raise AssertionError(f"runtime revalidated StrategyPackage {manifest.package_id}")

    def validate_manifest_identity_for_paper_trading(self, manifest) -> None:  # noqa: ANN001
        raise AssertionError(f"Paper runtime revalidated StrategyPackage {manifest.package_id}")

    def validate_for_paper_trading(self, manifest) -> None:  # noqa: ANN001
        raise AssertionError(f"Paper runner revalidated StrategyPackage {manifest.package_id}")


def _runtime_with_admitted_artifact(
    manifest,
    *,
    runtime_config: dict[str, Any],
) -> StrategyPackageRuntime:  # noqa: ANN001
    rows = [
        {
            "symbol": "000001.SZ",
            "score": 0.91,
            "rank": 1,
            "target_weight": 0.03,
            "reference_price": 10.0,
            "component_scores": ({"alpha_a1": 0.9, "alpha_fund": 0.8} if manifest.alpha_mode == AlphaMode.MULTI_ALPHA else {}),
        }
    ]
    artifact_repository = InMemorySelectionScoreArtifactRepository()
    artifact_repository.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=date(2024, 1, 2),
            data_source=MinuteDataSource.TDX_REALTIME.value,
            runtime_config_hash=selection_artifact_runtime_hash_for_manifest(manifest, runtime_config),
            scores_json=rows,
            score_count=len(rows),
            universe_count=len(rows),
            top_score_symbol=rows[0]["symbol"],
            metadata={
                "source_type": (
                    LIVE_MULTI_ALPHA_SELECTION_SOURCE_TYPE
                    if manifest.alpha_mode == AlphaMode.MULTI_ALPHA
                    else AUTHORITATIVE_SELECTION_SOURCE_TYPE
                ),
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "test_seeded": True,
            },
        )
    )
    return StrategyPackageRuntime(
        validator=RaiseStrategyPackageRevalidation(),
        artifact_repository=artifact_repository,
    )


def _runtime_config_with_history(*, top_k: int = 50, history: list[dict] | None = None) -> dict[str, Any]:
    return _runtime_config(
        top_k=top_k,
        extra_artifact={"multi_alpha_weight_history": history if history is not None else _live_weight_history()},
    )


def _admission_validator(
    *,
    package_repo: InMemoryStrategyPackageRepository | None = None,
    admission_repo: InMemoryMultiAlphaPaperAdmissionRepository | None = None,
):
    package_repo = package_repo or _make_parent(live_weight_policy=True)[0]
    service, artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    service._load_reference_prices = lambda symbols, _trade_date: {symbol: 10.0 for symbol in symbols}  # type: ignore[method-assign]
    admission_repo = admission_repo or InMemoryMultiAlphaPaperAdmissionRepository()
    validator = MultiAlphaPaperDryRunValidator(
        package_repository=package_repo,
        selection_artifact_service=service,
        admission_repository=admission_repo,
        clock=lambda: datetime(2026, 6, 28, tzinfo=timezone.utc),
    )
    return validator, admission_repo, artifact_repo


def _run_dry_run(
    validator: MultiAlphaPaperDryRunValidator,
    package_id: str,
    *,
    runtime_variant: str = "top_k=50",
    runtime_config: dict[str, Any] | None = None,
) -> MultiAlphaPaperDryRunResult:
    return validator.run(
        package_id=package_id,
        broker_backend="local_sim",
        trade_date=TRADE_DATE,
        runtime_variant=runtime_variant,
        confirmation=MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION,
        validated_by="pytest",
        runtime_config=runtime_config or _runtime_config_with_history(),
        initial_cash=1_000_000_000.0,
    )


class RaiseAdmissionReader:
    def get_eligible(self, **_kwargs):  # noqa: ANN003, ANN201
        raise AssertionError("legacy paper dry-run admission must not be consulted")


class RaiseSelectionArtifactReader:
    def list(self, **_kwargs):  # noqa: ANN003, ANN201
        raise AssertionError("selection artifact repository should not be read when signal evidence is persisted")


class RaiseRuntimePackageRevalidation:
    def require_eligible(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        raise AssertionError("Paper runtime must not revalidate StrategyPackage assets")


class EmptySelectionArtifactReader:
    def list(self, **_kwargs):  # noqa: ANN003, ANN201
        return []


def _asset_service(**kwargs) -> StrategyPackageAssetEligibilityService:  # noqa: ANN003
    kwargs.setdefault("admission_reader", RaiseAdmissionReader())
    return StrategyPackageAssetEligibilityService(**kwargs)


def _signal_admission(parent) -> dict[str, Any]:  # noqa: ANN001
    return deepcopy(parent.current_manifest().source_evidence["multi_alpha"]["signal_admission"])


def _save_parent_with_signal_evidence(parent, evidence: dict[str, Any]):  # noqa: ANN001
    source_evidence = deepcopy(parent.current_manifest().source_evidence)
    source_evidence["multi_alpha"]["signal_admission"] = evidence
    return _copy_parent_with_source_evidence(parent, source_evidence, "signal_evidence_failure")


def _copy_parent_with_source_evidence(parent, source_evidence: dict[str, Any], suffix: str):  # noqa: ANN001
    manifest = parent.current_manifest()
    return admit_manifest_for_test(
        manifest.model_copy(
            update={
                "package_id": f"{manifest.package_id}_{suffix}",
                "package_name": f"{manifest.package_name}_{suffix}",
                "source_evidence": source_evidence,
                "manifest_sha256": None,
            }
        )
    )


def _parent_without_signal_admission(parent, suffix: str):  # noqa: ANN001
    source_evidence = deepcopy(parent.current_manifest().source_evidence)
    source_evidence["multi_alpha"].pop("signal_admission", None)
    return _copy_parent_with_source_evidence(parent, source_evidence, suffix)


def test_local_sim_dry_run_writes_admission_and_is_deterministic_for_topk_variants() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    validator, admission_repo, artifact_repo = _admission_validator(package_repo=package_repo)

    top50 = _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")
    top25 = _run_dry_run(
        validator,
        parent.package_id,
        runtime_variant="top_k=25",
        runtime_config=_runtime_config_with_history(top_k=25),
    )
    replay = _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")

    assert top50.target_count == 50
    assert top50.order_intent_count == 50
    assert top25.target_count == 25
    assert top25.order_intent_count == 25
    assert top50.dry_run_run_id == replay.dry_run_run_id
    assert top50.admission.admission_id == replay.admission.admission_id
    assert top50.artifact_shas == replay.artifact_shas
    assert top50.artifact_shas["component_score_artifact_sha256"].keys() == {A1_LEG, FUND_LEG}
    assert top50.artifact_shas["weight_artifact_sha256"]
    assert top50.artifact_shas["combined_score_artifact_sha256"]
    assert top50.evidence_json["parent_asset_runtime"]["runtime_source"] == "parent_package_asset"
    assert top50.evidence_json["parent_asset_runtime"]["runtime_package_id"] == parent.package_id
    assert top50.evidence_json["parent_asset_runtime"]["model_params_origin"] == "package_asset"
    assert {
        leg_id: item["runtime_source"]
        for leg_id, item in top50.evidence_json["parent_asset_runtime"]["component_artifacts"].items()
    } == {A1_LEG: "parent_package_asset", FUND_LEG: "parent_package_asset"}
    assert {
        leg_id: item["model_params_origin"]
        for leg_id, item in top50.evidence_json["parent_asset_runtime"]["component_artifacts"].items()
    } == {A1_LEG: "package_asset", FUND_LEG: "package_asset"}
    assert len(artifact_repo.list(package_id=parent.package_id, manifest_sha256=parent.manifest_sha256)) >= 2

    local_summary = _asset_service().summarize(
        package_repo.get(parent.package_id),
        broker_backend="local_sim",
    )
    minqmt_summary = _asset_service().summarize(
        package_repo.get(parent.package_id),
        broker_backend="minqmt_sim",
    )

    assert local_summary.eligible is True
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER not in local_summary.blockers
    assert minqmt_summary.eligible is True
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER not in minqmt_summary.blockers

    selectable = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        asset_eligibility_service=_asset_service(),
        package_health_service=SelectionPackageHealthService(artifact_repository=artifact_repo),
    ).list_selectable_packages()
    assert parent.package_id in {item["package_id"] for item in selectable}


def test_multi_alpha_parent_enable_lifecycle_uses_shared_status_machine_after_dry_run() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)
    _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")
    service = StrategyPackageService(
        repository=package_repo,
        asset_eligibility=_asset_service(),
    )

    approved = service.transition_status(
        package_id=parent.package_id,
        to_status=PackageStatus.BACKTEST_APPROVED,
        reason="approve_backtest_for_multi_alpha_lifecycle_test",
    )
    selected = service.enable_selection(parent.package_id)
    paper = service.enable_paper(parent.package_id)

    assert approved.package_status == PackageStatus.BACKTEST_APPROVED
    assert selected.package_status == PackageStatus.SELECTION_ENABLED
    assert paper.package_status == PackageStatus.PAPER_ENABLED
    events = service.list_status_events(parent.package_id)
    assert [event.reason for event in events] == [
        "package_created",
        "approve_backtest_for_multi_alpha_lifecycle_test",
        "enable_selection",
        "enable_paper",
    ]
    assert (events[2].from_status, events[2].to_status) == (
        PackageStatus.BACKTEST_APPROVED,
        PackageStatus.SELECTION_ENABLED,
    )
    assert (events[3].from_status, events[3].to_status) == (
        PackageStatus.SELECTION_ENABLED,
        PackageStatus.PAPER_ENABLED,
    )


def test_multi_alpha_parent_enable_paper_without_dry_run_is_allowed_for_localsim_default() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    admission_repo = InMemoryMultiAlphaPaperAdmissionRepository()
    service = StrategyPackageService(
        repository=package_repo,
        asset_eligibility=_asset_service(),
    )
    service.transition_status(
        package_id=parent.package_id,
        to_status=PackageStatus.BACKTEST_APPROVED,
        reason="approve_backtest_for_multi_alpha_lifecycle_test",
    )

    paper_enabled = service.enable_paper(parent.package_id)
    summary = service.asset_eligibility.summarize(parent, broker_backend="local_sim")

    assert paper_enabled.package_status == PackageStatus.PAPER_ENABLED
    assert summary.eligible is True
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER not in summary.blockers
    assert MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING not in summary.warnings
    check = next(item for item in summary.checks if item.name == MULTI_ALPHA_SIGNAL_ADMISSION_PASSED)
    assert check.status == "PASS"
    assert check.context["evidence_source"] == "persisted_manifest_signal_admission"
    assert admission_repo.records == {}
    assert [event.reason for event in service.list_status_events(parent.package_id)] == [
        "package_created",
        "approve_backtest_for_multi_alpha_lifecycle_test",
        "enable_paper",
    ]


def test_selection_full_path_lists_multi_alpha_without_localsim_dry_run_admission() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    admission_repo = InMemoryMultiAlphaPaperAdmissionRepository()
    _artifact_service_instance, artifact_repo, _resolver, _provider = _artifact_service(package_repo)

    selectable = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        asset_eligibility_service=_asset_service(),
        package_health_service=SelectionPackageHealthService(artifact_repository=artifact_repo),
    ).list_selectable_packages(view="full")

    row = next(item for item in selectable if item["package_id"] == parent.package_id)
    assert row["asset_eligibility"]["eligible"] is True
    assert row["asset_eligibility"]["blockers"] == []
    assert row["asset_eligibility"]["revalidated"] is False
    assert row["asset_eligibility"].get("warnings") in (None, [])
    assert admission_repo.records == {}


def test_local_sim_and_minqmt_manual_portfolio_create_succeed_after_optional_dry_run() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)
    _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")
    paper_repo = InMemoryPaperTradingV2Repository()
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
        asset_eligibility_service=RaiseRuntimePackageRevalidation(),
    )

    portfolio = service.create_portfolio(
        package_id=parent.package_id,
        portfolio_name="multi alpha local sim",
        initial_cash=1_000_000,
        start_date=date(2024, 7, 3),
        data_source=MinuteDataSource.DB_HISTORICAL,
        broker_backend="local_sim",
    )

    assert portfolio.package_id == parent.package_id
    assert portfolio.broker_backend == "local_sim"
    assert portfolio.data_source == MinuteDataSource.DB_HISTORICAL

    minqmt_portfolio = service.create_portfolio(
        package_id=parent.package_id,
        portfolio_name="multi alpha minqmt manual",
        initial_cash=1_000_000,
        start_date=date(2024, 7, 3),
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    assert minqmt_portfolio.package_id == parent.package_id
    assert minqmt_portfolio.broker_backend == "minqmt_sim"
    assert minqmt_portfolio.data_source == MinuteDataSource.MINIQMT_REALTIME


def test_local_sim_and_minqmt_manual_portfolio_create_succeed_without_dry_run_admission() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    admission_repo = InMemoryMultiAlphaPaperAdmissionRepository()
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=InMemoryPaperTradingV2Repository(),
        asset_eligibility_service=_asset_service(),
    )

    portfolio = service.create_portfolio(
        package_id=parent.package_id,
        portfolio_name="multi alpha local sim no admission",
        initial_cash=1_000_000,
        start_date=date(2024, 7, 3),
        data_source=MinuteDataSource.DB_HISTORICAL,
        broker_backend="local_sim",
    )

    assert portfolio.package_id == parent.package_id
    assert portfolio.broker_backend == "local_sim"
    assert admission_repo.records == {}

    minqmt_portfolio = service.create_portfolio(
        package_id=parent.package_id,
        portfolio_name="multi alpha minqmt no admission",
        initial_cash=1_000_000,
        start_date=date(2024, 7, 3),
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    assert minqmt_portfolio.package_id == parent.package_id
    assert minqmt_portfolio.broker_backend == "minqmt_sim"
    assert minqmt_portfolio.data_source == MinuteDataSource.MINIQMT_REALTIME


@pytest.mark.parametrize("alpha_mode", [AlphaMode.SINGLE_ALPHA, AlphaMode.MULTI_ALPHA])
def test_localsim_full_day_runs_admitted_single_and_multi_without_package_revalidation(alpha_mode: AlphaMode) -> None:
    if alpha_mode == AlphaMode.MULTI_ALPHA:
        package_repo, record = _make_parent(live_weight_policy=True)
        manifest = record.current_manifest()
        runtime_config: dict[str, Any] = {
            "runtime_profile": {"selection": {"top_k": 50}},
            "selection_artifact_config": {"auto_generate": False},
        }
        guarded_package_repo = ChildFailingRepository(package_repo)
    else:
        package_repo = InMemoryStrategyPackageRepository()
        manifest = make_paper_enabled_manifest()
        package_repo.save_manifest(manifest)
        runtime_config = {"selection_artifact_config": {"auto_generate": False}}
        guarded_package_repo = package_repo

    twap_policy_json = make_paper_enabled_manifest().minute_execution_policy.model_dump(mode="json")
    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name=f"{alpha_mode.value}_localsim_twap",
        policy_json=twap_policy_json,
        source_backtest_id=f"{alpha_mode.value}_localsim_twap_backtest",
        source_backtest_status="BACKTEST_VALIDATED",
    )
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio = PaperTradingV2PortfolioService(
        package_repository=guarded_package_repo,
        repository=paper_repo,
        validator=RaiseStrategyPackageRevalidation(),
        asset_eligibility_service=RaiseRuntimePackageRevalidation(),
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name=f"{alpha_mode.value} LocalSIM acceptance",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
        broker_backend="local_sim",
        execution_policy={"validated_execution_policy_id": policy.policy_id},
    )
    runtime = _runtime_with_admitted_artifact(manifest, runtime_config=runtime_config)
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
    )

    result = PaperTradingDayRunner(
        repository=paper_repo,
        package_repository=guarded_package_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,
        runtime=runtime,
        validator=RaiseStrategyPackageRevalidation(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=RecordingRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        runtime_config=runtime_config,
    )

    assert result.run.status.value == "SUCCEEDED"
    assert result.run.runtime_config["validated_execution_policy"]["policy_sha256"] == policy.policy_sha256
    assert paper_repo.orders[result.run.run_id]
    assert paper_repo.fills[result.run.run_id]
    assert paper_repo.cash_entries[result.run.run_id]
    assert paper_repo.snapshots[result.run.run_id].nav > 0
    assert [
        event["event_type"]
        for event in paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
    ] == [
        "RUN_STARTED",
        "DATA_READY",
        "SIGNAL_GENERATED",
        "TRADABILITY_FILTERED",
        "TARGETS_GENERATED",
        "ORDER_INTENTS_GENERATED",
        "MARKET_DATA_LOADED",
        "ORDER_EXECUTED",
        "RUN_SUCCEEDED",
    ]
    if alpha_mode == AlphaMode.MULTI_ALPHA:
        assert all(not package_id.startswith("pkg_mac") for package_id in guarded_package_repo.get_calls)


def test_minqmt_auto_run_missing_account_still_blocked_after_signal_eligibility() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=InMemoryPaperTradingV2Repository(),
        asset_eligibility_service=_asset_service(),
    )

    summary = service.asset_eligibility_service.require_eligible(
        package_repo.get(parent.package_id),
        broker_backend="minqmt_sim",
    )
    assert summary.eligible is True

    with pytest.raises(RuntimeConfigInvalidError) as excinfo:
        service.create_minqmt_auto_run_portfolio(
            package_id=parent.package_id,
            portfolio_name="multi alpha minqmt missing account",
            initial_cash=1_000_000,
            start_date=date(2024, 7, 3),
            broker_account_id="",
            create_session=False,
        )
    assert excinfo.value.context["broker_backend"] == "minqmt_sim"


@pytest.mark.parametrize(
    ("mutator", "runtime_config", "expected_reason"),
    [
        (
            lambda parent: parent.manifest.source_evidence["multi_alpha"]["legs"][0].__setitem__("seed_run_ids", []),
            _runtime_config_with_history(),
            "multi_alpha_parent_leg_seed_metadata_missing",
        ),
        (
            lambda parent: _remove_first_leg_model_asset(parent),
            _runtime_config_with_history(),
            "multi_alpha_parent_leg_model_asset_missing",
        ),
        (
            lambda _parent: None,
            _runtime_config_with_history(history=_live_weight_history(samples=1)),
            "multi_alpha_label_window_insufficient",
        ),
    ],
)
def test_dry_run_failures_are_loud_and_do_not_write_admission(mutator, runtime_config, expected_reason) -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    mutator(parent)
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)

    with pytest.raises(DataUnavailableError) as excinfo:
        _run_dry_run(validator, parent.package_id, runtime_config=deepcopy(runtime_config))

    assert _reason(excinfo.value) == expected_reason
    assert admission_repo.records == {}


def _remove_first_leg_model_asset(parent) -> None:  # noqa: ANN001
    manifest = parent.manifest
    first_model_id = manifest.alpha_components[0].model_id
    models = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    parent.manifest = manifest.model_copy(update={"model_asset": [model for model in models if model.model_id != first_model_id]})


def test_dry_run_rejects_single_alpha_and_minqmt_without_admission_write() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    single = package_repo.save_manifest(freeze_manifest(_single_manifest("single_alpha_for_dry_run")))
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)

    with pytest.raises(StrategyPackageValidationError) as single_exc:
        _run_dry_run(validator, single.package_id)

    with pytest.raises(StrategyPackageValidationError) as broker_exc:
        validator.run(
            package_id=single.package_id,
            broker_backend="minqmt_sim",
            trade_date=TRADE_DATE,
            runtime_variant="top_k=50",
            confirmation=MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION,
        )

    assert _reason(single_exc.value) == REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE
    assert _reason(broker_exc.value) == REASON_MULTI_ALPHA_DRY_RUN_UNSUPPORTED_BROKER
    assert admission_repo.records == {}


def test_single_alpha_paper_create_still_passes_without_admission_reader() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    paper_repo = InMemoryPaperTradingV2Repository()

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
        asset_eligibility_service=_asset_service(),
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="single alpha local sim",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
        broker_backend="local_sim",
    )

    assert portfolio.package_id == manifest.package_id
    assert portfolio.broker_backend == "local_sim"


def test_unknown_multi_alpha_paper_admission_blocker_still_blocks_localsim() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    manifest = parent.manifest
    source_evidence = deepcopy(manifest.source_evidence)
    source_evidence["multi_alpha"]["paper_admission"] = {
        "eligible": False,
        "blocking": ["unsupported_multi_alpha_policy"],
    }
    updated = package_repo.save_manifest(
        freeze_manifest(
            manifest.model_copy(
                update={
                    "package_id": f"{parent.package_id}_unknown_blocker",
                    "package_name": f"{parent.package_name} unknown blocker",
                    "source_evidence": source_evidence,
                    "manifest_sha256": None,
                }
            )
        )
    )

    local_summary = _asset_service().summarize(updated, broker_backend="local_sim")
    minqmt_summary = _asset_service().summarize(updated, broker_backend="minqmt_sim")

    assert local_summary.eligible is False
    assert minqmt_summary.eligible is False
    assert MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER in local_summary.blockers
    assert MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER in minqmt_summary.blockers
    check = next(item for item in local_summary.checks if item.name == MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER)
    assert check.status == "FAIL"
    assert check.context["reason_code"] == MULTI_ALPHA_SIGNAL_UNKNOWN_MANIFEST_BLOCKER
    assert check.context["unknown_blocker"] == "unsupported_multi_alpha_policy"


def test_signal_eligibility_hot_path_reads_persisted_evidence_without_artifact_or_order_engines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)

    def explode(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("eligibility must not instantiate order dry-run engines or full self-check")

    monkeypatch.setattr("backend.services.strategy_package.runtime.TargetPositionEngine", explode)
    monkeypatch.setattr("backend.services.strategy_package.runtime.RebalanceEngine", explode)
    monkeypatch.setattr("backend.services.strategy_package.runtime.StrategyPackageRuntime.build_signal_snapshot", explode)
    monkeypatch.setattr("backend.services.strategy_package.multi_alpha_paper_dry_run.MultiAlphaPaperDryRunValidator", explode)
    monkeypatch.setattr("backend.services.strategy_package.live_inference.QEExperimentRuntimeAssetResolver.prepare_workspace", explode)
    monkeypatch.setattr(
        "backend.services.strategy_package.live_inference.QEExperimentRuntimeAssetResolver.load_source_for_strategy_package",
        explode,
    )
    monkeypatch.setattr(
        "backend.services.strategy_package.live_inference.QEExperimentRuntimeAssetResolver.load_source_for_strategy_package_leg",
        explode,
    )
    monkeypatch.setattr("backend.services.strategy_package.live_inference.WslStrategyPackageInferenceProvider.run", explode)
    monkeypatch.setattr("backend.services.strategy_package.live_inference.win_to_wsl_path", explode)
    monkeypatch.setattr("backend.services.strategy_package.multi_alpha_live.win_to_wsl_path", explode)
    monkeypatch.setattr("backend.services.strategy_package.selection_artifact.win_to_wsl_path", explode)
    monkeypatch.setattr(
        "backend.services.strategy_package.multi_alpha_live.MultiAlphaLivePredictionProvider.generate_artifacts",
        explode,
    )
    monkeypatch.setattr(
        "backend.services.strategy_package.selection_artifact.StrategyPackageSelectionArtifactService.generate_from_live_inference_dates",
        explode,
    )
    monkeypatch.setattr(
        "backend.services.strategy_package.package_asset_freeze.PackageAssetFreezeService.freeze_manifest_assets",
        explode,
    )
    monkeypatch.setattr(
        "backend.services.strategy_package.frozen_runtime_self_check.FrozenRuntimeSelfCheckService.assert_manifest_self_contained",
        explode,
    )
    summary = _asset_service(selection_artifact_reader=RaiseSelectionArtifactReader()).summarize(
        package_repo.get(parent.package_id),
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is True
    assert MULTI_ALPHA_SIGNAL_ADMISSION_PASSED not in summary.blockers
    pass_check = next(check for check in summary.checks if check.name == MULTI_ALPHA_SIGNAL_ADMISSION_PASSED)
    assert pass_check.context["evidence_source"] == "persisted_manifest_signal_admission"
    assert pass_check.context["hot_path_full_self_check_replayed"] is False


@pytest.mark.parametrize(
    ("mutator", "expected_reason"),
    [
        (lambda evidence: evidence.__setitem__("self_check_passed", False), "multi_alpha_signal_self_check_failed"),
        (
            lambda evidence: (
                evidence.__setitem__("leg_count", 0),
                evidence["combined_signal_smoke"].__setitem__("leg_count", 0),
            ),
            MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_EMPTY,
        ),
        (
            lambda evidence: evidence.__setitem__("deterministic", False),
            MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC,
        ),
        (
            lambda evidence: evidence.__setitem__("persisted_for_hot_path", False),
            MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
        ),
        (
            lambda evidence: evidence.__setitem__("paper_runtime_dry_run_required", True),
            MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
        ),
        (
            lambda evidence: evidence.__setitem__("self_check_manifest_sha256", None),
            MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED,
        ),
    ],
)
def test_persisted_signal_evidence_failures_are_fail_closed(mutator, expected_reason) -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    evidence = _signal_admission(parent)
    mutator(evidence)
    updated = package_repo.save_manifest(_save_parent_with_signal_evidence(parent, evidence))

    summary = _asset_service(selection_artifact_reader=RaiseSelectionArtifactReader()).summarize(
        updated,
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is False
    assert expected_reason in summary.blockers
    check = next(item for item in summary.checks if item.name == expected_reason)
    assert check.status == "FAIL"
    assert check.context["reason_code"] == expected_reason


def test_selection_artifact_evidence_is_used_when_manifest_signal_evidence_is_missing() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    legacy_parent = package_repo.save_manifest(_parent_without_signal_admission(parent, "artifact_evidence"))
    _artifact_service_instance, artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=legacy_parent.package_id,
            manifest_sha256=legacy_parent.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            runtime_config_hash="unit_signal_evidence",
            scores_json=[{"symbol": "000001.SZ", "rank": 1, "score": 1.0, "target_weight": 0.04}],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={"target_weight_policy": "equal_weight", "topk": 1},
        )
    )

    summary = _asset_service(selection_artifact_reader=artifact_repo).summarize(
        legacy_parent,
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is True
    assert summary.blockers == []
    assert MULTI_ALPHA_SELECTION_ARTIFACT_AVAILABLE in {check.name for check in summary.checks if check.status == "PASS"}
    assert MULTI_ALPHA_SIGNAL_ADMISSION_PASSED in {check.name for check in summary.checks if check.status == "PASS"}


def test_invalid_selection_artifact_evidence_fails_closed_before_structural_fallback() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    legacy_parent = package_repo.save_manifest(_parent_without_signal_admission(parent, "artifact_invalid"))
    _artifact_service_instance, artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=legacy_parent.package_id,
            manifest_sha256=legacy_parent.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            runtime_config_hash="unit_invalid_signal_evidence",
            scores_json=[{"symbol": "000001.SZ", "rank": 1, "score": 1.0, "target_weight": 0.04}],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={"topk": 1},
        )
    )

    summary = _asset_service(selection_artifact_reader=artifact_repo).summarize(
        legacy_parent,
        broker_backend="local_sim",
    )

    assert summary.eligible is False
    assert MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE in summary.blockers
    check = next(item for item in summary.checks if item.name == MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE)
    assert check.context["reason_code"] == MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE
    assert "target_weight_policy" in check.context["missing_metadata_fields"]


def test_selection_artifact_evidence_requires_sha256_digest() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    legacy_parent = package_repo.save_manifest(_parent_without_signal_admission(parent, "artifact_bad_digest"))
    _artifact_service_instance, artifact_repo, _resolver, _provider = _artifact_service(package_repo)
    stored = artifact_repo.save(
        SelectionScoreArtifact(
            package_id=legacy_parent.package_id,
            manifest_sha256=legacy_parent.manifest_sha256,
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            runtime_config_hash="unit_invalid_digest_signal_evidence",
            scores_json=[{"symbol": "000001.SZ", "rank": 1, "score": 1.0, "target_weight": 0.04}],
            score_count=1,
            universe_count=1,
            top_score_symbol="000001.SZ",
            metadata={"target_weight_policy": "equal_weight", "topk": 1},
        )
    )
    artifact_repo.artifacts[
        (
            stored.package_id,
            stored.manifest_sha256,
            stored.trade_date,
            stored.data_source,
            stored.runtime_config_hash,
        )
    ] = stored.model_copy(update={"artifact_sha256": "z" * 64})

    summary = _asset_service(selection_artifact_reader=artifact_repo).summarize(
        legacy_parent,
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is False
    assert MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC in summary.blockers
    check = next(item for item in summary.checks if item.name == MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC)
    assert check.context["reason_code"] == MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_NONDETERMINISTIC


def test_missing_persisted_evidence_uses_bounded_structural_smoke_for_legacy_parent() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    legacy_parent = package_repo.save_manifest(_parent_without_signal_admission(parent, "legacy_smoke"))

    summary = _asset_service(selection_artifact_reader=EmptySelectionArtifactReader()).summarize(
        legacy_parent,
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is True
    assert MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING in summary.warnings
    warning = next(check for check in summary.checks if check.name == MULTI_ALPHA_SIGNAL_EVIDENCE_MISSING)
    assert warning.context["cost_class"] == "cheap_structural_no_workspace_no_model_probe_no_wsl"
    assert warning.context["hot_path_full_self_check_replayed"] is False


def test_legacy_structural_smoke_requires_leg_ids_match_components_and_weights() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    source_evidence = deepcopy(parent.current_manifest().source_evidence)
    source_evidence["multi_alpha"].pop("signal_admission", None)
    source_evidence["multi_alpha"]["legs"][0]["leg_id"] = "unexpected_leg"
    invalid_parent = package_repo.save_manifest(
        _copy_parent_with_source_evidence(parent, source_evidence, "legacy_smoke_leg_mismatch")
    )

    summary = _asset_service(selection_artifact_reader=EmptySelectionArtifactReader()).summarize(
        invalid_parent,
        broker_backend="minqmt_sim",
    )

    assert summary.eligible is False
    assert MULTI_ALPHA_SIGNAL_SELECTION_ARTIFACT_UNAVAILABLE in summary.blockers


def test_missing_signal_evidence_and_invalid_structure_fail_closed() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    source_evidence = deepcopy(parent.current_manifest().source_evidence)
    source_evidence["authority"] = "source_evidence_not_runtime_authority"
    source_evidence["multi_alpha"].pop("signal_admission", None)
    invalid_parent = package_repo.save_manifest(
        _copy_parent_with_source_evidence(parent, source_evidence, "missing_signal_evidence")
    )

    summary = _asset_service(selection_artifact_reader=EmptySelectionArtifactReader()).summarize(
        invalid_parent,
        broker_backend="local_sim",
    )

    assert summary.eligible is False
    assert MULTI_ALPHA_SIGNAL_ADMISSION_NOT_VALIDATED in summary.blockers


def test_router_paper_runtime_dry_run_success_and_loud_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeAdmission:
        def to_dict(self) -> dict[str, Any]:
            return {"admission_id": "mapa_test"}

    class FakeResult:
        admission = FakeAdmission()

        def to_dict(self) -> dict[str, Any]:
            return {"dry_run_run_id": "mapdry_test", "target_count": 50}

    class FakeValidator:
        def run(self, **kwargs):  # noqa: ANN001
            assert kwargs["broker_backend"] == "local_sim"
            assert kwargs["runtime_variant"] == "top_k=50"
            return FakeResult()

    monkeypatch.setattr(router_module, "MultiAlphaPaperDryRunValidator", lambda: FakeValidator())
    app = FastAPI()
    app.include_router(router_module.router)
    client = TestClient(app)

    success = client.post(
        "/strategy-packages/pkg_multi/paper-runtime-dry-run",
        json={
            "broker_backend": "local_sim",
            "trade_date": "2024-07-02",
            "runtime_variant": "top_k=50",
            "confirmation": MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION,
        },
    )

    assert success.status_code == 200, success.text
    assert success.json()["admission"]["admission_id"] == "mapa_test"

    class FailingValidator:
        def run(self, **_kwargs):  # noqa: ANN001
            raise StrategyPackageValidationError(
                "dry-run rejected",
                context={"reason_code": REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE, "package_id": "pkg_single"},
            )

    monkeypatch.setattr(router_module, "MultiAlphaPaperDryRunValidator", lambda: FailingValidator())
    failure = client.post(
        "/strategy-packages/pkg_single/paper-runtime-dry-run",
        json={
            "broker_backend": "local_sim",
            "trade_date": "2024-07-02",
            "runtime_variant": "top_k=50",
            "confirmation": MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION,
        },
    )

    assert failure.status_code == 400, failure.text
    assert failure.json()["detail"]["context"]["reason_code"] == REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE
