from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.repository import InMemorySelectionCenterRepository
from backend.services.selection_center.package_health import SelectionPackageHealthService
from backend.services.selection_center.service import SelectionCenterService
from backend.services.strategy_package.asset_eligibility import (
    MULTI_ALPHA_LOCALSIM_DRY_RUN_NOT_REQUIRED,
    MULTI_ALPHA_PAPER_ADMISSION_BLOCKER,
    StrategyPackageAssetEligibilityService,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
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
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.tests.paper_trading_v2.test_day_runner import (
    make_paper_enabled_manifest,
    save_manifest_with_default_execution_policy,
)
from backend.tests.strategy_package.test_multi_alpha_base_schema import _single_manifest
from backend.tests.strategy_package.test_multi_alpha_live_selection import (
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


def _reason(exc: BaseException) -> str | None:
    return getattr(exc, "context", {}).get("reason_code")


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
    assert len(artifact_repo.list(package_id=parent.package_id, manifest_sha256=parent.manifest_sha256)) >= 2

    local_summary = StrategyPackageAssetEligibilityService(admission_reader=admission_repo).summarize(
        package_repo.get(parent.package_id),
        broker_backend="local_sim",
    )
    minqmt_summary = StrategyPackageAssetEligibilityService(admission_reader=admission_repo).summarize(
        package_repo.get(parent.package_id),
        broker_backend="minqmt_sim",
    )

    assert local_summary.eligible is True
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER not in local_summary.blockers
    assert minqmt_summary.eligible is False
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in minqmt_summary.blockers

    selectable = SelectionCenterService(
        package_repository=package_repo,
        repository=InMemorySelectionCenterRepository(),
        asset_eligibility_service=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
        package_health_service=SelectionPackageHealthService(artifact_repository=artifact_repo),
    ).list_selectable_packages()
    assert parent.package_id in {item["package_id"] for item in selectable}


def test_multi_alpha_parent_enable_lifecycle_uses_shared_status_machine_after_dry_run() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)
    _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")
    service = StrategyPackageService(
        repository=package_repo,
        asset_eligibility=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
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
        asset_eligibility=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
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
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in summary.warnings
    check = next(item for item in summary.checks if item.name == MULTI_ALPHA_PAPER_ADMISSION_BLOCKER)
    assert check.status == "WARN"
    assert check.context["reason_code"] == MULTI_ALPHA_LOCALSIM_DRY_RUN_NOT_REQUIRED
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
        asset_eligibility_service=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
        package_health_service=SelectionPackageHealthService(artifact_repository=artifact_repo),
    ).list_selectable_packages(view="full")

    row = next(item for item in selectable if item["package_id"] == parent.package_id)
    assert row["asset_eligibility"]["eligible"] is True
    assert row["asset_eligibility"]["blockers"] == []
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in row["asset_eligibility"]["warnings"]
    assert admission_repo.records == {}


def test_local_sim_portfolio_create_succeeds_after_admission_and_minqmt_stays_closed() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    validator, admission_repo, _artifact_repo = _admission_validator(package_repo=package_repo)
    _run_dry_run(validator, parent.package_id, runtime_variant="top_k=50")
    paper_repo = InMemoryPaperTradingV2Repository()
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
        asset_eligibility_service=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
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

    with pytest.raises(Exception) as excinfo:
        service.create_portfolio(
            package_id=parent.package_id,
            portfolio_name="multi alpha minqmt",
            initial_cash=1_000_000,
            start_date=date(2024, 7, 3),
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            broker_backend="minqmt_sim",
        )
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in getattr(excinfo.value, "context", {}).get("blockers", [])


def test_local_sim_portfolio_create_succeeds_without_admission_and_minqmt_stays_closed() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    admission_repo = InMemoryMultiAlphaPaperAdmissionRepository()
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=InMemoryPaperTradingV2Repository(),
        asset_eligibility_service=StrategyPackageAssetEligibilityService(admission_reader=admission_repo),
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

    with pytest.raises(Exception) as excinfo:
        service.create_portfolio(
            package_id=parent.package_id,
            portfolio_name="multi alpha minqmt no admission",
            initial_cash=1_000_000,
            start_date=date(2024, 7, 3),
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            broker_backend="minqmt_sim",
        )
    assert MULTI_ALPHA_PAPER_ADMISSION_BLOCKER in getattr(excinfo.value, "context", {}).get("blockers", [])


@pytest.mark.parametrize(
    ("mutator", "runtime_config", "expected_reason"),
    [
        (
            lambda parent: parent.manifest.source_evidence["multi_alpha"]["legs"][0].__setitem__("seed_run_ids", []),
            _runtime_config_with_history(),
            "multi_alpha_seed_prediction_missing",
        ),
        (
            lambda parent: parent.manifest.source_evidence["multi_alpha"]["legs"][0].__setitem__(
                "child_manifest_sha256",
                "0" * 64,
            ),
            _runtime_config_with_history(),
            "multi_alpha_child_manifest_mismatch",
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
        asset_eligibility_service=StrategyPackageAssetEligibilityService(
            admission_reader=InMemoryMultiAlphaPaperAdmissionRepository()
        ),
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

    summary = StrategyPackageAssetEligibilityService(
        admission_reader=InMemoryMultiAlphaPaperAdmissionRepository()
    ).summarize(updated, broker_backend="local_sim")

    assert summary.eligible is False
    assert "unsupported_multi_alpha_policy" in summary.blockers
    check = next(item for item in summary.checks if item.name == "unsupported_multi_alpha_policy")
    assert check.status == "FAIL"
    assert check.context["reason_code"] == "unsupported_multi_alpha_policy"


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
