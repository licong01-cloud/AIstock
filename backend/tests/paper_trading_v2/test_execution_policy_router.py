from __future__ import annotations

from datetime import date

import pytest

from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS
from backend.routers import execution_policy
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import (
    PaperTradingV2PortfolioService,
    VNPY_STYLE_TEMPLATE_POLICY_PREFIX,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.tests.paper_trading_v2.test_day_runner import (
    make_paper_enabled_manifest,
    save_manifest_with_default_execution_policy,
)


def _service_and_portfolio(*, broker_backend: str = "minqmt_sim"):
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name=f"{broker_backend} policy template test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=(
            MinuteDataSource.MINIQMT_REALTIME
            if broker_backend == "minqmt_sim"
            else MinuteDataSource.TDX_REALTIME
        ),
        broker_backend=broker_backend,  # type: ignore[arg-type]
    )
    return service, manifest, portfolio


def test_execution_policy_algo_catalog_exposes_vnpy_style_assets() -> None:
    catalog = execution_policy._algo_catalog()

    for algo_code, spec in VNPY_STYLE_ASSETS.items():
        assert algo_code in catalog
        item = catalog[algo_code]
        assert item["asset_version"] == spec.version
        assert item["live_supported"] is True
        assert item["qe_ready"] is True
        assert item["broker_backend_supported"] == ["minqmt_sim"]
        assert item["data_requirements"]["required"] == list(spec.data_requirements)
        assert item["policy_template"]["policy_json"]["algo_code"] == algo_code
        assert item["source_attribution"]["upstream_source_file"] == spec.source_file
        assert "model_weights" not in str(item)


def test_get_algo_returns_vnpy_style_detail() -> None:
    response = execution_policy.get_algo("SNIPER_MINIQMT")

    assert response["ok"] is True
    assert response["algo"]["algo_code"] == "SNIPER_MINIQMT"
    assert response["algo"]["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")


def test_validate_policy_contract_accepts_vnpy_style_live_requirements() -> None:
    policy_json = VNPY_STYLE_ASSETS["SNIPER_MINIQMT"].execution_policy_json()

    blockers = execution_policy._validate_policy_contract(policy_json, "SNIPER_MINIQMT")

    assert blockers == []
    assert policy_json["data_requirements"]["requires_minute_bar"] is False
    assert policy_json["data_requirements"]["requires_pre_close"] is False


def test_validate_policy_contract_rejects_invalid_vnpy_style_config() -> None:
    policy_json = VNPY_STYLE_ASSETS["BEST_LIMIT_MINIQMT"].execution_policy_json()
    policy_json["algo_config"] = {"min_volume": 1000, "max_volume": 100}

    blockers = execution_policy._validate_policy_contract(policy_json, "BEST_LIMIT_MINIQMT")

    assert any(item.startswith("invalid_algo_config:") for item in blockers)


def test_minqmt_portfolio_lists_vnpy_style_runtime_template_policies() -> None:
    service, _manifest, portfolio = _service_and_portfolio(broker_backend="minqmt_sim")

    rows = service.list_execution_policies(portfolio.portfolio_id)
    templates = {
        item["algo_code"]: item
        for item in rows
        if item.get("activation_policy_source") == "vnpy_style_asset_template"
    }

    assert set(templates) == set(VNPY_STYLE_ASSETS)
    sniper = templates["SNIPER_MINIQMT"]
    assert sniper["policy_id"] == f"{VNPY_STYLE_TEMPLATE_POLICY_PREFIX}SNIPER_MINIQMT"
    assert sniper["validated_execution_policy_id"] == sniper["policy_id"]
    assert sniper["runtime_selectable"] is True
    assert sniper["matches_portfolio_manifest"] is True
    assert sniper["policy_json"]["data_requirements"]["requires_broker_quote"] is True
    assert sniper["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")


def test_activate_vnpy_style_template_persists_policy_context() -> None:
    service, _manifest, portfolio = _service_and_portfolio(broker_backend="minqmt_sim")

    activation = service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id="vnpy_asset:SNIPER_MINIQMT",
        activated_by="unit_test",
        reason="select sniper template",
    )

    assert activation.policy_id == "vnpy_asset:SNIPER_MINIQMT"
    assert activation.policy_json["algo_code"] == "SNIPER_MINIQMT"
    assert activation.context["activation_policy_source"] == "vnpy_style_asset_template"
    assert activation.context["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")


def test_activate_vnpy_style_template_rejects_local_sim_portfolio() -> None:
    service, _manifest, portfolio = _service_and_portfolio(broker_backend="local_sim")

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.activate_execution_policy(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            policy_id="vnpy_asset:SNIPER_MINIQMT",
            activated_by="unit_test",
            reason="local sim must not select MiniQMT asset",
        )

    assert exc_info.value.context["required_broker_backend"] == "minqmt_sim"
