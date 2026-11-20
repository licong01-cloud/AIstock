"""Archived MiniQMT execution-policy and plan carrier contracts."""


from __future__ import annotations

from datetime import date

from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS
from backend.routers import execution_policy
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    ExecutionPlanCompiler,
    InMemorySimulationRuntimeRepository,
    RebalanceIntentService,
    SimulationBrokerBackend,
    StrategyRuntimeReleaseService,
    TargetPositionService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.tests.paper_trading_v2.test_execution_policy_router import (
    _service_and_portfolio,
)
from backend.tests.simulation_runtime.test_target_rebalance_shared import (
    _current_positions,
    _evidence,
    _snapshot,
)


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


def test_b0_execution_plan_reads_quote_policy_from_immutable_policy_json_snapshot() -> None:
    policy_json = {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {
            "price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE",
            "timer_iterations": 1,
            "tca": {
                "benchmark_policy": {
                    "benchmark_max_age_ms": 10_000,
                    "arrival_forward_window_ms": 2_000,
                    "clock_skew_tolerance_ms": 1_000,
                    "benchmark_max_transport_latency_ms": 3_000,
                    "policy_version": "miniqmt_execution_tca_benchmark_v1",
                }
            },
        },
        "bar_freq": "event",
        "execution_level": "event_driven_tick",
        "fallback_algo_code": None,
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "CALENDAR",
                "DEPTH_UNIT_SHARES",
                "EXCHANGE_TIMESTAMP",
                "FIVE_LEVEL_DEPTH",
                "RAW_PRICE_BASIS",
                "TRADABILITY",
            ],
            "max_receive_age_ms": 20_000,
            "max_source_lag_ms": 20_000,
            "max_exchange_age_ms": 20_000,
            "max_negative_skew_ms": 1_000,
            "max_clock_age_divergence_ms": 1_000,
            "max_dependency_group_skew_ms": 20_000,
            "auction_mode": "OBSERVE_ONLY",
        },
        "quote_evidence": {
            "schema_version": "miniqmt_quote_evidence_policy_v1",
            "benchmark_policy_version": "miniqmt_execution_tca_benchmark_v1",
            "mark_policy_version": "miniqmt_execution_tca_mark_selector_v1",
            "markout_max_lag_ms": 10_000,
        },
    }
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    policy_sha256 = canonical_json_sha256(policy_json)
    release = service.create_release(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        runtime_profile_id="runtime_profile_shared",
        runtime_profile_version_id="runtime_profile_shared_v1",
        runtime_profile_sha256="runtime_profile_hash_shared",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="b0-quote-v2-pilot-v1",
        execution_policy_sha256=policy_sha256,
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        execution_policy_json=policy_json,
        effective_from=date(2026, 5, 21),
        effective_to=date(2026, 5, 21),
    )
    binding = service.create_binding(
        strategy_id="strategy_minqmt_sim",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="acct_minqmt_sim",
        capital_allocation=100_000,
        miniqmt_quote_control={
            "schema_version": "miniqmt_quote_control_binding_v1",
            "control_revision": "B0_QUOTE_V2",
        },
        effective_from=date(2026, 5, 21),
        effective_to=date(2026, 5, 21),
    )
    evidence = _evidence(release)
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=_snapshot(),
        runtime_release=release,
        binding=binding,
        current_positions=_current_positions("portfolio_shared"),
    )
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions=_current_positions("portfolio_shared"),
        target_positions=targets,
    )

    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions,
        portfolio_id="portfolio_shared",
        execution_policy_payload=release.release_config_json["execution_policy"],
    )

    assert plan.plan_payload_json["execution_policy"]["payload"] == release.release_config_json["execution_policy"]
    assert plan.plan_payload_json["quote_control"]["binding"] == {
        "schema_version": "miniqmt_quote_control_binding_v1",
        "control_revision": "B0_QUOTE_V2",
    }
    revision = plan.plan_payload_json["quote_control"]["revision"]
    assert revision["execution_policy_sha256"] == policy_sha256
    assert revision["benchmark_policy_version"] == "miniqmt_execution_tca_benchmark_v1"
    assert revision["mark_policy_version"] == "miniqmt_execution_tca_mark_selector_v1"
    assert len(plan.plan_payload_json["quote_control"]["assignments"]) == len(plan.intents)
