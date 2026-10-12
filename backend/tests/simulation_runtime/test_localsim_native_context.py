from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import UTC, date, datetime
import os
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_native_context import (
    fee_model_from_localsim_profile,
    frozen_localsim_profile,
    load_native_account_context,
    selection_config_from_localsim_profile,
)
from backend.services.simulation_runtime.localsim_runtime_profile import LocalSimRuntimeProfileConfigV1
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider
from backend.services.simulation_runtime.successor_models import SimulationAccountStatus
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.backtest_contract import build_backtest_runtime_contract
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.services.trading_core.errors import RuntimeConfigInvalidError


HISTORICAL_DATE = date(2026, 1, 5)


def _manifest() -> StrategyPackageManifest:
    return StrategyPackageManifest.model_validate(
        {
            "manifest_version": "alpha_core_v1",
            "package_id": "pkg_native_test",
            "package_name": "native test",
            "source": {"source_type": "qe_evolution_loop", "source_id": "qe_native_test", "loop_id": "Loop4"},
            "alpha_mode": "single_alpha",
            "alpha_components": [{"alpha_id": "a1", "alpha_name": "a1", "component_weight": 1,
                "factor_ids": ["f1"], "holding_period": "1d", "rebalance_frequency": "daily",
                "score_direction": "higher_better"}],
            "alpha_combination_policy": {"method": "identity", "weights": {"a1": 1}},
            "factor_set": [{"factor_id": "f1", "factor_name": "f1"}],
            "model_asset": [{"model_id": "model_native"}],
            "backtest_context": {"daily_strategy": {"strategy_id": "score_weighted_topk_v2",
                "topk": 4, "n_drop": 3, "custom_params": {"topk": 4, "n_drop": 3}}},
            "backtest_summary": {"ic": 0.1},
            "manifest_sha256": "a" * 64,
        }
    )


def _profile(**changes) -> LocalSimRuntimeProfileConfigV1:
    payload = {
        "daily_strategy": {"strategy_id": "score_weighted_topk_v2", "strategy_version": "v2", "top_k": 2,
            "parameters": {"n_drop": 0, "max_n_drop": 0, "max_weight": 0.5, "weight_method": "equal"}},
        "hmm": {"enabled": False},
        "risk_policy": {"enabled": False},
        "fee_policy": {"open_cost": 0.000095, "close_cost": 0.000595, "min_cost": 5},
    }
    payload.update(changes)
    return LocalSimRuntimeProfileConfigV1.model_validate(payload)


def _bundle(profile=None):
    repo = InMemoryLocalSimSuccessorRepository()
    manifest = _manifest()
    profile = profile or _profile()
    policy = local_sim_twap_only_policy_snapshot()
    account, scope, release, binding = LocalSimControlPlaneService(repository=repo).create_account(
        account_name="Native replay 5m", package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256, admission_receipt_id="admission_native",
        initial_capital=5_000_000,
        runtime_profile_id="profile_native", runtime_profile_version_id="version_native",
        runtime_profile_sha256=canonical_json_sha256(profile.model_dump(mode="json")),
        daily_strategy_profile_version_id=f"lsdaily_{canonical_json_sha256(profile.daily_strategy.model_dump(mode='json'))[:16]}",
        execution_policy_version_id=policy["policy_version_id"], execution_policy_sha256=policy["policy_sha256"],
        execution_policy_json=policy["policy_json"], tail_policy_version_id="tail_native", tail_policy_sha256="b" * 64,
        release_metadata={"localsim_runtime_profile_config": profile.model_dump(mode="json")},
        effective_from=HISTORICAL_DATE, effective_to=date(2026, 10, 12), created_by="test",
    )
    return repo, manifest, profile, account, scope, release, binding


def test_native_account_context_uses_real_successor_authority_without_paper_portfolio():
    repo, manifest, profile, account, scope, release, binding = _bundle()
    paper = InMemoryPaperTradingV2Repository()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper,
        localsim_repository_factory=lambda: repo,
        package_manifest_loader=lambda _id: manifest,
        price_loader=lambda _symbols, _date: {},
        enable_localsim_broker=False,
    )
    context = provider.load_context_for_phase(
        runtime_release=release, binding=binding, trade_date=HISTORICAL_DATE,
        as_of_time=datetime(2026, 1, 5, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        require_localsim_realtime_quote=False,
    )
    assert not paper.portfolios
    assert context.portfolio_id == scope.ledger_scope_id == account.account_id
    assert context.cash == 5_000_000
    assert context.top_k == profile.daily_strategy.top_k
    assert context.market_data_source == "DB_HISTORICAL"
    assert context.context_diagnostics["localsim_state_authority"] == "simulation_account_v1"


def test_native_broker_consumes_frozen_fee_policy_and_historical_source():
    configured = _profile(fee_policy={"open_cost": 0.002, "close_cost": 0.003, "min_cost": 11})
    repo, manifest, _config, _account, _scope, release, binding = _bundle(configured)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=InMemoryPaperTradingV2Repository,
        localsim_repository_factory=lambda: repo,
        package_manifest_loader=lambda _id: manifest,
        price_loader=lambda _symbols, _date: {},
    )
    context = provider.load_context_for_phase(
        runtime_release=release, binding=binding, trade_date=HISTORICAL_DATE,
        as_of_time=datetime(2026, 1, 5, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        require_localsim_realtime_quote=False,
    )
    assert context.local_broker.data_source is MinuteDataSource.DB_HISTORICAL
    assert context.local_broker._ledger.fee_model.min_cost == 11
    assert context.local_broker._ledger.fee_model.open_cost == 0.002


def test_native_current_day_source_is_realtime_even_with_old_causal_cursor():
    repo, manifest, _profile, _account, _scope, release, binding = _bundle()
    state = load_native_account_context(binding=binding, release=release, repository=repo, manifest_loader=lambda _id: manifest)
    source = ProductionSimulationRunContextProvider._resolve_local_sim_market_data_source(
        portfolio=state, trade_date=date.today(), as_of_time=datetime(2026, 1, 5, 15, 30, tzinfo=UTC),
    )
    assert source is MinuteDataSource.TDX_REALTIME


def test_frozen_profile_projection_is_consumed_by_real_target_engine():
    _repo, manifest, profile, _account, _scope, release, _binding = _bundle()
    config = release.release_config_json["metadata"]["selection_runtime_config"]
    assert config == selection_config_from_localsim_profile(profile)
    contract = build_backtest_runtime_contract(manifest, config)
    assert contract["portfolio_strategy"]["params"]["topk"] == 2
    assert contract["portfolio_strategy"]["params"]["n_drop"] == 0
    candidates = [SelectionCandidate(symbol=f"00000{i}.SZ", score=10-i, rank=i, reference_price=10) for i in range(1,5)]
    targets = TargetPositionEngine().build_targets(
        snapshot=SignalSnapshot(package_id=manifest.package_id, manifest_sha256=manifest.manifest_sha256,
            trade_date=HISTORICAL_DATE, data_source="DB_HISTORICAL", candidates=candidates, runtime_config=config),
        total_equity=5_000_000, top_k=2, manifest=manifest,
    )
    assert {item.symbol for item in targets} == {"000001.SZ", "000002.SZ"}
    assert build_backtest_runtime_contract(manifest)["portfolio_strategy"]["params"]["topk"] == 4


@pytest.mark.parametrize("field,value", [("min_cost", -1), ("open_cost", float("nan")), ("close_cost", "invalid"), ("commission", 5)])
def test_invalid_fee_policy_fails_loud(field, value):
    with pytest.raises(RuntimeConfigInvalidError):
        fee_model_from_localsim_profile(_profile(fee_policy={field: value}))


@pytest.mark.parametrize("kind", ["hash", "daily_id", "projection"])
def test_tampered_frozen_profile_is_rejected(kind):
    _repo, _manifest, _profile, _account, _scope, release, _binding = _bundle()
    if kind == "hash":
        broken = release.model_copy(update={"runtime_profile_sha256": "f" * 64})
    elif kind == "daily_id":
        broken = release.model_copy(update={"daily_strategy_profile_version_id": "lsdaily_wrong"})
    else:
        payload = deepcopy(release.release_config_json)
        payload["metadata"]["selection_runtime_config"]["runtime_profile"]["selection"]["top_k"] = 3
        broken = release.model_copy(update={"release_config_json": payload})
    with pytest.raises(RuntimeConfigInvalidError):
        frozen_localsim_profile(broken)


def test_cross_account_binding_cannot_load_native_state():
    repo, manifest, _profile, _account, _scope, release, binding = _bundle()
    broken = binding.model_copy(update={"broker_account_id": "another_account"})
    with pytest.raises(RuntimeConfigInvalidError, match="account/ledger"):
        load_native_account_context(binding=broken, release=release, repository=repo, manifest_loader=lambda _id: manifest)


def test_paused_native_account_cannot_submit():
    repo, manifest, _profile, account, _scope, release, binding = _bundle()
    repo.transition_account(account_id=account.account_id, expected_version=1,
        target_status=SimulationAccountStatus.PAUSED, updated_at=datetime.now(UTC))
    with pytest.raises(RuntimeConfigInvalidError, match="not active"):
        load_native_account_context(binding=binding, release=release, repository=repo, manifest_loader=lambda _id: manifest)


@pytest.mark.skipif(os.getenv("AISTOCK_DEV_DB_E2E") != "1", reason="requires the existing DEV database")
def test_native_context_reads_existing_dev_postgres_without_legacy_account():
    """Exercise the real account/scope/economic SQL; roll back this exact transaction."""
    import psycopg2
    from dotenv import load_dotenv

    from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
    from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository
    from backend.services.strategy_package.repository import StrategyPackageRepository

    configured_env = os.getenv("AISTOCK_ENV_FILE")
    if configured_env:
        load_dotenv(Path(configured_env))
    required = ("HOST", "USER", "PASSWORD", "NAME")
    assert all(os.getenv("TDX_DB_DEV_" + key) for key in required), "DEV connection locations must be configured"
    assert os.getenv("TDX_DB_DEV_NAME") != os.getenv("TDX_DB_NAME"), "DEV must be distinct from production"
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_DEV_HOST"], port=os.getenv("TDX_DB_DEV_PORT", "5432"),
        user=os.environ["TDX_DB_DEV_USER"], password=os.environ["TDX_DB_DEV_PASSWORD"],
        dbname=os.environ["TDX_DB_DEV_NAME"],
    )
    @contextmanager
    def connection_factory():
        yield conn

    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout=15000")
            cur.execute("SELECT package_id FROM strategy_pkg.package WHERE manifest_sha256 IS NOT NULL ORDER BY created_at DESC LIMIT 50")
            candidates = cur.fetchall()
        from backend.services.trading_core.errors import StrategyPackageValidationError

        package_repository = StrategyPackageRepository(conn_factory=connection_factory)
        manifest = None
        for candidate in candidates:
            try:
                manifest = package_repository.get(candidate[0]).current_manifest()
                break
            except StrategyPackageValidationError:
                # DEV contains deliberately invalid negative-test manifests.
                continue
        assert manifest is not None, "a valid existing DEV package identity is required"
        repo = LocalSimSuccessorRepository(conn_factory=connection_factory)
        profile = _profile()
        policy = local_sim_twap_only_policy_snapshot()
        account, scope, release, binding = LocalSimControlPlaneService(repository=repo).create_account(
            account_name=f"test_native_context_{uuid4().hex}", package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256, admission_receipt_id="dev_native_context",
            initial_capital=5_000_000, runtime_profile_id="dev_native_context", runtime_profile_version_id="dev_native_context_v1",
            runtime_profile_sha256=canonical_json_sha256(profile.model_dump(mode="json")),
            daily_strategy_profile_version_id=f"lsdaily_{canonical_json_sha256(profile.daily_strategy.model_dump(mode='json'))[:16]}",
            execution_policy_version_id=policy["policy_version_id"], execution_policy_sha256=policy["policy_sha256"],
            execution_policy_json=policy["policy_json"], tail_policy_version_id="dev_tail", tail_policy_sha256="b"*64,
            release_metadata={"localsim_runtime_profile_config": profile.model_dump(mode="json")},
            effective_from=HISTORICAL_DATE, effective_to=date(2026,10,12), created_by="test_native_context",
        )
        paper = PaperTradingV2Repository(conn_factory=connection_factory)
        provider = ProductionSimulationRunContextProvider(
            paper_repository_factory=lambda: paper, localsim_repository_factory=lambda: repo,
            package_manifest_loader=lambda _id: manifest, price_loader=lambda _symbols,_date: {},
            enable_localsim_broker=False,
        )
        result = provider.load_context_for_phase(
            runtime_release=release, binding=binding, trade_date=HISTORICAL_DATE,
            as_of_time=datetime(2026,1,5,15,30,tzinfo=ZoneInfo("Asia/Shanghai")), require_localsim_realtime_quote=False,
        )
        assert result.cash == 5_000_000
        assert result.portfolio_id == scope.ledger_scope_id == account.account_id
        assert result.current_positions == {}
        assert result.market_data_source == "DB_HISTORICAL"
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM paper_v2.portfolio WHERE portfolio_id=%s", (account.account_id,))
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()
