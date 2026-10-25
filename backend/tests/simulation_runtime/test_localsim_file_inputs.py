from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backend.data_service.frozen_dataset import FrozenDatasetError, freeze_dataset_binding
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.frozen_file_adapters import FileMinuteMarketDataProvider
from backend.services.simulation_runtime.localsim_file_inputs import (
    FILE_BINDING_KEY, FILE_SOURCE_ID, current_file_dataset, frozen_file_binding, localsim_file_inputs,
    resolve_replay_source,
)
from backend.services.simulation_runtime.localsim_product_control import LocalSimHistoricalSourceAuthority
from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimHistoricalReplayDayExecutor
from backend.services.simulation_runtime.models import SimulationDailyRunStatus
from backend.tests.data_service.test_frozen_dataset import DAYS, SYMBOL, make_dataset


def _source_authority():
    return LocalSimHistoricalSourceAuthority(
        calendar=SimpleNamespace(latest_trading_day_on_or_before=lambda _: DAYS[-1]),
        clock=lambda: datetime(2026, 1, 22, 1, tzinfo=UTC))


def test_file_registration_is_frozen_in_native_bundle_and_retries_ignore_active_pointer(tmp_path, monkeypatch):
    from backend.tests.simulation_runtime.test_localsim_product_control_plane import _service, _replay_request
    profile = make_dataset(tmp_path)
    monkeypatch.setenv("AISTOCK_ACTIVE_DATASET_PROFILE_PATH", str(profile))
    service, repository, _ = _service()
    service.historical_source_authority = _source_authority()
    request = _replay_request().model_copy(update={"start_trade_date": DAYS[0], "end_trade_date": DAYS[-1],
        "effective_from": DAYS[0], "effective_to": DAYS[-1], "historical_source_id": FILE_SOURCE_ID})
    result = service.create_replay(request, created_by="file-test")
    binding = frozen_file_binding(result.release)
    assert binding and result.replay.historical_source_sha256 == binding["binding_hash"]
    assert result.release.release_config_json["metadata"][FILE_BINDING_KEY] == binding
    assert len(repository.replay_jobs) == 1
    profile.write_text("invalid active pointer now")
    restored = resolve_replay_source(service.historical_source_authority, job=result.replay, release=result.release)
    assert restored.historical_source_sha256 == result.replay.historical_source_sha256
    assert restored.trading_days == tuple(DAYS)
    with pytest.raises((ValueError, KeyError)):
        service.historical_source_authority.resolve(historical_source_id=FILE_SOURCE_ID,
            start_trade_date=DAYS[0], end_trade_date=DAYS[-1])


def test_day_executor_scopes_file_inputs_then_closes_without_controlling_services(tmp_path):
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    release = SimpleNamespace(release_config_json={"metadata": {FILE_BINDING_KEY: binding}})
    job = SimpleNamespace(replay_job_id="same-original-job", historical_source_id=FILE_SOURCE_ID,
        historical_source_sha256=binding["binding_hash"], release_id="release", simulation_account_id="account", binding_id="binding")
    calls = []

    def run_once(**kwargs):
        dataset = current_file_dataset(required=True)
        assert kwargs["data_source"] == "DATASET_FILES" and kwargs["trade_date"] == DAYS[1]
        assert dataset.raw_minute_rows(SYMBOL, DAYS[1])[0]["close"] == 10.5
        calls.append(dataset.run_id)
        run = SimpleNamespace(run_id="original-day", status=SimulationDailyRunStatus.SUCCEEDED,
                              run_payload_json={"no_rebalance_required": True})
        return SimpleNamespace(results=[SimpleNamespace(strategy_id="account", binding_id="binding", run=run, is_success=True)])

    closes = []
    def complete_close(**kwargs):
        assert current_file_dataset(required=True).run_id == "same-original-job:2026-01-20"
        assert kwargs == {"run_id": "original-day", "trade_date": DAYS[1], "strategy_id": "account",
                          "binding_id": "binding", "release_id": "release"}
        closes.append(kwargs)
    scheduler = SimpleNamespace(repository=SimpleNamespace(get_strategy_runtime_release=lambda _: release),
                                run_once=run_once, complete_localsim_replay_close=complete_close)
    LocalSimHistoricalReplayDayExecutor(lifecycle_scheduler=scheduler)(job, DAYS[1])
    assert calls == ["same-original-job:2026-01-20"] and len(closes) == 1 and current_file_dataset() is None
    job.historical_source_sha256 = "f" * 64
    with pytest.raises(Exception, match="source mismatch"):
        LocalSimHistoricalReplayDayExecutor(lifecycle_scheduler=scheduler)(job, DAYS[1])
    assert len(calls) == 1


def test_file_backend_matches_native_execution_cash_position_and_commission(tmp_path):
    from backend.tests.simulation_execution.test_localsim_runtime import _build_backend
    from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    policy = {"validated_execution_policy_id": "file-twap", "policy_json": {
        "algo_code": "TWAP", "algo_config": {"split_count": 1}}}
    with localsim_file_inputs(binding, run_id="execution") as dataset:
        file_market = FileMinuteMarketDataProvider(dataset).load_symbol_input(
            symbol=SYMBOL, trade_date=DAYS[1], source=MinuteDataSource.DATASET_FILES, require_suspend_status=True)
        file_backend, _, _ = _build_backend(data_source=MinuteDataSource.DATASET_FILES,
            provider=FileMinuteMarketDataProvider(dataset), execution_policy=policy)
        # Baseline is the native DB contract carrying the exact same legal raw
        # bars; both invoke the real TWAP engine and real ledger/fee model.
        from dataclasses import replace
        db_market = replace(file_market, source=MinuteDataSource.DB_HISTORICAL)
        db_provider = SimpleNamespace(load_symbol_input=lambda **_: db_market)
        db_backend, _, _ = _build_backend(provider=db_provider, execution_policy=policy)
        for backend in (file_backend, db_backend):
            intent = OrderIntent(package_id=backend.package_id, portfolio_id=backend.portfolio_id,
                symbol=SYMBOL, side=OrderSide.BUY, quantity=100, order_type=OrderType.MARKET, target_trade_date=DAYS[1])
            backend.submit_order_intent(intent)
        assert file_backend.query_account().cash == db_backend.query_account().cash
        assert file_backend.query_positions()[SYMBOL].quantity == db_backend.query_positions()[SYMBOL].quantity == 100
        assert file_backend.query_account().cash == pytest.approx(1_000_000 - 100 * 10.5 - 5)


def test_scope_failure_does_not_leak_and_original_db_release_is_unchanged(tmp_path):
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    db_release = SimpleNamespace(release_config_json={"metadata": {}})
    assert frozen_file_binding(db_release) is None
    with pytest.raises(RuntimeError):
        with localsim_file_inputs(binding, run_id="failed"):
            raise RuntimeError("interrupted")
    assert current_file_dataset() is None
    with pytest.raises(FrozenDatasetError):
        current_file_dataset(required=True)


def test_shared_artifact_entry_never_labels_a_db_provider_as_file():
    from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService
    from backend.services.trading_core.errors import DataUnavailableError
    service = object.__new__(StrategyPackageSelectionArtifactService)
    service._resolve_live_provider = lambda _: (SimpleNamespace(), "wsl")
    with pytest.raises(DataUnavailableError) as failure:
        service.prepare_from_live_inference_dates(package_id="any-native-package", trade_dates=[DAYS[1]], data_source="DATASET_FILES")
    assert failure.value.context["reason_code"] == "FROZEN_FILE_INFERENCE_PROVIDER_REQUIRED"


@pytest.mark.parametrize("boundary", ["stage", "binding", "selection"])
def test_native_scheduler_workers_own_file_readers_after_caller_timeout(tmp_path, monkeypatch, boundary):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler, SimulationSchedulerBindingResult
    from backend.services.simulation_runtime.models import SimulationBrokerBackend
    from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda *_, **__: pytest.fail("worker fell back to SQL"))
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    scheduler = object.__new__(SimulationLifecycleScheduler)
    scheduler._binding_tick_lock = scheduler._selection_inference_lock = threading.RLock()
    scheduler._binding_tick_inflight = {}
    scheduler._selection_inference_inflight = {}
    scheduler._selection_inference_shutdown = False
    scheduler._selection_inference_timeout_seconds = 10
    scheduler._timeout_seconds_from_env = lambda *_: .02
    scheduler._authoritative_selection_artifact_exists = lambda **_: False
    scheduler._selection_inference_key = lambda **_: ("file-binding", DAYS[1])
    scheduler._selection_inference_context = lambda **_: {"data_source": "DATASET_FILES"}
    release_worker = threading.Event()
    completed = threading.Event()
    observations = []
    carrier = SimulationSchedulerBindingResult(binding_id="file-binding", strategy_id="file-account",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, status="SUCCEEDED", data_source="DATASET_FILES")

    def work(**_kwargs):
        try:
            assert release_worker.wait(5)
            dataset = current_file_dataset(required=True)
            observations.append((dataset is not parent, dataset.identity, dataset.raw_minute_rows(SYMBOL, DAYS[1])[0]["close"]))
            return carrier
        finally:
            completed.set()

    scheduler._run_selection_sync = work
    with ThreadPoolExecutor(max_workers=1) as executor:
        scheduler._selection_inference_executor = executor
        try:
            with localsim_file_inputs(binding, run_id="original-day") as parent:
                if boundary == "stage":
                    with pytest.raises(RuntimeConfigInvalidError):
                        scheduler._run_callable_with_timeout(stage="FILE_READ", reason_code="TEST_TIMEOUT",
                            timeout_env_var="UNUSED_TEST_TIMEOUT", default_timeout_seconds=.02, context={}, func=work)
                elif boundary == "binding":
                    with pytest.raises(DataUnavailableError):
                        scheduler._run_local_sim_binding_single_flight(binding=SimpleNamespace(binding_id="file-binding"),
                            trade_date=DAYS[1], context={}, func=work)
                else:
                    with pytest.raises(DataUnavailableError):
                        scheduler._run_auto_generated_selection_nonblocking(binding=SimpleNamespace(), runtime_release=None,
                            trade_date=DAYS[1], data_source="DATASET_FILES", runtime_config={}, created_by="test")
            assert current_file_dataset() is None
        finally:
            release_worker.set()
        assert completed.wait(5)
        if boundary == "selection":
            entry = next(iter(scheduler._selection_inference_inflight.values()))
            assert entry.future.result(timeout=5) is carrier
        if boundary == "binding":
            next(iter(scheduler._binding_tick_inflight.values())).thread.join(timeout=5)
    assert observations == [(True, binding["binding_hash"], 10.5)]
    assert current_file_dataset() is None
