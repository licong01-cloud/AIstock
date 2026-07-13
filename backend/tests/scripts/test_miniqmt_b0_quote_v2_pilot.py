from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest

from backend.services.simulation_runtime.models import (
    RuntimeReleaseValidationState,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository
from backend.services.simulation_runtime.service import StrategyRuntimeReleaseService
from scripts import miniqmt_b0_quote_v2_pilot as pilot


def _args(*, apply: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        env_file=None,
        target_db=pilot.TARGET_PROD,
        source_binding_id="simbind-source-ma",
        trade_date=date(2026, 7, 14),
        execution_policy_version_id="b0_quote_v2:SNIPER_MINIQMT:ma_8ec5e389:20260714:v1",
        max_receive_age_ms=20_000,
        max_source_lag_ms=20_000,
        max_exchange_age_ms=20_000,
        max_negative_skew_ms=1_000,
        max_clock_age_divergence_ms=1_000,
        max_dependency_group_skew_ms=20_000,
        benchmark_max_age_ms=10_000,
        arrival_forward_window_ms=2_000,
        clock_skew_tolerance_ms=1_000,
        benchmark_max_transport_latency_ms=3_000,
        benchmark_policy_version="miniqmt_execution_tca_benchmark_v1",
        mark_policy_version="miniqmt_execution_tca_mark_selector_v1",
        markout_max_lag_ms=10_000,
        observation_runtime_id="mqrt_sim_a02d3f2bdd17624043c2fae8",
        observation_tick_count=1_505,
        observation_transport_lag_p99_ms=16_719.207,
        observation_transport_lag_max_ms=55_830.979,
        operator="unit-test",
        output=None,
        apply=apply,
        confirm_production_dml=apply,
        confirm_scratch_dml=False,
    )


def _source_repo():  # type: ignore[no-untyped-def]
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    source_policy = {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE", "timer_iterations": 1},
        "bar_freq": "event",
        "execution_level": "event_driven_tick",
        "fallback_algo_code": None,
        "fallback_policy": {
            "on_algo_error": "fail",
            "on_broker_reject": "record_terminal_rejected",
            "on_missing_broker_quote": "fail",
        },
        "unfilled_handler": "broker_authoritative",
    }
    release = service.create_release(
        package_id="pkg_ma_8ec5e389fa2c5e484a1ac7e9",
        manifest_sha256="f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016",
        runtime_profile_id="ma_8ec5e389_runtime_profile",
        runtime_profile_version_id="ma_8ec5e389_topk25_20260703",
        runtime_profile_sha256="de9b9a4854b95406e5754a80b4a4eadf17702bb8c630dc54ea04d798aadb9ac7",
        daily_strategy_profile_version_id="ma_8ec5e389_daily_top25_20260703",
        execution_policy_version_id="vnpy_asset:SNIPER_MINIQMT:final_multistrategy_dry_run_20260603",
        execution_policy_sha256=canonical_json_sha256(source_policy),
        tail_policy_version_id="broker_authoritative_tail_policy_v1",
        tail_policy_sha256="e62c46e7c3c7e81676dc12780d117b67500bd0717a97815046ea31620611242e",
        execution_policy_json=source_policy,
        validation_state=RuntimeReleaseValidationState.SIM_VALIDATING,
        release_metadata={
            "route": "event_loop",
            "selection_runtime_config": {"top_k": 25},
        },
        effective_from=date(2026, 7, 13),
        effective_to=date(2026, 7, 13),
    )
    binding = service.create_binding(
        strategy_id="ma_8ec5e389_sim_20260703",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="62266303",
        account_group_id="ag_minqmt_62266303_sim",
        strategy_slot_id="ma_8ec5e389_sim_20260703",
        capital_allocation=10_000_000,
        strategy_name="ma_8ec5e389_sim_20260703",
        order_remark_prefix="MA8EC5",
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        effective_from=date(2026, 7, 13),
        effective_to=date(2026, 7, 13),
    )
    return repo, binding


def test_build_pilot_reuses_package_and_preserves_business_policy() -> None:
    repo, source_binding = _source_repo()
    source_release = repo.get_strategy_runtime_release(source_binding.release_id)

    artifacts = pilot.build_pilot_artifacts(source_release=source_release, source_binding=source_binding, args=_args())

    assert artifacts.release.package_id == source_release.package_id
    assert artifacts.release.manifest_sha256 == source_release.manifest_sha256
    assert artifacts.binding.strategy_id == source_binding.strategy_id
    assert artifacts.binding.binding_config_json["miniqmt_quote_control"] == pilot.QUOTE_CONTROL
    assert artifacts.execution_policy_json["algo_code"] == "SNIPER_MINIQMT"
    assert artifacts.execution_policy_json["fallback_algo_code"] is None
    assert artifacts.execution_policy_json["quote_contract"]["max_receive_age_ms"] == 20_000
    assert artifacts.execution_policy_json["algo_config"]["tca"]["benchmark_policy"]["policy_version"] == (
        "miniqmt_execution_tca_benchmark_v1"
    )
    assert artifacts.revision.execution_policy_sha256 == artifacts.release.execution_policy_sha256
    assert artifacts.observation["no_strategy_package_created"] is True


def test_build_pilot_rejects_date_overlap_and_conflicting_policy() -> None:
    repo, source_binding = _source_repo()
    source_release = repo.get_strategy_runtime_release(source_binding.release_id)
    overlapping = _args()
    overlapping.trade_date = date(2026, 7, 13)
    with pytest.raises(pilot.PilotActivationError, match="must follow"):
        pilot.build_pilot_artifacts(source_release=source_release, source_binding=source_binding, args=overlapping)
    
    source_release.release_config_json["execution_policy"]["policy_json"]["quote_contract"] = {"unexpected": True}
    with pytest.raises(pilot.PilotActivationError, match="snapshot hash differs"):
        pilot.build_pilot_artifacts(source_release=source_release, source_binding=source_binding, args=_args())
    with pytest.raises(pilot.PilotActivationError, match="conflicting quote_contract"):
        pilot._put_exact(  # noqa: SLF001
            {"quote_contract": {"unexpected": True}},
            "quote_contract",
            {"schema_version": "miniqmt_quote_contract_policy_v2"},
        )


def test_production_apply_requires_both_existing_confirmation_controls(monkeypatch: pytest.MonkeyPatch) -> None:
    args = _args(apply=True)
    args.confirm_production_dml = False
    with pytest.raises(pilot.PilotActivationError, match="confirm-production-dml"):
        pilot._validate_apply_gate(args)  # noqa: SLF001

    args.confirm_production_dml = True
    monkeypatch.delenv(pilot.APPLY_CONFIRM_ENV, raising=False)
    with pytest.raises(pilot.PilotActivationError, match=pilot.APPLY_CONFIRM_ENV):
        pilot._validate_apply_gate(args)  # noqa: SLF001

    monkeypatch.setenv(pilot.APPLY_CONFIRM_ENV, pilot.APPLY_CONFIRM_VALUE)
    pilot._validate_apply_gate(args)  # noqa: SLF001

    scratch = _args(apply=True)
    scratch.target_db = pilot.TARGET_DEV
    scratch.confirm_scratch_dml = False
    with pytest.raises(pilot.PilotActivationError, match="confirm-scratch-dml"):
        pilot._validate_apply_gate(scratch)  # noqa: SLF001
    scratch.confirm_scratch_dml = True
    pilot._validate_apply_gate(scratch)  # noqa: SLF001


def test_strict_numeric_and_observation_validation_are_loud() -> None:
    with pytest.raises(pilot.PilotActivationError, match="must be an integer"):
        pilot._strict_non_negative(True, field_name="value")  # noqa: SLF001
    with pytest.raises(pilot.PilotActivationError, match="must be an integer"):
        pilot._strict_non_negative("bad", field_name="value")  # noqa: SLF001
    with pytest.raises(pilot.PilotActivationError, match="must be non-negative"):
        pilot._strict_non_negative(-1, field_name="value")  # noqa: SLF001
    with pytest.raises(pilot.PilotActivationError, match="must be positive"):
        pilot._strict_positive(0, field_name="value")  # noqa: SLF001

    repo, source_binding = _source_repo()
    args = _args()
    args.observation_transport_lag_p99_ms = 60_000
    with pytest.raises(pilot.PilotActivationError, match="p99 cannot exceed"):
        pilot.build_pilot_artifacts(
            source_release=repo.get_strategy_runtime_release(source_binding.release_id),
            source_binding=source_binding,
            args=args,
        )


def test_env_loading_and_database_target_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prod_keys = {
        "TDX_DB_HOST": "127.0.0.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "postgres",
        "TDX_DB_PASSWORD": "secret",
    }
    for key in prod_keys:
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(f"{key}={value}" for key, value in prod_keys.items()), encoding="utf-8")

    pilot._load_env_file(env_file)  # noqa: SLF001
    config = pilot._db_config(target_db=pilot.TARGET_PROD)  # noqa: SLF001
    assert config == {
        "host": "127.0.0.1",
        "port": 5432,
        "dbname": "aistock",
        "user": "postgres",
        "password": "secret",
    }

    monkeypatch.delenv("TDX_DB_DEV_HOST", raising=False)
    with pytest.raises(pilot.PilotActivationError, match="incomplete"):
        pilot._db_config(target_db=pilot.TARGET_DEV)  # noqa: SLF001
    for key, value in {
        "TDX_DB_DEV_HOST": "db.example.com",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "aistock",
        "TDX_DB_DEV_USER": "postgres",
        "TDX_DB_DEV_PASSWORD": "secret",
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(pilot.PilotActivationError, match="not a local scratch/dev"):
        pilot._db_config(target_db=pilot.TARGET_DEV)  # noqa: SLF001


class _PsycopgConnection:
    def __init__(self) -> None:
        self.session = None
        self.closed = False

    def set_session(self, *, readonly, autocommit):  # type: ignore[no-untyped-def]
        self.session = {"readonly": readonly, "autocommit": autocommit}

    def close(self) -> None:
        self.closed = True


def test_connection_sets_exact_readonly_mode_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _PsycopgConnection()
    monkeypatch.setattr(pilot, "_load_env_file", lambda _path: None)
    monkeypatch.setattr(pilot, "_db_config", lambda **_kwargs: {"dbname": "test"})
    monkeypatch.setattr(pilot.psycopg2, "connect", lambda **_kwargs: fake)

    with pilot._connection(env_file=None, target_db=pilot.TARGET_PROD, readonly=True) as yielded:  # noqa: SLF001
        assert yielded is fake
        assert fake.session == {"readonly": True, "autocommit": True}
    assert fake.closed is True


class _Cursor:
    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def execute(self, _sql, _params):  # type: ignore[no-untyped-def]
        return None


class _Connection:
    def __init__(self) -> None:
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self) -> _Cursor:
        return _Cursor()

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1


def test_apply_is_atomic_idempotent_and_independently_read_back(monkeypatch: pytest.MonkeyPatch) -> None:
    repo, source_binding = _source_repo()
    connection = _Connection()

    @contextmanager
    def fake_connection(**_kwargs):  # type: ignore[no-untyped-def]
        yield connection

    monkeypatch.setattr(pilot, "_connection", fake_connection)
    monkeypatch.setattr(pilot, "_repo", lambda _conn: repo)
    monkeypatch.setenv(pilot.APPLY_CONFIRM_ENV, pilot.APPLY_CONFIRM_VALUE)

    args = _args(apply=True)
    args.source_binding_id = source_binding.binding_id
    first = pilot.run(args)
    second = pilot.run(args)

    assert first["status"] == "applied"
    assert first["readback"]["status"] == "applied_and_verified"
    assert first["readback"]["active_binding_ids"] == [first["pilot"]["binding_id"]]
    assert second["pilot"]["binding_id"] == first["pilot"]["binding_id"]
    assert second["pilot"]["release_id"] == first["pilot"]["release_id"]
    assert second["status"] == "already_current"
    assert second["db_writes_executed"] is False
    assert connection.commit_count == 2
    assert connection.rollback_count == 0
    assert first["invariants"]["strategy_package_created"] is False
    assert first["invariants"]["broker_called"] is False


def test_dry_run_reports_conflicts_without_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    repo, source_binding = _source_repo()
    args = _args()
    args.source_binding_id = source_binding.binding_id
    source_release = repo.get_strategy_runtime_release(source_binding.release_id)
    artifacts = pilot.build_pilot_artifacts(source_release=source_release, source_binding=source_binding, args=args)
    repo.save_strategy_runtime_release(artifacts.release)
    service = StrategyRuntimeReleaseService(repository=repo)
    conflicting = service.create_binding(
        strategy_id=source_binding.strategy_id,
        release=artifacts.release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id=source_binding.broker_account_id,
        account_group_id=source_binding.account_group_id,
        strategy_slot_id=source_binding.strategy_slot_id,
        capital_allocation=source_binding.capital_allocation,
        approval_state=source_binding.approval_state,
        binding_metadata={"conflict": True},
        effective_from=args.trade_date,
        effective_to=args.trade_date,
    )
    assert conflicting.binding_id != artifacts.binding.binding_id

    @contextmanager
    def fake_connection(**_kwargs):  # type: ignore[no-untyped-def]
        yield _Connection()

    monkeypatch.setattr(pilot, "_connection", fake_connection)
    monkeypatch.setattr(pilot, "_repo", lambda _conn: repo)
    with pytest.raises(pilot.PilotActivationError, match="different active binding"):
        pilot.run(args)


def test_cli_main_writes_exact_report_without_applying(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "pilot.json"
    argv = [
        "miniqmt_b0_quote_v2_pilot.py",
        "--source-binding-id",
        "source",
        "--trade-date",
        "2026-07-14",
        "--execution-policy-version-id",
        "policy-v1",
        "--max-receive-age-ms",
        "1",
        "--max-source-lag-ms",
        "1",
        "--max-exchange-age-ms",
        "1",
        "--max-negative-skew-ms",
        "0",
        "--max-clock-age-divergence-ms",
        "1",
        "--max-dependency-group-skew-ms",
        "1",
        "--benchmark-max-age-ms",
        "1",
        "--arrival-forward-window-ms",
        "0",
        "--clock-skew-tolerance-ms",
        "0",
        "--benchmark-max-transport-latency-ms",
        "1",
        "--benchmark-policy-version",
        "benchmark-v1",
        "--mark-policy-version",
        "mark-v1",
        "--markout-max-lag-ms",
        "1",
        "--observation-runtime-id",
        "runtime",
        "--observation-tick-count",
        "1",
        "--observation-transport-lag-p99-ms",
        "1",
        "--observation-transport-lag-max-ms",
        "1",
        "--output",
        str(output),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(
        pilot,
        "run",
        lambda args: {"status": "ready_for_apply", "trade_date": args.trade_date.isoformat()},
    )

    assert pilot.main() == 0
    assert '"status": "ready_for_apply"' in output.read_text(encoding="utf-8")


def test_apply_rolls_back_release_and_binding_transaction_on_binding_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, source_binding = _source_repo()
    connection = _Connection()

    @contextmanager
    def fake_connection(**_kwargs):  # type: ignore[no-untyped-def]
        yield connection

    original_save = repo.save_simulation_release_binding

    def fail_pilot_binding(binding):  # type: ignore[no-untyped-def]
        if binding.binding_config_json.get("miniqmt_quote_control") == pilot.QUOTE_CONTROL:
            raise RuntimeError("injected binding persistence failure")
        return original_save(binding)

    monkeypatch.setattr(pilot, "_connection", fake_connection)
    monkeypatch.setattr(pilot, "_repo", lambda _conn: repo)
    monkeypatch.setattr(repo, "save_simulation_release_binding", fail_pilot_binding)
    monkeypatch.setenv(pilot.APPLY_CONFIRM_ENV, pilot.APPLY_CONFIRM_VALUE)
    args = _args(apply=True)
    args.source_binding_id = source_binding.binding_id

    with pytest.raises(RuntimeError, match="injected binding persistence failure"):
        pilot.run(args)

    assert connection.commit_count == 0
    assert connection.rollback_count == 1
