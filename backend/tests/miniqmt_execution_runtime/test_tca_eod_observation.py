from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.qmt_strategy_ledger.tca_rebuild import TcaRebuildOutcome
from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadRuntimeConfig
from backend.services.simulation_runtime.tca_eod_observation import TcaEodObservationHook


TRADE_DATE = date(2026, 7, 10)


def test_eod_observation_is_disabled_by_default_and_does_not_call_rebuild() -> None:
    rebuild = _RebuildService()
    hook = TcaEodObservationHook(
        rebuild_service_factory=lambda: rebuild,
        config_provider=lambda: TcaReadRuntimeConfig.from_environ({}),
    )

    outcomes = hook.observe_post_reconciliation(
        lifecycle_scheduler=_Lifecycle(),
        terminalized_runs=({"run_id": "run-1", "post_close_terminalization": True},),
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 7, 10, 15, 5, tzinfo=UTC),
    )

    assert outcomes == (
        {
            "status": "DISABLED",
            "reason_code": "ADAPTIVE_IS_TCA_EOD_OBSERVATION_DISABLED",
            "stage": "TCA_EOD_CONFIG",
        },
    )
    assert rebuild.requests == []


def test_eod_observation_rebuilds_only_terminal_miniqmt_sim_scope() -> None:
    rebuild = _RebuildService()
    hook = TcaEodObservationHook(
        rebuild_service_factory=lambda: rebuild,
        config_provider=_enabled_config,
        environ={
            "MINIQMT_TCA_EOD_CODE_COMMIT": "a" * 40,
            "MINIQMT_TCA_EOD_OPERATOR_PSEUDONYM": "operator_test_v1",
        },
    )
    lifecycle = _Lifecycle()

    outcomes = hook.observe_post_reconciliation(
        lifecycle_scheduler=lifecycle,
        terminalized_runs=({"run_id": "run-1", "post_close_terminalization": True},),
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 7, 10, 15, 5, tzinfo=UTC),
    )

    assert outcomes[0]["status"] == "REBUILT"
    assert outcomes[0]["receipt_id"] == "receipt-1"
    assert len(rebuild.requests) == 1
    request = rebuild.requests[0]
    assert request.snapshot_kind == "RECONCILED_FINAL"
    assert request.scope.environment == "SIM"
    assert request.scope.binding_ids == ("binding-1",)
    assert request.scope.account_ids == ("account-raw",)
    assert request.account_pseudonyms["account-raw"].startswith("acct_test-v1_")
    assert lifecycle.repository.update_calls == []


def test_eod_observation_failure_is_loud_and_does_not_change_run_or_reconcile(caplog) -> None:
    rebuild = _RebuildService(error=RuntimeError("rebuild exploded"))
    lifecycle = _Lifecycle()
    hook = TcaEodObservationHook(
        rebuild_service_factory=lambda: rebuild,
        config_provider=_enabled_config,
        environ={
            "MINIQMT_TCA_EOD_CODE_COMMIT": "a" * 40,
            "MINIQMT_TCA_EOD_OPERATOR_PSEUDONYM": "operator_test_v1",
        },
    )

    with caplog.at_level(logging.ERROR):
        outcomes = hook.observe_post_reconciliation(
            lifecycle_scheduler=lifecycle,
            terminalized_runs=({"run_id": "run-1", "post_close_terminalization": True},),
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 7, 10, 15, 5, tzinfo=UTC),
        )

    assert outcomes[0]["status"] == "FAILED"
    assert outcomes[0]["reason_code"] == "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION"
    assert outcomes[0]["stage"] == "TCA_EOD_REBUILD"
    assert "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION" in caplog.text
    assert lifecycle.run.status == "SUCCEEDED"
    assert lifecycle.run.run_payload_json["reconcile_after_submit"]["run"]["status"] == "SUCCEEDED"
    assert lifecycle.repository.update_calls == []


def _enabled_config() -> TcaReadRuntimeConfig:
    version = {
        "calculator_version": "calculator-v1",
        "formula_version": "formula-v1",
        "schema_version": "schema-v1",
        "query_version": "query-v1",
        "benchmark_policy_version": "benchmark-v1",
        "mark_policy_version": "mark-v1",
        "fee_policy_version": "fee-v1",
        "trade_provenance_policy_version": "trade-v1",
    }
    return TcaReadRuntimeConfig.from_environ(
        {
            "MINIQMT_TCA_ACTIVE_READ_VERSION": json.dumps(
                {**version, "config_sha256": canonical_json_sha256(version)}, sort_keys=True
            ),
            "AISTOCK_TCA_EXPORT_HMAC_KEY": "test-key",
            "AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION": "test-v1",
            "MINIQMT_TCA_EOD_OBSERVATION_ENABLED": "true",
        }
    )


class _Lifecycle:
    def __init__(self) -> None:
        self.run = SimpleNamespace(
            run_id="run-1",
            binding_id="binding-1",
            trade_date=TRADE_DATE,
            status="SUCCEEDED",
            run_payload_json={"reconcile_after_submit": {"run": {"status": "SUCCEEDED"}}},
        )
        self.repository = _Repository(self.run)


class _Repository:
    def __init__(self, run: Any) -> None:
        self._run = run
        self.update_calls: list[dict[str, Any]] = []

    def get_simulation_daily_run(self, run_id: str) -> Any:
        assert run_id == self._run.run_id
        return self._run

    def get_simulation_release_binding(self, binding_id: str) -> Any:
        assert binding_id == self._run.binding_id
        return SimpleNamespace(
            binding_id="binding-1",
            broker_backend="miniqmt_sim",
            broker_account_id="account-raw",
        )

    def update_simulation_daily_run(self, **kwargs: Any) -> None:
        self.update_calls.append(kwargs)
        raise AssertionError("TCA EOD observation must never update simulation runs")


class _RebuildService:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.requests: list[Any] = []
        self.error = error

    def rebuild(self, request: Any) -> TcaRebuildOutcome:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return TcaRebuildOutcome(
            receipt_id="receipt-1",
            receipt_status="COMPLETED",
            reused=False,
            receipt_generation=1,
            result_ids=("result-1",),
            canonical_input_sha256="a" * 64,
            canonical_output_sha256="b" * 64,
        )
