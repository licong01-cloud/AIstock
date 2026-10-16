"""HMM-owned daily admission; no database, inference, or scheduler startup."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
from threading import get_ident

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.hmm_data_source import coefficient_extension as ext
from backend.services.hmm_data_source.daily_input import DailyInputError
from backend.services.hmm_training_service import HMMTrainingService


@pytest.fixture
def daily(tmp_path, monkeypatch):
    model = tmp_path / "models.json"
    model.write_text('{"frozen":true}', encoding="utf-8")
    config = {
        "strict_no_leakage": True,
        "seed": 42,
        "train_end": "2022-12-30",
        "val_end": "2024-06-28",
        "signal_presets": {"preset_A": {"trending": 1.05, "neutral": 1.0, "fading": 0.96}},
    }
    policy = ext.make_daily_subscription(
        config=config,
        snapshot_id="snapshot",
        model_sha256=ext.file_sha256(model),
        signal_presets=["preset_A"],
        enabled=True,
    )
    config["daily_coefficient_subscription"] = policy
    state = {
        "config": config,
        "jobs": {},
        "planned": 0,
        "executed": [],
        "completion": {
            "read_cutoff_at": "2026-10-14T20:00:00+08:00",
            "completion_authority": "market.dataset_date_refresh_audit",
            "completion_statuses": {"sector_data": {"refreshed_at": "2026-10-14T19:00:00+08:00"}},
        },
    }
    snapshot = {"snapshot_id": "snapshot", "config_id": "config", "status": "completed", "model_path": str(model)}
    svc = HMMTrainingService()

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, sql, params=()):
            self.sql, self.params = sql, params
            if sql.lstrip().startswith("UPDATE model_train_configs"):
                state["config"] = deepcopy(params[0].adapted)

        def fetchone(self):
            return {"config_json": deepcopy(state["config"])}

        def fetchall(self):
            if "FROM model_train_configs" in self.sql:
                if not state["config"].get("daily_coefficient_subscription", {}).get("enabled"):
                    return []
                return [{"config_id": "config", "config_json": deepcopy(state["config"])}]
            if "subscription_readiness_key" in self.sql:
                return [
                    {"job_id": job["job_id"]}
                    for job in state["jobs"].values()
                    if job["plan_json"].get("subscription_readiness_key") == self.params[2]
                    or (
                        "subscription_policy" not in job["plan_json"]
                        and job["plan_json"]["snapshot_id"] == self.params[3]
                        and job["plan_json"]["signal_preset"] == self.params[4]
                        and job["plan_json"]["as_of_trade_date"] == self.params[5].isoformat()
                        and job["plan_json"]["effective_trade_date"] == self.params[6].isoformat()
                        and job["plan_json"]["generator_sha256"] == self.params[7]
                        and job["plan_json"]["database_completion"]["completion_statuses"] == self.params[8].adapted
                    )
                ]
            if "daily_key" in self.sql:
                return [
                    {"job_id": job["job_id"]}
                    for job in state["jobs"].values()
                    if job["plan_json"]["daily_key"] == self.params[-1]
                ]
            raise AssertionError(self.sql)

    @contextmanager
    def connection(**_):
        state.setdefault("transactions", []).append(_)
        yield type("Connection", (), {"cursor": lambda *_args, **_kwargs: Cursor()})()

    def plan(**kwargs):
        state["planned"] += 1
        return {
            "config_id": "config",
            "snapshot_id": "snapshot",
            "signal_preset": "preset_A",
            "source_mode": "database_daily",
            "generation_mode": "daily_asof_prediction_v1",
            "generation_available": True,
            "model_sha256": ext.file_sha256(model),
            "model_path": str(model),
            "generator_sha256": "a" * 64,
            "config_json": deepcopy(state["config"]),
            "input_identity": deepcopy(state["completion"]),
            "database_completion": deepcopy(state["completion"]),
            "as_of_trade_date": "2026-10-14",
            "effective_trade_date": "2026-10-15",
            "daily_key": str(state["planned"]),
        }

    def insert(plan, connection=None):
        job = {"job_id": plan["daily_key"], "status": "PENDING", "plan_json": deepcopy(plan)}
        state["jobs"][job["job_id"]] = job
        return deepcopy(job)

    def execute(job_id):
        state["executed"].append(job_id)
        state["jobs"][job_id]["status"] = "COMPLETED"

    def poison(*_, **__):
        raise AssertionError("forbidden file fallback / training")

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", connection)
    monkeypatch.setattr(svc, "_get_config", lambda _: {"config_json": deepcopy(state["config"])})
    monkeypatch.setattr(svc, "get_snapshot", lambda _: deepcopy(snapshot))
    monkeypatch.setattr(
        svc, "_database_daily_completion", lambda **_: (date(2026, 10, 14), deepcopy(state["completion"]))
    )
    monkeypatch.setattr(svc, "_next_trading_day", lambda _: date(2026, 10, 15))
    monkeypatch.setattr(svc, "_daily_generator_sha256", lambda: "a" * 64)
    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", plan)
    monkeypatch.setattr(svc, "_insert_daily_coefficient_job", insert)
    monkeypatch.setattr(svc, "get_daily_coefficient_job", lambda job: deepcopy(state["jobs"].get(job)))
    monkeypatch.setattr(svc, "run_daily_coefficients_job", execute)
    monkeypatch.setattr(svc, "trigger_training", poison)
    monkeypatch.setattr(svc, "run_training", poison)
    monkeypatch.setattr(ext, "resolve_active_input", poison)
    return svc, state, policy, model


def test_subscription_preserves_model_identity_and_does_not_enable_file_policy(daily):
    svc, state, policy, _ = daily
    before = ext.config_fingerprint(state["config"])
    saved = svc.configure_daily_coefficient_subscription(
        "config",
        snapshot_id="snapshot",
        signal_presets=["preset_A"],
        enabled=True,
    )
    assert saved["policy"] == policy
    assert policy["source_mode"] == "database_daily"
    assert ext.config_fingerprint(state["config"]) == before
    assert "coefficient_extension" not in state["config"]
    assert svc.get_daily_coefficient_subscription("config")["policy"] == policy
    assert {"autocommit": False, "manage_transaction": True} in state["transactions"]


def test_daily_enrollment_preserves_an_existing_file_policy(daily):
    svc, state, _, model = daily
    file_policy = ext.make_policy(
        config=state["config"],
        snapshot_id="snapshot",
        model_sha256=ext.file_sha256(model),
        coefficient_start="2024-07-01",
        signal_presets=["preset_A"],
        enabled=True,
    )
    state["config"]["coefficient_extension"] = deepcopy(file_policy)
    svc.configure_daily_coefficient_subscription(
        "config",
        snapshot_id="snapshot",
        signal_presets=["preset_A"],
        enabled=True,
    )
    assert state["config"]["coefficient_extension"] == file_policy
    ext.validate_policy(state["config"], file_policy)


def test_disable_does_not_need_a_model_file_or_rebind_identity(daily):
    svc, state, policy, model = daily
    model.unlink()
    saved = svc.configure_daily_coefficient_subscription(
        "config",
        snapshot_id="snapshot",
        signal_presets=["preset_A"],
        enabled=False,
    )
    assert saved["policy"] == {**policy, "enabled": False}
    assert not state["jobs"]


def test_completed_watermark_is_reused_without_preflight_or_inference(daily):
    svc, state, _, _ = daily
    assert svc.reconcile_daily_coefficient_subscriptions()[0]["status"] == "COMPLETED"
    state["completion"]["read_cutoff_at"] = "2026-10-14T21:00:00+08:00"
    assert svc.reconcile_daily_coefficient_subscriptions()[0]["status"] == "COMPLETED"
    assert state["planned"] == 1 and state["executed"] == ["1"]


def test_new_completion_audit_requires_new_full_plan(daily):
    svc, state, _, _ = daily
    svc.reconcile_daily_coefficient_subscriptions()
    state["completion"]["completion_statuses"]["sector_data"]["refreshed_at"] = "2026-10-14T20:00:00+08:00"
    svc.reconcile_daily_coefficient_subscriptions()
    assert state["planned"] == 2 and state["executed"] == ["1", "2"]


def test_existing_manual_daily_job_is_reused_without_replanning_or_policy_mutation(daily):
    svc, state, _, _ = daily
    svc.start_daily_coefficients_job("snapshot", signal_preset="preset_A", confirm_generate=True)
    svc.run_daily_coefficients_job("1")
    original = deepcopy(state["jobs"]["1"])
    for _ in range(2):
        assert svc.reconcile_daily_coefficient_subscriptions()[0] == original
    assert state["planned"] == 1 and state["executed"] == ["1"]
    assert state["jobs"]["1"] == original


def test_unknown_cached_status_is_blocked_not_a_success(daily):
    svc, state, _, _ = daily
    svc.reconcile_daily_coefficient_subscriptions()
    state["jobs"]["1"]["status"] = "UNKNOWN"
    state["executed"].clear()
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and result["reason_code"] == "hmm_daily_job_identity_conflict"
    assert not state["executed"]


@pytest.mark.parametrize("status", ["FAILED", "RUNNING", "PENDING"])
def test_existing_status_uses_existing_worker_contract(daily, status):
    svc, state, _, _ = daily
    svc.reconcile_daily_coefficient_subscriptions()
    state["jobs"]["1"]["status"] = status
    state["executed"].clear()
    svc.reconcile_daily_coefficient_subscriptions()
    assert state["planned"] == 1
    assert state["executed"] == ([] if status == "FAILED" else ["1"])


def test_file_enrollment_never_implicitly_enables_database_subscription(daily):
    svc, state, _, _ = daily
    state["config"]["coefficient_extension"] = {"enabled": True}
    del state["config"]["daily_coefficient_subscription"]
    assert svc.reconcile_daily_coefficient_subscriptions() == []
    assert state["planned"] == 0


@pytest.mark.parametrize("drift", ["model", "parameters", "schema"])
def test_identity_drift_blocks_without_job_or_alternative_model(daily, drift):
    svc, state, _, model = daily
    if drift == "model":
        model.write_bytes(b"changed")
    elif drift == "parameters":
        state["config"]["seed"] = 43
    else:
        state["config"]["daily_coefficient_subscription"]["schema_version"] = "unknown"
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "hmm_daily_subscription_identity_changed"
    assert not state["jobs"]


def test_invalid_input_is_not_reported_as_waiting_or_success(daily, monkeypatch):
    svc, state, _, _ = daily

    def invalid(**_):
        raise DailyInputError("hmm_database_daily_input_unavailable", "nonfinite required quote")

    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", invalid)
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and "nonfinite" in result["error"]
    assert not state["jobs"]


def test_stale_completion_waits_without_scanning_or_shifting_effective_day(daily):
    svc, state, _, _ = daily
    state["completion"]["read_cutoff_at"] = "2026-10-16T12:00:00+08:00"
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "WAITING"
    assert result["reason_code"] == "hmm_daily_historical_as_known_unavailable"
    assert state["planned"] == 0 and not state["jobs"]


def test_cached_job_cannot_borrow_another_sources_identity(daily):
    svc, state, _, _ = daily
    svc.reconcile_daily_coefficient_subscriptions()
    state["jobs"]["1"]["plan_json"]["source_mode"] = "qe_frozen_file"
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and result["reason_code"] == "hmm_daily_job_identity_conflict"
    assert state["planned"] == 1 and state["executed"] == ["1"]


def test_unexpected_exception_is_typed_not_reported_as_empty_success(daily, monkeypatch):
    svc, state, _, _ = daily
    monkeypatch.setattr(
        svc, "_database_daily_completion", lambda **_: (_ for _ in ()).throw(ValueError("broken input"))
    )
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and result["reason_code"] == "hmm_daily_subscription_failed"
    assert not state["jobs"]


def test_disable_between_preflight_and_admission_blocks(daily, monkeypatch):
    svc, state, _, _ = daily
    original = svc._resolve_daily_coefficient_plan

    def concurrent_disable(**kwargs):
        result = original(**kwargs)
        state["config"]["daily_coefficient_subscription"]["enabled"] = False
        return result

    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", concurrent_disable)
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and not state["jobs"]


@pytest.mark.parametrize("field,value", [("source_mode", "qe_frozen_file"), ("snapshot_id", "other-snapshot")])
def test_preflight_cannot_admit_a_different_source_or_snapshot(daily, monkeypatch, field, value):
    svc, state, _, _ = daily
    original = svc._resolve_daily_coefficient_plan

    def wrong_plan(**kwargs):
        result = original(**kwargs)
        result[field] = value
        return result

    monkeypatch.setattr(svc, "_resolve_daily_coefficient_plan", wrong_plan)
    result = svc.reconcile_daily_coefficient_subscriptions()[0]
    assert result["status"] == "BLOCKED" and not state["jobs"]


def test_public_readiness_entry_is_nonblocking_and_has_no_training_side_effect(monkeypatch):
    from backend.routers import hmm_training

    main_thread, calls = get_ident(), []

    class Service:
        def reconcile_coefficient_extensions(self):
            calls.append(("file", get_ident()))
            return []

        def reconcile_daily_coefficient_subscriptions(self):
            calls.append(("database", get_ident()))
            return []

        def trigger_training(self, *_):
            pytest.fail("training must never be called")

    monkeypatch.setattr(hmm_training, "HMMTrainingService", Service)
    asyncio.run(hmm_training.coefficient_readiness_tick())
    assert [name for name, _ in calls] == ["file", "database"]
    assert all(thread != main_thread for _, thread in calls)


def test_file_reconcile_failure_does_not_mask_database_reconcile(monkeypatch):
    from backend.routers import hmm_training

    class Service:
        def reconcile_coefficient_extensions(self):
            raise RuntimeError("file source unavailable")

        def reconcile_daily_coefficient_subscriptions(self):
            return [{"source_mode": "database_daily", "status": "WAITING"}]

    monkeypatch.setattr(hmm_training, "HMMTrainingService", Service)
    results = asyncio.run(hmm_training.coefficient_readiness_tick())
    assert results == [
        {"source_mode": "qe_frozen_file", "status": "BLOCKED", "reason_code": "hmm_coefficient_readiness_failed"},
        {"source_mode": "database_daily", "status": "WAITING"},
    ]


def test_shared_completion_binding_survives_missing_legacy_apscheduler(tmp_path, monkeypatch):
    import sys
    from backend.ingestion import tdx_scheduler
    from backend.routers import hmm_training
    from backend.services.hmm_data_source.daily_input import REQUIRED_DAILY_DATASETS

    monkeypatch.setattr(tdx_scheduler, "DEFAULT_TEST_OUTPUT_DIR", tmp_path)
    owner = tdx_scheduler.TDXScheduler()
    monkeypatch.setattr(owner, "_fetchall", lambda *_: [])
    monkeypatch.setattr(tdx_scheduler, "scheduler", owner)
    monkeypatch.setitem(sys.modules, "apscheduler.schedulers.asyncio", None)
    monkeypatch.setattr(hmm_training, "_scheduler", None)
    monkeypatch.setattr(hmm_training, "_completion_scheduler", None)
    calls = []

    class Service:
        def reconcile_coefficient_extensions(self):
            pytest.fail("database completion must not dispatch frozen-file extension")

        def reconcile_daily_coefficient_subscriptions(self):
            calls.append(get_ident())
            return [{"status": "WAITING", "reason_code": "hmm_database_daily_input_unavailable"}]

    monkeypatch.setattr(hmm_training, "HMMTrainingService", Service)

    async def scenario():
        hmm_training.init_hmm_scheduler()
        hmm_training.init_hmm_scheduler()
        assert len(owner._completion_callbacks) == 1
        entry = owner._completion_callbacks[hmm_training._HMM_COMPLETION_CALLBACK_ID]
        assert entry.datasets == tuple(sorted(REQUIRED_DAILY_DATASETS))
        owner._dispatch_completion_callbacks()
        await asyncio.wrap_future(entry.future)
        owner._dispatch_completion_callbacks()
        assert len(calls) == 1
        assert calls[0] != get_ident()
        hmm_training.shutdown_hmm_scheduler()
        assert not owner._completion_callbacks
        assert hmm_training._completion_scheduler is None

    try:
        asyncio.run(scenario())
    finally:
        owner.shutdown()


@pytest.mark.parametrize(
    "status,reason,accepted",
    [
        ("BLOCKED", "hmm_coefficient_readiness_failed", False),
        ("BLOCKED", "hmm_daily_subscription_failed", False),
        ("BLOCKED", "hmm_database_daily_input_unavailable", True),
        ("BLOCKED", "hmm_daily_job_identity_conflict", True),
        ("BLOCKED", "hmm_daily_subscription_identity_changed", True),
        ("BLOCKED", "another_typed_business_block", True),
        ("WAITING", "hmm_daily_historical_as_known_unavailable", True),
        ("FAILED", "hmm_daily_subscription_failed", True),
        ("FAILED", "hmm_coefficient_readiness_failed", True),
    ],
)
def test_completion_adapter_retries_only_orchestration_failure(monkeypatch, status, reason, accepted):
    from backend.routers import hmm_training

    calls = []

    async def tick(**kwargs):
        calls.append(kwargs)
        return [{"status": status, "reason_code": reason}]

    monkeypatch.setattr(hmm_training, "coefficient_readiness_tick", tick)
    assert asyncio.run(hmm_training._coefficient_completion_callback()) is accepted
    assert calls == [{"source_modes": ("database_daily",)}]


@pytest.mark.parametrize("failure_site", ["_database_daily_completion", "get_daily_coefficient_job"])
def test_same_watermark_retries_internal_database_error_without_reexecuting_job(daily, tmp_path, monkeypatch, failure_site):
    import psycopg2
    from backend.ingestion import tdx_scheduler
    from backend.routers import hmm_training

    svc, state, _, _ = daily
    monkeypatch.setattr(tdx_scheduler, "DEFAULT_TEST_OUTPUT_DIR", tmp_path)
    owner = tdx_scheduler.TDXScheduler()
    rows = [{"dataset": "sector_data", "trade_date": "2026-10-14", "status": "success", "row_count": 131}]
    monkeypatch.setattr(owner, "_fetchall", lambda *_: deepcopy(rows))
    monkeypatch.setattr(hmm_training, "HMMTrainingService", lambda: svc)
    original_lookup = getattr(svc, failure_site)
    failures = []

    def transient_lookup(*args, **kwargs):
        if not failures:
            failures.append(failure_site)
            raise psycopg2.OperationalError("temporary subscription database outage")
        return original_lookup(*args, **kwargs)

    monkeypatch.setattr(svc, failure_site, transient_lookup)
    original_reconcile = svc.reconcile_daily_coefficient_subscriptions
    results = []

    def reconcile():
        result = original_reconcile()
        results.append(result)
        return result

    monkeypatch.setattr(svc, "reconcile_daily_coefficient_subscriptions", reconcile)

    async def scenario():
        owner.register_completion_callback(
            "hmm", datasets=("sector_data",), callback=hmm_training._coefficient_completion_callback
        )
        entry = owner._completion_callbacks["hmm"]
        owner._dispatch_completion_callbacks()
        await asyncio.wrap_future(entry.future)
        assert results[0][0]["status"] == "BLOCKED"
        assert results[0][0]["reason_code"] == "hmm_daily_subscription_failed"
        assert entry.future.result() is False
        assert entry.completed_watermark != entry.dispatched_watermark
        watermark, version = entry.dispatched_watermark, entry.request_version

        owner._dispatch_completion_callbacks()
        await asyncio.wrap_future(entry.future)
        owner._dispatch_completion_callbacks()
        assert len(results) == 2
        assert results[1][0]["status"] == "COMPLETED"
        assert entry.completed_watermark == watermark
        assert entry.request_version == version
        assert entry.future is None
        assert state["planned"] == 1 and state["executed"] == ["1"]
        assert len(state["jobs"]) == 1
        assert rows == [{"dataset": "sector_data", "trade_date": "2026-10-14", "status": "success", "row_count": 131}]

    try:
        asyncio.run(scenario())
    finally:
        owner.shutdown()


def test_enabling_subscription_wakes_owner_without_running_reconcile(monkeypatch):
    from types import SimpleNamespace
    from backend.routers import hmm_training

    calls = []
    owner = SimpleNamespace(request_completion_check=lambda name: calls.append(name) or True)
    monkeypatch.setattr(hmm_training, "_completion_scheduler", owner)
    monkeypatch.setattr(
        hmm_training.service, "configure_daily_coefficient_subscription", lambda *_args, **kwargs: kwargs
    )
    request = hmm_training.DailyCoefficientSubscriptionRequest(
        snapshot_id="snapshot", signal_presets=["preset_A"], enabled=True
    )
    result = hmm_training.configure_daily_coefficient_subscription("config", request)
    assert result["enabled"] is True
    assert calls == [hmm_training._HMM_COMPLETION_CALLBACK_ID]
    request.enabled = False
    hmm_training.configure_daily_coefficient_subscription("config", request)
    assert len(calls) == 1


def test_readiness_status_is_read_only_and_rejects_inactive_binding(monkeypatch):
    from types import SimpleNamespace
    from backend.routers import hmm_training

    state = {
        "registered": True,
        "owner_running": True,
        "loop_running": True,
        "in_flight": False,
        "datasets": ["index_daily", "sector_data", "sw_daily"],
    }
    owner = SimpleNamespace(completion_callback_status=lambda _name: dict(state))
    monkeypatch.setattr(hmm_training, "_completion_scheduler", owner)
    monkeypatch.setattr(
        hmm_training.service,
        "reconcile_daily_coefficient_subscriptions",
        lambda: pytest.fail("status probe must not generate or query inputs"),
    )
    app = FastAPI()
    app.include_router(hmm_training.router, prefix="/api/v1")
    client = TestClient(app)
    url = "/api/v1/runtime-contracts/hmm-database-daily-dispatch"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json()["dispatch_binding_status"] == "ready"
    for key in ("registered", "owner_running", "loop_running"):
        state[key] = False
        response = client.get(url)
        assert response.status_code == 503
        assert response.json()["detail"]["reason_code"] == "hmm_completion_dispatch_unavailable"
        state[key] = True


def test_subscription_api_is_explicit_and_rejects_coerced_enable(monkeypatch):
    from backend.routers import hmm_training

    calls = []
    monkeypatch.setattr(
        hmm_training.service,
        "configure_daily_coefficient_subscription",
        lambda config, **kwargs: calls.append((config, kwargs)) or {"policy": kwargs},
    )
    app = FastAPI()
    app.include_router(hmm_training.router, prefix="/api/v1")
    client = TestClient(app)
    url = "/api/v1/hmm-training/configs/config/daily-coefficient-subscription"
    data = {"snapshot_id": "snapshot", "signal_presets": ["preset_A"], "enabled": True}
    assert client.post(url, json=data).status_code == 200
    assert calls == [("config", data)]
    assert client.post(url, json={**data, "enabled": "true"}).status_code == 422
    assert client.post(url, json={**data, "coefficient_start": "2024-07-01"}).status_code == 422
