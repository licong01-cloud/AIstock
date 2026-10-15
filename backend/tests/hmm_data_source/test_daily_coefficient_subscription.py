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
