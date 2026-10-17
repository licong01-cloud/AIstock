from __future__ import annotations

import asyncio
import hashlib
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.hmm_data_source import coefficient_extension as ext
from backend.services.hmm_training_service import HMMTrainingService


def _profile(tmp_path: Path, generation: str = "release-a", end: str = "2024-07-03"):
    root = tmp_path / generation
    root.mkdir()
    specs = {}
    for component in ext._FILE_COMPONENTS.values():
        path = root / f"{component}.data"
        path.write_bytes(component.encode())
        specs[component] = {"path": path.name, "sha256": ext.file_sha256(path)}
    calendar = root / "components/daily_bin_candidate/calendars/day.txt"
    calendar.parent.mkdir(parents=True)
    days = ["2024-07-01", "2024-07-02", "2024-07-03"]
    if end != days[-1]:
        days.append(end)
    calendar.write_text("\n".join(days) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "dataset_manifest_sha256": hashlib.sha256(generation.encode()).hexdigest(),
        "release_id": "qe_hmm_full_v2_test",
        "cutoff_trade_date": end,
        "components": specs,
        "qlib_calendar_sha256": ext.file_sha256(calendar),
    }
    path = root / "qe_dataset_manifest.json"
    path.write_bytes(ext.canonical_bytes(manifest))
    pins = {
        pin: specs[ext._FILE_COMPONENTS[key]]["sha256"]
        for key, pin in {
            "sector_data_h5": "sector_data_sha256",
            "sector_code_map_json": "code_map_sha256",
            "market_context_parquet": "market_context_sha256",
            "sector_membership_spans_parquet": "membership_sha256",
            "sector_quote_availability_json": "quote_availability_sha256",
        }.items()
    }
    pins["market_volume_definition"] = "sum_market_sw_daily_vol_all_rows_v1"
    return SimpleNamespace(
        controller_candidate_root=root,
        generation=generation,
        release_id=manifest["release_id"],
        cutoff=date.fromisoformat(end),
        profile_sha256="a" * 64,
        defaults={"test_start": "2024-07-01", "backtest_end": days[-2]},
        raw={
            "components": {
                "dataset_manifest_sha256": manifest["dataset_manifest_sha256"],
                "dataset_manifest_file_sha256": ext.file_sha256(path),
                "sector_context_pins": pins,
                "index_pins": {"sha256": specs["index_daily"]["sha256"]},
            },
            "node_bindings": {"wsl2-5080": {"candidate_root": f"/mnt/shared/{generation}"}},
        },
    )


def _plan(tmp_path: Path, profile=None):
    model = tmp_path / "models.json"
    model.write_text('{"801783.SI":{"frozen":true}}', encoding="utf-8")
    config = {
        "train_end": "2022-12-30",
        "val_end": "2024-06-28",
        "strict_no_leakage": True,
        "n_states": 3,
        "seed": 42,
        "signal_presets": {"preset_A": {"trending": 1.05, "neutral": 1.0, "fading": 0.96}},
    }
    policy = ext.make_policy(
        config=config,
        snapshot_id="snapshot-fixed",
        model_sha256=ext.file_sha256(model),
        coefficient_start="2024-07-01",
        signal_presets=["preset_A"],
        enabled=True,
    )
    config["coefficient_extension"] = policy
    config["coefficient_windows"] = [
        {
            "snapshot_id": policy["snapshot_id"],
            "preset": "preset_A",
            "strict_no_leakage": True,
            "test_start": "2024-07-01",
            "backtest_end": "2024-07-02",
            "dataset_manifest_identity": "old",
        }
    ]
    snapshot = {
        "snapshot_id": policy["snapshot_id"],
        "config_id": "config-fixed",
        "status": "completed",
        "model_path": str(model),
    }
    inputs = ext.resolve_active_input(profile or _profile(tmp_path))
    plan = ext.build_plan(
        config_id="config-fixed",
        config=config,
        snapshot=snapshot,
        signal_preset="preset_A",
        preset_coeffs=config["signal_presets"]["preset_A"],
        inputs=inputs,
    )
    return plan, config, snapshot, inputs


def _payload(plan):
    dates = plan["trade_dates"]
    return {
        "dataset_identity": plan["dataset_identity"],
        "model_sha256": plan["model_sha256"],
        "preset_key": plan["signal_preset"],
        "preset_coeffs": plan["preset_coeffs"],
        "input_file_sha256": {k: v["sha256"] for k, v in plan["frozen_input_bundle"]["files"].items()},
        "daily_coefficients": {d: {"801783.SI": 1.05} for d in dates},
        "stock_sector_membership_spans": {
            "000001.SZ": [{"from": dates[0], "to": dates[-1], "sector_code": "801783.SI"}],
            "000002.SZ": [{"from": dates[0], "to": dates[-1], "sector_code": "801019.SI"}],
        },
        "quote_unavailable_sector_codes_by_date": {d: ["801019.SI"] for d in dates},
        "input_data_max_dates_by_date": {d: {"sector_data": d, "index_daily": d, "market_context": d} for d in dates},
    }


def test_monthly_release_extends_same_model_and_keeps_old_exact_windows(tmp_path):
    old, config, snapshot, _ = _plan(tmp_path)
    new_input = ext.resolve_active_input(_profile(tmp_path, "release-b", "2024-07-04"))
    new = ext.build_plan(
        config_id="config-fixed",
        config=config,
        snapshot=snapshot,
        signal_preset="preset_A",
        preset_coeffs=old["preset_coeffs"],
        inputs=new_input,
    )
    assert old["model_sha256"] == new["model_sha256"]
    assert old["policy"] == new["policy"]
    assert new["test_start"] == old["test_start"]
    assert new["backtest_end"] == "2024-07-04"
    assert new["frozen_input_bundle"]["dataset_root"] == "/mnt/shared/release-b"
    assert new["frozen_input_bundle"]["market_volume_definition"] == "sum_market_sw_daily_vol_all_rows_v1"
    assert new["extension_key"] != old["extension_key"]
    assert ["2024-07-01", "2024-07-02"] in new["windows"]
    assert ["2024-07-01", "2024-07-03"] in new["windows"]  # Current portfolio end, not the signal cutoff.
    assert new == ext.build_plan(
        config_id="config-fixed",
        config=config,
        snapshot=snapshot,
        signal_preset="preset_A",
        preset_coeffs=old["preset_coeffs"],
        inputs=new_input,
    )


def test_extension_start_cannot_relabel_post_fit_history_as_no_leakage(tmp_path):
    plan, config, *_ = _plan(tmp_path)
    config["train_end"] = "2024-07-02"
    with pytest.raises(ext.CoefficientExtensionError, match="model_asof_invalid"):
        ext.make_policy(
            config=config,
            snapshot_id=plan["snapshot_id"],
            model_sha256=plan["model_sha256"],
            coefficient_start=plan["test_start"],
            signal_presets=["preset_A"],
            enabled=True,
        )


@pytest.mark.parametrize("change", ["weights", "parameter", "preset", "policy_schema", "disabled"])
def test_model_contract_drift_never_selects_or_trains_another_model(tmp_path, change):
    plan, config, snapshot, inputs = _plan(tmp_path)
    if change == "weights":
        Path(snapshot["model_path"]).write_text("changed", encoding="utf-8")
    elif change == "parameter":
        config["n_states"] = 2
    elif change == "preset":
        config["signal_presets"]["preset_A"]["trending"] = 1.2
    elif change == "policy_schema":
        config["coefficient_extension"]["schema_version"] = "unknown"
    else:
        config["coefficient_extension"]["enabled"] = False
    with pytest.raises(ext.CoefficientExtensionError):
        ext.build_plan(
            config_id="config-fixed",
            config=config,
            snapshot=snapshot,
            signal_preset="preset_A",
            preset_coeffs=plan["preset_coeffs"],
            inputs=inputs,
        )


@pytest.mark.parametrize("damage", ["manifest", "component_missing", "escape", "calendar_gap", "profile_pin"])
def test_release_errors_never_fall_back_to_an_old_root(tmp_path, damage):
    profile = _profile(tmp_path)
    manifest_path = profile.controller_candidate_root / "qe_dataset_manifest.json"
    if damage == "manifest":
        profile.raw["components"]["dataset_manifest_file_sha256"] = "0" * 64
    elif damage == "component_missing":
        (profile.controller_candidate_root / "market_context.data").unlink()
    elif damage == "profile_pin":
        profile.raw["components"]["sector_context_pins"]["membership_sha256"] = "0" * 64
    else:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if damage == "escape":
            manifest["components"]["sector_data"]["path"] = "../external.data"
        else:
            calendar = profile.controller_candidate_root / "components/daily_bin_candidate/calendars/day.txt"
            calendar.write_text("2024-07-01\n2024-07-02\n", encoding="utf-8")
            manifest["qlib_calendar_sha256"] = ext.file_sha256(calendar)
        manifest_path.write_bytes(ext.canonical_bytes(manifest))
        profile.raw["components"]["dataset_manifest_file_sha256"] = ext.file_sha256(manifest_path)
    with pytest.raises((ext.CoefficientExtensionError, OSError)):
        ext.resolve_active_input(profile)


def test_crop_preserves_legal_unavailable_and_does_not_materialize_stock_day_panel(tmp_path):
    plan, *_ = _plan(tmp_path)
    result = ext.window_payload(_payload(plan), plan, "2024-07-01", "2024-07-02")
    assert list(result["daily_coefficients"]) == ["2024-07-01", "2024-07-02"]
    assert "801019.SI" not in result["daily_coefficients"]["2024-07-02"]
    assert result["stock_sector_membership_spans"]["000002.SZ"][0]["to"] == "2024-07-02"
    assert "stock_sector_map_by_date" not in result


@pytest.mark.parametrize(
    "damage", ["missing_date", "nan", "unknown_gap", "fabricated_quote", "future", "input_hash", "model_hash"]
)
def test_generator_readback_must_close_identity_calendar_and_quote_contract(tmp_path, damage):
    plan, *_ = _plan(tmp_path)
    payload = _payload(plan)
    d = plan["trade_dates"][0]
    if damage == "missing_date":
        payload["daily_coefficients"].pop(d)
    elif damage == "nan":
        payload["daily_coefficients"][d]["801783.SI"] = float("nan")
    elif damage == "unknown_gap":
        payload["quote_unavailable_sector_codes_by_date"][d] = []
    elif damage == "fabricated_quote":
        payload["daily_coefficients"][d]["801019.SI"] = 1.0
    elif damage == "future":
        payload["input_data_max_dates_by_date"][d]["market_context"] = "2025-01-01"
    elif damage == "input_hash":
        payload["input_file_sha256"]["sector_data_h5"] = "0" * 64
    else:
        payload["model_sha256"] = "0" * 64
    with pytest.raises(ext.CoefficientExtensionError):
        ext.window_payload(payload, plan, plan["test_start"], plan["backtest_end"])


def test_immutable_publish_is_idempotent_and_matches_existing_qe_reader(tmp_path, monkeypatch):
    plan, *_ = _plan(tmp_path)
    payload = _payload(plan)
    window = ext.publish_window(payload, plan, "2024-07-01", "2024-07-02")
    assert window == ext.publish_window(payload, plan, "2024-07-01", "2024-07-02")
    asset = Path(plan["model_path"]).parent / window["coefficient_filename"]
    assert ext.file_sha256(asset) == window["coefficient_sha256"]
    from backend.services.quantevolver import config_composer as composer_module

    composer = object.__new__(composer_module.ConfigComposer)
    monkeypatch.setattr(composer, "_local_hmm_artifact_path", lambda _: Path(plan["model_path"]))
    monkeypatch.setattr(composer_module, "ensure_aistock_artifact_path", lambda path, **_: path)
    summary = dict(plan["dataset_identity"], profile_sha256=plan["dataset_identity"]["active_profile_sha256"])
    from backend.services.quantevolver.qe_active_dataset_profile import QE_ACTIVE_PROFILE_SUMMARY_PARAM

    content = composer._precompute_hmm_coefficients(
        {
            "sector_hmm_model_path": plan["model_path"],
            "hmm_signal_preset": "preset_A",
            "hmm_config_json": {"strict_no_leakage": True, "coefficient_windows": [window]},
            QE_ACTIVE_PROFILE_SUMMARY_PARAM: summary,
        },
        {"test_start": "2024-07-01", "backtest_end": "2024-07-02"},
    )
    assert json.loads(content)["backtest_end"] == "2024-07-02"
    original = asset.read_bytes()
    asset.write_bytes(b"collision")
    with pytest.raises(ext.CoefficientExtensionError, match="overwrite"):
        ext.publish_window(payload, plan, "2024-07-01", "2024-07-02")
    assert asset.read_bytes() == b"collision" and original != b"collision"


def test_executor_uses_file_only_worker_and_registers_all_exact_windows(tmp_path, monkeypatch):
    plan, *_ = _plan(tmp_path)
    service = HMMTrainingService()
    checks, registrations, calls = [], [], []
    monkeypatch.setattr(service, "_assert_coefficient_extension_current", lambda p: checks.append(p["extension_key"]))
    monkeypatch.setattr(service, "_register_coefficient_extension_windows", lambda p, w: registrations.extend(w))
    monkeypatch.setattr(service, "_to_wsl_path", lambda path: path)

    def worker(command, **kwargs):
        calls.append((command, json.loads(kwargs["input"])))
        Path(command[-1]).write_bytes(ext.canonical_bytes(_payload(plan)))
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("backend.services.hmm_training_service.subprocess.run", worker)
    result = service._execute_coefficient_extension_plan(plan)
    assert len(calls) == 1 and "PYTHONPATH=" in " ".join(calls[0][0])
    assert calls[0][1]["frozen_input_bundle"] == plan["frozen_input_bundle"]
    assert not any(k.startswith("db_") for k in calls[0][1])
    assert result["training_performed"] is False and len(registrations) == 2
    assert len(checks) == 2
    assert not list(tmp_path.glob("*.pending"))
    service._execute_coefficient_extension_plan(plan)
    assert len(calls) == 2  # Recovered RUNNING jobs may repeat inference, never fit.


@pytest.mark.parametrize("field", ["output_path", "frozen_input_bundle", "config_json", "windows"])
def test_persisted_plan_cannot_change_inputs_paths_or_parameters_under_same_key(tmp_path, monkeypatch, field):
    plan, config, snapshot, inputs = _plan(tmp_path)
    service = HMMTrainingService()
    monkeypatch.setattr(service, "_get_config", lambda _: {"config_json": config})
    monkeypatch.setattr(service, "get_snapshot", lambda _: snapshot)
    monkeypatch.setattr(ext, "resolve_active_input", lambda: inputs)
    service._assert_coefficient_extension_current(plan)
    tampered = deepcopy(plan)
    if field == "output_path":
        tampered[field] = str(tmp_path / "foreign.json")
    elif field == "frozen_input_bundle":
        tampered[field]["dataset_root"] = "/old-release"
    elif field == "config_json":
        tampered[field]["seed"] = 99
    else:
        tampered[field] = [["2024-07-02", "2024-07-03"]]
    with pytest.raises(RuntimeError, match="binding_changed"):
        service._assert_coefficient_extension_current(tampered)


def test_interrupted_atomic_publish_leaves_no_partial_final_asset(tmp_path, monkeypatch):
    plan, *_ = _plan(tmp_path)
    monkeypatch.setattr(ext.os, "link", lambda *args: (_ for _ in ()).throw(OSError("publication interrupted")))
    with pytest.raises(OSError, match="interrupted"):
        ext.publish_window(_payload(plan), plan, plan["test_start"], plan["backtest_end"])
    assert not list(tmp_path.glob("coefficients_*.json"))
    assert not list(tmp_path.glob(".hmm-coefficient-*.pending"))


@pytest.mark.parametrize(
    "lock_available,job_status", [(False, "RUNNING"), (True, "RUNNING"), (True, "FAILED"), (True, "COMPLETED")]
)
def test_worker_lock_crash_recovery_and_failed_job_stop(tmp_path, monkeypatch, lock_available, job_status):
    plan, *_ = _plan(tmp_path)
    service = HMMTrainingService()
    events = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params):
            events.append(query.split("(")[0])

        def fetchone(self):
            return (lock_available,)

    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=lambda **_: Cursor())

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", connection)
    monkeypatch.setattr(service, "get_daily_coefficient_job", lambda _: {"status": job_status, "plan_json": plan})
    monkeypatch.setattr(service, "_mark_daily_coefficient_job_running", lambda _: events.append("running"))
    monkeypatch.setattr(service, "_execute_coefficient_extension_plan", lambda p: {"output_path": p["output_path"]})
    monkeypatch.setattr(service, "_complete_daily_coefficient_job", lambda *args: events.append("completed"))
    service._run_coefficient_extension_job("job")
    assert ("completed" in events) is (lock_available and job_status == "RUNNING")
    assert ("SELECT pg_advisory_unlock" in events) is lock_available


def test_scheduler_is_nonblocking_and_never_calls_training(monkeypatch):
    from backend.routers import hmm_training

    calls = []

    class Service:
        def reconcile_coefficient_extensions(self):
            calls.append("extension")
            return []

    monkeypatch.setattr(hmm_training, "HMMTrainingService", Service)
    asyncio.run(hmm_training.coefficient_extension_tick())
    assert calls == ["extension"]


def test_scheduler_reconciles_new_release_without_reenrollment_or_retraining(tmp_path, monkeypatch):
    _, config, snapshot, inputs = _plan(tmp_path)
    next_inputs = ext.resolve_active_input(_profile(tmp_path, "release-next", "2024-07-04"))
    service = HMMTrainingService()
    enqueued, executed = [], []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            pass

        def fetchall(self):
            return [{"config_id": "config-fixed", "config_json": config}]

    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=lambda **_: Cursor())

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", connection)
    monkeypatch.setattr(service, "get_snapshot", lambda _: snapshot)
    monkeypatch.setattr(ext, "resolve_active_input", lambda: inputs)
    monkeypatch.setattr(
        service,
        "_enqueue_coefficient_extension",
        lambda p: enqueued.append(p) or {"job_id": p["extension_key"], "status": "PENDING"},
    )
    monkeypatch.setattr(service, "run_daily_coefficients_job", lambda job: executed.append(job))
    monkeypatch.setattr(service, "get_daily_coefficient_job", lambda job: {"job_id": job, "status": "COMPLETED"})
    service.reconcile_coefficient_extensions()
    inputs = next_inputs
    service.reconcile_coefficient_extensions()
    assert len(executed) == 2 and executed[0] != executed[1]
    assert enqueued[0]["policy"] == enqueued[1]["policy"]
    assert enqueued[1]["backtest_end"] == "2024-07-04"
    assert enqueued[0]["model_sha256"] == enqueued[1]["model_sha256"]


def test_worker_failure_is_persisted_not_reported_as_completed(tmp_path, monkeypatch):
    plan, *_ = _plan(tmp_path)
    service = HMMTrainingService()
    events = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            pass

        def fetchone(self):
            return (True,)

    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=lambda **_: Cursor())

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", connection)
    monkeypatch.setattr(service, "get_daily_coefficient_job", lambda _: {"status": "RUNNING", "plan_json": plan})
    monkeypatch.setattr(service, "_mark_daily_coefficient_job_running", lambda _: None)
    monkeypatch.setattr(
        service,
        "_execute_coefficient_extension_plan",
        lambda p: (_ for _ in ()).throw(RuntimeError("file-only reader rejected input")),
    )
    monkeypatch.setattr(service, "_complete_daily_coefficient_job", lambda *args: events.append("fake_success"))
    monkeypatch.setattr(
        service,
        "_fail_daily_coefficient_job",
        lambda job, message, context: events.append((job, message, context["reason_code"])),
    )
    service._run_coefficient_extension_job("job")
    assert events == [("job", "file-only reader rejected input", "hmm_coefficient_extension_failed")]


def test_api_enrollment_is_once_per_policy_and_requires_a_boolean(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers import hmm_training

    calls = []
    monkeypatch.setattr(
        hmm_training.service,
        "configure_coefficient_extension",
        lambda config_id, **kw: calls.append(kw) or {"config_id": config_id, "policy": kw},
    )
    app = FastAPI()
    app.include_router(hmm_training.router)
    with TestClient(app) as client:
        payload = {
            "snapshot_id": "fixed",
            "signal_presets": ["preset_A"],
            "coefficient_start": "2024-07-01",
            "enabled": True,
        }
        assert client.post("/hmm-training/configs/cfg/coefficient-extension", json=payload).status_code == 200
        payload["enabled"] = "true"
        assert client.post("/hmm-training/configs/cfg/coefficient-extension", json=payload).status_code == 422
    assert len(calls) == 1 and calls[0]["enabled"] is True


@pytest.mark.integration
def test_existing_dev_transaction_policy_job_and_registration_readback(tmp_path, monkeypatch):
    """Use only the explicitly configured existing DEV database, then roll back."""
    import psycopg2
    from psycopg2.extras import Json

    keys = ["TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD"]
    if not all(os.getenv(key) for key in keys):
        pytest.skip("explicit existing DEV connection is required")
    settings = dict(zip(["host", "port", "dbname", "user", "password"], [os.environ[key] for key in keys]))
    assert settings["dbname"] == "aistock_dev"
    _, config, snapshot, inputs = _plan(tmp_path)
    config_id, snapshot_id = str(uuid4()), str(uuid4())
    snapshot.update(config_id=config_id, snapshot_id=snapshot_id)
    config.pop("coefficient_extension")
    config["coefficient_windows"] = []
    service = HMMTrainingService()
    conn = psycopg2.connect(**settings, connect_timeout=8)

    @contextmanager
    def same_transaction():
        yield conn

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", same_transaction)
    monkeypatch.setattr(ext, "resolve_active_input", lambda: inputs)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == "aistock_dev"
            cur.execute(
                "INSERT INTO model_train_configs (config_id,model_type,display_name,config_json) VALUES (%s,%s,%s,%s)",
                (config_id, "sector_hmm", "auto-extension rollback validation", Json(config)),
            )
            cur.execute(
                "INSERT INTO model_train_snapshots (snapshot_id,config_id,model_path,sector_count,status) VALUES (%s,%s,%s,%s,%s)",
                (snapshot_id, config_id, snapshot["model_path"], 1, "completed"),
            )
        service.configure_coefficient_extension(
            config_id,
            snapshot_id=snapshot_id,
            signal_presets=["preset_A"],
            coefficient_start="2024-07-01",
            enabled=True,
        )
        saved_config = service._get_config(config_id)["config_json"]
        plan = ext.build_plan(
            config_id=config_id,
            config=saved_config,
            snapshot=snapshot,
            signal_preset="preset_A",
            preset_coeffs=saved_config["signal_presets"]["preset_A"],
            inputs=inputs,
        )
        job = service._enqueue_coefficient_extension(plan)
        duplicate = service._enqueue_coefficient_extension(plan)
        assert duplicate["job_id"] == job["job_id"]
        window = ext.publish_window(_payload(plan), plan, plan["test_start"], plan["backtest_end"])
        service._register_coefficient_extension_windows(plan, [window])
        service._register_coefficient_extension_windows(plan, [window])
        assert service._get_config(config_id)["config_json"]["coefficient_windows"] == [window]
        service._complete_daily_coefficient_job(
            job["job_id"],
            {
                "status": "REGISTERED",
                "output_path": plan["output_path"],
                "artifact_sha256": window["coefficient_sha256"],
            },
        )
        assert service.get_daily_coefficient_job(job["job_id"])["status"] == "COMPLETED"
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM model_train_configs WHERE config_id=%s", (config_id,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM model_train_daily_coefficient_jobs WHERE config_id=%s", (config_id,))
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()
