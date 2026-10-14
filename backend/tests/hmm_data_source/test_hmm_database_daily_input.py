"""Regression for the database/file source boundary, without market DB writes."""

import json
from contextlib import nullcontext
import sys
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from types import SimpleNamespace

import pytest

from backend.services.hmm_training_service import HMMTrainingService
from backend.services.hmm_data_source.daily_input import (
    QUOTE_COLUMNS,
    MONEYFLOW_COLUMNS,
    prepare_database_daily_inputs,
    read_database_daily_completion,
    snapshot_inference_origin,
)
from scripts.precompute_hmm_coefficients import main


def test_daily_executor_rejects_frozen_file_before_existing_artifact_or_worker(monkeypatch):
    service = HMMTrainingService()
    calls = []
    monkeypatch.setattr(
        "backend.services.hmm_training_service.subprocess.run",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    plan = {
        "source_mode": "qe_frozen_file",
        "existing_artifact": True,
        "existing_artifact_status": {"artifact_sha256": "a" * 64},
    }
    with pytest.raises(RuntimeError, match="hmm_daily_input_source_mismatch"):
        service._execute_daily_coefficient_plan(plan)
    assert calls == []


def test_legacy_file_only_daily_plan_cannot_claim_database_ready(monkeypatch):
    service = HMMTrainingService()
    plan = {
        "source_mode": "database_daily",
        "existing_artifact": False,
        "config_json": {
            "qe_frozen_input_bundle": {"schema_version": "qe_hmm_frozen_input_v1"},
        },
    }
    calls = []
    monkeypatch.setattr(
        "backend.services.hmm_training_service.subprocess.run",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    with pytest.raises(RuntimeError, match="hmm_database_daily_input_unavailable"):
        service._execute_daily_coefficient_plan(plan)
    assert calls == []


def test_old_qe_daily_asset_is_not_a_database_daily_asset(tmp_path):
    artifact = tmp_path / "daily.json"
    artifact.write_text(
        json.dumps(
            {
                "generation_mode": "daily_asof_prediction_v1",
                "snapshot_id": "snapshot_1",
                "config_id": "cfg_1",
                "preset_key": "preset_A",
                "as_of_trade_date": "2026-04-27",
                "effective_trade_date": "2026-04-28",
                "daily_coefficients": {"2026-04-28": {"801783.SI": 1.05}},
                "data_source": "frozen_qe_release",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="source_mode|data_source"):
        HMMTrainingService()._validate_existing_daily_artifact(
            output_path=str(artifact),
            snapshot_id="snapshot_1",
            config_id="cfg_1",
            signal_preset="preset_A",
            as_of_trade_date=date(2026, 4, 27),
            effective_trade_date=date(2026, 4, 28),
        )


@pytest.mark.parametrize(
    "source_params",
    [
        {"source_mode": "database_daily"},
        {"source_mode": "unknown"},
        {"source_mode": None},
        {"source_mode": "qe_frozen_file", "generation_mode": "daily_asof_prediction_v1"},
        {"output_trade_date": "2026-10-14"},
    ],
)
def test_file_cli_rejects_cross_source_before_model_file_io(tmp_path, monkeypatch, source_params):
    output = tmp_path / "must_not_exist.json"
    monkeypatch.setattr(sys, "argv", ["precompute_hmm_coefficients.py", "--output-path", str(output)])
    monkeypatch.setattr(sys, "stdin", StringIO(json.dumps(source_params)))
    with pytest.raises(ValueError, match="hmm_daily_input_source_mismatch"):
        main()
    assert not output.exists()


class _CompletionConnection:
    def __init__(self):
        self.query = ""
        self.calls = []
        self.read_cutoff = datetime(2026, 10, 13, tzinfo=timezone.utc)

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, parameters=None):
        self.query = query
        self.calls.append((query, parameters))

    def fetchone(self):
        if "current_setting" in self.query:
            return "on", self.read_cutoff, "repeatable read"
        # Physical maxima are newer than the verified completion date.
        return (date(2026, 10, 13),)

    def fetchall(self):
        if "GROUP BY dataset" in self.query:
            return [(name, date(2026, 10, 12)) for name in ("sector_data", "sw_daily", "index_daily")]
        return [(True,)]


def test_database_daily_plan_date_uses_completion_not_physical_maxima(monkeypatch):
    connection = _CompletionConnection()
    requested = []

    class Repository:
        def __init__(self, conn_factory):
            self.conn_factory = conn_factory

        def require_success(self, *, dataset, trade_date):
            with self.conn_factory() as captured:
                assert captured is connection
            requested.append((dataset, trade_date))
            return SimpleNamespace(
                dataset=dataset,
                trade_date=trade_date,
                data_source="existing_ingestion",
                status="success",
                quality_status="ok",
                row_count=100,
                refreshed_at=connection.read_cutoff,
                job_id="job",
            )

    monkeypatch.setattr("backend.services.hmm_data_source.daily_input.DataRefreshAuditRepository", Repository)
    day, identity = read_database_daily_completion(connection, requested_as_of=None, cutoff=date(2026, 10, 13))
    assert day == date(2026, 10, 12)
    assert {value[1] for value in requested} == {day}
    assert len(requested) == 3
    assert set(identity["max_dates"].values()) == {"2026-10-13"}
    assert identity["max_dates_diagnostic_only"] is True


def test_database_daily_completion_does_not_hide_latest_failed_attempt(monkeypatch):
    class Repository:
        def __init__(self, **kwargs):
            pass

        def require_success(self, **kwargs):
            raise RuntimeError("required dataset refresh did not succeed")

    monkeypatch.setattr("backend.services.hmm_data_source.daily_input.DataRefreshAuditRepository", Repository)
    with pytest.raises(RuntimeError, match="required dataset refresh did not succeed"):
        read_database_daily_completion(_CompletionConnection(), requested_as_of=None, cutoff=date(2026, 10, 13))


@pytest.fixture
def input_facts(monkeypatch):
    from backend.services.dataset_release import sw_l2_quote_policy as policy

    codes = sorted(
        {
            "801011.SI",
            "801019.SI",
            "801117.SI",
            "801207.SI",
            "801216.SI",
            "801961.SI",
            "801983.SI",
            "801783.SI",
            *[f"801{i:03d}.SI" for i in range(300, 423)],
        }
    )
    assert len(codes) == 131
    monkeypatch.setattr(policy, "SW2021_L2_TAXONOMY_DIGEST", policy.taxonomy_digest(codes))
    calendar = [date(2026, 9, 1) + timedelta(days=i) for i in range(32)]
    origin = {"history_start": calendar[0].isoformat(), "origin_authority": "fixture"}
    rows = []
    unavailable = {"801011.SI", "801019.SI", "801117.SI", "801207.SI", "801216.SI", "801961.SI", "801983.SI"}
    for code in codes:
        for i, day in enumerate(calendar):
            rows.append(
                {
                    "sector_code": code,
                    "trade_date": day,
                    **{k: None if code in unavailable else (0.2 + (i % 7) * 0.1) for k in QUOTE_COLUMNS},
                    **{k: 0.01 for k in MONEYFLOW_COLUMNS},
                }
            )
    return {
        "completion": {
            "read_cutoff_at": "2026-10-03T00:00:00+00:00",
            "completion_authority": "market.dataset_date_refresh_audit",
            "completion_statuses": {},
        },
        "origin": origin,
        "as_of": calendar[-1],
        "effective": date(2026, 10, 3),
        "calendar": calendar,
        "shared_code_to_id": {c: 1000 + i * 7 for i, c in enumerate(codes)},
        "membership_rows": [
            {"ts_code": f"{i:06d}.SZ", "l2_code": c, "in_date": calendar[0], "out_date": None}
            for i, c in enumerate(codes)
        ],
        "sector_rows": rows,
        "official_quote_rows": [
            {key: row[key] for key in ("sector_code", "trade_date", *QUOTE_COLUMNS)}
            for row in rows
            if row["sw2_pct_change"] is not None
        ],
        "index_rows": [(d, 1000.0 + i) for i, d in enumerate(calendar)],
        "volume_rows": [(d, 1000.0) for d in calendar],
    }


def test_database_adapter_preserves_sparse_shared_ids_and_finite_unpublished_moneyflow(input_facts):
    loaded = prepare_database_daily_inputs(**input_facts)
    assert len(loaded["ordered_sector_codes"]) == 131
    assert len(loaded["coefficient_sector_codes"]) == 124
    day = input_facts["as_of"]
    assert loaded["sector_data"]["801019.SI"][day]["sw2_pct_change"] is None
    assert loaded["sector_data"]["801019.SI"][day]["sw2_mf_net_amt"] == 0.01
    assert "dataset_root" not in loaded and "dataset_identity" not in loaded
    reverse = deepcopy(input_facts)
    reverse["sector_rows"].reverse()
    reverse["membership_rows"].reverse()
    reverse["official_quote_rows"].reverse()
    assert prepare_database_daily_inputs(**reverse)["input_identity"] == loaded["input_identity"]


def test_authentic_historical_quote_context_does_not_enable_unpublished_coefficients(input_facts):
    facts = deepcopy(input_facts)
    calendar = [date(2023, 1, 30) + timedelta(days=i) for i in range(32)]
    remap = dict(zip(facts["calendar"], calendar))
    facts.update(calendar=calendar, as_of=calendar[-1], effective=calendar[-1] + timedelta(days=1))
    facts["origin"]["history_start"] = calendar[0].isoformat()
    facts["completion"]["read_cutoff_at"] = f"{facts['effective']}T00:00:00+00:00"
    for member in facts["membership_rows"]:
        member["in_date"] = calendar[0]
    for row in facts["sector_rows"]:
        row["trade_date"] = remap[row["trade_date"]]
        row.update(dict(zip(QUOTE_COLUMNS, (1.38, 8269.43, 32835.1393))))
    facts["index_rows"] = [(remap[d], value) for d, value in facts["index_rows"]]
    facts["volume_rows"] = [(remap[d], value) for d, value in facts["volume_rows"]]
    facts["official_quote_rows"] = [
        {key: row[key] for key in ("sector_code", "trade_date", *QUOTE_COLUMNS)} for row in facts["sector_rows"]
    ]
    loaded = prepare_database_daily_inputs(**facts)
    assert loaded["sector_data"]["801011.SI"][calendar[0]]["sw2_pct_change"] == 1.38
    assert "801011.SI" not in loaded["coefficient_sector_codes"]
    assert "801011.SI" in loaded["quote_unavailable_sector_codes_by_date"][str(facts["as_of"])]
    assert loaded["input_identity"]["official_quote_authority"] == "market.sw_daily"
    assert loaded["input_identity"]["excluded_coefficient_quote_context_cells"] == len(calendar)
    changed = deepcopy(facts)
    for source in (changed["sector_rows"], changed["official_quote_rows"]):
        next(r for r in source if r["sector_code"] == "801011.SI")["sw2_pct_change"] += 0.01
    revised = prepare_database_daily_inputs(**changed)
    assert revised["coefficient_sector_codes"] == loaded["coefficient_sector_codes"]
    assert revised["input_identity"]["input_sha256"] != loaded["input_identity"]["input_sha256"]


@pytest.mark.parametrize(
    "corruption",
    ["missing", "mismatch", "duplicate", "future", "nonfinite", "partial", "negative", "missing_field", "unknown_code"],
)
def test_official_quote_authority_rejects_missing_or_corrupted_source(input_facts, corruption):
    facts = deepcopy(input_facts)
    row = next(r for r in facts["official_quote_rows"] if r["sector_code"] == "801783.SI")
    if corruption == "missing":
        facts["official_quote_rows"].remove(row)
    elif corruption == "mismatch":
        row["sw2_pct_change"] += 0.01
    elif corruption == "duplicate":
        facts["official_quote_rows"].append(deepcopy(row))
    elif corruption == "future":
        row["trade_date"] = facts["effective"]
    elif corruption == "nonfinite":
        row["sw2_pct_change"] = float("inf")
    elif corruption == "partial":
        row["sw2_vol"] = None
    elif corruption == "negative":
        row["sw2_vol"] = -1
    elif corruption == "missing_field":
        del row["sw2_vol"]
    elif corruption == "unknown_code":
        row["sector_code"] = "unknown"
    with pytest.raises(RuntimeError, match="hmm_database_daily_input_unavailable:.*official"):
        prepare_database_daily_inputs(**facts)


def test_quote_source_identity_changes_even_when_coefficient_scope_does_not(input_facts):
    first = prepare_database_daily_inputs(**input_facts)
    changed = deepcopy(input_facts)
    for source in (changed["sector_rows"], changed["official_quote_rows"]):
        next(r for r in source if r["sector_code"] == "801783.SI")["sw2_pct_change"] += 0.01
    second = prepare_database_daily_inputs(**changed)
    assert second["coefficient_sector_codes"] == first["coefficient_sector_codes"]
    assert second["input_identity"]["official_quote_sha256"] != first["input_identity"]["official_quote_sha256"]
    assert second["input_identity"]["input_sha256"] != first["input_identity"]["input_sha256"]


@pytest.mark.parametrize(
    "corruption,error",
    [
        ("published_missing", "missing numeric input"),
        ("published_nonfinite", "non-finite numeric"),
        ("unpublished_quote", "outside official source"),
        ("duplicate", "duplicate sector/date"),
        ("prefix_gap", "continuous inference prefix missing"),
        ("future", "outside the canonical prefix"),
        ("index_gap", "prefix calendar incomplete"),
        ("ambiguous_member", "overlapping/duplicate PIT"),
        ("unknown_id", "invalid PIT membership"),
    ],
)
def test_database_adapter_rejects_invalid_facts_without_fallback(input_facts, corruption, error):
    facts = deepcopy(input_facts)
    row = next(r for r in facts["sector_rows"] if r["sector_code"] == "801783.SI")
    if corruption == "published_missing":
        row["sw2_vol"] = None
    elif corruption == "published_nonfinite":
        row["sw2_vol"] = float("inf")
    elif corruption == "unpublished_quote":
        next(r for r in facts["sector_rows"] if r["sector_code"] == "801019.SI")["sw2_vol"] = 1.0
    elif corruption == "duplicate":
        facts["sector_rows"].append(deepcopy(row))
    elif corruption == "prefix_gap":
        facts["sector_rows"].remove(row)
    elif corruption == "future":
        row["trade_date"] = facts["effective"]
    elif corruption == "index_gap":
        facts["index_rows"].pop()
    elif corruption == "ambiguous_member":
        facts["membership_rows"].append(deepcopy(facts["membership_rows"][0]))
    elif corruption == "unknown_id":
        facts["membership_rows"][0]["l2_code"] = "unknown"
    with pytest.raises((RuntimeError, ValueError), match=error):
        prepare_database_daily_inputs(**facts)


def test_snapshot_origin_is_fixed_not_daily_rolling_and_config_dates_cannot_substitute():
    metrics = {"train_period": "2023-02-13 ~ 2026-02-13", "val_period": "2026-02-24 ~ 2026-05-19"}
    first = snapshot_inference_origin(metrics, as_of=date(2026, 10, 12))
    second = snapshot_inference_origin(metrics, as_of=date(2026, 10, 13))
    assert first == second
    assert first["history_start"] == "2023-01-26"
    for invalid in (None, {"train_start": "2023-02-13", "val_end": "2026-05-19"}):
        with pytest.raises(RuntimeError, match="hmm_daily_model_boundary_unavailable"):
            snapshot_inference_origin(invalid, as_of=date(2026, 10, 12))
    with pytest.raises(RuntimeError, match="must follow frozen"):
        snapshot_inference_origin(metrics, as_of=date(2026, 5, 19))


def test_current_mapping_does_not_claim_historical_as_known(input_facts):
    input_facts["completion"]["read_cutoff_at"] = "2026-10-04T00:00:00+00:00"
    with pytest.raises(RuntimeError, match="hmm_daily_historical_as_known_unavailable"):
        prepare_database_daily_inputs(**input_facts)


def test_real_shared_core_db_and_file_inputs_produce_identical_states_and_coefficients(
    tmp_path, monkeypatch, input_facts
):
    from scripts import precompute_hmm_coefficients as core
    from backend.services.hmm_data_source.coefficient_extension import file_sha256

    loaded = prepare_database_daily_inputs(**input_facts)
    model = {
        "n_states": 3,
        "covariance_type": "diag",
        "rolling_window": 5,
        "means": [[-0.02] * 7, [0.0] * 7, [0.02] * 7],
        "covars": [[0.1] * 7] * 3,
        "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        "state_labels": {"0": "fading", "1": "neutral", "2": "trending"},
    }
    path = tmp_path / "models.json"
    path.write_text(json.dumps({c: model for c in loaded["ordered_sector_codes"]}), encoding="utf-8")
    params = {
        "source_mode": "database_daily",
        "model_path": str(path),
        "model_sha256": file_sha256(path),
        "test_start": str(input_facts["as_of"]),
        "backtest_end": str(input_facts["as_of"]),
        "output_trade_date": str(input_facts["effective"]),
        "as_of_trade_date": str(input_facts["as_of"]),
        "preset_key": "preset_A",
        "preset_coeffs": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
    }
    monkeypatch.setattr("hmmlearn.hmm.GaussianHMM.fit", lambda *a, **k: pytest.fail("fit forbidden"))
    actual = core.generate_coefficient_payload(params, loaded_inputs=loaded)
    file_loaded = deepcopy(loaded)
    file_loaded.update({"dataset_root": str(tmp_path), "dataset_identity": {"fixture": True}, "file_sha256": {}})
    monkeypatch.setattr(core, "load_frozen_coefficient_inputs", lambda *a, **k: file_loaded)
    file_params = {k: v for k, v in params.items() if k not in {"output_trade_date", "as_of_trade_date"}}
    file_params.update({"source_mode": "qe_frozen_file", "frozen_input_bundle": {}})
    replay = core.generate_coefficient_payload(file_params)
    assert (
        actual["daily_coefficients"][str(input_facts["effective"])]
        == replay["daily_coefficients"][str(input_facts["as_of"])]
    )
    assert actual["input_data_max_dates_by_date"][str(input_facts["effective"])]["sector_data"] == str(
        input_facts["as_of"]
    )
    assert "dataset_identity" not in actual and actual["source_mode"] == "database_daily"


def test_feature_only_horizon_path_does_not_construct_targets(monkeypatch):
    from scripts import hmm_horizon_v2_train as train

    days = [date(2026, 9, 1) + timedelta(days=i) for i in range(25)]
    rows = [
        {
            "trade_date": d,
            "pct_change": 0.1,
            "vol": 10,
            "amount": 100,
            "mf_net_amt": 1,
            "mf_buy_elg_amt": 2,
            "mf_sell_elg_amt": 1,
        }
        for d in days
    ]
    expected, dates, _ = train.build_observations(rows, dict.fromkeys(days, 0.0), dict.fromkeys(days, 100.0))
    monkeypatch.setattr(train, "build_observations", lambda *a, **k: pytest.fail("target builder forbidden"))
    actual, actual_dates = train.build_feature_observations(rows, dict.fromkeys(days, 0.0), dict.fromkeys(days, 100.0))
    import numpy as np

    np.testing.assert_array_equal(actual, expected)
    assert actual_dates == dates


@pytest.fixture
def generation_case(tmp_path, monkeypatch, input_facts):
    from backend.services.hmm_data_source.daily_input import validate_database_models

    loaded = prepare_database_daily_inputs(**input_facts)
    model = {
        "n_states": 3,
        "covariance_type": "diag",
        "rolling_window": 5,
        "means": [[-0.02] * 7, [0.0] * 7, [0.02] * 7],
        "covars": [[0.1] * 7] * 3,
        "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        "state_labels": {"0": "fading", "1": "neutral", "2": "trending"},
    }
    models = {c: deepcopy(model) for c in loaded["ordered_sector_codes"]}
    preset = {"trending": 1.05, "neutral": 1.0, "fading": 0.96}
    validate_database_models(models, catalog=loaded["ordered_sector_codes"], preset=preset)
    path = tmp_path / "models.json"
    path.write_text(json.dumps(models), encoding="utf-8")
    service = HMMTrainingService()
    config = {"signal_presets": {"preset_A": preset}, "qe_frozen_input_bundle": {"poison": True}}
    snapshot = {"snapshot_id": "snapshot", "config_id": "config", "status": "completed", "model_path": str(path)}
    monkeypatch.setattr(service, "get_snapshot", lambda sid: snapshot)
    monkeypatch.setattr(service, "_get_config", lambda cid: {"config_json": deepcopy(config)})
    monkeypatch.setattr(service, "_database_daily_completion", lambda **k: (input_facts["as_of"], {"max_dates": {}}))
    monkeypatch.setattr(service, "_database_daily_inputs", lambda **k: deepcopy(loaded))
    monkeypatch.setattr(service, "_require_trading_day", lambda *a, **k: None)
    monkeypatch.setattr(service, "_next_trading_day", lambda day: input_facts["effective"])
    monkeypatch.setattr(service, "_daily_execution_lock", lambda key: nullcontext(True))
    monkeypatch.setattr(
        "backend.services.hmm_training_service.get_conn", lambda **k: pytest.fail("real DB IO forbidden")
    )
    monkeypatch.setattr(
        "backend.services.hmm_training_service.subprocess.run", lambda *a, **k: pytest.fail("file worker forbidden")
    )
    monkeypatch.setattr("hmmlearn.hmm.GaussianHMM.fit", lambda *a, **k: pytest.fail("fit forbidden"))
    return service, loaded, models, config


def test_database_service_infers_publishes_and_recovers_registration_failure_without_rewriting(
    generation_case, monkeypatch
):
    from pathlib import Path

    service, _, _, _ = generation_case
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    monkeypatch.setattr(
        service,
        "_register_database_daily_window",
        lambda *a: (_ for _ in ()).throw(RuntimeError("registration unavailable")),
    )
    with pytest.raises(RuntimeError, match="registration unavailable"):
        service._execute_daily_coefficient_plan(plan)
    path = Path(plan["output_path"])
    first = path.read_bytes()
    registered = []
    monkeypatch.setattr(service, "_register_database_daily_window", lambda p, w: registered.append(w))
    next_plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    result = service._execute_daily_coefficient_plan(next_plan)
    assert result["status"] == "EXISTS" and result["registration_status"] == "REGISTERED_READBACK"
    assert path.read_bytes() == first
    assert registered[0]["coefficient_sha256"] == result["artifact_sha256"]
    assert registered[0]["source_mode"] == "database_daily" and "dataset_manifest_identity" not in registered[0]
    payload = json.loads(first)
    assert len(payload["daily_coefficients"][plan["effective_trade_date"]]) == 124
    assert len(payload["stock_sector_map"]) == 131


def test_database_plan_reports_read_prefix_not_physical_maxima(generation_case, monkeypatch):
    service, loaded, _, _ = generation_case
    as_of = loaded["sector_dates"][-1]
    diagnostic = {
        key: (as_of + timedelta(days=1)).isoformat() for key in ("sector_data", "sw_daily", "index_daily_000300")
    }
    monkeypatch.setattr(service, "_database_daily_completion", lambda **kwargs: (as_of, {"max_dates": diagnostic}))
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    assert plan["input_data_max_dates"] == {key: as_of.isoformat() for key in diagnostic}
    assert plan["database_completion"]["max_dates"] == diagnostic


def test_existing_database_asset_anchors_new_plan_to_original_read_identity(generation_case, monkeypatch):
    from pathlib import Path

    service, loaded, _, _ = generation_case
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *args: None)
    first_plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    first_result = service._execute_daily_coefficient_plan(first_plan)
    original = Path(first_plan["output_path"]).read_bytes()
    cutoff = datetime.fromisoformat(loaded["input_identity"]["read_cutoff_at"])
    loaded["input_identity"]["read_cutoff_at"] = (cutoff + timedelta(minutes=1)).isoformat()
    second_plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    assert second_plan["daily_key"] == first_plan["daily_key"]
    assert second_plan["input_identity"] == first_plan["input_identity"]
    second_result = service._execute_daily_coefficient_plan(second_plan)
    assert second_result["input_identity"] == second_plan["input_identity"]
    assert second_result["artifact_sha256"] == first_result["artifact_sha256"]
    assert second_result["status"] == "EXISTS"
    assert Path(second_plan["output_path"]).read_bytes() == original


def test_database_executor_rejects_concurrent_asset_with_different_read_cutoff(generation_case, monkeypatch):
    service, loaded, _, _ = generation_case
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *args: None)
    first_plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    cutoff = datetime.fromisoformat(loaded["input_identity"]["read_cutoff_at"])
    loaded["input_identity"]["read_cutoff_at"] = (cutoff + timedelta(minutes=1)).isoformat()
    concurrent_plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    assert concurrent_plan["daily_key"] == first_plan["daily_key"]
    service._execute_daily_coefficient_plan(first_plan)
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *args: pytest.fail("registration forbidden"))
    with pytest.raises(RuntimeError, match="hmm_daily_input_identity_changed"):
        service._execute_daily_coefficient_plan(concurrent_plan)


@pytest.mark.parametrize("boundary", ["missing", "invalid", "naive", "future", "before_asof"])
def test_existing_database_asset_rejects_invalid_original_read_boundary(generation_case, monkeypatch, boundary):
    from pathlib import Path

    service, _, _, _ = generation_case
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *args: None)
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    service._execute_daily_coefficient_plan(plan)
    path = Path(plan["output_path"])
    payload = json.loads(path.read_bytes())
    cutoff = datetime.fromisoformat(payload["input_identity"]["read_cutoff_at"])
    invalid = {
        "invalid": "not-a-time",
        "naive": cutoff.replace(tzinfo=None).isoformat(),
        "future": (cutoff + timedelta(minutes=1)).isoformat(),
        "before_asof": (cutoff - timedelta(days=2)).isoformat(),
    }
    if boundary == "missing":
        del payload["input_identity"]["read_cutoff_at"]
    else:
        payload["input_identity"]["read_cutoff_at"] = invalid[boundary]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="hmm_daily_input_identity_changed"):
        service.preview_daily_coefficients("snapshot", signal_preset="preset_A")


@pytest.mark.parametrize(
    "drift", ["input", "model", "preset", "output_path", "generator", "durable_preset", "durable_key"]
)
def test_database_service_rejects_identity_drift_before_publication(generation_case, monkeypatch, drift):
    from pathlib import Path

    service, loaded, models, config = generation_case
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    if drift == "input":
        loaded["input_identity"]["input_sha256"] = "0" * 64
        monkeypatch.setattr(service, "_database_daily_inputs", lambda **k: deepcopy(loaded))
    elif drift == "model":
        models["801783.SI"]["means"][0][0] += 1
        Path(plan["model_path"]).write_text(json.dumps(models), encoding="utf-8")
    elif drift == "preset":
        config["signal_presets"]["preset_A"]["trending"] = 1.2
    elif drift == "generator":
        monkeypatch.setattr(service, "_daily_generator_sha256", lambda: "0" * 64)
    elif drift == "output_path":
        plan["output_path"] = str(Path(plan["model_path"]).parent / "wrong.json")
    elif drift == "durable_preset":
        plan["preset_coeffs"]["trending"] = 1.2
    elif drift == "durable_key":
        plan["daily_key"] = "0" * 64
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *a: pytest.fail("registration forbidden"))
    with pytest.raises(RuntimeError, match="hmm_daily_(input|model)_identity_changed"):
        service._execute_daily_coefficient_plan(plan)
    assert not Path(plan["output_path"]).exists()


def test_database_service_detects_post_publication_source_drift_without_registering(generation_case, monkeypatch):
    service, loaded, _, _ = generation_case
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    calls = 0

    def read(**kwargs):
        nonlocal calls
        calls += 1
        value = deepcopy(loaded)
        if calls == 2:
            value["input_identity"]["input_sha256"] = "0" * 64
        return value

    monkeypatch.setattr(service, "_database_daily_inputs", read)
    monkeypatch.setattr(service, "_register_database_daily_window", lambda *a: pytest.fail("registration forbidden"))
    with pytest.raises(RuntimeError, match="file not registered"):
        service._execute_daily_coefficient_plan(plan)


def test_database_model_schema_rejects_twenty_dimensions_instead_of_substituting_legacy(generation_case):
    from backend.services.hmm_data_source.daily_input import validate_database_models

    _, loaded, models, config = generation_case
    models["801783.SI"]["means"] = [[0.0] * 20] * 3
    with pytest.raises(RuntimeError, match="hmm_daily_model_schema_unsupported"):
        validate_database_models(
            models, catalog=loaded["ordered_sector_codes"], preset=config["signal_presets"]["preset_A"]
        )


def test_forward_filter_never_falls_back_to_uniform_on_nonfinite_likelihood():
    import numpy as np
    from scripts.precompute_hmm_coefficients import forward_filter_posteriors

    hmm = SimpleNamespace(_compute_log_likelihood=lambda obs: np.full((len(obs), 3), float("nan")))
    with pytest.raises(ValueError, match="non-finite observations or emission"):
        forward_filter_posteriors(hmm, np.zeros((2, 7)))


@pytest.mark.parametrize("damage", ["negative_covar", "missing_std", "bad_horizon_preprocess"])
def test_invalid_frozen_preprocessing_fails_instead_of_repairing_model(generation_case, damage):
    from backend.services.hmm_data_source.daily_input import validate_database_models
    from scripts.hmm_horizon_v2_train import FEATURE_NAMES

    _, loaded, models, config = generation_case
    model = models["801783.SI"]
    if damage == "negative_covar":
        model["covars"][0][0] = -0.1
    elif damage == "missing_std":
        model["zscore_mean"] = [0.0] * 7
    else:
        model.update(
            feature_names=list(FEATURE_NAMES),
            means=[[0.0] * 10] * 3,
            covars=[[0.1] * 10] * 3,
            preprocess={
                "winsor_lower": [-1.0] * 10,
                "winsor_upper": [1.0] * 10,
                "zscore_mean": [0.0] * 10,
                "zscore_std": [0.0] * 10,
            },
        )
    with pytest.raises(RuntimeError, match="hmm_daily_model_identity_changed"):
        validate_database_models(
            models, catalog=loaded["ordered_sector_codes"], preset=config["signal_presets"]["preset_A"]
        )


@pytest.mark.parametrize("outcome", ["new", "same", "conflict", "bad_readback"])
def test_daily_registration_preserves_qe_windows_and_requires_transactional_readback(
    generation_case, monkeypatch, outcome
):
    service, _, _, config = generation_case
    plan = service.preview_daily_coefficients("snapshot", signal_preset="preset_A")
    window = {"daily_key": plan["daily_key"], "coefficient_sha256": "a" * 64}
    qe_window = {"source_mode": "qe_frozen_file", "coefficient_sha256": "b" * 64}
    saved = deepcopy(config)
    saved["coefficient_windows"] = [qe_window]
    if outcome in {"same", "conflict"}:
        saved["coefficient_windows"].append(dict(window))
    if outcome == "conflict":
        saved["coefficient_windows"][-1]["coefficient_sha256"] = "c" * 64
    updates = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            if sql.lstrip().startswith("UPDATE"):
                updates.append(deepcopy(params[0].adapted))

        def fetchone(self):
            if not updates:
                return {"config_json": saved}
            return {"config_json": {} if outcome == "bad_readback" else updates[-1]}

    def connection(**kwargs):
        assert kwargs == {"autocommit": False, "manage_transaction": True}
        return nullcontext(SimpleNamespace(cursor=lambda **k: Cursor()))

    monkeypatch.setattr("backend.services.hmm_training_service.get_conn", connection)
    if outcome in {"conflict", "bad_readback"}:
        with pytest.raises(RuntimeError, match="hmm_daily_registration_(conflict|readback_mismatch)"):
            service._register_database_daily_window(plan, window)
        assert bool(updates) == (outcome == "bad_readback")
    else:
        service._register_database_daily_window(plan, window)
        assert updates[0]["coefficient_windows"] == [qe_window, window]


def test_daily_job_completion_requires_durable_running_row(monkeypatch):
    statements = []
    cursor = SimpleNamespace(
        execute=lambda sql, params: statements.append(sql),
        rowcount=0,
    )
    monkeypatch.setattr(
        "backend.services.hmm_training_service.get_conn",
        lambda **k: nullcontext(SimpleNamespace(cursor=lambda **k: nullcontext(cursor))),
    )
    with pytest.raises(RuntimeError, match="hmm_daily_job_completion_conflict"):
        HMMTrainingService()._complete_daily_coefficient_job("job", {"status": "GENERATED"})
    assert "status = 'RUNNING'" in statements[0]
    HMMTrainingService()._fail_daily_coefficient_job("job", "conflict")
    assert "status IN ('PENDING', 'RUNNING')" in statements[1]


def test_shared_feature_module_optional_host_discovery_does_not_silence_error(monkeypatch, capsys):
    import subprocess
    from pathlib import Path
    from scripts import hmm_horizon_v2_train as train

    def unavailable(*args, **kwargs):
        raise subprocess.TimeoutExpired("resolver discovery", 3)

    monkeypatch.setattr(train.subprocess, "check_output", unavailable)
    assert "configured-host" in train.candidate_hosts("configured-host")
    assert "hmm_optional_resolver_discovery_unavailable" in capsys.readouterr().err
    parser = train.build_arg_parser()
    assert (
        Path(parser.get_default("output_root")) == Path(train.__file__).resolve().parents[1] / "backend/data/hmm_models"
    )
