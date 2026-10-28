"""Daily parallel ranks must remain prospective, durable and separate from native actions."""
from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import json
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import lstm_spo_shadow_v1 as app
from backend.services.advisory_model_first.lstm_spo_shadow_source_v1 import LstmSpoShadowSource, attach_features, top50
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import STOCK_FEATURES


@pytest.fixture
def trial(tmp_path, monkeypatch):
    model = dict(contract_sha256=app.spo.sha(app.spo.CONTRACT), arm="candidate", fields=list(app.spo.FEATURES),
                 cutoff="2025-07-31", median=[0.]*8, scale=[1.]*8, coef=[1.]+[0.]*15)
    model["model_payload_sha256"] = app.spo.sha(model)
    packet = dict(schema=app.specialist.CONTRACT["schema"], contract_sha256=app.spo.sha(app.specialist.CONTRACT),
                  target=app.TARGET, fold=3, model=model)
    packet["payload_sha256"] = app.spo.sha(packet)
    raw = json.dumps(packet).encode()
    monkeypatch.setattr(app, "MODEL_SHA", hashlib.sha256(raw).hexdigest())
    monkeypatch.setattr(app, "PAYLOAD_SHA", packet["payload_sha256"])
    monkeypatch.setitem(app.RELEASE, "model_sha256", app.MODEL_SHA)
    monkeypatch.setitem(app.RELEASE, "model_payload_sha256", app.PAYLOAD_SHA)
    monkeypatch.setattr(app.spo, "fit", lambda *a, **k: pytest.fail("shadow must never fit"))
    rows = pd.DataFrame([{**app.TARGET, app.KEY[0]: pd.Timestamp("2026-10-27"),
        app.KEY[1]: pd.Timestamp("2026-10-28"), "instrument": f"{600000+i}.SH",
        "selection_effective_rank": i+1, "candidate_group_size": 50, "parent_score": 100.-i,
        **{f: i/100 for f in app.spo.FEATURES}} for i in range(50)])
    publication = dict(program_id=app.PROGRAM, binding_version_id=app.BINDING, decision_date="2026-10-27",
                       target_date="2026-10-28", list_version_id="advlv_test", selection_run_id="sel_test",
                       created_at="2026-10-27T08:45:00+00:00")
    now = [datetime(2026, 10, 27, 9, tzinfo=timezone.utc)]
    source = NS(publications=lambda: [publication], inputs=lambda p: (rows, {}), finance=lambda p: None,
                verify_capture=lambda receipt: None)
    service = app.LstmSpoShadowService(root=tmp_path, source=source, now=lambda: now[0])
    service.root.mkdir()
    (service.root / "model.json").write_bytes(raw)
    release = app.publish(service.root / "release.json", dict(**app.RELEASE, created_at="2026-10-27T00:00:00+00:00"))
    return NS(service=service, source=source, rows=rows, packet=packet, raw=raw, publication=publication,
              release=release, now=now, path=service.root / "days/2026-10-27/prediction.json")


def test_exact_model_current_inference_and_independent_arithmetic(trial):
    model = app.checked_model(trial.raw)
    trial.rows.loc[0, "ret_1"] = np.nan
    prediction = app.predict_current(trial.rows, model)
    x = trial.rows.loc[:, app.spo.FEATURES].to_numpy(float)
    x = np.c_[np.nan_to_num(x), np.isnan(x).astype(float)]
    expected = (x-x.mean(axis=0)) @ np.array(model["model"]["coef"])
    np.testing.assert_allclose(prediction.decision_score, expected)
    assert set(prediction.loc[prediction.selected, "selection_effective_rank"]) == set(range(41, 51))
    with pytest.raises(ValueError, match="file differs"):
        app.checked_model(trial.raw+b" ")
    changed = trial.rows.copy()
    changed["package_id"] = "another_lstm"
    with pytest.raises(ValueError, match="population"):
        app.predict_current(changed, model)


def test_capture_restart_reuses_original_even_when_source_changes(trial):
    assert trial.service.run_once()["results"][0]["status"] == "PREDICTED"
    raw = trial.path.read_bytes()
    trial.source.inputs = lambda p: pytest.fail("must not reread frozen inputs")
    restarted = app.LstmSpoShadowService(root=trial.service.root.parent, source=trial.source, now=lambda: trial.now[0])
    assert restarted.run_once()["results"][0]["status"] == "PREDICTION_REUSED"
    assert trial.path.read_bytes() == raw
    assert restarted.status(program_id=app.PROGRAM)["captured_days"] == 1
    trial.publication["selection_run_id"] = "changed"
    assert restarted.run_once()["status"] == "FAILED"
    assert restarted.status(program_id=app.PROGRAM)["last_failure"] is not None


def test_late_capture_never_publishes_or_reads_inputs(trial):
    trial.now[0] = app.clock("2026-10-28", 9, 30)
    trial.source.inputs = lambda p: pytest.fail("late capture may not read inputs")
    assert trial.service.run_once()["results"][0]["status"] == "MISSED_CAPTURE_WINDOW"
    assert not trial.path.exists()
    assert len(trial.service.status(program_id=app.PROGRAM)["missed"]) == 1


@pytest.mark.parametrize("phase", ["read", "post_compute"])
def test_waiting_is_durable_retries_without_fit_and_clears_after_capture(trial, phase):
    from backend.services.data_integrity.inputs import InputReadinessError
    original = trial.source.inputs
    def waiting(*_): raise InputReadinessError("benchmark revision is unverified", context={"dataset": "index_daily"})
    if phase == "read":
        trial.source.inputs = waiting
    else:
        trial.source.verify_capture = waiting
    assert trial.service.run_once()["results"][0]["status"] == "WAITING_DATA"
    assert not trial.path.exists()
    status = trial.service.status(program_id=app.PROGRAM)
    assert status["last_waiting"] and status["last_failure"] is None
    trial.source.inputs, trial.source.verify_capture = original, lambda *_: None
    assert trial.service.run_once()["results"][0]["status"] == "PREDICTED"
    assert trial.service.status(program_id=app.PROGRAM)["last_waiting"] is None


def test_extra_twenty_sessions_require_benchmark_and_adjustments(monkeypatch):
    from backend.services.advisory_model_first import lstm_spo_shadow_source_v1 as source
    from backend.services.data_integrity.inputs import InputReadinessError
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    days = list(pd.bdate_range("2026-09-01", periods=20).date)
    rows = [{"trade_date": str(d), "dataset": ds, "data_revision": 1, "validated_revision": 1,
             "validation_status": "VALID", "validation_id": "v", "scope_version": SCOPE_VERSION,
             "rule_version": RULE_VERSION} for d in days for ds in source.EXTRA_DATASETS]
    monkeypatch.setattr(source, "partition_pins", lambda *_: rows)
    assert len(source.extra_input_capture(object(), days)["partition_refs"]) == 60
    rows[-1]["validated_revision"] = 0
    with pytest.raises(InputReadinessError):
        source.extra_input_capture(object(), days)


def test_crossing_open_never_publishes_as_prospective(trial):
    trial.now[0] = app.clock("2026-10-28", 9, 29)
    def slow(p):
        trial.now[0] = app.clock("2026-10-28", 9, 30)
        return trial.rows, {}
    trial.source.inputs = slow
    assert trial.service.run_once()["status"] == "FAILED"
    assert not trial.path.exists()


def test_unknown_valuation_versions_keep_ten_slots_and_first_complete(trial):
    trial.service.run_once()
    finance = trial.rows.loc[:, app.spo.KEY].copy()
    finance["valuation_policy_sha256"] = app.RELEASE["valuation_policy_sha256"]
    finance["valuation_status"] = "AVAILABLE"
    finance["hypothetical_liquidation_net_bps"] = np.arange(50.)
    finance.loc[49, ["valuation_status", "hypothetical_liquidation_net_bps"]] = ["UNKNOWN", np.nan]
    trial.source.finance = lambda p: (finance.copy(), {"revision": 1})
    trial.service.run_once()
    status = trial.service.status(program_id=app.PROGRAM)
    assert status["complete_days"] == 0
    assert status["selected"]["evaluation"]["specialist_unknown_slots"] == 1
    finance.loc[49, ["valuation_status", "hypothetical_liquidation_net_bps"]] = ["AVAILABLE", 49.]
    trial.now[0] += pd.Timedelta(seconds=1)
    trial.service.run_once()
    status = trial.service.status(program_id=app.PROGRAM)
    assert status["mean_delta_bps"] == pytest.approx(40.)
    assert status["selected"]["evaluation_versions"] == 2
    trial.source.finance = lambda p: pytest.fail("first complete evaluation must be frozen")
    trial.service.run_once()
    assert trial.service.status(program_id=app.PROGRAM)["mean_delta_bps"] == 40.


def test_pause_preserves_release_and_history_without_reading_sources(trial, monkeypatch):
    trial.service.run_once()
    original = trial.path.read_bytes()
    monkeypatch.setenv("AISTOCK_ADVISORY_LSTM_SPO_SHADOW_ENABLED", "false")
    trial.source.publications = lambda: pytest.fail("paused trial may not read sources")
    assert trial.service.run_once()["status"] == "PAUSED"
    assert trial.service.status(program_id=app.PROGRAM)["captured_days"] == 1
    assert trial.path.read_bytes() == original


def test_restart_accounts_for_missed_days_before_new_capture(trial):
    publications = [{**trial.publication, "decision_date": f"2026-10-{d}", "target_date": f"2026-10-{d+1}",
                     "created_at": f"2026-10-{d}T08:45:00+00:00", "list_version_id": f"list_{d}"} for d in (27, 28, 29)]
    trial.source.publications = lambda: publications
    trial.now[0] = app.clock("2026-10-30", 8)
    trial.rows[app.KEY[0]], trial.rows[app.KEY[1]] = pd.Timestamp("2026-10-29"), pd.Timestamp("2026-10-30")
    results = trial.service.run_once()["results"]
    assert [r["status"] for r in results[:3]] == ["MISSED_CAPTURE_WINDOW", "MISSED_CAPTURE_WINDOW", "PREDICTED"]
    status = trial.service.status(program_id=app.PROGRAM)
    assert status["captured_days"] == 1 and len(status["missed"]) == 2


def test_original_score_top50_not_truncated_native_list(trial):
    from backend.services.strategy_package.selection_artifact import _canonical_json_sha256
    scores = [dict(rank=i+1, symbol=f"{600000+i}.SH", score=100.-i) for i in range(70)]
    run = NS(package_ids=[app.TARGET["package_id"]], manifest_sha256_by_package={app.TARGET["package_id"]: app.TARGET["manifest_sha256"]},
             status="SUCCEEDED", trade_date=date(2026, 10, 28), data_source="DB_HISTORICAL",
             runtime_config={"package_runtime_configs": {app.TARGET["package_id"]: {"qe_backtest_runtime_contract": {"source": app.TARGET}}}},
             package_results={app.TARGET["package_id"]: [NS(**s, component_scores={"raw_rank": s["rank"]}) for s in scores[:40]]})
    artifact = NS(**{k: app.TARGET[k] for k in ("package_id", "manifest_sha256")}, trade_date=run.trade_date,
        data_source=run.data_source, status="SUCCEEDED", metadata=dict(score_trade_date="2026-10-27", cutoff_date="2026-10-27",
        source_id=app.TARGET["source_id"]), scores_json=scores, artifact_sha256=_canonical_json_sha256(scores), score_count=70)
    original = top50(run, artifact, trial.publication)
    assert len(original) == 50 and original.instrument.iloc[-1] == "600049.SH"
    features = original.loc[:, [*app.KEY, "selection_effective_rank", "candidate_group_size"]].copy()
    for name in STOCK_FEATURES:
        features[name] = np.nan
    joined = attach_features(original, features)
    assert joined.loc[:, STOCK_FEATURES].isna().all().all()
    assert joined.parent_rank_fraction.iloc[-1] == 1
    run.package_results[app.TARGET["package_id"]][0].score += 1
    with pytest.raises(ValueError, match="consumed scores"):
        top50(run, artifact, trial.publication)


@pytest.fixture
def publication_reader(trial, monkeypatch):
    from backend.services.advisory_program import AdvisoryProgramPGRepository
    from backend.services.selection_center.repository import SelectionCenterRepository

    class AssetReadReached(RuntimeError):
        pass

    calls = []

    class Session:
        @contextmanager
        def connection(self):
            yield object()

        def close(self):
            pass

    version = NS(program_id=app.PROGRAM, binding_version_id=app.BINDING,
        version_status="PUBLISHED", created_at=datetime.fromisoformat(trial.publication["created_at"]),
        summary_json={"selection_run_id": "sel_test", "advisory_date_context": {
            "decision_as_of_trade_date": "2026-10-27", "target_trade_date": "2026-10-28"}})
    monkeypatch.setattr(AdvisoryProgramPGRepository, "get_list_version", lambda *_: version)

    def get_run(_repo, run_id):
        calls.append(run_id)
        raise AssetReadReached("publication accepted before any score or feature read")

    monkeypatch.setattr(SelectionCenterRepository, "get_run", get_run)
    return NS(source=LstmSpoShadowSource(session_factory=Session, now=lambda: trial.now[0]),
              version=version, calls=calls, accepted=AssetReadReached)


@pytest.mark.parametrize("stamp", ["2026-10-27T16:45:00+08:00", "2026-10-27T14:15:00+05:30",
                                  "2026-10-27T08:45:00Z"])
def test_source_accepts_same_publication_instant_across_timezones(trial, publication_reader, stamp):
    trial.publication["created_at"] = stamp
    with pytest.raises(publication_reader.accepted):
        publication_reader.source.inputs(trial.publication)
    assert publication_reader.calls == ["sel_test"]


@pytest.mark.parametrize("stamp", ["2026-10-27T16:45:00.000001+08:00", "2026-10-27T08:45:00",
                                  "not-a-time"])
def test_source_rejects_changed_or_ambiguous_clock_before_asset_reads(trial, publication_reader, stamp):
    trial.publication["created_at"] = stamp
    with pytest.raises(ValueError):
        publication_reader.source.inputs(trial.publication)
    assert publication_reader.calls == []


@pytest.mark.parametrize("field", ["program_id", "binding_version_id", "selection_run_id", "decision_date", "target_date"])
def test_equivalent_clock_does_not_relax_publication_identity(trial, publication_reader, field):
    trial.publication["created_at"] = "2026-10-27T16:45:00+08:00"
    trial.publication[field] = "changed"
    with pytest.raises(ValueError, match="original publication reference differs"):
        publication_reader.source.inputs(trial.publication)
    assert publication_reader.calls == []


@pytest.mark.parametrize("invalid", ["future", "naive_stored", "unpublished"])
def test_source_rejects_invalid_stored_publication(trial, publication_reader, invalid):
    if invalid == "future":
        trial.now[0] = datetime(2026, 10, 27, 8, tzinfo=timezone.utc)
    elif invalid == "naive_stored":
        publication_reader.version.created_at = publication_reader.version.created_at.replace(tzinfo=None)
    else:
        publication_reader.version.version_status = "FAILED"
    with pytest.raises(ValueError, match="original publication reference differs"):
        publication_reader.source.inputs(trial.publication)
    assert publication_reader.calls == []


def test_calendar_check_precedes_mature_market_reads_and_real_valuation(trial):
    days = [d.date() for d in pd.bdate_range("2026-10-27", periods=6)]
    calls = []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, sql, args): calls.append(sql)
        def fetchall(self):
            sql = calls[-1]
            if 'trading_calendar' in sql:
                return [(d,) for d in days]
            if 'kline_daily_raw' in sql:
                return [(d, s, 10000, 11000, 9000, 10000, 10, 1.) for d in days for s in trial.rows.instrument]
            if 'stk_limit' in sql:
                return [(d, s, 11., 9.) for d in days[1:] for s in trial.rows.instrument]
            return []
    class Session:
        @contextmanager
        def connection(self): yield NS(cursor=Cursor)
        def close(self): pass
    source = LstmSpoShadowSource(session_factory=Session, now=lambda: trial.now[0])
    packet = dict(inputs=app._records(trial.rows), publication=trial.publication)
    assert source.finance(packet) is None and len(calls) == 1
    trial.now[0] = app.clock(days[-1], 16)
    finance, receipt = source.finance(packet)
    expected = 10000*((1-5.95/10000)/(1+0.95/10000)-1)
    assert finance.valuation_status.eq("AVAILABLE").all()
    np.testing.assert_allclose(finance.hypothetical_liquidation_net_bps, expected)
    assert receipt["horizon_end"] == str(days[-1])
    assert len(calls) == 5


def test_optional_scheduler_failure_visible_and_read_api_never_runs(trial, monkeypatch):
    from backend.services.advisory_forward.service import AdvisoryForwardService
    from backend.routers.advisory import reranking_shadow
    def broken(): raise ValueError("source identity changed")
    service = object.__new__(AdvisoryForwardService)
    service.lstm_shadow_service = NS(run_once=broken)
    service.now_provider = lambda: trial.now[0]
    assert service._lstm_shadow_after_baseline()["status"] == "FAILED"
    monkeypatch.setattr(app.LstmSpoShadowService, "run_once", lambda *a: pytest.fail("GET may not run"))
    monkeypatch.setenv("AISTOCK_ADVISORY_MODEL_ROOT", str(trial.service.root.parent))
    assert reranking_shadow(app.PROGRAM, None)["status"] == "SHADOW_ONLY"
    assert reranking_shadow("other_program", None)["status"] == "NOT_APPLICABLE"
