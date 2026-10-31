"""V2 uses one fixed look, detects capture failure without opening any outcomes."""
from copy import deepcopy
from contextlib import contextmanager
from datetime import date, timedelta

import numpy as np
import pytest

from scripts import advisory_lstm_top10_business_review as v2

v1 = v2.v1


def calendar(length=300):
    start = date.fromisoformat(v1.RULE["first_d"])
    return dict(schema="confirmation_calendar_v1", source="market.trading_calendar_READ_ONLY",
        observed_at="2026-11-01T12:00:00+08:00", database_write=False,
        rows=[[(start + timedelta(days=i)).isoformat(), (start + timedelta(days=i)).weekday() < 5]
              for i in range(length)])


@pytest.fixture
def context(tmp_path, monkeypatch):
    monkeypatch.setattr(v2, "verify", lambda _: None)
    monkeypatch.setattr(v1, "LIVE", tmp_path / "live")
    v1.LIVE.mkdir()
    (v1.LIVE / "model.json").write_text("test model loading is stubbed, no actual model access")
    monkeypatch.setattr(v1.shadow, "checked_model", lambda _: {})
    monkeypatch.setattr(v1, "now_utc", lambda: v1.instant("2026-11-01T18:00:00+08:00"))
    monkeypatch.setattr(v1.shadow.spo, "fit", lambda *_: pytest.fail("fit forbidden"))
    monkeypatch.setattr(v1.shadow.LstmSpoShadowService, "_source", lambda *_: pytest.fail("market source forbidden"))
    path = tmp_path / "calendar.json"
    v1.shadow.publish(path, calendar())
    protocol = dict(study_root=str(tmp_path / "study"), program_id=v1.shadow.PROGRAM, binding_version_id=v1.shadow.BINDING)
    return protocol, path, tmp_path, monkeypatch


def test_partial_calendar_is_sufficient_for_not_yet_due_capture_health(context):
    protocol, _, root, _ = context
    path = root / "partial.json"
    v1.shadow.publish(path, calendar(60))
    result = v2.health(protocol, path)
    assert result["status"] == "CAPTURE_VIABLE_SO_FAR" and result["evaluation_files_read"] == 0
    assert result["known_calendar_decisions"] < 150 and result["unmaterialized_decisions"] > 0
    assert all(r["capture_status"] == "PLANNED" for r in result["rows"])
    assert v2.finalize(protocol, path)["status"] == "WAITING_CALENDAR"


def test_window_open_is_not_a_permanent_failure(context):
    protocol, path, _, monkeypatch = context
    monkeypatch.setattr(v1, "now_utc", lambda: v1.shadow.clock("2026-11-02", 22))
    state = v2.health(protocol, path)
    assert state["rows"][0]["capture_status"] == "CAPTURE_WINDOW_OPEN" and not state["capture_gaps"]


def test_capture_gap_finalizes_early_with_no_evaluation_file_access(context):
    protocol, path, root, monkeypatch = context
    monkeypatch.setattr(v1, "now_utc", lambda: v1.shadow.clock("2026-11-03", 9, 30))
    monkeypatch.setattr(v1, "collect", lambda *_: pytest.fail("early operational stop must never read outcomes"))
    directory = v1.LIVE / "days/2026-11-02/evaluations"
    directory.mkdir(parents=True)
    (directory / "trap.json").write_text("must never be opened")
    result = v2.finalize(protocol, path)
    assert result["status"] == "NOT_EVALUABLE_CAPTURE_GAP" and result["evaluation_files_read"] == 0
    assert not result["alpha_absent_proven"]
    first = (root / "study/evaluated/result.json").read_bytes()
    assert v2.finalize(protocol, path)["status"] == "FINAL_RESULT_REUSED"
    assert (root / "study/evaluated/result.json").read_bytes() == first


def test_unknown_evaluations_are_never_inspected_or_declared_irrecoverable_early(context):
    protocol, path, _, monkeypatch = context
    day = v1.LIVE / "days/2026-11-02"
    day.mkdir(parents=True)
    (day / "prediction.json").write_text("prediction reader is stubbed")
    (day / "evaluations").mkdir()
    (day / "evaluations/unknown.json").write_text("unknown evaluation must stay unread")
    monkeypatch.setattr(v1, "now_utc", lambda: v1.shadow.clock("2026-11-03", 10))
    monkeypatch.setattr(v1, "check_prediction", lambda *_: dict(observed_at="2026-11-02T20:00:00+08:00"))
    state = v2.health(protocol, path)
    assert state["rows"][0]["capture_status"] == "CAPTURED" and state["unknown_valuations_not_inspected"]
    assert v2.finalize(protocol, path)["status"] == "AWAITING_FIXED_LOOK"


def test_missed_identity_and_prediction_conflict_are_rejected(context):
    protocol, path, _, monkeypatch = context
    monkeypatch.setattr(v1, "now_utc", lambda: v1.shadow.clock("2026-11-03", 10))
    day = v1.LIVE / "days/2026-11-02"
    body = dict(release_sha256=v1.RELEASE_SHA, observed_at="2026-11-03T09:30:00+08:00",
        publication=dict(program_id=v1.shadow.PROGRAM, binding_version_id=v1.shadow.BINDING,
                         decision_date="2026-11-02", target_date="2026-11-03"))
    v1.shadow.publish(day / "missed.json", body)
    assert v2.health(protocol, path)["capture_gaps"] == ["2026-11-02"]
    (day / "prediction.json").write_text("conflict")
    with pytest.raises(ValueError, match="coexist"):
        v2.health(protocol, path)


@pytest.mark.parametrize("status", ["complete", "below_target", "unknown"])
def test_fixed_economic_look_reuses_original_statistics_and_keeps_target_separate(context, status):
    protocol, path, _, monkeypatch = context
    schedule = v1.scheduled(calendar())
    monkeypatch.setattr(v1, "now_utc", lambda: schedule["look_at"] + timedelta(hours=1))
    state = dict(rows=[], consumed={}, capture_gaps=[], status="CAPTURE_VIABLE_SO_FAR", calendar=v1.ref(path))
    monkeypatch.setattr(v2, "health", lambda *_: state)
    effect = 50. if status == "below_target" else 110.
    rows = [dict(status="COMPLETE", delta_bps=effect + np.sin(i), result=dict(native_net_bps=20.,
            specialist_net_bps=20. + effect + np.sin(i), intervention_count=7)) for i in range(150)]
    if status == "unknown":
        rows[20] = dict(status="UNKNOWN", delta_bps=None)
    monkeypatch.setattr(v1, "collect", lambda *_: (rows, {}, 150))
    result = v2.finalize(protocol, path)
    if status == "complete":
        assert result["statistical_support"] and result["business_review_status"] == "WORTH_BUSINESS_REVIEW"
        assert not result["true_mean_above_target_proven"]
    elif status == "below_target":
        assert result["statistical_support"]
        assert result["business_review_status"] == "TARGET_OR_STATISTICAL_SUPPORT_NOT_ESTABLISHED"
    else:
        assert result["status"] == "NOT_EVALUABLE" and result["unresolved_dates"] == 1
    assert result["target_increment_bps"] == 100 and not result["production_ranking_replaced"]


def test_legacy_hash_and_original_git_closure_are_preserved(tmp_path, monkeypatch):
    old = dict(rule=v1.RULE, producer_commit="old-producer", source_closure={"f.py": v1.files.code_sha(b"old\n")},
               study_root=str(tmp_path / "old-study"))
    path = tmp_path / "old-protocol.json"
    path.write_bytes(v1.files.json_bytes(old))
    monkeypatch.setattr(v2, "LEGACY", path)
    monkeypatch.setattr(v2, "LEGACY_SHA", v1.shadow.spo.sha(old))
    monkeypatch.setattr(v1, "read_stage", lambda *_a, **_k: {})
    calls = []
    def committed(args, **_kwargs):
        calls.append(args)
        return b"old\r\n"
    monkeypatch.setattr(v2.subprocess, "check_output", committed)
    assert v2.legacy() == old
    assert calls[0][-1] == "old-producer:f.py"
    (tmp_path / "old-study/evaluated").mkdir(parents=True)
    with pytest.raises(ValueError, match="already has a formal result"):
        v2.legacy()


def test_late_new_protocol_is_rejected(context, monkeypatch):
    _, _, root, _ = context
    monkeypatch.setattr(v2, "OUTPUT", root / "v2")
    monkeypatch.setattr(v1, "now_utc", lambda: v1.shadow.clock(v1.RULE["first_d"], 0))
    with pytest.raises(ValueError, match="registered before"):
        v2.freeze()


def test_v2_protocol_rejects_changed_business_target_scope_source_and_clock(tmp_path, monkeypatch):
    monkeypatch.setattr(v2, "OUTPUT", tmp_path)
    paths = {name: tmp_path / name for name in ("model", "release", "aggregate", "legacy_protocol", "legacy_manifest")}
    for path in paths.values():
        path.write_text("immutable input")
    monkeypatch.setattr(v2, "refs", lambda: paths)
    monkeypatch.setattr(v2, "closure", lambda: {"file.py": "source"})
    monkeypatch.setattr(v2, "legacy", lambda: {})
    protocol = dict(schema=v2.SCHEMA, rule=deepcopy(v1.RULE), business_contract=deepcopy(v2.CONTRACT),
        supersedes_v1=v2.LEGACY_SHA, program_id=v1.shadow.PROGRAM, binding_version_id=v1.shadow.BINDING,
        target=v1.shadow.TARGET, release_sha256=v1.RELEASE_SHA, source_closure={"file.py": "source"},
        registered_at="2026-11-01T12:00:00+08:00", study_root=str(tmp_path / "study"),
        sources={name: v1.ref(path) for name, path in paths.items()})
    v2.verify(protocol)
    changes = [("business_contract", {**v2.CONTRACT, "target_increment_bps": 50}),
               ("rule", {**v1.RULE, "cohorts": 149}), ("program_id", "other"),
               ("source_closure", {"file.py": "changed"}), ("study_root", str(tmp_path / "other")),
               ("registered_at", "2026-11-02T00:00:00+08:00")]
    for key, bad in changes:
        with pytest.raises(ValueError, match="identity"):
            v2.verify({**protocol, key: bad})
    paths["aggregate"].write_text("changed")
    with pytest.raises(ValueError, match="source changed"):
        v2.verify(protocol)


def test_absolute_budget_with_incomplete_calendar_stops_without_outcomes(context):
    protocol, _, root, monkeypatch = context
    path = root / "short.json"
    v1.shadow.publish(path, calendar(60))
    monkeypatch.setattr(v1, "now_utc", lambda: v1.instant(v1.RULE["absolute_close"]))
    state = dict(rows=[], consumed={}, capture_gaps=[], status="CAPTURE_VIABLE_SO_FAR", calendar=v1.ref(path))
    monkeypatch.setattr(v2, "health", lambda *_: state)
    monkeypatch.setattr(v1, "collect", lambda *_: pytest.fail("no calendar, no outcomes"))
    result = v2.finalize(protocol, path)
    assert result["status"] == "NOT_EVALUABLE" and result["evaluation_files_read"] == 0


def test_v2_calendar_collector_uses_only_existing_calendar_source_and_exact_output_root(tmp_path, monkeypatch):
    from backend.services.advisory_model_first.lstm_spo_shadow_source_v1 import LstmSpoShadowSource
    calls = []
    class Connection:
        @contextmanager
        def cursor(self):
            yield self

        def execute(self, sql, parameters):
            calls.append((sql, parameters))

        def fetchall(self):
            return [(date.fromisoformat(d), value) for d, value in calendar(60)["rows"]]

    @contextmanager
    def connection(_):
        yield Connection()

    monkeypatch.setattr(v2, "OUTPUT", tmp_path / "allowed")
    monkeypatch.setattr(LstmSpoShadowSource, "connection", connection)
    packet = v2.calendar_snapshot(tmp_path / "allowed/calendar.json")
    assert len(packet["rows"]) == 60 and packet["database_write"] is False
    assert len(calls) == 1 and "SELECT cal_date,is_trading FROM market.trading_calendar" in calls[0][0]
    with pytest.raises(ValueError, match="V2 evidence"):
        v2.calendar_snapshot(tmp_path / "other.json")
