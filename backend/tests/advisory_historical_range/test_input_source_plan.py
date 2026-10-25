from copy import deepcopy
from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range import input_source_plan as sources
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from backend.tests.data_service.test_frozen_dataset import make_dataset, make_monthly_dataset, SYMBOL

CALENDAR = [f"2026-01-{d}" for d in (19, 20, 21, 22)]
POLICY = {"policy_hash": "a" * 64, "universe_key": "TEST_PIT", "rule_version": "test-v1",
          "reader_contract": sources.READER_CONTRACT}


def _snapshot(tmp_path, *, gap=None, market_start=None, policy=POLICY, monthly_metadata=False):
    profile_path = make_dataset(tmp_path)
    profile = json.loads(profile_path.read_bytes())
    path = tmp_path / "qe_dataset_manifest.json"
    manifest = json.loads(path.read_bytes())
    if gap:
        component = manifest["components"][gap]
        calendar_path = tmp_path / component["path"]
        calendar_path.write_text("\n".join(line for line in calendar_path.read_text().splitlines()
                                         if not line.startswith(CALENDAR[1])) + "\n", encoding="utf-8")
        component["sha256"] = hashlib.sha256(calendar_path.read_bytes()).hexdigest()
    dates = {"start": CALENDAR[0], "end": CALENDAR[2]}
    for key in ("day_meta_export", "minute_meta_export", "factor_meta"):
        file = tmp_path / manifest["components"][key]["path"]
        meta = json.loads(file.read_bytes())
        meta.update(dates, universe_key=policy["universe_key"], rule_version=policy["rule_version"],
                    pit_ensure={"start_date": CALENDAR[0]})
        if monthly_metadata:
            meta.pop("pit_ensure")
            meta.update(source_freeze=True, full_history_content_hash=True)
        if key == "minute_meta_export" and market_start:
            meta["start"] = market_start
        file.write_text(json.dumps(meta), encoding="utf-8")
        manifest["components"][key]["sha256"] = hashlib.sha256(file.read_bytes()).hexdigest()
    suspend = tmp_path / "suspend-meta.json"
    suspend.write_text(json.dumps({**dates, "start": market_start or dates["start"]}), encoding="utf-8")
    manifest["components"]["suspend_meta"] = {"path": suspend.name, "sha256": hashlib.sha256(suspend.read_bytes()).hexdigest()}
    manifest["components"]["sector_membership_spans"].update(dates)
    manifest["st_pit_manifest"] = {"universe_key": policy["universe_key"], "rule_version": policy["rule_version"], "cutoff_trade_date": CALENDAR[2]}
    path.write_text(json.dumps(manifest), encoding="utf-8")
    profile["components"]["dataset_manifest_file_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    profile["cutoff"] = CALENDAR[2]
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    return sources.freeze_file_source(profile_path=profile_path)


def _plan(mode="DATABASE_ONLY", snapshot=None, *, days=None, consumer="ADVISORY", run="test-run"):
    return sources.build_source_plan(mode=mode, consumer_scope=consumer, run_id=run,
        days=days or [{"decision_date": d, "data_cutoff": CALENDAR[i], "target_trade_date": d,
                      "history_start": CALENDAR[0]} for i, d in enumerate(CALENDAR[1:])],
        native_policy=POLICY, trading_calendar=CALENDAR, file_source=snapshot)


def _clocks(row):
    return SelectionSignalDayV1(*(date.fromisoformat(row[k]) for k in ("decision_date", "data_cutoff", "target_trade_date")))


@pytest.mark.parametrize("consumer", ["ADVISORY", "LOCALSIM"])
def test_monthly_path_indexed_release_can_freeze_one_native_pit_plan(tmp_path, consumer):
    snapshot = sources.freeze_file_source(profile_path=make_monthly_dataset(tmp_path), consumer_scope=consumer)
    plan = _plan("FILE_THEN_DATABASE", snapshot, consumer=consumer)
    assert plan["component_coverage"]["pit"] == [CALENDAR[0], CALENDAR[2]]
    assert plan["component_coverage"]["sector"] == [CALENDAR[0], CALENDAR[2]]
    session = sources.create_input_session(plan, _clocks(plan["days"][0]), consumer_scope=consumer,
                                           run_id="test-run", max_bytes=10**6)
    try:
        assert session.file_dataset.members(date.fromisoformat(CALENDAR[0])) == [SYMBOL]
    finally:
        session.close()


@pytest.mark.parametrize("consumer", ["ADVISORY", "LOCALSIM"])
def test_monthly_pit_metadata_routes_file_to_database_and_restores_without_active(tmp_path, monkeypatch, consumer):
    snapshot = _snapshot(tmp_path, monthly_metadata=True)
    plan = _plan("FILE_THEN_DATABASE", snapshot, consumer=consumer)
    assert plan["component_coverage"]["pit"] == [CALENDAR[0], CALENDAR[2]]
    assert [row["source"] for row in plan["routes"]] == ["DATASET_FILES", "DATASET_FILES", "DATABASE"]
    assert plan["first_database_day"] == CALENDAR[-1]
    assert plan["native_policy"] == POLICY
    files = _plan("FILE_ONLY", snapshot, consumer=consumer, days=plan["days"][:2])
    assert all(row["source"] == "DATASET_FILES" for row in files["routes"])
    monkeypatch.setattr(sources, "freeze_file_source", lambda **_: pytest.fail("restore resolved active"))
    assert sources.validate_source_plan(plan, consumer_scope=consumer, run_id="test-run") == plan
    assert sources.validate_source_plan(files, consumer_scope=consumer, run_id="test-run") == files


@pytest.mark.parametrize("change", [
    "unsealed", "missing_hash_contract", "invalid_start", "missing_cutoff", "reversed", "invalid_legacy",
])
def test_monthly_pit_metadata_does_not_invent_missing_or_invalid_coverage(tmp_path, change):
    snapshot = _snapshot(tmp_path, monthly_metadata=True)
    day = snapshot["metadata"]["day_meta_export"]
    if change == "unsealed":
        day["source_freeze"] = False
    elif change == "missing_hash_contract":
        day.pop("full_history_content_hash")
    elif change == "invalid_start":
        day["start"] = "2026-1-19"
    elif change == "missing_cutoff":
        snapshot["pit_coverage"].pop("cutoff_trade_date")
    elif change == "reversed":
        snapshot["pit_coverage"]["cutoff_trade_date"] = "2026-01-16"
    else:
        day["pit_ensure"] = None  # Explicit legacy declarations cannot be ignored.
    with pytest.raises(sources.SourcePlanError, match="SOURCE_COMPONENT_COVERAGE_") as exc:
        sources._coverage(snapshot, POLICY)
    assert exc.value.receipt["component"] == "pit"
    assert exc.value.receipt["owner"] == "Data"


def test_three_modes_freeze_expiry_and_never_consult_active_on_retry(tmp_path, monkeypatch):
    snapshot = _snapshot(tmp_path)
    mixed = _plan("FILE_THEN_DATABASE", snapshot)
    assert [r["source"] for r in mixed["routes"]] == ["DATASET_FILES", "DATASET_FILES", "DATABASE"]
    assert mixed["first_database_day"] == CALENDAR[-1]
    files = _plan("FILE_ONLY", snapshot, days=mixed["days"][:2])
    db = _plan()
    assert all(r["source"] == "DATABASE" for r in db["routes"])
    assert files["first_database_day"] is None
    with pytest.raises(sources.SourcePlanError, match="COVERAGE_EXHAUSTED"):
        _plan("FILE_ONLY", snapshot)
    monkeypatch.setattr(sources, "freeze_file_source", lambda **_: pytest.fail("retry resolved active"))
    (tmp_path / "active.json").write_text("new active release", encoding="utf-8")
    assert sources.validate_source_plan(mixed, consumer_scope="ADVISORY", run_id="test-run") == mixed
    with pytest.raises(sources.SourcePlanError, match="IDENTITY"):
        sources.validate_source_plan(mixed, consumer_scope="LOCALSIM", run_id="test-run")
    tampered = deepcopy(mixed)
    tampered["routes"][0]["source"] = "DATABASE"
    with pytest.raises(sources.SourcePlanError):
        sources.validate_source_plan(tampered, consumer_scope="ADVISORY", run_id="test-run")


def test_internal_holes_policy_and_warmup_never_become_database_fallback(tmp_path):
    snapshot = _snapshot(tmp_path)
    for kind in ("calendar", "minute_days"):
        bad = deepcopy(snapshot)
        bad[kind].remove(CALENDAR[1])
        with pytest.raises(sources.SourcePlanError, match="FILE_METADATA_DRIFT"):
            _plan("FILE_THEN_DATABASE", bad)
    bad = deepcopy(snapshot)
    bad["metadata"]["day_meta_export"]["rule_version"] = "different"
    with pytest.raises(sources.SourcePlanError, match="FILE_METADATA_DRIFT"):
        _plan("FILE_THEN_DATABASE", bad)
    days = _plan()["days"]
    days[0]["history_start"] = "2026-01-16"
    with pytest.raises(sources.SourcePlanError, match="HISTORY_UNCOVERED"):
        _plan("FILE_THEN_DATABASE", snapshot, days=days)


@pytest.mark.parametrize("component,reason", [("day_calendar", "CALENDAR_GAP"), ("minute_calendar", "MINUTE_DAY_GAP")])
def test_genuine_pinned_calendar_holes_are_errors_inside_coverage(tmp_path, component, reason):
    snapshot = _snapshot(tmp_path, gap=component)
    with pytest.raises(sources.SourcePlanError, match=reason):
        _plan("FILE_THEN_DATABASE", snapshot)


def test_native_policy_mismatch_and_optional_component_coverage(tmp_path):
    snapshot = _snapshot(tmp_path)
    wrong = {**POLICY, "rule_version": "other-native-policy"}
    with pytest.raises(sources.SourcePlanError, match="PIT_POLICY_MISMATCH"):
        sources.build_source_plan(mode="FILE_ONLY", consumer_scope="ADVISORY", run_id="wrong",
            days=_plan()["days"][:2], trading_calendar=CALENDAR, native_policy=wrong, file_source=snapshot)
    coverage = sources._coverage(snapshot, {**POLICY, "required_components": ["daily", "minute", "factor", "pit"]})
    assert set(coverage) == {"daily", "minute", "factor", "pit"}


def test_market_coverage_uses_D_T_but_model_inputs_use_C_and_file_identity_is_explicit(tmp_path):
    snapshot = _snapshot(tmp_path, market_start=CALENDAR[1])
    plan = _plan("FILE_ONLY", snapshot, days=_plan()["days"][:2])
    route = sources.resolve_day_source(plan, _clocks(plan["days"][0]))
    assert route["data_cutoff"] == CALENDAR[0] and route["source"] == "DATASET_FILES"
    with pytest.raises(sources.SourcePlanError, match="NATIVE_PIT_IDENTITY_REQUIRED"):
        sources.validate_file_pit_declaration(route, request_identity=None, manifest_identity=None)
    sources.validate_file_pit_declaration(route, request_identity={"mode": "native-declaration"}, manifest_identity=None)
    sources.validate_file_pit_declaration({"source": "DATABASE"}, request_identity=None, manifest_identity=None)


def test_sessions_reuse_native_builders_full_db_window_and_private_inputs(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range import inference_inputs
    from backend.data_service.qe_data_service import build_static_factors
    plan = _plan("FILE_THEN_DATABASE", _snapshot(tmp_path))
    clocks = _clocks(plan["days"][1])
    file = sources.create_input_session(plan, clocks, consumer_scope="ADVISORY", run_id="test-run", max_bytes=10**6)
    assert file.static_builder.__code__ is build_static_factors.__code__
    assert file.price_reader([SYMBOL], date(2026, 1, 19), date(2026, 1, 20)).close.tolist() == [10.5, 11.5]
    calls = []
    def db(**kwargs):
        session = inference_inputs.InferenceInputSession(**kwargs)
        session.price_reader = lambda members, start, end: calls.append((members, start, end))
        return session
    monkeypatch.setattr(inference_inputs, "database_input_session", db)
    first_db = sources.create_input_session(plan, _clocks(plan["days"][-1]), consumer_scope="ADVISORY", run_id="test-run", max_bytes=10**6)
    first_db.price_reader([SYMBOL], date(2026, 1, 19), date(2026, 1, 21))
    assert calls == [([SYMBOL], date(2026, 1, 19), date(2026, 1, 21))]
    assert first_db.file_dataset is None and first_db is not file
    with pytest.raises(sources.SourcePlanError, match="OUTSIDE_C_WINDOW"):
        first_db.read(dataset="test", members=[SYMBOL], start=date(2026, 1, 19), end=date(2026, 1, 22),
                      loader=lambda *_: pytest.fail("future data reader called"))
    (tmp_path / "factors/daily_pv.h5").touch()
    with pytest.raises(ValueError, match="changed"):
        file.price_reader([SYMBOL], date(2026, 1, 19), date(2026, 1, 20))
    file.close()
    first_db.close()


def _payload(route):
    return {"ok": True, "scores": [{"symbol": SYMBOL, "score": 1.0}], "universe_count": 2,
            "metadata": {"universe_members": [SYMBOL, "OTHER"], "unscored_members": [{"symbol": "OTHER", "availability": "UNKNOWN"}]},
            "input_context": {"effective_trade_date": route["data_cutoff"], "window_start_date": route["history_start"]},
            "source_read_receipts": [{"source_role": k, "content_hash": "f" * 64, "row_count": 2}
                for k in ("pit_universe", "market_history", "fundamental_moneyflow", "trading_calendar")]}


def test_input_receipts_keep_actual_content_and_detect_drift_without_changing_results():
    plan = _plan()
    route = sources.resolve_day_source(plan, _clocks(plan["days"][-1]))
    payload = _payload(route)
    payload["input_context"]["source_plan_receipt"] = sources.input_receipt(route, payload)
    original = deepcopy(payload)
    sources.verify_retry_inputs(original, payload)
    assert original == payload
    for field in ("scores", "metadata"):
        changed = deepcopy(payload)
        changed[field] = [] if field == "scores" else {}
        with pytest.raises(sources.SourcePlanError, match="SOURCE_INPUT_DRIFT"):
            sources.verify_retry_inputs(original, changed)
    changed = deepcopy(payload)
    changed["source_read_receipts"][1]["content_hash"] = "0" * 64
    changed["input_context"]["source_plan_receipt"] = sources.input_receipt(route, changed)
    with pytest.raises(sources.SourcePlanError, match="SOURCE_INPUT_DRIFT"):
        sources.verify_retry_inputs(original, changed)
    changed = deepcopy(payload)
    changed["input_context"]["pit_identity"] = {"authority_generation": "different"}
    changed["input_context"]["source_plan_receipt"] = sources.input_receipt(route, changed)
    with pytest.raises(sources.SourcePlanError, match="SOURCE_INPUT_DRIFT"):
        sources.verify_retry_inputs(original, changed)
    route["data_cutoff"] = "2026-01-20"
    with pytest.raises(sources.SourcePlanError, match="WINDOW_MISMATCH"):
        sources.input_receipt(route, payload)


def test_native_window_guard_does_not_change_C_D_or_shorten_history():
    plan = _plan()
    clocks = _clocks(plan["days"][1])
    route = sources.resolve_day_source(plan, clocks)
    binding = SimpleNamespace(universe_key="TEST_PIT", rule_version="test-v1")
    sources.validate_native_window(route, start=date(2026, 1, 19), cutoff=clocks.data_cutoff,
                                   decision=clocks.decision_date, pit_binding=binding)
    for start, cutoff in ((date(2026, 1, 16), clocks.data_cutoff), (date(2026, 1, 19), clocks.decision_date)):
        with pytest.raises(sources.SourcePlanError, match="WINDOW_MISMATCH"):
            sources.validate_native_window(route, start=start, cutoff=cutoff, decision=clocks.decision_date, pit_binding=binding)
    binding.rule_version = "different"
    with pytest.raises(sources.SourcePlanError, match="PIT_POLICY_MISMATCH"):
        sources.validate_native_window(route, start=date(2026, 1, 19), cutoff=clocks.data_cutoff,
                                       decision=clocks.decision_date, pit_binding=binding)


def test_canonical_policy_accepts_frozen_storage_key_not_different_authority():
    from dataclasses import replace
    from backend.services.canonical_equity_pit import (
        CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION,
        PitConsumerBinding, PitAuthorityStatus, canonical_rule_parameters_digest,
    )
    binding = PitConsumerBinding(authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key="aistock_equity_pit_snapshot_release1", rule_version=CANONICAL_PIT_RULE_VERSION,
        rule_parameters_digest=canonical_rule_parameters_digest(), release_id="release1",
        snapshot_digest="a" * 64, cutoff=date(2026, 1, 21))
    route = {**sources.resolve_day_source(_plan(), _clocks(_plan()["days"][0])),
             "universe_key": CANONICAL_PIT_UNIVERSE_KEY, "rule_version": CANONICAL_PIT_RULE_VERSION,
             "pit_policy": {"authority_id": CANONICAL_PIT_AUTHORITY_ID,
                            "rule_parameters_digest": canonical_rule_parameters_digest()}}
    sources.validate_native_window(route, start=date(2026, 1, 19), cutoff=date(2026, 1, 19),
                                   decision=date(2026, 1, 20), pit_binding=binding)
    for changed in (replace(binding, rule_parameters_digest="b" * 64),
                    replace(binding, authority_id="other"), replace(binding, snapshot_digest=None)):
        with pytest.raises(sources.SourcePlanError, match="PIT_POLICY_MISMATCH"):
            sources.validate_native_window(route, start=date(2026, 1, 19), cutoff=date(2026, 1, 19),
                                           decision=date(2026, 1, 20), pit_binding=changed)


def _canonical_policy():
    from backend.services.canonical_equity_pit import CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION
    return {**POLICY, "universe_key": CANONICAL_PIT_UNIVERSE_KEY, "rule_version": CANONICAL_PIT_RULE_VERSION}


def test_file_identity_inherits_frozen_publisher_membership_without_live_pointer(tmp_path, monkeypatch):
    from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
    from backend.services.canonical_pit_inference_boundary import resolve_inference_pit_identity
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding",
                        lambda _: pytest.fail("file mode queried live pointer"))
    snapshot = _snapshot(tmp_path, policy=_canonical_policy())
    plan = sources.build_source_plan(mode="FILE_ONLY", consumer_scope="LOCALSIM", run_id="private",
        days=_plan()["days"][:2], trading_calendar=CALENDAR, native_policy=_canonical_policy(), file_source=snapshot)
    session = sources.create_input_session(plan, _clocks(plan["days"][0]), consumer_scope="LOCALSIM",
                                           run_id="private", max_bytes=0)
    try:
        identity = sources.session_pit_identity(session, request_identity=None, manifest_identity=None)
        parsed = resolve_inference_pit_identity(identity, version_tag="historical")
        assert parsed.universe_codes == (SYMBOL, "603616.SH") and str(parsed.universe_as_of) == CALENDAR[0]
        assert identity["snapshot_digest"] == snapshot["binding"]["files"]["day_selection_universe"]["sha256"]
        assert sources.session_pit_identity(session, request_identity=identity, manifest_identity=None) == identity
        for changed in ({**identity, "universe_as_of": CALENDAR[1]},
                        {**identity, "universe_codes": ["600001.SH"]}, {**identity, "snapshot_digest": "c" * 64}):
            with pytest.raises(sources.SourcePlanError, match="DECLARED_PIT_IDENTITY_MISMATCH"):
                sources.session_pit_identity(session, request_identity=changed, manifest_identity=None)
        session.source_plan_route = sources.resolve_day_source(plan, _clocks(plan["days"][1]))
        assert sources.session_pit_identity(session, request_identity=None, manifest_identity=None)["universe_as_of"] == CALENDAR[1]
    finally:
        session.close()


def test_canonical_database_identity_is_frozen_and_retry_cannot_read_new_pointer(monkeypatch):
    from backend.services.canonical_equity_pit import (
        CanonicalPitAuthorityResolver, PitConsumerBinding, PitAuthorityStatus, CANONICAL_PIT_AUTHORITY_ID,
        canonical_rule_parameters_digest,
    )
    policy = _canonical_policy()
    binding = PitConsumerBinding(authority_id=CANONICAL_PIT_AUTHORITY_ID, authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
        universe_key=policy["universe_key"], rule_version=policy["rule_version"],
        rule_parameters_digest=canonical_rule_parameters_digest(), activation_generation=1,
        activation_envelope_digest="a" * 64, expected_source_commit="commit", state_source_digest="b" * 64,
        coverage_start=date(2026, 1, 19), coverage_end=date(2026, 1, 22))
    calls = []
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding", lambda _: calls.append(1) or binding)
    plan = sources.build_source_plan(mode="DATABASE_ONLY", consumer_scope="ADVISORY", run_id="private",
        days=_plan()["days"], trading_calendar=CALENDAR, native_policy=policy)
    assert calls == [1]
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding", lambda _: pytest.fail("retry queried live pointer"))
    restored = sources.validate_source_plan(plan, consumer_scope="ADVISORY", run_id="private")
    route = sources.resolve_day_source(restored, _clocks(plan["days"][-1]))
    assert sources.session_pit_identity(SimpleNamespace(source_plan_route=route), request_identity=None,
                                        manifest_identity=None)["activation_generation"] == 1
    bad = deepcopy(plan)
    bad["runtime_pit_identity"]["coverage_end"] = CALENDAR[0]
    bad["plan_hash"] = sources.digest({k: v for k, v in bad.items() if k != "plan_hash"})
    with pytest.raises(ValueError, match="coverage"):
        sources.validate_source_plan(bad, consumer_scope="ADVISORY", run_id="private")


def test_shared_cli_switches_whole_days_and_rechecks_database_checkpoint(tmp_path, monkeypatch):
    import sys
    from backend.services.advisory_historical_range.inference_inputs import InferenceInputSession
    from backend.tests.advisory_historical_range.test_strategy_package_live_inference import _batch_args
    from scripts import strategy_package_live_inference as runner

    plan = _plan("FILE_THEN_DATABASE", _snapshot(tmp_path / "data"), run="test-owned-run")
    path = tmp_path / "source-plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    output = _batch_args(tmp_path, monkeypatch, [
        {k: r[k] for k in ("decision_date", "data_cutoff", "target_trade_date")} for r in plan["days"]])
    sys.argv[sys.argv.index("--input-revision") + 1] = plan["plan_hash"]
    sys.argv.extend(["--source-plan-file", str(path)])
    calls, sessions = [], []
    monkeypatch.setattr(runner, "InferenceEngine", lambda **kwargs: SimpleNamespace(**kwargs))
    def session(plan, clocks, **kwargs):
        route = sources.resolve_day_source(plan, clocks)
        value = InferenceInputSession(consumer_scope=kwargs["consumer_scope"], run_id=kwargs["run_id"],
                                      source_identity=sources.digest(route))
        if route["source"] == "DATASET_FILES":
            value.file_dataset = SimpleNamespace(binding={"files": {}}, assert_unchanged=lambda: None, close=lambda: None)
        sessions.append(value)
        return value
    monkeypatch.setattr(sources, "create_input_session", session)
    changed = []
    def run(engine, workspace, trade_date, cutoff_date, readonly, output_path, **kwargs):
        route = engine.input_session.source_plan_route
        calls.append((trade_date, route["source"]))
        result = _payload(route)
        if route["source"] == "DATASET_FILES":
            result["input_context"]["frozen_file_binding_hash"] = route["file_binding_hash"]
        result["metadata"]["effective_trade_date"] = cutoff_date
        if changed:
            result["source_read_receipts"][1]["content_hash"] = "0" * 64
        return result
    monkeypatch.setattr(runner, "_run_request", run)
    assert runner.main() == 0
    assert calls == [(r["decision_date"], r["source"]) for r in plan["routes"]]
    assert len(sessions) == 2  # One frozen file cache, then one fresh DB window.
    index = json.loads(output.read_bytes())
    originals = {r["output_path"]: Path(r["output_path"]).read_bytes() for r in index["results"]}
    sys.argv.append("--resume")
    assert runner.main() == 0 and len(calls) == 4  # Only DB recomputed; no republish.
    assert calls[-1] == (CALENDAR[-1], "DATABASE")
    changed.append(True)
    with pytest.raises(sources.SourcePlanError, match="SOURCE_INPUT_DRIFT"):
        runner.main()
    failed = json.loads(output.read_bytes())
    assert failed["status"] == "FAILED" and failed["source_gap_receipt"]["reason_code"] == "SOURCE_INPUT_DRIFT"
    assert all(Path(p).read_bytes() == data for p, data in originals.items())


@pytest.mark.parametrize("mode", ["DATABASE_ONLY", "FILE_THEN_DATABASE"])
def test_planned_multi_model_reuses_input_session_within_database_day_only(tmp_path, monkeypatch, mode):
    import sys
    from backend.services.advisory_historical_range.inference_inputs import InferenceInputSession
    from backend.tests.advisory_historical_range.test_strategy_package_live_inference import _batch_args
    from scripts import strategy_package_live_inference as runner

    snapshot = _snapshot(tmp_path / "data") if mode == "FILE_THEN_DATABASE" else None
    plan = _plan(mode, snapshot, run="test-owned-run")
    path = tmp_path / "source-plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    requests = [{**{k: row[k] for k in ("decision_date", "data_cutoff", "target_trade_date")},
                 "request_id": leg} for row in plan["days"] for leg in ("leg-A", "leg-B")]
    output = _batch_args(tmp_path, monkeypatch, requests)
    sys.argv[sys.argv.index("--input-revision") + 1] = plan["plan_hash"]
    sys.argv.extend(["--source-plan-file", str(path)])
    sessions, observed = [], []
    monkeypatch.setattr(runner, "InferenceEngine", lambda **kwargs: SimpleNamespace(**kwargs))

    def session(plan, clocks, **kwargs):
        route = sources.resolve_day_source(plan, clocks)
        value = InferenceInputSession(consumer_scope=kwargs["consumer_scope"], run_id=kwargs["run_id"],
                                      source_identity=sources.digest(route))
        if route["source"] == "DATASET_FILES":
            value.file_dataset = SimpleNamespace(close=lambda: None)
        sessions.append(value)
        return value

    def run(engine, workspace, trade_date, cutoff_date, readonly, output_path, **kwargs):
        route = engine.input_session.source_plan_route
        observed.append(engine.input_session)
        result = _payload(route)
        if route["source"] == "DATASET_FILES":
            result["input_context"]["frozen_file_binding_hash"] = route["file_binding_hash"]
        result["metadata"]["effective_trade_date"] = cutoff_date
        return result

    monkeypatch.setattr(sources, "create_input_session", session)
    monkeypatch.setattr(runner, "_run_request", run)
    assert runner.main() == 0
    index = json.loads(output.read_bytes())
    assert len(index["results"]) == len(requests) == 6
    assert len({row["output_path"] for row in index["results"]}) == 6
    for ordinal, route in enumerate(plan["routes"]):
        assert observed[2 * ordinal] is observed[2 * ordinal + 1]
        if ordinal and route["source"] == "DATABASE":
            assert observed[2 * ordinal] is not observed[2 * ordinal - 1]
    assert len(sessions) == (3 if mode == "DATABASE_ONLY" else 2)
