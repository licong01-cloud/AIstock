import json
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import payload
from backend.services.position_timing.intraday.inputs import file_sha256
from backend.services.position_timing.intraday.replay import implementation_identity, inspect, prepare, run
from backend.services.position_timing.intraday.store import IntradayError, IntradayStore
from test_intraday_core import at, case


def test_source_identity_survives_git_line_ending_conversion(monkeypatch):
    expected, read_bytes = implementation_identity(), Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda path: read_bytes(path).replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert implementation_identity() == expected


def replay_spec(tmp_path, *, cancel=False, missing=False):
    plan, market = case(source="FROZEN_REPLAY", prices=("10",) * 10)
    events = [{"available_at": at("09:30:00"), "plan": plan}]
    if cancel:
        events.append({"available_at": at("09:32:30"), "plan": plan.model_copy(update={"revision": 2, "lifecycle": "CANCELLED"})})
    if missing:
        market = market.model_copy(update={"bars": (*market.bars[:5], *market.bars[6:])})
    def freeze(name, value):
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(payload(value)), encoding="utf-8")
        return {"path": str(path), "sha256": file_sha256(path)}
    spec = {"candidate_manifest": freeze("candidate", {"schema_version": "functional_fixture_only"}),
            "plans": freeze("plans", events), "markets": freeze("markets", [market]),
            "start_as_of": at("09:31:10").isoformat(), "end_as_of": at("09:40:10").isoformat()}
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    return path


def test_replay_four_delays_hash_closed_inspection_and_exact_retry(tmp_path):
    prepared = prepare(replay_spec(tmp_path), tmp_path / "timing")
    first = run(prepared["request"])
    inspected = inspect(first["bundle"])
    retry = run(prepared["request"])
    assert first["manifest_sha256"] == inspected["manifest_sha256"] == retry["manifest_sha256"]
    assert retry["status"] == "ALREADY_MATERIALIZED"
    receipt = inspected["receipt"]
    assert receipt["source_preflight_passed"] and receipt["decision_replay_complete"]
    assert receipt["economic_evaluation"] == "NOT_RUN_LOCALSIM_ADAPTER_PENDING"
    assert receipt["outcomes_read"] is receipt["network_accessed"] is receipt["database_read"] is False
    store = IntradayStore(tmp_path / "timing")
    from pathlib import Path
    intents = store.read(Path(first["bundle"]) / "delay_intents.json")
    assert [row["delay_seconds"] for row in intents] == [0, 60, 180, 300]
    assert all(row["status"] == "EXECUTION_SLOT_ELIGIBLE" for row in intents)
    assert intents[0]["next_execution_slot"] == at("09:32:00").isoformat()
    assert intents[1]["next_execution_slot"] == at("09:33:00").isoformat()
    assert intents[3]["actual_wait_seconds"] == 350
    assert all(row["execution_confirmed"] is False for row in intents)


def test_cancel_event_invalidates_delays_without_retroactive_early_cancellation(tmp_path):
    from pathlib import Path
    prepared = prepare(replay_spec(tmp_path, cancel=True), tmp_path / "timing")
    result = run(prepared["request"])
    intents = IntradayStore(tmp_path / "timing").read(Path(result["bundle"]) / "delay_intents.json")
    assert intents[0]["status"] == "EXECUTION_SLOT_ELIGIBLE"
    assert all(row["status"] == "CANCELLED_PLAN_CHANGED" for row in intents[1:])


def test_missing_bar_and_mutated_source_fail_before_replay(tmp_path):
    with pytest.raises(IntradayError, match="COVERAGE_INCOMPLETE"):
        prepare(replay_spec(tmp_path, missing=True), tmp_path / "timing")
    path = replay_spec(tmp_path)
    prepared = prepare(path, tmp_path / "timing")
    (tmp_path / "markets.json").write_text("[]", encoding="utf-8")
    with pytest.raises(IntradayError, match="SOURCE_IDENTITY_MISMATCH"):
        run(prepared["request"])
