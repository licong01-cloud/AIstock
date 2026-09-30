from __future__ import annotations

from datetime import date, timedelta
import hashlib

import pytest

from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.rotation_l2 import (
    ACCEPTANCE_SCHEMA,
    INPUT_SCHEMA,
    MODEL_HASH,
    RotationL2Error,
    _score_and_states,
    build_consumer_artifact,
    close_processes,
    run_process,
)


def _weekdays(start: date, end: date) -> list[date]:
    values: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            values.append(cursor)
        cursor += timedelta(days=1)
    return values


@pytest.fixture(scope="module")
def input_bundle() -> dict:
    calendar = _weekdays(date(2024, 8, 1), date(2026, 8, 31))
    codes = [f"801{index:03d}.SI" for index in range(131)]
    start = calendar.index(date(2024, 9, 19)) - 25
    end = calendar.index(date(2026, 3, 31))
    daily = []
    for day_index, day in enumerate(calendar[start:end]):
        for code_index, code in enumerate(codes):
            amount = 1000.0 + code_index
            daily.append(
                {
                    "trade_date": day.isoformat(),
                    "sector_code": code,
                    "structural_eligible": True,
                    "eligible": True,
                    "reason_code": None,
                    "expected_contributors": 2,
                    "valid_contributors": 2,
                    "coverage": 1.0,
                    "net_mf_amount_cny": amount * (code_index + 1) * (day_index + 1) / 100000.0,
                    "amount_cny": amount,
                    "maximum_member_amount_share": 0.6,
                }
            )
    returns = []
    for day in calendar:
        if date(2024, 9, 19) <= day <= date(2026, 3, 31):
            for code_index, code in enumerate(codes):
                returns.append(
                    {
                        "trade_date": day.isoformat(),
                        "sector_code": code,
                        "quote_available": True,
                        "pct_change": (code_index - 65) / 10000.0,
                    }
                )
    source_git_commit = "a" * 40
    body = {
        "schema_version": INPUT_SCHEMA,
        "identity": {
            "source_commit": hashlib.sha256(source_git_commit.encode("ascii")).hexdigest(),
            "source_git_commit": source_git_commit,
            "profile_sha256": "2" * 64,
            "manifest_sha256": "3" * 64,
            "mapping_hash": "4" * 64,
            "quote_authority_hash": "5" * 64,
            "calendar_hash": "6" * 64,
            "source_file_hashes": {"test_input": "7" * 64},
            "release_id": "qe_hmm_full_v2_20260831",
            "cutoff": "2026-08-31",
            "sector_display_name_authority": "canonical_sw_l2_code_only",
        },
        "catalog": [{"sector_code": code, "sector_name": code} for code in codes],
        "calendar": [day.isoformat() for day in calendar],
        "daily_aggregates": daily,
        "sector_returns": returns,
        "benchmark_close": [
            {"trade_date": day.isoformat(), "close": 1000.0}
            for day in calendar
            if date(2024, 9, 19) <= day <= date(2026, 3, 31)
        ],
    }
    body["input_hash"] = canonical_sha256(body)
    return body


def test_score_uses_average_rank_and_neutralizes_boundary_ties() -> None:
    scores, states = _score_and_states({"a": -1.0, "b": 0.0, "c": 0.0, "d": 1.0})

    assert scores == {"a": -0.5, "b": 0.0, "c": 0.0, "d": 0.5}
    assert states == {"a": "fading", "b": "neutral", "c": "neutral", "d": "trending"}


def test_two_fresh_processes_close_without_fit_or_tail(input_bundle: dict) -> None:
    first = run_process(input_bundle, process_index=1)
    second = run_process(input_bundle, process_index=2)
    acceptance = close_processes(first, second, input_bundle=input_bundle)
    consumer = build_consumer_artifact(acceptance)

    assert acceptance["schema_version"] == ACCEPTANCE_SCHEMA
    assert acceptance["model_hash"] == MODEL_HASH
    assert acceptance["planned_fits"] == acceptance["completed_fits"] == 0
    assert acceptance["tail_accessed"] is False
    assert acceptance["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
    assert acceptance["rotation_l2_capability_status"] == "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
    assert consumer["schema_version"] == "hmm_rotation_l2_consumer_v1"
    assert len(consumer["rows"]) == len(acceptance["predictions"])


def test_close_rejects_a_tampered_child(input_bundle: dict) -> None:
    first = run_process(input_bundle, process_index=1)
    second = run_process(input_bundle, process_index=2)
    second["reproducibility_payload"]["tail_accessed"] = True

    with pytest.raises(RotationL2Error, match="receipt is invalid"):
        close_processes(first, second, input_bundle=input_bundle)


def test_bundle_hash_drift_fails_closed(input_bundle: dict) -> None:
    changed = {**input_bundle, "calendar": list(input_bundle["calendar"])}
    changed["calendar"][0] = "2024-07-31"

    with pytest.raises(RotationL2Error, match="canonical hash differs"):
        run_process(changed, process_index=1)
