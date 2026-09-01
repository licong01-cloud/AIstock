from datetime import UTC, date, datetime

import pytest

from scripts.stale_simulation_run_terminalization_core import (
    RETRY_CONTROL_KEY,
    TERMINALIZATION_CARRIER_KEY,
    FailedRunTerminalizationRequest,
    TerminalizationSafetyError,
    build_terminalization_plan_from_rows,
    canonical_sha256,
    classify_historical_failed_run,
    terminalized_payload,
)
from scripts import terminalize_stale_simulation_runs as cli


PACKAGES = ("pkg_local", "pkg_mini")
CUTOFF = date(2026, 8, 22)
APPLIED_AT = datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC)


def _request() -> FailedRunTerminalizationRequest:
    return FailedRunTerminalizationRequest.build(PACKAGES, CUTOFF)


def _row(*, run_id: str, backend: str, package_id: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "run_id": run_id,
        "trade_date": date(2026, 8, 21),
        "strategy_id": f"strategy_{run_id}",
        "broker_backend": backend,
        "package_id": package_id,
        "release_id": f"release_{run_id}",
        "binding_id": f"binding_{run_id}",
        "execution_plan_id": None,
        "status": "FAILED_RETRYABLE",
        "run_payload_json": payload,
        "updated_at": datetime(2026, 8, 23, tzinfo=UTC),
    }


def test_request_requires_two_explicit_retained_packages() -> None:
    with pytest.raises(TerminalizationSafetyError, match="at least two"):
        FailedRunTerminalizationRequest.build(["pkg_local"], CUTOFF)
    assert FailedRunTerminalizationRequest.build(["pkg_mini", "pkg_local", "pkg_local"], CUTOFF).package_ids == PACKAGES


def test_only_explicit_zero_side_effect_is_cancelled() -> None:
    result = classify_historical_failed_run({"broker_called": False, "submitted_intents": 0, "failed_intents": 40})
    assert result == {
        "terminal_status": "CANCELLED",
        "side_effect_state": "NONE_PROVEN",
        "reason_code": "SIMULATION_HISTORICAL_RETRY_WINDOW_EXPIRED_NO_SIDE_EFFECT",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"broker_called": False},
        {"broker_called": False, "submitted_intents": "0"},
        {"broker_called": True, "submitted_intents": 0},
        {"broker_called": False, "submitted_intents": 1},
        {"broker_called": False, "submitted_intents": 0, "qmt_batch_id": "batch-1"},
        {"broker_called": False, "submitted_intents": 0, "local_sim_persistence": {}},
        {"broker_called": False, "submitted_intents": 0, "broker_side_effect_state": "UNKNOWN"},
    ],
)
def test_missing_present_or_ambiguous_side_effect_evidence_fails_terminal(payload: dict[str, object]) -> None:
    result = classify_historical_failed_run(payload)
    assert result["terminal_status"] == "FAILED_TERMINAL"
    assert result["side_effect_state"] == "PRESENT_OR_UNKNOWN_PRESERVED"


def test_plan_is_deterministic_and_keeps_exact_classification_counts() -> None:
    rows = [
        _row(
            run_id="run-local",
            backend="local_sim",
            package_id="pkg_local",
            payload={"broker_called": False, "submitted_intents": 0},
        ),
        _row(
            run_id="run-mini",
            backend="minqmt_sim",
            package_id="pkg_mini",
            payload={"broker_called": False, "submitted_intents": 0, "qmt_batch_id": "batch-1"},
        ),
    ]
    left = build_terminalization_plan_from_rows(
        database_identity={"database": "aistock_dev"}, request=_request(), rows=rows
    )
    right = build_terminalization_plan_from_rows(
        database_identity={"database": "aistock_dev"}, request=_request(), rows=list(reversed(rows))
    )
    assert left == right
    assert left["candidate_count"] == 2
    assert left["terminal_status_counts"] == {"CANCELLED": 1, "FAILED_TERMINAL": 1}
    assert left["broker_backend_counts"] == {"local_sim": 1, "minqmt_sim": 1}
    assert left["plan_sha256"] == canonical_sha256({k: v for k, v in left.items() if k != "plan_sha256"})
    assert left["mutation_scope"]["runs_deleted"] is False
    assert left["mutation_scope"]["orders_mutated"] is False


def test_plan_rejects_non_object_payload_and_existing_carrier() -> None:
    malformed = _row(run_id="bad", backend="local_sim", package_id="pkg_local", payload={})
    malformed["run_payload_json"] = []
    with pytest.raises(TerminalizationSafetyError, match="not an object"):
        build_terminalization_plan_from_rows(
            database_identity={"database": "aistock_dev"}, request=_request(), rows=[malformed]
        )
    carrier = _row(
        run_id="carrier",
        backend="local_sim",
        package_id="pkg_local",
        payload={TERMINALIZATION_CARRIER_KEY: {}},
    )
    with pytest.raises(TerminalizationSafetyError, match="already has terminalization carrier"):
        build_terminalization_plan_from_rows(
            database_identity={"database": "aistock_dev"}, request=_request(), rows=[carrier]
        )


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"package_id": "pkg_foreign"}, "outside retained set"),
        ({"trade_date": CUTOFF}, "not before the exclusive cutoff"),
        ({"broker_backend": "live"}, "outside SIM scope"),
    ],
)
def test_plan_rejects_candidates_outside_exact_boundary(patch: dict[str, object], message: str) -> None:
    row = _row(
        run_id="boundary",
        backend="local_sim",
        package_id="pkg_local",
        payload={"broker_called": False, "submitted_intents": 0},
    )
    row.update(patch)
    with pytest.raises(TerminalizationSafetyError, match=message):
        build_terminalization_plan_from_rows(
            database_identity={"database": "aistock_dev"}, request=_request(), rows=[row]
        )


def test_terminalized_payload_preserves_evidence_and_removes_only_retry_control() -> None:
    source = {
        "last_stage": "FAILED_RETRYABLE",
        "broker_called": False,
        "submitted_intents": 0,
        "submit_failure": {"reason_code": "DATA_UNAVAILABLE"},
        RETRY_CONTROL_KEY: {"entries": {"BINDING_FAILED_RETRYABLE": {"attempt": 9}}},
    }
    row = _row(run_id="run-local", backend="local_sim", package_id="pkg_local", payload=source)
    plan = build_terminalization_plan_from_rows(
        database_identity={"database": "aistock_dev"}, request=_request(), rows=[row]
    )
    candidate = plan["candidates"][0]
    updated = terminalized_payload(
        candidate=candidate,
        source_payload=source,
        plan_sha256=plan["plan_sha256"],
        applied_at=APPLIED_AT,
    )
    assert updated["last_stage"] == "CANCELLED"
    assert updated["submit_failure"] == source["submit_failure"]
    assert RETRY_CONTROL_KEY not in updated
    carrier = updated[TERMINALIZATION_CARRIER_KEY]
    assert carrier["source_payload_sha256"] == canonical_sha256(source)
    assert carrier["retry_control_removed"] is True
    assert carrier["retry_control_sha256"] == canonical_sha256(source[RETRY_CONTROL_KEY])
    assert carrier["orders_mutated"] is False
    assert carrier["trades_mutated"] is False
    assert carrier["historical_evidence_deleted"] is False


def test_terminalized_payload_rejects_payload_drift() -> None:
    source = {"broker_called": False, "submitted_intents": 0}
    row = _row(run_id="run-local", backend="local_sim", package_id="pkg_local", payload=source)
    plan = build_terminalization_plan_from_rows(
        database_identity={"database": "aistock_dev"}, request=_request(), rows=[row]
    )
    with pytest.raises(TerminalizationSafetyError, match="source payload drift"):
        terminalized_payload(
            candidate=plan["candidates"][0],
            source_payload={**source, "new": "drift"},
            plan_sha256=plan["plan_sha256"],
            applied_at=APPLIED_AT,
        )


def test_production_apply_requires_explicit_bug_authorization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "terminalize_stale_simulation_runs.py",
            "--target",
            "production",
            "--package-id",
            "pkg_local",
            "--package-id",
            "pkg_mini",
            "--cutoff",
            "2026-08-22",
            "--apply",
        ],
    )
    with pytest.raises(TerminalizationSafetyError, match="--confirm-production --authorization BUG-1165"):
        cli.main()


def test_validate_rollback_is_dev_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "terminalize_stale_simulation_runs.py",
            "--target",
            "production",
            "--package-id",
            "pkg_local",
            "--package-id",
            "pkg_mini",
            "--cutoff",
            "2026-08-22",
            "--validate-rollback",
        ],
    )
    with pytest.raises(TerminalizationSafetyError, match="only permitted for DEV"):
        cli.main()
