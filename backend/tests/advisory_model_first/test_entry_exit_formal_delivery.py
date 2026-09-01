from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    build_n2_action_request,
)
from backend.services.advisory_model_first.entry_exit_formal_pipeline import (
    ENTRY_EXPERIMENT_ID,
    EXIT_EXPERIMENT_ID,
    _find_existing_bundle,
    _publish_bundle,
    _read_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_entry_exit_formal_contracts import (
    _request_values,
)
from scripts.advisory_entry_exit_formal_audit import main


def _request(tmp_path: Path):
    values = _request_values()
    values["output_root"] = str(tmp_path)
    values["registry_path"] = str(tmp_path / "registry.jsonl")
    values["route_path"] = str(tmp_path / "current_route.md")
    return build_n2_action_request(**values)


def _frame(name: str) -> pd.DataFrame:
    return pd.DataFrame({"artifact": [name], "value": [1.0]})


def _publish(tmp_path: Path) -> tuple[Path, object]:
    request = _request(tmp_path)
    entry_summary = {
        "arms": {
            arm.arm_id: {
                "decision_day_count": 60,
                "available_day_count": 60,
            }
            for arm in request.entry_arms
        },
        "deployable": False,
    }
    exit_summary = {
        "episode_count": 100,
        "evaluable_episode_count": 99,
        "deployable": False,
    }
    path = _publish_bundle(
        request=request,
        sources={"ref_payload": {"n1": {"sha256": "a" * 64}}},
        entry={
            "decisions": _frame("entry-decisions"),
            "labels": _frame("entry-labels"),
            "daily": _frame("entry-daily"),
            "summary": entry_summary,
            "support": {"FIXED_GAP_3": {"evidence_class": "EXPLORATORY_ONLY"}},
            "gap_parity": {"row_count": 1200, "max_abs_error": 0.0},
        },
        exit_result={
            "labels": _frame("exit-labels"),
            "decisions": _frame("exit-decisions"),
            "episode_best": _frame("exit-best"),
            "summary": exit_summary,
            "support": {"evidence_class": "EXPLORATORY_ONLY"},
            "baseline_parity": {"row_count": 100, "status": "EXACT"},
        },
        elapsed_seconds=1.0,
    )
    return path, request


def test_bundle_is_immutable_zero_trial_and_discoverable_for_exact_retry(tmp_path: Path) -> None:
    path, request = _publish(tmp_path)
    loaded = _read_bundle(path)

    assert path.name == loaded["manifest"]["bundle_id"]
    assert _find_existing_bundle(request) == path
    assert {item.experiment_id for item in loaded["records"]} == {
        ENTRY_EXPERIMENT_ID,
        EXIT_EXPERIMENT_ID,
    }
    assert all(item.planned_trial_count == 0 for item in loaded["records"])
    assert all(item.decision_use.value == "NAVIGATION_ONLY" for item in loaded["records"])
    assert loaded["manifest"]["sealed_holdout_accessed"] is False
    assert loaded["manifest"]["deployable"] is False


def test_bundle_member_mutation_fails_closed(tmp_path: Path) -> None:
    path, _ = _publish(tmp_path)
    summary_path = path / "entry_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["deployable"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as captured:
        _read_bundle(path)
    assert captured.value.reason_code == "ADVISORY_N2_ACTION_BUNDLE_INVALID"


def test_inspect_cli_emits_one_typed_json_document(tmp_path: Path, capsys) -> None:
    path, _ = _publish(tmp_path)

    assert main(["inspect", "--bundle", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["status"] == "VALID"
    assert result["bundle_id"] == path.name
    assert result["sealed_holdout_accessed"] is False
    assert result["deployable"] is False


def test_cli_argument_failure_is_typed_and_nonzero(capsys) -> None:
    assert main(["run", "--request", "missing.json"]) == 1
    result = json.loads(capsys.readouterr().out)

    assert result["status"] == "failed"
    assert result["reason_code"] == "ADVISORY_N2_ACTION_REQUEST_INVALID"


def test_delivery_refuses_to_advance_beyond_the_frozen_n2_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.services.advisory_model_first import entry_exit_formal_pipeline as pipeline

    path, request = _publish(tmp_path)
    monkeypatch.setattr(
        pipeline,
        "generate_current_route",
        lambda **_kwargs: {"next_task": "N3_SINGLE_MAINLINE_SELECTION"},
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        pipeline._deliver_bundle(request=request, bundle_path=path)
    assert captured.value.reason_code == "ADVISORY_RESEARCH_ROUTE_INCONSISTENT"
