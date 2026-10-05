from pathlib import Path

import pytest

from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_executor import read_json
from backend.services.hmm_risk.risk_l2_value_replay import ReplayError
from scripts.hmm_risk import replay_risk_l2_value as cli


def test_child_failure_has_durable_typed_receipt_without_false_success(tmp_path, monkeypatch):
    output = tmp_path / "new-result.json"
    monkeypatch.setattr(cli, "source_head", lambda: "a" * 40)

    def fail(*args):
        raise ReplayError("identity_mismatch", "frozen input differs")

    monkeypatch.setattr(cli, "execute", fail)
    assert (
        cli.main(
            [
                "child",
                "--request",
                str(tmp_path / "request.json"),
                "--request-sha256",
                "b" * 64,
                "--executor-commit",
                "a" * 40,
                "--output",
                str(output),
            ]
        )
        == 1
    )
    assert not output.exists()
    failure = read_json(output.with_name(output.name + ".failure.json"))
    verify_receipt(failure)
    assert failure["execution_status"] == "FAILED"
    assert failure["reason_code"] == "hmm_risk_l2_value_identity_mismatch"
    assert failure["new_fits"] == 0


def test_output_collision_never_overwrites_or_mutates_old_assets(tmp_path):
    output = tmp_path / "protected.json"
    output.write_bytes(b"original")
    assert (
        cli.main(["run", "--request", str(tmp_path / "missing"), "--request-sha256", "b" * 64, "--output", str(output)])
        == 1
    )
    assert output.read_bytes() == b"original"
    assert not output.with_name(output.name + ".failure.json").exists()


@pytest.mark.parametrize("drift", ["repeat", "self_rehashed_fit_count"])
def test_parent_final_readback_or_child_mismatch_stops_without_acceptance(tmp_path, monkeypatch, drift):
    monkeypatch.setattr(cli, "source_head", lambda: "a" * 40)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: None)
    from backend.services.hmm_risk.formal_state_model import receipt

    from backend.services.hmm_risk.risk_l2_value_replay import APPROVED_PINS, VERSION

    body = {
        "schema_version": VERSION + "_result",
        "request_sha256": "b" * 64,
        "source_pins": APPROVED_PINS,
        "planned_return_dates": 423,
        "sector_count": 131,
        "daily": [{}] * 423,
        "status": "REFERENCE_RISK_REDUCTION_OBSERVED",
        "new_fits": 0,
        "new_filter_calls": 0,
        "new_predict_calls": 0,
        "database_access": False,
        "tail_accessed": False,
        "dataset_write": False,
        "runtime_action": False,
        "zero_compute_poison_active": True,
    }
    results = iter(
        [
            receipt({**body, "x": 1 if drift == "repeat" else 2, "new_fits": 0 if drift == "repeat" else 1}),
            receipt({**body, "x": 2}),
        ]
    )
    monkeypatch.setattr(cli, "read_json", lambda path: next(results))
    output = tmp_path / "run"
    assert (
        cli.main(["run", "--request", str(tmp_path / "missing"), "--request-sha256", "b" * 64, "--output", str(output)])
        == 1
    )
    assert not (output / "acceptance.json").exists()
    expected = "repeat_mismatch" if drift == "repeat" else "identity_mismatch"
    assert read_json(Path(str(output) + ".failure.json"))["reason_code"] == "hmm_risk_l2_value_" + expected
