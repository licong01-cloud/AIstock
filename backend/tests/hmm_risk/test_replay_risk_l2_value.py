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


@pytest.mark.parametrize("drift", [None, "contract", "contract_type", "population", "source", "old_arm", "final_write"])
def test_persistence_dispatch_parent_authority_and_durable_finalization(tmp_path, monkeypatch, drift):
    from backend.services.hmm_risk import risk_l2_value_persistence as new
    from backend.services.hmm_risk.formal_state_model import receipt
    from backend.services.hmm_risk.risk_l2_value_replay import replay
    from backend.tests.hmm_risk.test_risk_l2_value_replay import panel

    c, d, s, r = panel()
    baseline = replay(c, d, s, r)
    request = {k + "_path": str(tmp_path / (k + ".json")) for k in new.SOURCES}
    result = new.compare(c, d, s, r, baseline, [])
    result.update(
        request_sha256="b" * 64,
        source_pins=new.APPROVED_PINS,
        source_paths={k: request[k + "_path"] for k in new.SOURCES},
        zero_compute_poison_active=True,
    )
    if drift == "contract":
        result["contract"] = {**new.CONTRACT, "confirmation_days": 3}
    elif drift == "contract_type":
        result["contract"] = {**new.CONTRACT, "initial_cash_latch": False}
    elif drift == "population":
        result["population_sha256"] = "a" * 64
    elif drift == "source":
        result["source_pins"] = {**new.APPROVED_PINS, "model_hash": "a" * 64}
    elif drift == "old_arm":
        from copy import deepcopy

        result = deepcopy(result)
        result["daily"][0]["arms"]["R"]["gross_return"] += 0.1
    child = receipt({k: v for k, v in result.items() if k != "receipt_sha256"})
    monkeypatch.setattr(new, "load_inputs", lambda *a: (request, c, d, s, r, baseline, []))
    monkeypatch.setattr(cli, "source_head", lambda: "a" * 40)
    real_write = cli.write_once
    children = []

    def run(command, **kwargs):
        assert command[-2:] == ["--contract-version", new.VERSION]
        children.append(command)
        real_write(Path(command[command.index("--output") + 1]), child)

    monkeypatch.setattr(cli.subprocess, "run", run)
    if drift == "final_write":

        def write(path, body):
            if path.name == "acceptance.json":
                raise OSError("finalization failed")
            real_write(path, body)

        monkeypatch.setattr(cli, "write_once", write)
    output = tmp_path / "new-run"
    code = cli.main(
        [
            "run",
            "--contract-version",
            new.VERSION,
            "--request",
            str(tmp_path / "request.json"),
            "--request-sha256",
            "b" * 64,
            "--output",
            str(output),
        ]
    )
    if drift is None:
        assert code == 0 and len(children) == 2
        acceptance = read_json(output / "acceptance.json")
        assert acceptance["schema_version"] == new.VERSION + "_acceptance"
        assert acceptance["fresh_process_bitwise_equal"] is True
        assert acceptance["result"]["action_sha256"] == child["action_sha256"]
        assert acceptance["completed_fits"] == 0
    else:
        assert code == 1 and not (output / "acceptance.json").exists()
        failure = read_json(Path(str(output) + ".failure.json"))
        assert failure["schema_version"] == new.VERSION + "_failure"
        assert failure["execution_status"] == "FAILED"
        assert failure["reason_code"] == "hmm_risk_l2_value_" + (
            "execution_failed" if drift == "final_write" else "identity_mismatch"
        )


def test_persistence_child_fresh_process_missing_inputs_is_typed_failure_not_fallback(tmp_path):
    import subprocess
    import sys
    from backend.services.hmm_risk import risk_l2_value_persistence as new

    output = tmp_path / "child.json"
    process = subprocess.run(
        [
            sys.executable,
            str(Path(cli.__file__).resolve()),
            "child",
            "--contract-version",
            new.VERSION,
            "--request",
            str(tmp_path / "absent.json"),
            "--request-sha256",
            "b" * 64,
            "--executor-commit",
            cli.source_head(),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 1
    failure = read_json(Path(str(output) + ".failure.json"))
    assert failure["schema_version"] == new.VERSION + "_failure"
    assert failure["new_fits"] == 0 and failure["database_access"] is False
    assert not output.exists()
