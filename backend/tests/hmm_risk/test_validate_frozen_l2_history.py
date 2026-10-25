"""Parent seal/outcome ordering and failure write safety, with synthetic children."""

from argparse import Namespace
from pathlib import Path
import sys

import pytest

from backend.services.hmm_risk import frozen_l2_history as h
from backend.services.hmm_risk.formal_state_model import receipt
from scripts.hmm_risk import validate_frozen_l2_history as cli


@pytest.mark.parametrize("fault", [None, "child_failure", "seal_drift", "ready_drift", "result_drift", "numeric_drift"])
def test_parent_releases_outcomes_only_after_two_bound_prediction_readbacks(tmp_path, monkeypatch, fault):
    request = receipt({"kind": "P1", "contract": h.CONTRACT})
    bundle = receipt(
        {
            "request_sha256": request["receipt_sha256"],
            "source_commit": "a" * 40,
            "contract": h.CONTRACT,
            "numeric_environment": {"synthetic": True},
        }
    )
    features = tmp_path / "features.json"
    cli.write_once(features, bundle)
    output = tmp_path / "run"
    args = Namespace(
        features=features, feature_sha256=bundle["receipt_sha256"], output=output, request=tmp_path / "request.json"
    )
    monkeypatch.setattr(cli, "_head", lambda: "a" * 40)
    monkeypatch.setattr(cli.subprocess, "check_output", lambda *a, **kw: "")
    children = []
    calls = []

    class Child:
        def __init__(self, command, **kwargs):
            self.path = Path(command[command.index("--output") + 1])
            self.failed = fault == "child_failure"
            if not self.failed:
                sealed = receipt(
                    {"input_sha256": bundle["receipt_sha256"], "value": len(children) if fault == "seal_drift" else 1}
                )
                cli.write_once(self.path.with_suffix(".sealed.json"), sealed)
                cli.write_once(
                    self.path.with_suffix(".ready.json"),
                    {"sealed_sha256": "0" * 64 if fault == "ready_drift" else sealed["receipt_sha256"]},
                )
            children.append(self)

        def poll(self):
            return 1 if self.failed else None

        def wait(self):
            if (output / "parent.failure.json").exists():
                return 1
            if self.path.exists():
                return 0
            facts = cli.read_json(output / "outcomes.json")
            assert cli.read_json(output / "outcomes.ready.json") == {"outcome_sha256": facts["receipt_sha256"]}
            cli.write_once(
                self.path,
                receipt(
                    {
                        "schema_version": h.VERSION + "_repeat",
                        "kind": "P1",
                        "executor_commit": "a" * 40,
                        "fits": 0,
                        "sealed_sha256": facts["sealed_prediction_sha256"],
                        "result": {
                            "status": "SYNTHETIC_ONLY",
                            "prediction_sha256": facts["sealed_prediction_sha256"],
                            "outcome_sha256": "0" * 64 if fault == "result_drift" else facts["receipt_sha256"],
                        },
                        "numeric_environment": {"synthetic": fault != "numeric_drift"},
                    }
                ),
            )
            return 0

    def outcomes(*args):
        assert len(children) == 2
        assert all(c.path.with_suffix(".ready.json").exists() for c in children)
        calls.append("outcome")
        return receipt({"feature_sha256": bundle["receipt_sha256"]})

    monkeypatch.setattr(cli.subprocess, "Popen", Child)
    monkeypatch.setattr(cli.source, "outcomes", outcomes)
    if fault:
        with pytest.raises(Exception):
            cli.run(args, request)
        assert calls == (["outcome"] if fault in ("result_drift", "numeric_drift") else [])
        assert not (output / "acceptance.json").exists()
        failure = cli.read_json(output / "parent.failure.json")
        assert failure["status"] == "FAILED" and failure["fits"] == 0
    else:
        cli.run(args, request)
        assert calls == ["outcome"]
        assert cli.read_json(output / "acceptance.json")["two_process_bitwise_equal"] is True


def test_rejected_output_never_writes_even_a_failure_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "history",
            "prepare",
            "--request",
            str(tmp_path / "request.json"),
            "--request-sha256",
            "0" * 64,
            "--output",
            str(tmp_path / "forbidden.json"),
        ],
    )

    def reject(path):
        raise h.risk.fail("forbidden output", "output_invalid")

    monkeypatch.setattr(cli, "validate_output_location", reject)
    monkeypatch.setattr(cli, "write_once", lambda *a: pytest.fail("rejected output must not be written"))
    assert cli.main() == 1
    assert not (tmp_path / "forbidden.json.failure.json").exists()
