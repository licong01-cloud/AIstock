from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_cli_preflight_missing_request_durable_failure_zero_fit(tmp_path):
    root = Path(__file__).resolve().parents[3]
    output = tmp_path / "preflight.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/hmm_risk/run_formal_state_model_set.py"),
            "preflight",
            "--request",
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and not output.exists()
    failure = json.loads(output.with_name("preflight.json.failure.json").read_text(encoding="utf-8"))
    assert failure["ready"] is False and failure["database_write"] is False
    assert failure["exception_type"] == "FileNotFoundError"


def test_cli_unsafe_output_does_not_write_a_failure_receipt(tmp_path):
    root = Path(__file__).resolve().parents[3]
    release = tmp_path / "release"
    release.mkdir()
    (release / "direct_monthly_state.json").write_text("{}", encoding="utf-8")
    output = release / "forbidden.json"
    result = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/hmm_risk/run_formal_state_model_set.py"),
            "preflight",
            "--request",
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and "without writing" in result.stderr
    assert not output.exists() and not output.with_name(output.name + ".failure.json").exists()
