from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_observer_cli_requires_an_explicit_dev_env_file_without_enable_gate(tmp_path) -> None:
    root = Path(__file__).resolve().parents[3]
    missing_env = tmp_path / "missing.env"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_phase1_source_observer.py",
            "observe-once",
            "--env-file",
            str(missing_env),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "ADVISORY_PHASE1_SOURCE_OBSERVER_CONFIG_INVALID" in completed.stderr
    assert "SOURCE_OBSERVER_ENABLED" not in completed.stderr
