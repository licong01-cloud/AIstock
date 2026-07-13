from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_observer_cli_reports_disabled_nonzero_without_opening_a_database_connection() -> None:
    root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    env.pop("AISTOCK_ADVISORY_PHASE1_SOURCE_OBSERVER_ENABLED", None)

    completed = subprocess.run(
        [sys.executable, "scripts/advisory_phase1_source_observer.py", "observe-once"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "ADVISORY_PHASE1_SOURCE_OBSERVER_DISABLED" in completed.stderr
