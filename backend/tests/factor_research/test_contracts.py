"""Compact contracts for request, CLI, candidate identity and safe defaults."""

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import CANONICAL_UNIVERSE, validate_spec
from scripts.factor_research import configure

ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = ROOT / "backend" / "services" / "factor_research" / "candidate_subprocess.py"


def spec(tmp_path):
    data, script = tmp_path / "data", tmp_path / "factor.py"
    data.mkdir()
    script.write_text("# reviewed", encoding="utf-8")
    return {
        "task_id": str(uuid4()),
        "record_id": str(uuid4()),
        "attempt_id": str(uuid4()),
        "expected_revision": 1,
        "universe_key": CANONICAL_UNIVERSE,
        "method_version": "1.0",
        "data_dir": str(data),
        "qlib_bin_path": str(data),
        "artifact_root": str(tmp_path / "out"),
        "instruments": ["000001.SZ"],
        "read_start": "2026-01-01",
        "signal_start": "2026-01-02",
        "signal_end": "2026-01-03",
        "read_end": "2026-01-04",
        "cutoff": "2026-01-05",
        "candidates": [{"factor_name": "m_trial", "script": str(script)}],
    }


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda value: value.update(universe_key="shsz_st_pit_active_v1"), "no PIT bootstrap"),
        (lambda value: value.update(activate_factor=True), "Unknown run fields"),
        (lambda value: value["candidates"].append(dict(value["candidates"][0])), "unique"),
    ],
)
def test_run_scope_and_candidate_identity_fail_closed(tmp_path, mutation, match):
    value = spec(tmp_path)
    mutation(value)
    with pytest.raises(ResearchError, match=match):
        validate_spec(value)


def test_cli_and_dev_configuration_default_to_no_database(tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/factor_research.py", "--help"],
        cwd=ROOT,
        capture_output=True,
        timeout=20,
        env={**os.environ, "TDX_DB_PORT": "invalid"},
    )
    assert result.returncode == 0 and b"attach" in result.stdout
    path = tmp_path / "dev.env"
    path.write_text("TDX_DB_NAME=production\n", encoding="utf-8")
    with pytest.raises(ResearchError, match="incomplete"):
        configure(path, "dev")


def test_candidate_subprocess_restores_large_instrument_cli_contract(tmp_path):
    (tmp_path / "candidate_helper.py").write_text(
        "def preserve(values):\n    return values\n",
        encoding="utf-8",
    )
    candidate = tmp_path / "candidate.py"
    candidate.write_text(
        """import argparse, json
from pathlib import Path
from candidate_helper import preserve
p = argparse.ArgumentParser()
p.add_argument('--data-dir', required=True)
p.add_argument('--output', required=True)
p.add_argument('--start-date', required=True)
p.add_argument('--end-date', required=True)
p.add_argument('--instruments', required=True)
a = p.parse_args()
Path(a.output).write_text(json.dumps(preserve(json.loads(a.instruments))), encoding='utf-8')
""",
        encoding="utf-8",
    )
    instruments = [f"{number:06d}.SZ" for number in range(1, 5342)]
    instruments_path = tmp_path / "scope_instruments.json"
    instruments_path.write_text(json.dumps(instruments), encoding="utf-8")
    output = tmp_path / "received.json"

    result = subprocess.run(
        [
            sys.executable,
            str(LAUNCHER),
            "--script",
            str(candidate),
            "--data-dir",
            str(tmp_path),
            "--output",
            str(output),
            "--start-date",
            "2018-08-01",
            "--end-date",
            "2026-08-31",
            "--instruments-file",
            str(instruments_path),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text(encoding="utf-8")) == instruments
