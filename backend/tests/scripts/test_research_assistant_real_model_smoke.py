from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]


def test_real_model_smoke_loud_skips_when_deepseek_key_missing(tmp_path: Path) -> None:
    output = tmp_path / "ra_real_model_smoke.json"
    env = os.environ.copy()
    env.pop("DEEPSEEK_API_KEY", None)
    env["PYTHONPATH"] = str(ROOT)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/research_assistant_real_model_smoke.py",
            "--output",
            str(output),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 77
    assert output.exists()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "skipped"
    assert payload["reason_code"] == "deepseek_api_key_missing"
    assert payload["llm_config"]["credential_source"] == "missing"
    assert payload["llm_config"]["db_config_lookup_allowed"] is False
    assert payload["safety"]["production_db_touched"] is False
    assert payload["safety"]["ddl_executed"] is False
    assert payload["safety"]["started_services"] is False
    combined_output = result.stdout + result.stderr
    assert "DEEPSEEK_API_KEY" in combined_output
    assert "fake_pass=false" in combined_output
