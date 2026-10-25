"""Direct-neighbor CLI coverage, sharing the store's existing record fixture."""

import json
from pathlib import Path
import subprocess
import sys

import pytest

from backend.services.hmm_risk import product_validation_store as store
from backend.tests.hmm_risk.test_product_validation_store import _record


@pytest.mark.parametrize("duplicate_field", [False, True])
def test_cli_registers_an_existing_result_without_db_env_or_server(tmp_path, duplicate_field):
    record = _record("rotation_l2")
    source = tmp_path / "existing-result.json"
    payload = json.dumps(record)
    if duplicate_field:
        payload = payload.replace('"writer_readback": true', '"writer_readback": false, "writer_readback": true')
    source.write_text(payload, encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "scripts.hmm_risk.register_product_validation",
        "--receipt",
        str(source),
        "--store-root",
        str(tmp_path / "store"),
    ]
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    if duplicate_field:
        assert result.returncode != 0
        assert "duplicate fields" in result.stderr
        assert not (tmp_path / "store").exists()
        return
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["registration_readback"] is True
    assert store.read_receipt(Path(value["path"])) == record
