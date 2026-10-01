from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import audit_qe_moneyflow_alias_coverage as audit_module


def test_provider_absence_keys_reject_duplicate_identity(tmp_path: Path) -> None:
    row = {
        "canonical_ts_code": "302132.SZ",
        "source_dataset": "market.moneyflow_ts",
        "source_ts_code": "300114.SZ",
        "trade_date": "2024-08-13",
    }
    path = tmp_path / "provider_absence.json"
    path.write_text(json.dumps({"schema_version": "test_v1", "rows": [row, row]}), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate provider absence key"):
        audit_module._provider_absence_keys(path)


def test_main_writes_receipt_create_exclusive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = tmp_path / "receipt.json"
    receipt = {
        "schema_version": audit_module.SCHEMA_VERSION,
        "status": "PASS",
        "database_read": True,
        "database_write": False,
    }
    monkeypatch.setattr(audit_module, "audit", lambda **_: receipt)
    args = [
        "--candidate-root",
        str(tmp_path),
        "--security-identity-manifest",
        str(tmp_path / "identity.json"),
        "--provider-absence-manifest",
        str(tmp_path / "absence.json"),
        "--start-date",
        "2024-08-13",
        "--end-date",
        "2025-02-14",
        "--output",
        str(output),
    ]

    assert audit_module.main(args) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == receipt
    with pytest.raises(FileExistsError):
        audit_module.main(args)
