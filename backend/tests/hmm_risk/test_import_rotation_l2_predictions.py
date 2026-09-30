from __future__ import annotations

import json
import sys

from scripts.hmm_risk import import_rotation_l2_predictions as subject


def test_import_cli_converts_acceptance_and_writes_exact_rows(tmp_path, monkeypatch, capsys) -> None:
    acceptance_path = tmp_path / "acceptance.json"
    acceptance = {"schema_version": "hmm_risk_rotation_l2_acceptance_v1", "run_id": "run-1"}
    acceptance_path.write_text(json.dumps(acceptance), encoding="utf-8")
    expected_rows = [{"run_id": "run-1", "sector_code": "801010.SI"}]
    captured: dict[str, object] = {}

    monkeypatch.setattr(subject, "rows_from_acceptance", lambda value: expected_rows if value == acceptance else [])

    class Repository:
        def write_rows(self, rows):
            captured["rows"] = rows
            return {"inserted": len(rows), "readback_verified": True}

    monkeypatch.setattr(subject, "RotationL2PredictionRepository", Repository)
    monkeypatch.setattr(sys, "argv", ["import_rotation_l2_predictions.py", "--acceptance", str(acceptance_path)])

    assert subject.main() == 0
    assert captured == {"rows": expected_rows}
    assert json.loads(capsys.readouterr().out) == {"inserted": 1, "readback_verified": True}


def test_import_cli_rejects_non_json_acceptance_before_database_write(tmp_path, monkeypatch) -> None:
    acceptance_path = tmp_path / "acceptance.json"
    acceptance_path.write_text("not-json", encoding="utf-8")
    repository_constructed = False

    class Repository:
        def __init__(self) -> None:
            nonlocal repository_constructed
            repository_constructed = True

    monkeypatch.setattr(subject, "RotationL2PredictionRepository", Repository)
    monkeypatch.setattr(sys, "argv", ["import_rotation_l2_predictions.py", "--acceptance", str(acceptance_path)])

    try:
        subject.main()
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("invalid acceptance JSON must fail before repository construction")
    assert repository_constructed is False
