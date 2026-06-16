from __future__ import annotations

from unittest.mock import MagicMock

from backend.services.quantevolver.factor_eligibility_service import FactorEligibilityService


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.cur = MagicMock()
        self.cur.description = [("id",), ("factor_name",), ("transformation_status",), ("is_available",), ("qe_code_path",), ("code_text",), ("correlation_computed_at",)]
        self.cur.fetchall.return_value = rows
        self.cur.fetchone.return_value = rows[0] if rows else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        cm = MagicMock()
        cm.__enter__.return_value = self.cur
        cm.__exit__.return_value = False
        return cm


def test_official_offline_eligibility_uses_code_text_not_transformation(monkeypatch):
    row = (1, "factor_a", "FAILED", True, None, "def compute(): pass", None)
    conn = _Conn([row])
    monkeypatch.setattr("backend.services.quantevolver.factor_eligibility_service.get_conn", lambda: conn)

    result = FactorEligibilityService().list_eligible_factors(["factor_a"], source_mode="official_offline")

    sql = conn.cur.execute.call_args.args[0]
    assert "code_text" in sql
    assert "transformation_status = 'SUCCESS'" not in sql
    assert "qe_code_path IS NOT NULL" not in sql
    assert result[0]["factor_name"] == "factor_a"
    assert result[0]["code_source"] == "code_text"


def test_realtime_transformed_mode_still_requires_transformed_file(monkeypatch, tmp_path):
    code_path = tmp_path / "factor.py"
    code_path.write_text("x = 1", encoding="utf-8")
    row = (1, "factor_live", "SUCCESS", True, str(code_path), "def offline(): pass", None)
    conn = _Conn([row])
    monkeypatch.setattr("backend.services.quantevolver.factor_eligibility_service.get_conn", lambda: conn)
    monkeypatch.setattr("backend.services.quantevolver.factor_eligibility_service._PROJECT_ROOT", "")

    result = FactorEligibilityService().list_eligible_factors(["factor_live"], source_mode="realtime_transformed")

    sql = conn.cur.execute.call_args.args[0]
    assert "transformation_status = 'SUCCESS'" in sql
    assert "qe_code_path IS NOT NULL" in sql
    assert result[0]["code_source"] == "qe_code_path"
