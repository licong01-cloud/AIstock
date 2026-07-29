from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_r5_sql_is_confined_to_historical_range_tables() -> None:
    source = (ROOT / "backend/services/advisory_historical_range/query_repository.py").read_text(encoding="utf-8")
    forbidden = (
        "app.advisory_program ", "app.paper_", "app.simulation_", "trading.", "qe.", "qlib."
    )
    assert not any(value in source for value in forbidden)
    assert "app.advisory_historical_range_" in source
