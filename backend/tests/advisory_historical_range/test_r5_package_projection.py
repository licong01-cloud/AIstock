from __future__ import annotations

from backend.services.advisory_historical_range.outcome_policy_catalog import (
    R4_DEFAULT_HORIZONS,
    R4_LONG_TREND_HORIZONS,
    load_historical_range_outcome_policy_catalog,
)
from backend.services.advisory_historical_range import composition


def test_options_use_exact_r4_catalog_without_new_policy() -> None:
    catalog = load_historical_range_outcome_policy_catalog()
    assert catalog.default_horizons == R4_DEFAULT_HORIZONS
    assert catalog.long_trend_horizons == R4_LONG_TREND_HORIZONS
    assert len(catalog.catalog_content_hash) == 64


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        assert "LIMIT" not in statement.upper()
        assert "manifest_json -> 'alpha_components'" in statement
        assert "AS alpha_count" in statement
        assert len(params[0]) == 4

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def set_session(self, **kwargs):
        assert kwargs == {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}

    def cursor(self, **_kwargs):
        return _Cursor(self.rows)

    def rollback(self):
        pass


def test_options_projection_does_not_truncate_more_than_500_admitted_packages(monkeypatch) -> None:
    rows = [
        {
            "package_id": f"pkg_{index:04d}",
            "package_name": f"Package {index:04d}",
            "alpha_mode": "single_alpha",
            "alpha_count": 1,
            "manifest_sha256": f"{index:064x}"[-64:],
            "package_version": "1",
            "package_status": "SELECTION_ENABLED",
        }
        for index in range(501)
    ]
    monkeypatch.setattr(
        composition,
        "AdvisoryProgramPGRepository",
        lambda **_kwargs: type("Programs", (), {"list_programs": lambda *_args, **_kwargs: []})(),
    )
    connection = _Connection(rows)
    monkeypatch.setattr(
        composition,
        "historical_read_only_connection_factory",
        lambda _factory: lambda: connection,
    )
    result = composition._project_historical_range_options(lambda: connection)
    projected = result["data"]["admitted_packages"]
    assert len(projected) == 501
    assert [item["package_id"] for item in projected] == sorted(item["package_id"] for item in projected)
