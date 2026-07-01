from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.routers import factor_library, quantevolver
from backend.services.strategy_package.factor_usage import (
    STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON,
    STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED,
    StrategyPackageFactorUsageQueryError,
    find_strategy_package_factor_usage,
)


class _UsageCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class _UsageConn:
    def __init__(self, rows):
        self.cursor_obj = _UsageCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def _usage_row(**overrides):
    row = {
        "package_id": "pkg_1",
        "package_name": "demo package",
        "package_status": "ASSET_VALIDATED",
        "alpha_mode": "single_alpha",
        "manifest_sha256": "sha_pkg_1",
        "reference_source": "package_asset",
        "referenced_factor_name": "factor_a",
        "referenced_factor_id": "factor_a",
        "asset_ref": "aistock-package-asset://blobs/abc?kind=factor_code&logical_name=factor_a",
        "asset_sha256": "sha_factor_a",
        "source_uri": "aistock_factor_catalog:7:code_text",
        "reference_count": 2,
        "package_count": 1,
    }
    row.update(overrides)
    return row


def test_strategy_package_factor_usage_includes_ledger_and_manifest_refs():
    rows = [
        _usage_row(reference_source="package_asset", reference_count=2, package_count=2),
        _usage_row(
            package_id="pkg_2",
            package_name="manifest package",
            reference_source="manifest_factor_set",
            asset_ref="legacy://factor_a",
            asset_sha256=None,
            source_uri=None,
            reference_count=2,
            package_count=2,
        ),
    ]

    usage = find_strategy_package_factor_usage("factor_a", conn=_UsageConn(rows), limit=10)

    assert usage["protected"] is True
    assert usage["reason_code"] == STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON
    assert usage["package_count"] == 2
    assert usage["reference_count"] == 2
    assert [ref["reference_source"] for ref in usage["references"]] == ["package_asset", "manifest_factor_set"]
    assert "strategy_pkg.package_asset.asset_type=factor_code" in usage["query_sources"]


def test_strategy_package_factor_usage_empty_is_not_protected():
    usage = find_strategy_package_factor_usage("factor_unused", conn=_UsageConn([]), limit=1)

    assert usage["protected"] is False
    assert usage["reason_code"] is None
    assert usage["package_count"] == 0
    assert usage["references"] == []


def test_usage_summary_exposes_strategy_package_references(monkeypatch):
    monkeypatch.setattr(
        factor_library,
        "_rows",
        lambda sql, params=(): [
            {
                "factor_name": "factor_a",
                "metric_version_count": 3,
                "latest_metric_at": "2026-06-30T00:00:00Z",
                "calc_batch_count": 2,
            }
        ],
    )
    monkeypatch.setattr(
        factor_library,
        "_strategy_package_usage_or_http",
        lambda factor_name, limit=20: {
            "protected": True,
            "reason_code": STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON,
            "package_count": 1,
            "reference_count": 1,
            "references": [_usage_row()],
            "query_sources": ["strategy_pkg.package_asset.asset_type=factor_code"],
        },
    )

    payload = factor_library.get_usage_summary("factor_a", limit=5)

    assert payload["items"][0]["metric_version_count"] == 3
    usage = payload["strategy_package_usage"]
    assert usage["protected"] is True
    assert usage["reason_code"] == STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON
    assert usage["package_count"] == 1
    assert usage["sample_references"][0]["package_id"] == "pkg_1"


def test_plan_deprecate_allows_strategy_package_referenced_factor(monkeypatch):
    monkeypatch.setattr(
        factor_library,
        "_one",
        lambda sql, params=(): {"factor_name": "factor_a", "source": "manual", "is_available": True},
    )
    monkeypatch.setattr(
        factor_library,
        "_strategy_package_usage_or_http",
        lambda factor_name: {
            "protected": True,
            "reason_code": STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON,
            "package_count": 1,
            "reference_count": 1,
            "references": [_usage_row()],
        },
    )

    payload = factor_library.plan_deprecate(
        factor_library.FactorDeprecatePlanRequest(factor_name="factor_a", source="manual", reason="superseded")
    )

    assert payload["will_write"] is True
    assert payload["deprecate_policy"] == "allowed_even_when_referenced_by_strategy_package"
    assert payload["strategy_package_usage"]["protected"] is True
    assert payload["strategy_package_usage"]["reason_code"] == STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON


class _DeleteCursor:
    def __init__(self, catalog_id=7):
        self.catalog_id = catalog_id
        self.statements = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        if "SELECT id FROM aistock_factor_catalog" in sql:
            self._last = (self.catalog_id,)
            self.rowcount = 1
        elif "SELECT COUNT(*) FROM qe_factor_correlations" in sql:
            self._last = (0,)
            self.rowcount = 1
        else:
            self._last = None
            self.rowcount = 0

    def fetchone(self):
        return self._last


class _DeleteConn:
    def __init__(self):
        self.autocommit = True
        self.cursor_obj = _DeleteCursor()
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_hard_delete_referenced_factor_is_blocked_before_delete(monkeypatch):
    conn = _DeleteConn()
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: conn)
    monkeypatch.setattr(
        quantevolver,
        "find_strategy_package_factor_usage",
        lambda factor_name, conn, limit=20: {
            "protected": True,
            "package_count": 1,
            "reference_count": 1,
            "references": [_usage_row()],
        },
    )

    with pytest.raises(HTTPException) as excinfo:
        quantevolver.delete_factor(factor_name="factor_a", source="manual")

    assert excinfo.value.status_code == 409
    assert excinfo.value.detail["reason_code"] == STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON
    assert excinfo.value.detail["package_count"] == 1
    assert excinfo.value.detail["allowed_action"] == "deprecate_factor"
    executed_sql = "\n".join(sql for sql, _params in conn.cursor_obj.statements)
    assert "DELETE FROM aistock_factor_catalog" not in executed_sql
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_hard_delete_fails_closed_when_usage_check_fails(monkeypatch):
    conn = _DeleteConn()
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: conn)

    def _raise_usage(*args, **kwargs):
        raise StrategyPackageFactorUsageQueryError(
            "boom",
            context={"reason_code": STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED, "factor_name": "factor_a"},
        )

    monkeypatch.setattr(quantevolver, "find_strategy_package_factor_usage", _raise_usage)

    with pytest.raises(HTTPException) as excinfo:
        quantevolver.delete_factor(factor_name="factor_a", source="manual")

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail["reason_code"] == STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED
    assert "fail-closed" in excinfo.value.detail["message"]
    executed_sql = "\n".join(sql for sql, _params in conn.cursor_obj.statements)
    assert "DELETE FROM aistock_factor_catalog" not in executed_sql
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_hard_delete_unreferenced_factor_keeps_existing_delete_path(monkeypatch):
    conn = _DeleteConn()
    monkeypatch.setattr("backend.db.pg_pool.get_conn", lambda: conn)
    monkeypatch.setattr(
        quantevolver,
        "find_strategy_package_factor_usage",
        lambda factor_name, conn, limit=20: {
            "protected": False,
            "package_count": 0,
            "reference_count": 0,
            "references": [],
        },
    )
    monkeypatch.setattr(quantevolver.os.path, "isfile", lambda path: False)
    monkeypatch.setattr(quantevolver.os.path, "isdir", lambda path: False)
    monkeypatch.setattr(quantevolver, "_invalidate_cache_meta", lambda *args, **kwargs: None)

    payload = quantevolver.delete_factor(factor_name="factor_unused", source="manual")

    assert payload["ok"] is True
    assert payload["factor_name"] == "factor_unused"
    executed_sql = "\n".join(sql for sql, _params in conn.cursor_obj.statements)
    assert "DELETE FROM aistock_factor_catalog" in executed_sql
    assert conn.commits == 1
    assert conn.rollbacks == 0
