from __future__ import annotations

import pytest

from backend.services.strategy_package import factor_usage as fu


class _UsageCursor:
    def __init__(self, rows=None, fail=False):
        self.rows = rows or []
        self.fail = fail
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        if self.fail:
            raise RuntimeError("sql boom")
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class _UsageConn:
    def __init__(self, rows=None, fail=False):
        self.cursor_obj = _UsageCursor(rows=rows, fail=fail)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def _row(**overrides):
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
        "package_count": 2,
    }
    row.update(overrides)
    return row


def test_factor_usage_reports_package_asset_and_manifest_refs():
    usage = fu.find_strategy_package_factor_usage(
        "factor_a",
        conn=_UsageConn(
            [
                _row(reference_source="package_asset"),
                _row(
                    package_id="pkg_2",
                    package_name="manifest package",
                    reference_source="manifest_factor_set",
                    asset_ref="legacy://factor_a",
                    asset_sha256=None,
                    source_uri=None,
                ),
            ]
        ),
        limit=5,
    )

    assert usage["protected"] is True
    assert usage["reason_code"] == fu.STRATEGY_PACKAGE_FACTOR_DELETE_BLOCK_REASON
    assert usage["package_count"] == 2
    assert usage["reference_count"] == 2
    assert [ref["reference_source"] for ref in usage["references"]] == ["package_asset", "manifest_factor_set"]
    assert fu.has_strategy_package_factor_usage("factor_a", conn=_UsageConn([_row()])) is True


def test_factor_usage_empty_and_zero_limit_are_safe():
    usage = fu.find_strategy_package_factor_usage("factor_unused", conn=_UsageConn([]), limit=0)

    assert usage["protected"] is False
    assert usage["reason_code"] is None
    assert usage["limit"] == 1
    assert usage["references"] == []
    assert fu.has_strategy_package_factor_usage("factor_unused", conn=_UsageConn([])) is False


def test_factor_usage_rejects_blank_factor_name():
    with pytest.raises(fu.StrategyPackageFactorUsageQueryError) as excinfo:
        fu.find_strategy_package_factor_usage("  ", conn=_UsageConn([]))

    assert excinfo.value.context["reason_code"] == fu.STRATEGY_PACKAGE_FACTOR_USAGE_QUERY_FAILED


def test_factor_usage_wraps_sql_and_connection_failures(monkeypatch):
    with pytest.raises(fu.StrategyPackageFactorUsageQueryError) as sql_exc:
        fu.find_strategy_package_factor_usage("factor_a", conn=_UsageConn(fail=True))
    assert "sql boom" in sql_exc.value.context["error"]

    monkeypatch.setattr(fu, "get_conn", lambda: _UsageConn([_row()]))
    assert fu.find_strategy_package_factor_usage("factor_a")["protected"] is True

    def _raise_conn():
        raise RuntimeError("connect boom")

    monkeypatch.setattr(fu, "get_conn", _raise_conn)
    with pytest.raises(fu.StrategyPackageFactorUsageQueryError) as conn_exc:
        fu.find_strategy_package_factor_usage("factor_a")
    assert "connect boom" in conn_exc.value.context["error"]
