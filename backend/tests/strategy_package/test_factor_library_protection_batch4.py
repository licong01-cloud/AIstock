from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from backend.routers import factor_library, quantevolver
from backend.services.quantevolver import factor_cleanup_service as factor_cleanup_module
from backend.services.strategy_package.factor_reference_guard import (
    FACTOR_DELETE_BLOCKED_REASON_CODE,
    find_strategy_packages_referencing_factor,
)
from backend.services.strategy_package.models import PackageStatus


class FakeConn:
    def __init__(
        self,
        *,
        factor_exists: bool = True,
        package_asset_refs: list[tuple[str, str]] | None = None,
        manifest_refs: list[tuple[str, str]] | None = None,
        raise_on_reference_query: bool = False,
        metric_rows: list[dict[str, Any]] | None = None,
        factor_disable_reason: str | None = None,
    ) -> None:
        self.autocommit = True
        self.factor_exists = factor_exists
        self.package_asset_refs = package_asset_refs or []
        self.manifest_refs = manifest_refs or []
        self.raise_on_reference_query = raise_on_reference_query
        self.metric_rows = metric_rows or []
        self.factor_disable_reason = factor_disable_reason
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.deprecated_rows: list[dict[str, Any]] = []
        self.registered_rows: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeConn":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def cursor(self, *args: Any, **kwargs: Any) -> "FakeCursor":
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn
        self.rowcount = 0
        self._result: list[Any] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        compact_sql = " ".join(sql.split())
        self.conn.queries.append((compact_sql, tuple(params)))
        self.rowcount = 0
        self._result = []

        if "SELECT disable_reason FROM aistock_factor_catalog" in compact_sql:
            self._result = (
                [{"disable_reason": self.conn.factor_disable_reason}]
                if self.conn.factor_exists
                else []
            )
            return

        if "FROM aistock_factor_catalog WHERE factor_name" in compact_sql:
            self._result = [(101,)] if self.conn.factor_exists else []
            return

        if "FROM strategy_pkg.package_asset" in compact_sql:
            self._result = self._reference_rows(
                self.conn.package_asset_refs,
                params,
                reference_source="package_asset",
            )
            return

        if "FROM strategy_pkg.package p" in compact_sql and "jsonb_array_elements" in compact_sql:
            self._result = self._reference_rows(
                self.conn.manifest_refs,
                params,
                reference_source="manifest",
            )
            return

        if "FROM aistock_factor_metrics" in compact_sql and "GROUP BY factor_name" in compact_sql:
            self._result = list(self.conn.metric_rows)
            return

        if "INSERT INTO aistock_factor_catalog" in compact_sql and "ON CONFLICT" in compact_sql:
            self.rowcount = 1
            self.conn.registered_rows = [
                {
                    "factor_name": params[0],
                    "source": params[1],
                    "is_available": True,
                    "updated_at": "2026-09-01T00:00:00+08:00",
                }
            ]
            self._result = self.conn.registered_rows
            return

        if (
            compact_sql.startswith("UPDATE aistock_factor_catalog")
            and ("SET is_available=FALSE" in compact_sql or "SET is_available = FALSE" in compact_sql)
            and "RETURNING factor_name" in compact_sql
        ):
            self.rowcount = 1
            self.conn.deprecated_rows = [
                {
                    "factor_name": params[-1],
                    "source": params[-2] if len(params) > 1 else "qe",
                    "is_available": False,
                    "updated_at": "2026-07-01T00:00:00+08:00",
                    "disable_reason": params[0] if len(params) > 2 else None,
                    "disable_batch_id": params[1] if len(params) > 3 else None,
                }
            ]
            self._result = self.conn.deprecated_rows
            return

        if compact_sql.startswith("UPDATE aistock_factor_catalog") and (
            "WHERE factor_name = %s AND source = %s" in compact_sql
            or "WHERE id = ANY(%s)" in compact_sql
            or "WHERE disable_batch_id = %s" in compact_sql
        ):
            self.rowcount = 1
            return

        if "SELECT COUNT(*) FROM qe_factor_correlations" in compact_sql:
            self._result = [(0,)]
            return

        if "SELECT COUNT(*) FROM aistock_factor_catalog" in compact_sql:
            self._result = [(0,)]
            return

        if compact_sql.startswith("DELETE") or compact_sql.startswith("UPDATE"):
            self.rowcount = 1 if "aistock_factor_catalog WHERE id" in compact_sql else 0
            return

    def _reference_rows(
        self,
        refs: list[tuple[str, str]],
        params: tuple[Any, ...],
        *,
        reference_source: str,
    ) -> list[tuple[str, str, str]]:
        if self.conn.raise_on_reference_query:
            raise RuntimeError("strategy package reference query failed")
        include_retired = bool(params[2] if reference_source == "package_asset" else params[1])
        rows: list[tuple[str, str, str]] = []
        for package_id, package_status in refs:
            if not include_retired and package_status == PackageStatus.RETIRED.value:
                continue
            rows.append((package_id, package_status, reference_source))
        return rows

    def fetchone(self) -> Any:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[Any]:
        return list(self._result)


def _disable_delete_cache_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(quantevolver.os.path, "isfile", lambda _path: False)
    monkeypatch.setattr(quantevolver.os.path, "isdir", lambda _path: False)
    monkeypatch.setattr(quantevolver, "_invalidate_cache_meta", lambda *args, **kwargs: None)


def _delete_sql(conn: FakeConn) -> list[str]:
    return [sql for sql, _params in conn.queries if sql.startswith("DELETE")]


def test_reference_query_merges_package_asset_and_manifest_sources() -> None:
    conn = FakeConn(
        package_asset_refs=[("pkg_asset_only", "BACKTEST_APPROVED"), ("pkg_both", "PAPER_ENABLED")],
        manifest_refs=[("pkg_manifest_only", "SELECTION_ENABLED"), ("pkg_both", "PAPER_ENABLED")],
    )

    refs = find_strategy_packages_referencing_factor(conn, "alpha_demo")

    assert [ref.to_dict() for ref in refs] == [
        {
            "package_id": "pkg_asset_only",
            "package_status": "BACKTEST_APPROVED",
            "reference_sources": ["package_asset"],
        },
        {
            "package_id": "pkg_both",
            "package_status": "PAPER_ENABLED",
            "reference_sources": ["manifest", "package_asset"],
        },
        {
            "package_id": "pkg_manifest_only",
            "package_status": "SELECTION_ENABLED",
            "reference_sources": ["manifest"],
        },
    ]


def test_reference_query_excludes_retired_by_default_and_can_include_for_readonly_audit() -> None:
    conn = FakeConn(
        package_asset_refs=[("pkg_retired", "RETIRED")],
        manifest_refs=[("pkg_active", "BACKTEST_APPROVED")],
    )

    default_refs = find_strategy_packages_referencing_factor(conn, "alpha_demo")
    audit_refs = find_strategy_packages_referencing_factor(conn, "alpha_demo", include_retired=True)

    assert [ref.package_id for ref in default_refs] == ["pkg_active"]
    assert [ref.package_id for ref in audit_refs] == ["pkg_active", "pkg_retired"]


def test_delete_factor_referenced_by_non_retired_package_returns_409_and_does_not_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn(
        package_asset_refs=[("pkg_guarded", "SELECTION_ENABLED")],
        manifest_refs=[("pkg_guarded", "SELECTION_ENABLED")],
    )
    import backend.db.pg_pool as pg_pool

    monkeypatch.setattr(pg_pool, "get_conn", lambda: conn)

    with pytest.raises(HTTPException) as exc_info:
        quantevolver.delete_factor(factor_name="alpha_guarded", source="qe")

    exc = exc_info.value
    assert exc.status_code == 409
    assert exc.detail["reason_code"] == FACTOR_DELETE_BLOCKED_REASON_CODE
    assert exc.detail["referenced_packages"] == [
        {
            "package_id": "pkg_guarded",
            "package_status": "SELECTION_ENABLED",
            "reference_sources": ["manifest", "package_asset"],
        }
    ]
    assert _delete_sql(conn) == []
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_deprecate_referenced_factor_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(package_asset_refs=[("pkg_guarded", "SELECTION_ENABLED")])
    monkeypatch.setattr(factor_library, "get_conn", lambda: conn)

    response = factor_library.deprecate_confirmed(
        factor_library.FactorDeprecateConfirmedRequest(
            factor_name="alpha_guarded",
            source="qe",
            reason="no longer selected for new experiments",
            confirm=factor_library.DEPRECATE_FACTOR_CONFIRM,
        )
    )

    assert response["ok"] is True
    assert response["deprecated"][0]["factor_name"] == "alpha_guarded"
    assert response["deprecated"][0]["is_available"] is False
    assert conn.commits == 1
    assert not any("strategy_pkg.package" in sql for sql, _params in conn.queries)

    update_sql = next(sql for sql, _params in conn.queries if sql.startswith("UPDATE aistock_factor_catalog"))
    update_assignments = update_sql.split("RETURNING", 1)[0]
    assert "disable_reason" in update_assignments
    assert "disable_batch_id" in update_assignments
    assert "disable_at" in update_assignments
    assert "updated_at" not in update_assignments


def test_register_rehabilitation_uses_live_catalog_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(factor_library, "get_conn", lambda: conn)

    response = factor_library.register_confirmed(
        factor_library.FactorRegisterConfirmedRequest(
            factor_name="alpha_rehabilitated",
            source="manual",
            metadata={},
            confirm=factor_library.REGISTER_FACTOR_CONFIRM,
        )
    )

    assert response["ok"] is True
    sql = next(sql for sql, _params in conn.queries if "INSERT INTO aistock_factor_catalog" in sql)
    conflict_assignments = sql.split("RETURNING", 1)[0]
    assert "last_rehab_at" in conflict_assignments
    assert "disable_reason = NULL" in conflict_assignments
    assert "NOT LIKE %s" in conflict_assignments
    assert "updated_at" not in conflict_assignments


def test_register_cannot_reactivate_pit_causality_quarantine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn(factor_disable_reason="PIT_CAUSALITY_VIOLATION: future append drift")
    monkeypatch.setattr(factor_library, "get_conn", lambda: conn)

    with pytest.raises(HTTPException) as exc_info:
        factor_library.register_confirmed(
            factor_library.FactorRegisterConfirmedRequest(
                factor_name="alpha_quarantined",
                source="manual",
                metadata={},
                confirm=factor_library.REGISTER_FACTOR_CONFIRM,
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error"] == "pit_causality_quarantine_requires_new_factor_identity"
    assert not any("INSERT INTO aistock_factor_catalog" in sql for sql, _params in conn.queries)


def test_quantevolver_factor_availability_uses_live_catalog_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.db.pg_pool as pg_pool

    disabled_conn = FakeConn()
    monkeypatch.setattr(pg_pool, "get_conn", lambda: disabled_conn)
    disabled = quantevolver.set_factor_availability(
        "alpha_guarded",
        quantevolver.FactorAvailabilityRequest(
            source="manual",
            is_available=False,
            reason="PIT_CAUSALITY_VIOLATION",
            batch_id="BUG-1302",
        ),
    )

    assert disabled["is_available"] is False
    disable_sql = next(
        sql
        for sql, _params in disabled_conn.queries
        if sql.startswith("UPDATE aistock_factor_catalog")
    )
    assert "disable_reason" in disable_sql
    assert "disable_batch_id" in disable_sql
    assert "disable_at" in disable_sql
    assert "updated_at" not in disable_sql

    enabled_conn = FakeConn()
    monkeypatch.setattr(pg_pool, "get_conn", lambda: enabled_conn)
    enabled = quantevolver.set_factor_availability(
        "alpha_guarded",
        quantevolver.FactorAvailabilityRequest(source="manual", is_available=True),
    )

    assert enabled["is_available"] is True
    enable_sql = next(
        sql
        for sql, _params in enabled_conn.queries
        if sql.startswith("UPDATE aistock_factor_catalog")
    )
    assert "last_rehab_at" in enable_sql
    assert "disable_reason = NULL" in enable_sql
    assert "updated_at" not in enable_sql


def test_quantevolver_cannot_reactivate_pit_causality_quarantine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.db.pg_pool as pg_pool

    conn = FakeConn(factor_disable_reason="PIT_CAUSALITY_VIOLATION: future append drift")
    monkeypatch.setattr(pg_pool, "get_conn", lambda: conn)

    with pytest.raises(HTTPException) as exc_info:
        quantevolver.set_factor_availability(
            "alpha_quarantined",
            quantevolver.FactorAvailabilityRequest(source="manual", is_available=True),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["error"] == "pit_causality_quarantine_requires_new_factor_identity"
    assert not any(sql.startswith("UPDATE aistock_factor_catalog") for sql, _params in conn.queries)


def test_quantevolver_batch_unavailable_records_reason_and_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn()
    monkeypatch.setattr(quantevolver, "get_conn", lambda: conn)

    response = quantevolver.batch_factor_action(
        quantevolver.BatchFactorActionRequest(
            action="set_unavailable",
            factors=[
                {
                    "factor_name": "alpha_guarded",
                    "source": "manual",
                    "reason": "PIT_CAUSALITY_VIOLATION",
                }
            ],
        )
    )

    assert response["ok"] is True
    assert response["succeeded_count"] == 1
    assert response["succeeded"][0]["disable_reason"] == "PIT_CAUSALITY_VIOLATION"
    assert response["succeeded"][0]["disable_batch_id"].startswith("factor_batch_action_")
    update_sql = next(
        sql
        for sql, _params in conn.queries
        if sql.startswith("UPDATE aistock_factor_catalog")
        and "WHERE factor_name = %s AND source = %s" in sql
    )
    assert "disable_reason" in update_sql
    assert "disable_batch_id" in update_sql
    assert "disable_at" in update_sql
    assert "updated_at" not in update_sql


def test_cleanup_execute_and_rollback_use_live_catalog_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn()
    monkeypatch.setattr(factor_cleanup_module, "get_conn", lambda: conn)
    service = factor_cleanup_module.FactorCleanupService()

    result = service.execute(
        [101],
        {101: service.REASON_PURE_NOISE},
        batch_id="BUG-1302-cleanup-contract",
    )
    rollback = service.rollback_batch("BUG-1302-cleanup-contract")

    assert result["ok"] is True
    assert result["disabled_count"] == 1
    assert rollback["rehab_count"] == 1
    assert rollback["blocked_quarantine_count"] == 0
    lifecycle_updates = [
        sql
        for sql, _params in conn.queries
        if sql.startswith("UPDATE aistock_factor_catalog")
        and ("WHERE id = ANY(%s)" in sql or "WHERE disable_batch_id = %s" in sql)
    ]
    assert len(lifecycle_updates) == 2
    assert "disable_at = NOW()" in lifecycle_updates[0]
    assert "updated_at" not in lifecycle_updates[0]
    assert "last_rehab_at = NOW()" in lifecycle_updates[1]
    assert "NOT LIKE %s" in lifecycle_updates[1]
    assert "updated_at" not in lifecycle_updates[1]


def test_delete_factor_unreferenced_factor_keeps_existing_hard_delete_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConn()
    import backend.db.pg_pool as pg_pool

    monkeypatch.setattr(pg_pool, "get_conn", lambda: conn)
    _disable_delete_cache_cleanup(monkeypatch)

    response = quantevolver.delete_factor(factor_name="alpha_unused", source="qe")

    assert response["ok"] is True
    assert response["factor_name"] == "alpha_unused"
    assert any("DELETE FROM aistock_factor_catalog WHERE id = %s" in sql for sql in _delete_sql(conn))
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_usage_summary_includes_strategy_package_references(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(
        metric_rows=[
            {
                "factor_name": "alpha_guarded",
                "metric_version_count": 2,
                "latest_metric_at": "2026-07-01T00:00:00+08:00",
                "calc_batch_count": 1,
            }
        ],
        package_asset_refs=[("pkg_asset_only", "BACKTEST_APPROVED")],
        manifest_refs=[("pkg_manifest_only", "PAPER_ENABLED")],
    )
    monkeypatch.setattr(factor_library, "get_conn", lambda: conn)

    payload = factor_library.get_usage_summary("alpha_guarded")

    assert payload["ok"] is True
    assert payload["strategy_package_references"] == {
        "referenced": True,
        "count": 2,
        "packages": [
            {
                "package_id": "pkg_asset_only",
                "package_status": "BACKTEST_APPROVED",
                "reference_sources": ["package_asset"],
            },
            {
                "package_id": "pkg_manifest_only",
                "package_status": "PAPER_ENABLED",
                "reference_sources": ["manifest"],
            },
        ],
        "blocking_policy": "non_retired_packages_block_hard_delete",
        "reason_code": FACTOR_DELETE_BLOCKED_REASON_CODE,
    }


def test_reference_query_failure_propagates_before_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn(raise_on_reference_query=True)
    import backend.db.pg_pool as pg_pool

    monkeypatch.setattr(pg_pool, "get_conn", lambda: conn)

    with pytest.raises(HTTPException) as exc_info:
        quantevolver.delete_factor(factor_name="alpha_guarded", source="qe")

    assert exc_info.value.status_code == 500
    assert "strategy package reference query failed" in str(exc_info.value.detail)
    assert _delete_sql(conn) == []
    assert conn.commits == 0
    assert conn.rollbacks == 1
