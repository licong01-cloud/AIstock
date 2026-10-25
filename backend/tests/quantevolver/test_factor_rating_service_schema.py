"""Runtime rating entries consume existing schema, never create it."""
from contextlib import contextmanager

import psycopg2
import pytest

from backend.services.quantevolver import factor_rating_service as module


@pytest.mark.parametrize("failure", [None, psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn,
                                     psycopg2.errors.InsufficientPrivilege])
def test_rating_schema_read_only_and_errors(monkeypatch, failure):
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql):
            statements.append(sql)
            assert sql.strip().upper().startswith("SELECT"), "runtime must not execute DDL/DML"
            if failure:
                raise failure("schema unavailable")

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(module, "get_conn", connection)
    service = module.FactorRatingService()
    if failure:
        expected = RuntimeError if failure in (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn) else failure
        with pytest.raises(expected):
            service.ensure_schema()
    else:
        service.ensure_schema()
        # A different DB/schema in the same process must not inherit readiness.
        service.ensure_schema()
        assert len(statements) == 6
        assert all("LIMIT 0" in sql.upper() for sql in statements)
        assert all(any(table in sql for sql in statements) for table in (
            "qe_rating_rule_versions", "qe_factor_rating_runs", "qe_factor_official_ratings"))


def test_selected_rating_keeps_existing_writer_without_ddl(monkeypatch):
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=()):
            verb = sql.strip().split()[0].upper()
            assert verb in {"SELECT", "INSERT", "UPDATE"}, "rating must never provision schema"
            statements.append((verb, sql, params))

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(module, "get_conn", connection)
    monkeypatch.setattr(module, "_RULES_SYNCED", False)
    service = module.FactorRatingService()
    rule = {"rule_version": "v2.0.0", "status": "active"}
    monkeypatch.setattr(service, "get_rule_detail", lambda version: rule)
    monkeypatch.setattr(service, "_ensure_rule_executable", lambda *args, **kwargs: None)
    target = {"id": 1545, "factor_name": "target", "source": "manual"}
    payload = {"selected_factors": [{"factor_name": "target", "source": "manual"}]}
    def scope(scope_type, scope_payload, version):
        assert (scope_type, scope_payload, version) == ("selected", payload, "v2.0.0")
        return [target]
    monkeypatch.setattr(service, "_resolve_scope", scope)
    monkeypatch.setattr(service, "_grade_factor", lambda *args: {"snapshot_date": "2026-08-31"})
    ratings = []
    monkeypatch.setattr(service, "_upsert_official_rating", lambda run, version, factor_id, result: ratings.append(factor_id))
    result = service.run_rating("v2.0.0", "selected", payload)
    assert result["success_count"] == 1 and result["failed_count"] == 0
    assert ratings == [1545]
    assert any(verb == "INSERT" and "qe_factor_rating_runs" in sql for verb, sql, _ in statements)
    assert any(verb == "UPDATE" and "qe_factor_rating_runs" in sql for verb, sql, _ in statements)


@pytest.mark.parametrize("kind, expected", [("direction", -1), ("core_ic", None), ("fitness", 0.0)])
def test_existing_invalid_metric_behavior_is_visible_not_changed(caplog, kind, expected):
    service = module.FactorRatingService()
    if kind == "direction":
        result = service._resolve_direction({}, {"direction": "invalid", "best_horizon": "invalid", "rank_ic_mean": -0.01})
    elif kind == "core_ic":
        result = service._compute_core_ic_v2({"ic_mean": "invalid", "rank_ic_20d": "invalid"}, 20)
    else:
        result = service._score_multi_alpha_fitness_v2(
            {"cluster_role": "member", "sector_exposure_corr": "invalid"}, 0, "unknown",
            {"multi_alpha_fitness_v2": {"cluster_role": {"member": "invalid"}}})
    assert result == expected
    assert len(caplog.records) == 2
