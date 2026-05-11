from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pytest

import scripts.governance_migration_smoke as smoke


def _query_literal(sql: str, field: str) -> str:
    match = re.search(rf"{re.escape(field)}\s*=\s*'([^']*)'", sql)
    if not match:
        raise AssertionError(f"missing {field!r} literal in SQL: {sql}")
    return match.group(1)


def _preflight_relations() -> dict[str, str]:
    relations = {
        "strategy_pkg.package": "r",
        "public.aistock_model_catalog": "r",
    }
    for spec in smoke.STACK_SPECS:
        relations.update({f"{spec.schema}.{table}": "r" for table in spec.tables})
        relations.update({f"{spec.schema}.{view}": "v" for view in spec.views})
    for spec in smoke.STACK_SPECS:
        for table, _column in spec.alter_columns:
            relations[f"{spec.schema}.{table}"] = "r"
    return relations


def _preflight_columns() -> dict[tuple[str, str], list[str]]:
    columns: dict[tuple[str, str], list[str]] = {}
    for spec in smoke.STACK_SPECS:
        for table, table_columns in smoke._expected_columns_for_spec(spec).items():
            columns[(spec.schema, table)] = list(table_columns)
    return columns


def _preflight_indexes() -> dict[str, set[tuple[str, str]]]:
    indexes: dict[str, set[tuple[str, str]]] = {}
    for spec in smoke.STACK_SPECS:
        for index, table in smoke._expected_index_tables_for_spec(spec).items():
            indexes.setdefault(spec.schema, set()).add((table, index))
    return indexes


def _preflight_constraints() -> dict[str, set[tuple[str, str]]]:
    constraints: dict[str, set[tuple[str, str]]] = {}
    for spec in smoke.STACK_SPECS:
        for constraint, table in smoke._expected_named_constraint_tables_for_spec(spec).items():
            constraints.setdefault(spec.schema, set()).add((table, constraint))
    return constraints


class ReadonlyPreflightCursor:
    def __init__(self, connection: "ReadonlyPreflightConnection") -> None:
        self.connection = connection
        self.last_sql = ""

    def __enter__(self) -> "ReadonlyPreflightCursor":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        first_keyword = sql.lstrip().split(None, 1)[0].upper()
        if first_keyword != "SELECT":
            raise AssertionError(f"production preflight executed non-readonly SQL: {sql}")
        forbidden = re.compile(r"\b(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|TRUNCATE|COMMENT|GRANT|REVOKE)\b", re.I)
        if forbidden.search(sql):
            raise AssertionError(f"production preflight executed write/DDL SQL: {sql}")
        self.last_sql = sql
        self.connection.executed_sql.append(sql)

    def fetchone(self) -> tuple[object, ...] | None:
        if "current_database()" in self.last_sql:
            return (self.connection.database_name,)
        if "inet_server_addr()" in self.last_sql:
            return (self.connection.server_addr,)
        if "inet_server_port()" in self.last_sql:
            return (self.connection.server_port,)
        if "FROM pg_class" in self.last_sql and "c.relkind" in self.last_sql:
            schema = _query_literal(self.last_sql, "n.nspname")
            relation_name = _query_literal(self.last_sql, "c.relname")
            relkind = self.connection.relations.get(f"{schema}.{relation_name}")
            return (relkind,) if relkind else None
        raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

    def fetchall(self) -> list[tuple[str, ...]]:
        if "information_schema.columns" in self.last_sql:
            schema = _query_literal(self.last_sql, "table_schema")
            table = _query_literal(self.last_sql, "table_name")
            return [(column,) for column in self.connection.columns.get((schema, table), [])]
        if "FROM pg_indexes" in self.last_sql:
            schema = _query_literal(self.last_sql, "schemaname")
            return sorted(self.connection.indexes.get(schema, set()))
        if "FROM pg_constraint" in self.last_sql:
            schema = _query_literal(self.last_sql, "n.nspname")
            return sorted(self.connection.constraints.get(schema, set()))
        raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")


class ReadonlyPreflightConnection:
    def __init__(self) -> None:
        self.autocommit = False
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0
        self.database_name = "aistock_prod"
        self.server_addr = "10.0.0.8"
        self.server_port = 5432
        self.executed_sql: list[str] = []
        self.relations = _preflight_relations()
        self.columns = _preflight_columns()
        self.indexes = _preflight_indexes()
        self.constraints = _preflight_constraints()

    def cursor(self) -> ReadonlyPreflightCursor:
        return ReadonlyPreflightCursor(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


def test_static_full_stack_smoke_passes_without_db_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default static governance smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    report = smoke.run_static_smoke()

    assert report.status == "passed"
    assert report.mode == "static_dry_run"
    assert report.db_target is None
    assert report.order == [spec.filename for spec in smoke.STACK_SPECS]
    assert set(report.checks) == {spec.filename for spec in smoke.STACK_SPECS}


def test_phase1a_apply_order_keeps_model_registry_last() -> None:
    ordered = smoke._specs_in_apply_order()

    assert [spec.filename for spec in ordered] == list(smoke.PHASE1A_APPLY_ORDER)
    assert ordered[0].filename == "strategy_pkg_package_asset_20260509.sql"
    assert ordered[-1].filename == "model_registry_phase5_20260509.sql"


def test_cli_default_json_is_static_and_lists_all_stack_files(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default CLI governance smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    assert smoke.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "passed"
    assert payload["mode"] == "static_dry_run"
    assert payload["db_target"] is None
    assert payload["order"] == [spec.filename for spec in smoke.STACK_SPECS]
    assert len(payload["files"]) == 6
    for spec in smoke.STACK_SPECS:
        assert str(spec.path) in payload["files"]
        assert payload["checks"][spec.filename]["schema"] == spec.schema


def test_static_smoke_validates_expected_objects_for_each_migration() -> None:
    report = smoke.run_static_smoke()

    model_registry = report.checks["model_registry_phase5_20260509.sql"]
    assert set(model_registry["tables"]) == {
        "model_template",
        "model_spec",
        "model_trial",
        "model_artifact",
        "model_lifecycle_event",
    }
    assert set(model_registry["views"]) == {
        "v_qe_selectable_model_spec",
        "v_model_catalog_compat",
        "v_legacy_aistock_model_catalog_bridge",
    }
    promotion = report.checks["strategy_pkg_promotion_review_20260509.sql"]
    assert set(promotion["tables"]) == {"promotion_review"}
    seed = report.checks["qe_phase4_master_seed_contract_20260509.sql"]
    assert set(seed["tables"]) == {"seed_fragility_score"}
    assert "package.seed_contract_sha256" in seed["alter_columns"]
    runtime = report.checks["strategy_pkg_runtime_variant_20260509.sql"]
    assert set(runtime["tables"]) == {"package_runtime_variant"}
    validation = report.checks["strategy_pkg_validation_run_20260509.sql"]
    assert set(validation["tables"]) == {"package_validation_run"}
    assert "strategy_pkg.package_runtime_variant" in validation["depends_on"]
    assets = report.checks["strategy_pkg_package_asset_20260509.sql"]
    assert set(assets["tables"]) == {"package_asset"}
    assert "package_asset.protected_asset" in assets["alter_columns"]


def test_static_smoke_rejects_missing_stack_file(tmp_path: Path) -> None:
    missing_spec = smoke.MigrationSpec(
        filename="missing_governance_migration.sql",
        phase="missing",
        schema="strategy_pkg",
    )

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="Unexpected migration order"):
        smoke.run_static_smoke((missing_spec,))


def test_spec_validator_rejects_destructive_sql() -> None:
    spec = smoke.STACK_SPECS[1]
    sql = spec.path.read_text(encoding="utf-8") + "\nDROP TABLE strategy_pkg.package;\n"

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="destructive SQL"):
        smoke._validate_spec(spec, sql)


def test_order_validator_rejects_stack_reordering() -> None:
    reordered = (smoke.STACK_SPECS[1], smoke.STACK_SPECS[0], *smoke.STACK_SPECS[2:])

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="Unexpected migration order"):
        smoke.run_static_smoke(reordered)


def test_db_transaction_check_requires_confirmation_and_env_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(smoke.ENV_DB_CHECK_ENABLED, raising=False)
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check="",
        confirm_apply="",
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock_dev", user="postgres")

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="confirm-db-check"):
        smoke._require_db_execution_safety(args, target)

    args.confirm_db_check = smoke.CONFIRM_DB_CHECK
    with pytest.raises(smoke.GovernanceMigrationSmokeError, match=smoke.ENV_DB_CHECK_ENABLED):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_refuses_production_like_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock", user="postgres")

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="production-like target"):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_allows_explicit_dev_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
    )
    target = smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres")

    smoke._require_db_execution_safety(args, target)


def test_production_readonly_preflight_requires_confirmation_and_env_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(confirm_production_readonly_preflight="")

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="confirm-production-readonly-preflight"):
        smoke._require_production_readonly_preflight_safety(args)

    args.confirm_production_readonly_preflight = smoke.CONFIRM_PRODUCTION_PREFLIGHT
    monkeypatch.delenv(smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED, raising=False)
    with pytest.raises(smoke.GovernanceMigrationSmokeError, match=smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED):
        smoke._require_production_readonly_preflight_safety(args)

    monkeypatch.setenv(smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED, "true")
    smoke._require_production_readonly_preflight_safety(args)


def test_cli_rejects_production_readonly_preflight_mutually_exclusive_modes() -> None:
    with pytest.raises(SystemExit, match="mutually exclusive"):
        smoke.main(["--production-readonly-preflight", "--apply"])

    with pytest.raises(SystemExit, match="mutually exclusive"):
        smoke.main(["--production-readonly-preflight", "--db-transaction-check"])


def test_cli_production_readonly_preflight_requires_guards_before_connect(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect_called = False

    def fail_connect(*_args: object, **_kwargs: object) -> object:
        nonlocal connect_called
        connect_called = True
        raise AssertionError("guard failure must happen before DB connect")

    monkeypatch.setattr(smoke, "_connect", fail_connect)
    monkeypatch.delenv(smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED, raising=False)

    assert smoke.main(["--production-readonly-preflight", "--json"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "production_readonly_preflight"
    assert "confirm-production-readonly-preflight" in payload["error"]
    assert connect_called is False

    assert (
        smoke.main(
            [
                "--production-readonly-preflight",
                "--confirm-production-readonly-preflight",
                smoke.CONFIRM_PRODUCTION_PREFLIGHT,
                "--json",
            ]
        )
        == 2
    )
    payload = json.loads(capsys.readouterr().out)
    assert smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED in payload["error"]
    assert connect_called is False


def test_cli_production_readonly_preflight_json_success(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = ReadonlyPreflightConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)
    monkeypatch.setenv(smoke.ENV_PRODUCTION_PREFLIGHT_ENABLED, "true")

    assert (
        smoke.main(
            [
                "--production-readonly-preflight",
                "--confirm-production-readonly-preflight",
                smoke.CONFIRM_PRODUCTION_PREFLIGHT,
                "--json",
                "--db-host",
                "10.0.0.1",
                "--db-port",
                "5432",
                "--db-name",
                "aistock_prod",
                "--db-user",
                "postgres",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["mode"] == "production_readonly_preflight"
    assert payload["db_target"] == "postgres@10.0.0.1:5432/aistock_prod"
    assert payload["checks"]["production_preflight"]["apply_needed"] is False
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 0
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in fake_conn.executed_sql)


def test_production_readonly_preflight_uses_readonly_sql_and_collects_catalog_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = ReadonlyPreflightConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)

    report = smoke.run_production_readonly_preflight(
        target=smoke.DbTarget(host="10.0.0.1", port=5432, dbname="aistock_prod", user="postgres")
    )

    assert report.status == "passed"
    assert report.mode == "production_readonly_preflight"
    assert report.db_target == "postgres@10.0.0.1:5432/aistock_prod"
    preflight = report.checks["production_preflight"]
    assert preflight["apply_needed"] is False
    assert preflight["missing_base_dependencies"] == []
    assert fake_conn.autocommit is True
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 0
    assert fake_conn.close_count == 1
    assert fake_conn.executed_sql
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in fake_conn.executed_sql)
    assert not any(re.search(r"\b(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|TRUNCATE|COMMENT)\b", sql, re.I) for sql in fake_conn.executed_sql)


def test_production_readonly_preflight_reports_missing_objects_and_base_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = ReadonlyPreflightConnection()
    fake_conn.relations.pop("public.aistock_model_catalog")
    fake_conn.relations.pop("model_registry.model_trial")
    fake_conn.relations["model_registry.v_model_catalog_compat"] = "r"
    fake_conn.indexes["strategy_pkg"].discard(("package_asset", "idx_package_asset_package_ref"))
    fake_conn.indexes["strategy_pkg"].add(("promotion_review", "idx_package_asset_package_ref"))
    fake_conn.constraints["strategy_pkg"].discard(("package_validation_run", "package_validation_status_check"))
    fake_conn.constraints["strategy_pkg"].add(("package_runtime_variant", "package_validation_status_check"))
    fake_conn.columns[("strategy_pkg", "package_asset")] = [
        column for column in fake_conn.columns[("strategy_pkg", "package_asset")] if column != "protected_asset"
    ]
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)

    report = smoke.run_production_readonly_preflight(
        target=smoke.DbTarget(host="10.0.0.1", port=5432, dbname="aistock_prod", user="postgres")
    )

    preflight = report.checks["production_preflight"]
    model_registry = preflight["specs"]["model_registry_phase5_20260509.sql"]
    asset_ledger = preflight["specs"]["strategy_pkg_package_asset_20260509.sql"]
    validation_runs = preflight["specs"]["strategy_pkg_validation_run_20260509.sql"]

    assert preflight["apply_needed"] is True
    assert preflight["missing_base_dependencies"] == ["public.aistock_model_catalog"]
    assert "model_registry.model_trial" in model_registry["missing_tables"]
    assert "model_registry.v_model_catalog_compat" in model_registry["missing_views"]
    assert any(item.startswith("model_registry.model_trial.") for item in model_registry["missing_columns"])
    assert "strategy_pkg.idx_package_asset_package_ref" in asset_ledger["missing_indexes"]
    assert "strategy_pkg.package_validation_status_check" in validation_runs["missing_named_constraints"]
    assert "strategy_pkg.package_asset.protected_asset" in asset_ledger["missing_columns"]


def test_db_transaction_check_rolls_back_and_never_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_files: list[str] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

        def execute(self, sql: str) -> None:
            self.last_sql = sql
            for spec in smoke.STACK_SPECS:
                if spec.path.read_text(encoding="utf-8") == sql:
                    executed_files.append(spec.filename)

        def fetchone(self) -> tuple[object, ...]:
            if "to_regclass('strategy_pkg.package')" in self.last_sql:
                return ("strategy_pkg.package", "public.aistock_model_catalog")
            if "FROM pg_class" in self.last_sql and "c.relkind" in self.last_sql:
                if "v_" in self.last_sql:
                    return ("v",)
                return ("r",)
            raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

        def fetchall(self) -> list[tuple[str]]:
            if "information_schema.columns" in self.last_sql:
                schema = _query_literal(self.last_sql, "table_schema")
                table = _query_literal(self.last_sql, "table_name")
                return [(column,) for column in _preflight_columns().get((schema, table), [])]
            if "FROM pg_indexes" in self.last_sql:
                schema = _query_literal(self.last_sql, "schemaname")
                return sorted(_preflight_indexes().get(schema, set()))
            if "FROM pg_constraint" in self.last_sql:
                schema = _query_literal(self.last_sql, "n.nspname")
                return sorted(_preflight_constraints().get(schema, set()))
            if "information_schema.tables" in self.last_sql and "model_registry" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    if spec.schema == "model_registry":
                        names.extend(spec.tables)
                return [(name,) for name in names]
            if "information_schema.tables" in self.last_sql and "strategy_pkg" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    if spec.schema == "strategy_pkg":
                        names.extend(spec.tables)
                return [(name,) for name in names]
            if "information_schema.views" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    names.extend(spec.views)
                return [(name,) for name in names]
            raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.commit_count = 0
            self.rollback_count = 0
            self.close_count = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commit_count += 1

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            self.close_count += 1

    fake_conn = FakeConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)

    report = smoke.run_db_execution(
        target=smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres"),
        apply=False,
    )

    assert report.mode == "db_transaction_check_rolled_back"
    assert report.order == list(smoke.PHASE1A_APPLY_ORDER)
    assert executed_files == list(smoke.PHASE1A_APPLY_ORDER)
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1
    assert fake_conn.close_count == 1


def test_apply_commits_per_file_in_phase1a_order(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_files: list[str] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

        def execute(self, sql: str) -> None:
            self.last_sql = sql
            for spec in smoke.STACK_SPECS:
                if spec.path.read_text(encoding="utf-8") == sql:
                    executed_files.append(spec.filename)

        def fetchone(self) -> tuple[object, ...]:
            if "to_regclass('strategy_pkg.package')" in self.last_sql:
                return ("strategy_pkg.package", "public.aistock_model_catalog")
            raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

        def fetchall(self) -> list[tuple[str]]:
            if "information_schema.columns" in self.last_sql:
                schema = _query_literal(self.last_sql, "table_schema")
                table = _query_literal(self.last_sql, "table_name")
                return [(column,) for column in _preflight_columns().get((schema, table), [])]
            if "FROM pg_class" in self.last_sql and "c.relkind" in self.last_sql:
                raise AssertionError("relation kind checks should use fetchone")
            if "FROM pg_indexes" in self.last_sql:
                schema = _query_literal(self.last_sql, "schemaname")
                return sorted(_preflight_indexes().get(schema, set()))
            if "FROM pg_constraint" in self.last_sql:
                schema = _query_literal(self.last_sql, "n.nspname")
                return sorted(_preflight_constraints().get(schema, set()))
            raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.commit_count = 0
            self.rollback_count = 0
            self.close_count = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commit_count += 1

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            self.close_count += 1

    relations = _preflight_relations()

    def relation_kind(cur: object, schema: str, relation_name: str) -> str | None:
        return relations.get(f"{schema}.{relation_name}")

    fake_conn = FakeConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)
    monkeypatch.setattr(smoke, "_relation_kind", relation_kind)

    report = smoke.run_db_execution(
        target=smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres"),
        apply=True,
    )

    assert report.mode == "apply"
    assert report.order == list(smoke.PHASE1A_APPLY_ORDER)
    assert report.checks["db_execution"]["transaction"] == "committed_per_file"
    assert report.checks["db_execution"]["applied_files"] == list(smoke.PHASE1A_APPLY_ORDER)
    assert executed_files == list(smoke.PHASE1A_APPLY_ORDER)
    assert fake_conn.commit_count == len(smoke.PHASE1A_APPLY_ORDER)
    assert fake_conn.rollback_count == 1
    assert fake_conn.close_count == 1


def test_apply_can_be_run_twice_with_idempotent_migrations(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_files_by_run: list[list[str]] = []

    class FakeCursor:
        def __init__(self, connection: "FakeConnection") -> None:
            self.connection = connection
            self.last_sql = ""

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

        def execute(self, sql: str) -> None:
            self.last_sql = sql
            for spec in smoke.STACK_SPECS:
                if spec.path.read_text(encoding="utf-8") == sql:
                    self.connection.current_run_files.append(spec.filename)

        def fetchone(self) -> tuple[object, ...]:
            if "to_regclass('strategy_pkg.package')" in self.last_sql:
                return ("strategy_pkg.package", "public.aistock_model_catalog")
            raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

        def fetchall(self) -> list[tuple[str]]:
            if "information_schema.columns" in self.last_sql:
                schema = _query_literal(self.last_sql, "table_schema")
                table = _query_literal(self.last_sql, "table_name")
                return [(column,) for column in _preflight_columns().get((schema, table), [])]
            if "FROM pg_indexes" in self.last_sql:
                schema = _query_literal(self.last_sql, "schemaname")
                return sorted(_preflight_indexes().get(schema, set()))
            if "FROM pg_constraint" in self.last_sql:
                schema = _query_literal(self.last_sql, "n.nspname")
                return sorted(_preflight_constraints().get(schema, set()))
            raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.commit_count = 0
            self.rollback_count = 0
            self.close_count = 0
            self.current_run_files: list[str] = []

        def cursor(self) -> FakeCursor:
            return FakeCursor(self)

        def commit(self) -> None:
            self.commit_count += 1

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            self.close_count += 1
            executed_files_by_run.append(list(self.current_run_files))
            self.current_run_files = []

    relations = _preflight_relations()

    def relation_kind(cur: object, schema: str, relation_name: str) -> str | None:
        return relations.get(f"{schema}.{relation_name}")

    connections: list[FakeConnection] = []

    def connect(_target: smoke.DbTarget) -> FakeConnection:
        connection = FakeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(smoke, "_connect", connect)
    monkeypatch.setattr(smoke, "_relation_kind", relation_kind)
    target = smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres")

    first_report = smoke.run_db_execution(target=target, apply=True)
    second_report = smoke.run_db_execution(target=target, apply=True)

    expected_order = list(smoke.PHASE1A_APPLY_ORDER)
    assert first_report.mode == "apply"
    assert second_report.mode == "apply"
    assert first_report.checks["db_execution"]["applied_files"] == expected_order
    assert second_report.checks["db_execution"]["applied_files"] == expected_order
    assert executed_files_by_run == [expected_order, expected_order]
    assert [connection.commit_count for connection in connections] == [len(expected_order), len(expected_order)]
    assert [connection.rollback_count for connection in connections] == [1, 1]
    assert [connection.close_count for connection in connections] == [1, 1]


def test_connect_wraps_driver_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg2

    def fail_connect(**_kwargs: object) -> object:
        raise RuntimeError("connection unavailable")

    monkeypatch.setattr(psycopg2, "connect", fail_connect)

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="failed to connect to DB target"):
        smoke._connect(smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres"))
