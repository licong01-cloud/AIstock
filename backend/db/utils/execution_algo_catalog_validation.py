"""Transactional helpers for execution algorithm catalog migrations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.execution_algo_catalog.validators import (
    CatalogAlgoValidationResult,
    RuntimeAssetResolver,
    validate_enabled_default_config_model_paths_from_db,
)


def run_execution_algo_catalog_migration_with_validation(
    connection: Any,
    *,
    migration_sql: str | None = None,
    migration_path: Path | str | None = None,
    resolver: RuntimeAssetResolver | None = None,
    copy_missing: bool = False,
) -> list[CatalogAlgoValidationResult]:
    """Apply catalog SQL and validate enabled model assets before commit.

    The helper is intentionally DB-API shaped so deployment scripts can wrap an
    execution_algorithm_catalog INSERT/UPDATE migration without hiding missing
    model artifacts. Validation uses copy_missing=False by default; callers must
    opt in to copying explicitly.
    """

    sql = _load_migration_sql(migration_sql=migration_sql, migration_path=migration_path)
    try:
        if sql:
            cursor = connection.cursor()
            cursor.execute(sql)
        results = validate_enabled_default_config_model_paths_from_db(
            connection,
            resolver=resolver,
            copy_missing=copy_missing,
        )
    except Exception:
        _call_if_present(connection, "rollback")
        raise

    _call_if_present(connection, "commit")
    return results


def _load_migration_sql(*, migration_sql: str | None, migration_path: Path | str | None) -> str:
    if migration_sql is not None and migration_path is not None:
        raise ValueError("provide either migration_sql or migration_path, not both")
    if migration_path is not None:
        return Path(migration_path).read_text(encoding="utf-8")
    return migration_sql or ""


def _call_if_present(target: Any, method_name: str) -> None:
    method = getattr(target, method_name, None)
    if callable(method):
        method()
