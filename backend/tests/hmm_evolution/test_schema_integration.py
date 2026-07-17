from __future__ import annotations

from copy import deepcopy

import pytest

from backend.db import init_hmm_evolution_schema as schema


class _SchemaCursor:
    def __init__(self) -> None:
        self.columns = {
            table: [tuple(row) for row in rows]
            for table, rows in schema.EXPECTED_COLUMN_CONTRACTS.items()
        }
        self.constraints = deepcopy(schema.EXPECTED_CONSTRAINT_DEFINITIONS)
        self.indexes = deepcopy(schema.EXPECTED_INDEX_DEFINITIONS)
        self.schema_comment = schema.SCHEMA_COMMENT
        self.table_comments = dict(schema.TABLE_COMMENTS)
        self.column_comments = {
            table: dict(comments) for table, comments in schema.COLUMN_COMMENTS.items()
        }
        self._one = None
        self._all = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query: str, params=None) -> None:
        normalized = " ".join(query.split())
        table = params[1] if params and len(params) > 1 else None
        self._one = None
        self._all = []
        if "obj_description(n.oid, 'pg_namespace')" in normalized:
            self._one = (self.schema_comment,)
        elif "FROM information_schema.columns" in normalized:
            self._all = self.columns[table]
        elif "pg_get_indexdef" in normalized:
            self._all = sorted(self.indexes[table].items())
        elif "FROM pg_constraint c" in normalized:
            self._all = sorted(self.constraints[table].items())
        elif "obj_description(c.oid, 'pg_class')" in normalized:
            self._one = (self.table_comments[table],)
        elif "col_description" in normalized:
            self._all = list(self.column_comments[table].items())
        else:  # pragma: no cover - catches verifier query drift.
            raise AssertionError(f"unexpected schema verification SQL: {normalized}")

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _Connection:
    def __init__(self, cursor: _SchemaCursor) -> None:
        self.cursor_instance = cursor

    def cursor(self):
        return self.cursor_instance


def test_verify_schema_accepts_complete_structural_contract() -> None:
    schema.verify_schema(_Connection(_SchemaCursor()))


@pytest.mark.parametrize(
    ("declared", "postgres_readback"),
    [
        (
            "CHECK (label_horizon_days BETWEEN 1 AND 30)",
            "CHECK ((label_horizon_days >= 1) AND (label_horizon_days <= 30))",
        ),
        (
            "CHECK (primary_coverage_ratio IS NULL OR primary_coverage_ratio BETWEEN 0 AND 1)",
            "CHECK (((primary_coverage_ratio IS NULL) OR "
            "((primary_coverage_ratio >= (0)::double precision) AND "
            "(primary_coverage_ratio <= (1)::double precision))))",
        ),
    ],
)
def test_constraint_normalization_accepts_postgresql_between_rewrite(
    declared: str,
    postgres_readback: str,
) -> None:
    assert schema._normalize_sql_definition(declared) == schema._normalize_sql_definition(  # noqa: SLF001
        postgres_readback
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("column", "column drift"),
        ("constraint", "constraint drift"),
        ("index", "index drift"),
        ("schema_comment", "schema comment drift"),
        ("table_comment", "table comment drift"),
        ("column_comment", "column comment drift"),
    ],
)
def test_verify_schema_rejects_every_supported_drift_dimension(
    mutation: str,
    message: str,
) -> None:
    cursor = _SchemaCursor()
    if mutation == "column":
        row = list(cursor.columns["candidate"][0])
        row[1] = "integer"
        cursor.columns["candidate"][0] = tuple(row)
    elif mutation == "constraint":
        cursor.constraints["candidate"]["candidate_row_version_ck"] = (
            "CHECK (row_version >= 0)"
        )
    elif mutation == "index":
        cursor.indexes["candidate"]["candidate_lifecycle_created_idx"] = (
            "CREATE INDEX candidate_lifecycle_created_idx ON hmm_evolution.candidate "
            "USING btree (created_at DESC)"
        )
    elif mutation == "schema_comment":
        cursor.schema_comment = "drifted"
    elif mutation == "table_comment":
        cursor.table_comments["candidate"] = "drifted"
    else:
        cursor.column_comments["candidate"]["candidate_id"] = "drifted"

    with pytest.raises(RuntimeError, match=message):
        schema.verify_schema(_Connection(cursor))
