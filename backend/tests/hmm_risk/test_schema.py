from __future__ import annotations

from copy import deepcopy

import pytest

from backend.db import init_hmm_risk_schema as schema


def _valid_contract(monkeypatch: pytest.MonkeyPatch) -> dict:
    tables = {}
    for table, names in schema.EXPECTED_COLUMNS.items():
        tables[table] = {
            "comment": schema.TABLE_COMMENTS[table],
            "columns": [
                {
                    "ordinal": ordinal,
                    "name": name,
                    "type": "text",
                    "not_null": True,
                    "default": None,
                    "comment": f"{table}.{name} exact {schema.SCHEMA_VERSION} contract",
                }
                for ordinal, name in enumerate(names, start=1)
            ],
            "constraints": [
                {
                    "name": name,
                    "type": "c",
                    "definition": f"CHECK ({name!r} IS NOT NULL)",
                    "comment": f"{name} enforces {schema.SCHEMA_VERSION}",
                }
                for name in sorted(schema.EXPECTED_CONSTRAINTS[table])
            ],
        }
    contract = {
        "schema_comment": schema.SCHEMA_COMMENT,
        "tables": tables,
        "indexes": [
            {
                "name": name,
                "definition": f"CREATE INDEX {name} ON hmm_risk.example (id)",
                "comment": schema.INDEX_COMMENTS[name],
            }
            for name in sorted(schema.EXPECTED_INDEXES)
        ],
        "views": [
            {
                "name": name,
                "definition": f"SELECT value FROM hmm_risk.{name}_source",
                "comment": schema.VIEW_COMMENTS[name],
            }
            for name in sorted(schema.EXPECTED_VIEWS)
        ],
    }
    monkeypatch.setattr(
        schema,
        "EXPECTED_STRUCTURE_SHA256",
        schema._canonical_sha256(schema._structure_payload(contract)),
    )
    return contract


def test_schema_ddl_contains_all_tables_views_comments_and_no_unsupported_json_function() -> None:
    ddl = "\n".join(schema.iter_ddl()).lower()

    for table in schema.EXPECTED_COLUMNS:
        assert f"create table if not exists hmm_risk.{table}" in ddl
    for view in schema.EXPECTED_VIEWS:
        assert f"create or replace view hmm_risk.{view}" in ddl
    assert "jsonb_object_length" not in ddl
    assert "state_origin='direct_hmm'" in ddl
    assert "state_probabilities - 'trending' - 'neutral' - 'fading' = '{}'::jsonb" in ddl
    assert "missing_evidence jsonb not null default" not in ddl
    assert "failed_count=0 and jsonb_array_length(missing_evidence)=0" in ddl
    assert "failed_count>0 and jsonb_array_length(missing_evidence)>0" in ddl
    assert "select *" not in ddl


def test_exact_contract_snapshot_accepts_only_the_frozen_structure(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = _valid_contract(monkeypatch)

    schema.verify_contract_snapshot(contract)

    drifted = deepcopy(contract)
    drifted["tables"]["daily_alert"]["columns"][0]["type"] = "uuid"
    with pytest.raises(RuntimeError, match="structure hash"):
        schema.verify_contract_snapshot(drifted)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(schema_comment="old"), "schema version/comment"),
        (
            lambda value: value["tables"]["daily_alert"]["columns"].pop(),
            "columns daily_alert",
        ),
        (
            lambda value: value["tables"]["risk_event"]["constraints"].pop(),
            "constraints risk_event",
        ),
        (lambda value: value["indexes"].pop(), "indexes"),
        (lambda value: value["views"].pop(), "views"),
    ],
)
def test_exact_contract_snapshot_rejects_named_drift(
    monkeypatch: pytest.MonkeyPatch,
    mutate,
    message: str,
) -> None:
    contract = _valid_contract(monkeypatch)
    mutate(contract)

    with pytest.raises(RuntimeError, match=message):
        schema.verify_contract_snapshot(contract)


def test_contract_snapshot_rejects_wildcard_view(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = _valid_contract(monkeypatch)
    contract["views"][0]["definition"] = "SELECT * FROM hmm_risk.hidden_source"

    with pytest.raises(RuntimeError, match="view wildcard"):
        schema.verify_contract_snapshot(contract)


class _Connection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.verified = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return self

    def execute(self, statement: str) -> None:
        self.executed.append(statement)


def test_bootstrap_executes_every_statement_then_verifies(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _Connection()

    def verify(conn) -> None:
        assert conn is connection
        connection.verified = True

    monkeypatch.setattr(schema, "verify_schema", verify)
    schema.bootstrap_schema(lambda: connection)

    assert connection.executed == list(schema.iter_ddl())
    assert connection.verified is True
