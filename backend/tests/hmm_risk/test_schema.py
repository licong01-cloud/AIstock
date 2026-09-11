from __future__ import annotations

from copy import deepcopy
from pathlib import Path

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
    assert "create table if not exists hmm_risk.rotation_l1_prediction" in ddl
    assert "rotation_score>'-infinity'::double precision" in ddl
    assert "isfinite(rotation_score)" not in ddl
    assert "advisory_status<>'available'" in ddl
    assert "research_surface_status='not_available'" in ddl
    assert "research_surface_status in ('not_available','available_experimental')" not in ddl
    assert "unique (model_hash,trade_date,sector_code,revision)" in ddl
    assert "prediction_id uuid" in ddl
    assert "supersedes_prediction_id uuid" in ddl
    assert "(trade_date,sector_code,revision desc)" in ddl
    assert "development_oof_rank_ic is not null" in ddl
    assert "development_oof_rank_ic_hac_lower is not null" in ddl
    assert "development_oof_rank_ic_hac_upper is not null" in ddl
    assert "jsonb_array_length(feature_contributions) in (10,11)" in ddl
    assert "jsonb_array_length(feature_contributions)=10" not in ddl
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


class _RotationSchemaCursor:
    def __init__(self, *, drift: bool = False, contribution_drift: bool = False) -> None:
        self.step = 0
        self.drift = drift
        self.contribution_drift = contribution_drift

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _statement, _values) -> None:
        self.step += 1

    def fetchall(self):
        if self.step == 1:
            rows = []
            for name in schema.ROTATION_L1_PREDICTION_COLUMNS:
                sql_type, not_null, default = schema.ROTATION_L1_PREDICTION_COLUMN_CONTRACT[name]
                comment = f"rotation_l1_prediction.{name} exact hmm_risk_rotation_l1_prediction_v1 contract"
                rows.append((name, sql_type, not_null, default, comment))
            if self.drift:
                rows[-1] = (*rows[-1][:-1], "old")
            return rows
        if self.step == 2:
            rows = [
                (
                    name,
                    " ".join(schema.ROTATION_L1_PREDICTION_CONSTRAINT_TOKENS[name]),
                    f"{name} enforces hmm_risk_rotation_l1_prediction_v1",
                )
                for name in sorted(schema.ROTATION_L1_PREDICTION_CONSTRAINTS)
            ]
            dimension_fragment = (
                "jsonb_array_length(feature_contributions) = 10"
                if self.contribution_drift
                else "jsonb_array_length(feature_contributions) = ANY (ARRAY[10, 11])"
            )
            return [
                (name, f"{definition} {dimension_fragment}", comment)
                if name == "ck_hmm_risk_rotation_l1_prediction_availability"
                else (name, definition, comment)
                for name, definition, comment in rows
            ]
        raise AssertionError(self.step)

    def fetchone(self):
        if self.step == 3:
            return ("Append-only G2-A L1 rotation prediction revisions; scores are not probabilities.",)
        if self.step == 4:
            return (
                "CREATE INDEX idx_hmm_risk_rotation_l1_lookup ON hmm_risk.rotation_l1_prediction "
                "USING btree (trade_date, sector_code, revision DESC)",
                "Date and sector L1 rotation revision lookup; model identity remains explicit.",
            )
        raise AssertionError(self.step)


class _RotationSchemaConnection:
    def __init__(self, *, drift: bool = False, contribution_drift: bool = False) -> None:
        self.drift = drift
        self.contribution_drift = contribution_drift

    def cursor(self):
        return _RotationSchemaCursor(drift=self.drift, contribution_drift=self.contribution_drift)


def test_rotation_l1_prediction_schema_verifier_accepts_exact_contract_and_rejects_drift() -> None:
    schema.verify_rotation_l1_prediction_schema(_RotationSchemaConnection())

    with pytest.raises(RuntimeError, match="column comments"):
        schema.verify_rotation_l1_prediction_schema(_RotationSchemaConnection(drift=True))

    with pytest.raises(RuntimeError, match="contribution dimensions"):
        schema.verify_rotation_l1_prediction_schema(_RotationSchemaConnection(contribution_drift=True))


def test_rotation_l1_prediction_contribution_migration_is_guarded_and_reversible() -> None:
    migration_root = Path(schema.__file__).parent / "migrations"
    apply_sql = (
        (migration_root / "alter_hmm_risk_rotation_l1_prediction_contributions_20260911.sql")
        .read_text(encoding="utf-8")
        .lower()
    )
    rollback_sql = (
        (migration_root / "alter_hmm_risk_rotation_l1_prediction_contributions_20260911.rollback.sql")
        .read_text(encoding="utf-8")
        .lower()
    )

    assert "lock table hmm_risk.rotation_l1_prediction in share row exclusive mode" in apply_sql
    assert "unexpected contribution dimensions" in apply_sql
    assert "stored contribution dimensions invalid" in apply_sql
    assert "jsonb_array_length(feature_contributions) in (10,11)" in apply_sql
    assert "comment on constraint ck_hmm_risk_rotation_l1_prediction_availability" in apply_sql
    assert "unexpected contribution dimensions" in rollback_sql
    assert "non-v1.3 contribution dimensions exist" in rollback_sql
    assert "jsonb_array_length(feature_contributions)=10" in rollback_sql
