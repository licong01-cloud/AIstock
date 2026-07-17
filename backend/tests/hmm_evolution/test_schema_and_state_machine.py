from __future__ import annotations

from contextlib import contextmanager
import inspect

import pytest

from backend.db import init_hmm_evolution_schema as schema
from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.repository import (
    HMMEvolutionRepository,
    WRITABLE_RELATIONS,
)
from backend.services.hmm_evolution.worker import HMMEvolutionWorker, WorkerConfig


def test_schema_bootstrap_is_complete_commented_and_operator_controlled() -> None:
    ddl = list(schema.iter_ddl())
    joined = "\n".join(ddl).lower()
    expected_comment_count = 1 + len(schema.EXPECTED_COLUMNS) + sum(
        len(columns) for columns in schema.EXPECTED_COLUMNS.values()
    )

    assert schema.SCHEMA_VERSION == "hmm_evolution_v1"
    assert joined.count("comment on ") == expected_comment_count
    assert "create role" not in joined
    assert " grant " not in joined
    assert "paper_v2" not in joined
    assert "model_train_configs" not in joined
    assert "strategy_packages" not in joined
    for table in schema.EXPECTED_COLUMNS:
        assert f"create table if not exists hmm_evolution.{table}" in joined
    assert "if __name__ == \"__main__\"" in inspect.getsource(schema)


def test_schema_contract_exposes_all_required_constraints() -> None:
    assert set(schema.EXPECTED_CONSTRAINTS) == set(schema.EXPECTED_COLUMNS)
    assert "offline_evaluation_logical_generation_key" in schema.EXPECTED_CONSTRAINTS[
        "offline_evaluation"
    ]
    assert "batch_test_item_ordinal_key" in schema.EXPECTED_CONSTRAINTS["batch_test_item"]
    assert "candidate_source_type_ck" in schema.EXPECTED_CONSTRAINTS["candidate"]


def test_schema_bootstrap_uses_one_managed_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, bool]] = []
    executed: list[str] = []
    verified: list[object] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement: str) -> None:
            executed.append(statement)

    class _Connection:
        def cursor(self):
            return _Cursor()

    connection = _Connection()

    @contextmanager
    def _get_conn(**kwargs):
        calls.append(kwargs)
        yield connection

    monkeypatch.setattr(schema, "get_conn", _get_conn)
    monkeypatch.setattr(schema, "verify_schema", verified.append)

    schema.bootstrap_schema()

    assert calls == [{"autocommit": False, "manage_transaction": True}]
    assert executed == list(schema.iter_ddl())
    assert verified == [connection]


def test_repository_write_allowlist_is_exact_and_no_external_business_schema_is_written() -> None:
    assert WRITABLE_RELATIONS == {
        "hmm_evolution.candidate",
        "hmm_evolution.offline_evaluation",
        "hmm_evolution.batch_test_run",
        "hmm_evolution.batch_test_item",
    }
    source = inspect.getsource(HMMEvolutionRepository).lower()
    for forbidden in (
        "update model_train_configs",
        "update model_train_snapshots",
        "update strategy_packages",
        "insert into paper_v2",
        "update paper_v2",
        "delete from",
    ):
        assert forbidden not in source
    assert "for update skip locked" in source
    assert "fencing_token" in source
    assert "row_version" in source


@pytest.mark.parametrize(
    ("current", "counts", "expected"),
    [
        ("running", {"running": 1}, "running"),
        ("cancel_requested", {"running": 1}, "cancel_requested"),
        ("running", {"succeeded": 2}, "completed"),
        ("running", {"succeeded": 1, "failed": 1}, "partial_failed"),
        ("running", {"failed": 2}, "failed"),
        ("running", {"cancelled": 2}, "cancelled"),
        ("running", {"timed_out": 2}, "timed_out"),
    ],
)
def test_batch_state_derivation_is_explicit(current, counts, expected) -> None:
    assert HMMEvolutionRepository._derive_batch_status(current, counts) == expected


def test_source_aliases_preserve_first_provenance() -> None:
    primary = {"root_alias": "research", "relative_path": "one.json"}
    alias = {"snapshot_id": "snap-1", "artifact_name": "one.json"}
    merged = HMMEvolutionRepository._append_source_alias(primary, alias)

    assert merged["root_alias"] == "research"
    assert merged["aliases"] == [alias]
    assert HMMEvolutionRepository._append_source_alias(merged, alias) == merged


class _NoopRepository:
    def claim_evaluation(self, **kwargs):  # pragma: no cover - must not be called.
        raise AssertionError("disabled worker must not claim durable work")


def test_worker_is_disabled_by_default_and_has_no_silent_placeholder_execution() -> None:
    worker = HMMEvolutionWorker(_NoopRepository(), owner_id="test-worker")  # type: ignore[arg-type]
    with pytest.raises(InvalidSpecError, match="disabled"):
        worker.run_once()

    enabled_without_executor = HMMEvolutionWorker(
        _NoopRepository(),  # type: ignore[arg-type]
        owner_id="test-worker",
        config=WorkerConfig(runtime_mode="api_worker"),
    )
    with pytest.raises(InvalidSpecError, match="P1-B"):
        enabled_without_executor.run_once()
