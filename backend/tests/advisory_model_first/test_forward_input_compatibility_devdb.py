"""Formal publication with real DEV locks/writes inside an always-rolled-back outer transaction."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta
import json
import os
from types import SimpleNamespace

import psycopg2
from psycopg2.extras import Json
import pytest

from backend.services.advisory_forward.input_readiness import summaries
from backend.services.advisory_forward.repository import AdvisoryForwardPGRepository
from backend.services.advisory_forward.service import AdvisoryForwardService
from backend.services.advisory_program import AdvisoryProgramPGRepository, AdvisoryProgramService
from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
from backend.services.data_integrity import inputs
from backend.services.data_integrity.advisory_scope import resolve_scope
from backend.tests.advisory_model_first.test_forward_boundaries import _Programs
from backend.tests.e2e.test_advisory_binding_lifecycle_devdb import _Calendar, _conn_factory
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn

pytest_plugins = ("backend.tests.paper_trading_v2.fixtures_dev_db",)
pytestmark = pytest.mark.skipif(os.environ.get("AISTOCK_DEV_DB_E2E") != "1", reason="existing DEV DB only")

DECISION = date(2026, 8, 14)
TARGET = date(2026, 8, 17)


@contextmanager
def _publication_factory(conn):
    """Exercise repository-owned commit/rollback using a real PG savepoint, never persist fixture rows."""
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT test_publication")

    class Connection:
        autocommit = True
        finished = False

        def cursor(self, *args, **kwargs):
            return conn.cursor(*args, **kwargs)

        def commit(self):
            with conn.cursor() as cur:
                cur.execute("RELEASE SAVEPOINT test_publication")
            self.finished = True

        def rollback(self):
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT test_publication")
            self.commit()

    wrapped = Connection()
    try:
        yield wrapped
    except BaseException:
        if not wrapped.finished:
            wrapped.rollback()
        raise
    finally:
        if not wrapped.finished:
            wrapped.commit()


@pytest.mark.parametrize("change", [
    "unchanged", "rolling_compatible", "rolling_unproven", "rolling_wrong_prefix", "rolling_members", "rolling_partition", "pit_digest",
    "generation", "members", "partition", "package_manifest", "package_retired",
    "program_version", "program_disabled", "binding_retired", "write_failure",
])
def test_formal_publish_checks_frozen_inputs_once_and_is_atomic(dev_db_conn, monkeypatch, change):
    conn = dev_db_conn
    resolver = CanonicalPitAuthorityResolver(lambda: _conn_factory(conn))
    pit = resolver.resolve_live_binding()
    with conn.cursor() as cur:
        cur.execute("""SELECT package_id,manifest_sha256 FROM strategy_pkg.package
            WHERE package_status NOT IN ('DRAFT','RETIRED') ORDER BY package_id LIMIT 1""")
        package_id, manifest = cur.fetchone()
        cur.execute("""SELECT dataset,trade_date,data_revision FROM market.dataset_date_refresh_audit
            WHERE data_source='canonical_db' AND trade_date=%s ORDER BY dataset LIMIT 1""", (DECISION,))
        partition = cur.fetchone()
    assert partition, "existing DEV released partition required"
    dataset, fact_date, revision = partition
    scope, members = resolve_scope(conn, asdict(pit), DECISION)
    binding = inputs.ready_binding(dict(binding_id=package_id, manifest_sha256=manifest, data_date=str(DECISION),
        dependencies={"datasets": [dataset], "window_trading_days": 1}, input_scope=scope,
        partition_refs=[dict(dataset=dataset, fact_date=str(fact_date), data_revision=revision)],
        global_pit=json.loads(json.dumps(asdict(pit), default=str))))
    frozen = {package_id: binding}
    original = deepcopy(frozen)
    programs = AdvisoryProgramPGRepository(conn_factory=lambda: _conn_factory(conn))
    program = AdvisoryProgramService(repository=programs, calendar_provider=_Calendar()).create_program(
        program_name="test_int_publication_compatibility", package_mode="single_package",
        package_ids=[package_id], target_count=5, status="ENABLED",
        review_schedule={"frequency": "daily_after_close"},
    )
    active = programs.get_active_binding_version(program.program_id)
    repo = AdvisoryForwardPGRepository(conn_factory=lambda: _publication_factory(conn))
    final_checks, diagnostics, commits = [], [], []
    real_verify = inputs.verify_current
    real_commit = repo.commit_publication

    def verify(transaction, bindings, *, lock=False):
        final_checks.append(lock)
        assert transaction.autocommit is False
        real_verify(transaction, bindings, lock=lock)

    def commit(**kwargs):
        if change == "write_failure":
            # Fail at item serialization, after the review/list INSERTs.
            kwargs["items"][0].evidence_json["not_json_serializable"] = object()
        result = real_commit(**kwargs)
        commits.append(kwargs)
        return result

    monkeypatch.setattr(inputs, "verify_current", verify)
    monkeypatch.setattr(repo, "commit_publication", commit)

    class Readiness:
        def prepare(self, packages, day):
            assert packages == [package_id] and day == DECISION
            return frozen

        def recheck(self, bindings):
            diagnostics.append(deepcopy(bindings))
            real_verify(conn, bindings)

    class Programs(_Programs):
        def active_binding(self, _program_id):
            return {"binding_version_id": active.binding_version_id, "package_ids": [package_id]}

        def prepare_forward_selection(self, *args, **kwargs):
            result = super().prepare_forward_selection(*args, **kwargs)
            with conn.cursor() as cur:
                if change.startswith("rolling_") or change == "pit_digest":
                    proof = [] if change == "rolling_unproven" else [{
                        "source_digest": "0" * 64 if change == "rolling_wrong_prefix" else pit.state_source_digest,
                        "coverage_start": str(pit.coverage_start), "coverage_end": str(pit.coverage_end),
                    }]
                    cur.execute("""UPDATE market.stock_universe_pit_state SET end_date=%s,
                        source_fingerprint_sha256=%s,
                        last_build_summary=jsonb_set(COALESCE(last_build_summary,'{}'),'{rolling_predecessors}',%s)
                        WHERE universe_key=%s""", (
                        pit.coverage_end + timedelta(days=1) if change.startswith("rolling_") else pit.coverage_end,
                        "f" * 64, Json(proof if change != "pit_digest" else []), pit.universe_key))
                elif change == "generation":
                    cur.execute("""UPDATE market.stock_universe_pit_authority_pointer
                        SET activation_generation=activation_generation+1,
                            activation_envelope_digest=%s,expected_source_commit=%s
                        WHERE authority_id=%s""", ("d" * 64, "test_int_generation_change", pit.authority_id))
                if change in {"members", "rolling_members"}:
                    cur.execute("DELETE FROM market.stock_universe_pit_spans WHERE universe_key=%s AND ts_code=%s",
                                (pit.universe_key, sorted(members)[0]))
                elif change in {"partition", "rolling_partition"}:
                    cur.execute("""UPDATE market.dataset_date_refresh_audit SET data_revision=data_revision+1
                        WHERE data_source='canonical_db' AND dataset=%s AND trade_date=%s""", (dataset, fact_date))
                elif change == "package_manifest":
                    cur.execute("UPDATE strategy_pkg.package SET manifest_sha256=%s WHERE package_id=%s", ("e"*64, package_id))
                elif change == "package_retired":
                    cur.execute("UPDATE strategy_pkg.package SET package_status='RETIRED' WHERE package_id=%s", (package_id,))
                elif change == "program_version":
                    cur.execute("UPDATE app.advisory_program SET version=version+1 WHERE program_id=%s", (program.program_id,))
                elif change == "program_disabled":
                    cur.execute("UPDATE app.advisory_program SET status='PAUSED' WHERE program_id=%s", (program.program_id,))
                elif change == "binding_retired":
                    cur.execute("""UPDATE app.advisory_strategy_binding_version SET activation_status='RETIRED'
                        WHERE binding_version_id=%s""", (active.binding_version_id,))
            return program, active, result[2], result[3]

    facade = Programs(program)
    facade.repository = programs
    service = AdvisoryForwardService(repository=repo, program_service=facade,
        input_readiness=Readiness(), model_service=SimpleNamespace(model_root=lambda: ""))
    succeeds = change in {"unchanged", "rolling_compatible"}
    if succeeds:
        result = service._publish(program.program_id, decision_date=DECISION, target_date=TARGET)
        assert result["status"] == "PUBLISHED" and final_checks == [True] and diagnostics == []
        first = repo.get(result["forward_run_id"])["forward_run"]
        assert first["run_payload_json"]["input_readiness"] == summaries(original)
        # Exact publication retry and formal entry retry both reuse the fact without input checks or inserts.
        again = real_commit(**commits[0])
        assert again["publication_payload_sha256"] == first["publication_payload_sha256"]
        service._resume_published_observation = lambda saved: {"status": "REUSED"}
        service.input_readiness.prepare = lambda *_: pytest.fail("published retry must not capture again")
        assert service._publish(program.program_id, decision_date=DECISION, target_date=TARGET)["status"] == "REUSED"
        assert final_checks == [True]
        with conn.cursor() as cur:
            cur.execute("SELECT runtime_config_json FROM app.advisory_review_run WHERE program_id=%s", (program.program_id,))
            assert cur.fetchone()[0]["selection_artifact_config"]["input_readiness_by_package"] == original
    else:
        from backend.services.trading_core.errors import InvalidStateTransitionError
        binding_change = change in {"program_version", "program_disabled", "binding_retired"}
        error = TypeError if change == "write_failure" else InvalidStateTransitionError if binding_change else inputs.InputReadinessError
        with pytest.raises(error):
            service._publish(program.program_id, decision_date=DECISION, target_date=TARGET)
        assert final_checks == ([] if binding_change else [True])
        assert len(diagnostics) == int(binding_change or change == "write_failure")
    assert frozen == original
    with conn.cursor() as cur:
        for table in ("advisory_review_run", "advisory_recommendation_list_version", "advisory_recommendation_list_item"):
            cur.execute(f"SELECT count(*) FROM app.{table} WHERE program_id=%s", (program.program_id,))
            assert cur.fetchone()[0] == int(succeeds)
        cur.execute("SELECT publication_status FROM app.advisory_forward_run WHERE program_id=%s", (program.program_id,))
        assert cur.fetchone()[0] == ("PUBLISHED" if succeeds else "WAITING_DATA" if change not in {
            "program_version", "program_disabled", "binding_retired", "write_failure"} else "FAILED")


def test_final_input_locks_block_concurrent_revision(dev_db_conn):
    """Actual second session must wait on authority, package and consumed partition locks."""
    conn = dev_db_conn
    pit = CanonicalPitAuthorityResolver(lambda: _conn_factory(conn)).resolve_live_binding()
    with conn.cursor() as cur:
        cur.execute("SELECT package_id,manifest_sha256 FROM strategy_pkg.package WHERE package_status='BACKTEST_APPROVED' LIMIT 1")
        package, manifest = cur.fetchone()
        cur.execute("""SELECT dataset,trade_date,data_revision FROM market.dataset_date_refresh_audit
            WHERE data_source='canonical_db' AND trade_date=%s ORDER BY dataset LIMIT 1""", (DECISION,))
        dataset, day, revision = cur.fetchone()
    scope, _ = resolve_scope(conn, asdict(pit), DECISION)
    binding = inputs.ready_binding(dict(binding_id=package, manifest_sha256=manifest, data_date=str(DECISION),
        global_pit=json.loads(json.dumps(asdict(pit), default=str)), input_scope=scope,
        dependencies={"datasets": [dataset], "window_trading_days": 1},
        partition_refs=[dict(dataset=dataset, fact_date=str(day), data_revision=revision)]))
    inputs.verify_current(conn, {package: binding}, lock=True)
    other = psycopg2.connect(**_dev_dsn())
    try:
        for sql, params in [
            ("UPDATE market.stock_universe_pit_authority_pointer SET activation_generation=activation_generation WHERE authority_id=%s", (pit.authority_id,)),
            ("UPDATE market.stock_universe_pit_state SET dirty=dirty WHERE universe_key=%s", (pit.universe_key,)),
            ("UPDATE strategy_pkg.package SET manifest_sha256=manifest_sha256 WHERE package_id=%s", (package,)),
            ("""UPDATE market.dataset_date_refresh_audit SET data_revision=data_revision
                WHERE data_source='canonical_db' AND dataset=%s AND trade_date=%s""", (dataset, day)),
        ]:
            with other.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    cur.execute(sql, params)
            other.rollback()
    finally:
        other.rollback()
        other.close()
