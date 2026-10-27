"""Opt-in contracts against the EXISTING DEV PostgreSQL, never a new database.

Operator applies add_daily_data_integrity_v1.sql first. Tests use unique exact
ledger keys and a temporary table; no production/configuration/market writes.
"""
from contextlib import contextmanager
from datetime import date
import os
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from backend.services.data_integrity.contracts import CheckCounts, PartitionState, ValidationResult
from backend.services.data_integrity.repository import IntegrityRepository, IntegrityRevisionChanged, mark_changed

pytestmark = pytest.mark.skipif(os.environ.get("AISTOCK_INTEGRITY_DEV_TEST") != "1", reason="explicit existing DEV opt-in")


@pytest.fixture
def db():
    from scripts.prepare_canonical_pit_monthly import _load_database_config

    config = _load_database_config("dev", Path(os.environ["AISTOCK_INTEGRITY_ENV_FILE"]))
    assert config.host in {"127.0.0.1", "localhost"} and config.port == 5433 and config.dbname == "aistock_dev"

    @contextmanager
    def connect():
        conn = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
            user=config.user, password=config.password, application_name="daily-integrity-DEV-test",
            options="-c statement_timeout=10000 -c lock_timeout=3000")
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()

    dataset = "integrity_test_" + uuid4().hex
    repo = IntegrityRepository(connect)
    try:
        yield dataset, repo
    finally:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market.data_sync_attempts WHERE target_id IN (SELECT target_id FROM market.data_sync_targets WHERE dataset=%s)", (dataset,))
            cur.execute("DELETE FROM market.data_sync_targets WHERE dataset=%s", (dataset,))
            cur.execute("DELETE FROM market.dataset_date_refresh_audit WHERE dataset=%s", (dataset,))


def changed(repo, dataset, day):
    with repo.transaction() as conn:
        return mark_changed(conn, dataset, [day], reason="DEV fault contract", source="dev_test")[day]


def result():
    return ValidationResult(PartitionState.VALID, (CheckCounts("row", 1, 1, 0, 0, 0, 0),), {"source": "DEV fixture"})


def test_actual_nested_readers_and_revision_use_one_short_dev_snapshot(db, monkeypatch):
    from backend.db import pg_pool
    from backend.services.data_integrity.inputs import capture_inputs

    ds, repo = db
    day = date(2026, 9, 1)
    changed(repo, ds, day)
    with repo._connect() as connection:
        class Pool:
            def getconn(self):
                return connection

            def putconn(self, conn):
                assert conn is connection

        monkeypatch.setattr(pg_pool, "_DB_POOL", Pool())
        with capture_inputs(day, day, datasets=[ds]) as captured:
            with pg_pool.get_conn() as first, pg_pool.get_conn() as second:
                assert first is second and first._connection is connection
                with first.cursor() as cur:
                    cur.execute("SHOW transaction_read_only")
                    assert cur.fetchone()[0] == "on"
                with pytest.raises(RuntimeError, match="ownership"):
                    first.commit()
            assert captured["partition_refs"][0]["data_revision"] == 1
            changed(repo, ds, day)  # Independent writer commits during capture.
            with pg_pool.get_conn() as second, second.cursor() as cur:
                cur.execute("SELECT data_revision FROM market.dataset_date_refresh_audit WHERE dataset=%s AND data_source='canonical_db'", (ds,))
                assert cur.fetchone()[0] == 1
            with pytest.raises(RuntimeError, match="ownership"):
                with pg_pool.get_conn(autocommit=False, manage_transaction=True):
                    pass
        # Transaction has ended before any model work; next capture sees R2.
        with capture_inputs(day, day, datasets=[ds]) as captured:
            assert captured["partition_refs"][0]["data_revision"] == 2


def publish(repo, ds, d, revision, claim):
    return repo.publish(dataset=ds, day=d, revision=revision, scope_version="s1", rule_version="r1",
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result())


def test_atomic_rollback_and_historical_date_rearm(db):
    ds, repo = db
    d = date(2026, 9, 1)
    with pytest.raises(RuntimeError, match="injected"):
        with repo.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE integrity_facts (value int) ON COMMIT DROP")
                cur.execute("INSERT INTO integrity_facts VALUES (1)")
            mark_changed(conn, ds, [d], reason="test", source="dev")
            raise RuntimeError("injected after fact and marker")
    with repo.transaction(readonly=True) as conn:
        assert repo.partitions(conn, d, d) == []
    assert changed(repo, ds, d) == 1
    claim = repo.claim(ds, d)
    assert repo.claim(ds, d) is None  # shared scheduler cannot claim twice
    first = publish(repo, ds, d, 1, claim)
    assert first["validated_revision"] == 1
    with repo.transaction(readonly=True) as conn, conn.cursor() as cur:
        cur.execute("""SELECT t.attempt_count,t.last_attempt_status,a.context_json->>'operation',
            a.finished_at>=a.started_at FROM market.data_sync_targets t
            JOIN market.data_sync_attempts a ON a.attempt_id=t.last_attempt_id WHERE t.target_id=%s""",
            (claim["target_id"],))
        assert cur.fetchone() == (1, "reconciled", "VALIDATE_ONLY", True)
    changed(repo, ds, date(2026, 9, 30))
    assert changed(repo, ds, d) == 2  # latest fact date unchanged
    claim2 = repo.claim(ds, d)
    assert claim2["metadata"]["desired_revision"] == 2
    assert claim2["target_id"] == claim["target_id"]  # not an unbounded revision queue
    with pytest.raises(IntegrityRevisionChanged):
        publish(repo, ds, d, 1, claim)
    second = publish(repo, ds, d, 2, claim2)
    assert second["data_revision"] == second["validated_revision"] == 2


def test_expired_worker_cannot_publish_even_same_revision(db):
    ds, repo = db
    d = date(2026, 9, 2)
    changed(repo, ds, d)
    old = repo.claim(ds, d)
    with repo.transaction() as conn, conn.cursor() as cur:
        cur.execute("""UPDATE market.data_sync_targets SET next_retry_at=clock_timestamp()-interval '1 second',
            metadata=metadata||jsonb_build_object('lease_until',clock_timestamp()-interval '1 second') WHERE target_id=%s""", (old["target_id"],))
    new = repo.claim(ds, d)
    assert new["metadata"]["lease_token"] != old["metadata"]["lease_token"]
    with pytest.raises(IntegrityRevisionChanged):
        publish(repo, ds, d, 1, old)
    assert not repo.heartbeat(old["target_id"], old["metadata"]["lease_token"])
    assert publish(repo, ds, d, 1, new)["status"] == "success"


def test_rule_upgrade_fences_old_claim_even_without_new_facts(db, monkeypatch):
    from backend.services.data_integrity import catalog
    from backend.services.data_integrity.repository import enqueue_validation
    ds, repo = db
    day = date(2026, 9, 1)
    changed(repo, ds, day)
    with repo.transaction() as conn:
        enqueue_validation(conn, ds, day)
    old = repo.claim(ds, day)
    old_rule = catalog.RULE_VERSION
    monkeypatch.setattr(catalog, "RULE_VERSION", "DEV_rule_upgrade")
    with repo.transaction() as conn:
        enqueue_validation(conn, ds, day)
    new = repo.claim(ds, day)
    assert new and new["target_id"] == old["target_id"]
    with pytest.raises(IntegrityRevisionChanged):
        repo.publish(dataset=ds, day=day, revision=1, scope_version=catalog.SCOPE_VERSION,
            rule_version=old_rule, target_id=old["target_id"], token=old["metadata"]["lease_token"], result=result())
    readback = repo.publish(dataset=ds, day=day, revision=1, scope_version=catalog.SCOPE_VERSION,
        rule_version=catalog.RULE_VERSION, target_id=new["target_id"], token=new["metadata"]["lease_token"], result=result())
    assert readback["rule_version"] == "DEV_rule_upgrade"


def test_fetch_retry_cannot_be_postponed_by_repeated_validation(db):
    from datetime import datetime, timedelta, timezone
    from backend.services.data_sync_targets import DataSyncTargetRecord, DataSyncTargetRepository

    ds, repo = db
    due = datetime.now(timezone.utc)-timedelta(minutes=1)
    targets = DataSyncTargetRepository(repo._connect)
    args = dict(dataset=ds, data_source="integrity_repair", target_date=date(2026, 9, 1),
                target_scope={"operation": "FETCH", "scope": "exact_fact_date"}, target_status="retry")
    first = targets.upsert_target(DataSyncTargetRecord(**args, next_retry_at=due))
    again = targets.upsert_target(DataSyncTargetRecord(**args, next_retry_at=due+timedelta(hours=1)))
    assert first["target_id"] == again["target_id"]
    assert again["next_retry_at"] == due


def test_minute_batches_invalidate_immediately_but_coalesce_physical_checks(db):
    _ds, repo = db
    day = date(2099, 1, 2)
    # This exact synthetic key must not touch an existing ledger row. Roll back
    # all writes even on assertion failure; never insert any price facts.
    with pytest.raises(RuntimeError, match="rollback synthetic minute target"):
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM market.dataset_date_refresh_audit WHERE dataset='kline_minute_raw' AND trade_date=%s", (day,))
            assert cur.fetchone() is None
            mark_changed(conn, "kline_minute_raw", [day], reason="DEV debounce", source="DEV")
            cur.execute("""SELECT a.validation_status,a.data_revision,
                EXTRACT(EPOCH FROM t.next_retry_at-clock_timestamp())
                FROM market.dataset_date_refresh_audit a JOIN market.data_sync_targets t
                ON t.dataset=a.dataset AND t.target_date=a.trade_date AND t.data_source='canonical_db'
                WHERE a.dataset='kline_minute_raw' AND a.trade_date=%s AND a.data_source='canonical_db'""", (day,))
            status, revision, delay = cur.fetchone()
            assert status == "UNVERIFIED" and revision == 1 and 25 < delay <= 30
            raise RuntimeError("rollback synthetic minute target")
