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
            cur.execute("DELETE FROM market.data_alerts WHERE dataset=%s", (dataset,))
            cur.execute("DELETE FROM market.data_sync_attempts WHERE target_id IN (SELECT target_id FROM market.data_sync_targets WHERE dataset=%s)", (dataset,))
            cur.execute("DELETE FROM market.data_sync_targets WHERE dataset=%s", (dataset,))
            cur.execute("DELETE FROM market.dataset_date_refresh_audit WHERE dataset=%s", (dataset,))


def changed(repo, dataset, day):
    with repo.transaction() as conn:
        return mark_changed(conn, dataset, [day], reason="DEV fault contract", source="dev_test")[day]


def result():
    return ValidationResult(PartitionState.VALID, (CheckCounts("row", 1, 1, 0, 0, 0, 0),), {"source": "DEV fixture"})


def test_legacy_zero_stk_limit_count_is_revalidated_once_without_fact_revision(db):
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_integrity.repository import enqueue_validation
    _ds, repo = db
    day = date(2099, 1, 3)
    # Only metadata on this unused synthetic date, all writes rolled back.
    with pytest.raises(RuntimeError, match="rollback legacy count fixture"):
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM market.dataset_date_refresh_audit WHERE dataset='stk_limit' AND trade_date=%s", (day,))
            assert cur.fetchone() is None
            cur.execute("""INSERT INTO market.dataset_date_refresh_audit
                (dataset,trade_date,data_source,status,row_count,expected_rows,data_revision,validated_revision,
                 validation_status,validation_id,scope_version,rule_version,quality_status)
                VALUES('stk_limit',%s,'canonical_db','success',0,2,1,1,'VALID','legacy-proof',%s,%s,'ok')""",
                (day, SCOPE_VERSION, RULE_VERSION))
            enqueue_validation(conn, "stk_limit", day)
            cur.execute("""SELECT target_status,metadata->>'desired_revision' FROM market.data_sync_targets
                WHERE dataset='stk_limit' AND target_date=%s AND data_source='canonical_db'""", (day,))
            assert cur.fetchone() == ("pending", "1")
            cur.execute("""UPDATE market.dataset_date_refresh_audit SET row_count=2,
                metadata='{"validation":{"checks":{"physical_rows":2}}}'::jsonb
                WHERE dataset='stk_limit' AND trade_date=%s AND data_source='canonical_db'""", (day,))
            cur.execute("""UPDATE market.data_sync_targets SET target_status='reconciled'
                WHERE dataset='stk_limit' AND target_date=%s AND data_source='canonical_db'""", (day,))
            enqueue_validation(conn, "stk_limit", day)
            cur.execute("""SELECT t.target_status,p.data_revision,p.row_count
                FROM market.data_sync_targets t JOIN market.dataset_date_refresh_audit p
                ON p.dataset=t.dataset AND p.trade_date=t.target_date AND p.data_source='canonical_db'
                WHERE t.dataset='stk_limit' AND t.target_date=%s AND t.data_source='canonical_db'""", (day,))
            assert cur.fetchone() == ("reconciled", 1, 2)
            raise RuntimeError("rollback legacy count fixture")


def test_schedule_recovery_native_query_is_bounded_idempotent_and_preserves_true_failures(db):
    from datetime import datetime, timedelta, timezone
    from psycopg2.extras import Json
    from backend.ingestion.tdx_scheduler import TDXScheduler
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_integrity.contracts import DENSE_TRADED_BASIS
    _ds, repo = db
    scheduler = TDXScheduler.__new__(TDXScheduler)
    statements = []
    scheduler._execute = lambda query, params: statements.append((query, params))
    scheduler._reconcile_recovered_schedule_states()
    query, params = statements[0]
    query = query.replace("market.ingestion_schedules", "pg_temp.schedule_projection")
    query = query.replace("market.dataset_date_refresh_audit", "pg_temp.partition_projection")
    query = query.replace("market.ingestion_jobs", "pg_temp.job_projection")
    assert "market." not in query
    failure = datetime(2026, 11, 5, 10, tzinfo=timezone.utc)
    proof = failure + timedelta(hours=5)
    cases = {"recovered": {}, "delayed": {"status": "delayed"},
        "rescheduled": {"updated": proof+timedelta(hours=12)},
        "newer_failure": {"run": proof+timedelta(minutes=1), "updated": proof+timedelta(minutes=1)},
        "late_failure": {"job_finished": proof+timedelta(minutes=1)},
        "late_next_day": {"job_created": failure+timedelta(days=1), "job_finished": proof+timedelta(days=1)},
        "next_day_unscoped": {"job_created": failure+timedelta(days=1), "job_finished": proof+timedelta(days=1),
                              "job_target_date": None},
        "old_failure": {"job_finished": proof-timedelta(minutes=1)},
        "other_date_retry": {"job_finished": proof+timedelta(minutes=1), "job_target_date": "2026-10-28"},
        "newer_day": {"run": failure+timedelta(days=1)},
        "dirty": {"revision": 2}, "missing": {"state": "INCOMPLETE"},
        "unknown": {"state": "UNVERIFIED"}, "error": {"state": "ERROR"},
        "old_rule": {"rule": "old"}, "no_id": {"validation_id": "  "},
        "queued": {"status": "queued"}, "running": {"status": "running"},
        "disabled": {"enabled": False}, "history_job": {"mode": "full"},
        "old_dense": {"dataset": "bak_basic"},
        "current_dense": {"dataset": "cyq_perf", "basis": DENSE_TRADED_BASIS},
        "alias": {"dataset": "moneyflow_ts", "schedule_dataset": "stock_moneyflow_ts"}}
    with repo.transaction() as conn, conn.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE schedule_projection(
            schedule_id text PRIMARY KEY,dataset text,enabled boolean,mode text,last_status text,
            last_run_at timestamptz,updated_at timestamptz,last_error text) ON COMMIT DROP""")
        cur.execute("""CREATE TEMP TABLE partition_projection(
            dataset text,trade_date date,data_source text,data_revision bigint,validated_revision bigint,
            validation_status text,validation_id text,scope_version text,rule_version text,
            refreshed_at timestamptz,metadata jsonb) ON COMMIT DROP""")
        cur.execute("""CREATE TEMP TABLE job_projection(
            status text,created_at timestamptz,finished_at timestamptz,summary jsonb) ON COMMIT DROP""")
        for name, overrides in cases.items():
            dataset = overrides.get("dataset", name)
            cur.execute("INSERT INTO pg_temp.schedule_projection VALUES(%s,%s,%s,%s,%s,%s,%s,'old failure')",
                (name, overrides.get("schedule_dataset", dataset), overrides.get("enabled", True),
                 overrides.get("mode", "incremental"), overrides.get("status", "failed"),
                 overrides.get("run", failure), overrides.get("updated", failure)))
            cur.execute("INSERT INTO pg_temp.partition_projection VALUES(%s,%s,'canonical_db',%s,1,%s,%s,%s,%s,%s,%s)",
                (dataset, failure.date(), overrides.get("revision", 1), overrides.get("state", "VALID"),
                 overrides.get("validation_id", "valid-proof"), SCOPE_VERSION, overrides.get("rule", RULE_VERSION),
                 proof, Json({"validation": {"checks": {"coverage_basis": overrides.get("basis")}}})))
            if "job_finished" in overrides:
                cur.execute("INSERT INTO pg_temp.job_projection VALUES('failed',%s,%s,%s)",
                    (overrides.get("job_created", failure), overrides["job_finished"], Json({"dataset": dataset,
                        "target_date": overrides.get("job_target_date", str(failure.date()))})))
        cur.execute(query, params)
        assert cur.rowcount == 8
        cur.execute("SELECT schedule_id FROM pg_temp.schedule_projection WHERE last_status='success'")
        assert {r[0] for r in cur.fetchall()} == {
            "recovered", "delayed", "current_dense", "alias", "rescheduled", "old_failure", "other_date_retry",
            "next_day_unscoped"}
        cur.execute(query, params)
        assert cur.rowcount == 0
        cur.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, params)
        plan = cur.fetchone()[0][0]
        print("DEV schedule metadata query execution_ms=", plan["Execution Time"])
        # Exact same native statement: at most 100 cards, never 100 markets.
        cur.execute("""INSERT INTO pg_temp.schedule_projection
            SELECT 'bounded-'||n,'recovered',TRUE,'incremental','failed',%s,%s,'old failure'
            FROM generate_series(1,105) AS n""", (failure, failure))
        cur.execute(query, params)
        assert cur.rowcount == 100
        cur.execute(query, params)
        assert cur.rowcount == 5


def test_unchanged_old_verdict_backs_off_but_new_source_completion_wakes_it(db):
    from datetime import datetime, timedelta, timezone
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_integrity.repository import enqueue_validation
    ds, repo = db
    day = date(2026, 5, 20)
    rev = changed(repo, ds, day)
    with repo.transaction() as conn:
        enqueue_validation(conn, ds, day)
    unknown = ValidationResult(PartitionState.UNVERIFIED,
        (CheckCounts("scope", 1, 0, 0, 0, 0, 1),), {}, reason="row_count_seed_is_not_a_complete_source_scan")
    for _ in range(2):
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute("UPDATE market.data_sync_targets SET next_retry_at=clock_timestamp()-interval '1 second' WHERE dataset=%s", (ds,))
        claim = repo.claim(ds, day)
        repo.publish(dataset=ds, day=day, revision=rev, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
            target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=unknown)
    with repo.transaction(readonly=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT next_retry_at FROM market.data_sync_targets WHERE dataset=%s", (ds,))
        assert cur.fetchone()[0] > datetime.now(timezone.utc) + timedelta(hours=5)
    with repo.transaction() as conn:
        enqueue_validation(conn, ds, day)  # Ordinary seeding must preserve backoff.
    assert repo.claim(ds, day) is None
    with repo.transaction() as conn:
        enqueue_validation(conn, ds, day, source_completed=True)
    assert repo.claim(ds, day) is not None  # Same facts, but query completion changed.


def test_new_revision_ignores_old_validation_backoff(db):
    from datetime import datetime, timedelta, timezone
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_integrity.repository import enqueue_validation
    ds, repo = db
    day = date(2026, 5, 20)
    rev = changed(repo, ds, day)
    claim = repo.claim(ds, day)
    repo.publish(dataset=ds, day=day, revision=rev, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"],
        result=ValidationResult(PartitionState.INCOMPLETE, (CheckCounts("row", 1, 0, 0, 1, 0, 0),), {}))
    with repo.transaction() as conn, conn.cursor() as cur:
        cur.execute("UPDATE market.dataset_date_refresh_audit SET data_revision=data_revision+1 WHERE dataset=%s", (ds,))
        cur.execute("UPDATE market.data_sync_targets SET next_retry_at=%s WHERE dataset=%s",
                    (datetime.now(timezone.utc)+timedelta(days=1), ds))
        enqueue_validation(conn, ds, day)
    new = repo.claim(ds, day)
    assert new is not None and new["metadata"]["desired_revision"] == rev+1


def test_schedule_recovery_preserves_other_day_failures_and_failed_job_history(db, monkeypatch):
    from datetime import datetime, timezone
    from backend.ingestion import tdx_scheduler as scheduling
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    ds, repo = db
    day = date(2026, 9, 1)
    rev = changed(repo, ds, day)
    claim = repo.claim(ds, day)
    repo.publish(dataset=ds, day=day, revision=rev, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result())
    sid = str(uuid4())
    try:
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO market.ingestion_schedules
                (schedule_id,dataset,mode,frequency,enabled,last_status)
                VALUES (%s,%s,'incremental','daily',false,'failed')""", (sid, ds))
        scheduler = scheduling.TDXScheduler.__new__(scheduling.TDXScheduler)
        monkeypatch.setattr(scheduling, "get_conn", lambda **_: repo.transaction())
        for stamp, status, expected in [("2026-09-01 18:00+08", "failed", "success"),
                ("2026-09-02 18:00+08", "failed", "failed"), ("2026-09-01 19:00+08", "partial", "partial")]:
            with repo.transaction() as conn, conn.cursor() as cur:
                cur.execute("UPDATE market.ingestion_schedules SET last_run_at=%s,last_status=%s WHERE schedule_id=%s", (stamp, status, sid))
            scheduler._reconcile_recovered_target_state(dataset=ds, target_id=claim["target_id"], target_date=day,
                trigger_source="DEV recovery", finished_at=datetime.now(timezone.utc))
            with repo.transaction(readonly=True) as conn, conn.cursor() as cur:
                cur.execute("SELECT last_status FROM market.ingestion_schedules WHERE schedule_id=%s", (sid,))
                assert cur.fetchone()[0] == expected
    finally:
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market.ingestion_schedules WHERE schedule_id=%s", (sid,))


def test_publication_transaction_holds_revision_lock_and_rolls_back_on_drift(db):
    from concurrent.futures import ThreadPoolExecutor
    from backend.services.advisory_forward.repository import AdvisoryForwardPGRepository
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_integrity.inputs import partition_pins, verify_partition_refs, InputReadinessError
    ds, repo = db
    day = date(2026, 9, 1)
    revision = changed(repo, ds, day)
    claim = repo.claim(ds, day)
    repo.publish(dataset=ds, day=day, revision=revision, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result())
    with repo._connect() as connection:
        @contextmanager
        def borrowed():
            yield connection
        publisher = AdvisoryForwardPGRepository(conn_factory=borrowed)
        with connection.cursor() as cur:
            cur.execute("CREATE TEMP TABLE publication_rollback_probe (id integer)")
        with pytest.raises(RuntimeError, match="injected publication failure"):
            with publisher._publication_connection() as conn:
                assert conn.autocommit is False
                pins = partition_pins(conn, day, day, [ds], lock=True)
                refs = [{"dataset": ds, "fact_date": str(day), **{k: pins[0][k] for k in
                         ("data_revision", "validation_id", "scope_version", "rule_version")}}]
                verify_partition_refs(refs, pins)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    writer = executor.submit(changed, repo, ds, day)
                    with pytest.raises(psycopg2.errors.LockNotAvailable):
                        writer.result(timeout=5)
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO publication_rollback_probe VALUES (1)")
                raise RuntimeError("injected publication failure")
        assert connection.autocommit is True
        with connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM publication_rollback_probe")
            assert cur.fetchone()[0] == 0
        changed(repo, ds, day)
        with pytest.raises(InputReadinessError):
            with publisher._publication_connection() as conn:
                verify_partition_refs(refs, partition_pins(conn, day, day, [ds], lock=True))
                pytest.fail("revised source must reject publication")
        assert connection.autocommit is True


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


def test_obsolete_dense_green_requeues_once_without_invalidating_other_datasets(db, monkeypatch):
    from backend.services.data_integrity import contracts, repository
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    ds, repo = db
    day = date(2026, 9, 1)
    # Apply the real dense-panel predicate to an isolated DEV-only ledger key.
    dense = (*contracts.DENSE_TRADED_DATASETS, ds)
    monkeypatch.setattr(contracts, "DENSE_TRADED_DATASETS", dense)
    monkeypatch.setattr(repository, "DENSE_TRADED_DATASETS", dense)
    changed(repo, ds, day)
    claim = repo.claim(ds, day)
    old = repo.publish(dataset=ds, day=day, revision=1, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result())
    assert not contracts.effective_partition(old, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]
    with repo.transaction() as conn:
        repository.enqueue_validation(conn, ds, day)
    new = repo.claim(ds, day)
    assert new and new["target_id"] == claim["target_id"]
    proof = ValidationResult(PartitionState.VALID, (CheckCounts("security_date", 1, 1, 0, 0, 0, 0),),
                             {"coverage_basis": contracts.DENSE_TRADED_BASIS})
    current = repo.publish(dataset=ds, day=day, revision=1, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=new["target_id"], token=new["metadata"]["lease_token"], result=proof)
    assert contracts.effective_partition(current, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]
    with repo.transaction() as conn:
        repository.enqueue_validation(conn, ds, day)
    assert repo.claim(ds, day) is None  # A current green is not repeatedly revalidated.
    assert contracts.effective_partition({**old, "dataset": "index_daily"},
        scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]


@pytest.mark.parametrize("state", ["recovering", "recovered", "incomplete"])
def test_dated_retry_alert_native_sql_and_recovery_readback_in_existing_dev(db, monkeypatch, state):
    from datetime import datetime, timedelta, timezone
    from psycopg2.extras import Json, RealDictCursor
    from types import SimpleNamespace
    from backend.ingestion import tdx_scheduler as scheduling
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    from backend.services.data_sync_targets import DataSyncTargetRecord, DataSyncTargetRepository
    ds, repo = db
    day = date(2026, 9, 1)
    changed(repo, ds, day)
    claim = repo.claim(ds, day)
    proof = result() if state == "recovered" else ValidationResult(PartitionState.INCOMPLETE,
        (CheckCounts("security_date", 2, 1, 0, 1, 0, 0),), {}, reason="real_missing_key")
    repo.publish(dataset=ds, day=day, revision=1, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
        target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=proof)
    targets = DataSyncTargetRepository(repo._connect)
    expired = targets.upsert_target(DataSyncTargetRecord(dataset=ds, data_source="tushare", target_date=day,
        target_status="final_blocked", required_before=datetime.now(timezone.utc)-timedelta(days=1)))
    if state == "recovering":
        targets.upsert_target(DataSyncTargetRecord(dataset=ds, data_source="integrity_repair", target_date=day,
            target_status="retry", target_scope={"operation": "FETCH"},
            next_retry_at=datetime.now(timezone.utc)+timedelta(hours=6)))
    alert_id = str(uuid4())
    with repo.transaction() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO market.data_alerts(alert_id,dataset,severity,alert_type,title,message,details)
            VALUES(%s,%s,'error','retry_exhausted','DEV fixture','DEV fixture',%s)""",
            (alert_id, ds, Json({"target_id": expired["target_id"], "target_date": str(day),
                                "alert_gate": "data_sync_targets_final_state"})))
    scheduler = scheduling.TDXScheduler.__new__(scheduling.TDXScheduler)
    scheduler._db_cfg = {}
    def fetch(query, params=()):
        with repo.transaction(readonly=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
    def execute(query, params=()):
        with repo.transaction() as conn, conn.cursor() as cur:
            cur.execute(query, params)
    scheduler._fetchall, scheduler._execute = fetch, execute
    alerts = []
    monkeypatch.setattr(scheduling, "get_conn", lambda **_: repo.transaction())
    monkeypatch.setattr(scheduling, "DataHealthAlerter", lambda *_: SimpleNamespace(
        flush=lambda values: alerts.extend(values) or {"written": len(values)}))
    scheduler._flush_final_data_sync_alerts([expired])
    with repo.transaction(readonly=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT acknowledged FROM market.data_alerts WHERE alert_id=%s", (alert_id,))
        assert cur.fetchone()[0] is (state != "incomplete")
        cur.execute("SELECT target_status FROM market.data_sync_targets WHERE target_id=%s", (expired["target_id"],))
        assert cur.fetchone()[0] == ("reconciled" if state == "recovered" else "final_blocked")
    assert [a.alert_type for a in alerts] == ([] if state == "recovered" else [
        "retry_pending" if state == "recovering" else "retry_exhausted"])


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
