"""Opt-in integration on the existing DEV DB; no new DB and no external sends."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import os
from uuid import uuid4

import psycopg2
import pytest

from backend.services.advisory_forward.notification_repository import AdvisoryNotificationRepository

pytestmark = pytest.mark.skipif(
    os.getenv('AISTOCK_NOTIFICATION_DEV_TEST') != '1', reason='explicit existing DEV integration only'
)
MIGRATION = Path(__file__).resolve().parents[2] / 'db/migrations/add_advisory_notifications_20261114.sql'


@pytest.fixture
def dev():
    from scripts.prepare_canonical_pit_monthly import _load_database_config
    cfg = _load_database_config('dev', Path('F:/Dev/AIstock/.env'))
    assert cfg.dbname == 'aistock_dev'
    options = dict(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
                   dbname=cfg.dbname, connect_timeout=5)
    try:
        conn = psycopg2.connect(**options)
    except psycopg2.Error:
        pytest.fail('Existing DEV connection unavailable; credentials suppressed', pytrace=False)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database()')
            assert cur.fetchone()[0] == 'aistock_dev'
            cur.execute(MIGRATION.read_text(encoding='utf-8'))
            cur.execute(MIGRATION.read_text(encoding='utf-8'))
            cur.execute('SELECT count(*) FROM app.advisory_notification_delivery')
            # Never claim or alter another developer's delivery events.
            assert cur.fetchone()[0] == 0, 'DEV queue is occupied; cannot run isolated claim test'
        conn.commit()  # Only the new DEV table/index; production is never connected.
        yield conn, options
    finally:
        conn.rollback()
        conn.close()


def repo_on(conn):
    @contextmanager
    def factory():
        yield conn
    return AdvisoryNotificationRepository(factory)


def event(now, **changes):
    return dict(event_key='test_'+uuid4().hex, kind='PUBLISHED', decision_date=now.date(),
                payload={'subject': 'DEV test only', 'message': 'not sent'},
                expires_at=now+timedelta(hours=1), **changes)


def test_real_migration_dedupe_retry_channel_and_token_fence(dev):
    conn, _ = dev
    repo = repo_on(conn)
    now = datetime.now(timezone.utc)
    e = event(now)
    assert repo.enqueue(e, 'email', now)
    assert not repo.enqueue({**e, 'payload': {'subject': 'mutated'}}, 'email', now)
    assert repo.enqueue(e, 'webhook', now)
    row = repo.claim(now, ['email'])
    assert row['payload'] == e['payload'] and row['attempts'] == 1
    assert repo.claim(now, ['email']) is None
    assert repo.claim(now, ['webhook'])['channel'] == 'webhook'
    stale = dict(row)
    assert repo.finish(row, accepted=False, now=now, error_code='NOT_ACCEPTED') == 'PENDING'
    assert repo.claim(now+timedelta(seconds=59), ['email']) is None
    row = repo.claim(now+timedelta(seconds=60), ['email'])
    assert row['attempts'] == 2 and row['lease_token'] != stale['lease_token']
    with pytest.raises(RuntimeError, match='LEASE_LOST'):
        repo.finish(stale, accepted=True, now=now)
    assert repo.finish(row, accepted=True, now=now+timedelta(seconds=61)) == 'SENT'
    with conn.cursor() as cur:
        cur.execute('SELECT status,sent_at,last_error_code FROM app.advisory_notification_delivery WHERE event_key=%s AND channel=%s', (e['event_key'], 'email'))
        status, sent, error = cur.fetchone()
    assert status == 'SENT' and sent is not None and error is None


def test_real_lease_crash_recovery_exhaustion_and_expiry(dev):
    conn, _ = dev
    repo, now = repo_on(conn), datetime.now(timezone.utc)
    e = event(now)
    repo.enqueue(e, 'email', now)
    row = repo.claim(now, ['email'])
    for attempt in range(2, 7):
        now += timedelta(seconds=121)
        row = repo.claim(now, ['email'])
        assert row['attempts'] == attempt
    assert repo.claim(now+timedelta(seconds=121), ['email']) is None
    with conn.cursor() as cur:
        cur.execute('SELECT status,last_error_code FROM app.advisory_notification_delivery WHERE event_key=%s', (e['event_key'],))
        assert cur.fetchone() == ('FAILED', 'ATTEMPTS_EXHAUSTED')
    e2 = event(now)
    repo.enqueue(e2, 'email', now)
    assert repo.claim(now+timedelta(hours=2), ['email']) is None
    with conn.cursor() as cur:
        cur.execute('SELECT status FROM app.advisory_notification_delivery WHERE event_key=%s', (e2['event_key'],))
        assert cur.fetchone()[0] == 'EXPIRED'


def test_real_skip_locked_separate_connections(dev):
    conn, options = dev
    repo, now = repo_on(conn), datetime.now(timezone.utc)
    e = event(now)
    repo.enqueue(e, 'email', now)
    conn.commit()  # Own exact test row made visible to the second DEV session.
    other = psycopg2.connect(**options)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM app.advisory_notification_delivery WHERE event_key=%s FOR UPDATE', (e['event_key'],))
        assert repo_on(other).claim(now, ['email']) is None
        other.rollback()
        conn.rollback()
        first = repo_on(other).claim(now, ['email'])
        other.commit()  # Durable lease; a fresh process must not send it again.
        assert repo.claim(now, ['email']) is None
        conn.rollback()
        recovered = repo.claim(now+timedelta(seconds=121), ['email'])
        assert recovered['lease_token'] != first['lease_token']
    finally:
        other.rollback()
        other.close()
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute('DELETE FROM app.advisory_notification_delivery WHERE event_key=%s', (e['event_key'],))
        conn.commit()  # Remove only the exact committed DEV test key.


def test_real_source_snapshot_calendar_and_same_day_filter(dev):
    conn, _ = dev
    target, rows = repo_on(conn).snapshot(date(2026, 11, 13))
    assert target == date(2026, 11, 16)
    assert len(rows) <= 100
    assert all(r['target_trade_date'] in {None, target} for r in rows)
    assert repo_on(conn).snapshot(date(2026, 11, 14)) == (None, [])
