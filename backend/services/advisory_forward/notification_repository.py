"""Advisory-owned durable delivery; never consumes monitor notification rows."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn


class AdvisoryNotificationRepository:
    def __init__(self, connection_factory=None):
        self.connection_factory = connection_factory or (
            lambda: get_conn(autocommit=False, manage_transaction=True)
        )

    @contextmanager
    def cursor(self):
        with self.connection_factory() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '3000ms'")
                cursor.execute("SET LOCAL lock_timeout = '1000ms'")
                yield cursor

    def snapshot(self, day):
        with self.cursor() as cur:
            cur.execute("SELECT is_trading FROM market.trading_calendar WHERE cal_date=%s", (day,))
            session = cur.fetchone()
            if session is None:
                raise ValueError("ADVISORY_NOTIFICATION_CALENDAR_UNKNOWN")
            if not session['is_trading']:
                return None, []
            cur.execute("SELECT min(cal_date) AS target FROM market.trading_calendar "
                        "WHERE is_trading AND cal_date>%s", (day,))
            target = cur.fetchone()['target']
            if target is None:
                raise ValueError("ADVISORY_NOTIFICATION_TARGET_UNKNOWN")
            cur.execute("""SELECT p.program_id,p.program_name,f.binding_version_id,
                f.list_version_id,f.target_trade_date,f.publication_status,f.published_at,
                f.settlement_status,f.last_reason_code
                FROM app.advisory_program p LEFT JOIN app.advisory_forward_run f
                ON f.program_id=p.program_id AND f.decision_as_of_trade_date=%s
                WHERE p.status='ENABLED' AND p.review_schedule->>'frequency'='daily_after_close'
                ORDER BY p.program_id LIMIT 101""", (day,))
            rows = [dict(r) for r in cur.fetchall()]
            if len(rows) > 100:
                raise ValueError("ADVISORY_NOTIFICATION_PROGRAM_LIMIT")
            return target, rows

    def enqueue(self, event, channel, now):
        with self.cursor() as cur:
            cur.execute("""INSERT INTO app.advisory_notification_delivery
                (event_key,channel,kind,decision_date,payload,available_at,expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (event['event_key'], channel, event['kind'], event['decision_date'],
                 Json(event['payload']), now, event['expires_at']))
            return cur.rowcount == 1

    def claim(self, now, channels):
        if not channels:
            return None
        with self.cursor() as cur:
            cur.execute("""UPDATE app.advisory_notification_delivery
                SET status='EXPIRED',lease_until=NULL,lease_token=NULL,
                    last_error_code='DELIVERY_WINDOW_EXPIRED',updated_at=%s
                WHERE status IN ('PENDING','SENDING') AND expires_at<=%s""", (now, now))
            cur.execute("""UPDATE app.advisory_notification_delivery
                SET status='FAILED',lease_until=NULL,lease_token=NULL,
                    last_error_code='ATTEMPTS_EXHAUSTED',updated_at=%s
                WHERE status='SENDING' AND lease_until<=%s AND attempts>=6""", (now, now))
            cur.execute("""WITH candidate AS (
                SELECT event_key,channel FROM app.advisory_notification_delivery
                WHERE channel=ANY(%s::text[]) AND expires_at>%s AND attempts<6
                  AND ((status='PENDING' AND available_at<=%s)
                    OR (status='SENDING' AND lease_until<=%s))
                ORDER BY created_at,event_key,channel FOR UPDATE SKIP LOCKED LIMIT 1
                ) UPDATE app.advisory_notification_delivery d SET
                status='SENDING',attempts=d.attempts+1,lease_until=%s,lease_token=%s,updated_at=%s
                FROM candidate c WHERE d.event_key=c.event_key AND d.channel=c.channel RETURNING d.*""",
                (list(channels), now, now, now, now+timedelta(seconds=120), uuid4().hex, now))
            row = cur.fetchone()
            return dict(row) if row else None

    def finish(self, row, *, accepted, now, error_code=None):
        retry_at = now + timedelta(seconds=min(1800, 60 * 2**(row['attempts']-1)))
        status = 'SENT' if accepted else (
            'FAILED' if row['attempts'] >= 6 or retry_at >= row['expires_at'] else 'PENDING'
        )
        with self.cursor() as cur:
            cur.execute("""UPDATE app.advisory_notification_delivery SET status=%s,
                lease_token=NULL,lease_until=NULL,sent_at=%s,last_error_code=%s,
                available_at=%s,updated_at=%s
                WHERE event_key=%s AND channel=%s AND status='SENDING' AND lease_token=%s""",
                (status, now if accepted else None, None if accepted else error_code,
                 retry_at if status == 'PENDING' else row['available_at'], now,
                 row['event_key'], row['channel'], row['lease_token']))
            if cur.rowcount != 1:
                raise RuntimeError("ADVISORY_NOTIFICATION_LEASE_LOST")
        return status
