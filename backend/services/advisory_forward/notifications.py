"""Post-publication notifications on the existing Advisory scheduler, opt-in only."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, time
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

from .notification_repository import AdvisoryNotificationRepository

CN = ZoneInfo('Asia/Shanghai')


def enabled():
    return os.getenv('AISTOCK_ADVISORY_NOTIFICATIONS_ENABLED', '').strip().lower() in {
        '1', 'true', 'yes', 'on'
    }


def configured_channels(config):
    channels = []
    if config.get('email_enabled') and all(config.get(k) for k in (
        'smtp_server', 'smtp_port', 'email_from', 'email_password', 'email_to'
    )):
        channels.append('email')
    if (config.get('webhook_enabled') and config.get('webhook_url')
            and config.get('webhook_type') in {'dingtalk', 'feishu'}):
        channels.append('webhook')
    return channels


def events_for_day(now, target, rows, base_url):
    """Publication success does not assert settlement, fills, or positive returns."""
    if now.tzinfo is None:
        raise ValueError('notification clock must be timezone-aware')
    now = now.astimezone(CN)
    if target is None or not rows:
        return []
    if target <= now.date():
        raise ValueError('ADVISORY_NOTIFICATION_TARGET_ORDER')
    parts = urlsplit(base_url)
    if parts.scheme not in {'http', 'https'} or not parts.netloc or parts.username or parts.password:
        raise ValueError('ADVISORY_NOTIFICATION_BASE_URL_INVALID')
    if parts.query or parts.fragment or parts.path not in {'', '/'}:
        raise ValueError('ADVISORY_NOTIFICATION_BASE_URL_INVALID')
    expires = datetime.combine(target, time(9, 30), CN)
    events, lines, missing = [], [], []

    def event(kind, identity, subject, message):
        key = hashlib.sha256(json.dumps([kind, *identity], ensure_ascii=True).encode()).hexdigest()
        return dict(event_key=key, kind=kind, decision_date=now.date(), expires_at=expires,
                    payload=dict(schema_version='advisory_notification_message_v1',
                                 subject=subject, message=message))

    for row in rows:
        state = row.get('publication_status') or 'NOT_STARTED'
        link = base_url.rstrip('/') + '/paper-v2/advisory?' + urlencode({'program_id': row['program_id']})
        line = f"{row['program_name']}：{state}；目标日 {target}；{link}"
        if state == 'PUBLISHED':
            if (not all(row.get(k) for k in ('binding_version_id', 'list_version_id', 'published_at'))
                    or row['published_at'].tzinfo is None
                    or row['target_trade_date'] != target):
                raise ValueError('ADVISORY_NOTIFICATION_PUBLICATION_IDENTITY_INVALID')
            line += (f"\n发布于 {row['published_at'].astimezone(CN).isoformat()}；"
                     f"名单 {row['list_version_id']}；结算 {row.get('settlement_status') or 'UNKNOWN'}")
            events.append(event('PUBLISHED', [row['program_id'], row['binding_version_id'],
                                str(target), row['list_version_id']], '荐股名单已发布', line))
        else:
            line += f"；原因 {row.get('last_reason_code') or 'UNKNOWN'}"
            missing.append(line)
        lines.append(line)
    footer = f'\n统计截至 {now.isoformat()}。发布状态与结算、成交及收益分别评价。'
    if now.time() >= time(21, 45):
        events.append(event('SUMMARY', [str(now.date())], '荐股每日运行汇总', '\n\n'.join(lines)+footer))
    if now.time() >= time(22) and missing:
        events.append(event('OVERDUE', [str(now.date())], '荐股任务尚未发布提醒', '\n\n'.join(missing)+footer))
    return events


class AdvisoryNotificationService:
    def __init__(self, repository=None, transport=None, now_provider=None):
        self.repository = repository
        self.transport = transport
        self.now_provider = now_provider or (lambda: datetime.now(CN))

    def run_once(self):
        # Do not import/configure the shared transport or borrow a connection when off.
        if not enabled():
            return {'status': 'DISABLED'}
        if self.transport is None:
            from .notification_transport import AdvisoryNotificationTransport
            self.transport = AdvisoryNotificationTransport()
        channels = configured_channels(self.transport.config)
        if not channels:
            return {'status': 'NOT_CONFIGURED'}
        if self.repository is None:
            self.repository = AdvisoryNotificationRepository()
        now = self.now_provider()
        if now.tzinfo is None:
            raise ValueError('notification clock must be timezone-aware')
        now = now.astimezone(CN)
        target, rows = self.repository.snapshot(now.date())
        events = events_for_day(now, target, rows,
            os.getenv('AISTOCK_ADVISORY_NOTIFICATION_BASE_URL', 'http://localhost:3000'))
        queued = sum(self.repository.enqueue(e, ch, now) for e in events for ch in channels)
        results = []
        for _ in range(4):
            row = self.repository.claim(self.now_provider().astimezone(CN), channels)
            if row is None:
                break
            try:
                accepted = self.transport.send_external_message(
                    row['channel'], row['payload']['subject'], row['payload']['message']) is True
                error = None if accepted else 'EXTERNAL_DELIVERY_NOT_ACCEPTED'
            except Exception:
                accepted, error = False, 'EXTERNAL_DELIVERY_EXCEPTION'
            state = self.repository.finish(row, accepted=accepted,
                now=self.now_provider().astimezone(CN), error_code=error)
            results.append({'event_key': row['event_key'], 'channel': row['channel'], 'status': state})
        return {'status': 'PROCESSED', 'queued': queued, 'deliveries': results}
