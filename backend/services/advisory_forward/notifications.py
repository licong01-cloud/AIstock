"""Post-publication notifications on the existing Advisory scheduler, opt-in only."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, time
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

from .notification_repository import (
    AdvisoryNotificationRepository, FEISHU_COMPLETION_ROUTE, FEISHU_DETAIL_ROUTE,
)

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
            and (config.get('webhook_type') == 'dingtalk'
                 or (config.get('webhook_type') == 'feishu'
                     and config.get('advisory_feishu_completion_only')))):
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


def completion_events_for_day(now, target, rows):
    """One brief notice after every current daily program has really published."""
    if now.tzinfo is None:
        raise ValueError('notification clock must be timezone-aware')
    now = now.astimezone(CN)
    if target is None or not rows:
        return []
    if target <= now.date():
        raise ValueError('ADVISORY_NOTIFICATION_TARGET_ORDER')
    if any(row.get('publication_status') != 'PUBLISHED' for row in rows):
        return []
    roster = []
    for row in rows:
        published = row.get('published_at')
        if (not all(row.get(k) for k in ('program_id', 'binding_version_id', 'list_version_id'))
                or not isinstance(published, datetime) or published.tzinfo is None
                or row.get('target_trade_date') != target
                or published.astimezone(CN).date() != now.date() or published > now):
            raise ValueError('ADVISORY_NOTIFICATION_PUBLICATION_IDENTITY_INVALID')
        roster.append([row['program_id'], row['binding_version_id'], row['list_version_id']])
    if len({item[0] for item in roster}) != len(roster) or len(roster) > 100:
        raise ValueError('ADVISORY_NOTIFICATION_COMPLETION_ROSTER_INVALID')
    key = hashlib.sha256(json.dumps([FEISHU_COMPLETION_ROUTE, str(now.date())]).encode()).hexdigest()
    roster_sha = hashlib.sha256(json.dumps(sorted(roster), ensure_ascii=True).encode()).hexdigest()
    return [dict(
        event_key=key, kind='SUMMARY', decision_date=now.date(),
        expires_at=datetime.combine(target, time(9, 30), CN),
        payload=dict(
            schema_version='advisory_notification_message_v1',
            delivery_route=FEISHU_COMPLETION_ROUTE,
            completion_roster_sha256=roster_sha,
            subject='AIstock', message=f'{now.date()} 荐股任务已完成。',
        ),
    )]


class AdvisoryNotificationService:
    def __init__(self, repository=None, transport=None, now_provider=None, detail_builder=None):
        self.repository = repository
        self.transport = transport
        self.now_provider = now_provider or (lambda: datetime.now(CN))
        self.detail_builder = detail_builder

    def run_once(self):
        # Do not import/configure the shared transport or borrow a connection when off.
        if not enabled():
            return {'status': 'DISABLED'}
        if self.transport is None:
            from .notification_transport import AdvisoryNotificationTransport
            self.transport = AdvisoryNotificationTransport()
        channels = configured_channels(self.transport.config)
        completion_only = bool(self.transport.config.get('advisory_feishu_completion_only'))
        details = completion_only and bool(self.transport.config.get('advisory_feishu_details'))
        route = FEISHU_DETAIL_ROUTE if details else FEISHU_COMPLETION_ROUTE
        completion_status = None
        if completion_only:
            completion_status = {
                'status': 'WAITING_COMPLETION' if 'webhook' in channels else 'NOT_CONFIGURED',
            }
            error = self.transport.config.get('advisory_feishu_configuration_error')
            if error:
                completion_status['reason_code'] = error
        elif self.transport.config.get('webhook_type') == 'feishu':
            completion_status = {'status': 'DISABLED'}
        if not channels:
            result = {'status': 'NOT_CONFIGURED'}
            if completion_status:
                result['feishu_completion'] = completion_status
                if details:
                    result['feishu_detail_sheet'] = completion_status
            return result
        if self.repository is None:
            self.repository = AdvisoryNotificationRepository()
        now = self.now_provider()
        if now.tzinfo is None:
            raise ValueError('notification clock must be timezone-aware')
        now = now.astimezone(CN)
        target, rows = self.repository.snapshot(now.date())
        legacy_channels = [ch for ch in channels if ch != 'webhook' or not completion_only]
        events = events_for_day(now, target, rows,
            os.getenv('AISTOCK_ADVISORY_NOTIFICATION_BASE_URL', 'http://localhost:3000')) if legacy_channels else []
        queued = sum(self.repository.enqueue(e, ch, now) for e in events for ch in legacy_channels)
        if completion_only and 'webhook' in channels:
            completed = completion_events_for_day(now, target, rows)
            if details and completed:
                from .notification_details import DetailDeliveryError, DetailSnapshotBuilder
                event = completed[0]
                # Preserve the existing daily key across both Feishu modes. A mode
                # change cannot create a second daily row or rewrite its frozen payload.
                event['payload']['delivery_route'] = route
                if not self.repository.detail_event_exists(event['event_key'], now.date()):
                    try:
                        builder = self.detail_builder or DetailSnapshotBuilder()
                        event['payload']['detail_snapshot'] = builder.build(now, target, rows)
                        event['payload']['sheet_delivery_state'] = {}
                        event['payload']['subject'] = f'AIstock 荐股详情｜{target}'
                        queued += self.repository.enqueue(event, 'webhook', now)
                    except Exception as exc:
                        completion_status = {'status': 'DETAIL_SNAPSHOT_FAILED', 'reason_code': (
                            exc.code if isinstance(exc, DetailDeliveryError) else 'DETAIL_SNAPSHOT_EXCEPTION'
                        )}
                else:
                    completion_status = {'status': 'EVENT_ALREADY_RECORDED', 'event_key': event['event_key']}
            else:
                queued += sum(self.repository.enqueue(e, 'webhook', now) for e in completed)
            if completed:
                if not completion_status or completion_status['status'] == 'WAITING_COMPLETION':
                    completion_status = {'status': 'EVENT_READY', 'event_key': completed[0]['event_key']}
        results = []
        for _ in range(4):
            claim_time = self.now_provider().astimezone(CN)
            row = self.repository.claim(claim_time, channels,
                webhook_route=route) if completion_only else self.repository.claim(claim_time, channels)
            if row is None:
                break
            try:
                if row['channel'] == 'webhook' and row['payload'].get('delivery_route') == FEISHU_DETAIL_ROUTE:
                    from .notification_presentation import mobile_card
                    accepted = self.transport.detail_delivery().deliver(
                        row['payload'],
                        lambda state: self.repository.checkpoint_detail(
                            row, state, self.now_provider().astimezone(CN)),
                        lambda subject, card: self.transport.send_detail_card(card),
                        prepare_message=mobile_card,
                    ) is True
                else:
                    accepted = self.transport.send_external_message(
                        row['channel'], row['payload']['subject'], row['payload']['message']) is True
                error = None if accepted else 'EXTERNAL_DELIVERY_NOT_ACCEPTED'
            except Exception as exc:
                from .notification_details import DetailDeliveryError
                accepted, error = False, exc.code if isinstance(exc, DetailDeliveryError) else 'EXTERNAL_DELIVERY_EXCEPTION'
            state = self.repository.finish(row, accepted=accepted,
                now=self.now_provider().astimezone(CN), error_code=error)
            result_row = {'event_key': row['event_key'], 'channel': row['channel'], 'status': state}
            if error:
                result_row['reason_code'] = error
            results.append(result_row)
        result = {'status': 'PROCESSED', 'queued': queued, 'deliveries': results}
        if completion_status:
            result['feishu_completion'] = completion_status
            if details:
                result['feishu_detail_sheet'] = completion_status
        return result
