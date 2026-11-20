"""Explicit labeled Feishu detail test. Reads production; never writes the outbox."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--decision-date', required=True, type=date.fromisoformat)
    parser.add_argument('--env-file', required=True, type=Path)
    parser.add_argument('--receipt', required=True, type=Path)
    parser.add_argument('--send-test', action='store_true', required=True)
    parser.add_argument('--refresh-test-table', action='store_true',
                        help='Repair an already sent labeled test table at the same URL; no new message.')
    args = parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv(args.env_file, override=False)
    from backend.services.advisory_forward.feishu_sheets import DetailSheetDelivery, FeishuSheetsClient
    from backend.services.advisory_forward.notification_details import DetailSnapshotBuilder
    from backend.services.advisory_forward.notification_repository import AdvisoryNotificationRepository
    from backend.services.advisory_forward.notification_transport import AdvisoryNotificationTransport
    from backend.services.advisory_forward.notifications import completion_events_for_day
    cn = ZoneInfo('Asia/Shanghai')
    now = datetime.now(cn)
    if args.decision_date >= now.date():
        parser.error('Use a completed prior decision day for the labeled historical test.')
    if not args.receipt.parent.is_dir():
        parser.error('The exact receipt parent directory must already exist.')

    def persist(value):
        # Replace one explicitly named local receipt; no secrets are serialized.
        staging = args.receipt.with_suffix(args.receipt.suffix + '.tmp')
        staging.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(staging, args.receipt)

    if args.receipt.exists():
        receipt = json.loads(args.receipt.read_text(encoding='utf-8'))
        if receipt.get('decision_date') != str(args.decision_date):
            parser.error('Receipt identity does not match the requested decision day.')
        payload = receipt['payload']
        if receipt.get('status') == 'FAILED':
            receipt.setdefault('resolved_failures', []).append({k: receipt[k] for k in (
                'reason_code', 'error_type', 'error_frames') if k in receipt})
            for key in ('reason_code', 'error_type', 'error_frames'):
                receipt.pop(key, None)
        if args.refresh_test_table and not receipt.get('pending_test_refresh'):
            if not (payload['subject'].startswith('【测试】') and payload.get('sheet_delivery_state', {}).get('message_accepted')):
                parser.error('Only an already sent labeled test document can be refreshed.')
            target, rows = AdvisoryNotificationRepository().snapshot(args.decision_date)
            if not completion_events_for_day(datetime.combine(args.decision_date, time(23, 59, 59), cn), target, rows):
                parser.error('The historical source roster is not fully published.')
            refreshed = DetailSnapshotBuilder().build(now, target, rows, decision_date=args.decision_date)
            receipt.setdefault('test_snapshot_revisions', []).append({
                'prior_sha256': payload['detail_snapshot']['sha256'], 'new_sha256': refreshed['sha256'],
                'reason': 'Explicit repair of test export after recipient reported incorrect UNKNOWN fields',
                'captured_at': now.isoformat(),
            })
            payload['detail_snapshot'] = refreshed
            receipt['pending_test_refresh'] = True
            for key in ('completed_tables', 'table_verified'):
                payload['sheet_delivery_state'].pop(key, None)
            persist(receipt)
    else:
        if args.refresh_test_table:
            parser.error('Test refresh requires the exact existing receipt and document.')
        target, rows = AdvisoryNotificationRepository().snapshot(args.decision_date)
        events = completion_events_for_day(datetime.combine(args.decision_date, time(23, 59, 59), cn),
                                           target, rows)
        if not events:
            parser.error('The selected day has no fully published enabled program roster.')
        snapshot = DetailSnapshotBuilder().build(now, target, rows, decision_date=args.decision_date)
        payload = dict(subject=f'【测试】AIstock 荐股详情｜{target}', detail_snapshot=snapshot,
                       sheet_delivery_state={})
        receipt = dict(schema_version='advisory_feishu_detail_test_v1', decision_date=str(args.decision_date),
                       production_outbox_writes=0, service_operations=0, payload=payload, status='PREPARED')
        persist(receipt)

    def checkpoint(state):
        payload['sheet_delivery_state'] = state
        persist(receipt)

    client = FeishuSheetsClient(*(os.getenv('AISTOCK_ADVISORY_FEISHU_' + key, '').strip()
                                  for key in ('APP_ID', 'APP_SECRET', 'CHAT_ID')))
    transport = AdvisoryNotificationTransport()
    # Test uses strict detail semantics without changing the environment/global sender.
    if not transport.config.get('advisory_feishu_completion_only'):
        parser.error('Dedicated Advisory Feishu sender must be configured.')
    transport.config = dict(transport.config, advisory_feishu_details=True)
    try:
        DetailSheetDelivery(client).deliver(payload, checkpoint, lambda subject, message:
            transport.send_external_message('webhook', subject, '【测试：历史已发布名单，非新的荐股通知】\n' + message),
            update_test_table=args.refresh_test_table)
    except Exception as exc:
        import traceback
        from backend.services.advisory_forward.notification_details import DetailDeliveryError
        receipt.update(status='FAILED', reason_code=exc.code if isinstance(exc, DetailDeliveryError)
                       else 'DETAIL_TEST_EXCEPTION', error_type=type(exc).__name__,
                       error_frames=[{'file': Path(f.filename).name, 'line': f.lineno, 'function': f.name}
                                     for f in traceback.extract_tb(exc.__traceback__)])
        persist(receipt)
        print(json.dumps({k: receipt[k] for k in ('status', 'reason_code', 'error_type', 'error_frames')}))
        return 1
    receipt.update(status='UPDATED_TEST_TABLE_NO_MESSAGE' if args.refresh_test_table else 'SENT_TEST_ONLY',
                   finished_at=datetime.now(cn).isoformat())
    receipt.pop('pending_test_refresh', None)
    persist(receipt)
    print(json.dumps({'status': receipt['status'], 'url': payload['sheet_delivery_state']['document_url'],
                      'programs': len(payload['detail_snapshot']['programs']), 'production_outbox_writes': 0}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
