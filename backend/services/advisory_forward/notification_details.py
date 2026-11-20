"""Freeze saved lists and the workbench's recommendation-period display values."""
from __future__ import annotations

import hashlib
import json
import math
import re
from time import monotonic

HEADERS = [
    '目标交易日', '策略包', '分组', '排名', '股票代码', '股票名称', '推荐状态',
    '连续推荐(交易日)', '首次生效日', '起始基准价(元)', '行情快照价(元)',
    '荐股期间累计涨跌', '今日涨跌', '行情日期', '行情获取时点', '收益估值日',
    '缺失原因', '名单版本', '绑定版本',
]
GROUPS = {'IN_SCOPE': '在荐', 'OUTSIDE_PENDING': '待复核', 'OUTSIDE_EXIT': '待退出',
          'OBSERVATION': '观察', 'UNKNOWN': '待核验'}
CHANGES = {'NEW': '新入选', 'CONTINUING': '持续推荐', 'REMOVED': '已移出', 'UNKNOWN': '待核验'}
REASONS = {
    'OPEN_NOT_YET_AVAILABLE': '开盘基准尚未形成', 'CONTINUITY_UNKNOWN': '推荐连续性待核验',
    'NOT_RECOMMENDED': '不属于本次连续推荐', 'BASE_OPEN_MISSING': '缺少首次生效日开盘价',
    'BASE_SUSPENDED': '首次生效日停牌', 'END_OPEN_MISSING': '缺少移出生效日开盘价',
    'QUOTE_NOT_VALID_FOR_SEGMENT': '等待本段有效行情',
}


class DetailDeliveryError(RuntimeError):
    """Only a fixed, non-secret code may cross the delivery boundary."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def cell(value):
    """Keep missing distinct from zero and neutralize spreadsheet formula prefixes."""
    if value is None:
        return 'UNKNOWN'
    if isinstance(value, str):
        return '\u200b' + value if value.lstrip().startswith(('=', '+', '-', '@')) else value
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
        return value
    raise DetailDeliveryError('DETAIL_CELL_INVALID')


def number(value):
    return value if (isinstance(value, (int, float)) and not isinstance(value, bool)
                     and math.isfinite(value)) else None


def percent(value):
    value = number(value)
    # Units from RecommendationReturns/QuoteService are basis points, not percent.
    return 'UNKNOWN' if value is None else cell(f'{value / 100:+.2f}%')


class DetailSnapshotBuilder:
    def __init__(self, history=None, returns=None, enrich=None, timer=monotonic):
        self.history, self.returns, self.enrich, self.timer = history, returns, enrich, timer

    def build(self, now, target, roster, *, decision_date=None):
        from backend.services.advisory_workbench.quotes import SYMBOL
        if self.history is None:
            from backend.services.advisory_workbench.history import HistoryService
            self.history = HistoryService()
        if self.returns is None:
            from backend.services.advisory_workbench.recommendations import RecommendationReturns
            self.returns = RecommendationReturns(clock=lambda: now)
        if self.enrich is None:
            from backend.services.advisory_program import AdvisoryProgramService
            self.enrich = AdvisoryProgramService()._enrich_display_names
        deadline = self.timer() + 90
        programs = []
        if not roster or len(roster) > 100:
            raise DetailDeliveryError('DETAIL_PROGRAM_LIMIT')
        for source in sorted(roster, key=lambda r: r['program_id']):
            if self.timer() >= deadline:
                raise DetailDeliveryError('DETAIL_SNAPSHOT_TIME_LIMIT')
            pid, lid = source['program_id'], source['list_version_id']
            detail = self.history.detail(pid, lid, group='ALL', limit=501, offset=0)
            if (detail.get('program_id') != pid or detail.get('list_version_id') != lid
                    or detail.get('binding_version_id') != source['binding_version_id']
                    or str(detail.get('target_date')) != str(target)
                    or detail.get('status') != 'PUBLISHED' or not detail.get('snapshot_id')):
                raise DetailDeliveryError('DETAIL_LIST_IDENTITY_INVALID')
            items = detail['items']
            if not items or detail['total_count'] > 500 or detail['total_count'] != len(items):
                raise DetailDeliveryError('DETAIL_LIST_EMPTY_OR_LIMIT')
            items = self.enrich(items)
            # Reuse the actual workbench symbol contract (000001.SZ). Invalid
            # source identities must fail, never silently skip every quote lookup.
            if any(not SYMBOL.fullmatch(r.get('symbol', '')) for r in items):
                raise DetailDeliveryError('DETAIL_QUOTE_SYMBOL_INVALID')
            symbols = sorted({r['symbol'] for r in items})
            quotes = {}
            for offset in range(0, len(symbols), 100):
                if self.timer() >= deadline:
                    raise DetailDeliveryError('DETAIL_SNAPSHOT_TIME_LIMIT')
                batch = symbols[offset:offset + 100]
                values = self.returns.get(pid, lid, ','.join(batch))
                if (values.get('program_id') != pid or values.get('list_version_id') != lid
                        or set(values['items']) != set(batch)
                        or any(not isinstance(v.get('recommendation'), dict) for v in values['items'].values())):
                    raise DetailDeliveryError('DETAIL_RETURN_IDENTITY_INVALID')
                quotes.update(values['items'])
            # The quote reads use separate transactions. Recheck the exact saved-list
            # fingerprint before freezing; an updated source must not become mixed data.
            self.history.detail(pid, lid, group='ALL', limit=0, offset=0,
                                snapshot_id=detail['snapshot_id'])
            rows = []
            for item in items:
                quote = quotes.get(item['symbol'], {})
                rec = quote.get('recommendation') or {}
                not_applicable = rec.get('state') == 'NOT_RECOMMENDED' and rec.get('reason_code') == 'NOT_RECOMMENDED'
                def recommendation_field(key, *, numeric=False):
                    value = number(rec.get(key)) if numeric else rec.get(key)
                    return '不适用（非本次连续推荐）' if value is None and not_applicable else value
                reasons = [item.get('membership_reason'), rec.get('reason_code'), quote.get('reason_code')]
                if not quote:
                    reasons.append('QUOTE_SYMBOL_INVALID')
                if number(quote.get('today_return_bps')) is None:
                    reasons.append('缺少有效行情或昨收')
                rows.append([cell(v) for v in [
                    str(target), source['program_name'], GROUPS.get(item.get('membership'), '待核验'),
                    item.get('rank'), item['symbol'], item.get('stock_name'),
                    CHANGES.get(item.get('change'), '待核验'), recommendation_field('consecutive_days', numeric=True),
                    recommendation_field('start_date'), recommendation_field('base_price', numeric=True), number(quote.get('price')),
                    '不适用（非本次连续推荐）' if not_applicable else percent(rec.get('return_bps')),
                    percent(quote.get('today_return_bps')),
                    quote.get('trade_date'), quote.get('observed_at'), recommendation_field('valuation_date'),
                    '；'.join(REASONS.get(r, r) for r in reasons if r) or '无',
                    lid, source['binding_version_id'],
                ]])
            programs.append(dict(program_id=pid, program_name=source['program_name'], list_version_id=lid,
                                 binding_version_id=source['binding_version_id'],
                                 source_snapshot_id=detail['snapshot_id'], cells=[HEADERS, *rows]))
        snapshot = dict(schema_version='advisory_detail_snapshot_v1', captured_at=now.isoformat(),
                        decision_date=str(decision_date or now.date()), target_date=str(target), programs=programs)
        if len(json.dumps(snapshot, ensure_ascii=False).encode()) > 8 * 1024 * 1024:
            raise DetailDeliveryError('DETAIL_SNAPSHOT_SIZE_LIMIT')
        snapshot['sha256'] = fingerprint(snapshot)
        return snapshot


def validate_snapshot(snapshot):
    if (not isinstance(snapshot, dict) or snapshot.get('schema_version') != 'advisory_detail_snapshot_v1'
            or snapshot.get('sha256') != fingerprint({k: v for k, v in snapshot.items() if k != 'sha256'})):
        raise DetailDeliveryError('DETAIL_SNAPSHOT_HASH_INVALID')
    return snapshot


def workbook_tables(snapshot):
    validate_snapshot(snapshot)
    summary = [
        ['AIstock 荐股详情', '发送时冻结快照'], ['决策日', snapshot['decision_date']],
        ['目标交易日', snapshot['target_date']], ['快照生成时间', snapshot['captured_at']],
        ['累计涨跌口径', '本次连续荐股期间：首次生效日开盘至有效行情；移出后冻结于移出生效日开盘。未复权、未扣费。'],
        ['未知值', 'UNKNOWN 表示待核验或尚无有效价格，不代表零收益。非本次连续推荐显示不适用；新入选等待目标日开盘基准。'],
        ['行情说明', '行情仅供展示；以各行行情日期和获取时点为准。打开表格不会更新行情。'],
        ['快照 SHA256', snapshot['sha256']],
        ['策略包', '名单版本', '绑定版本', '列表快照', '行数'],
    ]
    tables = [('使用说明', summary)]
    for p in snapshot['programs']:
        # Stable suffix avoids duplicate/invalid worksheet names and makes recovery deterministic.
        short = re.sub(r'[\[\]:*?/\\]', '_', p['program_name'])[:45]
        title = short + '_' + hashlib.sha256(p['program_id'].encode()).hexdigest()[:8]
        tables.append((title, p['cells']))
        summary.append([cell(p[k]) for k in ('program_name', 'list_version_id', 'binding_version_id',
                                             'source_snapshot_id')] + [len(p['cells']) - 1])
    return tables
