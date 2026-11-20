"""Mobile summary and desktop overview of the same immutable published snapshot."""
from __future__ import annotations

import json
from collections import Counter
from urllib.parse import urlsplit

from .notification_details import CHANGES, GROUPS, HEADERS, DetailDeliveryError, cell, validate_snapshot

PRESENTATION_VERSION = 'advisory_feishu_dual_view_v1'
MOBILE_TOP_N = 3
MAX_MOBILE_PROGRAMS = 8
CARD_BUDGET = 18 * 1024  # Leave room under the existing 20KB sender limit for signing.


def display(value, limit=80):
    return ' '.join(str(value).lstrip('\u200b').split())[:limit]


def program_rows(program):
    cells = program.get('cells') or []
    if not cells or cells[0] != HEADERS or any(len(row) != len(HEADERS) for row in cells[1:]):
        raise DetailDeliveryError('DETAIL_PRESENTATION_COLUMNS_INVALID')
    return [dict(zip(HEADERS, row)) for row in cells[1:]]


def statistics(program):
    rows = program_rows(program)
    active = [r for r in rows if r['分组'] == GROUPS['IN_SCOPE']]
    groups = Counter(r['分组'] for r in rows)
    changes = Counter(r['推荐状态'] for r in rows)
    return dict(rows=rows, active=active, groups=groups, changes=changes,
                quote_known=sum(isinstance(r['行情快照价(元)'], (int, float)) for r in active),
                return_known=sum(r['荐股期间累计涨跌'] != 'UNKNOWN'
                                 and not str(r['荐股期间累计涨跌']).startswith('不适用') for r in active))


def overview_table(snapshot):
    validate_snapshot(snapshot)
    header = ['策略包', '目标交易日', '在荐', '新入选(已确认)', '持续推荐(已确认)', '移出(已确认)',
              '推荐状态待核验', '观察', '待复核', '待退出', '分组待核验', '名单总行数',
              '在荐有效报价数', '在荐收益可用数', '名单版本', '绑定版本', '快照时间']
    cells = [header]
    for p in snapshot['programs']:
        s = statistics(p)
        cells.append([cell(p['program_name']), snapshot['target_date'], len(s['active']),
                      s['changes'][CHANGES['NEW']], s['changes'][CHANGES['CONTINUING']],
                      s['changes'][CHANGES['REMOVED']], s['changes'][CHANGES['UNKNOWN']],
                      s['groups'][GROUPS['OBSERVATION']], s['groups'][GROUPS['OUTSIDE_PENDING']],
                      s['groups'][GROUPS['OUTSIDE_EXIT']], s['groups'][GROUPS['UNKNOWN']], len(s['rows']),
                      s['quote_known'], s['return_known'], p['list_version_id'], p['binding_version_id'],
                      snapshot['captured_at']])
    return cells


def _stock_lines(rows, top_n):
    def rank(row):
        value = row['排名']
        return value if type(value) in (int, float) and value > 0 else float('inf')
    selected = sorted(rows, key=rank)[:top_n]  # Stable ties preserve saved-list order.
    lines = []
    for row in selected:
        label = str(row['排名']) if rank(row) != float('inf') else '排名待核验'
        name = display(row['股票名称'], 20)
        if name == 'UNKNOWN':
            name = '名称待核验'
        days = row['连续推荐(交易日)']
        day_label = f'连续{days}日' if type(days) in (int, float) else '连续天数待核验'
        value = display(row['荐股期间累计涨跌'])
        if value == 'UNKNOWN':
            value = '开盘基准待形成' if '开盘基准尚未形成' in row['缺失原因'] else '累计涨跌待核验'
        else:
            value = '累计' + value
        lines.extend([f"{label}. {name} · {display(row['股票代码'], 16)}",
                      f"{display(row['推荐状态'], 12)} · {day_label} · {value}"])
    return lines


def mobile_card(payload, url, *, top_n=MOBILE_TOP_N):
    from .feishu_sheets import document_url
    snapshot = validate_snapshot(payload['detail_snapshot'])
    document_url(url, urlsplit(url).path.rsplit('/', 1)[-1])
    if top_n not in {3, 5} or not snapshot.get('programs'):
        raise DetailDeliveryError('DETAIL_MOBILE_PREVIEW_INVALID')
    programs = snapshot['programs']
    preview = programs[:MAX_MOBILE_PROGRAMS]
    while preview:
        elements = [dict(tag='div', text=dict(tag='plain_text', content=(
            ('【测试】历史名单样例，非新的荐股通知。\n' if payload['subject'].startswith('【测试】') else '') +
            f"决策日 {snapshot['decision_date']} · 目标日 {snapshot['target_date']}\n"
            f"{len(programs)} 个策略包已发布；各包按原排名预览最多 {top_n} 只在荐股票。")))]
        for p in preview:
            s = statistics(p)
            lines = [display(p['program_name'], 60),
                     f"在荐 {len(s['active'])} · 新入选 {s['changes'][CHANGES['NEW']]} · "
                     f"持续 {s['changes'][CHANGES['CONTINUING']]} · 移出 {s['changes'][CHANGES['REMOVED']]}"]
            if s['changes'][CHANGES['UNKNOWN']]:
                lines.append(f"另有 {s['changes'][CHANGES['UNKNOWN']]} 项推荐状态待核验。")
            lines.extend(_stock_lines(s['active'], top_n) or ['本包没有在荐股票，其他分组见完整表格。'])
            elements.extend([dict(tag='hr'), dict(tag='div', text=dict(tag='plain_text', content='\n'.join(lines)))])
        if len(preview) < len(programs):
            elements.append(dict(tag='div', text=dict(tag='plain_text', content=(
                f'此卡展示 {len(preview)}/{len(programs)} 个策略包，其余策略及全部股票请打开完整表格。'))))
        elements.extend([
            dict(tag='action', actions=[dict(tag='button', text=dict(tag='plain_text', content='打开完整荐股表格'),
                                            type='primary', url=url)]),
            dict(tag='note', elements=[dict(tag='plain_text', content=(
                f"快照 {snapshot['captured_at']}；累计涨跌按连续荐股期间计算。\n"
                '行情为冻结快照，各股日期及缺失原因见表格。'))]),
        ])
        card = dict(config={'wide_screen_mode': False},
                    header=dict(title=dict(tag='plain_text', content=display(payload['subject'], 80)), template='blue'),
                    elements=elements)
        if len(json.dumps(card, ensure_ascii=True, allow_nan=False).encode()) <= CARD_BUDGET:
            return card
        preview = preview[:-1]
    raise DetailDeliveryError('FEISHU_MOBILE_CARD_TOO_LARGE')
