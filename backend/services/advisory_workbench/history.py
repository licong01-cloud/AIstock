"""All saved versions; exact immutable list identity, metadata-first pagination."""
from collections import defaultdict
from datetime import date, datetime, timedelta

from backend.services.advisory_program import _json_ready
from .comparison_repository import ComparisonRepository
from .projection import WorkbenchError, daily_projection, identity
from .quotes import CN


class HistoryService:
    def __init__(self, repository=None, clock=None):
        self.repository = repository or ComparisonRepository()
        self.clock = clock or (lambda: datetime.now(CN))

    def versions(self, program_id, *, start=None, end=None, source='ALL', status='ALL', limit=20, offset=0):
        if start and end and start > end:
            raise WorkbenchError('INVALID_DATE_RANGE', '开始日期不能晚于结束日期。')
        with self.repository.transaction():
            self.repository.program(program_id)
            rows, total = self.repository.history(program_id, start=start, end=end, source=source,
                                                   status=status, limit=limit, offset=offset)
        return _json_ready(dict(program_id=program_id, items=rows, total_count=total, limit=limit,
                                offset=offset, generated_at=self.clock(), schema_version='advisory_history_v1'))

    def detail(self, program_id, version_id, *, group='IN_SCOPE', change=None, q=None, limit=20,
               offset=0, snapshot_id=None):
        repo = self.repository
        with repo.transaction():
            repo.program(program_id)
            version = repo.version(program_id, version_id)
            previous = None
            if version.get('previous_list_version_id'):
                try:
                    previous = repo.version(program_id, version['previous_list_version_id'])
                except WorkbenchError as exc:
                    if exc.status != 404:
                        raise
            ids = [version_id] + ([previous['list_version_id']] if previous else [])
            items = repo.items(program_id, ids)
            stamp = identity((version, previous, items))
            if snapshot_id and snapshot_id != stamp:
                raise WorkbenchError('SNAPSHOT_CHANGED', '名单读取期间已更新，请刷新。', 409)
            expected = {v['list_version_id']: v['binding_version_id'] for v in (version, previous) if v}
            items = [r if r['program_id'] == program_id and r['binding_version_id'] == expected.get(r['list_version_id'])
                     else dict(r, action='UNKNOWN', reason_code='LIST_IDENTITY_UNKNOWN') for r in items]
            prior_known = False
            if not version.get('previous_list_version_id'):
                prior_known = not repo.has_prior(version)
            if previous and previous['binding_version_id'] == version['binding_version_id'] and previous['source'] == version['source']:
                first, last = date.fromisoformat(str(previous['trade_date'])), date.fromisoformat(str(version['trade_date']))
                facts = {str(r['cal_date']): r['is_trading'] for r in repo.calendar(first, last)}
                d = first + timedelta(days=1)
                prior_known = first < last and facts.get(str(first)) is True and facts.get(str(last)) is True
                while d < last:
                    prior_known = prior_known and facts.get(str(d)) is False
                    d += timedelta(days=1)
            rows, summary = daily_projection([r for r in items if r['list_version_id'] == version_id],
                [r for r in items if previous and r['list_version_id'] == previous['list_version_id']], prior_known=prior_known)
            filtered = [r for r in rows if (group == 'ALL' or (r['change'] == 'REMOVED' if group == 'REMOVED' else r['membership'] == group))
                        and (not change or r['change'] == change)
                        and (not q or q.lower() in (r['symbol'] + ' ' + str(r.get('stock_name') or '')).lower())]
            page = filtered[offset:offset + limit]
            links = defaultdict(set)
            for link in repo.entry_links(program_id, version, [r['symbol'] for r in page if not r.get('episode_id')]):
                links[link['symbol']].add(link['episode_id'])
            candidate_ids = sorted({eid for ids in links.values() for eid in ids})
            records, _ = repo.episodes(program_id, {'episode_ids': candidate_ids}) if candidate_ids else ([], 0)
            identities = {r['episode_id']: r for r in records}
            for row in page:
                candidates = links[row['symbol']]
                if row.get('episode_id') or not candidates:
                    continue
                candidate = identities.get(next(iter(candidates))) if len(candidates) == 1 else None
                if candidate and candidate['symbol'] == row['symbol'] and candidate['program_id'] == program_id and candidate['origin_binding_id'] == version['binding_version_id']:
                    row.update(episode_id=candidate['episode_id'], episode_link_state='LINKED_ENTER')
                else:
                    row['episode_link_state'] = 'AMBIGUOUS_OR_MISMATCHED_ENTER'
        return _json_ready(dict(schema_version='advisory_history_list_v1', program_id=program_id,
            binding_version_id=version['binding_version_id'], list_version_id=version_id,
            target_date=version['trade_date'], selected=version, snapshot_id=stamp, generated_at=self.clock(),
            status=version['version_status'], items=page, total_count=len(filtered),
            summary=summary, limit=limit, offset=offset))
