"""Fast overview: configurations and publication headers only, no financial scan."""
from collections import Counter
from datetime import datetime, timedelta

from backend.services.advisory_program import _json_ready
from .comparison_repository import ComparisonRepository
from .comparison_metrics import known_empty_scope
from .projection import calendar_context, membership, run_state
from .quotes import CN


class BriefService:
    def __init__(self, repository=None, clock=None):
        self.repository = repository or ComparisonRepository()
        self.clock = clock or (lambda: datetime.now(CN))

    def overview(self, status='ENABLED'):
        now = self.clock()
        with self.repository.transaction():
            repo = self.repository
            programs = repo.programs(status)
            ids = [p['program_id'] for p in programs]
            target = calendar_context(repo.calendar(now.date() - timedelta(days=7), now.date()), now.date())
            stats = {r['program_id']: r for r in repo.brief_versions(ids, target)}
            versions = {r['list_version_id']: r for r in repo.version_headers(
                list({r[k] for r in stats.values() for k in ('latest_id', 'selected_id') if r[k]}))}
            latest_ids = list({r['latest_id'] for r in stats.values() if r.get('latest_id')})
            latest_rows = repo.latest_items(latest_ids) if latest_ids else []
            bindings = {r['program_id']: r for r in repo.active_bindings(ids)}
            runs = {r['program_id']: r for r in repo.latest_runs(ids, target)}
            models = repo.package_models(list({pkg for p in programs for pkg in (
                bindings.get(p['program_id'], p).get('package_ids') or [])}))
            cards = []
            for p in programs:
                pid = p['program_id']
                stat = stats.get(pid, {})
                selected = versions.get(stat.get('selected_id'))
                latest = versions.get(stat.get('latest_id'))
                active = bindings.get(pid)
                packages = (active or p).get('package_ids') or []
                latest_members = [r for r in latest_rows if latest and r['list_version_id'] == latest['list_version_id']]
                counts = Counter(r['symbol'] for r in latest_members)
                verified = latest is not None and (bool(latest_members) or known_empty_scope(latest, []))
                verified = verified and all(counts[r['symbol']] == 1 and membership(r) != 'UNKNOWN'
                    and r['program_id'] == pid and r['binding_version_id'] == latest['binding_version_id'] for r in latest_members)
                in_scope = sum(membership(r) == 'IN_SCOPE' for r in latest_members) if verified else None
                public_program = {k: p.get(k) for k in ('program_id', 'program_name', 'status', 'enabled_since', 'target_count', 'package_ids')}
                cards.append(dict(program=public_program, program_id=pid, active_binding=active,
                    selected=selected, latest=latest, default_target_date=target, target_date=target,
                    latest_in_scope_count=in_scope,
                    runtime=run_state(runs.get(pid), has_target=bool(selected), configured=p['status']),
                    package_models=[r for r in models if r['package_id'] in packages],
                    coverage=dict(first_published_target_date=stat.get('first_published')),
                    versions_count=stat.get('versions_count', 0)))
        priority = {'ASSET_BLOCKED': 0, 'FAILED': 1, 'WAITING_DATA': 2, 'NO_CURRENT_LIST': 3, 'PUBLISHED': 4}
        cards.sort(key=lambda c: (priority.get(c['runtime']['state'], 3), c['program']['program_name']))
        return _json_ready(dict(schema_version='advisory_overview_brief_v1', generated_at=now,
            items=cards, count=len(cards), status_counts=dict(Counter(c['runtime']['state'] for c in cards))))
