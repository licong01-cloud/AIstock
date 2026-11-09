from backend.services.advisory_workbench.comparison_repository import ComparisonRepository


class Repo(ComparisonRepository):
    def __init__(self):
        self.queries = []

    def rows(self, sql, params=()):
        self.queries.append((sql, params))
        return [{'n': 43}] if 'count(*) AS n' in sql else []


def test_history_paginates_metadata_filters_without_items_episodes_or_price_hash():
    repo = Repo()
    rows, count = repo.history('p', start='2026-01-01', end='2026-11-01', source='REPLAY',
                                status='REPLAY', limit=20, offset=20)
    assert rows == [] and count == 43
    assert len(repo.queries) == 2
    sql, params = repo.queries[-1]
    assert params[-2:] == [20, 20] and params[:4] == ['p', '2026-01-01', '2026-11-01', 'REPLAY']
    assert 'r.run_type' in sql and 'v.version_status' in sql
    assert not any(word in sql for word in ('kline', 'episode_return', 'list_item', 'md5', 'string_agg'))


def test_comparison_reads_original_item_slots_and_bounded_versions():
    repo = Repo()
    repo.versions_many(['p'], '2026-01-01', '2026-11-01')
    repo.items_many(['v'])
    version_sql = repo.queries[0][0]
    item_sql = repo.queries[1][0]
    assert 'LIMIT 50001' in version_sql and 'LIMIT 1000001' in item_sql
    assert 'reason_code' in item_sql and 'rank<=' not in item_sql and 'rank <=' not in item_sql
    assert all(sql.lstrip().startswith('SELECT') for sql, _ in repo.queries)


def test_brief_never_requests_episode_or_price_watermarks_and_whitelists_program():
    from contextlib import contextmanager
    from datetime import date, datetime
    from backend.services.advisory_workbench.brief import BriefService
    from backend.services.advisory_workbench.quotes import CN

    class BriefRepo:
        with_latest = False
        @contextmanager
        def transaction(self):
            yield self

        def programs(self, status):
            return [dict(program_id='p', program_name='p', status='ENABLED', package_ids=[], private_payload='excluded')]

        def calendar(self, *args):
            return [dict(cal_date=date(2026, 11, 9), is_trading=True)]

        def brief_versions(self, ids, target):
            return [dict(program_id='p', latest_id='v', selected_id='v', versions_count=1)] if self.with_latest else []

        def version_headers(self, ids):
            return [dict(list_version_id='v', binding_version_id='b', active_count=1)] if ids else []

        def latest_items(self, ids):
            assert ids == ['v']
            return [dict(program_id='p', binding_version_id='b', list_version_id='v', symbol='A',
                         action='HOLD', item_state='ACTIVE'),
                    dict(program_id='p', binding_version_id='b', list_version_id='v', symbol='B',
                         action='WATCH', reason_code='PENDING_TARGET_OPEN_ENTRY'),
                    dict(program_id='p', binding_version_id='b', list_version_id='v', symbol='C',
                         action='WATCH', reason_code='OUTSIDE_ENTRY_THRESHOLD')]

        def active_bindings(self, ids):
            return []

        def latest_runs(self, ids, target):
            return []

        def package_models(self, ids):
            return []

        def __getattr__(self, name):
            raise AssertionError('Unexpected financial or history read: ' + name)
    result = BriefService(BriefRepo(), lambda: datetime(2026, 11, 9, 16, tzinfo=CN)).overview()
    assert result['count'] == 1 and 'metrics' not in result['items'][0]
    assert 'private_payload' not in result['items'][0]['program']
    repository = BriefRepo()
    repository.with_latest = True
    result = BriefService(repository, lambda: datetime(2026, 11, 9, 16, tzinfo=CN)).overview()
    assert result['items'][0]['latest']['active_count'] == 1
    assert result['items'][0]['latest_in_scope_count'] == 2  # Pending target-open entry is already recommended.


def test_history_program_shell_has_no_financial_read_or_placeholder_metrics():
    from contextlib import contextmanager
    from datetime import date, datetime
    from backend.services.advisory_workbench.brief import BriefService
    from backend.services.advisory_workbench.quotes import CN

    class MetadataRepo:
        @contextmanager
        def transaction(self):
            yield self

        def program(self, pid):
            return dict(program_id=pid, program_name='program', status='ENABLED', package_ids=[])

        def versions(self, pid):
            return [dict(program_id=pid, list_version_id='v', binding_version_id='b',
                         version_status='PUBLISHED', trade_date=date(2026, 11, 9))]

        def bindings(self, pid):
            return [dict(binding_version_id='b', activation_status='ACTIVE', package_ids=[])]

        def calendar(self, start, end):
            return [dict(cal_date=date(2026, 11, i), is_trading=date(2026, 11, i).weekday() < 5) for i in range(2, 11)]

        def forward(self, *args):
            return []

        def package_models(self, ids):
            return []

        def __getattr__(self, name):
            raise AssertionError('History shell must not read financial history: ' + name)
    view = BriefService(MetadataRepo(), lambda: datetime(2026, 11, 9, 16, tzinfo=CN)).program('p')
    assert view['schema_version'] == 'advisory_program_metadata_v1'
    assert view['selected']['list_version_id'] == 'v' and 'metrics' not in view
    assert view['metric_read'] == 'NOT_REQUESTED'
