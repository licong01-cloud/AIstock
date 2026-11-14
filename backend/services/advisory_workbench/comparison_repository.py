"""Bounded read-only facts. Publication identity, never regenerated rankings."""
from .projection import WorkbenchError
from .repository import ReadRepository, VERSION_COLUMNS


SOURCE_SQL = """CASE WHEN r.run_type='REPLAY' THEN 'REPLAY'
 WHEN EXISTS (SELECT 1 FROM app.advisory_forward_run f
 WHERE f.program_id=v.program_id AND f.review_run_id=v.review_run_id) THEN 'FORWARD'
 ELSE 'UNKNOWN' END"""
HEADER_SQL = ', '.join('v.' + part.strip() for part in VERSION_COLUMNS.split(','))


class ComparisonRepository(ReadRepository):
    def first_date(self, program_ids, source, binding_ids=None):
        params = [program_ids]
        where = ["v.program_id=ANY(%s)", "v.version_status IN ('PUBLISHED','REPLAY')"]
        if source != 'ALL':
            where.append(f'({SOURCE_SQL})=%s')
            params.append(source)
        if binding_ids is not None:
            where.append('v.binding_version_id=ANY(%s)')
            params.append(binding_ids)
        return self.rows("""SELECT min(v.trade_date) AS first_date FROM app.advisory_recommendation_list_version v
            LEFT JOIN app.advisory_review_run r ON r.program_id=v.program_id AND r.review_run_id=v.review_run_id
            WHERE """ + ' AND '.join(where), params)[0]['first_date']

    def versions_many(self, program_ids, start, end):
        rows = self.rows(f"""SELECT {HEADER_SQL}, {SOURCE_SQL} AS source
            FROM app.advisory_recommendation_list_version v
            LEFT JOIN app.advisory_review_run r ON r.program_id=v.program_id AND r.review_run_id=v.review_run_id
            WHERE v.program_id=ANY(%s) AND v.trade_date BETWEEN %s AND %s
              AND v.version_status IN ('PUBLISHED','REPLAY')
            ORDER BY v.trade_date,v.list_version_id LIMIT 50001""", (program_ids, start, end))
        if len(rows) > 50000:
            raise WorkbenchError('COMPARISON_RANGE_LIMIT', '所选范围过大，请缩短时间或减少策略。')
        return rows

    def items_many(self, version_ids):
        if not version_ids:
            return []
        rows = self.rows("""SELECT list_version_id,program_id,binding_version_id,symbol,item_state,
            action,reason_code,rank,episode_id FROM app.advisory_recommendation_list_item
            WHERE list_version_id=ANY(%s)
            ORDER BY list_version_id,rank NULLS LAST,symbol LIMIT 1000001""", (version_ids,))
        if len(rows) > 1000000:
            raise WorkbenchError('COMPARISON_ITEM_LIMIT', '所选名单条目过多，请缩短时间。')
        return rows

    def history(self, program_id, *, start=None, end=None, source='ALL', status='ALL', limit=20, offset=0):
        where, params = ['v.program_id=%s'], [program_id]
        for value, condition in ((start, 'v.trade_date>=%s'), (end, 'v.trade_date<=%s'),
                                 (None if status == 'ALL' else status, 'v.version_status=%s')):
            if value:
                where.append(condition)
                params.append(value)
        if source != 'ALL':
            where.append(f'({SOURCE_SQL})=%s')
            params.append(source)
        base = f"""FROM app.advisory_recommendation_list_version v
            LEFT JOIN app.advisory_review_run r ON r.program_id=v.program_id AND r.review_run_id=v.review_run_id
            WHERE {' AND '.join(where)}"""
        total = self.rows('SELECT count(*) AS n ' + base, params)[0]['n']
        rows = self.rows(f"SELECT {HEADER_SQL}, {SOURCE_SQL} AS source " +
                         base + ' ORDER BY v.trade_date DESC,v.created_at DESC,v.list_version_id LIMIT %s OFFSET %s',
                         [*params, limit, offset])
        return rows, total

    def version(self, program_id, version_id):
        rows = self.rows(f"""SELECT {HEADER_SQL}, {SOURCE_SQL} AS source
            FROM app.advisory_recommendation_list_version v
            LEFT JOIN app.advisory_review_run r ON r.program_id=v.program_id AND r.review_run_id=v.review_run_id
            WHERE v.program_id=%s AND v.list_version_id=%s""", (program_id, version_id))
        if len(rows) != 1:
            raise WorkbenchError('HISTORY_VERSION_NOT_FOUND', '该策略没有此历史名单版本。', 404)
        return rows[0]

    def has_prior(self, version):
        return self.rows("""SELECT EXISTS (SELECT 1 FROM app.advisory_recommendation_list_version
            WHERE program_id=%s AND version_status=%s AND trade_date<=%s AND list_version_id<>%s) AS present""",
            (version['program_id'], version['version_status'], version['trade_date'], version['list_version_id']))[0]['present']

    def brief_versions(self, program_ids, target):
        return self.rows("""SELECT program_id,count(*) AS versions_count,
            min(trade_date) FILTER (WHERE version_status='PUBLISHED') AS first_published,
            (array_agg(list_version_id ORDER BY trade_date DESC,created_at DESC)
             FILTER (WHERE version_status='PUBLISHED'))[1] AS latest_id,
            (array_agg(list_version_id ORDER BY created_at DESC)
             FILTER (WHERE version_status='PUBLISHED' AND trade_date=%s))[1] AS selected_id
            FROM app.advisory_recommendation_list_version WHERE program_id=ANY(%s) GROUP BY program_id""", (target, program_ids))

    def version_headers(self, version_ids):
        if not version_ids:
            return []
        return self.rows('SELECT ' + VERSION_COLUMNS +
                         ' FROM app.advisory_recommendation_list_version WHERE list_version_id=ANY(%s)', (version_ids,))

    def latest_items(self, version_ids):
        return self.rows("""SELECT list_version_id,program_id,binding_version_id,symbol,item_state,action,reason_code
            FROM app.advisory_recommendation_list_item WHERE list_version_id=ANY(%s)
            ORDER BY list_version_id,symbol""", (version_ids,)) if version_ids else []

    def active_bindings(self, program_ids):
        rows = self.rows("""SELECT program_id,binding_version_id,program_version,package_ids,activation_status,
            effective_from_trade_date FROM app.advisory_strategy_binding_version
            WHERE program_id=ANY(%s) AND activation_status='ACTIVE'""", (program_ids,))
        if len(rows) != len({r['program_id'] for r in rows}):
            raise WorkbenchError('ACTIVE_BINDING_AMBIGUOUS', '策略存在多个生效绑定，请核对源身份。', 503)
        return rows

    def latest_runs(self, program_ids, target):
        return self.rows("""SELECT DISTINCT ON (program_id) program_id,publication_status,settlement_status,
            target_trade_date,decision_as_of_trade_date,attempt_count,last_stage,last_reason_code,last_error_json,updated_at
            FROM app.advisory_forward_run WHERE program_id=ANY(%s)
            ORDER BY program_id,(target_trade_date=%s) DESC,updated_at DESC""", (program_ids, target))
