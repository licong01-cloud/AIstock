"""Native publication comparison; read-only, bounded, selected-end financial clock."""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from statistics import mean

from backend.services.advisory_program import _json_ready
from backend.services.advisory_historical_range.program_performance import (
    cost_contract, reporting_cutoff,
)
from .comparison_cache import comparison_cache
from .comparison_market import ComparisonMarketReader, Market, observation_keys
from .comparison_metrics import cohort, summarize
from .comparison_portfolio import reference_portfolio
from .comparison_repository import ComparisonRepository
from .comparison_tracking import read_tracking, tracking_metrics
from .projection import WorkbenchError, identity
from .quotes import CN


class ComparisonService:
    def __init__(self, repository=None, market_reader=None, cache=None, clock=None):
        self.repository = repository or ComparisonRepository()
        self.market_reader = market_reader or ComparisonMarketReader()
        self.cache = cache or comparison_cache
        self.clock = clock or (lambda: datetime.now(CN))

    def get(self, *, series=False, force_refresh=False, **filters):
        now = self.clock()
        key = identity((filters, reporting_cutoff(now), 'advisory_native_comparison_v1'))
        full = self.cache.get(key, lambda: self.build(now=now, **filters), force=force_refresh)
        if series:
            return {k: full[k] for k in ('schema_version', 'source_version', 'generated_at', 'range', 'series')}
        return {k: v for k, v in full.items() if k != 'series'}

    def build(self, *, now, status='ENABLED', program_ids='', period='60', start=None, end=None,
              source='FORWARD', binding='CURRENT', sample='COMMON', buy_bps=.95, sell_bps=5.95,
              slippage_bps=0):
        cutoff = reporting_cutoff(now)
        end_day = date.fromisoformat(end) if end and period == 'CUSTOM' else cutoff
        if end_day > cutoff:
            end_day = cutoff
        if period == 'CUSTOM' and (not start or not end):
            raise WorkbenchError('CUSTOM_RANGE_REQUIRED', '自定义时间需要同时选择开始和结束日期。')
        cost = cost_contract(buy_bps + slippage_bps, sell_bps + slippage_bps)
        repo = self.repository
        with repo.transaction():
            programs = repo.programs(status)
            selected = set(filter(None, program_ids.split(',')))
            if selected:
                if selected - {p['program_id'] for p in programs}:
                    raise WorkbenchError('PROGRAM_SELECTION_CHANGED', '所选策略已变化，请刷新策略概况。', 409)
                programs = [p for p in programs if p['program_id'] in selected]
            if not programs or len(programs) > 20:
                raise WorkbenchError('PROGRAM_SELECTION_REQUIRED', '请选择1至20个有效策略。')
            ids = [p['program_id'] for p in programs]
            active = {r['program_id']: r for r in repo.active_bindings(ids)}
            earliest = end_day - timedelta(days=400)
            if period == 'ALL':
                earliest = repo.first_date(ids, source, [b['binding_version_id'] for b in active.values()]
                                          if binding == 'CURRENT' else None) or end_day
            if period == 'CUSTOM':
                earliest = date.fromisoformat(start)
                if earliest < date(1990, 1, 1):
                    raise WorkbenchError('INVALID_DATE_RANGE', '开始日期不能早于1990年。')
            calendar = repo.calendar(earliest, end_day)
            days = sorted(str(r['cal_date']) for r in calendar if r['is_trading'] and r['cal_date'] <= end_day)
            if period == 'CUSTOM':
                start_day = date.fromisoformat(start)
            elif period == 'YTD':
                start_day = date(end_day.year, 1, 1)
            elif period == 'ALL':
                start_day = earliest
            else:
                count = int(period)
                if len(days) < count:
                    raise WorkbenchError('CALENDAR_UNAVAILABLE', '正式交易日历没有覆盖所选时间。', 503)
                start_day = date.fromisoformat(days[-count])
            if not earliest <= start_day <= end_day:
                raise WorkbenchError('INVALID_DATE_RANGE', '开始日期不能晚于结束日期。')
            all_versions = repo.versions_many(ids, start_day, end_day)
            versions = [v for v in all_versions if (source == 'ALL' or v['source'] == source)
                and (binding == 'ALL' or v['binding_version_id'] == active.get(v['program_id'], {}).get('binding_version_id'))]
            raw_items = repo.items_many([v['list_version_id'] for v in versions])
            episodes = read_tracking(repo, ids, start_day, end_day)
            if len(episodes) > 100000:
                raise WorkbenchError('COMPARISON_EPISODE_LIMIT', '跟踪样本超过读取限额，请缩短时间。')
            symbols = sorted({r['symbol'] for r in raw_items + episodes})
            observed = observation_keys(versions, raw_items, episodes, calendar, end_day)
            market_raw = self.market_reader.load(symbols=symbols, start=start_day, end=end_day,
                cutoff=end_day, benchmark='000300.SH', observations=observed, calendar=calendar)
        market = Market(market_raw)
        if not market.covers(str(start_day), str(end_day)):
            raise WorkbenchError('CALENDAR_UNAVAILABLE', '正式交易日历存在缺口，暂不能比较。', 503)
        items = defaultdict(list)
        for row in raw_items:
            items[row['list_version_id']].append(row)
        arms, outcomes = [], {}
        for p in programs:
            bound = sorted({v['binding_version_id'] for v in all_versions if v['program_id'] == p['program_id']})
            if binding == 'CURRENT':
                bound = [active.get(p['program_id'], {}).get('binding_version_id')]
            for bid in bound or [None]:
                arm_id = identity((p['program_id'], bid))[:20]
                native = [v for v in versions if v['program_id'] == p['program_id'] and v['binding_version_id'] == bid]
                daily = [cohort(v, items[v['list_version_id']], market, str(end_day), cost) for v in native]
                duplicates = Counter(r['date'] for r in daily)
                uncertain_dates = {str(v['trade_date']) for v in all_versions if v['program_id'] == p['program_id']
                    and v['binding_version_id'] == bid and v['source'] == 'UNKNOWN' and source not in {'UNKNOWN', 'ALL'}}
                for row in daily:
                    if duplicates[row['date']] > 1:
                        row.update(status='DUPLICATE_PUBLICATION', net_bps=None, gross_bps=None, excess_bps=None,
                                   unknown_slots=max(row['unknown_slots'], row['original_slots']))
                    elif row['date'] in uncertain_dates:
                        row.update(status='PUBLICATION_SOURCE_UNKNOWN', net_bps=None, gross_bps=None, excess_bps=None,
                                   unknown_slots=max(row['unknown_slots'], row['original_slots']))
                outcomes[arm_id] = daily
                arms.append(dict(arm_id=arm_id, program_id=p['program_id'],
                    name=p['program_name'] + (f' · {(bid or "UNKNOWN")[-8:]}' if binding == 'ALL' else ''),
                    binding_version_id=bid, native=native,
                    publication_start=str(active.get(p['program_id'], {}).get('effective_from_trade_date') or '')
                        if binding == 'CURRENT' else min((str(v['trade_date']) for v in all_versions
                            if v['program_id'] == p['program_id'] and v['binding_version_id'] == bid), default='')))
        if len(arms) > 50:
            raise WorkbenchError('COMPARISON_BINDING_LIMIT', '历史绑定过多，请减少参与策略或缩短时间。')
        sets = [{r['date'] for r in outcomes[a['arm_id']] if r['excess_bps'] is not None} for a in arms]
        common = set.intersection(*sets) if sets else set()
        result, charts = [], []
        for arm in arms:
            daily = outcomes[arm['arm_id']]
            rows = [r for r in daily if r['date'] in common] if sample == 'COMMON' else daily
            eligible = [r for r in rows if r['net_bps'] is not None]
            native = arm.pop('native')
            entered = [r for r in episodes if r['program_id'] == arm['program_id']
                       and r.get('binding_version_id') == arm['binding_version_id']
                       and (source == 'ALL' or r['source'] == source)]
            # ALL starts at first actual selected publication, avoiding years of empty chart points.
            nav_start = str(start_day)
            if period == 'ALL' and versions:
                nav_start = min(str(v['trade_date']) for v in versions)
            headers = [v for v in all_versions if v['program_id'] == arm['program_id'] and v['binding_version_id'] == arm['binding_version_id']]
            unknown_dates = {str(v['trade_date']) for v in headers if v['source'] == 'UNKNOWN' and source not in {'UNKNOWN', 'ALL'}}
            nav = reference_portfolio(native, items, market, nav_start, str(end_day), cost, unknown_dates=unknown_dates)
            benchmark = reference_portfolio(native, items, market, nav_start, str(end_day), cost, benchmark=True, unknown_dates=unknown_dates)
            if not native:
                for view in (nav, benchmark):
                    view.update(status='NOT_EVALUATED', reason='NO_PUBLICATIONS_IN_SCOPE', total_return_bps=None,
                        max_drawdown_bps=None, final_nav=None, cash_fraction=None, invested_fraction=None, cost_drag_bps=None, series=[])
            original = summarize(daily)
            first_publication = arm.pop('publication_start')
            expected = [d for d in market.days if max(nav_start, first_publication) <= d <= str(end_day)]
            scoped_dates = {r['date'] for r in daily}
            all_dates = {str(v['trade_date']) for v in headers}
            missing = sorted(set(expected) - all_dates)
            coverage = dict(published_count=len(native), mature_count=sum(bool(r['maturity']) and r['maturity'] <= str(end_day) for r in daily),
                evaluable_count=sum(r['net_bps'] is not None for r in daily), compared_count=len(eligible),
                common_count=len(common), missing_publication_dates=missing,
                missing_publication_count=len(missing), original_slots=original['original_slots'],
                unknown_slots=original['unknown_slots'], cash_slots=original['cash_slots'],
                status_counts=original['status_counts'], source_counts=dict(Counter(v['source'] for v in native)),
                source_excluded_date_count=len(set(expected) & (all_dates - scoped_dates)),
                unverified_origin_episode_count=sum(r['program_id'] == arm['program_id'] and
                    (not r.get('binding_version_id') or r['source'] == 'UNKNOWN') for r in episodes))
            monthly = defaultdict(list)
            for row in rows:
                if row['excess_bps'] is not None:
                    monthly[row['date'][:7]].append(row['excess_bps'])
            charts.append(dict(arm_id=arm['arm_id'], name=arm['name'],
                nav=self.downsample(nav.pop('series'), max(30, min(800, 3000 // max(1, 2 * len(arms))))),
                benchmark=self.downsample(benchmark.pop('series'), max(30, min(800, 3000 // max(1, 2 * len(arms))))),
                monthly=[dict(month=m, excess_bps=mean(v), count=len(v)) for m, v in sorted(monthly.items())]))
            result.append(dict(**arm, fixed_5td=summarize(rows), coverage=coverage,
                tracking=tracking_metrics(entered, market, str(start_day), str(end_day), cost),
                portfolio=nav, benchmark_portfolio=benchmark))
        market_identity = {**market_raw, 'suspended': sorted(market_raw['suspended'])}
        contract = dict(cost=cost, start=str(start_day), end=str(end_day), source=source, binding=binding, sample=sample,
                        arms=[(r['arm_id'], r['binding_version_id'], r['name']) for r in result], version='native_comparison_v1')
        stamp = identity((all_versions, raw_items, episodes, market_identity, contract))
        return _json_ready(dict(schema_version='advisory_native_comparison_v1', source_version=stamp,
            generated_at=now, cache_ttl_seconds=self.cache.ttl, range=dict(start=start_day, end=end_day, period=period),
            filters=dict(source=source, binding=binding, sample=sample), cost=dict(**cost, slippage_bps=slippage_bps),
            benchmark='000300.SH', benchmark_basis='SAME_WINDOW_GROSS_PRICE_RETURN',
            metric_basis='FROZEN_PUBLISHED_ORIGINAL_SLOTS_T_OPEN_T4_CLOSE',
            status='READY' if any(r['coverage']['compared_count'] for r in result) else 'NO_COMPARABLE_SAMPLE',
            common_count=len(common), items=result, series=charts))

    @staticmethod
    def downsample(rows, limit=800):
        def points(indices):
            return [[rows[i]['date'], round(rows[i]['nav'], 8) if rows[i]['nav'] is not None else None] for i in indices]
        if len(rows) <= limit:
            return points(range(len(rows)))
        # Keep endpoints, local NAV extrema and the exact first unknown boundary.
        buckets = max(1, (limit - 4) // 2)
        stride = max(1, (len(rows) + buckets - 1) // buckets)
        selected = {0, len(rows) - 1}
        for start in range(0, len(rows), stride):
            chunk = range(start, min(len(rows), start + stride))
            valid = [i for i in chunk if rows[i]['nav'] is not None]
            if valid:
                selected.add(min(valid, key=lambda i: rows[i]['nav']))
                selected.add(max(valid, key=lambda i: rows[i]['nav']))
        unknown = next((i for i, row in enumerate(rows) if row['nav'] is None), None)
        if unknown is not None:
            selected.add(unknown)
        return points(sorted(selected))
