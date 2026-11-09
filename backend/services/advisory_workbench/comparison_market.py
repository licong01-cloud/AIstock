"""Batch market observations and conservative execution semantics (never fills)."""
from datetime import date

from backend.services.advisory_historical_range.program_performance import number
from backend.db.pg_pool import get_conn
from backend.services.advisory_program import MARKET_PRICE_UNIT_DIVISOR
from .projection import WorkbenchError, membership


def positive(value):
    parsed = number(value)
    return parsed if parsed is not None and parsed > 0 else None


def observation_keys(versions, items, episodes, calendar, end):
    """Only observed holding windows; no symbol-by-entire-range Cartesian read."""
    days = sorted(str(r['cal_date']) for r in calendar if r['is_trading'])
    positions = {d: i for i, d in enumerate(days)}
    by_version = {v['list_version_id']: v for v in versions}
    keys = set()
    for row in items:
        version = by_version.get(row['list_version_id'])
        if not version or membership(row) != 'IN_SCOPE':
            continue
        i = positions.get(str(version['trade_date']))
        if i is not None:
            keys.update((row['symbol'], d) for d in days[i:i + 5] if d <= str(end))
    final = next((d for d in reversed(days) if d <= str(end)), None)
    for row in episodes:
        for d in (str(row['effective_entry_date']), str(row.get('effective_exit_date') or ''), final):
            if d and d <= str(end):
                keys.add((row['symbol'], d))
    if len(keys) > 500000:
        raise WorkbenchError('MARKET_OBSERVATION_LIMIT', '所选行情观察点过多，请缩短时间。')
    return sorted(keys)


class ComparisonMarketReader:
    def load(self, *, symbols, start, end, cutoff, benchmark, observations, calendar):
        codes, dates = zip(*observations) if observations else ([], [])
        # All values remain bound parameters. Existing table indexes resolve exact facts.
        requested = 'WITH requested AS (SELECT * FROM unnest(%s::text[],%s::date[]) AS t(symbol,day)) '
        with get_conn() as connection:
            with connection.cursor() as cur:
                cur.execute(requested + """SELECT TRIM(k.ts_code) AS symbol,k.trade_date,
                    k.open_li,k.close_li,k.high_li,k.low_li,a.adj_factor,l.up_limit,l.down_limit
                    FROM requested r JOIN market.kline_daily_raw k ON k.ts_code=r.symbol::char(9) AND k.trade_date=r.day
                    LEFT JOIN market.adj_factor a ON a.ts_code=r.symbol AND a.trade_date=r.day
                      AND a.trade_date BETWEEN %s AND %s
                    LEFT JOIN market.stk_limit l ON l.ts_code=r.symbol AND l.trade_date=r.day
                      AND l.trade_date BETWEEN %s AND %s
                    WHERE k.trade_date BETWEEN %s AND %s ORDER BY k.trade_date,k.ts_code""",
                    (list(codes), list(dates), start, cutoff, start, cutoff, start, cutoff))
                fields = [d.name for d in cur.description]
                prices = [dict(zip(fields, row)) for row in cur.fetchall()]
                cur.execute(requested + """SELECT TRIM(s.ts_code),s.trade_date
                    FROM requested r JOIN market.suspend_d s ON s.ts_code=r.symbol AND s.trade_date=r.day
                    WHERE COALESCE(TRIM(s.suspend_type),'S')='S' AND s.trade_date BETWEEN %s AND %s""",
                    (list(codes), list(dates), start, cutoff))
                suspended = set(cur.fetchall())
                cur.execute("""SELECT trade_date,open,close FROM market.index_daily
                    WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date""", (benchmark, start, min(end, cutoff)))
                benchmark_rows = [dict(zip(('trade_date', 'open', 'close'), row)) for row in cur.fetchall()]
        return dict(calendar=calendar, prices=prices, suspended=suspended, benchmark=benchmark_rows)


class Market:
    def __init__(self, raw):
        self.raw = raw
        self.calendar = self.unique(raw['calendar'], lambda r: str(r['cal_date']))
        self.days = sorted(k for k, r in self.calendar.items() if r['is_trading'])
        self.positions = {d: i for i, d in enumerate(self.days)}
        self.maturities = {}
        self.prices = self.unique(raw['prices'], lambda r: (r['symbol'], str(r['trade_date'])))
        self.index = self.unique(raw['benchmark'], lambda r: str(r['trade_date']))
        self.suspended = {(s, str(d)) for s, d in raw['suspended']}

    @staticmethod
    def unique(rows, key):
        result = {}
        for row in rows:
            identity = key(row)
            if identity in result:
                raise WorkbenchError('DUPLICATE_MARKET_IDENTITY', '行情来源存在重复身份，不能计算。', 503)
            result[identity] = row
        return result

    def maturity(self, start):
        if start in self.maturities:
            return self.maturities[start]
        i = self.positions.get(start)
        if i is None or i + 4 >= len(self.days):
            return None
        end = self.days[i + 4]
        # Calendar gaps must not silently shorten the five-session window.
        if not self.covers(start, end):
            return None
        self.maturities[start] = end
        return end

    def covers(self, start, end):
        span = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        rows = [r for d, r in self.calendar.items() if start <= d <= end]
        return len(rows) == span and all(isinstance(r['is_trading'], bool) for r in rows)

    def price(self, symbol, day, field='close', *, execution=False, side=None, adjusted=True):
        if symbol == '__HS300__':
            return positive(self.index.get(day, {}).get(field)), 'KNOWN'
        row = self.prices.get((symbol, day), {})
        buying = (side or ('BUY' if field == 'open' else 'SELL')) == 'BUY'
        if (symbol, day) in self.suspended:
            return None, 'CASH_SUSPENDED' if buying and execution else 'SUSPENDED'
        raw = positive(row.get(field + '_li'))
        factor = positive(row.get('adj_factor')) if adjusted else 1
        if not raw:
            return None, 'PRICE_OR_ADJUSTMENT_MISSING'
        if execution:
            limit = positive(row.get('up_limit' if buying else 'down_limit'))
            low = positive(row.get('low_li'))
            if not limit or not low:
                return None, 'LIMIT_DATA_MISSING'
            if buying and raw / MARKET_PRICE_UNIT_DIVISOR >= limit and low / MARKET_PRICE_UNIT_DIVISOR >= limit:
                return None, 'CASH_LIMIT_UP'
            if not buying and raw / MARKET_PRICE_UNIT_DIVISOR <= limit:
                return None, 'EXIT_NOT_EXECUTABLE'
        if not factor:
            return None, 'PRICE_OR_ADJUSTMENT_MISSING'
        return raw / MARKET_PRICE_UNIT_DIVISOR * factor, 'KNOWN'
