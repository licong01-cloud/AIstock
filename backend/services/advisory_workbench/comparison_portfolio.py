"""Finite-capital reference portfolio. No leverage, overlap compounding or fills."""
from .projection import membership
from .comparison_metrics import known_empty_scope, list_identity_valid


def reference_portfolio(versions, items, market, start, end, cost, *, benchmark=False, unknown_dates=()):
    cash, pending, nav, fees = 1.0, 0.0, 1.0, 0.0
    positions, series, reasons = [], [], {}
    by_date = {}
    for v in versions:
        by_date.setdefault(str(v['trade_date']), []).append(v)
    unknown = None if market.covers(start, end) else 'CALENDAR_UNKNOWN'
    peak, mdd = 1.0, 0.0
    gross_nav = 1.0
    for day in (d for d in market.days if start <= d <= end):
        cash += pending
        pending = 0.0
        published = by_date.get(day, [])
        if day in unknown_dates:
            unknown = 'PUBLICATION_SOURCE_UNKNOWN'
        if unknown is None:
            if len(published) > 1:
                unknown = 'DUPLICATE_PUBLICATION'
            elif published:
                version = published[0]
                rows = [r for r in items.get(version['list_version_id'], []) if membership(r) == 'IN_SCOPE']
                symbols = ['__HS300__'] if benchmark else [r['symbol'] for r in rows]
                if not symbols and known_empty_scope(version, items.get(version['list_version_id'], [])):
                    reasons['EMPTY_SCOPE_CASH'] = reasons.get('EMPTY_SCOPE_CASH', 0) + 1
                elif (not symbols or not list_identity_valid(version, items.get(version['list_version_id'], []))
                        or any(membership(r) == 'UNKNOWN' for r in items.get(version['list_version_id'], []))):
                    unknown = 'LIST_IDENTITY_UNKNOWN'
                maturity = market.maturity(day)
                if symbols and not maturity:
                    unknown = 'CALENDAR_UNKNOWN'
                budget = min(nav * .2, cash)
                slot = budget / len(symbols) if symbols else 0
                for symbol in symbols:
                    if slot <= 0:
                        reasons['NO_CASH_BUDGET'] = reasons.get('NO_CASH_BUDGET', 0) + 1
                        continue
                    price, state = market.price(symbol, day, 'open', execution=True)
                    if state.startswith('CASH_'):
                        reasons[state] = reasons.get(state, 0) + 1
                        continue
                    if not price:
                        unknown = state
                        break
                    buy = 0 if benchmark else cost['buy_cost_bps'] / 10000
                    units = slot / (price * (1 + buy))
                    positions.append(dict(symbol=symbol, units=units, maturity=maturity))
                    cash -= slot
                    fees += units * price * buy
            else:
                reasons['NO_PUBLICATION_CASH'] = reasons.get('NO_PUBLICATION_CASH', 0) + 1
        value, remaining = 0.0, []
        if unknown is None:
            for position in positions:
                exits = position['maturity'] == day
                price, state = market.price(position['symbol'], day, execution=exits)
                if not price:
                    unknown = state
                    break
                amount = position['units'] * price
                if exits:
                    sell = 0 if benchmark else cost['sell_cost_bps'] / 10000
                    pending += amount * (1 - sell)
                    fees += amount * sell
                else:
                    value += amount
                    remaining.append(position)
            positions = remaining
        if unknown is None:
            nav = cash + pending + value
            gross_nav = nav + fees  # additive fee burden, not a separate cost-free trading simulation
            peak = max(peak, nav)
            mdd = min(mdd, (nav / peak - 1) * 10000)
            series.append(dict(date=day, nav=nav, cash=cash + pending, invested=value,
                               fees=fees, drawdown_bps=(nav / peak - 1) * 10000, reason=None))
        else:
            series.append(dict(date=day, nav=None, cash=None, invested=None, fees=None,
                               drawdown_bps=None, reason=unknown))
    ready = bool(series) and unknown is None
    return dict(basis='FINITE_CAPITAL_5_TRANCHES_REFERENCE_NOT_ACCOUNT', status='READY' if ready else 'UNKNOWN',
                reason=unknown or (None if series else 'NO_TRADING_DAYS'),
                total_return_bps=(nav - 1) * 10000 if ready else None,
                max_drawdown_bps=mdd if ready else None, final_nav=nav if ready else None,
                cash_fraction=(cash + pending) / nav if ready and nav > 0 else None,
                invested_fraction=value / nav if ready and nav > 0 else None,
                cost_drag_bps=(gross_nav - nav) * 10000 if ready else None,
                cost_drag_basis='CUMULATIVE_FEES_PER_INITIAL_CAPITAL',
                unknown_from=next((r['date'] for r in series if r['nav'] is None), None),
                diagnostics=reasons, series=series)
