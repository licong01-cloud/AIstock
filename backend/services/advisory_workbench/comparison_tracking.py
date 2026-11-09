"""As-of tracking metrics: separate from the comparable daily published cohorts."""
from collections import Counter
from statistics import mean

from backend.services.advisory_historical_range.program_performance import distribution, net_return


def tracking_metrics(rows, market, start, end, cost):
    settled, floating, holding, marks = [], [], [], []
    missing, pending, entered, exits = 0, 0, 0, Counter()
    final = next((d for d in reversed(market.days) if d <= end), None)
    for row in rows:
        entry = str(row['effective_entry_date'])
        if not start <= entry <= end:
            continue
        entered += 1
        symbol = row['symbol']
        basis = row.get('entry_price_basis')
        field = 'open' if basis == 'next_open_executable' else 'close'
        base, state = market.price(symbol, entry, field, execution=(field == 'open'), adjusted=False)
        if not base:
            missing += 1
            continue
        exit_date = str(row['effective_exit_date']) if row.get('effective_exit_date') else None
        ended = row.get('episode_status', row.get('status')) == 'EXITED' and exit_date and exit_date <= end
        if ended:
            exit_field = 'open' if row.get('exit_price_basis') == 'next_open_executable' else 'close'
            # Close-based execution also requires non-suspended, executable evidence.
            mark, state = market.price(symbol, exit_date, exit_field, execution=True, side='SELL', adjusted=False)
            if mark and not state.startswith('CASH_'):
                gross = (mark / base - 1) * 10000
                settled.append(gross)
                positions = market.positions
                if entry in positions and exit_date in positions:
                    holding.append(positions[exit_date] - positions[entry] + 1)
                exits[row.get('exit_reason') or 'UNKNOWN'] += 1
                continue
            pending += 1
        mark_date = final if final and final >= entry else None
        mark, _ = market.price(symbol, mark_date, adjusted=False) if mark_date else (None, None)
        if mark:
            floating.append((mark / base - 1) * 10000)
            marks.append(mark_date)
        else:
            missing += 1
    return dict(basis='AS_OF_RAW_REFERENCE_EPISODE_ENTRY_COHORTS_OWN_SAMPLE',
                entered_count=entered,
                gross=distribution(settled), net=distribution(net_return(v, cost) for v in settled),
                floating_gross=distribution(floating), floating_liquidation_net=distribution(
                    net_return(v, cost) for v in floating), avg_holding_days=mean(holding) if holding else None,
                exit_reasons=dict(exits), missing_mark_count=missing, pending_exit_count=pending,
                mark_date=max(marks) if marks else None, as_of=end)


def read_tracking(repo, program_ids, start, end):
    """Current canonical ledger, clipped by financial dates; no historical-knowledge claim."""
    return repo.rows("""WITH latest AS (
        SELECT DISTINCT ON (program_id,episode_id) program_id,episode_id,symbol,episode_status,
            effective_entry_date,effective_exit_date,entry_price_basis,exit_price_basis,exit_reason,
            created_at,updated_at,snapshot_id FROM app.advisory_episode_return
        WHERE program_id=ANY(%s)
        ORDER BY program_id,episode_id,updated_at DESC,created_at DESC,snapshot_id DESC
    ), origins AS (
        SELECT DISTINCT ON (d.program_id,d.episode_id) d.program_id,d.episode_id,d.binding_version_id,
        CASE WHEN r.run_type='REPLAY' THEN 'REPLAY'
         WHEN EXISTS (SELECT 1 FROM app.advisory_forward_run f WHERE f.program_id=d.program_id
                      AND f.review_run_id=d.review_run_id) THEN 'FORWARD' ELSE 'UNKNOWN' END AS source
        FROM app.advisory_daily_review d LEFT JOIN app.advisory_review_run r
         ON r.program_id=d.program_id AND r.review_run_id=d.review_run_id
        WHERE d.program_id=ANY(%s) AND d.action='ENTER'
        ORDER BY d.program_id,d.episode_id,d.trade_date,d.review_run_id
    ) SELECT latest.*,origins.binding_version_id,COALESCE(origins.source,'UNKNOWN') AS source
      FROM latest LEFT JOIN origins USING (program_id,episode_id)
      WHERE latest.effective_entry_date BETWEEN %s AND %s LIMIT 100001""",
        (program_ids, program_ids, start, end))
