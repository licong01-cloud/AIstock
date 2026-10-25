from contextlib import contextmanager
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_moneyflow_price_source_v1 as source
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import AMOUNT_FIELDS
from backend.tests.advisory_model_first.test_economic_moneyflow_price_v1 import flow_fixture


@pytest.mark.parametrize('value', [False, -1, float('inf'), float('nan'), Decimal('NaN'), '1', complex(1, 2)])
def test_raw_side_amount_errors_are_not_coerced_to_missing_or_units(value):
    raw = pd.DataFrame([dict(instrument='000001.SZ', trade_date=date(2025, 1, 2), **dict.fromkeys(AMOUNT_FIELDS, 1))], dtype=object)
    raw.loc[0, AMOUNT_FIELDS[0]] = value
    with pytest.raises(ValueError, match='raw amount'):
        source.normalize_moneyflow_boundary_v1(raw)


def test_exact_once_public_normalizer_eight_fields_NULL_and_no_magnitude_guess():
    raw = pd.DataFrame([dict(instrument='000001.SZ', trade_date=date(2025, 1, 2), **dict.fromkeys(AMOUNT_FIELDS, Decimal('.001')))], dtype=object)
    raw.loc[0, AMOUNT_FIELDS[0]] = None
    cny = source.normalize_moneyflow_boundary_v1(raw)
    assert cny.loc[0, AMOUNT_FIELDS[1]] == 10. and pd.isna(cny.loc[0, AMOUNT_FIELDS[0]])
    assert raw.loc[0, AMOUNT_FIELDS[1]] == Decimal('.001')
    with pytest.raises(ValueError, match='already normalized'):
        source.normalize_moneyflow_boundary_v1(cny)


@pytest.mark.parametrize('defect', ['none', 'duplicate', 'query_error', 'schema'])
def test_bounded_batch_two_selects_exact_date_pruning_and_ALWAYS_close(defect):
    args = flow_fixture()
    candidates = args['candidates'].iloc[[1]].copy()
    days, calls = args['calendar'][:6], []

    class Session:
        closed = False

        @contextmanager
        def connection(self):
            try:
                yield self
            finally:
                calls.append(('rollback',))

        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, sql, params):
            calls.append((sql, params))
            if defect == 'query_error' and len(calls) == 2:
                raise RuntimeError('budget expired')

        def fetchone(self):
            return {**dict.fromkeys(AMOUNT_FIELDS, 'numeric'), 'ts_code': 'text', 'trade_date': 'text' if defect == 'schema' else 'date'}, list(days.date)

        def fetchall(self):
            sql, params = calls[-1]
            assert 'm.trade_date>=%s::date AND m.trade_date<=%s::date' in sql
            assert params[2] == days[0].date() and params[3] == days[4].date()
            rows = [('000001.SZ', day.date(), *([Decimal('1')]*8)) for day in days[:5]]
            return rows+[rows[0]] if defect == 'duplicate' else rows

        def close(self):
            self.closed = True

    session = Session()
    if defect == 'none':
        batch = source.load_moneyflow_source_v1(candidates=candidates, session_factory=lambda: session)
        assert len(batch.amounts) == 5 and batch.amounts[AMOUNT_FIELDS[0]].eq(10000.).all()
        assert batch.receipt['selects'] == 2 and batch.receipt['native_identity'] == 'UNPROVEN' and not batch.receipt['database_written']
    else:
        with pytest.raises((RuntimeError, ValueError)):
            source.load_moneyflow_source_v1(candidates=candidates, session_factory=lambda: session)
    assert session.closed and calls[-1] == ('rollback',)
    assert sum(call[0].startswith(('SELECT', 'WITH requested')) for call in calls) == (1 if defect == 'schema' else 2)
