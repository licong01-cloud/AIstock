"""One bounded cursor fixture; no formulas duplicated and no production writes."""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import generic_daily_db_input_v1 as source
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def packet(day=date(2026, 4, 28), *, empty=False, package="a"):
    rows = pd.DataFrame([dict(decision_as_of_trade_date=day, target_trade_date=(pd.Timestamp(day)+pd.offsets.BDay()).date(),
                             instrument="000001.SZ", selection_effective_rank=1, candidate_group_size=1)],
                        columns=source.ROSTER)
    return dict(decision_date=day, target_date=(pd.Timestamp(day)+pd.offsets.BDay()).date(),
                candidates=rows.iloc[:0].copy() if empty else rows,
                metadata=dict(package_id=package, run_id=None, list_version_id=None, universe_identity={"mode": "single_index", "index": "000300.SH"}))


class Session:
    def __init__(self, mutation=None):
        self.calls, self.closed, self.mutation = [], False, mutation
        self.budget = source.EntryWorkBudget()
    @contextmanager
    def connection(self):
        yield self
    @contextmanager
    def cursor(self):
        yield self
    def execute(self, sql, params):
        assert "SELECT" in sql and not any(word in sql.upper() for word in ("UPDATE ", "INSERT ", "DELETE "))
        self.calls.append((sql, params))
    def fetchall(self):
        sql, params = self.calls[-1]
        if "advisory_generic_calendar" in sql:
            rows = [(i, day, (pd.Timestamp(d)+pd.offsets.BDay()).date())
                    for i, d in zip(*params[:2], strict=True) for day in pd.bdate_range(end=d, periods=20).date]
        elif "advisory_generic_raw" in sql:
            rows = [(d, day, symbol, 99000, 101000, 97000, 100000, 2., 2. if d == day else 1., 2.)
                    for d, day, symbol in zip(*params[:3], strict=True)]
        else:
            rows = [(day, "000300.SH", float(1000+(day-date(2026, 1, 1)).days)) for day in params[0]]
        return self.mutation(sql, rows) if self.mutation else rows
    def close(self):
        self.closed = True


def reader(mutation=None, now=None):
    sessions = []
    def factory():
        value = Session(mutation)
        sessions.append(value)
        return value
    return source.GenericDailyReadonlyDBInputV1(session_factory=factory, now=now), sessions


def test_exact_D_anchor_units_one_kernel_and_batch_three_selects_preserve_inputs():
    value, sessions = reader()
    first, second = packet(), packet(date(2026, 4, 29), package="b")
    original = deepcopy(first)
    single, receipt = value.load_day(**first)
    assert single.ret_1.iloc[0] == 1. and single.volume_ratio_5_to_20.iloc[0] == 1.
    assert single.close_location_in_day.iloc[0] == .75 and np.isnan(single.market_up_ratio.iloc[0])
    assert receipt["raw_volume_unit_multiplier"] == 100. and receipt["calendar_verified"]
    batch = value.load_batch(packets=[first, second])
    pd.testing.assert_frame_equal(single, batch[0][0])
    assert len(sessions[-1].calls) == 3 and all(s.closed for s in sessions)
    assert batch[1][1]["context"]["package_id"] == "b" and not batch[1][1]["outcomes_read"]
    pd.testing.assert_frame_equal(first["candidates"], original["candidates"])
    assert first["metadata"] == original["metadata"]


@pytest.mark.parametrize("halted", [False, True])
def test_missing_D_anchor_and_halted_bar_keep_every_candidate_and_volume_independent(halted):
    def missing(sql, rows):
        if "advisory_generic_raw" in sql:
            return [tuple(list(row[:-1])+[None]) for row in (rows[1:] if halted else rows)]
        return rows
    value, _ = reader(missing)
    features, receipt = value.load_day(**packet())
    assert len(features) == 1 and np.isnan(features.ret_1.iloc[0])
    if halted:
        assert np.isnan(features.volume_ratio_5_to_20.iloc[0])  # Missing original session is not compressed.
    else:
        assert features.volume_ratio_5_to_20.iloc[0] == 1.
    assert receipt["candidate_count"] == 1 and not receipt["historical_vintage_proven"]


def test_empty_or_unclosed_day_does_not_connect_and_never_fakes_no_candidates():
    value, sessions = reader(now=lambda: datetime(2026, 4, 28, 6, tzinfo=timezone.utc))
    features, receipt = value.load_day(**packet(empty=True))
    assert features.empty and receipt["status"] == "NO_CANDIDATES" and not sessions
    features, receipt = value.load_day(**packet())
    assert len(features) == 1 and receipt["status"] == "DEFERRED_D_NOT_CLOSED" and not sessions
    assert receipt["query_count"] == 0 and not receipt["calendar_verified"]
    assert value.load_batch(packets=[]) == [] and not sessions
    with pytest.raises(AdvisoryModelFirstError, match="future decision date"):
        value.load_day(**packet(date(2026, 4, 29)))
    assert not sessions


def test_calendar_foreign_quotes_duplicate_and_bad_factor_fail_closed_and_close():
    def bad_calendar(sql, rows):
        return [(*row[:2], date(2026, 5, 1)) for row in rows] if "advisory_generic_calendar" in sql else rows
    def future(sql, rows):
        return [(*rows[0][:1], date(2026, 4, 29), *rows[0][2:]), *rows[1:]] if "advisory_generic_raw" in sql else rows
    def duplicate(sql, rows):
        return [rows[1], *rows[1:]] if "advisory_generic_raw" in sql else rows
    def bad_factor(sql, rows):
        return [(*row[:-1], 0.) for row in rows] if "advisory_generic_raw" in sql else rows
    def bad_unknown_coordinate(sql, rows):
        return [(*row[:4], 90000, *row[5:-1], None) for row in rows] if "advisory_generic_raw" in sql else rows
    for mutation in (bad_calendar, future, duplicate, bad_factor, bad_unknown_coordinate):
        value, sessions = reader(mutation)
        with pytest.raises(AdvisoryModelFirstError):
            value.load_day(**packet())
        assert sessions and sessions[0].closed


def test_database_failure_is_not_empty_success_or_old_path_fallback():
    def failure(*_):
        raise RuntimeError("fixture")
    value, sessions = reader(failure)
    with pytest.raises(AdvisoryModelFirstError) as error:
        value.load_day(**packet())
    assert error.value.reason_code == "ADVISORY_GENERIC_DAILY_DB_INPUT_UNAVAILABLE"
    assert len(sessions[0].calls) == 1 and sessions[0].closed


def test_all_missing_quotes_are_unknown_features_not_empty_original_population():
    def missing(sql, rows):
        return rows if "advisory_generic_calendar" in sql else []
    value, sessions = reader(missing)
    features, receipt = value.load_day(**packet())
    assert len(features) == 1 and features.loc[:, source.FEATURES].isna().all().all()
    assert receipt["status"] == "COMPUTED" and receipt["candidate_count"] == 1
    assert len(sessions[0].calls) == 3 and sessions[0].closed


def test_batch_contradictions_are_rejected_before_read_and_deadline_closes_snapshot():
    value, sessions = reader()
    original = packet()
    invalid = deepcopy(original)
    invalid["metadata"]["universe_identity"] = {"invalid": float("nan")}
    for batch in ([original]*21, [original, original], [invalid]):
        with pytest.raises(AdvisoryModelFirstError):
            value.load_batch(packets=batch)
    assert not sessions
    session = Session()
    def timeout(sql, rows):
        if "advisory_generic_benchmark" in sql:
            session.budget = source.EntryWorkBudget(monotonic=lambda: 0.)
            session.budget._clock = lambda: 31.
        return rows
    session.mutation = timeout
    bounded = source.GenericDailyReadonlyDBInputV1(session_factory=lambda: session)
    with pytest.raises(AdvisoryModelFirstError) as error:
        bounded.load_day(**original)
    assert error.value.reason_code == "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET"
    assert session.closed and len(session.calls) == 3
