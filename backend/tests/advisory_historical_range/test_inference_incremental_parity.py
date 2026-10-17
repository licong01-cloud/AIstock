from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.inference_engine import InferenceEngine
from backend.services.advisory_historical_range.inference_features import InferenceFeatureSession
from backend.services.advisory_historical_range.inference_inputs import InferenceInputSession, bind_readers


def _test_public_reader():
    return "public"


def _frame():
    index = pd.MultiIndex.from_product([pd.bdate_range("2026-01-01", periods=90), ["A", "B"]], names=["datetime", "instrument"])
    close = 10 + np.arange(len(index)) / 100
    return pd.DataFrame({"open": close - .01, "high": close + .1, "low": close - .1,
                         "close": close, "volume": 100 + close}, index=index)


def test_source_append_revision_population_and_consumer_isolation():
    source, reads = _frame(), []
    def loader(members, start, end):
        reads.append((tuple(members), start, end))
        return InferenceInputSession._slice(source, members, start, end)
    first = InferenceInputSession(consumer_scope="ADVISORY", run_id="own", source_identity="v1")
    days = source.index.get_level_values("datetime").unique()
    for end in days[70:73]:
        result = first.read(dataset="raw", members=["A"], start=days[5].date(), end=end.date(), loader=loader)
        pd.testing.assert_frame_equal(result, loader(["A"], days[5].date(), end.date()))
    assert first.append_reads == 2
    assert reads[2][1] == (days[70] + pd.Timedelta(days=1)).date()
    source.iloc[20, source.columns.get_loc("close")] = 200
    first.invalidate(source_identity="v2")
    revised = first.read(dataset="raw", members=["A", "B"], start=days[5].date(), end=days[72].date(), loader=loader)
    assert 200 in revised.close.values
    other = InferenceInputSession(consumer_scope="LOCALSIM", run_id="own", source_identity="v2")
    other.read(dataset="raw", members=["A", "B"], start=days[5].date(), end=days[72].date(), loader=loader)
    assert other.reads == 1 and other.hits == 0
    revised.iloc[0, 0] = -99
    reread = first.read(dataset="raw", members=["A", "B"], start=days[5].date(), end=days[72].date(), loader=loader)
    assert reread.iloc[0, 0] != -99


def test_unknown_recursive_factor_keeps_moving_origin_and_no_future_rows():
    source = _frame()
    session = InferenceInputSession(consumer_scope="ADVISORY", run_id="r", source_identity="revision")
    def reader(members, start, end):
        return session._slice(source, members, start, end)
    dates = source.index.get_level_values("datetime").unique()
    for position in (72, 73):
        start, end = dates[position - 70].date(), dates[position].date()
        legal = session.read(dataset="pv", members=["A", "B"], start=start, end=end, loader=reader)
        expected = reader(["A", "B"], start, end)
        def recursive(frame):
            return frame.close.groupby(level="instrument").transform(lambda x: x.ewm(span=14, min_periods=10).mean())
        pd.testing.assert_series_equal(recursive(legal), recursive(expected))
        assert legal.index.get_level_values("datetime").max().date() == end


def test_pool_change_reads_new_member_history_and_only_common_member_delta():
    source, reads = _frame(), []
    session = InferenceInputSession(consumer_scope="ADVISORY", run_id="r", source_identity="revision")
    def loader(members, start, end):
        reads.append((tuple(members), start, end))
        return session._slice(source, members, start, end)
    dates = source.index.get_level_values("datetime").unique()
    session.read(dataset="raw", members=["A"], start=dates[2].date(), end=dates[75].date(), loader=loader)
    actual = session.read(dataset="raw", members=["A", "B"], start=dates[3].date(), end=dates[76].date(), loader=loader)
    pd.testing.assert_frame_equal(actual, session._slice(source, ["A", "B"], dates[3].date(), dates[76].date()))
    assert reads[1:] == [(("A",), (dates[75] + pd.Timedelta(days=1)).date(), dates[76].date()),
                         (("B",), dates[3].date(), dates[76].date())]
    # A new package with a longer warm-up gets the missing PREFIX, not an
    # incorrectly reused shorter history, and never another consumer's result.
    actual = session.read(dataset="raw", members=["A", "B"], start=dates[0].date(), end=dates[76].date(), loader=loader)
    pd.testing.assert_frame_equal(actual, session._slice(source, ["A", "B"], dates[0].date(), dates[76].date()))
    assert reads[-1][1:] == (dates[0].date(), (dates[3] - pd.Timedelta(days=1)).date())


def test_source_disjoint_reverse_and_evicted_views_never_claim_unread_gap():
    source, reads = _frame(), []
    days = source.index.get_level_values("datetime").unique()
    session = InferenceInputSession(consumer_scope="ADVISORY", run_id="r", source_identity="v1")
    def loader(members, start, end):
        reads.append((tuple(members), start, end))
        return session._slice(source, members, start, end)
    for first, last in ((10, 20), (40, 50), (15, 45), (2, 5), (3, 4)):
        actual = session.read(dataset="pv", members=["A"], start=days[first].date(), end=days[last].date(), loader=loader)
        pd.testing.assert_frame_equal(actual, session._slice(source, ["A"], days[first].date(), days[last].date()))
        assert reads[-1][2] <= days[last].date() or (first, last) == (3, 4)
    assert len(reads) == 4 and session.hits == 2
    session.max_bytes = 0
    session.read(dataset="other", members=["B"], start=days[0].date(), end=days[70].date(), loader=loader)
    session.invalidate(source_identity="v2")
    for _ in range(2):
        session.read(dataset="pv", members=["A"], start=days[0].date(), end=days[70].date(), loader=loader)
    assert session.retained_bytes == 0
    before = len(reads)
    assert session.read(dataset="pv", members=[], start=days[0].date(), end=days[70].date(), loader=loader).empty
    assert len(reads) == before


def test_known_alpha_overlap_equals_full_recompute_and_invalidates_correction():
    source = _frame()
    cached = InferenceEngine(feature_session=InferenceFeatureSession())
    reference = InferenceEngine()
    dates = source.index.get_level_values("datetime").unique()
    columns = ["KLEN", "WVMA60", "CORR10", "CORD60", "RSQR20", "ROC60"]
    for end in (dates[75], dates[76]):
        legal = source.loc[(source.index.get_level_values("datetime") >= dates[2])
                           & (source.index.get_level_values("datetime") <= end)]
        pd.testing.assert_frame_equal(cached._compute_alpha158_sequence(legal, columns, 5),
                                      reference._compute_alpha158_sequence(legal, columns, 5))
    assert cached.feature_session.hits == 4
    source.loc[(dates[74], "A"), "close"] += 1
    pd.testing.assert_frame_equal(cached._compute_alpha158_sequence(source, columns, 5),
                                  reference._compute_alpha158_sequence(source, columns, 5))


def test_equal_alpha_tails_do_not_erase_native_population_order_at_warmup_origin():
    source = _frame()
    dates = source.index.get_level_values("datetime").unique()
    source = source.drop([(day, "A") for day in dates[3:6]])
    cached = InferenceEngine(feature_session=InferenceFeatureSession())
    reference = InferenceEngine(performance_reference=True)
    columns = ["WVMA5", "STD5", "ROC60"]
    for origin in (dates[0], dates[3], dates[0]):
        view = source.loc[source.index.get_level_values("datetime") >= origin]
        pd.testing.assert_frame_equal(cached._compute_alpha158_sequence(view, columns, 1),
                                      reference._compute_alpha158_sequence(view, columns, 1), check_exact=True)
    assert cached.feature_session.hits == 1


def test_database_views_use_cutoff_only_adjustments_without_public_global_patch(monkeypatch):
    from backend.data_service import api, qe_data_service
    from backend.data_service.adj_factor_provider import AdjFactorProvider
    from backend.services.advisory_historical_range.inference_inputs import database_input_session
    days = pd.date_range("2026-01-01", periods=3)
    index = pd.MultiIndex.from_product([days, ["A"]], names=["datetime", "instrument"])
    source = pd.DataFrame({"open": 10., "high": 10., "low": 10., "close": 10., "volume": 100., "amount": 1000.}, index=index)
    queries = []
    def raw(codes, start, end, **kwargs):
        assert kwargs["adj"] == "none" and not kwargs["allow_xtquant_fallback"] and not kwargs["allow_tushare_adj_fallback"]
        queries.append(end.date())
        return InferenceInputSession._slice(source, codes, start.date(), end.date())
    def adjustment(self, codes, first, last):
        assert self.use_tushare_fallback is False
        queries.append(last)
        frame = pd.DataFrame({"trade_date": days, "ts_code": "A", "adj_factor": [1., 2., 200.]})
        return frame.loc[(frame.trade_date >= pd.Timestamp(first)) & (frame.trade_date <= pd.Timestamp(last))]
    original_static_pv = qe_data_service.load_daily_pv
    monkeypatch.setattr(api, "get_history_window", raw)
    monkeypatch.setattr(AdjFactorProvider, "get_adj_factor", adjustment)
    session = database_input_session(consumer_scope="ADVISORY", run_id="r", source_identity="revision", max_bytes=1024**2)
    end = days[1].date()
    engine_view = session.price_reader(["A"], days[0].date(), end)
    static_view = session.static_builder.__globals__["load_daily_pv"](["A"], days[0].date(), end)
    assert engine_view.close.tolist() == [10., 10.] and engine_view.factor.tolist() == [.5, 1.]
    assert static_view.close.tolist() == [5., 10.] and static_view.volume.tolist() == [200., 100.]
    assert queries == [end, end] and qe_data_service.load_daily_pv is original_static_pv


def test_builder_binding_never_modifies_public_or_other_consumers():
    # FunctionType uses copied globals, not module mutation.
    def builder():
        return _test_public_reader()
    first = bind_readers(builder, {"_test_public_reader": lambda: "advisory"})
    second = bind_readers(builder, {"_test_public_reader": lambda: "localsim"})
    assert (first(), second(), builder()) == ("advisory", "localsim", "public")


def test_uncacheable_unknown_factor_and_budget_never_become_package_gate():
    cache = InferenceFeatureSession(max_bytes=0)
    result = _frame()
    for _ in range(2):
        pd.testing.assert_frame_equal(cache.evaluate(identity="source-code-input", calculate=lambda: result), result)
    assert cache.retained_bytes == cache.hits == 0
    assert cache.dynamic_identity(module=SimpleNamespace(), function=lambda: result, input_identity="x") is None
    session = InferenceInputSession(consumer_scope="ADVISORY", run_id="r", source_identity="v1", max_bytes=0)
    with pytest.raises(ValueError, match="outside"):
        session.read(dataset="bad", members=["A", "B"], start=date(2026, 1, 1), end=date(2026, 1, 2), loader=lambda *args: result)


def test_feature_provenance_revision_and_consumer_change_invalidate_even_equal_values():
    cache = InferenceFeatureSession()
    calls = []
    for scope in (("ADVISORY", "r", "v1"), ("ADVISORY", "r", "v1"),
                  ("ADVISORY", "r", "v2"), ("LOCALSIM", "r", "v2")):
        cache.bind_source(scope)
        def calculate():
            calls.append(scope)
            return _frame()
        cache.evaluate(identity="identical-code-and-values", calculate=calculate)
    assert len(calls) == 3 and cache.hits == 1


def test_unbound_factor_initialization_or_dependency_is_not_an_eligibility_gate():
    from backend.services.advisory_historical_range.inference_features import _self_contained_factor
    inputs = ("daily_pv.h5", "static_factors.parquet")
    assert _self_contained_factor(b'import pandas as pd\nx = pd.read_hdf("daily_pv.h5")\ny = x.close.pct_change()', inputs)
    for source in (b'import numpy as np\nx = np.random.normal()', b'from own_mutable_helper import function',
                   b'import pandas as pd\nx = pd.Timestamp.now()', b'import pandas as pd\nx = pd.read_hdf("/external/data.h5")',
                   b'from numpy.random import normal\nx = normal(size=3)',
                   b'from numpy import random as rng\nx = rng.normal(size=3)',
                   b'import numpy as np\nx = np.load("external.npy")',
                   b'import pandas as pd\nx = pd.HDFStore("external.h5")',
                   b'from pathlib import Path\nx = Path(__file__).parent.name',
                   b'from numpy import *\nx = load("external.npy")'):
        assert not _self_contained_factor(source, inputs)
    # A cache miss never prohibits calculation of that supported factor.
    cache = InferenceFeatureSession(max_bytes=0)
    pd.testing.assert_frame_equal(cache.evaluate(identity="uncached-original", calculate=_frame), _frame())
