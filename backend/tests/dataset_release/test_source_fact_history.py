from datetime import date
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release.source_fact_history import filter_source_fact_history


def test_fact_population_excludes_b_shares_not_st_ipo_or_delisted():
    from backend.services.dataset_release.source_fact_history import fact_codes
    assert fact_codes(['000861.SZ', '301396.SZ', '600811.SH', '200001.SZ', '900901.SH',
                       '000985.SH', '430047.BJ', '600811.sh ', '038043.SZ', '072452.SZ',
                       '082452.SZ', '310001.SZ']) == ('000861.SZ', '301396.SZ', '600811.SH')


def test_factor_default_keeps_observed_history_before_pit_entry():
    from types import SimpleNamespace
    from backend.services.dataset_release.factor_materializer import _filter_producer_population
    index = pd.MultiIndex.from_tuples([(pd.Timestamp('2024-07-01'), '600811.SH')],
        names=['datetime', 'instrument'])
    frame = pd.DataFrame({'close': [10.]}, index=index)
    spec = SimpleNamespace(fact_stock_codes=(), qfq_denominator_authority=SimpleNamespace(by_code={'600811.SH': 1}),
        pit_snapshot=SimpleNamespace(spans=(), cutoff=date(2026, 9, 30)))
    actual, receipt = _filter_producer_population(frame, spec,
        SimpleNamespace(start=date(2024, 7, 1), end=date(2024, 7, 31)))
    pd.testing.assert_frame_equal(actual, frame)
    assert receipt['pit_selection_unchanged'] is True


def test_real_facts_outside_buy_spans_keep_all_daily_and_minute_fields():
    from backend.tests.dataset_release.test_canonical_stock_transformer import (
        _spec, _daily, _minute_day, _adj, _limits, DAY1, DAY2,
    )
    from backend.services.dataset_release.canonical_stock_transformer import CanonicalStockTransformer
    from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS
    original = _spec()
    pit = replace(original.pit_snapshot, spans=())
    authority = replace(original.qfq_denominators, pit_spans_sha256=pit.spans_sha256)
    spec = replace(original, pit_snapshot=pit, qfq_denominators=authority)
    daily = list(CanonicalStockTransformer().transform_daily(spec,
        daily_rows=[_daily(DAY1), _daily(DAY2)], adj_factor_rows=_adj(),
        stk_limit_rows=_limits(DAY1, DAY2), suspend_rows=[]))
    minute = list(CanonicalStockTransformer().transform_minute(spec,
        minute_rows=_minute_day(DAY1) + _minute_day(DAY2), adj_factor_rows=_adj(),
        stk_limit_rows=_limits(DAY1, DAY2), suspend_rows=[]))
    assert len(daily) == 2 and len(minute) == 480
    assert set(daily[0]) == set(minute[0]) == {'datetime', 'instrument', *QLIB_STOCK_FIELDS}




def test_monthly_producer_keeps_raw_and_static_facts_outside_pit(tmp_path):
    from dataclasses import replace
    from backend.tests.dataset_release.test_factor_partition_producer import FixtureReader, _spec
    from backend.services.dataset_release.factor_materializer import FactorPartitionProducer
    from backend.services.dataset_release.pit import freeze_pit_snapshot

    spec = _spec(tmp_path)
    original = spec.pit_snapshot
    rows = [span.as_dict() for span in original.spans]
    rows[0]["eligible_start"] = "2026-06-05"
    pit = freeze_pit_snapshot(rows, universe_key=original.universe_key, rule_version=original.rule_version,
                              scope_start=original.scope_start, cutoff=original.cutoff,
                              state_identity=original.state_identity,
                              source_fingerprint_sha256=original.source_fingerprint_sha256,
                              parameter_hash=original.parameter_hash)
    qfq = replace(spec.qfq_denominator_authority, pit_spans_sha256=pit.spans_sha256)
    produced = FactorPartitionProducer().produce(replace(spec, pit_snapshot=pit, qfq_denominator_authority=qfq), reader=FixtureReader())
    root = produced.source_root / spec.partitions[0].partition_key
    raw = pd.read_parquet(root / "daily_basic.parquet")
    static = pd.read_parquet(root / "static_factors.parquet")
    assert (pd.Timestamp("2026-06-01"), "000001.SZ") in raw.index
    assert (pd.Timestamp("2026-06-01"), "000001.SZ") in static.index
    assert set(raw.index.get_level_values("instrument")) == set(static.index.get_level_values("instrument"))


def test_history_retains_prior_facts_not_new_population():
    frame = pd.DataFrame(
        {"db_circ_mv": np.array([10, 11, 99, 12], dtype="float32")},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp(day), symbol) for day, symbol in
             [("2024-06-28", "000001.SZ"), ("2024-07-01", "000001.SZ"),
              ("2024-06-28", "999999.SZ"), ("2024-07-02", "000001.SZ")]],
            names=["datetime", "instrument"],
        ),
    )
    kept = filter_source_fact_history(frame, codes=["000001.SZ"],
                                     start=date(2024, 6, 28), end=date(2024, 7, 1))
    assert kept["db_circ_mv"].tolist() == [10, 11]
    assert str(kept.db_circ_mv.dtype) == "float32"
    assert set(kept.index.get_level_values("instrument")) == {"000001.SZ"}


def test_duplicate_or_intraday_source_facts_fail_closed():
    index = pd.MultiIndex.from_tuples([(pd.Timestamp("2024-07-01"), "000001.SZ")]*2,
                                     names=["datetime", "instrument"])
    frame = pd.DataFrame({"db_circ_mv": [10, 20]}, index=index)
    with pytest.raises(ValueError, match="duplicate"):
        filter_source_fact_history(frame, codes=["000001.SZ"], start=date(2024,7,1), end=date(2024,7,1))


def test_candidate_validator_accepts_prior_history_not_other_securities(tmp_path):
    from backend.services.dataset_release.candidate_validator import _audit_h5, CandidateValidationError
    path = tmp_path / "daily_basic.h5"
    frame = pd.DataFrame(
        {"db_circ_mv": np.array([10, 12], dtype="float32")},
        index=pd.MultiIndex.from_tuples([(pd.Timestamp("2024-06-28"), "000001.SZ"),
                                         (pd.Timestamp("2024-07-01"), "000001.SZ")],
                                        names=["datetime", "instrument"]),
    )
    frame.to_hdf(path, key="data", format="table")
    kwargs = dict(max_rows=1, spans={"000001.SZ": [(date(2024,7,1), date(2024,7,1))]},
                  collect_dates=False, expected_columns=["db_circ_mv"],
                  expected_dtypes={"db_circ_mv": "float32"}, exact_expected_keys=None)
    assert _audit_h5(path, source_fact_dates=frozenset(["2024-06-28", "2024-07-01"]), **kwargs)["rows"] == 2
    with pytest.raises(CandidateValidationError, match="outside PIT"):
        _audit_h5(path, **kwargs)
    kwargs["spans"] = {"000002.SZ": [(date(2024,7,1), date(2024,7,1))]}
    with pytest.raises(CandidateValidationError, match="outside PIT"):
        _audit_h5(path, source_fact_dates=frozenset(["2024-06-28", "2024-07-01"]), **kwargs)
    frame = frame.iloc[:1].copy()
    frame.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2024-07-01 12:00"), "000001.SZ")],
                                           names=["datetime", "instrument"])
    with pytest.raises(ValueError, match="daily"):
        filter_source_fact_history(frame, codes=["000001.SZ"], start=date(2024,7,1), end=date(2024,7,1))
