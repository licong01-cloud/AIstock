from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release.source_fact_history import filter_source_fact_history


def test_monthly_producer_keeps_raw_history_but_static_remains_pit(tmp_path):
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
    assert (pd.Timestamp("2026-06-01"), "000001.SZ") not in static.index
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
