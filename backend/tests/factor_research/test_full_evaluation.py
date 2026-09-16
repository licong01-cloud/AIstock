"""Compact full-evaluation schema, denominator and immutable-output contracts."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.full_evaluation import (
    compute_correlation_views,
    load_reference_values,
    validate_full_evaluation_spec,
)
from backend.services.factor_research.models import ResearchError


def values(path, column, dates, symbols, *, parquet=False):
    index = pd.MultiIndex.from_product([dates, symbols], names=["datetime", "instrument"])
    frame = pd.DataFrame({column: np.tile(np.arange(len(symbols), dtype=float), len(dates))}, index=index)
    frame.to_parquet(path) if parquet else frame.to_hdf(path, key="data")


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda frame: frame.rename(columns={"value": "wrong"}), "official value column"),
        (lambda frame: frame.reset_index(drop=True), "MultiIndex"),
        (lambda frame: pd.concat([frame, frame.iloc[[0]]]), "duplicate"),
    ],
)
def test_reference_schema_is_strict_but_nonfinite_is_explicit(tmp_path, mutation, match):
    dates, symbols = pd.bdate_range("2026-01-01", periods=2), ["000001.SZ", "000002.SZ"]
    path = tmp_path / "reference.parquet"
    values(path, "value", dates, symbols, parquet=True)
    frame = pd.read_parquet(path)
    mutation(frame).to_parquet(path)
    with pytest.raises(ResearchError, match=match):
        load_reference_values(path, "m_reference")
    values(path, "value", dates, symbols, parquet=True)
    frame = pd.read_parquet(path)
    frame.iloc[:2, 0] = [np.inf, np.nan]
    frame.to_parquet(path)
    loaded = load_reference_values(path, "m_reference")
    assert loaded["m_reference"].iloc[:2].isna().all()
    assert loaded.attrs["reference_value_quality"]["infinite_values_excluded"] == 1


def test_full_evaluation_computes_only_requested_candidate_pairs(tmp_path):
    dates = pd.bdate_range("2024-01-02", periods=4)
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    paths = {}
    for name in ("m_a", "m_b"):
        paths[name] = tmp_path / f"{name}.h5"
        values(paths[name], name, dates, symbols)
    reference = tmp_path / "reference.parquet"
    values(reference, "value", dates, symbols, parquet=True)
    settings = {
        "reference_value_artifacts": {"m_reference": str(reference)},
        "correlation_batch_size": 1,
        "correlation_half_life": 2,
        "correlation_min_stocks": 3,
        "correlation_min_effective_days": 2,
    }
    normalized = validate_full_evaluation_spec(settings, candidate_names={"m_a", "m_b"}, repo_root=Path.cwd())
    windows = {"full": {"start": str(dates[0].date()), "end": str(dates[-1].date())}}
    result = compute_correlation_views(paths, normalized, windows)
    assert result["reference_reference_pairs_computed"] == 0
    assert all(row["requested_pairs"] == 1 for row in result["candidate_candidate_windows"])
