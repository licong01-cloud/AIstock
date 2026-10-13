from __future__ import annotations

import os
import stat

import pandas as pd
import pytest

from backend.services.advisory_historical_range.factor_input_materialization import STATIC_ALIASES, materialize_factor_aliases


def test_compatible_aliases_share_one_physical_hdf_and_match_original_schema(tmp_path):
    index = pd.MultiIndex.from_product([pd.date_range("2025-12-29", periods=2), ["SH600001"]], names=["datetime", "instrument"])
    price = pd.DataFrame({"close": [5.0, 6.0], "$close": [5.0, 6.0]}, index=index)
    fundamental = pd.DataFrame({"db_turnover_rate": [1.0, 2.0], "sector": ["A", "A"]}, index=index)
    price.to_hdf(tmp_path / "daily_pv.h5", key="data")
    fundamental.to_parquet(tmp_path / "static_factors.parquet")
    lease = materialize_factor_aliases(root=tmp_path, price=price, fundamental=fundamental)
    try:
        for name in STATIC_ALIASES:
            assert os.path.samefile(tmp_path / name, tmp_path / "static_factors.h5")
            pd.testing.assert_frame_equal(pd.read_hdf(tmp_path / name), fundamental, check_exact=True)
        pd.testing.assert_frame_equal(pd.read_hdf(tmp_path / "daily_pv_clean.h5"), price[["close"]], check_exact=True)
        assert lease.counters["static_hdf_materializations"] == 1
        reader_link = tmp_path / "reader-reference.h5"
        os.link(tmp_path / "static_factors.h5", reader_link)
        lease.verify()
        if os.name == "nt":
            reader_link.chmod(stat.S_IRUSR | stat.S_IWUSR)
        reader_link.unlink()
        (tmp_path / "static_factors.h5").chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        lease.verify()
        # A factor can deliberately remove read-only protection; the lease must
        # still detect the write rather than publishing a contaminated score.
        changed = tmp_path / "moneyflow.h5"
        changed.chmod(stat.S_IRUSR | stat.S_IWUSR)
        changed.write_bytes(b"invalid modified factor input")
        with pytest.raises(RuntimeError, match="changed a shared"):
            lease.verify()
    finally:
        lease.release()


def test_alias_link_failure_never_falls_back_to_copy(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range import factor_input_materialization as materialization

    def unavailable(*args, **kwargs):
        raise OSError("link unavailable")

    monkeypatch.setattr(materialization.os, "link", unavailable)
    monkeypatch.setattr(materialization.Path, "symlink_to", unavailable)
    with pytest.raises(RuntimeError, match="copying large inputs is forbidden"):
        materialization._link(tmp_path / "source", tmp_path / "alias")
