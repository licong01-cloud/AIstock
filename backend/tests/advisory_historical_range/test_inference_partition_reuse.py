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


def test_identical_physical_view_is_written_once_and_correction_rebuilds(tmp_path, monkeypatch):
    from backend.services.advisory_historical_range.factor_input_materialization import FactorInputSession
    index = pd.MultiIndex.from_product([pd.date_range("2026-01-01", periods=2), ["A"]], names=["datetime", "instrument"])
    price = pd.DataFrame({"close": [5., 6.], "$close": [5., 6.]}, index=index)
    fundamental = pd.DataFrame({"db_close": [5., 6.]}, index=index)
    session = FactorInputSession(root=tmp_path, max_entries=1)
    directories = [tmp_path / name for name in ("first", "hit", "correction")]
    for directory in directories:
        directory.mkdir()
    try:
        first = session.acquire(root=directories[0], price=price, fundamental=fundamental, aliases=True)
        first_inode = (directories[0] / "daily_pv.h5").stat().st_ino
        first.verify()
        first.release()
        with monkeypatch.context() as patch:
            patch.setattr(pd.DataFrame, "to_hdf", lambda *a, **k: pytest.fail("identical view must not serialize again"))
            patch.setattr(pd.DataFrame, "to_parquet", lambda *a, **k: pytest.fail("identical view must not serialize again"))
            hit = session.acquire(root=directories[1], price=price.copy(), fundamental=fundamental.copy(), aliases=True)
            assert hit.counters["materialized_view_hits"] == 1
            assert (directories[1] / "daily_pv.h5").stat().st_ino == first_inode
            assert os.path.samefile(directories[1] / "moneyflow.h5", directories[1] / "static_factors.h5")
            hit.verify()
            hit.release()
        price.iloc[0, 0] = 50.
        changed = session.acquire(root=directories[2], price=price, fundamental=fundamental, aliases=True)
        assert changed.counters["materialized_view_writes"] == 1
        pd.testing.assert_frame_equal(pd.read_hdf(directories[2] / "daily_pv.h5"), price)
        changed.verify()
        changed.release()
        assert len(list(session.root.iterdir())) == 1
    finally:
        session.close()
    assert not session.root.exists()


def test_zero_disk_budget_is_not_a_package_gate_and_alias_replacement_fails(tmp_path):
    from backend.services.advisory_historical_range.factor_input_materialization import FactorInputSession
    session = FactorInputSession(root=tmp_path, max_bytes=0)
    price = pd.DataFrame({"close": [1.]})
    destination = tmp_path / "reader"
    destination.mkdir()
    try:
        lease = session.acquire(root=destination, price=price, fundamental=pd.DataFrame(), aliases=False)
        assert session.retained_bytes == 0
        with pytest.raises(RuntimeError, match="active lease"):
            session.acquire(root=destination, price=price, fundamental=pd.DataFrame(), aliases=False)
        path = destination / "daily_pv.h5"
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        path.unlink()
        path.write_bytes(b"replacement")
        with pytest.raises(RuntimeError, match="task input alias"):
            lease.verify()
        lease.release()
        assert not list(session.root.iterdir())
    finally:
        session.close()
