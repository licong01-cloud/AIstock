from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.hmm_risk import observed_industry_authority as subject


def test_refresh_reuses_bytes_and_first_observation_but_records_new_check(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    raw = _official_workbook_bytes()
    now = [datetime(2026, 11, 5, 9, tzinfo=timezone.utc)]
    monkeypatch.setattr(source, "_download", lambda: raw)
    monkeypatch.setattr(source, "_now", lambda: now[0])
    first = source.refresh(tmp_path / "sources")
    now[0] = datetime(2026, 11, 6, 9, tzinfo=timezone.utc)
    second = source.refresh(tmp_path / "sources")
    a, b = (source.load_capture(Path(p["path"]), p["sha256"])[0] for p in (first, second))
    assert a["first_observed_at"] == b["first_observed_at"] == "2026-11-05T09:00:00+00:00"
    assert b["captured_at"] == "2026-11-06T09:00:00+00:00"
    assert a["path"] == b["path"]
    assert len(list((tmp_path / "sources" / "workbooks").iterdir())) == 1
    assert source.read_current(tmp_path / "sources") == second


def test_failed_refresh_keeps_last_observation_without_claiming_success(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    previous = source.refresh(tmp_path / "sources")
    monkeypatch.setattr(source, "_download", lambda: b"<html>blocked</html>")
    with pytest.raises(ValueError):
        source.refresh(tmp_path / "sources")
    assert source.read_current(tmp_path / "sources") == previous


def _official_workbook_bytes(code="480301", included="2021-07-30"):
    import io

    output = io.BytesIO()
    pd.DataFrame(
        [["000001", pd.Timestamp(included), code, pd.NaT]], columns=["股票代码", "计入日期", "行业代码", "更新日期"]
    ).to_excel(output, index=False)
    return output.getvalue()


def test_returning_old_content_is_a_new_observation_segment(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    a, b = _official_workbook_bytes(), _official_workbook_bytes(code="370102")
    current = [a, datetime(2026, 11, 3, 9, tzinfo=timezone.utc)]
    monkeypatch.setattr(source, "_download", lambda: current[0])
    monkeypatch.setattr(source, "_now", lambda: current[1])
    source.refresh(tmp_path / "sources")
    current[:] = [b, datetime(2026, 11, 4, 9, tzinfo=timezone.utc)]
    source.refresh(tmp_path / "sources")
    current[:] = [a, datetime(2026, 11, 5, 9, tzinfo=timezone.utc)]
    pin = source.refresh(tmp_path / "sources")
    capture, _ = source.load_capture(Path(pin["path"]), pin["sha256"])
    assert capture["first_observed_at"] == "2026-11-05T09:00:00+00:00"
    assert capture["first_observation"] is None
    assert len(list((tmp_path / "sources" / "workbooks").iterdir())) == 2


@pytest.mark.parametrize("fault", ["network", "hash_drift", "clock_rollback", "locked"])
def test_refresh_errors_do_not_advance_observation_pointer(tmp_path, monkeypatch, fault):
    from scripts.hmm_risk import observed_industry_source as source

    now = datetime(2026, 11, 5, 9, tzinfo=timezone.utc)
    monkeypatch.setattr(source, "_now", lambda: now)
    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    root = tmp_path / "sources"
    pin = source.refresh(root)
    pointer = (root / "current.json").read_bytes()
    if fault == "network":

        def fail():
            raise OSError("injected network failure")

        monkeypatch.setattr(source, "_download", fail)
    elif fault == "hash_drift":
        capture = json.loads(Path(pin["path"]).read_text())
        Path(capture["path"]).write_bytes(b"changed")
    elif fault == "clock_rollback":
        monkeypatch.setattr(source, "_now", lambda: datetime(2026, 11, 4, tzinfo=timezone.utc))
    if fault == "locked":
        with source._locked(root), pytest.raises(ValueError, match="in progress"):
            source.refresh(root)
    else:
        with pytest.raises((ValueError, OSError)):
            source.refresh(root)
    assert (root / "current.json").read_bytes() == pointer


@pytest.mark.parametrize("fault", [None, "missing_symbol", "projection_hash", "unknown_l2", "ambiguous"])
def test_direct_source_mapping_preserves_official_text_identity_and_time(tmp_path, monkeypatch, fault):
    import io
    from scripts.hmm_risk import observed_industry_source as source
    from backend.services.dataset_release.canonical import digest_named_fields

    monkeypatch.setattr(source, "_now", lambda: datetime(2026, 11, 5, 9, tzinfo=timezone.utc))
    raw = _official_workbook_bytes()
    if fault == "ambiguous":
        frame = pd.read_excel(io.BytesIO(raw), dtype=object)
        frame.loc[1] = ["000001", pd.Timestamp("2021-07-30"), "370102", pd.NaT]
        stream = io.BytesIO()
        frame.to_excel(stream, index=False)
        raw = stream.getvalue()
    monkeypatch.setattr(source, "_download", lambda: raw)
    pin = source.refresh(tmp_path / "sources")
    projection = {"rows": [{"taxonomy_l2_code": "480300", "canonical_l2_code": "801783.SI"}]}
    if fault == "unknown_l2":
        projection["rows"][0]["taxonomy_l2_code"] = "370100"
    projection["canonical_hash"] = digest_named_fields(source.HMM_L2_CODE_PROJECTION_SCHEMA, projection)
    if fault == "projection_hash":
        projection["canonical_hash"] = "0" * 64
    symbols = {"000001.SZ", "000002.SZ"} if fault == "missing_symbol" else {"000001.SZ"}
    args = (Path(pin["path"]), pin["sha256"], {"l2_projection": projection})
    if fault:
        with pytest.raises(ValueError):
            source.observation_from_capture(*args, data_date=date(2026, 11, 6), symbols=symbols)
    else:
        observed = source.observation_from_capture(*args, data_date=date(2026, 11, 6), symbols=symbols)
        row = observed.rows["000001.SZ"]
        assert row["canonical_l2_code"] == "801783.SI"
        assert row["publication_at"] is None
        assert row["publication_time_quality"] == "FIRST_PUBLICATION_UNVERIFIED"
        assert subject.known_day(observed.captured_at, (date(2026, 11, 5), date(2026, 11, 6))) == date(2026, 11, 6)
        with pytest.raises(ValueError, match="no next session"):
            subject.known_day(observed.captured_at, (date(2026, 10, 29),))


def test_copied_earlier_timestamp_without_source_evidence_is_rejected(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    pin = source.refresh(tmp_path / "sources")
    body = json.loads(Path(pin["path"]).read_bytes())
    body["first_observed_at"] = "2021-01-01T00:00:00+00:00"
    forged = tmp_path / "forged.json"
    forged.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="no evidence"):
        source.load_capture(forged, hashlib.sha256(forged.read_bytes()).hexdigest())


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "unknown"),
        ("tls_verification_disabled", True),
        ("final_url", "http://invalid.example/file.xls"),
        ("bytes", 1),
        ("row_count", 999),
        ("first_publication_verified", True),
        ("business_preflight_passed", True),
    ],
)
def test_capture_metadata_drift_is_not_accepted(tmp_path, monkeypatch, field, value):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    pin = source.refresh(tmp_path / "sources")
    body = json.loads(Path(pin["path"]).read_bytes())
    body[field] = value
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(body))
    with pytest.raises(ValueError):
        source.load_capture(changed, hashlib.sha256(changed.read_bytes()).hexdigest())
