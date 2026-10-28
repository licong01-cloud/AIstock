"""Physical batching must preserve market facts, failures and causal isolation."""
import json
import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.intraday.contracts import payload
from backend.services.position_timing.intraday.inputs import file_sha256
from backend.services.position_timing.intraday.inputs import visible_prefix
from backend.services.position_timing.intraday.research_inputs import CandidateReader
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.store import IntradayError
from test_intraday_research import qlib_fixture


@pytest.fixture
def candidate(tmp_path):
    ref = qlib_fixture(tmp_path)
    manifest = json.loads((tmp_path / "qe_dataset_manifest.json").read_text())
    parts = manifest["components"]
    def write(relative, data):
        path = tmp_path / relative
        path.write_bytes(data)
        parts[relative] = {"path": relative, "size": len(data), "sha256": file_sha256(path)}
    for relative in list(parts):
        if relative.endswith("calendars/day.txt"):
            write(relative, b"2026-10-22\n2026-10-23\n2026-10-26\n")
        elif relative.endswith("calendars/1min.txt"):
            write(relative, b"2026-10-22 09:31:00\n2026-10-23 09:31:00\n2026-10-26 09:31:00\n")
        elif relative.endswith("instruments/all.txt"):
            write(relative, b"000001.SZ\t2026-10-22 09:31:00\t2026-10-26 15:00:00\n")
        elif relative.endswith(".bin"):
            values = np.fromfile(tmp_path / relative, dtype="<f4")
            write(relative, np.array([0, values[1], values[1], values[1]], dtype="<f4").tobytes())
    suspension = "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
    pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": [pd.Timestamp("2026-10-23")],
                  "suspend_type": ["S"], "suspend_timing": [None]}).to_parquet(tmp_path / suspension)
    write(suspension, (tmp_path / suspension).read_bytes())
    (tmp_path / "qe_dataset_manifest.json").write_text(json.dumps(manifest))
    return FileRef(path=ref.path, sha256=file_sha256(tmp_path / "qe_dataset_manifest.json"))


DAYS = [date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 26)]
SYMBOL = "000001.SZ"


def test_multiday_objects_pins_suspension_and_window_replacement(candidate):
    daily, batch = CandidateReader(candidate), CandidateReader(candidate)
    batch.prepare_window(DAYS[0], DAYS[-1], [SYMBOL, "999999.SZ"])
    assert not batch.pins  # speculative inputs are not consumed evidence
    for day in DAYS:
        assert payload(daily.market(SYMBOL, day)) == payload(batch.market(SYMBOL, day))
        daily.verify_pins()
        batch.verify_pins()
        assert daily.pins == batch.pins
    assert batch.market(SYMBOL, DAYS[1]).day_facts.is_suspended
    batch.prepare_window(DAYS[-1], DAYS[-1], [SYMBOL])
    assert payload(daily.market(SYMBOL, DAYS[0])) == payload(batch.market(SYMBOL, DAYS[0]))
    assert batch.window_status["cache_hits"] > 0
    batch.release_window()
    assert batch.window_status["array_bytes"] == 0
    assert payload(daily.market(SYMBOL, DAYS[-1])) == payload(batch.market(SYMBOL, DAYS[-1]))


@pytest.mark.parametrize("fault,reason", [("missing", "COMPONENT_NOT_IN_MANIFEST"),
                                          ("nan", "DAILY_PRICE_UNAVAILABLE"),
                                          ("history", "BIN_HISTORY_INCOMPLETE")])
def test_optional_prefetch_defers_necessary_failures(candidate, fault, reason):
    root = Path(candidate.path).parent
    manifest = json.loads((root / "qe_dataset_manifest.json").read_text())
    relative = "components/daily_bin_candidate/features/000001.sz/prev_close.day.bin"
    if fault == "missing":
        del manifest["components"][relative]
    else:
        (root / relative).write_bytes(np.array([2, 10] if fault == "history" else [0, 10, 10, np.nan], dtype="<f4").tobytes())
        manifest["components"][relative].update(size=(root / relative).stat().st_size, sha256=file_sha256(root / relative))
    (root / "qe_dataset_manifest.json").write_text(json.dumps(manifest))
    ref = FileRef(path=candidate.path, sha256=file_sha256(root / "qe_dataset_manifest.json"))
    for prepared in (False, True):
        reader = CandidateReader(ref)
        if prepared:
            reader.prepare_window(DAYS[0], DAYS[-1], [SYMBOL])
        # A suspended day must not demand the unusable daily/SELL input.
        assert reader.market(SYMBOL, DAYS[1]).day_facts.is_suspended
        reader.verify_pins()
        bad_day = DAYS[0] if fault in {"missing", "history"} else DAYS[-1]
        with pytest.raises(IntradayError, match=reason):
            reader.market(SYMBOL, bad_day)
        reader.close()


def test_same_size_same_mtime_revision_fails_and_consumers_are_isolated(candidate):
    first, second = CandidateReader(candidate), CandidateReader(candidate)
    first.prepare_window(DAYS[0], DAYS[-1], [SYMBOL])
    second.prepare_window(DAYS[-1], DAYS[-1], [SYMBOL])
    before = payload(first.market(SYMBOL, DAYS[0]))
    second.release_window()
    assert first.window_status["array_bytes"] > 0
    assert before == payload(first.market(SYMBOL, DAYS[0]))
    first.verify_pins()
    path = Path(candidate.path).parent / "components/minute_bin_candidate/features/000001.sz/close.1min.bin"
    stat = path.stat()
    raw = np.frombuffer(path.read_bytes(), dtype="<f4").copy()
    raw[-1] += 1
    path.write_bytes(raw.tobytes())
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(IntradayError, match="SOURCE_CHANGED_DURING_READ"):
        first.verify_pins()
    with pytest.raises(IntradayError, match="SOURCE_CHANGED_DURING_READ"):
        first.values(SYMBOL, DAYS[0], "close", "1min")
    first.close()
    second.close()


def test_readonly_budget_close_and_future_payload_not_exposed(candidate, monkeypatch):
    reader = CandidateReader(candidate)
    monkeypatch.setattr(reader, "MAX_ARRAY_BYTES", 64)
    reader.prepare_window(DAYS[0], DAYS[-1], [SYMBOL])
    _, values = reader.values(SYMBOL, DAYS[0], "close", "1min")
    assert len(values) == 1
    with pytest.raises(ValueError):
        values.setflags(write=True)
    with pytest.raises(ValueError):
        reader.calendar("1min").values.setflags(write=True)
    market = reader.market(SYMBOL, DAYS[0])
    assert all(bar.end.date() == DAYS[0] for bar in market.bars)
    assert not visible_prefix(market, market.bars[0].end).bars
    assert len(visible_prefix(market, market.bars[0].received_at).bars) == 1
    status = reader.window_status
    assert status["array_bytes"] <= 64 and status["evictions"] > 0
    assert status["open_mappings"] == 0 and status["peak_open_mappings"] == 1
    reader.close()
    reader.close()
    assert reader.window_status["array_bytes"] == reader.window_status["index_bytes"] == 0
    for action in (lambda: reader.market(SYMBOL, DAYS[0]), lambda: reader.verify_pins(),
                   lambda: reader.prepare_window(DAYS[0], DAYS[0], [SYMBOL])):
        with pytest.raises(IntradayError, match="READER_CLOSED"):
            action()
    fresh = CandidateReader(candidate)
    with pytest.raises(IntradayError, match="WINDOW_INVALID"):
        fresh.prepare_window(DAYS[-1], DAYS[0], [SYMBOL])
    with pytest.raises(IntradayError, match="WINDOW_BUDGET_EXCEEDED"):
        fresh.prepare_window(DAYS[0], DAYS[-1], map(str, range(1025)))
