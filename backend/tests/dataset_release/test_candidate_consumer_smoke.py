from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import pytest

from backend.services.dataset_release.candidate_consumer_smoke import (
    CANDIDATE_CONSUMER_SMOKE_SCHEMA,
    CandidateConsumerSmokeError,
    CandidateConsumerSmokeSpec,
    run_candidate_consumer_smoke,
)
from backend.services.dataset_release.index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    INDEX_H5_COLUMNS,
)


CUTOFF = date(2026, 7, 31)
INDEX_CODES = tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)


class _FakeQlib:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def init(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))


class _FakeD:
    def __init__(
        self,
        *,
        omit_index: str | None = None,
        minute_rows: int = 240,
        duplicate_index: bool = False,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.omit_index = omit_index
        self.minute_rows = minute_rows
        self.duplicate_index = duplicate_index

    def features(
        self,
        instruments,
        fields,
        *,
        start_time,
        end_time,
        freq,
    ) -> pd.DataFrame:
        codes = [value for value in instruments if value != self.omit_index]
        if self.duplicate_index and len(codes) > 1:
            codes.append(codes[0])
        if freq == "1min":
            cutoff = pd.Timestamp(end_time).date()
            moments = [datetime.combine(cutoff, time(9, 31)) + timedelta(minutes=value) for value in range(120)] + [
                datetime.combine(cutoff, time(13, 1)) + timedelta(minutes=value) for value in range(120)
            ]
            moments = moments[: self.minute_rows]
        else:
            moments = [pd.Timestamp(end_time)]
        index = pd.MultiIndex.from_tuples(
            [(code, moment) for code in codes for moment in moments],
            names=["instrument", "datetime"],
        )
        frame = pd.DataFrame(
            [[float(position + 1) for position in range(len(fields))] for _ in range(len(index))],
            index=index,
            columns=list(fields),
        )
        self.calls.append(
            {
                "instruments": tuple(instruments),
                "fields": tuple(fields),
                "start_time": start_time,
                "end_time": end_time,
                "freq": freq,
            }
        )
        return frame


def _index_h5(path: Path) -> None:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-07-30"), "000300.SH"),
            (pd.Timestamp("2026-07-31"), "000300.SH"),
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(
        [[float(position + 1) for position in range(len(INDEX_H5_COLUMNS))]] * 2,
        index=index,
        columns=list(INDEX_H5_COLUMNS),
    )
    frame.to_hdf(path, key="data", format="table", data_columns=True)


def _spec(tmp_path: Path) -> CandidateConsumerSmokeSpec:
    h5 = tmp_path / "index_daily.h5"
    _index_h5(h5)
    return CandidateConsumerSmokeSpec(
        daily_provider_uri="/candidate/daily_bin/qlib",
        minute_provider_uri="/candidate/minute_bin/qlib",
        index_h5_path=h5,
        cutoff=CUTOFF,
        stock_instrument="000001.SZ",
        expected_index_codes=INDEX_CODES,
        profile="qe_backtest_monthly_v1",
        run_id="run-1",
        attempt_id="attempt-1",
        attempt_fence=1,
        release_id="release-1",
        release_digest="a" * 64,
        staging_relative_path=".staging/release-1",
        max_h5_rows=1,
        stage_timeout_seconds=43_200,
        execution_kind="fixture_contract_test",
    )


def test_consumer_smoke_calls_actual_qlib_public_contract_and_h5_loader(
    tmp_path: Path,
) -> None:
    runtime = _FakeQlib()
    data = _FakeD()
    checkpoints: list[bool] = []

    receipt = run_candidate_consumer_smoke(
        _spec(tmp_path),
        checkpoint=lambda: checkpoints.append(True),
        qlib_runtime=runtime,
        data_api=data,
    )

    assert receipt["schema_version"] == CANDIDATE_CONSUMER_SMOKE_SCHEMA
    assert receipt["execution_kind"] == "fixture_contract_test"
    assert runtime.calls == [
        {
            "provider_uri": {
                "day": "/candidate/daily_bin/qlib",
                "1min": "/candidate/minute_bin/qlib",
            },
            "region": "cn",
            "clear_mem_cache": True,
        }
    ]
    assert [call["freq"] for call in data.calls] == ["day", "1min", "day", "day"]
    assert data.calls[2]["instruments"] == INDEX_CODES
    assert data.calls[3]["fields"] == ("$close/Ref($close,1)-1",)
    assert receipt["qe"]["indices"]["codes"] == list(INDEX_CODES)
    assert receipt["hmm_index_contract"]["benchmark"] == "000300.SH"
    assert receipt["consumer_activation"]["existing_hmm"] == ("not_activated_not_switched")
    assert len(checkpoints) >= 8


def test_consumer_smoke_fails_closed_when_one_required_index_is_missing(
    tmp_path: Path,
) -> None:
    with pytest.raises(CandidateConsumerSmokeError, match="omits required index"):
        run_candidate_consumer_smoke(
            _spec(tmp_path),
            checkpoint=lambda: None,
            qlib_runtime=_FakeQlib(),
            data_api=_FakeD(omit_index=INDEX_CODES[-1]),
        )


@pytest.mark.parametrize(
    "data_api",
    [
        _FakeD(minute_rows=1),
        _FakeD(minute_rows=239),
        _FakeD(duplicate_index=True),
    ],
)
def test_consumer_smoke_rejects_short_minute_or_duplicate_index_keys(tmp_path: Path, data_api: _FakeD) -> None:
    with pytest.raises(CandidateConsumerSmokeError, match="exact contract"):
        run_candidate_consumer_smoke(
            _spec(tmp_path),
            checkpoint=lambda: None,
            qlib_runtime=_FakeQlib(),
            data_api=data_api,
        )
