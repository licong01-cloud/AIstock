from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_risk.rotation_l2 import RotationL2Error
from backend.services.hmm_risk import rotation_l2_input as subject
from backend.services.hmm_risk.rotation_l2_input import _availability, _daily_aggregates, _expand_expected


def test_quote_availability_uses_only_frozen_spans() -> None:
    entries = {"801011.SI": ((date(2024, 7, 1), date(2026, 4, 28)),)}

    assert _availability(entries, "801011.SI", date(2026, 4, 28)) is True
    assert _availability(entries, "801011.SI", date(2026, 4, 29)) is False


def test_expected_population_is_pit_and_excludes_only_full_day_suspend() -> None:
    membership = pd.DataFrame(
        [
            {
                "instrument": "000001.SZ",
                "start_date": date(2024, 7, 1),
                "end_date": date(2024, 7, 3),
                "l2_code_id": 133,
            },
            {
                "instrument": "000002.SZ",
                "start_date": date(2024, 7, 2),
                "end_date": date(2024, 7, 3),
                "l2_code_id": 133,
            },
        ]
    )
    expected, members = _expand_expected(
        membership,
        id_to_code={133: "801783.SI"},
        provider_spans={
            "000001.SZ": (date(2024, 7, 1), date(2024, 7, 3)),
            "000002.SZ": (date(2024, 7, 1), date(2024, 7, 3)),
        },
        source_days=(date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)),
        suspended={(date(2024, 7, 2), "000001.SZ")},
    )

    assert members[(date(2024, 7, 2), "801783.SI")] == 2
    day_two = expected[expected["trade_date"] == date(2024, 7, 2)]
    assert day_two["instrument"].tolist() == ["000002.SZ"]


def test_unknown_provider_instrument_fails_closed() -> None:
    membership = pd.DataFrame(
        [{"instrument": "000001.SZ", "start_date": date(2024, 7, 1), "end_date": date(2024, 7, 1), "l2_code_id": 1}]
    )

    with pytest.raises(RotationL2Error, match="absent from PIT provider universe"):
        _expand_expected(
            membership,
            id_to_code={1: "801011.SI"},
            provider_spans={},
            source_days=(date(2024, 7, 1),),
            suspended=set(),
        )


def _aggregate_inputs(tmp_path, monkeypatch, *, authorize_missing: bool):
    day = date(2025, 1, 2)
    instruments = [f"0000{index:02d}.SZ" for index in range(10)]
    membership = pd.DataFrame(
        [{"instrument": instrument, "start_date": day, "end_date": day, "l2_code_id": 1} for instrument in instruments]
    )
    provider_path = tmp_path / "all.txt"
    provider_path.write_text(
        "".join(f"{instrument}\t{day.isoformat()}\t{day.isoformat()}\n" for instrument in instruments),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject.pd,
        "read_parquet",
        lambda *_args, **_kwargs: pd.DataFrame(columns=["trade_date", "ts_code", "suspend_type", "suspend_timing"]),
    )
    monkeypatch.setattr(
        subject,
        "_hdf_slice",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "datetime": [day] * 9,
                "instrument": instruments[:9],
                "mf_net_amt": [1.0] * 9,
            }
        ),
    )
    monkeypatch.setattr(
        subject,
        "_qlib_amount_frame",
        lambda *_args, **_kwargs: (
            pd.DataFrame({"trade_date": [day] * 10, "instrument": instruments, "amount_cny": [10.0] * 10}),
            "a" * 64,
        ),
    )
    security = SimpleNamespace(resolve=lambda instrument, _day, _dataset: SimpleNamespace(source_ts_code=instrument))
    absence_key = (instruments[-1], "market.moneyflow_ts", instruments[-1], day)
    absence = SimpleNamespace(by_key={absence_key: object()} if authorize_missing else {})
    return day, membership, provider_path, security, absence


def test_exact_provider_absence_stays_in_denominator_at_ninety_percent_boundary(tmp_path, monkeypatch) -> None:
    day, membership, provider_path, security, absence = _aggregate_inputs(tmp_path, monkeypatch, authorize_missing=True)

    rows, amount_set_sha256 = _daily_aggregates(
        root=tmp_path,
        manifest={},
        membership=membership,
        id_to_code={1: "801011.SI"},
        quote_entries={"801011.SI": ((day, day),)},
        catalog=["801011.SI"],
        source_days=[day],
        provider_path=provider_path,
        suspend_path=tmp_path / "suspend.parquet",
        moneyflow_path=tmp_path / "moneyflow.h5",
        qlib_root=tmp_path,
        calendar=[day],
        security_identity=security,
        provider_absence=absence,
    )

    assert rows[0]["expected_contributors"] == 10
    assert rows[0]["valid_contributors"] == 9
    assert rows[0]["coverage"] == 0.9
    assert rows[0]["eligible"] is True
    assert amount_set_sha256 == "a" * 64


def test_unknown_moneyflow_gap_fails_closed(tmp_path, monkeypatch) -> None:
    day, membership, provider_path, security, absence = _aggregate_inputs(
        tmp_path, monkeypatch, authorize_missing=False
    )

    with pytest.raises(RotationL2Error, match="without frozen authority"):
        _daily_aggregates(
            root=tmp_path,
            manifest={},
            membership=membership,
            id_to_code={1: "801011.SI"},
            quote_entries={"801011.SI": ((day, day),)},
            catalog=["801011.SI"],
            source_days=[day],
            provider_path=provider_path,
            suspend_path=tmp_path / "suspend.parquet",
            moneyflow_path=tmp_path / "moneyflow.h5",
            qlib_root=tmp_path,
            calendar=[day],
            security_identity=security,
            provider_absence=absence,
        )


def test_observed_zero_amount_is_not_misclassified_as_provider_absence(tmp_path, monkeypatch) -> None:
    day, membership, provider_path, security, absence = _aggregate_inputs(tmp_path, monkeypatch, authorize_missing=True)
    monkeypatch.setattr(
        subject,
        "_qlib_amount_frame",
        lambda *_args, **_kwargs: (
            pd.DataFrame(
                {
                    "trade_date": [day] * 10,
                    "instrument": membership["instrument"].tolist(),
                    "amount_cny": [0.0] * 10,
                }
            ),
            "b" * 64,
        ),
    )

    rows, amount_set_sha256 = _daily_aggregates(
        root=tmp_path,
        manifest={},
        membership=membership,
        id_to_code={1: "801011.SI"},
        quote_entries={"801011.SI": ((day, day),)},
        catalog=["801011.SI"],
        source_days=[day],
        provider_path=provider_path,
        suspend_path=tmp_path / "suspend.parquet",
        moneyflow_path=tmp_path / "moneyflow.h5",
        qlib_root=tmp_path,
        calendar=[day],
        security_identity=security,
        provider_absence=absence,
    )

    assert rows[0]["expected_contributors"] == 10
    assert rows[0]["valid_contributors"] == 9
    assert rows[0]["amount_cny"] == 0.0
    assert rows[0]["eligible"] is False
    assert rows[0]["reason_code"] == "hmm_risk_rotation_l2_source_invalid"
    assert amount_set_sha256 == "b" * 64


@pytest.mark.parametrize(
    ("suspend_timing", "expect_pass"),
    [
        (None, True),
        ("", True),
        (" 09:30-09:30 ", True),
        ("09:30-10:00", False),
    ],
)
def test_daily_aggregates_applies_existing_full_day_suspension_contract(
    tmp_path,
    monkeypatch,
    suspend_timing,
    expect_pass,
) -> None:
    day = date(2026, 1, 16)
    instruments = [f"6880{index:02d}.SH" for index in range(10)]
    membership = pd.DataFrame(
        [{"instrument": instrument, "start_date": day, "end_date": day, "l2_code_id": 1} for instrument in instruments]
    )
    provider_path = tmp_path / "all.txt"
    provider_path.write_text(
        "".join(f"{instrument}\t{day.isoformat()}\t{day.isoformat()}\n" for instrument in instruments),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject.pd,
        "read_parquet",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "ts_code": instruments[0],
                    "suspend_type": "S",
                    "suspend_timing": suspend_timing,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        subject,
        "_hdf_slice",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "datetime": [day] * 9,
                "instrument": instruments[1:],
                "mf_net_amt": [1.0] * 9,
            }
        ),
    )
    monkeypatch.setattr(
        subject,
        "_qlib_amount_frame",
        lambda _root, *, calendar, expected: (
            pd.DataFrame(
                {
                    "trade_date": [day] * 9,
                    "instrument": instruments[1:],
                    "amount_cny": [10.0] * 9,
                }
            ),
            "c" * 64,
        ),
    )
    security = SimpleNamespace(resolve=lambda instrument, _day, _dataset: SimpleNamespace(source_ts_code=instrument))
    absence = SimpleNamespace(by_key={})

    def invoke():
        return _daily_aggregates(
            root=tmp_path,
            manifest={},
            membership=membership,
            id_to_code={1: "801011.SI"},
            quote_entries={"801011.SI": ((day, day),)},
            catalog=["801011.SI"],
            source_days=[day],
            provider_path=provider_path,
            suspend_path=tmp_path / "suspend.parquet",
            moneyflow_path=tmp_path / "moneyflow.h5",
            qlib_root=tmp_path,
            calendar=[day],
            security_identity=security,
            provider_absence=absence,
        )

    if not expect_pass:
        with pytest.raises(RotationL2Error, match="expected Qlib amount rows are absent"):
            invoke()
        return

    rows, amount_set_sha256 = invoke()
    assert rows[0]["expected_contributors"] == 9
    assert rows[0]["valid_contributors"] == 9
    assert rows[0]["coverage"] == 1.0
    assert rows[0]["eligible"] is True
    assert amount_set_sha256 == "c" * 64


def test_non_textual_suspend_timing_fails_closed() -> None:
    with pytest.raises(RotationL2Error, match="suspend_d timing is not textual"):
        subject._is_full_day_suspend_timing(930)
