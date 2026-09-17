from __future__ import annotations

import hashlib

import pandas as pd

from scripts.hmm_risk import build_qe_assistance_artifact as cli

MODEL_HASH = "a" * 64
MODEL_CONTRACT = "hmm_risk_rotation_l1_g2a_v1_6"


def test_cli_filters_frozen_pickle_and_v16_rows_to_approved_window(tmp_path, monkeypatch) -> None:
    prediction_path = tmp_path / "pred.pkl"
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-07-01"), "SZ000001"),
            (pd.Timestamp("2024-07-02"), "SH600000"),
            (pd.Timestamp("2026-04-01"), "SZ000002"),
        ]
    )
    pd.DataFrame({"score": [1.0, -1.0, 3.0]}, index=index).to_pickle(prediction_path)
    monkeypatch.setattr(cli, "EXPECTED_SOURCE_FILE_SHA256", hashlib.sha256(prediction_path.read_bytes()).hexdigest())

    rows = cli.load_prediction_rows(
        prediction_path,
        ["2024-07-01", "2024-07-02", "2024-07-03", "2026-04-01", "2026-04-02"],
    )

    assert rows == [
        {
            "source_date": "2024-07-01",
            "trade_date": "2024-07-02",
            "instrument": "000001.SZ",
            "score": 1.0,
        },
        {
            "source_date": "2024-07-02",
            "trade_date": "2024-07-03",
            "instrument": "600000.SH",
            "score": -1.0,
        },
    ]

    report_rows = [
        {"trade_date": "2024-07-01", "model_hash": MODEL_HASH},
        {"trade_date": "2024-07-02", "model_hash": MODEL_HASH},
        {"trade_date": "2026-04-01", "model_hash": MODEL_HASH},
    ]
    states, model_hash = cli._state_rows({"oof_prediction_rows": report_rows}, MODEL_CONTRACT)
    assert states == [{**report_rows[1], "model_contract": MODEL_CONTRACT}]
    assert model_hash == MODEL_HASH
