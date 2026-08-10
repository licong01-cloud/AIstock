from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_contracts import (
    PriceRangeInputArtifactV1,
)
from backend.services.advisory_model_first.price_range_pipeline import (
    _build_labels_in_date_batches,
    _read_bound_parquet,
    _write_json,
)
from backend.services.advisory_model_first import price_range_pipeline


def test_price_range_parquet_readback_requires_exact_frozen_identity(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    frame = pd.DataFrame({"instrument": ["000001.SZ"], "score": [0.5]})
    frame.to_parquet(path, index=False)
    descriptor = PriceRangeInputArtifactV1(
        path=str(path),
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=1,
        columns=tuple(frame.columns),
    )

    pd.testing.assert_frame_equal(_read_bound_parquet(descriptor), frame)
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(AdvisoryModelFirstError) as error:
        _read_bound_parquet(descriptor)
    assert error.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH"


def test_price_range_json_writer_is_atomic_and_rejects_unsupported_values(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    _write_json(path, {"date": pd.Timestamp("2026-01-02")})
    assert path.read_text(encoding="utf-8") == '{"date":"2026-01-02T00:00:00"}'
    assert not path.with_suffix(".json.tmp").exists()
    with pytest.raises(TypeError, match="unsupported price-range JSON value"):
        _write_json(path, {"invalid": object()})


def test_price_range_label_projection_uses_bounded_date_batches_and_cleans_scratch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calendar = pd.bdate_range("2025-01-02", periods=34)
    candidates = pd.DataFrame(
        {
            "decision_as_of_trade_date": calendar[:-1],
            "target_trade_date": calendar[1:],
            "instrument": "000001.SZ",
        }
    )
    projections: list[tuple[str, str]] = []

    def fake_daily(_symbols, *, start, end, fields):
        projections.append((start, end))
        dates = pd.bdate_range(start, end)
        index = pd.MultiIndex.from_product(
            [dates, ["000001.SZ"]], names=["datetime", "instrument"]
        )
        return pd.DataFrame(
            {
                "open": 10.0,
                "low": 9.8,
                "close": 10.0,
                "factor": 1.0,
                "up_limit_price": 11.0,
                "prev_close": 10.0,
                "limit_up": 0.0,
            },
            index=index,
        )

    monkeypatch.setattr(price_range_pipeline, "load_qlib_daily", fake_daily)
    monkeypatch.setattr(
        price_range_pipeline,
        "load_suspend_rows",
        lambda *_args, **_kwargs: pd.DataFrame(
            columns=["trade_date", "instrument", "suspend_type"]
        ),
    )

    result, stats = _build_labels_in_date_batches(
        candidates=candidates,
        trading_calendar=calendar,
        suspend_data_root="/unused",
        scratch_parent=tmp_path,
    )

    assert len(result.labels) == 33
    assert stats["decision_batch_count"] == 2
    assert len(projections) == 2
    assert not list(tmp_path.glob(".price-range-label-parts-*"))
