from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from scripts.precompute_hmm_coefficients import (
    FROZEN_QUOTE_AVAILABILITY_SCHEMA,
    build_stock_sector_membership_spans,
    load_frozen_coefficient_inputs,
    main,
    resolve_coefficient_membership_maps,
)


def test_build_stock_sector_membership_spans_closes_changes_and_exits() -> None:
    result = build_stock_sector_membership_spans(
        {
            "2026-06-01": {"000001.SZ": "A.SI", "000002.SZ": "B.SI"},
            "2026-06-02": {"000001.SZ": "C.SI", "000002.SZ": "B.SI"},
            "2026-06-03": {"000001.SZ": "C.SI"},
        }
    )

    assert result == {
        "000001.SZ": [
            {"start_date": "2026-06-01", "end_date": "2026-06-01", "sector_code": "A.SI"},
            {"start_date": "2026-06-02", "end_date": "2026-06-03", "sector_code": "C.SI"},
        ],
        "000002.SZ": [
            {"start_date": "2026-06-01", "end_date": "2026-06-02", "sector_code": "B.SI"},
        ],
    }


def test_resolve_daily_output_uses_as_of_membership_without_future_row() -> None:
    result = resolve_coefficient_membership_maps(
        {"2026-06-01": {"000001.SZ": "A.SI"}},
        {"2026-06-02": {"A.SI": 1.1}},
        quote_unavailable_sector_codes_by_date={"2026-06-02": []},
        output_trade_date="2026-06-02",
        as_of_trade_date="2026-06-01",
        backtest_end="2026-06-01",
    )

    assert result == {"2026-06-02": {"000001.SZ": "A.SI"}}


def test_resolve_membership_rejects_sector_without_daily_coefficient() -> None:
    with pytest.raises(ValueError, match="coefficient gaps differ from quote availability"):
        resolve_coefficient_membership_maps(
            {"2026-06-01": {"000001.SZ": "A.SI"}},
            {"2026-06-01": {"B.SI": 1.1}},
            quote_unavailable_sector_codes_by_date={"2026-06-01": []},
            output_trade_date=None,
            as_of_trade_date=None,
            backtest_end="2026-06-01",
        )


def test_resolve_membership_allows_only_explicit_quote_unavailable_sector() -> None:
    result = resolve_coefficient_membership_maps(
        {"2026-06-01": {"000001.SZ": "A.SI", "000002.SZ": "B.SI"}},
        {"2026-06-01": {"A.SI": 1.1}},
        quote_unavailable_sector_codes_by_date={"2026-06-01": ["B.SI"]},
        output_trade_date=None,
        as_of_trade_date=None,
        backtest_end="2026-06-01",
    )

    assert result["2026-06-01"]["000002.SZ"] == "B.SI"


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _shared_code_map_payload(
    entries: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    normalized_entries = entries or [{"l2_code_id": 133, "canonical_l2_code": "801783.SI"}]
    authority = {
        "authority_id": "aistock_sw_l2_shared_catalog_fixture",
        "authority_sha256": "a" * 64,
    }
    return {
        "schema_version": "aistock_release_sw_l2_code_map_v1",
        "mapping_authority": authority,
        "entries": normalized_entries,
        "code_map_digest": digest_named_fields(
            "aistock_release_sw_l2_code_map_v1",
            {
                "mapping_authority": authority,
                "entries": sorted(
                    normalized_entries,
                    key=lambda row: (int(row["l2_code_id"]), str(row["canonical_l2_code"])),
                ),
            },
        ),
    }


def _write_frozen_input_fixture(
    tmp_path,
    *,
    invalid_sector_value: bool = False,
    sector_id: int = 133,
    membership_sector_id: int | None = None,
    code_map_entries: list[dict[str, object]] | None = None,
    quote_spans_by_code: dict[str, list[dict[str, str]]] | None = None,
):
    root = tmp_path / "release"
    root.mkdir()
    dates = pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-03"])
    sector_index = pd.MultiIndex.from_product([dates, ["000001.SZ"]], names=["datetime", "instrument"])
    sector = pd.DataFrame(
        {
            "l2_code_id": [sector_id] * len(sector_index),
            "sw2_pct_change": [0.1, 0.2, 0.3][-len(sector_index) :],
            "sw2_vol": 100.0,
            "sw2_amount": float("nan") if invalid_sector_value else 1000.0,
            "sw2_mf_net_amt": 10.0,
            "sw2_mf_buy_elg_amt": 20.0,
            "sw2_mf_sell_elg_amt": 10.0,
        },
        index=sector_index,
    )
    index = pd.DataFrame(
        {
            "trade_date": dates,
            "ts_code": ["000300.SH"] * len(dates),
            "close": [100.0, 101.0, 102.0],
        }
    )
    paths = {
        "sector_data_h5": root / "sector_data.h5",
        "index_daily_h5": root / "index_daily.h5",
        "sector_code_map_json": root / "sector_code_map.json",
        "market_context_parquet": root / "market_context.parquet",
        "sector_membership_spans_parquet": root / "sector_membership_spans.parquet",
        "sector_quote_availability_json": root / "sector_quote_availability.json",
    }
    sector.to_hdf(paths["sector_data_h5"], key="data", format="table", data_columns=True)
    index.to_hdf(paths["index_daily_h5"], key="data", format="fixed")
    paths["sector_code_map_json"].write_text(
        json.dumps(_shared_code_map_payload(code_map_entries)),
        encoding="utf-8",
    )
    code_map_payload = _shared_code_map_payload(code_map_entries)
    quote_entries = [
        {
            "canonical_l2_code": str(entry["canonical_l2_code"]),
            "availability_spans": (
                quote_spans_by_code[str(entry["canonical_l2_code"])]
                if quote_spans_by_code is not None
                else [{"start_date": "2020-01-01", "end_date": "2099-12-31"}]
            ),
        }
        for entry in sorted(code_map_payload["entries"], key=lambda item: str(item["canonical_l2_code"]))
    ]
    quote_payload = {
        "schema_version": FROZEN_QUOTE_AVAILABILITY_SCHEMA,
        "mapping_authority": code_map_payload["mapping_authority"],
        "entries": quote_entries,
        "quote_availability_digest": digest_named_fields(
            FROZEN_QUOTE_AVAILABILITY_SCHEMA,
            {
                "mapping_authority": code_map_payload["mapping_authority"],
                "entries": quote_entries,
            },
        ),
    }
    paths["sector_quote_availability_json"].write_text(json.dumps(quote_payload), encoding="utf-8")
    pd.DataFrame({"trade_date": dates, "sw_daily_total_vol": [1000.0, 1100.0, 1200.0]}).to_parquet(
        paths["market_context_parquet"], index=False
    )
    pd.DataFrame(
        {
            "instrument": ["000001.SZ"],
            "start_date": [date(2026, 6, 1)],
            "end_date": [date(2026, 6, 3)],
            "l2_code_id": [sector_id if membership_sector_id is None else membership_sector_id],
        }
    ).to_parquet(paths["sector_membership_spans_parquet"], index=False)
    bundle = {
        "schema_version": "qe_hmm_frozen_input_v1",
        "dataset_root": str(root),
        "dataset_identity": {"generation": "fixture"},
        "market_volume_definition": "sum_market_sw_daily_vol_all_rows_v1",
        "files": {key: {"relative_path": path.name, "sha256": _sha256(path)} for key, path in paths.items()},
    }
    return bundle


def test_load_frozen_coefficient_inputs_uses_hash_pinned_files_only(tmp_path) -> None:
    result = load_frozen_coefficient_inputs(
        _write_frozen_input_fixture(tmp_path),
        history_start=date(2026, 6, 1),
        test_start=date(2026, 6, 1),
        backtest_end=date(2026, 6, 3),
    )

    assert result["dataset_identity"] == {"generation": "fixture"}
    assert result["sector_data"]["801783.SI"][date(2026, 6, 1)]["sw2_pct_change"] == 0.1
    assert result["sector_code_by_id"] == {133: "801783.SI"}
    assert result["active_sector_codes"] == ["801783.SI"]
    assert result["quote_available_sector_codes_by_date"] == {
        "2026-06-01": ["801783.SI"],
        "2026-06-02": ["801783.SI"],
        "2026-06-03": ["801783.SI"],
    }
    assert result["quote_unavailable_sector_codes_by_date"] == {
        "2026-06-01": [],
        "2026-06-02": [],
        "2026-06-03": [],
    }
    assert result["sector_code_map_authority"] == {
        "authority_id": "aistock_sw_l2_shared_catalog_fixture",
        "authority_sha256": "a" * 64,
    }
    assert result["stock_sector_membership_spans"]["000001.SZ"] == [
        {
            "start_date": "2026-06-01",
            "end_date": "2026-06-03",
            "sector_code": "801783.SI",
        }
    ]
    assert date(2026, 6, 2) in result["csi300"]


def _rewrite_code_map(bundle, payload: dict[str, object]) -> None:
    spec = bundle["files"]["sector_code_map_json"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    spec["sha256"] = _sha256(path)


def test_load_frozen_coefficient_inputs_accepts_sparse_shared_ids_without_indexing(
    tmp_path,
) -> None:
    entries = [
        {"l2_code_id": 133, "canonical_l2_code": "801783.SI"},
        {"l2_code_id": 1, "canonical_l2_code": "801011.SI"},
    ]
    result = load_frozen_coefficient_inputs(
        _write_frozen_input_fixture(tmp_path, sector_id=133, code_map_entries=entries),
        history_start=date(2026, 6, 1),
        test_start=date(2026, 6, 1),
        backtest_end=date(2026, 6, 3),
    )

    assert result["sector_code_by_id"] == {
        1: "801011.SI",
        133: "801783.SI",
    }
    assert set(result["sector_data"]) == {"801783.SI"}
    assert result["active_sector_codes"] == ["801783.SI"]
    assert result["ordered_sector_codes"] == ["801011.SI", "801783.SI"]


def test_load_frozen_coefficient_inputs_rejects_membership_sector_without_data(tmp_path) -> None:
    entries = [
        {"l2_code_id": 1, "canonical_l2_code": "801011.SI"},
        {"l2_code_id": 133, "canonical_l2_code": "801783.SI"},
    ]
    with pytest.raises(ValueError, match="membership references sectors without sector_data"):
        load_frozen_coefficient_inputs(
            _write_frozen_input_fixture(
                tmp_path,
                sector_id=133,
                membership_sector_id=1,
                code_map_entries=entries,
            ),
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_duplicate_membership_rows(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_membership_spans_parquet"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    frame = pd.read_parquet(path)
    pd.concat([frame, frame], ignore_index=True).to_parquet(path, index=False)
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="membership spans contain duplicate rows"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_overlapping_membership_spans(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_membership_spans_parquet"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    frame = pd.read_parquet(path)
    overlapping = frame.copy()
    overlapping["start_date"] = date(2026, 6, 2)
    pd.concat([frame, overlapping], ignore_index=True).to_parquet(path, index=False)
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="membership spans overlap"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_unknown_shared_id(tmp_path) -> None:
    with pytest.raises(ValueError, match=r"unmapped l2_code_id values: \[132\]"):
        load_frozen_coefficient_inputs(
            _write_frozen_input_fixture(tmp_path, sector_id=132),
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


@pytest.mark.parametrize(
    ("entries", "error"),
    [
        (
            [
                {"l2_code_id": 1, "canonical_l2_code": "801011.SI"},
                {"l2_code_id": 1, "canonical_l2_code": "801012.SI"},
            ],
            "duplicated l2_code_id",
        ),
        (
            [
                {"l2_code_id": 1, "canonical_l2_code": "801011.SI"},
                {"l2_code_id": 133, "canonical_l2_code": "801011.SI"},
            ],
            "duplicated canonical_l2_code",
        ),
    ],
)
def test_load_frozen_coefficient_inputs_rejects_ambiguous_shared_mapping(tmp_path, entries, error) -> None:
    with pytest.raises(ValueError, match=error):
        load_frozen_coefficient_inputs(
            _write_frozen_input_fixture(tmp_path, code_map_entries=entries),
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_dense_legacy_code_map(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    ordered_codes = ["801783.SI"]
    _rewrite_code_map(
        bundle,
        {
            "schema_version": "qe_sw_l2_code_map_v1",
            "ordered_codes": ordered_codes,
            "code_map_digest": digest_named_fields(
                "dataset_release_sw_l2_code_map_v1",
                {"ordered_codes": ordered_codes},
            ),
        },
    )

    with pytest.raises(ValueError, match="invalid frozen SW L2 code-map schema"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_missing_mapping_authority(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    payload = _shared_code_map_payload()
    payload.pop("mapping_authority")
    _rewrite_code_map(bundle, payload)

    with pytest.raises(ValueError, match="requires mapping_authority"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_non_integral_shared_id(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    sector_spec = bundle["files"]["sector_data_h5"]
    sector_path = Path(bundle["dataset_root"]) / sector_spec["relative_path"]
    sector = pd.read_hdf(sector_path, key="data")
    sector["l2_code_id"] = 133.5
    sector.to_hdf(sector_path, key="data", format="table", data_columns=True, mode="w")
    sector_spec["sha256"] = _sha256(sector_path)

    with pytest.raises(ValueError, match="non-integral l2_code_id"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_missing_membership_day(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_membership_spans_parquet"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    frame = pd.read_parquet(path)
    frame["start_date"] = date(2026, 6, 2)
    frame.to_parquet(path, index=False)
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="empty point-in-time stock-sector map"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_hash_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    bundle["files"]["sector_data_h5"]["sha256"] = "0" * 64

    with pytest.raises(ValueError, match="sha256 mismatch"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_code_map_digest_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_code_map_json"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["code_map_digest"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="code-map digest mismatch"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_quote_availability_digest_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_quote_availability_json"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["quote_availability_digest"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="quote-availability digest mismatch"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_quote_mapping_authority_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_quote_availability_json"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["mapping_authority"]["authority_id"] = "different-authority"
    payload["quote_availability_digest"] = digest_named_fields(
        FROZEN_QUOTE_AVAILABILITY_SCHEMA,
        {
            "mapping_authority": payload["mapping_authority"],
            "entries": payload["entries"],
        },
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="mapping authority differs"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_distinguishes_stopped_quote_from_missing_published_row(
    tmp_path,
) -> None:
    bundle = _write_frozen_input_fixture(
        tmp_path,
        quote_spans_by_code={
            "801783.SI": [{"start_date": "2020-01-01", "end_date": "2026-06-01"}],
        },
    )
    sector_spec = bundle["files"]["sector_data_h5"]
    sector_path = Path(bundle["dataset_root"]) / sector_spec["relative_path"]
    sector = pd.read_hdf(sector_path, key="data")
    stopped_mask = sector.index.get_level_values("datetime") > pd.Timestamp("2026-06-01")
    metric_columns = [column for column in sector.columns if column != "l2_code_id"]
    sector.loc[stopped_mask, metric_columns] = float("nan")
    sector.to_hdf(sector_path, key="data", format="table", data_columns=True, mode="w")
    sector_spec["sha256"] = _sha256(sector_path)

    result = load_frozen_coefficient_inputs(
        bundle,
        history_start=date(2026, 6, 1),
        test_start=date(2026, 6, 1),
        backtest_end=date(2026, 6, 3),
    )

    assert result["quote_available_sector_codes_by_date"] == {
        "2026-06-01": ["801783.SI"],
        "2026-06-02": [],
        "2026-06-03": [],
    }
    assert result["quote_unavailable_sector_codes_by_date"] == {
        "2026-06-01": [],
        "2026-06-02": ["801783.SI"],
        "2026-06-03": ["801783.SI"],
    }
    assert set(result["sector_data"]["801783.SI"]) == {date(2026, 6, 1)}


def test_load_frozen_coefficient_inputs_rejects_quote_values_after_authority_end(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(
        tmp_path,
        quote_spans_by_code={
            "801783.SI": [{"start_date": "2020-01-01", "end_date": "2026-06-01"}],
        },
    )

    with pytest.raises(ValueError, match="quote values outside availability authority"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_non_finite_sector_value(tmp_path) -> None:
    with pytest.raises(ValueError, match="non-finite frozen sector value"):
        load_frozen_coefficient_inputs(
            _write_frozen_input_fixture(tmp_path, invalid_sector_value=True),
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_main_frozen_mode_never_imports_database_driver(monkeypatch, tmp_path, capsys) -> None:
    import builtins
    import numpy as np
    import scripts.precompute_hmm_coefficients as module

    days = pd.bdate_range("2026-05-01", periods=30).date.tolist()
    coefficient_days = [day for day in days if day >= date(2026, 6, 1)]
    model_path = tmp_path / "models.json"
    model_path.write_text(
        json.dumps(
            {
                "A.SI": {
                    "n_states": 2,
                    "means": [[0.0] * 4, [1.0] * 4],
                    "state_labels": {"0": "neutral", "1": "trending"},
                },
                "B.SI": {
                    "n_states": 2,
                    "means": [[0.0] * 4, [1.0] * 4],
                    "state_labels": {"0": "neutral", "1": "trending"},
                },
            }
        ),
        encoding="utf-8",
    )
    params = {
        "model_path": str(model_path),
        "model_sha256": _sha256(model_path),
        "test_start": "2026-06-01",
        "backtest_end": coefficient_days[-1].isoformat(),
        "frozen_input_bundle": {"schema_version": "qe_hmm_frozen_input_v1"},
    }
    maps_by_date = {day.isoformat(): {"000001.SZ": "A.SI"} for day in coefficient_days}
    monkeypatch.setattr(module, "parse_stdin", lambda: params)
    monkeypatch.setattr(
        module,
        "load_frozen_coefficient_inputs",
        lambda *_args, **_kwargs: {
            "dataset_root": str(tmp_path),
            "dataset_identity": {"generation": "fixture"},
            "file_sha256": {key: "1" * 64 for key in module.FROZEN_FILE_KEYS},
            "sector_code_map_digest": "2" * 64,
            "ordered_sector_codes": ["A.SI", "B.SI"],
            "active_sector_codes": ["A.SI"],
            "coefficient_sector_codes": ["A.SI"],
            "quote_available_sector_codes_by_date": {day.isoformat(): ["A.SI"] for day in coefficient_days},
            "quote_unavailable_sector_codes_by_date": {day.isoformat(): [] for day in coefficient_days},
            "quote_availability_authority": {
                "authority_id": "aistock_sw_l2_shared_catalog_fixture",
                "authority_sha256": "a" * 64,
            },
            "quote_availability_digest": "3" * 64,
            "sector_code_map_authority": {
                "authority_id": "aistock_sw_l2_shared_catalog_fixture",
                "authority_sha256": "a" * 64,
            },
            "sector_data": {"A.SI": {day: {} for day in days}},
            "sector_rows": {"A.SI": []},
            "sector_dates": days,
            "csi300": {day: 0.0 for day in days},
            "index_dates": days,
            "market_vol": {day: 1.0 for day in days},
            "market_volume_dates": days,
            "stock_sector_maps_by_date": maps_by_date,
            "stock_sector_membership_spans": build_stock_sector_membership_spans(maps_by_date),
        },
    )
    monkeypatch.setattr(
        module,
        "restore_hmm",
        lambda _info: SimpleNamespace(means_=np.zeros((2, 4))),
    )
    monkeypatch.setattr(
        module,
        "build_legacy_observations",
        lambda *_args, **_kwargs: (np.ones((len(days), 4)), days),
    )
    monkeypatch.setattr(
        module,
        "forward_filter_posteriors",
        lambda _hmm, obs: np.tile(np.asarray([[1.0, 0.0]]), (len(obs), 1)),
    )
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "psycopg2" or name.startswith("psycopg2."):
            raise AssertionError("frozen QE mode must not import psycopg2")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(sys, "argv", ["precompute_hmm_coefficients.py"])

    main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["data_source"] == "frozen_qe_release"
    assert payload["catalog_sector_count"] == 2
    assert payload["active_sector_count"] == 1
    assert payload["inactive_catalog_sector_codes"] == ["B.SI"]
    assert payload["coefficient_sector_scope"] == "membership_and_quote_available_by_date"
    assert payload["membership_active_sector_count"] == 1
    assert payload["quote_unavailable_sector_count_at_end"] == 0
    assert "stock_sector_map_by_date" not in payload
    assert payload["stock_sector_membership_spans"] == {
        "000001.SZ": [
            {
                "start_date": coefficient_days[0].isoformat(),
                "end_date": coefficient_days[-1].isoformat(),
                "sector_code": "A.SI",
            }
        ]
    }


def test_main_rejects_missing_sector_state_date(monkeypatch, tmp_path, capsys) -> None:
    import numpy as np
    import scripts.precompute_hmm_coefficients as module

    days = pd.bdate_range("2026-05-01", periods=30).date.tolist()
    coefficient_days = [day for day in days if day >= date(2026, 6, 1)]
    missing_day = coefficient_days[0]
    model_path = tmp_path / "models.json"
    model_path.write_text(
        json.dumps(
            {
                "A.SI": {
                    "n_states": 2,
                    "means": [[0.0] * 4, [1.0] * 4],
                    "state_labels": {"0": "neutral", "1": "trending"},
                }
            }
        ),
        encoding="utf-8",
    )
    maps_by_date = {day.isoformat(): {"000001.SZ": "A.SI"} for day in coefficient_days}
    monkeypatch.setattr(
        module,
        "parse_stdin",
        lambda: {
            "model_path": str(model_path),
            "model_sha256": _sha256(model_path),
            "test_start": "2026-06-01",
            "backtest_end": coefficient_days[-1].isoformat(),
            "frozen_input_bundle": {"schema_version": "qe_hmm_frozen_input_v1"},
        },
    )
    monkeypatch.setattr(
        module,
        "load_frozen_coefficient_inputs",
        lambda *_args, **_kwargs: {
            "dataset_root": str(tmp_path),
            "dataset_identity": {"generation": "fixture"},
            "file_sha256": {key: "1" * 64 for key in module.FROZEN_FILE_KEYS},
            "sector_code_map_digest": "2" * 64,
            "ordered_sector_codes": ["A.SI"],
            "active_sector_codes": ["A.SI"],
            "coefficient_sector_codes": ["A.SI"],
            "quote_available_sector_codes_by_date": {day.isoformat(): ["A.SI"] for day in coefficient_days},
            "quote_unavailable_sector_codes_by_date": {day.isoformat(): [] for day in coefficient_days},
            "quote_availability_authority": {
                "authority_id": "aistock_sw_l2_shared_catalog_fixture",
                "authority_sha256": "a" * 64,
            },
            "quote_availability_digest": "3" * 64,
            "sector_data": {"A.SI": {day: {} for day in days}},
            "sector_rows": {"A.SI": []},
            "sector_dates": days,
            "csi300": {day: 0.0 for day in days},
            "index_dates": days,
            "market_vol": {day: 1.0 for day in days},
            "market_volume_dates": days,
            "stock_sector_maps_by_date": maps_by_date,
            "stock_sector_membership_spans": build_stock_sector_membership_spans(maps_by_date),
        },
    )
    monkeypatch.setattr(
        module,
        "restore_hmm",
        lambda _info: SimpleNamespace(means_=np.zeros((2, 4))),
    )
    observed_days = [day for day in days if day != missing_day]
    monkeypatch.setattr(
        module,
        "build_legacy_observations",
        lambda *_args, **_kwargs: (np.ones((len(observed_days), 4)), observed_days),
    )
    monkeypatch.setattr(
        module,
        "forward_filter_posteriors",
        lambda _hmm, obs: np.tile(np.asarray([[1.0, 0.0]]), (len(obs), 1)),
    )
    monkeypatch.setattr(sys, "argv", ["precompute_hmm_coefficients.py"])

    with pytest.raises(SystemExit):
        main()

    assert "decoded state dates differ from quote availability" in capsys.readouterr().err


def test_precompute_source_has_no_database_data_plane() -> None:
    import scripts.precompute_hmm_coefficients as module

    source = Path(module.__file__).read_text(encoding="utf-8")

    for forbidden in ("psycopg2", "market.", "TDX_DB", "DATABASE_URL"):
        assert forbidden not in source
