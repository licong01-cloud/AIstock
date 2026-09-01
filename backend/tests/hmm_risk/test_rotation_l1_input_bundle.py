from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import h5py
import numpy as np
import pandas as pd
import pytest

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    canonical_rule_parameters_digest,
)
from backend.services.hmm_risk import rotation_l1_input_bundle as subject
from backend.services.dataset_release.copy_on_write import tree_merkle
from backend.services.dataset_release.stock_schema import qlib_stock_schema_digest
from backend.services.hmm_risk.state_model_set import canonical_sha256
from scripts.hmm_risk import build_rotation_l1_input_bundle as cli


def _receipt(label: str) -> dict[str, object]:
    body: dict[str, object] = {"schema_version": f"{label}_v1", "status": "complete", "count": 1}
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _panel(count: int, level: str, *, offset: float = 0.0) -> pd.DataFrame:
    dates = (subject.SOURCE_START, subject.SOURCE_END)
    codes = [f"{index:06d}.{level}" for index in range(count)]
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(dates):
        for code_index, code in enumerate(codes):
            row: dict[str, object] = {"trade_date": day, "sector_code": code}
            for feature_index, feature in enumerate(subject.FEATURE_NAMES):
                row[feature] = offset + day_index + code_index / 1000 + feature_index / 100
            rows.append(row)
    rows[0][subject.FEATURE_NAMES[-1]] = np.nan
    return pd.DataFrame(rows).set_index(["trade_date", "sector_code"])


def _mapping_manifest() -> dict[str, object]:
    return {
        "schema_version": subject.HMM_MAPPING_MANIFEST_SCHEMA,
        "universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}frozen-release",
        "source_window_start": subject.SOURCE_START.isoformat(),
        "source_window_end": subject.SOURCE_END.isoformat(),
        "canonical_l1_count": 31,
        "canonical_l2_count": 131,
        "source_classification_authority_receipt_hash": "1" * 64,
        "classification_authority_receipt_hash": "2" * 64,
        "index_membership_authority_receipt_hash": "3" * 64,
        "classification_candidate_hash": "4" * 64,
        "stable_backcast_candidate_sha256": "5" * 64,
        "index_membership_candidate_hash": "6" * 64,
        "candidate_bundle_hash": "7" * 64,
        "candidate_preflight_canonical_hash": "8" * 64,
        "research_basis_contract_sha256": "9" * 64,
        "active_classification_basis": "stable_taxonomy_backcast",
        "non_as_known_taxonomy": True,
        "l1_code_projection_sha256": "a" * 64,
        "constituent_manifest_hash": "b" * 64,
    }


def _inputs(*, offset: float = 0.0) -> dict[str, object]:
    dates = (subject.SOURCE_START, subject.SOURCE_END)
    return {
        "panel": _panel(31, "L1", offset=offset),
        "l2_panel": _panel(131, "L2", offset=offset),
        "trading_dates": dates,
        "dataset_manifest": {
            "calendar_benchmark": {"rows": [[day.isoformat(), 0.001 + index / 1000] for index, day in enumerate(dates)]}
        },
        "mapping_manifest": _mapping_manifest(),
        "security_identity_manifest": {"schema_version": "security_v1", "manifest_sha256": "a" * 64},
        "provider_absence_manifest": {"schema_version": "absence_v1", "manifest_sha256": "b" * 64},
        "feature_definition": {"schema_version": "feature_v1", "features": list(subject.FEATURE_NAMES)},
        "l2_feature_definition": {"schema_version": "feature_v1", "features": list(subject.FEATURE_NAMES)},
        "c010_diagnostic": {
            "eligibility": _receipt("eligibility"),
            "aggregate_evidence": _receipt("aggregate"),
            "l1_cross_section_evidence": _receipt("cross_section"),
        },
        "source_build_resource_receipts": [
            {"stage": stage, "elapsed_seconds": float(index + 1), "rss_bytes": 1024 * (index + 1)}
            for index, stage in enumerate(subject.SOURCE_BUILD_STAGES)
        ],
        "input_bundle_evidence": {
            "unavailable_reason": [
                {
                    "trade_date": subject.SOURCE_START.isoformat(),
                    "level": "L1",
                    "sector_code": "000000.L1",
                    "field": subject.FEATURE_NAMES[-1],
                    "reason_code": "hmm_risk_rotation_l1_feature_warmup",
                    "source_observation_date": subject.SOURCE_START.isoformat(),
                }
            ],
            "security_identity_intervals": [
                {
                    "canonical_security_id": "000001.SZ",
                    "valid_from": subject.SOURCE_START.isoformat(),
                    "valid_to": subject.SOURCE_END.isoformat(),
                    "source_code": "000001.SZ",
                }
            ],
            "industry_projection_intervals": [
                {
                    "canonical_security_id": "000001.SZ",
                    "effective_from": subject.SOURCE_START.isoformat(),
                    "effective_to": subject.SOURCE_END.isoformat(),
                    "l1_code": "000000.L1",
                    "l2_code": "000000.L2",
                }
            ],
            "source_status_intervals": [
                {
                    "canonical_security_id": "000001.SZ",
                    "valid_from": subject.SOURCE_START.isoformat(),
                    "valid_to": subject.SOURCE_END.isoformat(),
                    "status": "available",
                    "reason_code": "",
                    "provider": "frozen_release",
                }
            ],
        },
    }


def _release_identity(release_id: str = "frozen-release") -> dict[str, object]:
    return {
        "schema_version": "qe_formal_canonical_pit_dataset_binding_v1",
        "usage_mode": "formal_training",
        "authority_id": CANONICAL_PIT_AUTHORITY_ID,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "rule_parameters_digest": canonical_rule_parameters_digest(),
        "release_id": release_id,
        "cutoff": subject.SOURCE_END.isoformat(),
        "frozen_snapshot_digest": "1" * 64,
        "manifest_digest": "2" * 64,
    }


def _source() -> dict[str, object]:
    return {
        "source_start": subject.SOURCE_START.isoformat(),
        "source_end": subject.SOURCE_END.isoformat(),
        "source_revision": subject.SOURCE_REVISION,
        "circ_mv_history_start": subject.SOURCE_START.isoformat(),
        "universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}frozen-release",
        "universe_rule_version": CANONICAL_PIT_RULE_VERSION,
    }


def _source_identity() -> dict[str, object]:
    return {
        "release_identity": _release_identity(),
        "source_inventory_sha256": "b" * 64,
        "source_binding_manifest_sha256": "c" * 64,
        "qlib_schema_version": subject.QLIB_STOCK_SCHEMA_VERSION,
        "qlib_schema_sha256": qlib_stock_schema_digest(),
        "c013_bundle_sha256": "e" * 64,
    }


def _write(tmp_path: Path, *, name: str = "bundle", offset: float = 0.0) -> tuple[Path, dict[str, object]]:
    root = tmp_path / name
    receipt = subject.write_rotation_l1_input_bundle(
        inputs=_inputs(offset=offset),
        source=_source(),
        source_identity=_source_identity(),
        output_root=root,
        producer_commit="f" * 40,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
    )
    return root, receipt


def _rewrite_manifest_and_receipt(root: Path, manifest: dict[str, object]) -> None:
    body = {
        key: value for key, value in manifest.items() if key not in {"manifest_body_sha256", "bundle_canonical_sha256"}
    }
    body_hash = canonical_sha256(body)
    bundle_hash = canonical_sha256(
        {
            "schema_version": subject.MANIFEST_SCHEMA_VERSION,
            "manifest_body_sha256": body_hash,
            "h5_sha256": manifest["h5_file"]["sha256"],
        }
    )
    manifest["manifest_body_sha256"] = body_hash
    manifest["bundle_canonical_sha256"] = bundle_hash
    (root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt = json.loads((root / "build.receipt.json").read_text(encoding="utf-8"))
    receipt["manifest_body_sha256"] = body_hash
    receipt["bundle_canonical_sha256"] = bundle_hash
    receipt["receipt_sha256"] = canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    )
    (root / "build.receipt.json").write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_bundle_round_trip_is_complete_and_restores_only_model_inputs(tmp_path: Path) -> None:
    root, receipt = _write(tmp_path)
    expected = _inputs()

    loaded = subject.read_rotation_l1_input_bundle(
        root,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
    )

    assert receipt["status"] == "success"
    assert {item.name for item in root.iterdir()} == {
        "rotation_l1_input.h5",
        "manifest.json",
        "build.receipt.json",
    }
    assert loaded["panel"].shape == (62, 9)
    assert loaded["l2_panel"].shape == (262, 9)
    pd.testing.assert_frame_equal(loaded["panel"], expected["panel"].sort_index())
    pd.testing.assert_frame_equal(loaded["l2_panel"], expected["l2_panel"].sort_index())
    assert loaded["trading_dates"] == (subject.SOURCE_START, subject.SOURCE_END)
    assert np.isnan(loaded["panel"].iloc[0][subject.FEATURE_NAMES[-1]])
    assert loaded["input_bundle_evidence"]["unavailable_reason"][0]["reason_code"] == (
        "hmm_risk_rotation_l1_feature_warmup"
    )
    assert "database" not in loaded
    assert set(loaded["input_bundle_identity"]) == {
        "bundle_canonical_sha256",
        "manifest_body_sha256",
        "h5_sha256",
    }


def test_exact_existing_bundle_is_read_only_and_different_content_collides(tmp_path: Path) -> None:
    root, first = _write(tmp_path)
    before = {
        item.name: (item.stat().st_mtime_ns, hashlib.sha256(item.read_bytes()).hexdigest()) for item in root.iterdir()
    }

    repeat_inputs = _inputs()
    for receipt in repeat_inputs["source_build_resource_receipts"]:
        receipt["elapsed_seconds"] += 10.0
        receipt["rss_bytes"] += 4096
    existing = subject.write_rotation_l1_input_bundle(
        inputs=repeat_inputs,
        source=_source(),
        source_identity=_source_identity(),
        output_root=root,
        producer_commit="f" * 40,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
    )

    assert existing["status"] == "EXISTING_BUNDLE"
    assert existing["bundle_canonical_sha256"] == first["bundle_canonical_sha256"]
    assert before == {
        item.name: (item.stat().st_mtime_ns, hashlib.sha256(item.read_bytes()).hexdigest()) for item in root.iterdir()
    }
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.write_rotation_l1_input_bundle(
            inputs=_inputs(offset=1.0),
            source=_source(),
            source_identity=_source_identity(),
            output_root=root,
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert exc_info.value.reason_code == subject.REASON_COLLISION


def test_holdout_rows_fail_before_final_bundle_write(tmp_path: Path) -> None:
    inputs = _inputs()
    panel = inputs["panel"].reset_index()
    panel.loc[0, "trade_date"] = date(2026, 4, 1)
    inputs["panel"] = panel.set_index(["trade_date", "sector_code"])
    root = tmp_path / "bundle"

    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.write_rotation_l1_input_bundle(
            inputs=inputs,
            source=_source(),
            source_identity=_source_identity(),
            output_root=root,
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )

    assert exc_info.value.reason_code == subject.REASON_HOLDOUT_CONTAMINATION
    assert not root.exists()
    failures = list(tmp_path.glob(".bundle.partial.*/build.failure.json"))
    assert len(failures) == 1


def test_h5_mutation_is_rejected_before_any_panel_is_returned(tmp_path: Path) -> None:
    root, _ = _write(tmp_path)
    with h5py.File(root / "rotation_l1_input.h5", "r+") as handle:
        handle["l1_panel"][0] = handle["l1_panel"][1]

    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.read_rotation_l1_input_bundle(root, forbidden_roots=(Path(__file__).resolve().parents[3],))

    assert exc_info.value.reason_code == subject.REASON_HASH_MISMATCH


def test_self_hashed_metadata_drift_is_rejected_by_independent_readback(tmp_path: Path) -> None:
    root, _ = _write(tmp_path)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["metadata"]["feature_definition"]["schema_version"] = "self_consistent_but_wrong"
    _rewrite_manifest_and_receipt(root, manifest)

    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.read_rotation_l1_input_bundle(root, forbidden_roots=(Path(__file__).resolve().parents[3],))

    assert exc_info.value.reason_code == subject.REASON_HASH_MISMATCH


def test_missing_panel_key_and_resource_receipt_fail_closed(tmp_path: Path) -> None:
    missing = _inputs()
    missing["panel"] = missing["panel"].iloc[1:]
    with pytest.raises(subject.RotationL1InputBundleError) as missing_exc:
        subject.write_rotation_l1_input_bundle(
            inputs=missing,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "missing-key",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert missing_exc.value.reason_code == subject.REASON_SOURCE_RANGE_INCOMPLETE

    no_resource_receipt = _inputs()
    no_resource_receipt["source_build_resource_receipts"] = []
    with pytest.raises(subject.RotationL1InputBundleError) as resource_exc:
        subject.write_rotation_l1_input_bundle(
            inputs=no_resource_receipt,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "missing-resource-receipt",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert resource_exc.value.reason_code == subject.REASON_INCOMPLETE


def test_repository_internal_output_is_rejected(tmp_path: Path) -> None:
    repository_root = Path(__file__).resolve().parents[3]
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.write_rotation_l1_input_bundle(
            inputs=_inputs(),
            source=_source(),
            source_identity=_source_identity(),
            output_root=repository_root / "tmp" / "forbidden-bundle",
            producer_commit="f" * 40,
            forbidden_roots=(repository_root,),
        )
    assert exc_info.value.reason_code == subject.REASON_MANIFEST_INVALID


def test_invalid_c010_receipt_and_duplicate_panel_fail_closed(tmp_path: Path) -> None:
    invalid = _inputs()
    invalid["c010_diagnostic"]["eligibility"]["count"] = 2
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.write_rotation_l1_input_bundle(
            inputs=invalid,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "invalid",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert exc_info.value.reason_code == subject.REASON_HASH_MISMATCH

    duplicate = _inputs()
    panel = duplicate["panel"].reset_index()
    duplicate["panel"] = pd.concat([panel, panel.iloc[[0]]], ignore_index=True).set_index(["trade_date", "sector_code"])
    with pytest.raises(subject.RotationL1InputBundleError) as duplicate_exc:
        subject.write_rotation_l1_input_bundle(
            inputs=duplicate,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "duplicate",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert duplicate_exc.value.reason_code == subject.REASON_DUPLICATE_KEY

    unknown = _inputs()
    unknown["input_bundle_evidence"]["unavailable_reason"][0]["reason_code"] = "unknown_reason"
    with pytest.raises(subject.RotationL1InputBundleError) as unknown_exc:
        subject.write_rotation_l1_input_bundle(
            inputs=unknown,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "unknown-reason",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert unknown_exc.value.reason_code == subject.REASON_SOURCE_SCHEMA_INVALID

    gap = _inputs()
    gap["input_bundle_evidence"]["source_status_intervals"][0]["valid_to"] = subject.SOURCE_START.isoformat()
    with pytest.raises(subject.RotationL1InputBundleError) as gap_exc:
        subject.write_rotation_l1_input_bundle(
            inputs=gap,
            source=_source(),
            source_identity=_source_identity(),
            output_root=tmp_path / "authority-gap",
            producer_commit="f" * 40,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
    assert gap_exc.value.reason_code == subject.REASON_AUTHORITY_AMBIGUOUS


def test_fixed_h5_reader_is_bounded_and_rejects_column_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2020-07-30"), "000001.SZ"),
            (pd.Timestamp("2020-07-31"), "000001.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(
        np.arange(2 * len(subject._DAILY_BASIC_COLUMNS), dtype=np.float32).reshape(2, -1),
        index=index,
        columns=subject._DAILY_BASIC_COLUMNS,
    )
    path = tmp_path / "daily_basic.h5"
    frame.to_hdf(path, key="data", format="fixed")

    chunks = list(
        subject._iter_fixed_h5_frames(
            path,
            expected_columns=subject._DAILY_BASIC_COLUMNS,
            expected_dtype="<f4",
            max_rows=1,
        )
    )

    assert [len(item) for item in chunks] == [1, 1]
    assert chunks[0].dtypes.tolist() == [np.dtype("float32")] * len(subject._DAILY_BASIC_COLUMNS)
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        list(
            subject._iter_fixed_h5_frames(
                path,
                expected_columns=tuple(reversed(subject._DAILY_BASIC_COLUMNS)),
                expected_dtype="<f4",
            )
        )
    assert exc_info.value.reason_code == subject.REASON_SOURCE_SCHEMA_INVALID
    monkeypatch.setattr(
        subject,
        "_iter_fixed_h5_frames",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("window reader rescanned the full H5")),
    )
    window = subject._load_fixed_h5_window(
        path,
        expected_columns=subject._DAILY_BASIC_COLUMNS,
        expected_dtype="<f4",
        start=date(2020, 7, 31),
        end=date(2020, 7, 31),
        max_rows=1,
    )
    assert window.index.tolist() == [(pd.Timestamp("2020-07-31"), "000001.SZ")]


def test_fixed_h5_reader_preserves_index_float64_and_rejects_component_dtype_drift(tmp_path: Path) -> None:
    columns = (
        "idx_open_point",
        "idx_high_point",
        "idx_low_point",
        "idx_close_point",
        "idx_pre_close_point",
        "idx_return_1d",
        "idx_volume_hand_source",
        "idx_volume_share_equiv",
        "idx_amount_cny",
    )
    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2020-07-30"), "000300.SH")],
        names=["datetime", "instrument"],
    )
    path = tmp_path / "index_daily.h5"
    pd.DataFrame(np.ones((1, len(columns)), dtype=np.float64), index=index, columns=columns).to_hdf(
        path, key="data", format="fixed"
    )

    inventory = subject._fixed_h5_inventory(path, expected_columns=columns, expected_dtype="<f8")
    frame = subject._load_fixed_h5_window(
        path,
        expected_columns=columns,
        expected_dtype="<f8",
        start=date(2020, 7, 30),
        end=date(2020, 7, 30),
    )

    assert inventory["dtype"] == "float64"
    assert frame.dtypes.tolist() == [np.dtype("float64")] * len(columns)
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject._fixed_h5_inventory(path, expected_columns=columns, expected_dtype="<f4")
    assert exc_info.value.reason_code == subject.REASON_SOURCE_SCHEMA_INVALID


def test_qlib_qfq_values_are_reconstructed_to_raw_units_and_invalid_factor_fails() -> None:
    row = np.zeros(1, dtype=subject._QLIB_SOURCE_DTYPE)[0]
    row["factor"] = 0.5
    for field, value in {
        "open": 5.0,
        "high": 6.0,
        "low": 4.0,
        "close": 5.5,
        "volume": 200.0,
        "amount": 1000.0,
        "prev_close": 10.0,
        "up_limit_price": 11.0,
        "down_limit_price": 9.0,
        "limit_up": 0.0,
        "limit_down": 0.0,
    }.items():
        row[field] = value

    values = subject._raw_qlib_values(row)

    assert values["open"] == 10.0
    assert values["close"] == 11.0
    assert values["volume"] == 100.0
    assert values["prev_close"] == 10.0
    row["factor"] = 0.0
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject._raw_qlib_values(row)
    assert exc_info.value.reason_code == subject.REASON_SOURCE_UNIT_INVALID


def test_qlib_stock_reader_requires_all_twelve_aligned_float32_fields(tmp_path: Path) -> None:
    qlib = tmp_path / "qlib"
    feature_root = qlib / "features" / "000001.sz"
    feature_root.mkdir(parents=True)
    calendar = (subject.SOURCE_START, subject.SOURCE_END)
    for index, field in enumerate(subject.QLIB_STOCK_FIELDS, start=1):
        np.asarray([0.0, float(index), float(index + 1)], dtype="<f4").tofile(feature_root / f"{field}.day.bin")

    rows = subject._read_qlib_stock_rows(
        qlib,
        symbol="000001.SZ",
        calendar=calendar,
        active_spans=((subject.SOURCE_START, subject.SOURCE_END),),
    )

    assert rows["trade_date"].tolist() == [20200730, 20260331]
    assert rows["open"].tolist() == [1.0, 2.0]
    (feature_root / "close.day.bin").unlink()
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject._read_qlib_stock_rows(
            qlib,
            symbol="000001.SZ",
            calendar=calendar,
            active_spans=((subject.SOURCE_START, subject.SOURCE_END),),
        )
    assert exc_info.value.reason_code == subject.REASON_SOURCE_COMPONENT_MISSING


def test_qlib_stock_reader_vectorized_materialization_is_byte_exact_for_sparse_active_spans(tmp_path: Path) -> None:
    qlib = tmp_path / "qlib"
    feature_root = qlib / "features" / "000001.sz"
    feature_root.mkdir(parents=True)
    calendar = tuple(subject.SOURCE_START + timedelta(days=index) for index in range(6))
    for field_index, field in enumerate(subject.QLIB_STOCK_FIELDS, start=1):
        values = np.asarray(
            [0.0, *(field_index * 100.0 + index for index in range(len(calendar)))],
            dtype="<f4",
        )
        values.tofile(feature_root / f"{field}.day.bin")

    rows = subject._read_qlib_stock_rows(
        qlib,
        symbol="000001.SZ",
        calendar=calendar,
        active_spans=((calendar[0], calendar[1]), (calendar[4], calendar[5])),
    )

    expected = np.zeros(4, dtype=subject._QLIB_SOURCE_DTYPE)
    for output_index, calendar_index in enumerate((0, 1, 4, 5)):
        expected[output_index]["trade_date"] = int(calendar[calendar_index].strftime("%Y%m%d"))
        expected[output_index]["symbol"] = b"000001.SZ"
        for field_index, field in enumerate(subject.QLIB_STOCK_FIELDS, start=1):
            expected[output_index][field] = field_index * 100.0 + calendar_index

    assert rows.dtype == subject._QLIB_SOURCE_DTYPE
    assert rows.flags.c_contiguous
    assert rows.tobytes() == expected.tobytes()


def test_qlib_month_spool_vectorized_slices_preserve_symbol_and_date_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dates = (
        date(2022, 1, 30),
        date(2022, 1, 31),
        date(2022, 2, 1),
        date(2022, 2, 2),
    )

    def rows_for_symbol(_root, *, symbol, **_kwargs):
        rows = np.zeros(len(dates), dtype=subject._QLIB_SOURCE_DTYPE)
        rows["trade_date"] = [20220130, 20220131, 20220201, 20220202]
        rows["symbol"] = symbol.encode("ascii")
        rows["close"] = np.arange(len(dates), dtype=np.float32)
        return rows

    monkeypatch.setattr(subject, "_read_qlib_stock_rows", rows_for_symbol)
    paths = subject._spool_qlib_months(
        tmp_path / "qlib",
        calendar=dates,
        spans={"000002.SZ": ((dates[0], dates[-1]),), "000001.SZ": ((dates[0], dates[-1]),)},
        spool_root=tmp_path / "spool",
    )

    assert [path.name for path in paths] == ["202201.bin", "202202.bin"]
    january = np.fromfile(paths[0], dtype=subject._QLIB_SOURCE_DTYPE)
    february = np.fromfile(paths[1], dtype=subject._QLIB_SOURCE_DTYPE)
    assert january[["trade_date", "symbol"]].tolist() == [
        (20220130, b"000001.SZ"),
        (20220131, b"000001.SZ"),
        (20220130, b"000002.SZ"),
        (20220131, b"000002.SZ"),
    ]
    assert february[["trade_date", "symbol"]].tolist() == [
        (20220201, b"000001.SZ"),
        (20220202, b"000001.SZ"),
        (20220201, b"000002.SZ"),
        (20220202, b"000002.SZ"),
    ]


def test_industry_unavailable_day_preserves_independent_price_and_circ_mv_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index) for index in range(6))
    symbol = "000001.SZ"
    source_rows = np.zeros(len(dates), dtype=subject._QLIB_SOURCE_DTYPE)
    for index, day in enumerate(dates):
        source_rows[index]["trade_date"] = int(day.strftime("%Y%m%d"))
        source_rows[index]["symbol"] = symbol.encode("ascii")
        for field in subject.QLIB_STOCK_FIELDS:
            source_rows[index][field] = 0.0
        source_rows[index]["factor"] = 1.0
        source_rows[index]["open"] = 10.0 + index
        source_rows[index]["high"] = 11.0 + index
        source_rows[index]["low"] = 9.0 + index
        source_rows[index]["close"] = 10.0 + index
        source_rows[index]["prev_close"] = 9.0 + index
        source_rows[index]["up_limit_price"] = 20.0 + index
        source_rows[index]["down_limit_price"] = 5.0 + index
    month_path = tmp_path / "202201.bin"
    source_rows.tofile(month_path)

    index = pd.MultiIndex.from_arrays([pd.to_datetime(dates), [symbol] * len(dates)], names=["datetime", "instrument"])
    basic = pd.DataFrame(1.0, index=index, columns=subject._DAILY_BASIC_COLUMNS, dtype=np.float32)
    basic["db_total_mv"] = np.arange(200.0, 206.0, dtype=np.float32)
    basic["db_circ_mv"] = np.arange(100.0, 106.0, dtype=np.float32)
    moneyflow = pd.DataFrame(1.0, index=index, columns=subject._MONEYFLOW_COLUMNS, dtype=np.float32)

    def load_window(_path, *, expected_columns, **_kwargs):
        return basic if tuple(expected_columns) == subject._DAILY_BASIC_COLUMNS else moneyflow

    monkeypatch.setattr(subject, "_load_fixed_h5_window", load_window)

    class Adapter:
        @staticmethod
        def resolve(_symbol, day):
            if day == dates[-2]:
                return SimpleNamespace(status="unavailable", reason_code="classification:missing")
            return SimpleNamespace(
                status="resolved",
                reason_code=None,
                l1_code="801010.SI",
                l1_name="L1",
                l2_code="801011.SI",
                l2_name="L2",
            )

    class Resolution:
        source_ts_code = symbol

        @staticmethod
        def evidence():
            return {"source_ts_code": symbol}

    class Security:
        @staticmethod
        def resolve(_symbol, _day, _dataset):
            return Resolution()

    captured: list[dict[str, object]] = []

    def capture(rows, *, level, **_kwargs):
        if level == "L1":
            captured.extend(dict(row) for row in rows)

    monkeypatch.setattr(subject, "_append_feature_domain_aggregate", capture)
    subject._build_stock_fact_aggregates(
        month_paths=(month_path,),
        assets={"files": {"daily_basic": tmp_path / "basic.h5", "moneyflow": tmp_path / "moneyflow.h5"}},
        calendar=dates,
        spans={symbol: ((dates[0], dates[-1]),)},
        adapter=Adapter(),
        security=Security(),
        provider_absence=SimpleNamespace(),
        suspension_keys=frozenset(),
        contributor_eligibility={symbol: True},
    )

    final = next(row for row in captured if row["trade_date"] == dates[-1])
    assert final["prev_close_5_yuan"] == 10.0
    assert final["prev_circ_mv_cny"] == 104.0 * 10_000.0


def test_csi300_benchmark_return_is_recomputed_from_frozen_close_and_preclose(tmp_path: Path) -> None:
    columns = (
        "idx_open_point",
        "idx_high_point",
        "idx_low_point",
        "idx_close_point",
        "idx_pre_close_point",
        "idx_return_1d",
        "idx_volume_hand_source",
        "idx_volume_share_equiv",
        "idx_amount_cny",
    )
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp(subject.SOURCE_START), "000300.SH"),
            (pd.Timestamp(subject.SOURCE_END), "000300.SH"),
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(np.ones((2, len(columns)), dtype=np.float64), index=index, columns=columns)
    frame["idx_close_point"] = np.asarray([110.0, 90.0], dtype=np.float64)
    frame["idx_pre_close_point"] = np.asarray([100.0, 100.0], dtype=np.float64)
    frame["idx_return_1d"] = np.asarray([9.0, 9.0], dtype=np.float64)
    path = tmp_path / "index.h5"
    frame.to_hdf(path, key="data", format="fixed")

    values = subject._load_benchmark_returns(path, calendar=(subject.SOURCE_START, subject.SOURCE_END))

    assert values[subject.SOURCE_START] == pytest.approx(0.1)
    assert values[subject.SOURCE_END] == pytest.approx(-0.1)


def _asset_binding(tmp_path: Path) -> tuple[Path, Path]:
    release = tmp_path / "release"
    qlib = release / "daily_bin"
    (qlib / "calendars").mkdir(parents=True)
    (qlib / "instruments").mkdir()
    (qlib / "features" / "000001.sz").mkdir(parents=True)
    (qlib / "calendars" / "day.txt").write_text(
        f"{subject.SOURCE_START.isoformat()}\n{subject.SOURCE_END.isoformat()}\n",
        encoding="utf-8",
    )
    (qlib / "instruments" / "all.txt").write_text(
        f"000001.SZ\t{subject.SOURCE_START.isoformat()}\t{subject.SOURCE_END.isoformat()}\n",
        encoding="utf-8",
    )
    for field in subject.QLIB_STOCK_FIELDS:
        np.asarray([0.0, 1.0, 1.0], dtype="<f4").tofile(qlib / "features" / "000001.sz" / f"{field}.day.bin")
    files: dict[str, dict[str, str]] = {}
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp(subject.SOURCE_START), "000001.SZ"),
            (pd.Timestamp(subject.SOURCE_END), "000001.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    index_context_index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp(subject.SOURCE_START), "000300.SH"),
            (pd.Timestamp(subject.SOURCE_END), "000300.SH"),
        ],
        names=["datetime", "instrument"],
    )
    for name, columns, frame_index in (
        ("daily_basic", subject._DAILY_BASIC_COLUMNS, index),
        ("moneyflow", subject._MONEYFLOW_COLUMNS, index),
        (
            "index_context",
            (
                "idx_open_point",
                "idx_high_point",
                "idx_low_point",
                "idx_close_point",
                "idx_pre_close_point",
                "idx_return_1d",
                "idx_volume_hand_source",
                "idx_volume_share_equiv",
                "idx_amount_cny",
            ),
            index_context_index,
        ),
    ):
        path = release / f"{name}.asset"
        pd.DataFrame(
            np.ones((2, len(columns)), dtype=np.float64 if name == "index_context" else np.float32),
            index=frame_index,
            columns=columns,
        ).to_hdf(path, key="data", format="fixed")
        files[name] = {"relative_path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    for name in (
        "suspend_data",
        "suspend_manifest",
        "security_identity",
        "provider_absence",
    ):
        path = release / f"{name}.asset"
        path.write_bytes(name.encode("ascii"))
        files[name] = {"relative_path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    _files, merkle = tree_merkle(qlib)
    body = {
        "schema_version": subject.SOURCE_ASSET_SCHEMA_VERSION,
        "release_identity": _release_identity("fixture-release"),
        "daily_bin": {
            "relative_root": "daily_bin",
            "tree_merkle_sha256": merkle,
            "schema_version": subject.QLIB_STOCK_SCHEMA_VERSION,
            "schema_sha256": qlib_stock_schema_digest(),
        },
        "files": files,
        "source_end": subject.SOURCE_END.isoformat(),
    }
    manifest = {
        **body,
        "release_root": str(release.resolve()),
        "manifest_body_sha256": canonical_sha256(body),
    }
    manifest_path = tmp_path / "release-binding.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path, release


def test_source_asset_binding_verifies_tree_files_and_excludes_locator_from_identity(tmp_path: Path) -> None:
    manifest_path, release = _asset_binding(tmp_path)

    loaded = subject.load_rotation_l1_source_assets(manifest_path)

    assert loaded["release_root"] == release.resolve()
    assert loaded["inventory"]["qlib"]["schema_version"] == subject.QLIB_STOCK_SCHEMA_VERSION
    assert len(loaded["inventory"]["required_fields"]) == 22
    assert loaded["binding_manifest_sha256"] == json.loads(manifest_path.read_text())["manifest_body_sha256"]
    (release / "moneyflow.asset").write_bytes(b"tampered")
    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject.load_rotation_l1_source_assets(manifest_path)
    assert exc_info.value.reason_code == subject.REASON_HASH_MISMATCH


def test_suspend_sidecar_uses_only_full_day_rows_and_preserves_intraday_observations(tmp_path: Path) -> None:
    data_path = tmp_path / "suspend_d.parquet"
    manifest_path = tmp_path / "manifest.json"
    rows = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": pd.Timestamp(subject.SOURCE_START),
                "suspend_type": "S",
                "suspend_timing": None,
            },
            {
                "ts_code": "000002.SZ",
                "trade_date": pd.Timestamp(subject.SOURCE_START),
                "suspend_type": "S",
                "suspend_timing": "09:30-10:00",
            },
        ]
    )
    rows.to_parquet(data_path, index=False)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "suspend_d_dataset_manifest_v1",
                "source": {"contract": "tushare_suspend_d_shsz_S_v1"},
                "start": subject.SOURCE_START.isoformat(),
                "end": subject.SOURCE_END.isoformat(),
                "artifacts": {"suspend_d.parquet": {"sha256": hashlib.sha256(data_path.read_bytes()).hexdigest()}},
            }
        ),
        encoding="utf-8",
    )

    keys = subject._load_suspend_keys(data_path, manifest_path, calendar=(subject.SOURCE_START,))

    assert keys == frozenset({(subject.SOURCE_START, "000001.SZ")})


@pytest.mark.parametrize(
    ("suspend_type", "suspend_timing"),
    (("R", None), ("S", "")),
)
def test_suspend_sidecar_rejects_unknown_type_or_empty_intraday_timing(
    tmp_path: Path,
    suspend_type: str,
    suspend_timing: str | None,
) -> None:
    data_path = tmp_path / "suspend_d.parquet"
    manifest_path = tmp_path / "manifest.json"
    pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": pd.Timestamp(subject.SOURCE_START),
                "suspend_type": suspend_type,
                "suspend_timing": suspend_timing,
            }
        ]
    ).to_parquet(data_path, index=False)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "suspend_d_dataset_manifest_v1",
                "source": {"contract": "tushare_suspend_d_shsz_S_v1"},
                "start": subject.SOURCE_START.isoformat(),
                "end": subject.SOURCE_END.isoformat(),
                "artifacts": {"suspend_d.parquet": {"sha256": hashlib.sha256(data_path.read_bytes()).hexdigest()}},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject._load_suspend_keys(data_path, manifest_path, calendar=(subject.SOURCE_START,))
    assert exc_info.value.reason_code == subject.REASON_SOURCE_SCHEMA_INVALID


def test_builder_cli_persists_typed_failure_without_final_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = tmp_path / "industry.json"
    authority.write_text("{}", encoding="utf-8")
    output = tmp_path / "bundle"

    def fail_build(**_kwargs):
        raise subject.RotationL1InputBundleError(
            subject.REASON_SOURCE_COMPONENT_MISSING,
            "missing frozen component",
            context={"component": "daily_basic"},
        )

    monkeypatch.setattr(cli, "build_rotation_l1_inputs_from_assets", fail_build)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_rotation_l1_input_bundle.py",
            "--dataset-release-manifest",
            str(tmp_path / "release.json"),
            "--industry-pit-authority",
            str(authority),
            "--output-root",
            str(output),
            "--source-end",
            subject.SOURCE_END.isoformat(),
            "--producer-commit",
            "f" * 40,
        ],
    )

    assert cli.main() == 2
    assert not output.exists()
    failures = list(tmp_path.glob(".bundle.failed.*/build.failure.json"))
    assert len(failures) == 1
    failure = json.loads(failures[0].read_text(encoding="utf-8"))
    assert failure["primary_reason_code"] == subject.REASON_SOURCE_COMPONENT_MISSING
    assert failure["database_read_performed"] is False
    assert failure["model_write_performed"] is False


def test_moneyflow_contributor_eligibility_is_frozen_train_only_not_day_local() -> None:
    dates = tuple(date(2022, 1, 4) + timedelta(days=index) for index in range(10))
    spans = {"000001.SZ": ((dates[0], dates[-1]),)}

    class Adapter:
        @staticmethod
        def resolve(symbol, day):
            return SimpleNamespace(status="resolved", canonical_symbol=symbol, trade_date=day)

    class Security:
        @staticmethod
        def resolve(symbol, day, dataset):
            assert dataset == "market.moneyflow_ts"
            return SimpleNamespace(source_ts_code=symbol)

    provider = SimpleNamespace(
        rows=(SimpleNamespace(canonical_ts_code="000001.SZ", source_ts_code="000001.SZ", trade_date=dates[0]),)
    )

    eligibility, receipt = subject._build_train_only_contributor_eligibility(
        spans=spans,
        calendar=dates,
        adapter=Adapter(),
        security=Security(),
        provider_absence=provider,
        suspension_keys=frozenset(),
    )

    assert eligibility == {"000001.SZ": True}
    assert receipt["eligible_count"] == 1
    provider.rows = (
        *provider.rows,
        SimpleNamespace(canonical_ts_code="000001.SZ", source_ts_code="000001.SZ", trade_date=dates[1]),
    )
    eligibility, _receipt = subject._build_train_only_contributor_eligibility(
        spans=spans,
        calendar=dates,
        adapter=Adapter(),
        security=Security(),
        provider_absence=provider,
        suspension_keys=frozenset(),
    )
    assert eligibility == {"000001.SZ": False}


def test_moneyflow_contributor_eligibility_rejects_provider_alias_drift() -> None:
    trade_date = date(2022, 1, 4)

    class Adapter:
        @staticmethod
        def resolve(symbol, day):
            return SimpleNamespace(status="resolved", canonical_symbol=symbol, trade_date=day)

    class Security:
        @staticmethod
        def resolve(symbol, day, dataset):
            assert dataset == "market.moneyflow_ts"
            return SimpleNamespace(source_ts_code="000001.SZ")

    provider = SimpleNamespace(
        rows=(
            SimpleNamespace(
                canonical_ts_code="000001.SZ",
                source_ts_code="000001.OLD",
                trade_date=trade_date,
            ),
        )
    )

    with pytest.raises(subject.RotationL1InputBundleError) as exc_info:
        subject._build_train_only_contributor_eligibility(
            spans={"000001.SZ": ((trade_date, trade_date),)},
            calendar=(trade_date,),
            adapter=Adapter(),
            security=Security(),
            provider_absence=provider,
            suspension_keys=frozenset(),
        )
    assert exc_info.value.reason_code == subject.REASON_AUTHORITY_AMBIGUOUS
