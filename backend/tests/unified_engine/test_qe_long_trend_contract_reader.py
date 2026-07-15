from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from backend.services.quantevolver.long_trend_data_reader import (
    QELongTrendDatasetReader,
    canonicalize_instrument,
    inspect_qe_snapshot_identity,
    verify_outcome_snapshot_extension,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    FamilyComputationStatus,
    FamilyEvidenceStatus,
    QEDatasetSnapshotIdentity,
    QELongTrendEvaluationContext,
    QELongTrendError,
    QELongTrendReason,
    QE_LONG_TREND_PROFILE_V1,
    SnapshotOverlapParityReceipt,
    build_evaluation_id,
    canonical_input_manifest,
    canonical_sha256,
    get_long_trend_profile,
    require_registered_profile,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_CONTRACT_ID,
    QE_DATASET_SIGNAL_END_DATE,
    QE_DATASET_START_DATE,
)


def _daily_frame() -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [pd.date_range("2026-01-05", periods=3, freq="B"), ["SZ000001", "SH600000"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame(
        {
            "open": 10.0,
            "close": 10.5,
            "high": 11.0,
            "low": 9.5,
            "volume": 100.0,
            "factor": 1.0,
            "amount": 1000.0,
        },
        index=index,
    )


def _sector_frame() -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [pd.date_range("2026-01-05", periods=3, freq="B"), ["SZ000001", "SH600000"]],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"l2_code_id": [1, 2] * 3, "sw2_close": 100.0}, index=index)


def _prepare_reader_paths(tmp_path: Path) -> tuple[Path, Path, QEDatasetSnapshotIdentity]:
    workspace = tmp_path / "workspace"
    data_root = tmp_path / "factor_data"
    workspace.mkdir()
    data_root.mkdir()
    (data_root / "meta.json").write_text(
        json.dumps(
            {
                "snapshot_id": QE_DATASET_CONTRACT_ID,
                "start": QE_DATASET_START_DATE.isoformat(),
                "end": QE_DATASET_SIGNAL_END_DATE.isoformat(),
                "lineage_parent_ids": ["qe-parent-snapshot"],
            }
        ),
        encoding="utf-8",
    )
    for name in ("daily_pv.h5", "sector_data.h5"):
        source = data_root / name
        source.write_bytes(name.encode("utf-8"))
        os.link(source, workspace / name)
    return workspace, data_root, inspect_qe_snapshot_identity(data_root)


def test_profile_and_evaluation_identity_are_versioned_and_null_explicit() -> None:
    profile = get_long_trend_profile("qe_long_trend_v1")
    assert profile is QE_LONG_TREND_PROFILE_V1
    assert profile.horizons == (20, 30, 40, 60, 120, 180)
    assert profile.barriers == (0.30, 0.50, 0.70)
    assert profile.profile_sha256 == canonical_sha256(profile.canonical_payload())

    with pytest.raises(QELongTrendError) as exc_info:
        get_long_trend_profile("unknown")
    assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value

    missing = canonical_input_manifest({"prediction_sha256": "pred-sha"})
    restored = canonical_input_manifest({"prediction_sha256": "pred-sha", "position_sha256": "position-sha"})
    assert missing["position_sha256"] == {
        "type": "explicit_null",
        "field": "position_sha256",
    }
    first = build_evaluation_id(
        run_id="qe_run_1",
        profile_sha256=profile.profile_sha256,
        evaluator_source_sha256="source-sha",
        feature_dataset_manifest_sha256=None,
        outcome_dataset_manifest_sha256=None,
        input_manifest_sha256=canonical_sha256(missing),
    )
    second = build_evaluation_id(
        run_id="qe_run_1",
        profile_sha256=profile.profile_sha256,
        evaluator_source_sha256="source-sha",
        feature_dataset_manifest_sha256=None,
        outcome_dataset_manifest_sha256=None,
        input_manifest_sha256=canonical_sha256(restored),
    )
    assert first.startswith("qelt_")
    assert first != second

    extended = canonical_input_manifest(
        {
            "prediction_sha256": "pred-sha",
            "custom_present_sha256": "custom-sha",
            "custom_missing_sha256": None,
        }
    )
    assert extended["custom_present_sha256"] == "custom-sha"
    assert extended["custom_missing_sha256"] == {
        "type": "explicit_null",
        "field": "custom_missing_sha256",
    }

    with pytest.raises(QELongTrendError) as exc_info:
        build_evaluation_id(
            run_id="",
            profile_sha256=profile.profile_sha256,
            evaluator_source_sha256="source-sha",
            feature_dataset_manifest_sha256=None,
            outcome_dataset_manifest_sha256=None,
            input_manifest_sha256=canonical_sha256(missing),
        )
    assert exc_info.value.reason_code == QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING.value

    with pytest.raises(QELongTrendError) as exc_info:
        require_registered_profile(replace(profile, bootstrap_seed=1))
    assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value


@pytest.mark.parametrize(
    "changes",
    [
        {"profile_id": ""},
        {"horizons": ()},
        {"horizons": (4, 2)},
        {"horizons": (1, 2.0)},
        {"barriers": (0.5, 0.3)},
        {"barriers": (0.0, 0.3)},
        {"fixed_k": (0, 20)},
        {"fixed_k": (50, 20)},
        {"include_strategy_topk_up_to": 0},
        {"entry_coverage_reference": 0.0},
        {"path_coverage_reference": 1.1},
        {"bootstrap_samples": 0},
        {"calendar_slices": ("all_oos", "all_oos")},
        {"entry_rule": "entry_on_signal_day_close"},
        {"terminal_rule": "unsupported_terminal"},
        {"sector_projection": "unsupported_sector"},
    ],
)
def test_profile_rejects_invalid_frozen_contracts(changes: dict[str, object]) -> None:
    with pytest.raises(QELongTrendError) as exc_info:
        replace(QE_LONG_TREND_PROFILE_V1, **changes)
    assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value


def test_family_status_serializes_all_evidence_axes() -> None:
    status = FamilyEvidenceStatus(
        status=FamilyComputationStatus.COMPUTED_WITH_LIMITATIONS,
        available_inputs=("prediction",),
        missing_inputs=("position",),
        coverage={"ratio": 0.5},
        limitations=("limited",),
        supporting_artifacts=("pred.pkl",),
        reason_codes=(QELongTrendReason.POSITION_ARTIFACT_MISSING.value,),
        data_actions=(
            {
                "action": "restore_position",
                "source_candidates": ["qe_recorder"],
                "required_fields": ["position_date", "instrument", "amount"],
                "time_range": {"start": "run_start", "end": "evaluation_asof"},
                "historical_backfill": True,
                "recoverable_family": "position_episode",
            },
        ),
    ).as_dict()
    assert status["status"] == "COMPUTED_WITH_LIMITATIONS"
    assert status["data_actions"][0]["action"] == "restore_position"
    assert status["data_actions"][0]["historical_backfill"] is True


def test_contract_identities_reject_incomplete_or_inconsistent_context() -> None:
    invalid_snapshots = (
        {"snapshot_id": "", "manifest_sha256": "sha", "start_date": "2026-01-01", "end_date": "2026-01-02"},
        {"snapshot_id": "snap", "manifest_sha256": "", "start_date": "2026-01-01", "end_date": "2026-01-02"},
        {"snapshot_id": "snap", "manifest_sha256": "sha", "start_date": "invalid", "end_date": "2026-01-02"},
        {"snapshot_id": "snap", "manifest_sha256": "sha", "start_date": "2026-01-03", "end_date": "2026-01-02"},
        {
            "snapshot_id": "snap",
            "manifest_sha256": "sha",
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "lineage_parent_ids": ("snap",),
        },
        {
            "snapshot_id": "snap",
            "manifest_sha256": "sha",
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
            "lineage_parent_ids": ("parent", "parent"),
        },
    )
    for kwargs in invalid_snapshots:
        with pytest.raises(QELongTrendError) as exc_info:
            QEDatasetSnapshotIdentity(**kwargs)
        assert exc_info.value.reason_code == QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING.value

    with pytest.raises(QELongTrendError) as exc_info:
        FamilyEvidenceStatus(
            status=FamilyComputationStatus.NOT_COMPUTABLE,
            data_actions=({"action": "restore_missing_input"},),
        )
    assert exc_info.value.reason_code == QELongTrendReason.PROFILE_INVALID.value

    invalid_receipts = (
        {
            "feature_snapshot_id": "feature",
            "outcome_snapshot_id": "outcome",
            "overlap_start": "2026-01-01",
            "overlap_end": "2026-01-02",
            "row_count": 0,
            "column_count": 4,
            "overlap_price_parity_sha256": "sha",
            "relation": "verified_extension",
        },
        {
            "feature_snapshot_id": "feature",
            "outcome_snapshot_id": "outcome",
            "overlap_start": "2026-01-01",
            "overlap_end": "2026-01-02",
            "row_count": 2.0,
            "column_count": 4,
            "overlap_price_parity_sha256": "sha",
            "relation": "verified_extension",
        },
        {
            "feature_snapshot_id": "feature",
            "outcome_snapshot_id": "outcome",
            "overlap_start": "2026-01-01",
            "overlap_end": "2026-01-02",
            "row_count": 2,
            "column_count": 3,
            "overlap_price_parity_sha256": "sha",
            "relation": "verified_extension",
        },
        {
            "feature_snapshot_id": "feature",
            "outcome_snapshot_id": "outcome",
            "overlap_start": "2026-01-01",
            "overlap_end": "2026-01-02",
            "row_count": 2,
            "column_count": 4,
            "overlap_price_parity_sha256": "sha",
            "relation": "unverified",
        },
    )
    for kwargs in invalid_receipts:
        with pytest.raises(QELongTrendError) as exc_info:
            SnapshotOverlapParityReceipt(**kwargs)
        assert exc_info.value.reason_code == QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION.value

    feature = QEDatasetSnapshotIdentity(
        snapshot_id="feature",
        manifest_sha256="feature-sha",
        start_date="2026-01-01",
        end_date="2026-01-02",
    )
    outcome = QEDatasetSnapshotIdentity(
        snapshot_id="outcome",
        manifest_sha256="outcome-sha",
        start_date="2026-01-01",
        end_date="2026-01-03",
        lineage_parent_ids=("feature",),
    )
    receipt = SnapshotOverlapParityReceipt(
        feature_snapshot_id="feature",
        outcome_snapshot_id="outcome",
        overlap_start="2026-01-01",
        overlap_end="2026-01-02",
        row_count=2,
        column_count=4,
        overlap_price_parity_sha256="overlap-sha",
        relation="verified_extension",
    )
    with pytest.raises(QELongTrendError) as exc_info:
        QELongTrendEvaluationContext(
            run_id="",
            evaluator_source_sha256="source-sha",
            feature_snapshot=feature,
            outcome_snapshot=outcome,
            overlap_receipt=receipt,
            input_artifact_hashes={},
        )
    assert exc_info.value.reason_code == QELongTrendReason.FEATURE_DATASET_IDENTITY_MISSING.value

    for broken_receipt in (
        replace(receipt, feature_snapshot_id="other-feature"),
        replace(receipt, outcome_snapshot_id="other-outcome"),
        replace(receipt, overlap_end="2026-01-01"),
        replace(receipt, relation="same_snapshot"),
    ):
        with pytest.raises(QELongTrendError) as exc_info:
            QELongTrendEvaluationContext(
                run_id="qe-run",
                evaluator_source_sha256="source-sha",
                feature_snapshot=feature,
                outcome_snapshot=outcome,
                overlap_receipt=broken_receipt,
                input_artifact_hashes={},
            )
        assert exc_info.value.reason_code == QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION.value

    context = QELongTrendEvaluationContext(
        run_id="qe-run",
        evaluator_source_sha256="source-sha",
        feature_snapshot=feature,
        outcome_snapshot=outcome,
        overlap_receipt=receipt,
        input_artifact_hashes={"prediction_sha256": "prediction-sha"},
    )
    base_id = context.evaluation_id(profile_sha256=QE_LONG_TREND_PROFILE_V1.profile_sha256)
    label_id = context.evaluation_id(
        profile_sha256=QE_LONG_TREND_PROFILE_V1.profile_sha256,
        evaluation_parameters={"label_horizon": 20},
    )
    topk_id = context.evaluation_id(
        profile_sha256=QE_LONG_TREND_PROFILE_V1.profile_sha256,
        evaluation_parameters={"strategy_topk": 25},
    )
    assert len({base_id, label_id, topk_id}) == 3
    assert context.as_dict(profile_sha256=QE_LONG_TREND_PROFILE_V1.profile_sha256)["input_manifest"][
        "evaluation_parameters"
    ]["label_horizon"] == {
        "type": "explicit_null",
        "field": "label_horizon",
    }


def test_canonicalize_instrument_supports_both_qe_conventions() -> None:
    assert canonicalize_instrument("SZ000001") == "000001.SZ"
    assert canonicalize_instrument("600000.sh") == "600000.SH"
    with pytest.raises(ValueError):
        canonicalize_instrument("AAPL")


def test_reader_loads_only_qe_daily_and_sector_files(tmp_path: Path) -> None:
    workspace, data_root, snapshot_identity = _prepare_reader_paths(tmp_path)
    (data_root / "moneyflow.h5").touch()
    calls: list[str] = []

    def fake_reader(path: Path) -> pd.DataFrame:
        calls.append(path.name)
        if path.name == "daily_pv.h5":
            return _daily_frame()
        if path.name == "sector_data.h5":
            return _sector_frame()
        raise AssertionError(f"unexpected file read: {path}")

    reader = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=fake_reader,
    )
    loaded = reader.load(
        start_date="2026-01-05",
        end_date="2026-01-07",
        instruments=["000001.SZ"],
    )
    assert calls == ["daily_pv.h5", "sector_data.h5"]
    assert snapshot_identity.lineage_parent_ids == ("qe-parent-snapshot",)
    assert list(loaded.prices.columns) == [
        "open_qfq",
        "close_qfq",
        "high_qfq",
        "low_qfq",
        "volume_qfq",
    ]
    assert loaded.prices.index.get_level_values("instrument").unique().tolist() == ["000001.SZ"]
    assert str(loaded.sectors["l2_code_id"].dtype) == "Int16"

    bindings = reader.verify_workspace_binding()
    assert set(bindings) == {"daily_pv.h5", "sector_data.h5"}
    assert calls == ["daily_pv.h5", "sector_data.h5"]


def test_reader_rejects_non_qe_identity_and_invalid_sector_schema(tmp_path: Path) -> None:
    workspace, data_root, snapshot_identity = _prepare_reader_paths(tmp_path)

    with pytest.raises(QELongTrendError) as exc_info:
        QELongTrendDatasetReader(
            factor_data_dir=data_root,
            qe_workspace_root=workspace,
            qe_dataset_contract_id="live_selection_snapshot",
            snapshot_identity=snapshot_identity,
        )
    assert exc_info.value.reason_code == QELongTrendReason.NON_QE_SOURCE_REJECTED.value

    def invalid_sector_reader(path: Path) -> pd.DataFrame:
        if path.name == "daily_pv.h5":
            return _daily_frame()
        return _sector_frame().drop(columns=["l2_code_id"])

    reader = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=invalid_sector_reader,
    )
    with pytest.raises(QELongTrendError) as exc_info:
        reader.load(start_date="2026-01-05", end_date="2026-01-07")
    assert exc_info.value.reason_code == QELongTrendReason.SECTOR_DATA_SCHEMA_INVALID.value

    def sentinel_sector_reader(path: Path) -> pd.DataFrame:
        if path.name == "daily_pv.h5":
            return _daily_frame()
        frame = _sector_frame()
        frame.iloc[0, frame.columns.get_loc("l2_code_id")] = -1
        return frame

    sentinel_reader = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=sentinel_sector_reader,
    )
    sentinel_loaded = sentinel_reader.load(
        start_date="2026-01-05",
        end_date="2026-01-07",
    )
    assert sentinel_loaded.sectors is not None
    assert int(sentinel_loaded.sectors["l2_code_id"].isna().sum()) == 1


def test_reader_price_only_and_structured_read_failure(tmp_path: Path) -> None:
    workspace, data_root, snapshot_identity = _prepare_reader_paths(tmp_path)

    reader = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=lambda _path: _daily_frame(),
    )
    prices = reader.load_prices(start_date="2026-01-05", end_date="2026-01-07")
    assert len(prices) == 6

    failing = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=lambda _path: (_ for _ in ()).throw(OSError("broken h5")),
    )
    with pytest.raises(QELongTrendError) as exc_info:
        failing.load_prices(start_date="2026-01-05", end_date="2026-01-07")
    assert exc_info.value.reason_code == QELongTrendReason.DAILY_PV_SCHEMA_INVALID.value


def test_reader_requires_workspace_file_binding_and_exact_manifest(tmp_path: Path) -> None:
    workspace, data_root, snapshot_identity = _prepare_reader_paths(tmp_path)
    os.unlink(workspace / "daily_pv.h5")
    (workspace / "daily_pv.h5").write_bytes(b"different-file")
    reader = QELongTrendDatasetReader(
        factor_data_dir=data_root,
        qe_workspace_root=workspace,
        qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
        snapshot_identity=snapshot_identity,
        hdf_reader=lambda _path: _daily_frame(),
    )
    with pytest.raises(QELongTrendError) as exc_info:
        reader.load_prices(start_date="2026-01-05", end_date="2026-01-07")
    assert exc_info.value.reason_code == QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH.value

    wrong_identity = replace(snapshot_identity, manifest_sha256="wrong")
    with pytest.raises(QELongTrendError) as exc_info:
        QELongTrendDatasetReader(
            factor_data_dir=data_root,
            qe_workspace_root=workspace,
            qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
            snapshot_identity=wrong_identity,
        )
    assert exc_info.value.reason_code == QELongTrendReason.DATASET_ROOT_IDENTITY_MISMATCH.value


def test_outcome_snapshot_requires_lineage_and_exact_overlap_price_parity() -> None:
    feature_prices = _daily_frame()
    extra_index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-01-08")], ["SZ000001", "SH600000"]],
        names=["datetime", "instrument"],
    )
    extra = pd.DataFrame(
        {
            "open": 10.0,
            "close": 10.5,
            "high": 11.0,
            "low": 9.5,
            "volume": 100.0,
            "factor": 1.0,
            "amount": 1000.0,
        },
        index=extra_index,
    )
    outcome_prices = pd.concat([feature_prices, extra]).sort_index()
    feature_identity = QEDatasetSnapshotIdentity(
        snapshot_id="qe_feature_v1",
        manifest_sha256="feature-sha",
        start_date="2026-01-05",
        end_date="2026-01-07",
    )
    outcome_identity = QEDatasetSnapshotIdentity(
        snapshot_id="qe_outcome_v2",
        manifest_sha256="outcome-sha",
        start_date="2026-01-05",
        end_date="2026-01-08",
        lineage_parent_ids=("qe_feature_v1",),
    )
    receipt = verify_outcome_snapshot_extension(
        feature_identity=feature_identity,
        outcome_identity=outcome_identity,
        feature_prices=feature_prices,
        outcome_prices=outcome_prices,
    )
    assert receipt.relation == "verified_extension"
    assert receipt.row_count == len(feature_prices)

    mutated = outcome_prices.copy()
    mutated.loc[(pd.Timestamp("2026-01-06"), "SZ000001"), "close"] += 1e-9
    with pytest.raises(QELongTrendError) as exc_info:
        verify_outcome_snapshot_extension(
            feature_identity=feature_identity,
            outcome_identity=outcome_identity,
            feature_prices=feature_prices,
            outcome_prices=mutated,
        )
    assert exc_info.value.reason_code == QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION.value

    missing_lineage = replace(outcome_identity, lineage_parent_ids=())
    with pytest.raises(QELongTrendError) as exc_info:
        verify_outcome_snapshot_extension(
            feature_identity=feature_identity,
            outcome_identity=missing_lineage,
            feature_prices=feature_prices,
            outcome_prices=outcome_prices,
        )
    assert exc_info.value.reason_code == QELongTrendReason.OUTCOME_SNAPSHOT_NOT_EXTENSION.value

    same_identity = replace(
        feature_identity,
        snapshot_id=feature_identity.snapshot_id,
        manifest_sha256=feature_identity.manifest_sha256,
    )
    same = verify_outcome_snapshot_extension(
        feature_identity=feature_identity,
        outcome_identity=same_identity,
        feature_prices=feature_prices,
        outcome_prices=feature_prices,
    )
    assert same.relation == "same_snapshot"

    empty_index = pd.MultiIndex.from_arrays(
        [[], []],
        names=["datetime", "instrument"],
    )
    empty_prices = pd.DataFrame(
        columns=["open", "high", "low", "close"],
        index=empty_index,
    )
    with pytest.raises(QELongTrendError) as exc_info:
        verify_outcome_snapshot_extension(
            feature_identity=feature_identity,
            outcome_identity=feature_identity,
            feature_prices=empty_prices,
            outcome_prices=empty_prices,
        )
    assert exc_info.value.reason_code == QELongTrendReason.SNAPSHOT_OVERLAP_EMPTY.value
