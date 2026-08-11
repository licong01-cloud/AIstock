from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.quality_contracts import (
    M5A_PARENT_BUNDLE_ID,
    QualityProjectionDescriptor,
    QualityTrialMatrix,
    build_quality_train_request,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.quality_pipeline import create_quality_test_request


def _descriptor() -> QualityProjectionDescriptor:
    return QualityProjectionDescriptor(
        path="/data/train_validation.parquet",
        sha256="a" * 64,
        row_count=10,
        date_start="2024-07-04",
        date_end="2026-03-10",
        split_names=("train", "validation"),
    )


def _values() -> dict[str, object]:
    return {
        "output_root": "/out/a",
        "parent_bundle_id": "9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629",
        "parent_request_id": "parent-request",
        "parent_artifacts": {
            name: {"path": f"/bundle/{name}", "sha256": character * 64}
            for name, character in zip(
                ("training_request.json", "feature_schema.json", "label_policy.json", "split.json"),
                "cdef",
                strict=True,
            )
        },
        "parent_split_sha256": "f" * 64,
        "train_validation_projection": _descriptor(),
        "package_id": "pkg",
        "manifest_sha256": "1" * 64,
        "style_profile_id": "style",
        "style_profile_hash": "2" * 64,
        "selection_runtime_semantics_hash": "3" * 64,
        "repository_root": "/repo",
        "repository_commit": "4" * 40,
        "lightgbm_version": "4.6.0",
    }


def test_quality_train_request_identity_excludes_created_at_and_output_root() -> None:
    first = build_quality_train_request(**_values(), created_at="2026-08-12T00:00:00Z")
    second_values = _values()
    second_values["output_root"] = "/out/b"
    second = build_quality_train_request(**second_values, created_at="2026-08-12T01:00:00Z")
    assert first.request_id == second.request_id
    assert first.request_sha256 == second.request_sha256
    assert not hasattr(first, "test_projection")


def test_quality_train_request_rejects_test_rows_and_matrix_reduction() -> None:
    values = _values()
    values["train_validation_projection"] = _descriptor().model_copy(update={"split_names": ("train", "test")})
    with pytest.raises(ValidationError, match="Stage A projection"):
        build_quality_train_request(**values)
    with pytest.raises(ValidationError, match="trial matrix"):
        QualityTrialMatrix(seeds=(20260808,))


def test_quality_contract_module_is_independent_of_output_files(tmp_path: Path) -> None:
    request = build_quality_train_request(**_values())
    target = tmp_path / "request.json"
    request.write_json(target)
    assert target.is_file()
    assert request.model_validate_json(target.read_text(encoding="utf-8")) == request


def test_test_request_cannot_be_created_before_winner_receipt_exists(tmp_path: Path) -> None:
    request = build_quality_train_request(**_values())
    test_projection = QualityProjectionDescriptor(
        path="/data/test.parquet",
        sha256="9" * 64,
        row_count=10,
        date_start="2025-11-07",
        date_end="2026-03-10",
        split_names=("test",),
    )
    assert request.parent_bundle_id == M5A_PARENT_BUNDLE_ID
    with pytest.raises(AdvisoryModelFirstError) as raised:
        create_quality_test_request(
            train_request=request,
            winner_receipt_path=tmp_path / "missing-winner.json",
            test_projection=test_projection,
            output_root=tmp_path,
        )
    assert raised.value.reason_code == "ADVISORY_M5_TEST_ACCESSED_BEFORE_WINNER_FREEZE"
