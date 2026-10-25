from __future__ import annotations

import hashlib
import json

import pytest

import backend.services.advisory_model_first.price_range_runtime_bundle as runtime
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH
from backend.services.advisory_model_first.price_range_contracts import (
    PRICE_RANGE_MODEL_NAMES,
    PRICE_RANGE_QUANTILE_MODEL_NAMES,
    canonical_json_sha256,
)


def _write_exact_binding(root, *, parent_id: str, outcome_id: str):
    package_id = "pkg-test"
    manifest_sha = "a" * 64
    style_hash = "b" * 64
    bundle_id = "c" * 64
    bundle_path = root / "price_range_bundles" / bundle_id
    bundle_path.mkdir(parents=True)
    manifest_path = bundle_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    (bundle_path / "feature_schema.json").write_text(
        json.dumps({"categorical_vocabulary": {"l2_code_id": [1]}}),
        encoding="utf-8",
    )
    binding = {
        "schema_version": "advisory_price_range_binding_v1",
        "package_id": package_id,
        "manifest_sha256": manifest_sha,
        "style_profile_id": "style-test",
        "style_profile_hash": style_hash,
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_price_range_label_policy_v1",
        "parent_bundle_id": parent_id,
        "outcome_bundle_id": outcome_id,
        "price_range_bundle_id": bundle_id,
        "price_range_bundle_manifest_sha256": hashlib.sha256(b"{}").hexdigest(),
        "activated_at": "2026-08-10T00:00:00+00:00",
    }
    binding["binding_sha256"] = canonical_json_sha256(binding)
    path = runtime.price_range_binding_path(
        root,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(binding), encoding="utf-8")
    manifest = {
        "schema_version": "advisory_price_range_bundle_v1",
        "status": "EXPERIMENTAL_SHADOW",
        "calibration_state": "UNCALIBRATED",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_price_range_label_policy_v1",
        "entry_gap_condition": "ENTRY_EXECUTABLE",
        "model_names": list(PRICE_RANGE_MODEL_NAMES),
        "model_count": len(PRICE_RANGE_MODEL_NAMES),
        "package_id": package_id,
        "manifest_sha256": manifest_sha,
        "style_profile_id": "style-test",
        "style_profile_hash": style_hash,
        "parent_bundle_id": parent_id,
        "outcome_bundle_id": outcome_id,
        "request_id": "advprreq-test",
    }
    return package_id, manifest_sha, style_hash, bundle_id, manifest


def test_exact_loader_requires_parent_and_outcome_identity(monkeypatch, tmp_path) -> None:
    parent_id = "d" * 64
    outcome_id = "e" * 64
    package_id, manifest_sha, style_hash, bundle_id, manifest = _write_exact_binding(
        tmp_path,
        parent_id=parent_id,
        outcome_id=outcome_id,
    )
    monkeypatch.setattr(runtime, "read_price_range_bundle_manifest", lambda *_args, **_kwargs: manifest)
    loaded = runtime.load_exact_price_range_bundle(
        model_root=tmp_path,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
        parent_bundle_id=parent_id,
        outcome_bundle_id=outcome_id,
        booster_factory=lambda path: path.name,
    )
    assert loaded.price_range_bundle_id == bundle_id
    assert set(loaded.models) == set(PRICE_RANGE_MODEL_NAMES)

    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        runtime.load_exact_price_range_bundle(
            model_root=tmp_path,
            package_id=package_id,
            manifest_sha256=manifest_sha,
            style_profile_hash=style_hash,
            parent_bundle_id="f" * 64,
            outcome_bundle_id=outcome_id,
            booster_factory=lambda path: path.name,
        )
    assert exc_info.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH"


def test_missing_binding_is_typed_unavailable(tmp_path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        runtime.load_exact_price_range_bundle(
            model_root=tmp_path,
            package_id="pkg-test",
            manifest_sha256="a" * 64,
            style_profile_hash="b" * 64,
            parent_bundle_id="d" * 64,
            outcome_bundle_id="e" * 64,
            booster_factory=lambda path: path.name,
        )
    assert exc_info.value.reason_code == "ADVISORY_PRICE_RANGE_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"


def test_v2_exact_loader_exposes_calibration_spec_without_replacing_models(monkeypatch, tmp_path) -> None:
    parent_id = "d" * 64
    outcome_id = "e" * 64
    package_id, manifest_sha, style_hash, bundle_id, manifest = _write_exact_binding(
        tmp_path, parent_id=parent_id, outcome_id=outcome_id
    )
    manifest.update(
        {
            "schema_version": "advisory_price_range_bundle_v2",
            "calibration_state": "CALIBRATED_INTERVAL",
            "entry_gap_calibration_state": "CALIBRATED",
            "entry_executable_calibration_state": "UNCALIBRATED",
        }
    )
    bundle_path = tmp_path / "price_range_bundles" / bundle_id
    calibration = {
        "schema_version": "advisory_price_range_calibration_spec_v1",
        "state": "CALIBRATED",
        "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION",
        "delta": 0.0125,
    }
    (bundle_path / "calibration_spec.json").write_text(json.dumps(calibration), encoding="utf-8")
    monkeypatch.setattr(runtime, "_read_runtime_bundle_manifest", lambda *_a, **_k: manifest)

    loaded = runtime.load_exact_price_range_bundle(
        model_root=tmp_path,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
        parent_bundle_id=parent_id,
        outcome_bundle_id=outcome_id,
        booster_factory=lambda path: path.name,
    )

    assert loaded.calibration_spec == calibration
    assert set(loaded.models) == set(PRICE_RANGE_MODEL_NAMES)


def test_v3_exact_loader_accepts_only_three_quantile_heads(monkeypatch, tmp_path) -> None:
    parent_id = "d" * 64
    outcome_id = "e" * 64
    package_id, manifest_sha, style_hash, bundle_id, manifest = _write_exact_binding(
        tmp_path, parent_id=parent_id, outcome_id=outcome_id
    )
    binding_path = runtime.price_range_binding_path(
        tmp_path,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
    )
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["label_policy_version"] = "advisory_price_range_label_policy_v2"
    binding_without_hash = {key: value for key, value in binding.items() if key != "binding_sha256"}
    binding["binding_sha256"] = canonical_json_sha256(binding_without_hash)
    binding_path.write_text(json.dumps(binding), encoding="utf-8")
    manifest.update(
        {
            "schema_version": "advisory_price_range_bundle_v3",
            "output_schema_version": "advisory_daily_price_envelope_v1",
            "objective_contract": "RISK_MANAGED_ADVISORY",
            "calibration_state": "UNCALIBRATED",
            "nominal_coverage": 0.8,
            "label_policy_version": "advisory_price_range_label_policy_v2",
            "entry_gap_condition": "NEXT_TRADING_DAY_VALID_OPEN",
            "model_names": list(PRICE_RANGE_QUANTILE_MODEL_NAMES),
            "model_count": len(PRICE_RANGE_QUANTILE_MODEL_NAMES),
        }
    )
    monkeypatch.setattr(runtime, "_read_runtime_bundle_manifest", lambda *_args, **_kwargs: manifest)

    loaded = runtime.load_exact_price_range_bundle(
        model_root=tmp_path,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
        parent_bundle_id=parent_id,
        outcome_bundle_id=outcome_id,
        booster_factory=lambda path: path.name,
    )

    assert set(loaded.models) == set(PRICE_RANGE_QUANTILE_MODEL_NAMES)
    assert loaded.calibration_spec is None


def test_v4_runtime_manifest_requires_calibrated_three_head_contract() -> None:
    manifest = {
        "schema_version": "advisory_price_range_bundle_v4",
        "output_schema_version": "advisory_daily_price_envelope_v1",
        "objective_contract": "RISK_MANAGED_ADVISORY",
        "status": "EXPERIMENTAL_SHADOW",
        "calibration_state": "CALIBRATED_INTERVAL",
        "nominal_coverage": 0.8,
        "entry_gap_calibration_state": "CALIBRATED",
        "entry_admission_model_status": "RETIRED_NON_IDENTIFIABLE",
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": FEATURE_SCHEMA_HASH,
        "label_policy_version": "advisory_price_range_label_policy_v2",
        "entry_gap_condition": "NEXT_TRADING_DAY_VALID_OPEN",
        "model_names": list(PRICE_RANGE_QUANTILE_MODEL_NAMES),
        "model_count": len(PRICE_RANGE_QUANTILE_MODEL_NAMES),
    }

    runtime._validate_runtime_manifest(manifest)
    manifest["model_names"] = list(PRICE_RANGE_MODEL_NAMES)
    with pytest.raises(AdvisoryModelFirstError):
        runtime._validate_runtime_manifest(manifest)


def test_frozen_v4_loader_does_not_require_price_binding(monkeypatch, tmp_path) -> None:
    parent_id = "d" * 64
    outcome_id = "e" * 64
    package_id, manifest_sha, style_hash, bundle_id, manifest = _write_exact_binding(
        tmp_path, parent_id=parent_id, outcome_id=outcome_id
    )
    binding_path = runtime.price_range_binding_path(
        tmp_path,
        package_id=package_id,
        manifest_sha256=manifest_sha,
        style_profile_hash=style_hash,
    )
    binding_path.unlink()
    manifest.update(
        {
            "schema_version": "advisory_price_range_bundle_v4",
            "output_schema_version": "advisory_daily_price_envelope_v1",
            "objective_contract": "RISK_MANAGED_ADVISORY",
            "calibration_state": "CALIBRATED_INTERVAL",
            "nominal_coverage": 0.8,
            "entry_gap_calibration_state": "CALIBRATED",
            "entry_admission_model_status": "RETIRED_NON_IDENTIFIABLE",
            "label_policy_version": "advisory_price_range_label_policy_v2",
            "entry_gap_condition": "NEXT_TRADING_DAY_VALID_OPEN",
            "model_names": list(PRICE_RANGE_QUANTILE_MODEL_NAMES),
            "model_count": len(PRICE_RANGE_QUANTILE_MODEL_NAMES),
        }
    )
    bundle_path = tmp_path / "price_range_bundles" / bundle_id
    calibration = {
        "schema_version": "advisory_daily_price_envelope_calibration_spec_v1",
        "state": "CALIBRATED",
        "method": "CQR_CENTRAL_80_NONNEGATIVE_EXPANSION",
        "delta": 0.0,
    }
    (bundle_path / "calibration_spec.json").write_text(json.dumps(calibration), encoding="utf-8")
    monkeypatch.setattr(runtime, "_read_runtime_bundle_manifest", lambda *_args, **_kwargs: manifest)

    loaded = runtime.load_frozen_price_range_bundle(
        model_root=tmp_path,
        price_range_bundle_id=bundle_id,
        price_range_bundle_manifest_sha256=hashlib.sha256(b"{}").hexdigest(),
        expected_package_id=package_id,
        expected_manifest_sha256=manifest_sha,
        expected_style_profile_hash=style_hash,
        expected_parent_bundle_id=parent_id,
        expected_outcome_bundle_id=outcome_id,
        booster_factory=lambda path: path.name,
    )

    assert loaded.price_range_bundle_id == bundle_id
    assert set(loaded.models) == set(PRICE_RANGE_QUANTILE_MODEL_NAMES)
    assert loaded.calibration_spec == calibration
