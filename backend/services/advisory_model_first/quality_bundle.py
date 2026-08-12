from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.model_bundle import _read_and_validate_bundle
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.quality_contracts import (
    ENSEMBLE_SCORE_POLICY,
    QUALITY_BASELINE_NAMES,
    QUALITY_SEEDS,
    SELECTION_PRIOR_POLICY,
    AdvisoryRerankerQualityTestRequestV1,
    AdvisoryRerankerQualityTrainRequestV1,
    QualityWinnerReceiptV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def publish_quality_model_bundle(
    *,
    model_root: str | Path,
    train_request: AdvisoryRerankerQualityTrainRequestV1,
    test_request: AdvisoryRerankerQualityTestRequestV1,
    winner_receipt: QualityWinnerReceiptV1,
    test_report_path: str | Path,
) -> tuple[str, Path, dict[str, Any]]:
    winner = winner_receipt.winner
    if winner.model_weight == 0.0:
        raise AdvisoryModelFirstError(
            "selection-prior-only result cannot be published as an M5A model bundle",
            reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
        )
    if winner.seeds != QUALITY_SEEDS or len(winner.member_model_paths) != len(QUALITY_SEEDS):
        raise AdvisoryModelFirstError(
            "M5A winner does not contain the complete five-seed ensemble",
            reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
        )
    frozen_winner_path = Path(test_request.winner_receipt_path)
    if (
        winner_receipt.train_request_id != train_request.request_id
        or winner_receipt.train_request_sha256 != train_request.request_sha256
        or test_request.train_request_id != train_request.request_id
        or test_request.train_request_sha256 != train_request.request_sha256
        or test_request.parent_bundle_id != train_request.parent_bundle_id
        or test_request.parent_split_sha256 != train_request.parent_split_sha256
        or test_request.winner_receipt_id != winner_receipt.receipt_id
        or not frozen_winner_path.is_file()
        or sha256_file(frozen_winner_path) != test_request.winner_receipt_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5A publish inputs differ from the frozen train and winner",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    try:
        frozen_winner = QualityWinnerReceiptV1.model_validate_json(
            frozen_winner_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "M5A frozen winner receipt cannot be read during publication",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
            context={"error_type": type(exc).__name__},
        ) from exc
    if frozen_winner != winner_receipt:
        raise AdvisoryModelFirstError(
            "M5A publish winner differs from the test request winner artifact",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    for artifact_name, descriptor in train_request.parent_artifacts.items():
        artifact_path = Path(descriptor.path)
        if not artifact_path.is_file() or sha256_file(artifact_path) != descriptor.sha256:
            raise AdvisoryModelFirstError(
                "M5A parent artifact changed before bundle publication",
                reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
                context={"artifact": artifact_name},
            )
    parent_bundle_path = Path(train_request.parent_artifacts["training_request.json"].path).parent
    parent_manifest = _read_json(parent_bundle_path / "manifest.json")
    parent_training_request = _read_json(parent_bundle_path / "training_request.json")
    expected_parent_identity = {
        "bundle_id": train_request.parent_bundle_id,
        "request_id": train_request.parent_request_id,
        "package_id": train_request.package_id,
        "manifest_sha256": train_request.manifest_sha256,
        "style_profile_id": train_request.style_profile_id,
        "style_profile_hash": train_request.style_profile_hash,
        "selection_runtime_semantics_hash": train_request.selection_runtime_semantics_hash,
    }
    expected_training_identity = {
        "program_id": train_request.program_id,
        "binding_version_id": train_request.binding_version_id,
    }
    if (
        {key: parent_manifest.get(key) for key in expected_parent_identity} != expected_parent_identity
        or {key: parent_training_request.get(key) for key in expected_training_identity}
        != expected_training_identity
    ):
        raise AdvisoryModelFirstError(
            "M5A parent bundle differs from the frozen request",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    tournament_report_path = Path(winner_receipt.tournament_report_path)
    if (
        not tournament_report_path.is_file()
        or sha256_file(tournament_report_path) != winner_receipt.tournament_report_sha256
    ):
        raise AdvisoryModelFirstError(
            "M5A tournament report changed after winner freeze",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )
    test_path = Path(test_report_path)
    test_report = _read_json(test_path)
    winner_metrics = test_report.get("winner_metrics")
    baselines = test_report.get("baselines")
    if (
        test_report.get("schema_version") != "advisory_reranker_quality_test_report_v1"
        or test_report.get("evaluation_id") != test_request.evaluation_id
        or test_report.get("test_request_sha256") != test_request.request_sha256
        or test_report.get("winner_receipt_id") != winner_receipt.receipt_id
        or test_report.get("winner_receipt_sha256") != winner_receipt.receipt_sha256
        or not isinstance(winner_metrics, dict)
        or not isinstance(baselines, dict)
        or not set(QUALITY_BASELINE_NAMES).issubset(baselines)
    ):
        raise AdvisoryModelFirstError(
            "M5A test report differs from the frozen Stage B evaluation",
            reason_code="ADVISORY_M5_INPUT_IDENTITY_MISMATCH",
        )

    root = Path(model_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    bundles_root = root / "bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="advisory_m5_bundle_", dir=root))
    try:
        members = []
        for seed, source_value, expected_hash in zip(
            QUALITY_SEEDS,
            winner.member_model_paths,
            winner.member_model_sha256,
            strict=True,
        ):
            source = Path(source_value)
            filename = f"model_seed_{seed}.txt"
            if not source.is_file() or source.stat().st_size <= 0 or sha256_file(source) != expected_hash:
                raise AdvisoryModelFirstError(
                    "M5A winning booster is missing, empty, or corrupt",
                    reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
                    context={"seed": seed},
                )
            shutil.copyfile(source, temporary / filename)
            members.append({"seed": seed, "filename": filename, "sha256": expected_hash})

        vocabulary_path = Path(str(winner.categorical_vocabulary_path))
        if not vocabulary_path.is_file() or sha256_file(vocabulary_path) != winner.categorical_vocabulary_sha256:
            raise AdvisoryModelFirstError(
                "M5A winning categorical vocabulary changed before publication",
                reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
            )
        vocabulary = _read_json(vocabulary_path).get("categorical_vocabulary")
        feature_schema = _read_json(parent_bundle_path / "feature_schema.json")
        feature_schema["categorical_vocabulary"] = vocabulary
        _write_json(temporary / "feature_schema.json", feature_schema)
        shutil.copyfile(
            train_request.parent_artifacts["fresh_hmm_models.json"].path,
            temporary / "fresh_hmm_models.json",
        )
        _write_json(
            temporary / "baseline_comparison.json",
            {
                "model_top5": winner_metrics,
                **baselines,
                "comparison_context": {
                    "schema_version": "advisory_m5_baseline_comparison_v1",
                    "winner_receipt_id": winner_receipt.receipt_id,
                },
            },
        )
        train_request.write_json(temporary / "quality_train_request.json")
        winner_receipt.write_json(temporary / "winner_receipt.json")
        shutil.copyfile(winner_receipt.tournament_report_path, temporary / "tournament_report.json")
        shutil.copyfile(test_path, temporary / "test_report.json")

        files = {
            path.name: {"sha256": sha256_file(path), "size_bytes": path.stat().st_size}
            for path in sorted(temporary.iterdir())
            if path.is_file()
        }
        manifest_without_id = {
            "schema_version": "advisory_model_bundle_v2",
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "NOT_APPLICABLE_RANKING_SCORE",
            "request_id": train_request.request_id,
            "request_sha256": train_request.request_sha256,
            "program_id": train_request.program_id,
            "binding_version_id": train_request.binding_version_id,
            "package_id": train_request.package_id,
            "manifest_sha256": train_request.manifest_sha256,
            "package_asset_closure_hash": parent_manifest["package_asset_closure_hash"],
            "style_profile_id": train_request.style_profile_id,
            "style_profile_hash": train_request.style_profile_hash,
            "selection_runtime_semantics_id": parent_manifest["selection_runtime_semantics_id"],
            "selection_runtime_semantics_hash": train_request.selection_runtime_semantics_hash,
            "selection_runtime_semantics": parent_manifest["selection_runtime_semantics"],
            "terminal_weights": parent_manifest["terminal_weights"],
            "continuation_cutoff": parent_manifest["continuation_cutoff"],
            "feature_schema_version": parent_manifest["feature_schema_version"],
            "feature_schema_hash": parent_manifest["feature_schema_hash"],
            "label_policy_version": parent_manifest["label_policy_version"],
            "decision_clock_version": parent_manifest["decision_clock_version"],
            "parent_bundle_id": train_request.parent_bundle_id,
            "parent_split_sha256": train_request.parent_split_sha256,
            "window_id": winner.window_id,
            "family_id": winner.family_id,
            "seeds": list(QUALITY_SEEDS),
            "model_weight": winner.model_weight,
            "ensemble_score_policy": ENSEMBLE_SCORE_POLICY,
            "selection_prior_policy": SELECTION_PRIOR_POLICY,
            "explanation_policy": "MODEL_MEMBER_RAW_CONTRIBUTION_MEAN_V1",
            "ensemble_members": members,
            "winner_receipt_id": winner_receipt.receipt_id,
            "winner_receipt_sha256": winner_receipt.receipt_sha256,
            "tournament_report_sha256": sha256_file(Path(winner_receipt.tournament_report_path)),
            "test_report_sha256": sha256_file(test_path),
            "repository_commit": train_request.repository_commit,
            "files": files,
        }
        bundle_id = canonical_json_sha256(manifest_without_id)
        manifest = {"bundle_id": bundle_id, **manifest_without_id}
        _write_json(temporary / "manifest.json", manifest)
        _read_and_validate_bundle(
            temporary,
            expected_bundle_id=bundle_id,
            expected_manifest_file_sha256=sha256_file(temporary / "manifest.json"),
        )
        target = bundles_root / bundle_id
        if target.exists():
            existing = _read_and_validate_bundle(
                target,
                expected_bundle_id=bundle_id,
                expected_manifest_file_sha256=sha256_file(target / "manifest.json"),
            )[0]
            if existing != manifest:
                raise AdvisoryModelFirstError(
                    "existing M5A bundle identity has different content",
                    reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
                )
            shutil.rmtree(temporary)
            return bundle_id, target, manifest
        os.replace(temporary, target)
        return bundle_id, target, manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "M5A bundle authority JSON cannot be read",
            reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
            context={"path": str(path), "error_type": type(exc).__name__},
        ) from exc
    if not isinstance(payload, dict):
        raise AdvisoryModelFirstError(
            "M5A bundle authority JSON is not an object",
            reason_code="ADVISORY_M5_BUNDLE_INCOMPLETE",
            context={"path": str(path)},
        )
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
