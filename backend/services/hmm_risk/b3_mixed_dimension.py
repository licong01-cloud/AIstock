from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from hashlib import sha256
from typing import Any

import numpy as np

from backend.services.hmm_risk.state_model_set import (
    ALL_CORE_FEATURES,
    StateModelSetError,
    _apply_preprocess,
    canonical_json_bytes,
    canonical_sha256,
)


MIXED_DIMENSION_CONTRACT_VERSION = "hmm_risk_c008_b3_d1_d5_compat_a_v1"
MIXED_LEVEL_SCHEMA_VERSION = "hmm_risk_b3_level_model_set_v2_projection"
MIXED_REPEAT_SCHEMA_VERSION = "hmm_risk_b3_level_repeat_receipt_v2_projection"
MIXED_TRAINING_ENTRY_SCHEMA_VERSION = "hmm_risk_b3_training_entry_receipt_v2_projection"
MIXED_MODEL_SCHEMA_VERSION = "hmm_risk_b3_inactive_dimension_model_entry_v1"
PROJECTION_RECEIPT_SCHEMA_VERSION = "hmm_risk_b3_dimension_projection_receipt_v2"
PROJECTION_ALGORITHM_VERSION = "hmm_risk_b3_fixed_column_projection_v2"
TARGET_FAMILY = "autocycle_all_core"
TARGET_LEVEL = "L2"
TARGET_SECTOR = "801207.SI"
TARGET_INACTIVE_FEATURE_INDICES = (19,)
TARGET_SOURCE_PROFILE_RECEIPT_SHA256 = "36cc1afd004796ce3458ab7090010abd07ddd94807d2701318e39d6d80f84e3d"
INACTIVE_DIMENSION_REASON_CODE = "hmm_risk_model_inactive_dimension_contract_invalid"


def uses_mixed_dimension_level(family: str, level: str) -> bool:
    return family == TARGET_FAMILY and level == TARGET_LEVEL


def expected_active_feature_indices(
    *, family: str, level: str, sector_code: str, full_feature_names: Sequence[str]
) -> tuple[int, ...]:
    names = tuple(str(value) for value in full_feature_names)
    if uses_mixed_dimension_level(family, level):
        if names != ALL_CORE_FEATURES:
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        if sector_code == TARGET_SECTOR:
            if any(index < 0 or index >= len(names) for index in TARGET_INACTIVE_FEATURE_INDICES):
                raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
            return tuple(index for index in range(len(names)) if index not in TARGET_INACTIVE_FEATURE_INDICES)
    return tuple(range(len(names)))


def _float64_sha256(value: Any) -> str:
    array = np.ascontiguousarray(np.asarray(value, dtype="<f8"))
    header = canonical_json_bytes({"dtype": "float64_le", "shape": list(array.shape)})
    digest = sha256()
    digest.update(len(header).to_bytes(8, "big"))
    digest.update(header)
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def _formal_source_profile_identity(
    *,
    family: str,
    level: str,
    sector_code: str,
    full_feature_names_sha256: str,
    full_preprocess_sha256: str,
    train_input_manifest_sha256: str,
) -> str:
    return canonical_sha256(
        {
            "schema_version": "hmm_risk_b3_projection_source_profile_identity_v1",
            "contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
            "family": family,
            "level": level,
            "sector_code": sector_code,
            "full_feature_names_sha256": full_feature_names_sha256,
            "full_preprocess_sha256": full_preprocess_sha256,
            "train_input_manifest_sha256": train_input_manifest_sha256,
        }
    )


def _exact_zero_evidence(
    raw: np.ndarray,
    preprocessed: np.ndarray,
    expected_preprocessed: np.ndarray,
) -> dict[str, Any]:
    row_count = int(raw.shape[0])
    feature_count = int(raw.shape[1])
    raw_values = np.ascontiguousarray(raw, dtype="<f8")
    preprocessed_values = np.ascontiguousarray(preprocessed, dtype="<f8")

    def summarize(values: np.ndarray) -> dict[str, Any]:
        normalized = np.ascontiguousarray(np.where(values == 0.0, 0.0, values), dtype="<f8")
        return {
            "variance_ddof0_by_feature": (
                np.var(values, axis=0, ddof=0).astype(np.float64).tolist() if feature_count else []
            ),
            "normalized_unique_bit_pattern_count_by_feature": [
                int(np.unique(normalized[:, index].view("<u8")).size) for index in range(feature_count)
            ],
            "zero_count": int(np.count_nonzero(values == 0.0)),
            "positive_zero_count": int(np.count_nonzero((values == 0.0) & ~np.signbit(values))),
            "negative_zero_count": int(np.count_nonzero((values == 0.0) & np.signbit(values))),
            "all_values_zero": bool(np.all(values == 0.0)),
        }

    expected_values = np.ascontiguousarray(expected_preprocessed, dtype="<f8")
    expected_sha256 = _float64_sha256(expected_values)
    observed_sha256 = _float64_sha256(preprocessed_values)
    body = {
        "schema_version": "hmm_risk_b3_inactive_exact_zero_evidence_v2",
        "status": "accepted" if feature_count else "not_applicable_identity_projection",
        "observation_rows": row_count,
        "inactive_feature_count": feature_count,
        "raw_exact_zero_required": bool(feature_count),
        "raw_exact_zero": bool(not feature_count or np.all(raw_values == 0.0)),
        "preprocessed_exact_zero_required": False,
        "preprocessed_matches_approved_transform": bool(np.array_equal(preprocessed_values, expected_values)),
        "expected_preprocessed_vector_sha256": expected_sha256,
        "observed_preprocessed_vector_sha256": observed_sha256,
        "raw": summarize(raw_values),
        "preprocessed": summarize(preprocessed_values),
    }
    return {**body, "exact_zero_evidence_sha256": canonical_sha256(body)}


def build_projection_receipt(
    *,
    family: str,
    level: str,
    sector_code: str,
    full_feature_names: Sequence[str],
    preprocess: Mapping[str, Any],
    raw_observations: Any,
    preprocessed_observations: Any,
    train_input_manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], np.ndarray]:
    names = tuple(str(value) for value in full_feature_names)
    raw = np.asarray(raw_observations, dtype=np.float64)
    preprocessed = np.asarray(preprocessed_observations, dtype=np.float64)
    if (
        not names
        or raw.ndim != 2
        or preprocessed.shape != raw.shape
        or raw.shape[1] != len(names)
        or not np.isfinite(raw).all()
        or not np.isfinite(preprocessed).all()
    ):
        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    active = expected_active_feature_indices(
        family=family,
        level=level,
        sector_code=sector_code,
        full_feature_names=names,
    )
    inactive = tuple(index for index in range(len(names)) if index not in active)
    mask = [index in active for index in range(len(names))]
    expected_preprocessed = _apply_preprocess(raw, preprocess)
    if not np.array_equal(preprocessed, expected_preprocessed):
        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    raw_inactive = raw[:, inactive] if inactive else np.empty((raw.shape[0], 0), dtype=np.float64)
    preprocessed_inactive = (
        preprocessed[:, inactive] if inactive else np.empty((preprocessed.shape[0], 0), dtype=np.float64)
    )
    expected_preprocessed_inactive = (
        expected_preprocessed[:, inactive]
        if inactive
        else np.empty((expected_preprocessed.shape[0], 0), dtype=np.float64)
    )
    exact_zero = bool(
        not inactive
        or (np.var(raw_inactive, axis=0, ddof=0).tolist() == [0.0] * len(inactive) and np.all(raw_inactive == 0.0))
    )
    if inactive and not exact_zero:
        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    projected = np.ascontiguousarray(preprocessed[:, active], dtype=np.float64)
    source_hashes = {
        field: train_input_manifest.get(field)
        for field in (
            "dataset_manifest_hash",
            "mapping_manifest_hash",
            "calendar_manifest_hash",
            "l2_stock_fact_manifest_hash",
            "feature_domain_policy_sha256",
        )
    }
    source_hashes["formula_version"] = train_input_manifest.get("formula_version")
    feature_names_sha256 = canonical_sha256(list(names))
    preprocess_sha256 = canonical_sha256(dict(preprocess))
    train_manifest_sha256 = canonical_sha256(dict(train_input_manifest))
    formal_source_profile_sha256 = _formal_source_profile_identity(
        family=family,
        level=level,
        sector_code=sector_code,
        full_feature_names_sha256=feature_names_sha256,
        full_preprocess_sha256=preprocess_sha256,
        train_input_manifest_sha256=train_manifest_sha256,
    )
    exact_zero_evidence = _exact_zero_evidence(
        raw_inactive,
        preprocessed_inactive,
        expected_preprocessed_inactive,
    )
    body = {
        "schema_version": PROJECTION_RECEIPT_SCHEMA_VERSION,
        "contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "projection_algorithm_version": PROJECTION_ALGORITHM_VERSION,
        "family": family,
        "level": level,
        "sector_code": sector_code,
        "full_feature_names": list(names),
        "full_feature_count": len(names),
        "full_feature_names_sha256": feature_names_sha256,
        "active_feature_names": [names[index] for index in active],
        "active_feature_indices": list(active),
        "inactive_feature_names": [names[index] for index in inactive],
        "inactive_feature_indices": list(inactive),
        "active_feature_mask": mask,
        "active_feature_mask_sha256": canonical_sha256(mask),
        "likelihood_feature_count": len(active),
        "dynamic_activation": False,
        "full_preprocess_sha256": preprocess_sha256,
        "raw_inactive_vector_sha256": _float64_sha256(raw_inactive),
        "preprocessed_inactive_vector_sha256": _float64_sha256(preprocessed_inactive),
        "inactive_exact_zero": exact_zero,
        "inactive_exact_zero_evidence": exact_zero_evidence,
        "source_profile_authority": (
            "c008_b3_d1_treatment_profile_receipt"
            if sector_code == TARGET_SECTOR
            else "formal_identity_projection_profile"
        ),
        "source_profile_receipt_sha256": (
            TARGET_SOURCE_PROFILE_RECEIPT_SHA256 if sector_code == TARGET_SECTOR else formal_source_profile_sha256
        ),
        "formal_source_profile_identity_sha256": formal_source_profile_sha256,
        "projected_matrix_shape": list(projected.shape),
        "projected_matrix_sha256": _float64_sha256(projected),
        "source_identities": source_hashes,
        "train_input_manifest_sha256": train_manifest_sha256,
    }
    receipt = {**body, "projection_sha256": canonical_sha256(body)}
    validate_projection_receipt(
        receipt,
        family=family,
        level=level,
        sector_code=sector_code,
        full_feature_names=names,
        preprocess=preprocess,
    )
    return receipt, projected


def validate_projection_receipt(
    receipt: Mapping[str, Any],
    *,
    family: str,
    level: str,
    sector_code: str,
    full_feature_names: Sequence[str],
    preprocess: Mapping[str, Any],
    means_shape: Sequence[int] | None = None,
    covariance_shape: Sequence[int] | None = None,
) -> int:
    names = tuple(str(value) for value in full_feature_names)
    active = expected_active_feature_indices(
        family=family,
        level=level,
        sector_code=sector_code,
        full_feature_names=names,
    )
    inactive = tuple(index for index in range(len(names)) if index not in active)
    mask = [index in active for index in range(len(names))]
    expected_hash = str(receipt.get("projection_sha256") or "")
    body = {key: value for key, value in receipt.items() if key != "projection_sha256"}
    source_identities = receipt.get("source_identities")
    valid_sources = (
        isinstance(source_identities, Mapping)
        and all(
            isinstance(source_identities.get(field), str)
            and len(source_identities[field]) == 64
            and source_identities[field] == source_identities[field].lower()
            and all(character in "0123456789abcdef" for character in source_identities[field].lower())
            for field in (
                "dataset_manifest_hash",
                "mapping_manifest_hash",
                "calendar_manifest_hash",
                "l2_stock_fact_manifest_hash",
                "feature_domain_policy_sha256",
            )
        )
        and isinstance(source_identities.get("formula_version"), str)
        and bool(source_identities["formula_version"])
    )
    projected_shape = receipt.get("projected_matrix_shape")
    exact_evidence = receipt.get("inactive_exact_zero_evidence")
    exact_evidence_valid = isinstance(exact_evidence, Mapping)
    if exact_evidence_valid:
        exact_body = {key: value for key, value in exact_evidence.items() if key != "exact_zero_evidence_sha256"}
        raw_evidence = exact_evidence.get("raw")
        preprocessed_evidence = exact_evidence.get("preprocessed")
        expected_cells = int(exact_evidence.get("observation_rows") or 0) * len(inactive)
        expected_variances = [0.0] * len(inactive)
        expected_unique_counts = [1] * len(inactive)
        expected_status = "accepted" if inactive else "not_applicable_identity_projection"
        exact_evidence_valid = (
            exact_evidence.get("schema_version") == "hmm_risk_b3_inactive_exact_zero_evidence_v2"
            and exact_evidence.get("status") == expected_status
            and isinstance(projected_shape, list)
            and exact_evidence.get("observation_rows") == projected_shape[0]
            and exact_evidence.get("inactive_feature_count") == len(inactive)
            and isinstance(raw_evidence, Mapping)
            and isinstance(preprocessed_evidence, Mapping)
            and exact_evidence.get("raw_exact_zero_required") is bool(inactive)
            and exact_evidence.get("raw_exact_zero") is True
            and exact_evidence.get("preprocessed_exact_zero_required") is False
            and exact_evidence.get("preprocessed_matches_approved_transform") is True
            and exact_evidence.get("expected_preprocessed_vector_sha256")
            == exact_evidence.get("observed_preprocessed_vector_sha256")
            and exact_evidence.get("observed_preprocessed_vector_sha256")
            == receipt.get("preprocessed_inactive_vector_sha256")
            and exact_evidence.get("exact_zero_evidence_sha256") == canonical_sha256(exact_body)
        )
        if isinstance(raw_evidence, Mapping):
            raw_variances = raw_evidence.get("variance_ddof0_by_feature")
            raw_unique_counts = raw_evidence.get("normalized_unique_bit_pattern_count_by_feature")
            raw_zero_count = raw_evidence.get("zero_count")
            raw_positive_zero_count = raw_evidence.get("positive_zero_count")
            raw_negative_zero_count = raw_evidence.get("negative_zero_count")
            exact_evidence_valid = exact_evidence_valid and (
                raw_variances == expected_variances
                and isinstance(raw_variances, list)
                and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in raw_variances)
                and raw_unique_counts == (expected_unique_counts if inactive else [])
                and isinstance(raw_unique_counts, list)
                and all(isinstance(value, int) and not isinstance(value, bool) for value in raw_unique_counts)
                and raw_zero_count == expected_cells
                and isinstance(raw_zero_count, int)
                and not isinstance(raw_zero_count, bool)
                and isinstance(raw_positive_zero_count, int)
                and not isinstance(raw_positive_zero_count, bool)
                and 0 <= raw_positive_zero_count <= expected_cells
                and isinstance(raw_negative_zero_count, int)
                and not isinstance(raw_negative_zero_count, bool)
                and 0 <= raw_negative_zero_count <= expected_cells
                and raw_positive_zero_count + raw_negative_zero_count == expected_cells
                and raw_evidence.get("all_values_zero") is True
            )
        if isinstance(preprocessed_evidence, Mapping):
            preprocessed_variances = preprocessed_evidence.get("variance_ddof0_by_feature")
            preprocessed_unique_counts = preprocessed_evidence.get("normalized_unique_bit_pattern_count_by_feature")
            preprocessed_zero_count = preprocessed_evidence.get("zero_count")
            preprocessed_positive_zero_count = preprocessed_evidence.get("positive_zero_count")
            preprocessed_negative_zero_count = preprocessed_evidence.get("negative_zero_count")
            exact_evidence_valid = exact_evidence_valid and (
                isinstance(preprocessed_variances, list)
                and len(preprocessed_variances) == len(inactive)
                and all(
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and np.isfinite(value)
                    and value >= 0.0
                    for value in preprocessed_variances
                )
                and isinstance(preprocessed_unique_counts, list)
                and len(preprocessed_unique_counts) == len(inactive)
                and all(
                    isinstance(value, int) and not isinstance(value, bool) and value >= 1
                    for value in preprocessed_unique_counts
                )
                and isinstance(preprocessed_zero_count, int)
                and not isinstance(preprocessed_zero_count, bool)
                and 0 <= preprocessed_zero_count <= expected_cells
                and isinstance(preprocessed_positive_zero_count, int)
                and not isinstance(preprocessed_positive_zero_count, bool)
                and 0 <= preprocessed_positive_zero_count <= expected_cells
                and isinstance(preprocessed_negative_zero_count, int)
                and not isinstance(preprocessed_negative_zero_count, bool)
                and 0 <= preprocessed_negative_zero_count <= expected_cells
                and preprocessed_positive_zero_count + preprocessed_negative_zero_count == preprocessed_zero_count
                and isinstance(preprocessed_evidence.get("all_values_zero"), bool)
            )
    formal_source_profile_sha256 = _formal_source_profile_identity(
        family=family,
        level=level,
        sector_code=sector_code,
        full_feature_names_sha256=str(receipt.get("full_feature_names_sha256") or ""),
        full_preprocess_sha256=str(receipt.get("full_preprocess_sha256") or ""),
        train_input_manifest_sha256=str(receipt.get("train_input_manifest_sha256") or ""),
    )
    expected_source_profile_sha256 = (
        TARGET_SOURCE_PROFILE_RECEIPT_SHA256 if sector_code == TARGET_SECTOR else formal_source_profile_sha256
    )
    expected_source_profile_authority = (
        "c008_b3_d1_treatment_profile_receipt" if sector_code == TARGET_SECTOR else "formal_identity_projection_profile"
    )
    valid = (
        receipt.get("schema_version") == PROJECTION_RECEIPT_SCHEMA_VERSION
        and receipt.get("contract_version") == MIXED_DIMENSION_CONTRACT_VERSION
        and receipt.get("projection_algorithm_version") == PROJECTION_ALGORITHM_VERSION
        and receipt.get("family") == family
        and receipt.get("level") == level
        and receipt.get("sector_code") == sector_code
        and tuple(receipt.get("full_feature_names") or ()) == names
        and receipt.get("full_feature_count") == len(names)
        and receipt.get("full_feature_names_sha256") == canonical_sha256(list(names))
        and tuple(receipt.get("active_feature_indices") or ()) == active
        and tuple(receipt.get("inactive_feature_indices") or ()) == inactive
        and tuple(receipt.get("active_feature_names") or ()) == tuple(names[index] for index in active)
        and tuple(receipt.get("inactive_feature_names") or ()) == tuple(names[index] for index in inactive)
        and receipt.get("active_feature_mask") == mask
        and receipt.get("active_feature_mask_sha256") == canonical_sha256(mask)
        and receipt.get("likelihood_feature_count") == len(active)
        and receipt.get("dynamic_activation") is False
        and receipt.get("inactive_exact_zero") is True
        and exact_evidence_valid
        and receipt.get("source_profile_authority") == expected_source_profile_authority
        and receipt.get("source_profile_receipt_sha256") == expected_source_profile_sha256
        and receipt.get("formal_source_profile_identity_sha256") == formal_source_profile_sha256
        and receipt.get("full_preprocess_sha256") == canonical_sha256(dict(preprocess))
        and isinstance(projected_shape, list)
        and len(projected_shape) == 2
        and isinstance(projected_shape[0], int)
        and not isinstance(projected_shape[0], bool)
        and projected_shape[0] > 0
        and projected_shape[1] == len(active)
        and all(
            len(str(receipt.get(field) or "")) == 64
            and all(character in "0123456789abcdef" for character in str(receipt[field]).lower())
            for field in (
                "full_preprocess_sha256",
                "raw_inactive_vector_sha256",
                "preprocessed_inactive_vector_sha256",
                "projected_matrix_sha256",
                "train_input_manifest_sha256",
            )
        )
        and valid_sources
        and len(expected_hash) == 64
        and canonical_sha256(body) == expected_hash
    )
    if means_shape is not None:
        valid = valid and tuple(means_shape) == (3, len(active))
    if covariance_shape is not None:
        valid = valid and tuple(covariance_shape) == (3, len(active))
    if not valid:
        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    return len(active)


def build_level_dimension_identity(
    entries: Sequence[Mapping[str, Any]], *, family: str, level: str, expected_sector_codes: Sequence[str]
) -> dict[str, Any]:
    codes = tuple(sorted(str(value) for value in expected_sector_codes))
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in sorted(entries, key=lambda value: str(value.get("sector_code") or "")):
        code = str(entry.get("sector_code") or "")
        if code in seen:
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        seen.add(code)
        projection = entry.get("projection_receipt")
        if not isinstance(projection, Mapping):
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        count = validate_projection_receipt(
            projection,
            family=family,
            level=level,
            sector_code=code,
            full_feature_names=entry.get("feature_names") or (),
            preprocess=entry.get("preprocess") or {},
            means_shape=np.asarray(entry.get("means"), dtype=np.float64).shape,
            covariance_shape=np.asarray(entry.get("covars"), dtype=np.float64).shape,
        )
        model_hash = str(entry.get("model_payload_sha256") or "")
        if (
            len(model_hash) != 64
            or model_hash != model_hash.lower()
            or any(character not in "0123456789abcdef" for character in model_hash)
        ):
            raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
        ordered.append(
            {
                "sector_code": code,
                "model_entry_sha256": model_hash,
                "projection_sha256": projection["projection_sha256"],
                "likelihood_feature_count": count,
            }
        )
    if tuple(item["sector_code"] for item in ordered) != codes:
        raise StateModelSetError("hmm_risk_model_selection_level_incomplete")
    histogram = dict(sorted(Counter(str(item["likelihood_feature_count"]) for item in ordered).items()))
    if uses_mixed_dimension_level(family, level) and histogram != {"19": 1, "20": 130}:
        raise StateModelSetError(INACTIVE_DIMENSION_REASON_CODE)
    return {
        "schema_version": MIXED_LEVEL_SCHEMA_VERSION,
        "contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "feature_count": len(ALL_CORE_FEATURES),
        "likelihood_feature_count_histogram": histogram,
        "ordered_entry_dimension_identities": ordered,
        "ordered_entry_dimension_identities_sha256": canonical_sha256(ordered),
    }
