from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    OnboardingArtifactRef,
    PackageInventoryCandidate,
    PackageClosureStatus,
    PortableRelationRowSet,
    RealDevOnboardingRequest,
    SourceFactEligibility,
    serialize_postgres_value,
)
from backend.services.advisory_phase1.phase1g_contract import Phase1GInputArtifactKind


SHA_A = "a" * 64
SHA_B = "b" * 64


def _request_payload(request) -> dict:
    return request.model_dump(mode="python", exclude={"request_hash"})


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda p: p.update(source_package_ids=("pkg_single", "pkg_single")), "source_package_ids"),
        (lambda p: p.update(source_program_refs=("same", "same")), "source_program_refs"),
        (lambda p: p.update(required_alpha_modes=(AlphaMode.SINGLE,)), "required_alpha_modes"),
        (lambda p: p.update(expected_program_packages={"dev_single": "pkg_single"}), "expected_program_packages"),
        (lambda p: p.update(expected_package_manifest_sha256s={"pkg_single": SHA_A}), "manifest map"),
        (lambda p: p.update(decision_trade_date=date(2026, 7, 19)), "binding interval"),
    ],
)
def test_request_rejects_incomplete_or_ambiguous_inputs(onboarding_request, mutator, message) -> None:
    payload = _request_payload(onboarding_request)
    mutator(payload)
    with pytest.raises(ValidationError, match=message):
        RealDevOnboardingRequest.model_validate(payload)


def test_request_rejects_wrong_release_artifact_kind(onboarding_request) -> None:
    payload = _request_payload(onboarding_request)
    payload["release_receipt_ref"] = onboarding_request.release_receipt_ref.model_copy(
        update={"artifact_kind": Phase1GInputArtifactKind.PHASE1E_EXECUTION_PLAN}
    )
    with pytest.raises(ValidationError, match="Phase 1F.2 release receipt"):
        RealDevOnboardingRequest.model_validate(payload)


def test_request_rejects_wrong_release_store_policy(onboarding_request) -> None:
    payload = _request_payload(onboarding_request)
    payload["release_receipt_ref"] = onboarding_request.release_receipt_ref.model_copy(
        update={"store_policy_hash": SHA_A}
    )
    with pytest.raises(ValidationError, match="registered Phase 1F.2 policy"):
        RealDevOnboardingRequest.model_validate(payload)


def test_request_rejects_hash_tamper(onboarding_request) -> None:
    payload = onboarding_request.model_dump(mode="python")
    payload["request_hash"] = SHA_A
    with pytest.raises(ValidationError, match="does not match canonical payload"):
        RealDevOnboardingRequest.model_validate(payload)


@pytest.mark.parametrize("path", ("/absolute.json", "C:/absolute.json", "../escape.json"))
def test_evidence_ref_rejects_non_contained_path(path: str) -> None:
    with pytest.raises(ValidationError, match="contained relative path"):
        OnboardingArtifactRef(
            evidence_kind="request",
            relative_path=path,
            semantic_content_hash=SHA_A,
            file_sha256=SHA_B,
        )


def _component(alpha_id: str) -> AlphaComponentEvidence:
    return AlphaComponentEvidence(
        alpha_id=alpha_id,
        alpha_name=f"Alpha {alpha_id}",
        component_weight=1.0,
        holding_period="5d",
        rebalance_frequency="1d",
        score_direction="higher_better",
        score_normalization="rank",
        factor_ids=("factor",),
    )


def _candidate_payload() -> dict:
    return {
        "package_id": "pkg",
        "manifest_sha256": SHA_A,
        "alpha_mode": AlphaMode.SINGLE,
        "package_status": "SELECTION_ENABLED",
        "components": (_component("one"),),
        "package_asset_count": 1,
        "has_runtime_assets": True,
        "has_source_evidence": True,
        "closure_status": PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED,
        "binding_fact_eligibility": SourceFactEligibility.MISSING,
        "dse_fact_eligibility": SourceFactEligibility.MISSING,
        "package_eligible": True,
    }


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"components": (_component("one"), _component("one"))}, "unique alpha ids"),
        ({"components": (_component("one"), _component("two"))}, "eligible single Alpha"),
        ({"alpha_mode": AlphaMode.MULTI}, "eligible native multi"),
        ({"dse_schema_counts": {"v1": -1}}, "non-negative"),
        ({"reason_codes": ("unexpected",)}, "eligible package"),
        ({"package_eligible": False}, "ineligible package"),
        ({"source_program_refs": ("same", "same")}, "source_program_refs"),
    ],
)
def test_package_candidate_rejects_invalid_closure(update: dict, message: str) -> None:
    payload = _candidate_payload()
    payload.update(update)
    with pytest.raises(ValidationError, match=message):
        PackageInventoryCandidate.model_validate(payload)


def _asset_row_set_payload() -> dict:
    return {
        "relation_name": "strategy_pkg.package_asset",
        "primary_or_natural_key_fields": ("package_id", "asset_type", "asset_ref"),
        "semantic_column_names": ("package_id", "asset_type", "asset_ref", "asset_sha256"),
        "source_provenance_column_names": ("asset_id",),
        "sorted_rows": (
            {
                "asset_id": 1,
                "package_id": "pkg",
                "asset_type": "MODEL",
                "asset_ref": "model.bin",
                "asset_sha256": SHA_A,
            },
        ),
    }


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda p: p.update(source_provenance_column_names=("asset_id", "asset_ref")), "disjoint"),
        (lambda p: p.update(primary_or_natural_key_fields=("missing",)), "natural key"),
        (lambda p: p.update(source_provenance_column_names=()), "asset_id"),
        (
            lambda p: p.update(
                sorted_rows=(
                    {
                        "asset_id": 1,
                        "package_id": "pkg",
                        "asset_type": "MODEL",
                        "asset_ref": "model.bin",
                    },
                )
            ),
            "row columns",
        ),
        (
            lambda p: p.update(sorted_rows=p["sorted_rows"] + ({**p["sorted_rows"][0], "asset_id": 2},)),
            "duplicate natural key",
        ),
        (lambda p: p.update(column_contract_hash=SHA_A), "column_contract_hash"),
        (lambda p: p.update(row_content_hashes=(SHA_A,)), "row_content_hashes"),
        (lambda p: p.update(row_set_hash=SHA_A), "row_set_hash"),
    ],
)
def test_relation_row_set_rejects_contract_drift(mutator, message: str) -> None:
    payload = _asset_row_set_payload()
    mutator(payload)
    with pytest.raises(ValidationError, match=message):
        PortableRelationRowSet.model_validate(payload)


def test_postgres_serializer_rejects_unknown_and_invalid_envelopes() -> None:
    with pytest.raises(ValueError, match="unsupported PostgreSQL value type"):
        serialize_postgres_value(object())
    with pytest.raises(ValueError, match="invalid typed PostgreSQL value envelope"):
        serialize_postgres_value({"type": "date", "wrong": "2026-07-16"})
    with pytest.raises(ValueError):
        serialize_postgres_value({"type": "date", "value": "not-a-date"})
