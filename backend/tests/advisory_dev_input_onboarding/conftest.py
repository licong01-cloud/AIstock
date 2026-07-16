from __future__ import annotations

from datetime import date

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaMode,
    EvidenceKind,
    OnboardingArtifactRef,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    TargetDevProgramSpec,
)
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
    Phase1GInputArtifactRef,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def release_ref() -> Phase1GInputArtifactRef:
    return Phase1GInputArtifactRef(
        artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
        store_policy_hash=PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash,
        relative_path=f"receipts/{SHA_D}.json",
        semantic_content_hash=SHA_D,
        file_sha256=SHA_C,
    )


@pytest.fixture
def onboarding_request() -> RealDevOnboardingRequest:
    return RealDevOnboardingRequest(
        source_program_refs=("prod_multi", "prod_single"),
        source_package_ids=("pkg_single", "pkg_multi"),
        target_dev_program_specs=(
            TargetDevProgramSpec(
                program_id="dev_multi",
                package_id="pkg_multi",
                alpha_mode=AlphaMode.MULTI,
                target_count=5,
                review_policy={"mode": "manual_research_review"},
                style="oversold_rebound",
            ),
            TargetDevProgramSpec(
                program_id="dev_single",
                package_id="pkg_single",
                alpha_mode=AlphaMode.SINGLE,
                target_count=5,
                review_policy={"mode": "manual_research_review"},
                style="trend_following",
            ),
        ),
        binding_effective_from_trade_date=date(2026, 7, 20),
        decision_trade_date=date(2026, 7, 21),
        expected_program_packages={"dev_single": "pkg_single", "dev_multi": "pkg_multi"},
        expected_package_manifest_sha256s={"pkg_single": SHA_A, "pkg_multi": SHA_B},
        policy_registry_id="advisory_default",
        policy_registry_version="v1",
        policy_registry_hash=SHA_C,
        release_receipt_ref=release_ref(),
    )


@pytest.fixture
def onboarding_request_ref(onboarding_request: RealDevOnboardingRequest) -> OnboardingArtifactRef:
    return OnboardingArtifactRef(
        evidence_kind=EvidenceKind.REQUEST,
        relative_path=f"requests/{onboarding_request.request_hash[:2]}/{onboarding_request.request_hash}.json",
        semantic_content_hash=onboarding_request.request_hash,
        file_sha256=SHA_C,
    )


@pytest.fixture
def onboarding_inventory_query(onboarding_request: RealDevOnboardingRequest) -> RealDevOnboardingInventoryQuery:
    return RealDevOnboardingInventoryQuery(
        source_program_refs=onboarding_request.source_program_refs,
        source_package_ids=onboarding_request.source_package_ids,
        target_dev_program_specs=onboarding_request.target_dev_program_specs,
        binding_effective_from_trade_date=onboarding_request.binding_effective_from_trade_date,
        decision_trade_date=onboarding_request.decision_trade_date,
        release_receipt_ref=onboarding_request.release_receipt_ref,
    )


@pytest.fixture
def onboarding_inventory_query_ref(
    onboarding_inventory_query: RealDevOnboardingInventoryQuery,
) -> OnboardingArtifactRef:
    identity = onboarding_inventory_query.inventory_query_hash
    return OnboardingArtifactRef(
        evidence_kind=EvidenceKind.INVENTORY_QUERY,
        relative_path=f"inventory-queries/{identity[:2]}/{identity}.json",
        semantic_content_hash=identity,
        file_sha256=SHA_C,
    )
