from __future__ import annotations

from itertools import combinations

from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1


def p0_reference(role: str) -> dict[str, object]:
    values = {
        "P0D_V2_REFERENCE": (
            "a",
            "b",
            "ARM_P0D_V2_BINARY_PARITY",
            "FAMILY_POLICY_UTILITY_CORE_HMM",
            20260813,
            "BINARY_TAKE_SKIP_PARITY_V2",
        ),
        "P0F_V2_REFERENCE": (
            "c",
            "d",
            "ARM_P0F_V2_HUBER_UTILITY",
            "FAMILY_POLICY_UTILITY_CORE",
            20260817,
            "HUBER_CONTINUOUS_POLICY_NET_EXCESS_V2",
        ),
        "P0G_V1_REFERENCE": (
            "e",
            "f",
            "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
            "FAMILY_TURNOVER_CONSTRAINED_CORE",
            20260817,
            "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1",
        ),
        "P0H_V1_REFERENCE": (
            "1",
            "2",
            "ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
            "FAMILY_DUAL_HEAD_CORE_HMM",
            20260823,
            "P0H_DUAL_HEAD_OUTPUT_CONSTRAINT_V1",
        ),
    }
    bundle, manifest, arm, family, seed, objective = values[role]
    return {
        "role": role,
        "bundle_root": f"/models/{role.lower()}",
        "bundle_id": bundle * 64,
        "manifest_file_sha256": manifest * 64,
        "arm_id": arm,
        "winner_family_id": family,
        "winner_seed": seed,
        "winner_training_objective": objective,
        "winner_boost_rounds": 2 if role == "P0D_V2_REFERENCE" else 17,
    }


def eight_block_cpcv_paths(dates: list[str]) -> list[dict[str, object]]:
    return [
        {
            "path_id": f"path-{index:02d}",
            "status": "READY",
            "train_dates": [day for offset, day in enumerate(dates) if offset not in validation],
            "validation_dates": [day for offset, day in enumerate(dates) if offset in validation],
        }
        for index, validation in enumerate(combinations(range(8), 2))
    ]


def margin_evidence_refs() -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_margin_generator_manifest",
        "n3_margin_generator_receipt",
        "n3_margin_n2b_manifest",
        "n3_margin_n2b_request",
        "n3_margin_n2b_outcomes",
        "n3_margin_n1_manifest",
        "n3_margin_n1_cpcv",
        "n3_margin_n1_regime_daily",
        "n3_margin_source_manifest",
        "n3_margin_source_receipt",
        "n3_margin_source_projection",
        "n3_margin_source_coverage",
        "n3_margin_cross_snapshot_parity",
        "n3_margin_candidate_state_snapshot",
    )
    return tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/evidence-{index}",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(roles)
    )
