from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from backend.services.industry_pit.artifact_store import (
    read_candidate_bundle,
    require_repo_external_root,
    write_candidate_bundle,
)
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
    TaxonomyIdentity,
    UnavailableReason,
    make_candidate_interval,
)


SOURCE = "a" * 64
DENOMINATOR = "d" * 64
IDENTITY = TaxonomyIdentity("220000", "基础化工", "220300", "化学制品", "220315", "食品及饲料添加剂")


def _receipt(authority: AuthorityType) -> AuthorityReceipt:
    return AuthorityReceipt(
        authority_type=authority,
        authority_schema=(
            CLASSIFICATION_CANDIDATE_SCHEMA
            if authority is AuthorityType.CLASSIFICATION
            else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
        ),
        authority_version=f"{authority.value}_v1",
        taxonomy_contract_id="sw2021_classification_catalog_v1",
        taxonomy_version="SW2021",
        knowledge_time_policy=KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        source_ids=("test:source",),
        source_hashes=(SOURCE,),
        frozen_denominator=1,
        denominator_digest=DENOMINATOR,
    )


def _row(receipt: AuthorityReceipt, *, resolved: bool):
    return make_candidate_interval(
        canonical_symbol="300741.SZ",
        authority_type=receipt.authority_type,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        valid_from=date(2021, 7, 30),
        valid_to_exclusive=None,
        eligible_from=date(2021, 7, 30),
        eligible_to_exclusive=date(2021, 8, 3),
        causal_use_from=date(2021, 8, 2),
        causal_use_to_exclusive=None,
        known_from=date(2021, 8, 2),
        source_effective_field=(
            "计入日期" if receipt.authority_type is AuthorityType.CLASSIFICATION else "membership_enter_date"
        ),
        source_last_updated_at=None,
        research_basis=receipt.research_basis,
        non_as_known_taxonomy=False,
        identity=IDENTITY if resolved else None,
        authority_identity=(
            {
                "classification_l1_code": IDENTITY.l1_code,
                "classification_l2_code": IDENTITY.l2_code,
                "classification_l3_code": IDENTITY.l3_code,
            }
            if resolved
            else {}
        ),
        unavailable_reason=None if resolved else UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE,
        source_ids=receipt.source_ids,
        source_hashes=(SOURCE,),
        lineage_hashes=(SOURCE,),
    )


def _catalog():
    return {
        "schema_version": "sw2021_taxonomy_catalog_v1",
        "contract_id": "sw2021_classification_catalog_v1",
        "version": "SW2021",
        "source_sha256": SOURCE,
        "identities": {"220315": IDENTITY.as_dict()},
        "catalog_hash": "c" * 64,
    }


def _report():
    return {
        "schema_version": "industry_pit_full_denominator_preflight_v1",
        "classification": {"resolved": 1, "unavailable": 0},
        "index_membership": {"resolved": 0, "unavailable": 1},
        "unavailable_by_reason": {
            "sw_industry_index_membership_pit:membership_boundary_unavailable": 1
        },
        "canonical_hash": "e" * 64,
    }


def test_writer_readback_uses_one_schema_and_preserves_separate_hashes(tmp_path: Path) -> None:
    forbidden = tmp_path / "repo"
    forbidden.mkdir()
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    readback = write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(forbidden,),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    assert readback.artifact_root == target.resolve()
    assert readback.manifest["classification_candidate_hash"] != readback.manifest[
        "index_membership_candidate_hash"
    ]
    assert readback.classification_receipt.receipt_hash == classification_receipt.receipt_hash
    assert readback.index_membership_receipt.receipt_hash == index_receipt.receipt_hash


def test_tamper_is_typed_writer_readback_hash_mismatch(tmp_path: Path) -> None:
    target = tmp_path / "artifacts" / "candidate"
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    write_candidate_bundle(
        artifact_root=target,
        forbidden_roots=(tmp_path / "repo",),
        taxonomy_catalog=_catalog(),
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=(_row(classification_receipt, resolved=True),),
        index_membership_intervals=(_row(index_receipt, resolved=False),),
        preflight_report=_report(),
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    with (target / "classification_candidate.jsonl").open("ab") as handle:
        handle.write(b" ")
    with pytest.raises(IndustryPitContractError, match="writer_readback_hash_mismatch"):
        read_candidate_bundle(artifact_root=target, forbidden_roots=(tmp_path / "repo",))


def test_writer_refuses_repo_root_overwrite_and_non_finite_json(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(IndustryPitContractError, match="repo-external"):
        require_repo_external_root(repo / "candidate", forbidden_roots=(repo,))

    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    report = dict(_report())
    report["invalid"] = float("nan")
    with pytest.raises(Exception, match="non-finite"):
        write_candidate_bundle(
            artifact_root=tmp_path / "artifacts" / "nan-candidate",
            forbidden_roots=(repo,),
            taxonomy_catalog=_catalog(),
            classification_receipt=classification_receipt,
            index_membership_receipt=index_receipt,
            classification_intervals=(_row(classification_receipt, resolved=True),),
            index_membership_intervals=(_row(index_receipt, resolved=False),),
            preflight_report=report,
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )
