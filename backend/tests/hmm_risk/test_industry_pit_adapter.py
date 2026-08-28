from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.industry_pit.artifact_store import CandidateBundleReadback
from backend.services.industry_pit.candidate_builder import FrozenDenominator, UniverseSpan
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    KnowledgeTimePolicy,
    ResearchBasis,
    TaxonomyIdentity,
    UnavailableReason,
    make_candidate_interval,
)
from backend.services.hmm_risk.industry_pit_adapter import (
    HMMIndustryPitAdapter,
    build_l1_code_projection_authority,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError
from backend.services.hmm_risk.stock_fact_repository import PostgresStockFactReader


HASH = "a" * 64
DENOMINATOR = "d" * 64


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
        source_hashes=(HASH,),
        frozen_denominator=131,
        denominator_digest=DENOMINATOR,
    )


def _identity(l1_number: int, l2_number: int) -> TaxonomyIdentity:
    return TaxonomyIdentity(
        l1_code=f"{l1_number:02d}0000",
        l1_name=f"L1-{l1_number:02d}",
        l2_code=f"{l1_number:02d}{l2_number:02d}00",
        l2_name=f"L2-{l1_number:02d}-{l2_number:02d}",
        l3_code=f"{l1_number:02d}{l2_number:02d}01",
        l3_name=f"L3-{l1_number:02d}-{l2_number:02d}",
    )


def _resolved_interval(
    *,
    symbol: str,
    authority: AuthorityType,
    receipt: AuthorityReceipt,
    identity: TaxonomyIdentity,
    l1_code: str,
    l2_code: str,
):
    prefix = "classification" if authority is AuthorityType.CLASSIFICATION else "index"
    return make_candidate_interval(
        canonical_symbol=symbol,
        authority_type=authority,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        valid_from=date(2022, 1, 1),
        valid_to_exclusive=None,
        eligible_from=date(2022, 1, 1),
        eligible_to_exclusive=date(2022, 1, 5),
        causal_use_from=date(2022, 1, 1),
        causal_use_to_exclusive=None,
        known_from=date(2022, 1, 1),
        source_effective_field="test_date",
        source_last_updated_at=None,
        research_basis=receipt.research_basis,
        non_as_known_taxonomy=False,
        identity=identity,
        authority_identity={
            f"{prefix}_l1_code": l1_code,
            f"{prefix}_l2_code": l2_code,
            f"{prefix}_l3_code": identity.l3_code,
        },
        unavailable_reason=None,
        source_ids=receipt.source_ids,
        source_hashes=(HASH,),
        lineage_hashes=(HASH,),
    )


def _bundle(tmp_path: Path, *, target_unavailable: bool = False, drop_last_l1: bool = False):
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    classification = []
    index = []
    ordinal = 0
    target_symbol = "000001.SZ"
    for l1_number in range(1, 32):
        if drop_last_l1 and l1_number == 31:
            continue
        l2_count = 5 if l1_number <= 7 else 4
        l1_index = f"801{l1_number:03d}"
        for l2_number in range(1, l2_count + 1):
            ordinal += 1
            symbol = target_symbol if ordinal == 1 else f"{ordinal:06d}.SZ"
            identity = _identity(l1_number, l2_number)
            l2_index = f"802{ordinal:03d}"
            index.append(
                _resolved_interval(
                    symbol=symbol,
                    authority=AuthorityType.INDEX_MEMBERSHIP,
                    receipt=index_receipt,
                    identity=identity,
                    l1_code=l1_index,
                    l2_code=l2_index,
                )
            )
            if ordinal == 1 and target_unavailable:
                classification.append(
                    make_candidate_interval(
                        canonical_symbol=symbol,
                        authority_type=AuthorityType.CLASSIFICATION,
                        taxonomy_contract_id=classification_receipt.taxonomy_contract_id,
                        taxonomy_version=classification_receipt.taxonomy_version,
                        authority_receipt_hash=classification_receipt.receipt_hash,
                        valid_from=date(2022, 1, 1),
                        valid_to_exclusive=None,
                        eligible_from=date(2022, 1, 1),
                        eligible_to_exclusive=date(2022, 1, 5),
                        causal_use_from=date(2022, 1, 1),
                        causal_use_to_exclusive=None,
                        known_from=date(2022, 1, 1),
                        source_effective_field="test_date",
                        source_last_updated_at=None,
                        research_basis=classification_receipt.research_basis,
                        non_as_known_taxonomy=False,
                        identity=None,
                        authority_identity={},
                        unavailable_reason=UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE,
                        source_ids=classification_receipt.source_ids,
                        source_hashes=(HASH,),
                        lineage_hashes=(HASH,),
                    )
                )
            else:
                classification.append(
                    _resolved_interval(
                        symbol=symbol,
                        authority=AuthorityType.CLASSIFICATION,
                        receipt=classification_receipt,
                        identity=identity,
                        l1_code=identity.l1_code,
                        l2_code=identity.l2_code,
                    )
                )
    return CandidateBundleReadback(
        artifact_root=tmp_path / "candidate",
        manifest={
            "bundle_hash": "b" * 64,
            "classification_candidate_hash": "c" * 64,
            "index_membership_candidate_hash": "e" * 64,
        },
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=tuple(classification),
        index_membership_intervals=tuple(index),
        preflight_report={"canonical_hash": "f" * 64},
    )


def _bind(adapter: HMMIndustryPitAdapter) -> None:
    adapter.bind_l1_code_projection(
        build_l1_code_projection_authority(
            taxonomy_contract_id="sw2021_classification_catalog_v1",
            taxonomy_version="SW2021",
            projection_version="sw2021_taxonomy_to_published_l1_v1",
            taxonomy_rows=[
                {"industry_code": f"{number:02d}0000", "industry_name": f"L1-{number:02d}"} for number in range(1, 32)
            ],
            published_index_rows=[
                {
                    "industry_code": f"{number:02d}0000",
                    "industry_name": f"L1-{number:02d}",
                    "index_code": f"801{number:03d}.SI",
                }
                for number in range(1, 32)
            ],
            source_ids=("test:taxonomy", "test:index"),
            source_hashes=(HASH, "b" * 64),
        )
    )


def _research_basis(*, active_mode: str = "historical_replay") -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": "hmm_risk_industry_pit_research_basis_v1",
        "contract_version": "c013_g2a_data_a_v1",
        "active_mode": active_mode,
        "historical_classification_basis": "stable_taxonomy_backcast",
        "historical_non_as_known_taxonomy": True,
        "forward_classification_basis": "as_published_pit",
        "forward_non_as_known_taxonomy": False,
    }
    return {
        **body,
        "canonical_hash": digest_named_fields("hmm_risk_industry_pit_research_basis_v1", body),
    }


def _knowledge_unverified_bundle(
    tmp_path: Path,
    *,
    conflict_source_ids: list[str] | None = None,
    conflict_source_hashes: list[str] | None = None,
) -> CandidateBundleReadback:
    bundle = _bundle(tmp_path)
    first = bundle.classification_intervals[0]
    assert first.identity is not None
    conflict = {
        "authority_identity": dict(first.authority_identity),
        "identity": first.identity.as_dict(),
        "identity_hash": first.identity.identity_hash,
        "industry_code": first.identity.l3_code,
        "lineage_hash": HASH,
        "source_hashes": [HASH] if conflict_source_hashes is None else conflict_source_hashes,
        "source_ids": ["test:source"] if conflict_source_ids is None else conflict_source_ids,
    }
    unavailable = make_candidate_interval(
        canonical_symbol=first.canonical_symbol,
        authority_type=first.authority_type,
        taxonomy_contract_id=first.taxonomy_contract_id,
        taxonomy_version=first.taxonomy_version,
        authority_receipt_hash=first.authority_receipt_hash,
        valid_from=first.valid_from,
        valid_to_exclusive=first.valid_to_exclusive,
        eligible_from=first.eligible_from,
        eligible_to_exclusive=first.eligible_to_exclusive,
        causal_use_from=first.valid_from,
        causal_use_to_exclusive=first.valid_to_exclusive,
        known_from=None,
        source_effective_field=first.source_effective_field,
        source_last_updated_at=first.source_last_updated_at,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        non_as_known_taxonomy=False,
        identity=None,
        authority_identity={},
        unavailable_reason=UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED,
        conflict_candidates=(conflict,),
        source_ids=first.source_ids,
        source_hashes=first.source_hashes,
        lineage_hashes=first.lineage_hashes,
    )
    return CandidateBundleReadback(
        artifact_root=bundle.artifact_root,
        manifest=bundle.manifest,
        classification_receipt=bundle.classification_receipt,
        index_membership_receipt=bundle.index_membership_receipt,
        classification_intervals=(unavailable, *bundle.classification_intervals[1:]),
        index_membership_intervals=bundle.index_membership_intervals,
        preflight_report=bundle.preflight_report,
    )


def test_adapter_uses_classification_assignment_and_aligned_index_code_projection(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path))
    adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))
    _bind(adapter)
    projection = adapter.resolve("000001.SZ", date(2022, 1, 4))

    assert projection.status == "resolved"
    assert projection.l1_code == "801001.SI"
    assert projection.l1_name == "L1-01"
    assert projection.l2_code == "010100"
    assert projection.alignment_state == "aligned"
    assert len(adapter.classification_lookup) == 62
    assert len(adapter.constituents) == 31
    assert sum(len(value["l2_codes"]) for value in adapter.constituents.values()) == 131
    assert projection.as_dict()["classification_research_basis"] == "as_published_pit"
    assert projection.as_dict()["non_as_known_taxonomy"] is False


def test_adapter_preserves_typed_unavailable_without_neutral_or_previous_fallback(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path, target_unavailable=True))
    adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))
    _bind(adapter)
    projection = adapter.resolve("000001.SZ", date(2022, 1, 4))

    assert projection.status == "unavailable"
    assert projection.reason_code == "classification:membership_boundary_unavailable"
    assert projection.l1_code is None
    assert projection.l2_code is None


def test_historical_backcast_resolves_unique_frozen_candidate_and_marks_non_as_known(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_knowledge_unverified_bundle(tmp_path))
    adapter.bind_research_basis_contract(_research_basis())
    _bind(adapter)

    projection = adapter.resolve("000001.SZ", date(2022, 1, 4))
    manifest = adapter.mapping_manifest(
        universe_key="unit",
        source_start=date(2022, 1, 3),
        source_end=date(2022, 1, 4),
    )

    assert projection.status == "resolved"
    assert projection.l1_code == "801001.SI"
    assert manifest["active_classification_basis"] == "stable_taxonomy_backcast"
    assert manifest["non_as_known_taxonomy"] is True
    assert manifest["stable_backcast_candidate_sha256"] is not None
    assert manifest["source_classification_authority_receipt_hash"] != manifest["classification_authority_receipt_hash"]
    assert projection.as_dict()["classification_research_basis"] == "stable_taxonomy_backcast"
    assert projection.as_dict()["non_as_known_taxonomy"] is True


@pytest.mark.parametrize(
    ("source_ids", "source_hashes"),
    [([], [HASH]), (["test:source"], [])],
)
def test_historical_backcast_rejects_empty_conflict_provenance(
    tmp_path: Path,
    source_ids: list[str],
    source_hashes: list[str],
) -> None:
    adapter = HMMIndustryPitAdapter(
        authority_bundle=_knowledge_unverified_bundle(
            tmp_path,
            conflict_source_ids=source_ids,
            conflict_source_hashes=source_hashes,
        )
    )

    with pytest.raises(StateModelSetError, match="conflict provenance"):
        adapter.bind_research_basis_contract(_research_basis())


def test_binding_historical_basis_after_projection_refreshes_constituent_receipt(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_knowledge_unverified_bundle(tmp_path))
    _bind(adapter)
    source_receipt = adapter.authority_bundle.classification_receipt.receipt_hash

    adapter.bind_research_basis_contract(_research_basis())
    manifest = adapter.mapping_manifest(
        universe_key="unit",
        source_start=date(2022, 1, 3),
        source_end=date(2022, 1, 4),
    )

    assert manifest["classification_authority_receipt_hash"] != source_receipt
    assert {value["classification_authority_receipt_hash"] for value in adapter.constituents.values()} == {
        manifest["classification_authority_receipt_hash"]
    }


def test_forward_basis_keeps_knowledge_unverified_classification_unavailable(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_knowledge_unverified_bundle(tmp_path))
    adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))
    _bind(adapter)

    projection = adapter.resolve("000001.SZ", date(2022, 1, 4))

    assert projection.status == "unavailable"
    assert projection.reason_code == "classification:classification_knowledge_time_unverified"


def test_adapter_rejects_resolution_before_explicit_research_basis_binding(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path))
    _bind(adapter)

    with pytest.raises(StateModelSetError, match="research-basis contract has not been bound"):
        adapter.resolve("000001.SZ", date(2022, 1, 4))


def test_adapter_rejects_rebinding_research_basis_to_a_different_mode(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path))
    adapter.bind_research_basis_contract(_research_basis())

    with pytest.raises(StateModelSetError, match="research-basis contract cannot be rebound"):
        adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))


def test_601d_preflight_closes_full_denominator_and_performs_no_model_work(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path, target_unavailable=True))
    adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))
    _bind(adapter)
    denominator = FrozenDenominator.build(
        window_start=date(2022, 1, 3),
        window_end=date(2022, 1, 4),
        trading_dates=(date(2022, 1, 3), date(2022, 1, 4)),
        universe_spans=(UniverseSpan("000001.SZ", date(2022, 1, 1), date(2022, 1, 5)),),
    )
    report = adapter.preflight(denominator, expected_trading_days=2)

    assert report["total_opportunities"] == 2
    assert report["resolved"] == 0
    assert report["unavailable"] == 2
    assert report["closure"] == {
        "resolved_plus_unavailable": 2,
        "expected_denominator": 2,
        "passed": True,
    }
    assert report["fit_count"] == 0
    assert report["selection_performed"] is False
    assert report["d5_performed"] is False
    assert report["d6_performed"] is False
    assert report["model_or_ready_written"] is False
    assert report["l1_code_projection_status"] == "bound"
    assert report["l1_code_projection_sha256"] is not None


def test_preflight_rejects_unbound_l1_projection(tmp_path: Path) -> None:
    adapter = HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path))
    adapter.bind_research_basis_contract(_research_basis(active_mode="forward"))
    denominator = FrozenDenominator.build(
        window_start=date(2022, 1, 3),
        window_end=date(2022, 1, 4),
        trading_dates=(date(2022, 1, 3), date(2022, 1, 4)),
        universe_spans=(UniverseSpan("000001.SZ", date(2022, 1, 1), date(2022, 1, 5)),),
    )

    with pytest.raises(StateModelSetError, match="L1 code projection has not been bound"):
        adapter.preflight(denominator, expected_trading_days=2)


@pytest.mark.parametrize("method_name", ["iter_stock_fact_rows", "iter_missing_price_rows"])
def test_shared_industry_pit_reader_rejects_l2_without_canonical_l2_projection(method_name: str) -> None:
    reader = object.__new__(PostgresStockFactReader)
    reader.industry_pit_adapter = object()

    rows = getattr(reader, method_name)(sector_level="L2")
    with pytest.raises(StateModelSetError, match="supports only direct L1"):
        next(rows)


def test_adapter_rejects_partial_31_l1_projection(tmp_path: Path) -> None:
    with pytest.raises(StateModelSetError, match="canonical L1=31"):
        HMMIndustryPitAdapter(authority_bundle=_bundle(tmp_path, drop_last_l1=True))


def test_projection_builder_rejects_name_inference_when_numeric_code_sources_disagree() -> None:
    taxonomy_rows = [
        {"industry_code": f"{number:02d}0000", "industry_name": f"L1-{number:02d}"} for number in range(1, 32)
    ]
    published_rows = [
        {
            "industry_code": f"{number:02d}0000",
            "industry_name": f"L1-{number:02d}",
            "index_code": f"801{number:03d}.SI",
        }
        for number in range(1, 32)
    ]
    published_rows[0] = {**published_rows[0], "industry_code": "990000"}

    with pytest.raises(StateModelSetError, match="same 31 taxonomy codes"):
        build_l1_code_projection_authority(
            taxonomy_contract_id="sw2021_classification_catalog_v1",
            taxonomy_version="SW2021",
            projection_version="sw2021_taxonomy_to_published_l1_v1",
            taxonomy_rows=taxonomy_rows,
            published_index_rows=published_rows,
            source_ids=("test:taxonomy", "test:index"),
            source_hashes=(HASH, "b" * 64),
        )
