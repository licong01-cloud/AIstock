from __future__ import annotations

import itertools
from datetime import date

import pytest

from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AlignmentState,
    AuthorityReceipt,
    AuthorityType,
    IndustryPitContractError,
    KnowledgeTimePolicy,
    ResearchBasis,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    TaxonomyIdentity,
    UnavailableIndustryIdentity,
    UnavailableReason,
    make_candidate_interval,
)
from backend.services.industry_pit.resolver import IndustryPitResolver, resolve_dual_authority


SOURCE_A = "a" * 64
SOURCE_B = "b" * 64
DENOMINATOR = "d" * 64
MANDATORY_SYMBOLS = ("300741.SZ", "300858.SZ", "603020.SH", "605077.SH")

OLD = TaxonomyIdentity("340000", "食品饮料", "340400", "食品加工", "340404", "其他食品")
NEW = TaxonomyIdentity("220000", "基础化工", "220300", "化学制品", "220315", "食品及饲料添加剂")


def _receipt(authority: AuthorityType, *, basis: ResearchBasis = ResearchBasis.AS_PUBLISHED_PIT) -> AuthorityReceipt:
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
        knowledge_time_policy=(
            KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE
            if basis is ResearchBasis.AS_PUBLISHED_PIT
            else KnowledgeTimePolicy.NON_AS_KNOWN_RESEARCH
        ),
        research_basis=basis,
        source_ids=("test:source",),
        source_hashes=(SOURCE_A,),
        frozen_denominator=100,
        denominator_digest=DENOMINATOR,
    )


def _interval(
    receipt: AuthorityReceipt,
    *,
    symbol: str,
    identity: TaxonomyIdentity | None,
    valid_from: date,
    valid_to: date | None,
    causal_from: date | None,
    causal_to: date | None,
    known_from: date | None,
    source_hash: str = SOURCE_A,
    lineage_hash: str = SOURCE_A,
    reason: UnavailableReason | None = None,
    updated_at: str | None = None,
    source_ids: tuple[str, ...] | None = None,
):
    return make_candidate_interval(
        canonical_symbol=symbol,
        authority_type=receipt.authority_type,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        valid_from=valid_from,
        valid_to_exclusive=valid_to,
        eligible_from=date(2020, 1, 1),
        eligible_to_exclusive=date(2027, 1, 1),
        causal_use_from=causal_from,
        causal_use_to_exclusive=causal_to,
        known_from=known_from,
        source_effective_field=(
            "计入日期" if receipt.authority_type is AuthorityType.CLASSIFICATION else "membership_enter_date"
        ),
        source_last_updated_at=updated_at,
        research_basis=receipt.research_basis,
        non_as_known_taxonomy=receipt.research_basis is not ResearchBasis.AS_PUBLISHED_PIT,
        identity=identity,
        authority_identity=(
            (
                {
                    "classification_l1_code": identity.l1_code,
                    "classification_l2_code": identity.l2_code,
                    "classification_l3_code": identity.l3_code,
                }
                if receipt.authority_type is AuthorityType.CLASSIFICATION
                else {
                    "index_l1_code": "801030" if identity == NEW else "801120",
                    "index_l2_code": "801034" if identity == NEW else "801124",
                    "index_l3_code": "850135" if identity == NEW else "851244",
                }
            )
            if identity is not None
            else {}
        ),
        unavailable_reason=reason,
        source_ids=source_ids or receipt.source_ids,
        source_hashes=(source_hash,),
        lineage_hashes=(lineage_hash,),
    )


def _request(receipt: AuthorityReceipt, symbol: str, day: date) -> ResolutionRequest:
    return ResolutionRequest(
        canonical_symbol=symbol,
        trade_date=day,
        authority_type=receipt.authority_type,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        knowledge_time_policy=receipt.knowledge_time_policy,
        research_basis=receipt.research_basis,
    )


def _classification_rows(receipt: AuthorityReceipt, symbol: str):
    return [
        _interval(
            receipt,
            symbol=symbol,
            identity=OLD,
            valid_from=date(2020, 1, 1),
            valid_to=date(2021, 7, 30),
            causal_from=date(2020, 1, 1),
            causal_to=date(2021, 8, 2),
            known_from=date(2020, 1, 1),
        ),
        _interval(
            receipt,
            symbol=symbol,
            identity=NEW,
            valid_from=date(2021, 7, 30),
            valid_to=None,
            causal_from=date(2021, 8, 2),
            causal_to=None,
            known_from=date(2021, 8, 2),
            updated_at="2022-08-21T11:46:00Z" if symbol == "605077.SH" else "2021-07-31T08:18:00Z",
        ),
    ]


def _index_rows(receipt: AuthorityReceipt, symbol: str):
    return [
        _interval(
            receipt,
            symbol=symbol,
            identity=OLD,
            valid_from=date(2020, 1, 1),
            valid_to=date(2021, 12, 13),
            causal_from=date(2020, 1, 1),
            causal_to=date(2021, 12, 13),
            known_from=date(2020, 1, 1),
        ),
        _interval(
            receipt,
            symbol=symbol,
            identity=NEW,
            valid_from=date(2021, 12, 13),
            valid_to=None,
            causal_from=date(2021, 12, 13),
            causal_to=None,
            known_from=date(2021, 12, 13),
        ),
    ]


@pytest.mark.parametrize("symbol", MANDATORY_SYMBOLS)
def test_four_mandatory_symbols_keep_classification_and_index_boundaries_separate(symbol: str) -> None:
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    classification = IndustryPitResolver(
        receipt=classification_receipt,
        intervals=_classification_rows(classification_receipt, symbol),
        known_taxonomy_versions={(classification_receipt.taxonomy_contract_id, classification_receipt.taxonomy_version)},
    )
    index = IndustryPitResolver(
        receipt=index_receipt,
        intervals=_index_rows(index_receipt, symbol),
        known_taxonomy_versions={(index_receipt.taxonomy_contract_id, index_receipt.taxonomy_version)},
    )

    july_30 = resolve_dual_authority(
        classification_resolver=classification,
        index_membership_resolver=index,
        classification_request=_request(classification_receipt, symbol, date(2021, 7, 30)),
        index_membership_request=_request(index_receipt, symbol, date(2021, 7, 30)),
    )
    august_2 = resolve_dual_authority(
        classification_resolver=classification,
        index_membership_resolver=index,
        classification_request=_request(classification_receipt, symbol, date(2021, 8, 2)),
        index_membership_request=_request(index_receipt, symbol, date(2021, 8, 2)),
    )
    december_13 = resolve_dual_authority(
        classification_resolver=classification,
        index_membership_resolver=index,
        classification_request=_request(classification_receipt, symbol, date(2021, 12, 13)),
        index_membership_request=_request(index_receipt, symbol, date(2021, 12, 13)),
    )

    assert isinstance(july_30.classification, ResolvedIndustryIdentity)
    assert july_30.classification.identity.leaf_code == "340404"
    assert isinstance(august_2.classification, ResolvedIndustryIdentity)
    assert august_2.classification.identity.as_dict() == NEW.as_dict()
    assert august_2.classification.as_dict()["identity_hash"] == NEW.identity_hash
    assert august_2.classification.sequential_interval_resolved is True
    assert august_2.classification.valid_from == date(2021, 7, 30)
    assert august_2.classification.known_from == date(2021, 8, 2)
    assert august_2.alignment_state is AlignmentState.UNALIGNED
    assert december_13.alignment_state is AlignmentState.ALIGNED
    assert isinstance(december_13.index_membership, ResolvedIndustryIdentity)
    assert december_13.index_membership.valid_from == date(2021, 12, 13)


def test_605077_update_date_is_lineage_only() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    rows = _classification_rows(receipt, "605077.SH")
    new_row = rows[1]
    assert new_row.valid_from == date(2021, 7, 30)
    assert new_row.known_from == date(2021, 8, 2)
    assert new_row.source_last_updated_at == "2022-08-21T11:46:00Z"


def test_exact_duplicate_collapse_and_permutation_are_order_invariant() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    first = _classification_rows(receipt, "300741.SZ")[0]
    duplicate = _interval(
        receipt,
        symbol="300741.SZ",
        identity=OLD,
        valid_from=first.valid_from,
        valid_to=first.valid_to_exclusive,
        causal_from=first.causal_use_from,
        causal_to=first.causal_use_to_exclusive,
        known_from=first.known_from,
        source_hash=SOURCE_B,
        lineage_hash=SOURCE_B,
        source_ids=("test:independent-source",),
    )
    results = []
    for rows in itertools.permutations([first, duplicate]):
        resolver = IndustryPitResolver(
            receipt=receipt,
            intervals=rows,
            known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
        )
        results.append(resolver.resolve(_request(receipt, "300741.SZ", date(2021, 7, 29))).as_dict())
    assert results[0] == results[1]
    assert results[0]["exact_duplicate_collapsed"] is True
    assert results[0]["sequential_interval_resolved"] is False
    assert results[0]["source_hashes"] == [SOURCE_A, SOURCE_B]
    assert results[0]["source_ids"] == ["test:independent-source", "test:source"]


def test_authority_identity_shape_and_receipt_basis_policy_fail_closed() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    with pytest.raises(IndustryPitContractError, match="authority-specific identity keys"):
        make_candidate_interval(
            canonical_symbol="300741.SZ",
            authority_type=AuthorityType.CLASSIFICATION,
            taxonomy_contract_id=receipt.taxonomy_contract_id,
            taxonomy_version=receipt.taxonomy_version,
            authority_receipt_hash=receipt.receipt_hash,
            valid_from=date(2021, 7, 30),
            valid_to_exclusive=None,
            eligible_from=date(2021, 7, 30),
            eligible_to_exclusive=None,
            causal_use_from=date(2021, 8, 2),
            causal_use_to_exclusive=None,
            known_from=date(2021, 8, 2),
            source_effective_field="计入日期",
            source_last_updated_at=None,
            research_basis=ResearchBasis.AS_PUBLISHED_PIT,
            non_as_known_taxonomy=False,
            identity=NEW,
            authority_identity={"classification_l3_code": NEW.l3_code},
            unavailable_reason=None,
            source_ids=receipt.source_ids,
            source_hashes=(SOURCE_A,),
            lineage_hashes=(SOURCE_A,),
        )

    with pytest.raises(IndustryPitContractError, match="knowledge-time policy"):
        AuthorityReceipt(
            authority_type=AuthorityType.CLASSIFICATION,
            authority_schema=CLASSIFICATION_CANDIDATE_SCHEMA,
            authority_version="invalid_basis_policy_v1",
            taxonomy_contract_id="sw2021_classification_catalog_v1",
            taxonomy_version="SW2021",
            knowledge_time_policy=KnowledgeTimePolicy.NON_AS_KNOWN_RESEARCH,
            research_basis=ResearchBasis.AS_PUBLISHED_PIT,
            source_ids=("test:source",),
            source_hashes=(SOURCE_A,),
            frozen_denominator=1,
            denominator_digest=DENOMINATOR,
        )


def test_same_boundary_conflict_interval_overlap_and_authority_mismatch_fail_closed() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    conflict_rows = [
        _interval(
            receipt,
            symbol="300741.SZ",
            identity=identity,
            valid_from=date(2021, 7, 30),
            valid_to=None,
            causal_from=date(2021, 8, 2),
            causal_to=None,
            known_from=date(2021, 8, 2),
            source_hash=source,
            lineage_hash=source,
        )
        for identity, source in ((OLD, SOURCE_A), (NEW, SOURCE_B))
    ]
    resolver = IndustryPitResolver(
        receipt=receipt,
        intervals=conflict_rows,
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    )
    result = resolver.resolve(_request(receipt, "300741.SZ", date(2021, 8, 2)))
    assert isinstance(result, UnavailableIndustryIdentity)
    assert result.reason is UnavailableReason.SAME_BOUNDARY_IDENTITY_CONFLICT
    assert len(result.conflict_candidates) == 2

    wrong_request = ResolutionRequest(
        canonical_symbol="300741.SZ",
        trade_date=date(2021, 8, 2),
        authority_type=AuthorityType.INDEX_MEMBERSHIP,
        taxonomy_contract_id=receipt.taxonomy_contract_id,
        taxonomy_version=receipt.taxonomy_version,
        authority_receipt_hash=receipt.receipt_hash,
        knowledge_time_policy=receipt.knowledge_time_policy,
        research_basis=receipt.research_basis,
    )
    assert resolver.resolve(wrong_request).reason is UnavailableReason.AUTHORITY_SOURCE_MISMATCH


def test_unknown_taxonomy_and_future_mutation_do_not_change_historical_resolution() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    base_rows = _classification_rows(receipt, "300741.SZ")
    future = _interval(
        receipt,
        symbol="300741.SZ",
        identity=OLD,
        valid_from=date(2026, 1, 1),
        valid_to=None,
        causal_from=date(2026, 1, 2),
        causal_to=None,
        known_from=date(2026, 1, 2),
        source_hash=SOURCE_B,
        lineage_hash=SOURCE_B,
    )
    before = IndustryPitResolver(
        receipt=receipt,
        intervals=base_rows,
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    ).resolve(_request(receipt, "300741.SZ", date(2021, 8, 2)))
    after = IndustryPitResolver(
        receipt=receipt,
        intervals=[future, *base_rows],
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    ).resolve(_request(receipt, "300741.SZ", date(2021, 8, 2)))
    assert before.as_dict() == after.as_dict()

    unknown = ResolutionRequest(
        canonical_symbol="300741.SZ",
        trade_date=date(2021, 8, 2),
        authority_type=AuthorityType.CLASSIFICATION,
        taxonomy_contract_id="unknown",
        taxonomy_version="unknown",
        authority_receipt_hash=receipt.receipt_hash,
        knowledge_time_policy=receipt.knowledge_time_policy,
        research_basis=receipt.research_basis,
    )
    assert IndustryPitResolver(
        receipt=receipt,
        intervals=base_rows,
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    ).resolve(unknown).reason is UnavailableReason.TAXONOMY_VERSION_UNAVAILABLE


def test_complete_interval_permutation_and_future_index_mutation_are_order_invariant() -> None:
    receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP)
    rows = _index_rows(receipt, "300741.SZ")
    future = _interval(
        receipt,
        symbol="300741.SZ",
        identity=OLD,
        valid_from=date(2026, 1, 5),
        valid_to=None,
        causal_from=date(2026, 1, 5),
        causal_to=None,
        known_from=date(2026, 1, 5),
        source_hash=SOURCE_B,
        lineage_hash=SOURCE_B,
    )
    outputs = []
    for permutation in itertools.permutations([*rows, future]):
        resolver = IndustryPitResolver(
            receipt=receipt,
            intervals=permutation,
            known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
        )
        outputs.append(resolver.resolve(_request(receipt, "300741.SZ", date(2021, 12, 13))).as_dict())
    assert all(output == outputs[0] for output in outputs)
    assert outputs[0]["authority_identity"] == {
        "index_l1_code": "801030",
        "index_l2_code": "801034",
        "index_l3_code": "850135",
    }


def test_non_same_boundary_overlap_and_unknown_knowledge_time_are_typed() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    overlapping = [
        _interval(
            receipt,
            symbol="300741.SZ",
            identity=NEW,
            valid_from=start,
            valid_to=None,
            causal_from=start,
            causal_to=None,
            known_from=start,
            source_hash=source,
            lineage_hash=source,
        )
        for start, source in ((date(2021, 8, 2), SOURCE_A), (date(2021, 9, 1), SOURCE_B))
    ]
    resolver = IndustryPitResolver(
        receipt=receipt,
        intervals=overlapping,
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    )
    result = resolver.resolve(_request(receipt, "300741.SZ", date(2021, 9, 2)))
    assert isinstance(result, UnavailableIndustryIdentity)
    assert result.reason is UnavailableReason.INTERVAL_OVERLAP

    unavailable = _interval(
        receipt,
        symbol="300741.SZ",
        identity=None,
        valid_from=date(2022, 1, 4),
        valid_to=None,
        causal_from=date(2022, 1, 4),
        causal_to=None,
        known_from=None,
        reason=UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED,
    )
    unavailable_resolver = IndustryPitResolver(
        receipt=receipt,
        intervals=[unavailable],
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    )
    assert unavailable_resolver.resolve(
        _request(receipt, "300741.SZ", date(2022, 1, 4))
    ).reason is UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED


def test_stable_taxonomy_backcast_is_explicitly_non_as_known() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION, basis=ResearchBasis.STABLE_TAXONOMY_BACKCAST)
    row = _interval(
        receipt,
        symbol="300741.SZ",
        identity=NEW,
        valid_from=date(2021, 7, 30),
        valid_to=None,
        causal_from=None,
        causal_to=None,
        known_from=None,
    )
    resolver = IndustryPitResolver(
        receipt=receipt,
        intervals=[row],
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    )
    result = resolver.resolve(_request(receipt, "300741.SZ", date(2021, 7, 30)))
    assert isinstance(result, ResolvedIndustryIdentity)
    assert result.non_as_known_taxonomy is True


def test_invalid_dates_and_naive_fallback_shapes_are_rejected() -> None:
    receipt = _receipt(AuthorityType.CLASSIFICATION)
    with pytest.raises(IndustryPitContractError, match="eligible interval"):
        make_candidate_interval(
            canonical_symbol="300741.SZ",
            authority_type=AuthorityType.CLASSIFICATION,
            taxonomy_contract_id=receipt.taxonomy_contract_id,
            taxonomy_version=receipt.taxonomy_version,
            authority_receipt_hash=receipt.receipt_hash,
            valid_from=date(2021, 1, 1),
            valid_to_exclusive=None,
            eligible_from=date(2022, 1, 1),
            eligible_to_exclusive=date(2021, 1, 1),
            causal_use_from=date(2021, 1, 1),
            causal_use_to_exclusive=None,
            known_from=date(2021, 1, 1),
            source_effective_field="计入日期",
            source_last_updated_at=None,
            research_basis=ResearchBasis.AS_PUBLISHED_PIT,
            non_as_known_taxonomy=False,
            identity=NEW,
            authority_identity={
                "classification_l1_code": NEW.l1_code,
                "classification_l2_code": NEW.l2_code,
                "classification_l3_code": NEW.l3_code,
            },
            unavailable_reason=None,
            source_ids=receipt.source_ids,
            source_hashes=(SOURCE_A,),
            lineage_hashes=(SOURCE_A,),
        )
