from __future__ import annotations

from collections import Counter
from datetime import date, datetime

import pytest

from backend.services.industry_pit.candidate_builder import (
    FrozenDenominator,
    UniverseSpan,
    build_classification_intervals,
    build_index_membership_intervals,
    build_taxonomy_catalog,
    full_denominator_preflight,
)
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    KnowledgeTimePolicy,
    ResearchBasis,
    ResolutionRequest,
    ResolvedIndustryIdentity,
    UnavailableIndustryIdentity,
    UnavailableReason,
    IndustryPitContractError,
)
from backend.services.industry_pit.resolver import IndustryPitResolver, resolve_dual_authority


SOURCE = "a" * 64
PDF_SOURCE = "b" * 64
CONFLICT_SYMBOLS = (
    "000016.SZ",
    "000716.SZ",
    "002481.SZ",
    "002507.SZ",
    "002557.SZ",
    "002582.SZ",
    "002597.SZ",
    "002719.SZ",
    "002738.SZ",
    "003030.SZ",
    "300699.SZ",
    "300741.SZ",
    "300777.SZ",
    "300783.SZ",
    "300858.SZ",
    "300892.SZ",
    "300915.SZ",
    "300972.SZ",
    "603020.SH",
    "603077.SH",
    "603697.SH",
    "605077.SH",
    "605300.SH",
)
MANDATORY = ("300741.SZ", "300858.SZ", "603020.SH", "605077.SH")


def _catalog():
    return build_taxonomy_catalog(
        [
            {
                "industry_code": "340404",
                "l1_name": "食品饮料",
                "l2_name": "食品加工",
                "l3_name": "其他食品",
            },
            {
                "industry_code": "220315",
                "l1_name": "基础化工",
                "l2_name": "化学制品",
                "l3_name": "食品及饲料添加剂",
            },
        ],
        source_sha256=SOURCE,
    )


def _denominator(symbols=CONFLICT_SYMBOLS):
    return FrozenDenominator.build(
        window_start=date(2021, 7, 30),
        window_end=date(2021, 12, 13),
        trading_dates=(date(2021, 7, 30), date(2021, 8, 2), date(2021, 12, 10), date(2021, 12, 13)),
        universe_spans=(
            UniverseSpan(symbol, date(2021, 7, 30), date(2021, 12, 13)) for symbol in symbols
        ),
    )


def _receipts(denominator, catalog):
    common = {
        "taxonomy_contract_id": catalog.contract_id,
        "taxonomy_version": catalog.version,
        "knowledge_time_policy": KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        "research_basis": ResearchBasis.AS_PUBLISHED_PIT,
        "frozen_denominator": denominator.total_opportunities,
        "denominator_digest": denominator.digest,
    }
    classification = AuthorityReceipt(
        authority_type=AuthorityType.CLASSIFICATION,
        authority_schema=CLASSIFICATION_CANDIDATE_SCHEMA,
        authority_version="classification_v1",
        source_ids=("test:classification",),
        source_hashes=(SOURCE,),
        **common,
    )
    index = AuthorityReceipt(
        authority_type=AuthorityType.INDEX_MEMBERSHIP,
        authority_schema=INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
        authority_version="index_v1",
        source_ids=("test:index",),
        source_hashes=(PDF_SOURCE,),
        **common,
    )
    return classification, index


def _history_rows():
    rows = []
    for symbol in CONFLICT_SYMBOLS:
        rows.extend(
            [
                {
                    "stock_code": symbol[:6],
                    "classification_valid_from": date(2020, 1, 1),
                    "industry_code": "340404",
                    "source_last_updated_at": datetime(2020, 1, 2, 9, 0),
                },
                {
                    "stock_code": symbol[:6],
                    "classification_valid_from": date(2021, 7, 30),
                    "industry_code": "220315",
                    "source_last_updated_at": (
                        datetime(2022, 8, 21, 19, 46)
                        if symbol == "605077.SH"
                        else datetime(2021, 7, 31, 16, 18)
                    ),
                },
            ]
        )
    return rows


def _index_evidence():
    rows = []
    for symbol in MANDATORY:
        rows.extend(
            [
                {
                    "canonical_symbol": symbol,
                    "industry_code": "340404",
                    "index_l1_code": "801120",
                    "index_l2_code": "801124",
                    "index_l3_code": "851244",
                    "membership_enter_date": "2020-01-01",
                    "membership_exit_date_exclusive": "2021-12-13",
                    "known_from": "2020-01-01",
                    "source_sha256": PDF_SOURCE,
                },
                {
                    "canonical_symbol": symbol,
                    "industry_code": "220315",
                    "index_l1_code": "801030",
                    "index_l2_code": "801034",
                    "index_l3_code": "850135",
                    "membership_enter_date": "2021-12-13",
                    "membership_exit_date_exclusive": None,
                    "known_from": "2021-12-13",
                    "source_sha256": PDF_SOURCE,
                },
            ]
        )
    return rows


def test_all_23_regressions_close_the_frozen_denominator_without_inner_join() -> None:
    denominator = _denominator()
    catalog = _catalog()
    classification_receipt, index_receipt = _receipts(denominator, catalog)
    classification, diagnostics = build_classification_intervals(
        _history_rows(),
        catalog=catalog,
        receipt=classification_receipt,
        denominator=denominator,
        classification_source_hash=SOURCE,
    )
    index, _ = build_index_membership_intervals(
        _index_evidence(), catalog=catalog, receipt=index_receipt, denominator=denominator
    )
    known = {(catalog.contract_id, catalog.version)}
    classification_resolver = IndustryPitResolver(
        receipt=classification_receipt, intervals=classification, known_taxonomy_versions=known
    )
    index_resolver = IndustryPitResolver(
        receipt=index_receipt, intervals=index, known_taxonomy_versions=known
    )
    report = full_denominator_preflight(
        denominator=denominator,
        classification_resolver=classification_resolver,
        index_membership_resolver=index_resolver,
        conflict_inventory={
            symbol: {"legacy_conflict_opportunities": 1, "diagnostic_only_not_authority_source": True}
            for symbol in CONFLICT_SYMBOLS
        },
        mandatory_symbols=MANDATORY,
    )
    assert denominator.total_opportunities == 23 * 4
    assert report["closure"] == {
        "classification_resolved_plus_unavailable": 23 * 4,
        "index_resolved_plus_unavailable": 23 * 4,
        "expected_denominator": 23 * 4,
        "passed": True,
    }
    assert set(report["conflict_inventory"]) == set(CONFLICT_SYMBOLS)
    assert set(report["mandatory_regression"]) == set(MANDATORY)
    assert diagnostics["same_boundary_identity_conflict"] == 0

    point_classification: Counter[str] = Counter()
    point_index: Counter[str] = Counter()
    point_alignment: Counter[str] = Counter()
    point_reasons: Counter[str] = Counter()
    for span in denominator.universe_spans:
        for trade_date in denominator.dates_for_span(span):
            result = resolve_dual_authority(
                classification_resolver=classification_resolver,
                index_membership_resolver=index_resolver,
                classification_request=ResolutionRequest(
                    span.canonical_symbol,
                    trade_date,
                    AuthorityType.CLASSIFICATION,
                    catalog.contract_id,
                    catalog.version,
                    classification_receipt.receipt_hash,
                    classification_receipt.knowledge_time_policy,
                    classification_receipt.research_basis,
                ),
                index_membership_request=ResolutionRequest(
                    span.canonical_symbol,
                    trade_date,
                    AuthorityType.INDEX_MEMBERSHIP,
                    catalog.contract_id,
                    catalog.version,
                    index_receipt.receipt_hash,
                    index_receipt.knowledge_time_policy,
                    index_receipt.research_basis,
                ),
            )
            point_classification[result.classification.status] += 1
            point_index[result.index_membership.status] += 1
            point_alignment[result.alignment_state.value] += 1
            for authority_result in (result.classification, result.index_membership):
                if isinstance(authority_result, UnavailableIndustryIdentity):
                    point_reasons[
                        f"{authority_result.authority_type.value}:{authority_result.reason.value}"
                    ] += 1
    assert report["classification"] == dict(point_classification)
    assert report["index_membership"] == dict(point_index)
    assert report["alignment"] == dict(point_alignment)
    assert report["unavailable_by_reason"] == dict(point_reasons)


def test_classification_update_date_does_not_change_valid_or_known_boundaries() -> None:
    denominator = _denominator(("605077.SH",))
    catalog = _catalog()
    classification_receipt, _ = _receipts(denominator, catalog)
    intervals, _ = build_classification_intervals(
        [row for row in _history_rows() if row["stock_code"] == "605077"],
        catalog=catalog,
        receipt=classification_receipt,
        denominator=denominator,
        classification_source_hash=SOURCE,
    )
    resolver = IndustryPitResolver(
        receipt=classification_receipt,
        intervals=intervals,
        known_taxonomy_versions={(catalog.contract_id, catalog.version)},
    )
    july = resolver.resolve(
        ResolutionRequest(
            "605077.SH",
            date(2021, 7, 30),
            AuthorityType.CLASSIFICATION,
            catalog.contract_id,
            catalog.version,
            classification_receipt.receipt_hash,
            classification_receipt.knowledge_time_policy,
            classification_receipt.research_basis,
        )
    )
    august = resolver.resolve(
        ResolutionRequest(
            "605077.SH",
            date(2021, 8, 2),
            AuthorityType.CLASSIFICATION,
            catalog.contract_id,
            catalog.version,
            classification_receipt.receipt_hash,
            classification_receipt.knowledge_time_policy,
            classification_receipt.research_basis,
        )
    )
    assert isinstance(july, ResolvedIndustryIdentity)
    assert july.identity.leaf_code == "340404"
    assert july.valid_from != july.known_from
    assert isinstance(august, ResolvedIndustryIdentity)
    assert august.identity.leaf_code == "220315"
    assert august.valid_from == date(2021, 7, 30)
    assert august.known_from == date(2021, 8, 2)


def test_same_boundary_history_conflict_and_unknown_knowledge_time_are_typed() -> None:
    denominator = _denominator(("300741.SZ",))
    catalog = _catalog()
    classification_receipt, _ = _receipts(denominator, catalog)
    rows = [
        {
            "stock_code": "300741",
            "classification_valid_from": date(2021, 7, 30),
            "industry_code": code,
            "source_last_updated_at": datetime(2021, 7, 31, 16, 18),
        }
        for code in ("340404", "220315")
    ]
    intervals, diagnostics = build_classification_intervals(
        rows,
        catalog=catalog,
        receipt=classification_receipt,
        denominator=denominator,
        classification_source_hash=SOURCE,
    )
    resolver = IndustryPitResolver(
        receipt=classification_receipt,
        intervals=intervals,
        known_taxonomy_versions={(catalog.contract_id, catalog.version)},
    )
    result = resolver.resolve(
        ResolutionRequest(
            "300741.SZ",
            date(2021, 8, 2),
            AuthorityType.CLASSIFICATION,
            catalog.contract_id,
            catalog.version,
            classification_receipt.receipt_hash,
            classification_receipt.knowledge_time_policy,
            classification_receipt.research_basis,
        )
    )
    assert isinstance(result, UnavailableIndustryIdentity)
    assert result.reason is UnavailableReason.SAME_BOUNDARY_IDENTITY_CONFLICT
    assert diagnostics["same_boundary_identity_conflict"] == 1

    later_rows = [
        {
            "stock_code": "300741",
            "classification_valid_from": date(2022, 1, 4),
            "industry_code": "220315",
            "source_last_updated_at": datetime(2022, 2, 1, 12, 0),
        }
    ]
    later, _ = build_classification_intervals(
        later_rows,
        catalog=catalog,
        receipt=classification_receipt,
        denominator=denominator,
        classification_source_hash=SOURCE,
    )
    later_resolver = IndustryPitResolver(
        receipt=classification_receipt,
        intervals=later,
        known_taxonomy_versions={(catalog.contract_id, catalog.version)},
    )
    historical = later_resolver.resolve(
        ResolutionRequest(
            "300741.SZ",
            date(2021, 12, 13),
            AuthorityType.CLASSIFICATION,
            catalog.contract_id,
            catalog.version,
            classification_receipt.receipt_hash,
            classification_receipt.knowledge_time_policy,
            classification_receipt.research_basis,
        )
    )
    assert isinstance(historical, UnavailableIndustryIdentity)
    assert historical.reason is UnavailableReason.CLASSIFICATION_AUTHORITY_UNAVAILABLE


def test_missing_index_evidence_is_unavailable_not_default_industry() -> None:
    denominator = _denominator(("300741.SZ",))
    catalog = _catalog()
    _, index_receipt = _receipts(denominator, catalog)
    intervals, diagnostics = build_index_membership_intervals(
        [], catalog=catalog, receipt=index_receipt, denominator=denominator
    )
    resolver = IndustryPitResolver(
        receipt=index_receipt,
        intervals=intervals,
        known_taxonomy_versions={(catalog.contract_id, catalog.version)},
    )
    result = resolver.resolve(
        ResolutionRequest(
            "300741.SZ",
            date(2021, 12, 13),
            AuthorityType.INDEX_MEMBERSHIP,
            catalog.contract_id,
            catalog.version,
            index_receipt.receipt_hash,
            index_receipt.knowledge_time_policy,
            index_receipt.research_basis,
        )
    )
    assert isinstance(result, UnavailableIndustryIdentity)
    assert result.reason is UnavailableReason.MEMBERSHIP_BOUNDARY_UNAVAILABLE
    assert result.conflict_candidates == ()
    assert diagnostics["authoritative_evidence_row_count"] == 0


def test_catalog_same_code_different_identity_fails_closed() -> None:
    with pytest.raises(IndustryPitContractError, match="catalog identity conflicts"):
        build_taxonomy_catalog(
            [
                {
                    "industry_code": "220315",
                    "l1_name": "基础化工",
                    "l2_name": "化学制品",
                    "l3_name": "食品及饲料添加剂",
                },
                {
                    "industry_code": "220315",
                    "l1_name": "基础化工",
                    "l2_name": "化学制品",
                    "l3_name": "冲突名称",
                },
            ],
            source_sha256=SOURCE,
        )
