from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_context_consumer_v1 as m
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.industry_pit.contracts import (
    AuthorityReceipt, AuthorityType, CLASSIFICATION_CANDIDATE_SCHEMA, KnowledgeTimePolicy,
    ResearchBasis, TaxonomyIdentity, UnavailableReason, make_candidate_interval,
)
from backend.services.industry_pit.resolver import IndustryPitResolver


@pytest.fixture
def source():
    receipt = AuthorityReceipt(AuthorityType.CLASSIFICATION, CLASSIFICATION_CANDIDATE_SCHEMA, "v1", "catalog", "SW2021",
        KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE, ResearchBasis.AS_PUBLISHED_PIT, ("fixture",), ("a"*64,), 3, "b"*64)
    category = TaxonomyIdentity("340000", "食品", "340400", "加工", "340404", "其他")
    intervals = []
    for symbol, known in (("000001.SZ", True), ("000002.SZ", False)):
        intervals.append(make_candidate_interval(canonical_symbol=symbol, authority_type=receipt.authority_type,
            taxonomy_contract_id="catalog", taxonomy_version="SW2021", authority_receipt_hash=receipt.receipt_hash,
            valid_from=date(2024, 1, 1), valid_to_exclusive=None, eligible_from=date(2024, 1, 1), eligible_to_exclusive=None,
            causal_use_from=date(2024, 1, 1), causal_use_to_exclusive=None, known_from=date(2024, 1, 1) if known else None,
            source_effective_field="计入日期", source_last_updated_at="2024-01-01T00:00:00+00:00",
            research_basis=receipt.research_basis, non_as_known_taxonomy=False, identity=category if known else None,
            authority_identity={"classification_l1_code": "340000", "classification_l2_code": "340400", "classification_l3_code": "340404"} if known else {},
            unavailable_reason=None if known else UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED,
            source_ids=("fixture",), source_hashes=("a"*64,), lineage_hashes=("a"*64,)))
    resolver = IndustryPitResolver(receipt=receipt, intervals=intervals, known_taxonomy_versions=(("catalog", "SW2021"),))
    spans = [(date(2024, 1, 1), date(2026, 12, 31))]
    pools = {"stock_universe": {"000001.SZ": spans, "000002.SZ": spans}}
    dated = pd.DataFrame([(s, *spans[0], i) for i, s in enumerate(pools["stock_universe"])],
        columns=["instrument", "start_date", "end_date", "l2_code_id"])
    return m.EconomicContextSourceV1({"cutoff": "2026-12-31", "universe_selection": {"mode": "stock_universe", "pool_ids": []},
        "sector": {"classification_receipt_hash": receipt.receipt_hash}}, pools, dated, resolver, {})


def roster(days=("2025-09-01", "2025-09-02")):
    return pd.DataFrame([(d, (pd.Timestamp(d)+pd.Timedelta(days=1)).date(), s) for d in days
        for s in ("000001.SZ", "000002.SZ", "000003.SZ")], columns=KEY)


def test_partial_context_does_not_block_or_drop_population_and_day_equals_batch(source):
    keys = roster()
    rows, summary = m.build_economic_context_rows_v1(candidates=keys, source=source)
    assert rows[KEY].equals(m.context_roster_v1(keys)) and summary["rows"] == 6
    assert summary["optional_classification"] == "PARTIAL" and summary["core_input_identity"] == "VERIFIED"
    assert (summary["classification_available"], summary["dated_assignment_available"], summary["outside_current_pool"]) == (2, 4, 2)
    assert rows.loc[rows.instrument.ne("000001.SZ"), "classification_l2_code"].isna().all()
    assert summary["unknown_reasons"] == {"classification_knowledge_time_unverified": 2, "classification_authority_unavailable": 2}
    day_rows = [m.build_economic_context_rows_v1(candidates=group, source=source)[0] for _, group in keys.groupby(KEY[0])]
    pd.testing.assert_frame_equal(rows, pd.concat(day_rows, ignore_index=True))
    assert summary["deployable"] is False and summary["outcomes_read"] is False and summary["fit_count"] == 0


@pytest.mark.parametrize("pools", [["csi300"], ["csi300", "csi500"]])
def test_qe_compatible_pool_union_always_intersects_canonical_pool(source, pools):
    source.identity["universe_selection"] = {"mode": "single_index" if len(pools) == 1 else "index_union", "pool_ids": pools}
    for index, pool in enumerate(pools):
        source.pools[pool] = {f"00000{index+1}.SZ": [(date(2024, 1, 1), date(2026, 12, 31))],
            "000003.SZ": [(date(2024, 1, 1), date(2026, 12, 31))]}
    rows, _ = m.build_economic_context_rows_v1(candidates=roster(("2025-09-01",)), source=source)
    assert rows.current_pool_eligible.tolist() == [True, len(pools) == 2, False]


@pytest.mark.parametrize("defect", ["duplicate", "intraday", "future_cutoff", "same_target", "wrong_pool", "conflict", "foreign_receipt", "authority_conflict", "formal"])
def test_unsafe_identity_clocks_conflicts_and_formal_use_fail_closed(source, defect):
    keys, purpose = roster(), "EXPLORATORY_SCREEN"
    if defect == "duplicate":
        keys = pd.concat([keys, keys.iloc[:1]])
    elif defect == "intraday":
        keys.loc[0, KEY[0]] = "2025-09-01T10:00:00"
    elif defect == "future_cutoff":
        source.identity["cutoff"] = "2025-08-31"
    elif defect == "same_target":
        keys.loc[0, KEY[1]] = keys.loc[0, KEY[0]]
    elif defect == "wrong_pool":
        source.pools["csi300"] = {}
    elif defect == "conflict":
        source.membership.loc[len(source.membership)] = source.membership.iloc[0]
    elif defect == "foreign_receipt":
        source.identity["sector"]["classification_receipt_hash"] = "f"*64
    elif defect == "authority_conflict":
        source.classification._global_authority_mismatch = True
    else:
        purpose = "CONFIRMATION"
    with pytest.raises(AdvisoryModelFirstError):
        m.build_economic_context_rows_v1(candidates=keys, source=source, purpose=purpose)


@pytest.mark.parametrize("changed_file", ["pool", "profile", "coverage"])
def test_raw_crlf_sidecar_hash_and_changed_pins_are_not_silently_normalized(tmp_path, monkeypatch, changed_file):
    profile, pool, coverage = tmp_path/"profile.json", tmp_path/"stock_universe.txt", tmp_path/"coverage.json"
    profile.write_bytes(b"profile")
    coverage.write_bytes(b"coverage")
    pool.write_bytes(b"000001.SZ\t2024-01-01\t2026-12-31\r\n")
    profile_hash = m._digest(profile)
    parsed = SimpleNamespace(profile_sha256=profile_hash, controller_stock_pool_root=tmp_path,
        coverage_receipt_path=coverage, qe={"coverage_receipt_sha256": m._digest(coverage)},
        universes={"stock_universe": {"filename": pool.name, "sha256": m._digest(pool), "membership_revision": "rule"}},
        generation="generation", release_id="release", cutoff=date(2026, 12, 31),
        raw={"components": {"day_pins": {"rule_version": "rule", "universe_key": "canonical"}}})
    monkeypatch.setattr(m, "load_qe_profile", lambda p: parsed)
    source = m.load_economic_context_source_v1(profile_path=profile, profile_sha256=profile_hash,
        universe_selection={"mode": "stock_universe", "pool_ids": []})
    assert "000001.SZ" in source.pools["stock_universe"] and source.classification is None
    changed = {"pool": pool, "profile": profile, "coverage": coverage}[changed_file]
    changed.write_bytes(changed.read_bytes().replace(b"\r\n", b"\n") if changed_file == "pool" else b"changed")
    with pytest.raises(AdvisoryModelFirstError, match="pin"):
        source.verify_unchanged()
