from __future__ import annotations

import copy
from datetime import date, timedelta

import pytest

from backend.services.hmm_risk import formal_state_domains as subject
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt
from backend.services.hmm_risk.stock_fact_observation import (
    StateModelSetError,
    validate_c010_provider_absence_domain_partition,
)


@pytest.fixture
def domain():
    dates = [(date(2022, 1, 3) + timedelta(days=i)).isoformat() for i in range(10)]
    mapping = {
        "schema_version": "hmm_risk_pit_mapping_manifest_v3",
        "universe_key": "test_frozen_pit",
        "source_window_start": "2020-07-30",
        "source_window_end": "2025-04-30",
        "canonical_l1_count": 31,
        "canonical_l2_count": 131,
        "active_classification_basis": "stable_taxonomy_backcast",
        "non_as_known_taxonomy": True,
        "stable_backcast_candidate_sha256": "a" * 64,
    }
    mapping.update(
        {
            key: "a" * 64
            for key in (
                "source_classification_authority_receipt_hash",
                "classification_authority_receipt_hash",
                "index_membership_authority_receipt_hash",
                "classification_candidate_hash",
                "index_membership_candidate_hash",
                "candidate_bundle_hash",
                "candidate_preflight_canonical_hash",
                "research_basis_contract_sha256",
                "l1_code_projection_sha256",
                "l2_code_projection_sha256",
                "constituent_manifest_hash",
            )
        }
    )
    authorities = {
        "provider_absence_manifest_identity": subject.authority_identity(
            "provider_absence_manifest", {"manifest_sha256": "a" * 64}
        ),
        "security_resolver_identity": subject.authority_identity(
            "security_source_identity_manifest", {"manifest_sha256": "b" * 64}
        ),
        "pit_authority_identity": subject.authority_identity(
            "stock_universe_pit_state_and_spans", {"manifest_sha256": "c" * 64}
        ),
        "price_source_identity": subject.authority_identity("market.kline_daily_raw", {"manifest_sha256": "d" * 64}),
        "sw_mapping_classify_identity": subject.authority_identity(
            "hmm_industry_pit_classification_projection", mapping
        ),
    }

    def entry(day, *, price_present=True, pit_count=1):
        projection = dict(
            status="resolved",
            canonical_symbol="000001.SZ",
            trade_date=day,
            l1_code="801780.SI",
            l1_name="Bank",
            l2_code="801783.SI",
            l2_name="Bank category",
            reason_code=None,
            classification_receipt_hash="a" * 64,
            index_membership_receipt_hash="a" * 64,
            classification_row_hashes=["a" * 64],
            index_membership_row_hashes=[],
            alignment_state="classification_only",
            classification_research_basis="stable_taxonomy_backcast",
            non_as_known_taxonomy=True,
        )
        resolution = {"canonical_ts_code": "000001.SZ", "source_ts_code": "000001.SZ"}
        return subject.partition_entry(
            provider_row={**resolution, "trade_date": day, "row_hash": "e" * 64},
            authorities=authorities,
            provider_resolution={**resolution, "source_dataset": "market.moneyflow_ts"},
            price_resolution={**resolution, "source_dataset": "market.kline_daily_raw"},
            pit_candidates=[{"eligible_interval": ["2022-01-01", "2024-06-30"]}] * pit_count,
            price_candidates=[{"row_sha256": "f" * 64}] if price_present else [],
            sw_candidates=[projection],
        )

    return dates, authorities, entry


def test_partition_outside_domain_does_not_poison_train_only_eligibility(domain):
    days, authorities, entry = domain
    entries = [entry(days[0]), entry(days[-1], price_present=False)]
    keys = [("000001.SZ", day) for day in days[:-1]]
    partition, opportunity, eligibility = subject.build_a5_receipts(
        authorities=authorities,
        partition_entries=entries,
        opportunity_keys=keys,
        provider_keys=[("000001.SZ", days[0]), ("000001.SZ", days[-1])],
    )
    assert partition["p_all_entry_count"] == 2 and partition["p_in_entry_count"] == partition["p_out_entry_count"] == 1
    assert opportunity["opportunity_key_count"] == 9
    assert eligibility["entries"][0]["provider_absence_count"] == 1
    assert eligibility["entries"][0]["moneyflow_contributor_eligible"] is False  # 8/9, not rounded to 90%.
    with pytest.raises(FormalStateError, match="ambiguous"):
        entry(days[0], pit_count=2)
    with pytest.raises(FormalStateError, match="fully partitioned"):
        subject.build_a5_receipts(
            authorities=authorities,
            partition_entries=entries[:1],
            opportunity_keys=keys,
            provider_keys=[("000001.SZ", days[0]), ("000001.SZ", days[-1])],
        )
    with pytest.raises(StateModelSetError, match="P_out intersects"):
        subject.build_a5_receipts(
            authorities=authorities,
            partition_entries=entries,
            opportunity_keys=keys + [("000001.SZ", days[-1])],
            provider_keys=[("000001.SZ", days[0]), ("000001.SZ", days[-1])],
        )


def test_unknown_or_incomplete_mapping_manifest_cannot_be_rehashed_into_acceptance(domain):
    days, authorities, entry = domain
    partition, _, _ = subject.build_a5_receipts(
        authorities=authorities,
        partition_entries=[entry(days[0])],
        opportunity_keys=[("000001.SZ", day) for day in days],
        provider_keys=[("000001.SZ", days[0])],
    )
    for change in ("unknown_schema", "missing_projection_hash"):
        value = copy.deepcopy(partition)
        mapping = value["sw_mapping_classify_identity"]["authority"]
        if change == "unknown_schema":
            mapping["schema_version"] = "hmm_risk_pit_mapping_manifest_v4"
        else:
            mapping.pop("l2_code_projection_sha256")
        value["sw_mapping_classify_identity"] = subject.authority_identity(
            "hmm_industry_pit_classification_projection", mapping
        )
        value = receipt({k: v for k, v in value.items() if k != "receipt_sha256"})
        with pytest.raises(StateModelSetError, match="authority"):
            validate_c010_provider_absence_domain_partition(value)
    assert canonical_sha256(authorities["sw_mapping_classify_identity"]["authority"]) != "a" * 64
