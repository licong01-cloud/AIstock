from __future__ import annotations

import copy
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
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


def test_file_a5_uses_raw_presence_not_numeric_completeness_and_closes_provider_keys(domain, monkeypatch):
    from backend.services.hmm_risk import formal_state_input as inputs, rotation_l1_input_bundle as reader

    days, authorities, entry = domain
    raw = np.zeros(10, dtype=reader._QLIB_SOURCE_DTYPE)
    raw["trade_date"] = [int(day.replace("-", "")) for day in days]
    raw["symbol"] = b"000001.SZ"
    for field in reader.QLIB_STOCK_FIELDS:
        raw[field] = 1.0
        raw[field][-1] = np.nan  # A legal empty sentinel is NOT a price row.
    raw[reader.QLIB_STOCK_FIELDS[0]][1] = 0.0  # Row presence is not a positivity/feature-completeness test.
    monkeypatch.setattr(reader, "_read_spooled_month", lambda path: raw)
    projection = entry(days[0])["sw_l1_identity_valid"]["authority_receipt"]["candidates"][0]
    projector = SimpleNamespace(
        resolve=lambda symbol, day: SimpleNamespace(status="resolved", as_dict=lambda: projection)
    )
    security = SimpleNamespace(
        resolve=lambda symbol, day, dataset: SimpleNamespace(
            evidence=lambda: {"canonical_ts_code": symbol, "source_ts_code": symbol, "source_dataset": dataset}
        )
    )
    provider = SimpleNamespace(
        rows=[
            SimpleNamespace(
                canonical_ts_code="000001.SZ",
                trade_date=date.fromisoformat(day),
                source_dataset="market.moneyflow_ts",
                evidence=lambda day=day: {
                    "canonical_ts_code": "000001.SZ",
                    "source_ts_code": "000001.SZ",
                    "trade_date": day,
                    "row_hash": "e" * 64,
                },
            )
            for day in (days[0], days[-1])
        ]
    )
    args = dict(
        month_paths=[Path("unused.bin")],
        spans={"000001.SZ": [(date(2022, 1, 1), date(2024, 6, 30))]},
        security=security,
        projection=projector,
        provider=provider,
        authorities=authorities,
        calendar=[date.fromisoformat(day) for day in days],
    )
    partition, opportunity, eligibility = inputs._source_a5(**args)
    assert opportunity["opportunity_key_count"] == 9
    assert partition["p_in_entry_count"] == partition["p_out_entry_count"] == 1
    assert eligibility["entries"][0]["provider_absence_count"] == 1
    assert (
        partition["entries"][-1]["sw_l1_identity_valid"]["authority_receipt"]["candidates"][0]["trade_date"] == days[-1]
    )
    # Physical order is irrelevant; canonical source reader independently rejects duplicates.
    monkeypatch.setattr(reader, "_read_spooled_month", lambda path: raw[::-1])
    assert inputs._source_a5(**args) == (partition, opportunity, eligibility)
    with pytest.raises(FormalStateError, match="ambiguous PIT"):
        inputs._source_a5(**{**args, "spans": {"000001.SZ": args["spans"]["000001.SZ"] * 2}})
    with pytest.raises(FormalStateError, match="non-calendar"):
        inputs._source_a5(**{**args, "calendar": args["calendar"][:-1]})
    raw[reader.QLIB_STOCK_FIELDS[0]][1] = np.nan
    with pytest.raises(reader.RotationL1InputBundleError, match="partially finite"):
        inputs._source_a5(**args)


def test_formal_circ_mv_allows_approved_cross_entry_but_rejects_future_or_broken_lineage():
    from backend.services.hmm_risk import formal_state_input as inputs

    calendar = [date(2021, 12, 31), date(2022, 1, 4)]
    row = {
        "symbol": "000001.SZ",
        "is_suspended": False,
        "circ_mv_fact_status": "available",
        "circ_mv_source_date": calendar[0],
        "circ_mv_pit_eligible_start": calendar[1],
        "circ_mv_crossed_pit_entry_boundary": True,
        "circ_mv_staleness_trading_days": 1,
        "circ_mv_history_start": inputs.SOURCE_START,
        "circ_mv_lookback_contract_version": "hmm_risk_causal_circ_mv_source_window_v1",
    }
    crossing = inputs._circ_mv_crossings(calendar[1], [row], calendar)
    assert crossing[0]["source_date"] == "2021-12-31"
    for mutation in (
        {"circ_mv_source_date": calendar[1]},
        {"circ_mv_staleness_trading_days": 0},
        {"circ_mv_crossed_pit_entry_boundary": False},
        {"circ_mv_history_start": calendar[1]},
    ):
        with pytest.raises(FormalStateError, match="lineage"):
            inputs._circ_mv_crossings(calendar[1], [{**row, **mutation}], calendar)


def test_file_collector_retains_independent_price_and_moneyflow_receipts():
    from backend.services.hmm_risk import formal_state_input as inputs
    from backend.tests.hmm_risk.test_stock_fact_observation import _stock_row

    rows = [{**_stock_row(i), "l2_code": "801783.SI", "l2_name": "L2"} for i in range(10)]
    eligibility = {row["symbol"]: True for row in rows}
    eligibility[rows[-1]["symbol"]] = False
    for field in ("net_mf_amount_cny", "buy_elg_amount_cny"):
        rows[-1][field] = None  # The train-only exclusion does not delete its valid price row.
    aggregates = {"L1": [], "L2": []}
    evidence = {
        f"{prefix}_{kind}": [] for prefix in ("l1", "l2") for kind in ("domain_receipts", "invalid_price_domain")
    }
    args = dict(eligibility=eligibility, aggregates=aggregates, evidence=evidence)
    inputs._collect_domains(rows[0]["trade_date"], rows, **args)
    for prefix in ("l1", "l2"):
        item = evidence[f"{prefix}_domain_receipts"][0]
        assert len(item["price_expected_symbols"]) == 10
        assert len(item["moneyflow_expected_symbols"]) == 9
        assert item["moneyflow_domain_status"] == "available"
        assert item["moneyflow_contributor_amount"] == sum(row["amount_cny"] for row in rows[:-1])
    broken = [{**row, "prev_circ_mv_cny": None} if i == 0 else row for i, row in enumerate(rows)]
    inputs._collect_domains(rows[0]["trade_date"], broken, **args)
    assert evidence["l2_invalid_price_domain"][0]["price_expected_weight"] is None
    assert (
        evidence["l2_invalid_price_domain"][0]["price_domain_reason_code"]
        == "hmm_risk_c010_price_domain_weight_denominator_invalid"
    )
    assert evidence["l2_invalid_price_domain"][0]["missing_evidence"]
    double_failure = [dict(row) for row in broken]
    double_failure[1]["close_yuan"] = None
    inputs._collect_domains(rows[0]["trade_date"], double_failure, **args)
    item = evidence["l2_invalid_price_domain"][-1]
    assert len(item["price_complete_symbols"]) == 8
    assert item["price_count_coverage"] == 0.8 and item["price_weight_coverage"] is None
    assert {entry["symbol"] for entry in item["missing_evidence"]} == {rows[0]["symbol"], rows[1]["symbol"]}
