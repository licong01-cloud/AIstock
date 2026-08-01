from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk.observation_eligibility import (
    audit_feature_mask_candidates,
    build_expected_opportunity_receipt,
    build_provider_absence_domain_partition,
    build_train_only_observation_eligibility,
    canonical_authority_identity,
    load_feature_domain_direct_aggregates,
)
from backend.services.hmm_risk.provider_absence import (
    MONEYFLOW_DATASET,
    MONEYFLOW_MISSING_FIELDS,
    ProviderAbsenceEvidence,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256
from backend.services.hmm_risk.stock_fact_observation import (
    C010_POLICY_VERSION,
    C010_POLICY_VERSION_V1,
    _validate_c010_eligibility_receipt,
    validate_c010_provider_absence_domain_partition,
)


def _absence(symbol: str, trade_date: date) -> ProviderAbsenceEvidence:
    body = {
        "canonical_ts_code": symbol,
        "source_dataset": MONEYFLOW_DATASET,
        "source_ts_code": symbol,
        "trade_date": trade_date.isoformat(),
        "missing_fields": list(MONEYFLOW_MISSING_FIELDS),
        "provider_audit_receipt_sha256": "a" * 64,
    }
    return ProviderAbsenceEvidence(
        canonical_ts_code=symbol,
        source_dataset=MONEYFLOW_DATASET,
        source_ts_code=symbol,
        trade_date=trade_date,
        missing_fields=MONEYFLOW_MISSING_FIELDS,
        provider_audit_receipt_sha256="a" * 64,
        row_hash=canonical_sha256(body),
    )


def _authority(label: str) -> dict:
    return canonical_authority_identity(label, {"version": "test-v1", "label": label})


def _build_eligibility(
    absences: list[ProviderAbsenceEvidence],
    expected: dict[str, tuple[date, ...]],
    *,
    train_start: date,
    train_end: date = date(2024, 6, 30),
    statuses: dict[tuple[str, date], dict[str, str]] | None = None,
    minimum_availability_ratio: float = 0.9,
):
    authorities = {
        "provider": _authority("provider_absence_manifest"),
        "resolver": _authority("security_source_identity_manifest"),
        "pit": _authority("stock_universe_pit_state_and_spans"),
        "price": _authority("market.kline_daily_raw"),
        "sw": _authority("sw_index_member_and_classify_mapping"),
    }
    predicate_evidence = {}
    for row in absences:
        overrides = (statuses or {}).get((row.canonical_ts_code, row.trade_date), {})
        resolver_receipt = {
            "security_resolver_identity_sha256": authorities["resolver"]["identity_sha256"],
            "provider_absence_source_resolution": {
                "canonical_ts_code": row.canonical_ts_code,
                "source_dataset": "market.moneyflow_ts",
                "source_ts_code": row.source_ts_code,
            },
            "price_source_resolution": {
                "canonical_ts_code": row.canonical_ts_code,
                "source_dataset": "market.kline_daily_raw",
                "source_ts_code": row.canonical_ts_code,
            },
        }
        predicate_evidence[(row.canonical_ts_code, row.trade_date)] = {
            "source_ts_code": row.source_ts_code,
            "stable_security_identity": f"canonical:{row.canonical_ts_code}",
            "security_resolver_receipt": resolver_receipt,
            **{
                name: {
                    "status": overrides.get(name, "available"),
                    "authority_receipt": {
                        "authority_identity_sha256": authorities[
                            {
                                "pit_eligible": "pit",
                                "price_authority_present": "price",
                                "sw_l1_identity_valid": "sw",
                                "sw_l2_identity_valid": "sw",
                            }[name]
                        ]["identity_sha256"],
                        "name": name,
                    },
                }
                for name in (
                    "pit_eligible",
                    "price_authority_present",
                    "sw_l1_identity_valid",
                    "sw_l2_identity_valid",
                )
            },
        }
    partition = build_provider_absence_domain_partition(
        absences,
        predicate_evidence_by_key=predicate_evidence,
        train_start=train_start,
        train_end=train_end,
        provider_absence_manifest_identity=authorities["provider"],
        security_resolver_identity=authorities["resolver"],
        pit_authority_identity=authorities["pit"],
        price_source_identity=authorities["price"],
        sw_mapping_classify_identity=authorities["sw"],
        formal_policy=True,
    )
    opportunity = build_expected_opportunity_receipt(
        expected,
        train_start=train_start,
        train_end=train_end,
        authority_identities=[authorities["resolver"], authorities["pit"], authorities["price"], authorities["sw"]],
    )
    return build_train_only_observation_eligibility(
        absences,
        expected_opportunity_receipt=opportunity,
        provider_absence_partition_receipt=partition,
        train_start=train_start,
        train_end=train_end,
        minimum_availability_ratio=minimum_availability_ratio,
    )


def test_train_only_eligibility_excludes_structural_absence_without_changing_stock_universe() -> None:
    start = date(2022, 1, 1)
    absences = [_absence("689009.SH", start + timedelta(days=index)) for index in range(9)]
    absences.append(_absence("603595.SH", start))

    result = _build_eligibility(
        absences,
        {
            "000001.SZ": tuple(start + timedelta(days=index) for index in range(20)),
            "689009.SH": tuple(start + timedelta(days=index) for index in range(10)),
            "603595.SH": tuple(start + timedelta(days=index) for index in range(100)),
        },
        train_start=start,
    )

    assert result.excluded_moneyflow_symbols == frozenset({"689009.SH"})
    evidence = result.evidence()
    assert evidence["pit_universe_changed"] is False
    assert evidence["selection_universe_changed"] is False
    assert evidence["runtime_prediction_eligibility_changed"] is False
    assert evidence["diagnostic_only"] is True
    assert evidence["entry_count"] == 3
    by_symbol = {entry["canonical_ts_code"]: entry for entry in evidence["entries"]}
    assert by_symbol["000001.SZ"]["provider_absence_count"] == 0
    assert by_symbol["000001.SZ"]["availability_ratio"] == 1.0
    assert by_symbol["000001.SZ"]["moneyflow_contributor_eligible"] is True
    assert evidence["entries"][0]["expected_opportunity_contract"] == ("hmm_risk_c010_expected_opportunity_dates_v2")
    assert evidence["provider_absence_partition_receipt"]["partition_complete"] is True
    assert len(evidence["entries"][0]["expected_opportunity_date_sha256"]) == 64
    assert evidence["receipt_sha256"] == result.evidence()["receipt_sha256"]


def test_train_only_eligibility_rejects_missing_or_inconsistent_denominator() -> None:
    row = _absence("689009.SH", date(2022, 1, 4))

    with pytest.raises(StateModelSetError, match="opportunity ledger is empty or invalid"):
        _build_eligibility(
            [row],
            {},
            train_start=date(2022, 1, 1),
        )


def test_train_only_eligibility_uses_exact_integer_ninety_percent_boundary_and_formal_receipt() -> None:
    start = date(2022, 1, 1)
    dates = tuple(start + timedelta(days=index) for index in range(10))
    result = _build_eligibility(
        [_absence("000001.SZ", dates[0]), _absence("000002.SZ", dates[0]), _absence("000002.SZ", dates[1])],
        {"000001.SZ": dates, "000002.SZ": dates},
        train_start=start,
    )

    by_symbol = result.moneyflow_contributor_eligibility
    assert by_symbol == {"000001.SZ": True, "000002.SZ": False}
    formal = result.evidence(formal_policy=True)
    assert formal["diagnostic_only"] is False
    assert formal["formal_policy_activated"] is True
    assert formal["availability_integer_contract"] == "10*(expected-missing) >= 9*expected"
    with pytest.raises(StateModelSetError, match="hmm_risk_c010_policy_identity_mismatch"):
        _build_eligibility(
            [],
            {"000001.SZ": dates},
            train_start=start,
            minimum_availability_ratio=0.95,
        )


def test_train_only_eligibility_partitions_sw_domain_out_without_counting_it_as_available() -> None:
    start = date(2022, 1, 1)
    out_date = start + timedelta(days=1)
    result = _build_eligibility(
        [_absence("689009.SH", out_date)],
        {"000001.SZ": (start,)},
        train_start=start,
        statuses={("689009.SH", out_date): {"sw_l1_identity_valid": "unavailable"}},
    )

    assert [entry.canonical_ts_code for entry in result.entries] == ["000001.SZ"]
    partition = result.provider_absence_partition_receipt
    assert partition["p_in_entry_count"] == 0
    assert partition["p_out_entry_count"] == 1
    assert partition["entries"][0]["primary_reason_code"] == ("hmm_risk_c010_sw_identity_unavailable_for_opportunity")


def test_domain_partition_keeps_same_symbol_in_and_out_keys_and_counts_only_p_in() -> None:
    start = date(2022, 1, 1)
    in_date = start
    out_date = start + timedelta(days=1)
    result = _build_eligibility(
        [_absence("000001.SZ", in_date), _absence("000001.SZ", out_date)],
        {"000001.SZ": (in_date, out_date)},
        train_start=start,
        statuses={("000001.SZ", out_date): {"sw_l2_identity_valid": "unavailable"}},
    )

    entry = result.entries[0]
    assert entry.expected_opportunity_count == 2
    assert entry.provider_absence_count == 1
    assert entry.availability_ratio == 0.5
    assert entry.moneyflow_contributor_eligible is False
    assert [item["partition"] for item in result.provider_absence_partition_receipt["entries"]] == [
        "in_domain",
        "out_of_domain",
    ]


def test_domain_partition_preserves_all_failed_predicates_and_primary_reason_priority() -> None:
    key_date = date(2022, 1, 4)
    result = _build_eligibility(
        [_absence("689009.SH", key_date)],
        {"000001.SZ": (key_date,)},
        train_start=date(2022, 1, 1),
        statuses={
            ("689009.SH", key_date): {
                "pit_eligible": "unavailable",
                "price_authority_present": "unavailable",
                "sw_l1_identity_valid": "unavailable",
                "sw_l2_identity_valid": "unavailable",
            }
        },
    )

    entry = result.provider_absence_partition_receipt["entries"][0]
    assert entry["failed_predicates"] == [
        "pit_eligible",
        "price_authority_present",
        "sw_l1_identity_valid",
        "sw_l2_identity_valid",
    ]
    assert entry["primary_reason_code"] == "hmm_risk_c010_pit_ineligible_for_opportunity"


def test_domain_partition_invalid_predicate_fails_closed_instead_of_becoming_p_out() -> None:
    key_date = date(2022, 1, 4)
    with pytest.raises(StateModelSetError, match="invalid predicate cannot become P_out"):
        _build_eligibility(
            [_absence("689009.SH", key_date)],
            {"000001.SZ": (key_date,)},
            train_start=date(2022, 1, 1),
            statuses={("689009.SH", key_date): {"sw_l1_identity_valid": "invalid"}},
        )


def test_domain_partition_readback_rejects_nested_hash_drift() -> None:
    key_date = date(2022, 1, 4)
    result = _build_eligibility(
        [_absence("000001.SZ", key_date)],
        {"000001.SZ": (key_date,)},
        train_start=date(2022, 1, 1),
    )
    tampered = deepcopy(result.provider_absence_partition_receipt)
    tampered["entries"][0]["pit_eligible"]["authority_receipt"]["tampered"] = True

    with pytest.raises(StateModelSetError, match="canonical identity is invalid"):
        validate_c010_provider_absence_domain_partition(tampered)


def test_eligibility_v2_writer_and_readback_share_authority_while_v1_remains_read_only() -> None:
    key_date = date(2022, 1, 4)
    result = _build_eligibility(
        [_absence("000001.SZ", key_date)],
        {"000001.SZ": (key_date,)},
        train_start=date(2022, 1, 1),
    )
    v2 = result.evidence(formal_policy=True)
    ledger, excluded = _validate_c010_eligibility_receipt(v2, policy_version=C010_POLICY_VERSION)
    assert ledger == v2["entries"]
    assert excluded == ["000001.SZ"]

    v1_entry_body = {
        "canonical_ts_code": "000001.SZ",
        "expected_opportunity_count": 1,
        "expected_opportunity_contract": "hmm_risk_c010_expected_opportunity_dates_v1",
        "expected_opportunity_date_sha256": canonical_sha256(["2022-01-04"]),
        "provider_absence_count": 1,
        "availability_ratio": 0.0,
        "moneyflow_contributor_eligible": False,
        "provider_absence_key_sha256": canonical_sha256(
            [{"canonical_ts_code": "000001.SZ", "trade_date": "2022-01-04", "row_hash": "a" * 64}]
        ),
    }
    v1_entry = {**v1_entry_body, "entry_sha256": canonical_sha256(v1_entry_body)}
    v1_body = {
        "schema_version": "hmm_risk_c010_train_observation_eligibility_v1",
        "train_start": "2022-01-01",
        "train_end": "2024-06-30",
        "minimum_availability_ratio": 0.9,
        "availability_integer_contract": "10*(expected-missing) >= 9*expected",
        "entry_count": 1,
        "entries": [v1_entry],
        "excluded_moneyflow_symbols": ["000001.SZ"],
        "pit_universe_changed": False,
        "selection_universe_changed": False,
        "runtime_prediction_eligibility_changed": False,
        "diagnostic_only": False,
        "formal_policy_activated": True,
    }
    v1 = {**v1_body, "receipt_sha256": canonical_sha256(v1_body)}
    historical_ledger, historical_excluded = _validate_c010_eligibility_receipt(
        v1, policy_version=C010_POLICY_VERSION_V1
    )
    assert historical_ledger == [v1_entry]
    assert historical_excluded == ["000001.SZ"]


def test_feature_mask_candidate_keeps_full_features_or_excludes_only_moneyflow_domain() -> None:
    dates = pd.date_range("2022-01-01", periods=130, freq="D")
    index = pd.MultiIndex.from_product([dates, ["L2-A", "L2-B"]], names=["trade_date", "l1_code"])
    panel = pd.DataFrame(
        {
            "daily_return": np.linspace(0.0, 0.1, len(index)),
            "net_mf_ratio": np.linspace(0.0, 0.2, len(index)),
        },
        index=index,
    )
    panel.loc[(slice(None), "L2-B"), "net_mf_ratio"] = np.nan

    report = audit_feature_mask_candidates(
        panel,
        family="legacy_covfix",
        feature_names=("daily_return", "net_mf_ratio"),
        train_start=date(2022, 1, 1),
        train_end=date(2022, 5, 30),
        direct_sector_level="L2",
        expected_sector_count=2,
        moneyflow_unavailable_sector_codes={"L2-B"},
    )

    by_code = {entry["sector_code"]: entry for entry in report["entries"]}
    assert by_code["L2-A"]["status"] == "full_feature_set"
    assert by_code["L2-A"]["feature_mask"] == ["daily_return", "net_mf_ratio"]
    assert by_code["L2-B"]["status"] == "moneyflow_domain_excluded_candidate"
    assert by_code["L2-B"]["feature_mask"] == ["daily_return"]
    assert report["feature_mask_candidate_valid"] is True
    assert report["fit_performed"] is False
    assert report["formal_policy_activated"] is False


def test_feature_mask_does_not_rescue_unrelated_or_mandatory_feature_gaps() -> None:
    dates = pd.date_range("2022-01-01", periods=130, freq="D")
    index = pd.MultiIndex.from_product([dates, ["L2-A"]], names=["trade_date", "l1_code"])
    panel = pd.DataFrame({"daily_return": np.nan, "net_mf_ratio": np.nan}, index=index)

    report = audit_feature_mask_candidates(
        panel,
        family="legacy_covfix",
        feature_names=("daily_return", "net_mf_ratio"),
        train_start=date(2022, 1, 1),
        train_end=date(2022, 5, 30),
        direct_sector_level="L2",
        expected_sector_count=1,
        moneyflow_unavailable_sector_codes={"L2-A"},
    )

    assert report["feature_mask_candidate_valid"] is False
    assert report["entries"][0]["status"] == "blocked_insufficient_train_rows"


def _stock_fact_row(symbol: str, *, moneyflow_available: bool) -> dict:
    moneyflow = 10.0 if moneyflow_available else None
    return {
        "trade_date": date(2024, 1, 2),
        "symbol": symbol,
        "l1_code": "L1-A",
        "l1_name": "L1 A",
        "l2_code": "L2-A",
        "l2_name": "L2 A",
        "is_suspended": False,
        "open_yuan": 10.0,
        "high_yuan": 10.5,
        "low_yuan": 9.5,
        "close_yuan": 10.2,
        "prev_close_yuan": 10.0,
        "prev_close_5_yuan": 9.8,
        "prev_close_10_yuan": 9.5,
        "volume_shares": 1_000.0,
        "amount_cny": 10_000.0,
        "total_mv_cny": 1_000_000.0,
        "prev_circ_mv_cny": 800_000.0,
        "buy_sm_amount_cny": moneyflow,
        "sell_sm_amount_cny": moneyflow,
        "buy_elg_amount_cny": moneyflow,
        "sell_elg_amount_cny": moneyflow,
        "net_mf_amount_cny": moneyflow,
        "up_limit_yuan": 11.0,
    }


def test_direct_feature_domain_loader_keeps_excluded_stock_in_price_and_sector_identity() -> None:
    start = date(2022, 1, 1)
    eligibility = _build_eligibility(
        [_absence("689009.SH", start + timedelta(days=index)) for index in range(9)],
        {
            "000001.SZ": tuple(start + timedelta(days=index) for index in range(10)),
            "689009.SH": tuple(start + timedelta(days=index) for index in range(10)),
        },
        train_start=start,
    )

    class Reader:
        @staticmethod
        def iter_missing_price_rows():
            return iter(())

        @staticmethod
        def iter_stock_fact_rows():
            return iter(
                [
                    _stock_fact_row("000001.SZ", moneyflow_available=True),
                    _stock_fact_row("689009.SH", moneyflow_available=False),
                ]
            )

    l1, l2, evidence = load_feature_domain_direct_aggregates(Reader(), eligibility)

    assert l1[0].l1_amount == 20_000.0
    assert l1[0].moneyflow_amount == 10_000.0
    assert l1[0].moneyflow_excluded_symbols == ("689009.SH",)
    assert l2[0].l1_code == "L2-A"
    assert evidence["impacted_l1_codes"] == ["L1-A"]
    assert evidence["impacted_l2_codes"] == ["L2-A"]
    assert evidence["formal_policy_activated"] is False
    assert evidence["l1_aggregate_count"] == len(evidence["l1_domain_receipts"])
    l1_receipt = evidence["l1_domain_receipts"][0]
    assert l1_receipt["price_expected_symbols"] == ["000001.SZ", "689009.SH"]
    assert l1_receipt["price_complete_symbols"] == ["000001.SZ", "689009.SH"]
    assert l1_receipt["moneyflow_expected_symbols"] == ["000001.SZ"]
    assert l1_receipt["moneyflow_complete_symbols"] == ["000001.SZ"]
    assert l1_receipt["moneyflow_domain_status"] == "available"
    assert l1_receipt["entry_sha256"] == canonical_sha256(
        {key: value for key, value in l1_receipt.items() if key != "entry_sha256"}
    )


def test_direct_feature_domain_loader_rejects_train_symbol_missing_from_full_universe_ledger() -> None:
    eligibility = _build_eligibility(
        [],
        {"000001.SZ": (date(2024, 1, 2),)},
        train_start=date(2022, 1, 1),
    )

    class Reader:
        @staticmethod
        def iter_missing_price_rows():
            return iter(())

        @staticmethod
        def iter_stock_fact_rows():
            return iter([_stock_fact_row("000002.SZ", moneyflow_available=True)])

    with pytest.raises(StateModelSetError, match="hmm_risk_c010_contributor_receipt_mismatch"):
        load_feature_domain_direct_aggregates(Reader(), eligibility, formal_policy=True)


def test_direct_feature_domain_loader_keeps_post_train_symbol_with_unavailable_moneyflow_status() -> None:
    eligibility = _build_eligibility(
        [],
        {"000001.SZ": (date(2024, 1, 2),)},
        train_start=date(2022, 1, 1),
    )
    row = _stock_fact_row("000002.SZ", moneyflow_available=True)
    row["trade_date"] = date(2024, 7, 1)

    class Reader:
        @staticmethod
        def iter_missing_price_rows():
            return iter(())

        @staticmethod
        def iter_stock_fact_rows():
            return iter([row])

    l1, _, evidence = load_feature_domain_direct_aggregates(Reader(), eligibility, formal_policy=True)
    assert l1[0].moneyflow_domain_status == "structurally_unavailable"
    assert l1[0].missing_evidence[-1]["reason_code"] == "hmm_risk_c010_train_eligibility_unavailable"
    assert evidence["train_eligibility_unavailable_symbols"] == ["000002.SZ"]
    assert evidence["impacted_l2_codes"] == ["L2-A"]
    assert evidence["formal_policy_activated"] is True
