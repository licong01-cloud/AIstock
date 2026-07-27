from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk.observation_eligibility import (
    audit_feature_mask_candidates,
    build_train_only_observation_eligibility,
    load_feature_domain_direct_aggregates,
)
from backend.services.hmm_risk.provider_absence import (
    MONEYFLOW_DATASET,
    MONEYFLOW_MISSING_FIELDS,
    ProviderAbsenceEvidence,
)
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


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


def test_train_only_eligibility_excludes_structural_absence_without_changing_stock_universe() -> None:
    start = date(2022, 1, 1)
    absences = [_absence("689009.SH", start + timedelta(days=index)) for index in range(9)]
    absences.append(_absence("603595.SH", start))

    result = build_train_only_observation_eligibility(
        absences,
        expected_opportunity_dates_by_symbol={
            "689009.SH": tuple(start + timedelta(days=index) for index in range(10)),
            "603595.SH": tuple(start + timedelta(days=index) for index in range(100)),
        },
        train_start=start,
        train_end=date(2024, 6, 30),
    )

    assert result.excluded_moneyflow_symbols == frozenset({"689009.SH"})
    evidence = result.evidence()
    assert evidence["pit_universe_changed"] is False
    assert evidence["selection_universe_changed"] is False
    assert evidence["runtime_prediction_eligibility_changed"] is False
    assert evidence["diagnostic_only"] is True
    assert evidence["entries"][0]["expected_opportunity_contract"] == ("hmm_risk_c010_expected_opportunity_dates_v1")
    assert len(evidence["entries"][0]["expected_opportunity_date_sha256"]) == 64
    assert evidence["receipt_sha256"] == result.evidence()["receipt_sha256"]


def test_train_only_eligibility_rejects_missing_or_inconsistent_denominator() -> None:
    row = _absence("689009.SH", date(2022, 1, 4))

    with pytest.raises(StateModelSetError, match="expected opportunity count is invalid"):
        build_train_only_observation_eligibility(
            [row],
            expected_opportunity_dates_by_symbol={},
            train_start=date(2022, 1, 1),
            train_end=date(2024, 6, 30),
        )


def test_train_only_eligibility_rejects_absence_outside_exact_opportunity_dates() -> None:
    start = date(2022, 1, 1)

    with pytest.raises(StateModelSetError, match="provider absence is outside expected opportunities"):
        build_train_only_observation_eligibility(
            [_absence("689009.SH", start + timedelta(days=1))],
            expected_opportunity_dates_by_symbol={"689009.SH": (start,)},
            train_start=start,
            train_end=date(2024, 6, 30),
        )


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
    eligibility = build_train_only_observation_eligibility(
        [_absence("689009.SH", start + timedelta(days=index)) for index in range(9)],
        expected_opportunity_dates_by_symbol={"689009.SH": tuple(start + timedelta(days=index) for index in range(10))},
        train_start=start,
        train_end=date(2024, 6, 30),
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
