from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import stock_fact_observation as subject
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, StateModelSetError


def _classifications() -> list[dict]:
    rows = [
        {
            "level": "L1",
            "index_code": f"L1-{index:02d}",
            "industry_code": f"I1-{index:02d}",
            "industry_name": f"L1 Sector {index}",
        }
        for index in range(31)
    ]
    rows.extend(
        {
            "level": "L2",
            "index_code": f"L2-{index:03d}",
            "industry_code": f"I2-{index:03d}",
            "industry_name": f"L2 Sector {index}",
        }
        for index in range(131)
    )
    return rows


def test_mapping_normalizes_equivalent_industry_and_index_codes_without_selecting_a_row() -> None:
    lookup = subject.build_classification_lookup(_classifications())
    trade_date = date(2024, 1, 2)
    rows = [
        {
            "trade_date": trade_date,
            "ts_code": "000001.SZ",
            "l1_code": "I1-00",
            "l2_code": "I2-000",
            "in_date": date(2020, 1, 1),
            "out_date": None,
        },
        {
            "trade_date": trade_date,
            "ts_code": "000001.SZ",
            "l1_code": "L1-00",
            "l2_code": "L2-000",
            "in_date": date(2021, 1, 1),
            "out_date": None,
        },
    ]

    canonical, manifest = subject.canonicalize_mapping_rows(rows, lookup)

    assert len(canonical) == 1
    assert canonical[0]["l1_code"] == "L1-00"
    assert canonical[0]["l2_code"] == "L2-000"
    assert manifest["source_row_count"] == 2
    assert manifest["canonical_row_count"] == 1

    conflicting = [*rows, {**rows[0], "l1_code": "I1-01"}]
    with pytest.raises(StateModelSetError, match="multiple canonical sector identities"):
        subject.canonicalize_mapping_rows(conflicting, lookup)


def _stock_row(index: int, *, complete: bool = True) -> dict:
    close = 10.0 + index * 0.1
    row = {
        "trade_date": date(2024, 1, 2),
        "symbol": f"000{index:03d}.SZ",
        "l1_code": "L1-00",
        "l1_name": "L1 Sector 0",
        "l2_code": f"L2-{index % 3:03d}",
        "is_suspended": False,
        "open_yuan": close - 0.1,
        "high_yuan": close + 0.2,
        "low_yuan": close - 0.2,
        "close_yuan": close,
        "prev_close_yuan": close / 1.01,
        "prev_close_5_yuan": close / 1.05,
        "prev_close_10_yuan": close / 1.10,
        "volume_shares": 1000.0 + index,
        "amount_cny": 10_000.0 + index,
        "total_mv_cny": 1_000_000.0 + index,
        "prev_circ_mv_cny": 100_000.0,
        "buy_sm_amount_cny": 100.0,
        "sell_sm_amount_cny": 80.0,
        "buy_elg_amount_cny": 200.0,
        "sell_elg_amount_cny": 150.0,
        "net_mf_amount_cny": 70.0,
        "moneyflow_fact_status": "available",
        "moneyflow_source_identity": {
            "security_identity_id": f"canonical:000{index:03d}.SZ",
            "canonical_ts_code": f"000{index:03d}.SZ",
            "source_dataset": "market.moneyflow_ts",
            "source_ts_code": f"000{index:03d}.SZ",
            "resolution_kind": "canonical_same_code",
        },
        "up_limit_yuan": close if index == 0 else close + 1.0,
    }
    if not complete:
        row["net_mf_amount_cny"] = None
        row["moneyflow_fact_status"] = "provider_absence"
        row["moneyflow_provider_absence"] = {
            "fact_status": "provider_absence",
            "canonical_ts_code": row["symbol"],
            "source_dataset": "market.moneyflow_ts",
            "source_ts_code": row["symbol"],
            "trade_date": row["trade_date"].isoformat(),
            "missing_fields": ["net_mf_amount_cny"],
            "provider_audit_receipt_sha256": "a" * 64,
            "row_hash": "b" * 64,
        }
    return row


def test_stock_fact_aggregation_is_weighted_recomputed_and_records_missing_evidence() -> None:
    rows = [_stock_row(index, complete=index != 9) for index in range(10)]

    result = subject.aggregate_l1_day(rows)

    assert not hasattr(result, "moneyflow_amount")
    assert not hasattr(result, "moneyflow_domain_status")

    assert result.count_coverage == pytest.approx(0.9)
    assert result.weight_coverage == pytest.approx(0.9)
    assert result.l1_return == pytest.approx(0.01)
    assert result.limit_up_ratio == pytest.approx(1 / 9)
    assert result.net_mf_amount == pytest.approx(9 * 70.0)
    assert result.missing_evidence == (
        {
            "symbol": "000009.SZ",
            "fields": ["net_mf_amount_cny"],
            "moneyflow_fact_status": "provider_absence",
            "moneyflow_source_identity": rows[9]["moneyflow_source_identity"],
            "moneyflow_provider_absence": rows[9]["moneyflow_provider_absence"],
        },
    )
    assert result.breadth_1d == 1.0
    assert result.breadth_5d == 1.0

    rows[8]["buy_elg_amount_cny"] = None
    with pytest.raises(StateModelSetError, match="coverage is insufficient"):
        subject.aggregate_l1_day(rows)

    rows = [_stock_row(index) for index in range(10)]
    rows[0]["prev_circ_mv_cny"] = None
    with pytest.raises(subject.ObservationCoverageError, match="denominator is incomplete") as exc_info:
        subject.aggregate_l1_day(rows)
    assert exc_info.value.l1_code == "L1-00"
    assert exc_info.value.weight_coverage == 0.0


def test_feature_domain_aggregation_excludes_only_moneyflow_contribution() -> None:
    available = _stock_row(0)
    unavailable = _stock_row(1, complete=False)
    unavailable["symbol"] = "689009.SH"
    unavailable["buy_sm_amount_cny"] = None
    unavailable["sell_sm_amount_cny"] = None
    unavailable["buy_elg_amount_cny"] = None
    unavailable["sell_elg_amount_cny"] = None

    aggregate = subject.aggregate_l1_day(
        [available, unavailable],
        moneyflow_excluded_symbols=frozenset({"689009.SH"}),
    )

    assert aggregate.count_coverage == 1.0
    assert aggregate.weight_coverage == 1.0
    assert aggregate.l1_amount == pytest.approx(available["amount_cny"] + unavailable["amount_cny"])
    assert aggregate.moneyflow_amount == pytest.approx(available["amount_cny"])
    assert aggregate.net_mf_amount == pytest.approx(available["net_mf_amount_cny"])
    assert aggregate.moneyflow_domain_status == "available"
    assert aggregate.moneyflow_excluded_symbols == ("689009.SH",)


def test_singleton_sector_keeps_price_domain_and_marks_moneyflow_structurally_unavailable() -> None:
    unavailable = _stock_row(1, complete=False)
    unavailable["symbol"] = "689009.SH"
    unavailable["buy_sm_amount_cny"] = None
    unavailable["sell_sm_amount_cny"] = None
    unavailable["buy_elg_amount_cny"] = None
    unavailable["sell_elg_amount_cny"] = None

    aggregate = subject.aggregate_l1_day(
        [unavailable],
        moneyflow_excluded_symbols=frozenset({"689009.SH"}),
    )

    assert np.isfinite(aggregate.l1_return)
    assert aggregate.net_mf_amount is None
    assert aggregate.moneyflow_amount is None
    assert aggregate.moneyflow_domain_status == "structurally_unavailable"
    assert aggregate.count_coverage == 1.0


def test_formal_feature_domain_weight_and_amount_denominators_fail_closed_with_typed_state() -> None:
    rows = [_stock_row(index) for index in range(10)]
    eligibility = {row["symbol"]: True for row in rows}
    with pytest.raises(StateModelSetError, match="hmm_risk_c010_feature_identity_drift"):
        subject.aggregate_l1_day(
            rows,
            min_coverage=0.91,
            moneyflow_contributor_eligibility=eligibility,
        )
    rows[0]["prev_circ_mv_cny"] = None
    with pytest.raises(subject.ObservationCoverageError) as exc_info:
        subject.aggregate_l1_day(rows, moneyflow_contributor_eligibility=eligibility)
    assert exc_info.value.reason_code == "hmm_risk_c010_price_domain_weight_denominator_invalid"

    rows = [_stock_row(index) for index in range(10)]
    for row in rows:
        row["amount_cny"] = 0.0
    aggregate = subject.aggregate_l1_day(rows, moneyflow_contributor_eligibility=eligibility)
    assert aggregate.moneyflow_domain_status == "denominator_invalid"
    assert aggregate.moneyflow_amount is None
    assert aggregate.net_mf_amount is None

    unseen = _stock_row(0)
    aggregate = subject.aggregate_l1_day([unseen], moneyflow_contributor_eligibility={})
    assert aggregate.moneyflow_domain_status == "structurally_unavailable"
    assert aggregate.missing_evidence[-1]["moneyflow_eligibility_status"] == "train_eligibility_unavailable"

    rows = [_stock_row(index, complete=index != 9) for index in range(10)]
    eligibility = {row["symbol"]: True for row in rows}
    exact_boundary = subject.aggregate_l1_day(rows, moneyflow_contributor_eligibility=eligibility)
    assert exact_boundary.moneyflow_domain_status == "available"
    assert exact_boundary.moneyflow_count_coverage == pytest.approx(0.9)
    assert exact_boundary.moneyflow_weight_coverage == pytest.approx(0.9)
    rows[9]["prev_circ_mv_cny"] = 200_000.0
    below_weight_boundary = subject.aggregate_l1_day(rows, moneyflow_contributor_eligibility=eligibility)
    assert below_weight_boundary.moneyflow_domain_status == "coverage_insufficient"
    assert below_weight_boundary.moneyflow_count_coverage == pytest.approx(0.9)
    assert below_weight_boundary.moneyflow_weight_coverage < 0.9


def test_diagnostic_moneyflow_features_use_only_contributor_amount() -> None:
    calendar = [date(2024, 1, 2) + timedelta(days=index) for index in range(5)]
    aggregates = []
    for trade_date in calendar:
        for code_index in range(31):
            available = _stock_row(0)
            excluded = _stock_row(1, complete=False)
            for row in (available, excluded):
                row["trade_date"] = trade_date
                row["l1_code"] = f"L1-{code_index:02d}"
                row["l1_name"] = f"L1 Sector {code_index}"
            available["amount_cny"] = 100.0
            available["buy_sm_amount_cny"] = 100.0
            available["sell_sm_amount_cny"] = 0.0
            excluded["symbol"] = "689009.SH"
            excluded["amount_cny"] = 900.0
            aggregates.append(
                subject.aggregate_l1_day(
                    [available, excluded],
                    moneyflow_excluded_symbols=frozenset({"689009.SH"}),
                )
            )

    panel, definition = subject.build_l1_feature_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns={value: 0.0 for value in calendar},
        cross_section_min_coverage=0.90,
        use_moneyflow_amount_denominator=True,
    )

    row = panel.loc[(pd.Timestamp(calendar[-1]), "L1-00")]
    assert row["moneyflow_amount"] == 100.0
    assert row["l1_amount"] == 1000.0
    assert row["small_net_ratio"] == 1.0
    assert row["sf_small_net_ratio_5d"] == 1.0
    assert set(definition["moneyflow_denominator_by_feature"].values()) == {"moneyflow_contributor_amount"}


def _aggregate(day: date, code_index: int, day_index: int) -> subject.L1DailyAggregate:
    phase = day_index / 30.0 + code_index / 20.0
    daily_return = 0.004 * np.sin(phase) + code_index * 1e-5
    return subject.L1DailyAggregate(
        trade_date=day,
        l1_code=f"L1-{code_index:02d}",
        l1_name=f"L1 Sector {code_index}",
        l2_codes=(f"L2-{code_index * 4:03d}",),
        l1_return=float(daily_return),
        l1_volume=1_000_000.0 + code_index * 10_000 + day_index,
        l1_amount=100_000_000.0 + code_index * 100_000 + day_index,
        l1_total_mv=10_000_000_000.0 + code_index * 1_000_000,
        l1_range_ratio=0.02 + code_index * 0.0001 + abs(daily_return),
        l1_true_range_ratio=0.025 + code_index * 0.0001 + abs(daily_return),
        net_mf_amount=1_000_000.0 * np.sin(phase),
        buy_sm_amount=2_000_000.0 + code_index,
        sell_sm_amount=1_900_000.0 - code_index,
        buy_elg_amount=3_000_000.0 + code_index,
        sell_elg_amount=2_800_000.0 - code_index,
        limit_up_ratio=0.01 * ((day_index + code_index) % 4),
        breadth_1d=0.45 + 0.1 * np.sin(phase),
        breadth_5d=0.48 + 0.1 * np.cos(phase),
        breadth_10d=0.5 + 0.08 * np.sin(phase / 2),
        dispersion_1d=0.01 + code_index * 0.00001,
        dispersion_5d=0.03 + code_index * 0.00002,
        dispersion_10d=0.05 + code_index * 0.00003,
        median_stock_return_1d=daily_return,
        median_stock_return_5d=daily_return * 5,
        median_stock_return_10d=daily_return * 10,
        mean_stock_return_1d=daily_return,
        mean_stock_return_5d=daily_return * 5,
        mean_stock_return_10d=daily_return * 10,
        count_coverage=1.0,
        weight_coverage=1.0,
        missing_evidence=(),
    )


def _feature_domain_aggregate(day: date, code_index: int, day_index: int) -> subject.FeatureDomainDailyAggregate:
    base = _aggregate(day, code_index, day_index)
    return subject.FeatureDomainDailyAggregate(
        **base.__dict__,
        moneyflow_amount=base.l1_amount,
        moneyflow_count_coverage=1.0,
        moneyflow_weight_coverage=1.0,
        moneyflow_domain_status="available",
    )


def test_feature_panel_recomputes_all_20_features_and_freezes_training_series() -> None:
    calendar = [item.date() for item in pd.bdate_range("2022-01-03", periods=360)]
    aggregates = [
        _aggregate(day, code_index, day_index) for day_index, day in enumerate(calendar) for code_index in range(31)
    ]
    benchmark = {day: 0.001 * np.cos(index / 40.0) for index, day in enumerate(calendar)}

    panel, definition = subject.build_l1_feature_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns=benchmark,
    )

    tail = panel.xs("L1-00", level="l1_code").iloc[-1]
    assert all(np.isfinite(float(tail[name])) for name in ALL_CORE_FEATURES)
    assert definition["all_core_features"] == list(ALL_CORE_FEATURES)
    assert definition["cross_section_required_l1_count"] == 31
    assert "diagnostic_only" not in definition
    assert "cross_section_contract" not in definition
    assert "moneyflow_denominator_by_feature" not in definition

    constituent = {
        f"L1-{index:02d}": {
            "schema_version": "pit_l2_constituents_v1",
            "l2_codes": [f"L2-{index * 4:03d}"],
        }
        for index in range(31)
    }
    frozen_identity = {
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "feature_domain_policy_sha256": "e" * 64,
    }
    series = subject.build_l1_training_series(
        panel,
        feature_names=ALL_CORE_FEATURES,
        train_start=calendar[120],
        train_end=calendar[269],
        validation_start=calendar[270],
        validation_end=calendar[329],
        constituent_manifest_by_l1=constituent,
        frozen_input_identity=frozen_identity,
    )

    assert len(series) == 31
    assert series["L1-00"].train_observations.shape == (142, 20)
    assert series["L1-00"].validation_observations.shape == (60, 20)
    carrier = series["L1-00"].validation_calendar_series
    assert carrier is not None
    assert len(carrier.calendar_dates) == 60
    assert carrier.observation_available_positions == tuple(range(60))
    assert carrier.utility_available_positions == tuple(range(60))
    assert np.isfinite(carrier.combined_utility_values_f64).all()
    assert series["L1-00"].validation_input_manifest["schema_version"] == "hmm_risk_d6_frozen_input_manifest_v2"

    sparse_panel = panel.copy()
    sparse_panel.loc[(pd.Timestamp(calendar[300]), "L1-00"), ALL_CORE_FEATURES[0]] = np.nan
    sparse = subject.build_l1_training_series(
        sparse_panel,
        feature_names=ALL_CORE_FEATURES,
        train_start=calendar[120],
        train_end=calendar[269],
        validation_start=calendar[270],
        validation_end=calendar[329],
        constituent_manifest_by_l1=constituent,
        frozen_input_identity=frozen_identity,
    )["L1-00"]
    sparse_carrier = sparse.validation_calendar_series
    assert sparse_carrier is not None
    assert len(sparse_carrier.calendar_dates) == 60
    missing_position = calendar[270:330].index(calendar[300])
    assert sparse_carrier.observation_available_mask[missing_position] is False
    assert sparse_carrier.utility_available_mask[missing_position] is True
    assert sparse_carrier.availability_ledger[missing_position]["mode"] == "transition_only"
    assert sparse_carrier.availability_ledger[missing_position]["evidence_included"] is False
    assert sparse.validation_observations.shape == (59, 20)

    legacy = subject.build_legacy_dense_diagnostic_series(
        sparse_panel,
        feature_names=ALL_CORE_FEATURES,
        train_start=calendar[120],
        train_end=calendar[269],
        validation_start=calendar[270],
        validation_end=calendar[329],
        constituent_manifest_by_l1=constituent,
        frozen_input_identity=frozen_identity,
    )["L1-00"]
    assert legacy.validation_calendar_series is None
    assert legacy.validation_observations.shape == (59, 20)
    assert legacy.validation_future_utility.shape == (59,)
    assert set(legacy.validation_future_components) == {
        "excess_return_5d",
        "excess_return_10d",
        "excess_return_20d",
    }
    legacy.validate(20)


def test_cross_sectional_features_fail_closed_when_one_l1_day_is_missing() -> None:
    calendar = [item.date() for item in pd.bdate_range("2023-01-02", periods=25)]
    aggregates = [
        _aggregate(day, code_index, day_index)
        for day_index, day in enumerate(calendar)
        for code_index in range(31)
        if not (day_index == 24 and code_index == 30)
    ]
    benchmark = {day: 0.0 for day in calendar}

    panel, _ = subject.build_l1_feature_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns=benchmark,
    )

    row = panel.loc[(pd.Timestamp(calendar[-1]), "L1-00")]
    assert np.isnan(row["sf_vol_vs_market_20d"])
    assert np.isnan(row["sf_excess_breadth_5d"])

    diagnostic, definition = subject.build_l1_feature_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns=benchmark,
        cross_section_min_coverage=0.90,
    )
    diagnostic_row = diagnostic.loc[(pd.Timestamp(calendar[-1]), "L1-00")]
    assert np.isfinite(diagnostic_row["sf_vol_vs_market_20d"])
    assert np.isfinite(diagnostic_row["sf_excess_breadth_5d"])
    assert definition["cross_section_contract"] == "coverage_aware_diagnostic"
    assert definition["cross_section_min_coverage"] == 0.90
    assert set(definition["moneyflow_denominator_by_feature"].values()) == {"l1_amount"}


def test_c010_feature_domain_panel_uses_per_feature_masks_and_never_resurrects_invalid_current_date() -> None:
    calendar = [item.date() for item in pd.bdate_range("2023-01-02", periods=12)]
    aggregates = [
        _feature_domain_aggregate(day, code_index, day_index)
        for day_index, day in enumerate(calendar)
        for code_index in range(31)
    ]
    last_day = calendar[-1]
    exact_boundary_day = calendar[-2]
    updated = []
    for aggregate in aggregates:
        if aggregate.trade_date == last_day and int(aggregate.l1_code[-2:]) >= 27:
            aggregate = replace(aggregate, l1_range_ratio=float("nan"))
        elif aggregate.trade_date == exact_boundary_day and int(aggregate.l1_code[-2:]) >= 28:
            aggregate = replace(aggregate, l1_range_ratio=float("nan"))
        if aggregate.trade_date == last_day and aggregate.l1_code == "L1-00":
            aggregate = replace(
                aggregate,
                net_mf_amount=None,
                buy_sm_amount=None,
                sell_sm_amount=None,
                buy_elg_amount=None,
                sell_elg_amount=None,
                moneyflow_amount=None,
                moneyflow_domain_status="coverage_insufficient",
            )
        updated.append(aggregate)

    panel, definition, evidence = subject.build_c010_feature_domain_panel(
        updated,
        trading_dates=calendar,
        csi300_returns={value: 0.0 for value in calendar},
    )

    exact_row = panel.loc[(pd.Timestamp(exact_boundary_day), "L1-00")]
    assert np.isfinite(exact_row["sf_range_vs_market_10d"])
    assert np.isnan(panel.loc[(pd.Timestamp(exact_boundary_day), "L1-30"), "sf_range_vs_market_10d"])
    last_row = panel.loc[(pd.Timestamp(last_day), "L1-00")]
    assert np.isfinite(last_row["volume_ratio"])
    assert np.isnan(last_row["sf_range_vs_market_10d"])
    assert np.isnan(last_row["sf_small_net_ratio_5d"])
    assert definition["schema_version"] == subject.C010_FORMULA_VERSION
    assert definition["cross_section_min_valid_sector_count"] == 28
    assert definition["range_cross_section_rolling_post_mask_required"] is True
    assert set(definition["formula_diff_by_feature"]) == {
        "net_mf_ratio",
        "elg_net_mf_ratio",
        "sf_mf_net_ratio_std_5d_neg",
        "sf_small_net_ratio_5d",
        "volume_ratio",
        "sf_range_vs_market_10d",
        "sf_vol_vs_market_20d",
        "sf_excess_breadth_5d",
    }
    receipt = next(
        item
        for item in evidence["entries"]
        if item["feature_name"] == "sf_range_vs_market_10d" and item["trade_date"] == last_day.isoformat()
    )
    assert receipt["status"] == "coverage_insufficient"
    assert receipt["valid_sector_count"] == 27
    assert receipt["post_mask_subset_of_pre_mask"] is True


def test_c010_feature_domain_panel_records_reference_invalid_without_default_value() -> None:
    calendar = [item.date() for item in pd.bdate_range("2023-01-02", periods=3)]
    aggregates = [
        replace(_feature_domain_aggregate(day, code_index, day_index), l1_volume=0.0)
        for day_index, day in enumerate(calendar)
        for code_index in range(31)
    ]

    panel, _, evidence = subject.build_c010_feature_domain_panel(
        aggregates,
        trading_dates=calendar,
        csi300_returns={value: 0.0 for value in calendar},
    )

    assert panel["volume_ratio"].isna().all()
    volume_receipts = [item for item in evidence["entries"] if item["feature_name"] == "volume_ratio"]
    assert {item["status"] for item in volume_receipts} == {"reference_invalid"}
    assert {item["source_domain"] for item in volume_receipts} == {"price"}
    all_false_mask = subject.canonical_sha256(
        [{"sector_code": f"L1-{index:02d}", "active": False} for index in range(31)]
    )
    assert {item["pre_mask_sha256"] for item in volume_receipts} == {all_false_mask}
    assert {item["post_mask_sha256"] for item in volume_receipts} == {all_false_mask}
    assert {item["reason_code"] for item in volume_receipts} == {
        "hmm_risk_c010_feature_cross_section_reference_invalid"
    }


def test_c010_domain_receipts_materialize_every_frozen_level_sector_date_identity() -> None:
    body = {
        "schema_version": subject.C010_AGGREGATE_RECEIPT_VERSION,
        "l1_aggregate_count": 0,
        "l2_aggregate_count": 0,
        "l1_domain_receipts": [],
        "l2_domain_receipts": [],
        "l1_invalid_price_domain": [],
        "l2_invalid_price_domain": [],
        "formal_policy_activated": True,
    }
    source = {**body, "receipt_sha256": subject.canonical_sha256(body)}
    trading_dates = (date(2024, 1, 2), date(2024, 1, 3))
    evidence = subject.complete_c010_domain_receipts(
        source,
        trading_dates=trading_dates,
        l1_sector_codes=[f"L1-{index:02d}" for index in range(31)],
        l2_sector_codes=[f"L2-{index:03d}" for index in range(131)],
    )

    assert evidence["l1_domain_expected_count"] == 62
    assert evidence["l1_domain_receipt_count"] == 62
    assert evidence["l2_domain_expected_count"] == 262
    assert evidence["l2_domain_receipt_count"] == 262
    assert len(evidence["l1_invalid_price_domain"]) == 62
    assert len(evidence["l2_invalid_price_domain"]) == 262
    assert {entry["price_domain_reason_code"] for entry in evidence["l2_invalid_price_domain"]} == {
        "hmm_risk_c010_expected_opportunity_missing"
    }
    assert evidence["receipt_sha256"] == subject.canonical_sha256(
        {key: value for key, value in evidence.items() if key != "receipt_sha256"}
    )


def test_direct_l2_projection_uses_canonical_stock_fact_identity_without_mutating_l1() -> None:
    source = {
        "trade_date": date(2024, 1, 2),
        "symbol": "000001.SZ",
        "l1_code": "801010.SI",
        "l1_name": "农林牧渔",
        "l2_code": "801012.SI",
        "l2_name": "农产品加工",
    }
    projected = subject.project_stock_fact_rows_for_direct_level([source], sector_level="L2")
    assert projected[0]["l1_code"] == "801012.SI"
    assert projected[0]["l1_name"] == "农产品加工"
    assert source["l1_code"] == "801010.SI"

    with pytest.raises(subject.StateModelSetError, match="direct L2 stock fact identity is incomplete"):
        subject.project_stock_fact_rows_for_direct_level(
            [{**source, "l2_code": ""}],
            sector_level="L2",
        )
