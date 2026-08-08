from __future__ import annotations

import json
import math
from collections import deque
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk.b3_acceptance import (
    D3_CONTRACT_VERSION,
    D5_SELECTION_VERSION,
    RESTART_SCHEDULE,
    evaluate_covariance_acceptance,
    evaluate_likelihood_acceptance,
    evaluate_semantic_validation,
    evaluate_train_occupancy,
)
from backend.services.hmm_risk.b3_training import (
    B3TrainingStageError,
    B3TrainOnlySeries,
    B3FittedModel,
    audit_train_only_coverage,
    build_train_only_series,
    models_from_repeat,
    read_b3_ready_model_set,
    read_b3_selected_level_artifact,
    run_level_repeat,
    write_b3_ready_model_set,
)
from backend.services.hmm_risk.b3_mixed_dimension import (
    MIXED_DIMENSION_CONTRACT_VERSION,
    MIXED_LEVEL_SCHEMA_VERSION,
    MIXED_MODEL_SCHEMA_VERSION,
    MIXED_REPEAT_SCHEMA_VERSION,
    MIXED_TRAINING_ENTRY_SCHEMA_VERSION,
    TARGET_SECTOR,
    TARGET_SOURCE_PROFILE_RECEIPT_SHA256,
    build_level_dimension_identity,
    build_projection_receipt,
)
from backend.services.hmm_risk import b3_training as training_subject
from backend.services.hmm_risk.state_model_set import (
    StateModelSetError,
    canonical_json_bytes,
    canonical_sha256,
)
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, BASE_FEATURES
from backend.services.hmm_risk.stock_fact_observation import (
    C010_APPROVED_TRAIN_END,
    C010_APPROVED_TRAIN_START,
    C010_CROSS_SECTION_FEATURES,
    C010_CROSS_SECTION_OPERATORS,
    C010_FORMULA_DIFF_BY_FEATURE,
    C010_FORMULA_VERSION,
    C010_MONEYFLOW_DENOMINATOR_BY_FEATURE,
    validate_c010_policy_manifest,
)


APPROVED_TRAIN_EXCHANGE_HOLIDAYS = {
    "2022-01-03",
    "2022-01-31",
    "2022-02-01",
    "2022-02-02",
    "2022-02-03",
    "2022-02-04",
    "2022-04-04",
    "2022-04-05",
    "2022-05-02",
    "2022-05-03",
    "2022-05-04",
    "2022-06-03",
    "2022-09-12",
    "2022-10-03",
    "2022-10-04",
    "2022-10-05",
    "2022-10-06",
    "2022-10-07",
    "2023-01-02",
    "2023-01-23",
    "2023-01-24",
    "2023-01-25",
    "2023-01-26",
    "2023-01-27",
    "2023-04-05",
    "2023-05-01",
    "2023-05-02",
    "2023-05-03",
    "2023-06-22",
    "2023-06-23",
    "2023-09-29",
    "2023-10-02",
    "2023-10-03",
    "2023-10-04",
    "2023-10-05",
    "2023-10-06",
    "2024-01-01",
    "2024-02-09",
    "2024-02-12",
    "2024-02-13",
    "2024-02-14",
    "2024-02-15",
    "2024-02-16",
    "2024-04-04",
    "2024-04-05",
    "2024-05-01",
    "2024-05-02",
    "2024-05-03",
    "2024-06-10",
}


def _train_manifest(
    dates: tuple[date, ...],
    observations: np.ndarray,
    *,
    sector_code: str = "S001",
    direct_sector_level: str = "L1",
) -> dict:
    values = [item.isoformat() for item in dates]
    return {
        "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
        "direct_sector_level": direct_sector_level,
        "sector_code": sector_code,
        "train_dates": values,
        "train_dates_sha256": canonical_sha256(values),
        "train_observation_sha256": canonical_sha256(np.asarray(observations, dtype=np.float64).tolist()),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "feature_domain_policy_sha256": TEST_POLICY_SHA256,
    }


def _feature_domain_policy_manifest() -> dict:
    trade_dates = [
        value.date().isoformat()
        for value in pd.bdate_range(C010_APPROVED_TRAIN_START, C010_APPROVED_TRAIN_END)
        if value.date().isoformat() not in APPROVED_TRAIN_EXCHANGE_HOLIDAYS
    ]
    l1_codes = [f"L1_{index:03d}" for index in range(31)]
    l2_codes = [f"L2_{index:03d}" for index in range(131)]
    ledger_entry = {
        "canonical_ts_code": "000001.SZ",
        "expected_opportunity_count": len(trade_dates),
        "expected_opportunity_contract": "hmm_risk_c010_expected_opportunity_dates_v1",
        "expected_opportunity_date_sha256": canonical_sha256(
            [{"canonical_ts_code": "000001.SZ", "trade_date": trade_date} for trade_date in trade_dates]
        ),
        "provider_absence_count": 0,
        "availability_ratio": 1.0,
        "moneyflow_contributor_eligible": True,
        "provider_absence_key_sha256": canonical_sha256([]),
    }
    ledger = [{**ledger_entry, "entry_sha256": canonical_sha256(ledger_entry)}]
    eligibility_body = {
        "schema_version": "hmm_risk_c010_train_observation_eligibility_v1",
        "train_start": C010_APPROVED_TRAIN_START.isoformat(),
        "train_end": C010_APPROVED_TRAIN_END.isoformat(),
        "minimum_availability_ratio": 0.9,
        "availability_integer_contract": "10*(expected-missing) >= 9*expected",
        "entry_count": 1,
        "entries": ledger,
        "excluded_moneyflow_symbols": [],
        "pit_universe_changed": False,
        "selection_universe_changed": False,
        "runtime_prediction_eligibility_changed": False,
        "diagnostic_only": False,
        "formal_policy_activated": True,
    }
    eligibility = {**eligibility_body, "receipt_sha256": canonical_sha256(eligibility_body)}

    def domain_entry(level: str, code: str, trade_date: str) -> dict:
        value = {
            "direct_sector_level": level,
            "sector_code": code,
            "trade_date": trade_date,
            "price_domain_status": "available",
            "price_domain_reason_code": None,
            "price_expected_symbols": ["000001.SZ"],
            "price_expected_symbol_sha256": canonical_sha256(["000001.SZ"]),
            "price_complete_symbols": ["000001.SZ"],
            "price_complete_symbol_sha256": canonical_sha256(["000001.SZ"]),
            "price_count_coverage": 1.0,
            "price_expected_weight": 1.0,
            "price_complete_weight": 1.0,
            "price_weight_coverage": 1.0,
            "moneyflow_domain_status": "available",
            "moneyflow_domain_reason_code": None,
            "moneyflow_expected_symbols": ["000001.SZ"],
            "moneyflow_expected_symbol_sha256": canonical_sha256(["000001.SZ"]),
            "moneyflow_complete_symbols": ["000001.SZ"],
            "moneyflow_complete_symbol_sha256": canonical_sha256(["000001.SZ"]),
            "moneyflow_count_coverage": 1.0,
            "moneyflow_expected_weight": 1.0,
            "moneyflow_complete_weight": 1.0,
            "moneyflow_weight_coverage": 1.0,
            "moneyflow_contributor_amount": 1.0,
            "moneyflow_excluded_symbols": [],
            "missing_evidence": [],
        }
        return {**value, "entry_sha256": canonical_sha256(value)}

    l1_domain = [domain_entry("L1", code, trade_date) for trade_date in trade_dates for code in l1_codes]
    l2_domain = [domain_entry("L2", code, trade_date) for trade_date in trade_dates for code in l2_codes]
    aggregate_body = {
        "schema_version": "hmm_risk_c010_feature_domain_aggregate_evidence_v1",
        "l1_aggregate_count": len(l1_domain),
        "l2_aggregate_count": len(l2_domain),
        "l1_domain_receipts": l1_domain,
        "l2_domain_receipts": l2_domain,
        "l1_invalid_price_domain": [],
        "l2_invalid_price_domain": [],
        "l1_domain_expected_count": len(l1_domain),
        "l1_domain_receipt_count": len(l1_domain),
        "l2_domain_expected_count": len(l2_domain),
        "l2_domain_receipt_count": len(l2_domain),
        "formal_policy_activated": True,
    }
    aggregate = {**aggregate_body, "receipt_sha256": canonical_sha256(aggregate_body)}

    def mask_hash(codes: list[str], active_codes: list[str]) -> str:
        active = set(active_codes)
        return canonical_sha256([{"sector_code": code, "active": code in active} for code in codes])

    def cross_receipt(level: str, codes: list[str]) -> dict:
        entries = []
        for feature in C010_CROSS_SECTION_FEATURES:
            for date_index, trade_date in enumerate(trade_dates):
                post_codes = [] if feature == "sf_range_vs_market_10d" and date_index < 4 else codes
                entry = {
                    "feature_name": feature,
                    "trade_date": trade_date,
                    "direct_sector_level": level,
                    "operator": C010_CROSS_SECTION_OPERATORS[feature],
                    "source_domain": "price",
                    "expected_sector_count": len(codes),
                    "expected_sector_sha256": canonical_sha256(codes),
                    "valid_sector_count": len(codes),
                    "valid_sector_codes": codes,
                    "valid_sector_sha256": canonical_sha256(codes),
                    "missing_sector_codes": [],
                    "missing_sector_sha256": canonical_sha256([]),
                    "feature_cross_section_coverage": 1.0,
                    "reference_value": 1.0,
                    "pre_mask_sha256": mask_hash(codes, codes),
                    "post_mask_sha256": mask_hash(codes, post_codes),
                    "post_mask_subset_of_pre_mask": True,
                    "status": "accepted",
                    "reason_code": None,
                }
                entries.append({**entry, "entry_sha256": canonical_sha256(entry)})
        value = {
            "schema_version": "hmm_risk_c010_feature_cross_section_receipt_set_v1",
            "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
            "feature_domain_policy_version": "hmm_risk_c010_feature_domain_policy_v1",
            "direct_sector_level": level,
            "expected_sector_count": len(codes),
            "expected_sector_codes": codes,
            "expected_sector_sha256": canonical_sha256(codes),
            "entry_count": len(entries),
            "entries": entries,
            "diagnostic_only": False,
        }
        return {**value, "receipt_sha256": canonical_sha256(value)}

    l1_cross = cross_receipt("L1", l1_codes)
    l2_cross = cross_receipt("L2", l2_codes)

    def feature_definition(level: str) -> dict:
        expected_count = 31 if level == "L1" else 131
        return {
            "schema_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
            "feature_domain_policy_version": "hmm_risk_c010_feature_domain_policy_v1",
            "diagnostic_only": False,
            "direct_sector_level": level,
            "cross_section_required_sector_count": expected_count,
            "cross_section_min_coverage": 0.9,
            "cross_section_min_valid_sector_count": 28 if level == "L1" else 118,
            "base_features": list(BASE_FEATURES),
            "all_core_features": list(ALL_CORE_FEATURES),
            "moneyflow_mandatory_fields": [
                "buy_sm_amount_cny",
                "sell_sm_amount_cny",
                "buy_elg_amount_cny",
                "sell_elg_amount_cny",
                "net_mf_amount_cny",
            ],
            "moneyflow_denominator_by_feature": dict(C010_MONEYFLOW_DENOMINATOR_BY_FEATURE),
            "cross_section_operator_by_feature": dict(C010_CROSS_SECTION_OPERATORS),
            "formula_diff_by_feature": {
                feature: dict(formula) for feature, formula in C010_FORMULA_DIFF_BY_FEATURE.items()
            },
            "moneyflow_rolling_post_mask_required": True,
            "range_cross_section_rolling_post_mask_required": True,
        }

    l1_definition = feature_definition("L1")
    l2_definition = feature_definition("L2")
    feature_order = {
        "legacy_covfix": list(BASE_FEATURES),
        "autocycle_all_core": list(ALL_CORE_FEATURES),
    }
    circ_mv = {
        "L1": {
            "circ_mv_lookback_contract_version": "v1",
            "circ_mv_history_start": "2020-01-01",
            "circ_mv_pit_boundary_crossing_key_sha256": "1" * 64,
        },
        "L2": {
            "circ_mv_lookback_contract_version": "v1",
            "circ_mv_history_start": "2020-01-01",
            "circ_mv_pit_boundary_crossing_key_sha256": "2" * 64,
        },
    }
    body = {
        "schema_version": "hmm_risk_c010_feature_domain_policy_v1",
        "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
        "producer_commit": "c" * 40,
        "train_start": C010_APPROVED_TRAIN_START.isoformat(),
        "train_end": C010_APPROVED_TRAIN_END.isoformat(),
        "receipt_trading_dates": trade_dates,
        "receipt_trading_date_count": len(trade_dates),
        "receipt_trading_date_sha256": canonical_sha256(trade_dates),
        "contributor_min_availability": 0.9,
        "domain_min_count_coverage": 0.9,
        "domain_min_weight_coverage": 0.9,
        "feature_cross_section_min_coverage": 0.9,
        "moneyflow_mandatory_fields": [
            "buy_sm_amount_cny",
            "sell_sm_amount_cny",
            "buy_elg_amount_cny",
            "sell_elg_amount_cny",
            "net_mf_amount_cny",
        ],
        "eligibility_receipt": eligibility,
        "eligibility_receipt_sha256": eligibility["receipt_sha256"],
        "eligibility_entry_count": len(ledger),
        "contributor_ledger": ledger,
        "contributor_ledger_sha256": canonical_sha256(ledger),
        "excluded_moneyflow_symbols": [],
        "excluded_moneyflow_symbol_sha256": canonical_sha256([]),
        "aggregate_receipt": aggregate,
        "aggregate_receipt_sha256": aggregate["receipt_sha256"],
        "l1_cross_section_receipt": l1_cross,
        "l1_cross_section_receipt_sha256": l1_cross["receipt_sha256"],
        "l2_cross_section_receipt": l2_cross,
        "l2_cross_section_receipt_sha256": l2_cross["receipt_sha256"],
        "l1_feature_definition": l1_definition,
        "l1_feature_definition_sha256": canonical_sha256(l1_definition),
        "l2_feature_definition": l2_definition,
        "l2_feature_definition_sha256": canonical_sha256(l2_definition),
        "feature_order_by_family": feature_order,
        "feature_order_sha256": canonical_sha256(feature_order),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "calendar_manifest_hash": "c" * 64,
        "security_identity_manifest_sha256": "e" * 64,
        "provider_absence_manifest_sha256": "f" * 64,
        "causal_circ_mv_identity": circ_mv,
        "causal_circ_mv_identity_sha256": canonical_sha256(circ_mv),
        "pit_universe_changed": False,
        "selection_universe_changed": False,
        "runtime_prediction_eligibility_changed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


TEST_POLICY_MANIFEST = _feature_domain_policy_manifest()
TEST_POLICY_SHA256 = TEST_POLICY_MANIFEST["receipt_sha256"]


def _rehash_policy(manifest: dict) -> dict:
    body = {key: value for key, value in manifest.items() if key != "receipt_sha256"}
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _policy_with_rehashed_aggregate_entry(**changes: object) -> dict:
    manifest = dict(TEST_POLICY_MANIFEST)
    aggregate = dict(TEST_POLICY_MANIFEST["aggregate_receipt"])
    entries = list(aggregate["l1_domain_receipts"])
    entry = {**entries[0], **changes}
    entry_body = {key: value for key, value in entry.items() if key != "entry_sha256"}
    entries[0] = {**entry_body, "entry_sha256": canonical_sha256(entry_body)}
    aggregate["l1_domain_receipts"] = entries
    aggregate_body = {key: value for key, value in aggregate.items() if key != "receipt_sha256"}
    manifest["aggregate_receipt"] = {**aggregate_body, "receipt_sha256": canonical_sha256(aggregate_body)}
    manifest["aggregate_receipt_sha256"] = manifest["aggregate_receipt"]["receipt_sha256"]
    return _rehash_policy(manifest)


def _policy_with_rehashed_cross_section_entry(**changes: object) -> dict:
    manifest = dict(TEST_POLICY_MANIFEST)
    receipt = dict(TEST_POLICY_MANIFEST["l1_cross_section_receipt"])
    entries = list(receipt["entries"])
    entry = {**entries[0], **changes}
    entry_body = {key: value for key, value in entry.items() if key != "entry_sha256"}
    entries[0] = {**entry_body, "entry_sha256": canonical_sha256(entry_body)}
    receipt["entries"] = entries
    receipt_body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    manifest["l1_cross_section_receipt"] = {**receipt_body, "receipt_sha256": canonical_sha256(receipt_body)}
    manifest["l1_cross_section_receipt_sha256"] = manifest["l1_cross_section_receipt"]["receipt_sha256"]
    return _rehash_policy(manifest)


def test_c010_policy_validator_rejects_minimal_self_hashed_manifest() -> None:
    entry = {"canonical_ts_code": "000001.SZ", "moneyflow_contributor_eligible": True}
    ledger = [{**entry, "entry_sha256": canonical_sha256(entry)}]
    body = {
        "schema_version": "hmm_risk_c010_feature_domain_policy_v1",
        "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
        "eligibility_entry_count": 1,
        "contributor_ledger": ledger,
        "contributor_ledger_sha256": canonical_sha256(ledger),
        "excluded_moneyflow_symbols": [],
    }
    with pytest.raises(StateModelSetError, match="fields are incomplete"):
        validate_c010_policy_manifest({**body, "receipt_sha256": canonical_sha256(body)})


def test_c010_policy_validator_rejects_rehashed_cross_section_semantic_skeleton() -> None:
    manifest = json.loads(json.dumps(TEST_POLICY_MANIFEST))
    skeleton = {"index": 0}
    manifest["l1_cross_section_receipt"]["entries"][0] = {
        **skeleton,
        "entry_sha256": canonical_sha256(skeleton),
    }
    cross_body = {key: value for key, value in manifest["l1_cross_section_receipt"].items() if key != "receipt_sha256"}
    manifest["l1_cross_section_receipt"] = {
        **cross_body,
        "receipt_sha256": canonical_sha256(cross_body),
    }
    manifest["l1_cross_section_receipt_sha256"] = manifest["l1_cross_section_receipt"]["receipt_sha256"]
    manifest = _rehash_policy(manifest)
    with pytest.raises(StateModelSetError, match="valid sector codes is missing"):
        validate_c010_policy_manifest(manifest)


def test_c010_policy_validator_rejects_rehashed_incomplete_domain_cartesian_set() -> None:
    manifest = json.loads(json.dumps(TEST_POLICY_MANIFEST))
    aggregate = manifest["aggregate_receipt"]
    aggregate["l2_domain_receipts"].pop()
    aggregate["l2_aggregate_count"] -= 1
    aggregate["l2_domain_expected_count"] -= 1
    aggregate["l2_domain_receipt_count"] -= 1
    aggregate_body = {key: value for key, value in aggregate.items() if key != "receipt_sha256"}
    manifest["aggregate_receipt"] = {
        **aggregate_body,
        "receipt_sha256": canonical_sha256(aggregate_body),
    }
    manifest["aggregate_receipt_sha256"] = manifest["aggregate_receipt"]["receipt_sha256"]
    manifest = _rehash_policy(manifest)
    with pytest.raises(StateModelSetError, match="receipt set is incomplete"):
        validate_c010_policy_manifest(manifest)


def test_c010_policy_validator_rejects_rehashed_one_day_train_contract() -> None:
    manifest = dict(TEST_POLICY_MANIFEST)
    manifest.update(
        {
            "train_start": "2022-01-04",
            "train_end": "2022-01-04",
            "receipt_trading_dates": ["2022-01-04"],
            "receipt_trading_date_count": 1,
            "receipt_trading_date_sha256": canonical_sha256(["2022-01-04"]),
        }
    )
    with pytest.raises(StateModelSetError, match="fixed contract"):
        validate_c010_policy_manifest(_rehash_policy(manifest))


def test_c010_policy_validator_rejects_rehashed_moneyflow_contributor_outside_train_ledger() -> None:
    symbol = ["999999.SZ"]
    manifest = _policy_with_rehashed_aggregate_entry(
        moneyflow_expected_symbols=symbol,
        moneyflow_expected_symbol_sha256=canonical_sha256(symbol),
        moneyflow_complete_symbols=symbol,
        moneyflow_complete_symbol_sha256=canonical_sha256(symbol),
    )
    with pytest.raises(StateModelSetError, match="available domain receipt semantics"):
        validate_c010_policy_manifest(manifest)


@pytest.mark.parametrize(
    "changes",
    (
        {"operator": "bogus:future_aware_operator"},
        {"reference_value": 0.0},
        {"post_mask_sha256": canonical_sha256([])},
    ),
)
def test_c010_policy_validator_rejects_rehashed_cross_section_business_semantic_drift(
    changes: dict[str, object],
) -> None:
    with pytest.raises(StateModelSetError, match="cross-section (entry semantics|post-mask evidence)"):
        validate_c010_policy_manifest(_policy_with_rehashed_cross_section_entry(**changes))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("moneyflow_denominator_by_feature", {"net_mf_ratio": "price_domain_l1_amount"}),
        ("cross_section_operator_by_feature", {feature: "bogus" for feature in C010_CROSS_SECTION_FEATURES}),
        ("formula_diff_by_feature", {feature: {"v1": "old", "v2": "new"} for feature in C010_CROSS_SECTION_FEATURES}),
    ),
)
def test_c010_policy_validator_rejects_rehashed_feature_definition_value_drift(
    field: str,
    value: object,
) -> None:
    manifest = dict(TEST_POLICY_MANIFEST)
    definition = {**TEST_POLICY_MANIFEST["l1_feature_definition"], field: value}
    manifest["l1_feature_definition"] = definition
    manifest["l1_feature_definition_sha256"] = canonical_sha256(definition)
    with pytest.raises(StateModelSetError, match="feature definition is invalid"):
        validate_c010_policy_manifest(_rehash_policy(manifest))


def _model(*, family: str = "legacy_covfix", level: str = "L1", seed: int = 42, code: str = "S001") -> B3FittedModel:
    feature_names = BASE_FEATURES if family == "legacy_covfix" else ALL_CORE_FEATURES
    feature_count = len(feature_names)
    preprocess = (
        {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}
        if family == "legacy_covfix"
        else {
            "family": "winsor_zscore_1_99_train_global_v1",
            "winsor_low": [-3.0] * feature_count,
            "winsor_high": [3.0] * feature_count,
            "center": [0.0] * feature_count,
            "scale": [1.0] * feature_count,
        }
    )
    projection_receipt = None
    effective_count = feature_count
    if family == "autocycle_all_core" and level == "L2":
        raw = np.ones((180, feature_count), dtype=np.float64)
        if code == TARGET_SECTOR:
            raw[:, -1] = 0.0
        projection_receipt, _ = build_projection_receipt(
            family=family,
            level=level,
            sector_code=code,
            full_feature_names=feature_names,
            preprocess=preprocess,
            raw_observations=raw,
            preprocessed_observations=raw,
            train_input_manifest={
                "dataset_manifest_hash": "a" * 64,
                "mapping_manifest_hash": "b" * 64,
                "calendar_manifest_hash": "c" * 64,
                "l2_stock_fact_manifest_hash": "d" * 64,
                "feature_domain_policy_sha256": TEST_POLICY_SHA256,
                "formula_version": C010_FORMULA_VERSION,
            },
        )
        effective_count = int(projection_receipt["likelihood_feature_count"])
    body = {
        "schema_version": MIXED_MODEL_SCHEMA_VERSION if projection_receipt else "hmm_risk_b3_fitted_model_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": code,
        "feature_names": list(feature_names),
        "preprocess": preprocess,
        "startprob": [1 / 3, 1 / 3, 1 / 3],
        "transmat": [[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]],
        "means": [[-1.0] * effective_count, [0.0] * effective_count, [1.0] * effective_count],
        "covariance_type": "diag",
        "covars": [[1.0] * effective_count, [1.0] * effective_count, [1.0] * effective_count],
        "parameter_profile_sha256": "a" * 64,
        "numeric_environment_sha256": "b" * 64,
        "observation_manifest_hash": "c" * 64,
        "pit_constituent_manifest_hash": "d" * 64,
    }
    if projection_receipt is not None:
        body.update(
            {
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "feature_count": feature_count,
                "likelihood_feature_names": list(projection_receipt["active_feature_names"]),
                "likelihood_feature_count": effective_count,
                "projection_receipt": projection_receipt,
                "projection_sha256": projection_receipt["projection_sha256"],
            }
        )
    return B3FittedModel(
        family=family,
        level=level,
        seed=seed,
        sector_code=code,
        feature_names=feature_names,
        preprocess=preprocess,
        startprob=np.asarray(body["startprob"], dtype=np.float64),
        transmat=np.asarray(body["transmat"], dtype=np.float64),
        means=np.asarray(body["means"], dtype=np.float64),
        covars=np.asarray(body["covars"], dtype=np.float64),
        parameter_profile_sha256=body["parameter_profile_sha256"],
        numeric_environment_sha256=body["numeric_environment_sha256"],
        observation_manifest_hash=body["observation_manifest_hash"],
        pit_constituent_manifest_hash=body["pit_constituent_manifest_hash"],
        model_payload_sha256=canonical_sha256(body),
        projection_receipt=projection_receipt,
    )


def test_repeat_model_payload_roundtrip_rejects_hash_drift() -> None:
    model = _model()
    payloads = [model.payload()]
    repeat = {
        "family": "legacy_covfix",
        "level": "L1",
        "canonical_sector_codes": ["S001"],
        "feature_names": list(BASE_FEATURES),
        "models": payloads,
        "model_payload_sha256": canonical_sha256(payloads),
    }
    restored = models_from_repeat(repeat)
    assert restored[(42, "S001")].model_payload_sha256 == model.model_payload_sha256

    drifted = json.loads(json.dumps(repeat))
    drifted["models"][0]["covars"][0][0] = 2.0
    with pytest.raises(StateModelSetError, match="payload hash mismatch"):
        models_from_repeat(drifted)


def test_mixed_dimension_repeat_roundtrip_preserves_target_19d_and_identity_20d() -> None:
    target = _model(family="autocycle_all_core", level="L2", code=TARGET_SECTOR)
    identity = _model(family="autocycle_all_core", level="L2", code="801011.SI")
    payloads = [identity.payload(), target.payload()]
    repeat = {
        "schema_version": MIXED_REPEAT_SCHEMA_VERSION,
        "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "feature_count": 20,
        "family": "autocycle_all_core",
        "level": "L2",
        "canonical_sector_codes": ["801011.SI", TARGET_SECTOR],
        "feature_names": list(ALL_CORE_FEATURES),
        "models": payloads,
        "model_payload_sha256": canonical_sha256(payloads),
    }

    restored = models_from_repeat(repeat)

    assert restored[(42, TARGET_SECTOR)].means.shape == (3, 19)
    assert restored[(42, "801011.SI")].means.shape == (3, 20)


def test_mixed_dimension_repeat_rejects_rehashed_projection_mask_drift() -> None:
    target = _model(family="autocycle_all_core", level="L2", code=TARGET_SECTOR)
    payload = json.loads(json.dumps(target.payload()))
    projection = payload["projection_receipt"]
    projection["active_feature_mask"][-1] = True
    projection_body = {key: value for key, value in projection.items() if key != "projection_sha256"}
    projection["projection_sha256"] = canonical_sha256(projection_body)
    payload["projection_sha256"] = projection["projection_sha256"]
    model_body = {key: value for key, value in payload.items() if key != "model_payload_sha256"}
    payload["model_payload_sha256"] = canonical_sha256(model_body)
    repeat = {
        "schema_version": MIXED_REPEAT_SCHEMA_VERSION,
        "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "feature_count": 20,
        "family": "autocycle_all_core",
        "level": "L2",
        "canonical_sector_codes": [TARGET_SECTOR],
        "feature_names": list(ALL_CORE_FEATURES),
        "models": [payload],
        "model_payload_sha256": canonical_sha256([payload]),
    }

    with pytest.raises(StateModelSetError, match="inactive_dimension_contract_invalid"):
        models_from_repeat(repeat)


@pytest.mark.parametrize("drift", ["source_profile", "exact_zero_evidence"])
def test_mixed_dimension_repeat_rejects_rehashed_authority_evidence_drift(drift: str) -> None:
    target = _model(family="autocycle_all_core", level="L2", code=TARGET_SECTOR)
    payload = json.loads(json.dumps(target.payload()))
    projection = payload["projection_receipt"]
    if drift == "source_profile":
        projection["source_profile_receipt_sha256"] = "0" * 64
    else:
        exact_evidence = projection["inactive_exact_zero_evidence"]
        exact_evidence["raw"]["variance_ddof0_by_feature"] = [1.0]
        exact_body = {key: value for key, value in exact_evidence.items() if key != "exact_zero_evidence_sha256"}
        exact_evidence["exact_zero_evidence_sha256"] = canonical_sha256(exact_body)
    projection_body = {key: value for key, value in projection.items() if key != "projection_sha256"}
    projection["projection_sha256"] = canonical_sha256(projection_body)
    payload["projection_sha256"] = projection["projection_sha256"]
    model_body = {key: value for key, value in payload.items() if key != "model_payload_sha256"}
    payload["model_payload_sha256"] = canonical_sha256(model_body)
    repeat = {
        "schema_version": MIXED_REPEAT_SCHEMA_VERSION,
        "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
        "feature_count": 20,
        "family": "autocycle_all_core",
        "level": "L2",
        "canonical_sector_codes": [TARGET_SECTOR],
        "feature_names": list(ALL_CORE_FEATURES),
        "models": [payload],
        "model_payload_sha256": canonical_sha256([payload]),
    }

    with pytest.raises(StateModelSetError, match="inactive_dimension_contract_invalid"):
        models_from_repeat(repeat)


def test_target_projection_rejects_nonzero_inactive_feature() -> None:
    model = _model(family="autocycle_all_core", level="L2", code="801011.SI")
    raw = np.ones((180, 20), dtype=np.float64)
    with pytest.raises(StateModelSetError, match="inactive_dimension_contract_invalid"):
        build_projection_receipt(
            family="autocycle_all_core",
            level="L2",
            sector_code=TARGET_SECTOR,
            full_feature_names=ALL_CORE_FEATURES,
            preprocess=model.preprocess,
            raw_observations=raw,
            preprocessed_observations=raw,
            train_input_manifest={
                "dataset_manifest_hash": "a" * 64,
                "mapping_manifest_hash": "b" * 64,
                "calendar_manifest_hash": "c" * 64,
                "l2_stock_fact_manifest_hash": "d" * 64,
                "feature_domain_policy_sha256": TEST_POLICY_SHA256,
                "formula_version": C010_FORMULA_VERSION,
            },
        )


@pytest.mark.parametrize("invalid_value", [1e-12, np.nan, np.inf, -np.inf])
def test_target_projection_rejects_near_zero_or_nonfinite_raw_inactive_feature(invalid_value: float) -> None:
    raw = np.ones((180, 20), dtype=np.float64)
    raw[:, 19] = 0.0
    raw[0, 19] = invalid_value
    preprocess = _global_preprocess_with_nonzero_inactive_transform()
    preprocessed = (
        training_subject._apply_preprocess(raw, preprocess) if np.isfinite(invalid_value) else np.zeros_like(raw)
    )

    with pytest.raises(StateModelSetError, match="inactive_dimension_contract_invalid"):
        build_projection_receipt(
            family="autocycle_all_core",
            level="L2",
            sector_code=TARGET_SECTOR,
            full_feature_names=ALL_CORE_FEATURES,
            preprocess=preprocess,
            raw_observations=raw,
            preprocessed_observations=preprocessed,
            train_input_manifest={
                "dataset_manifest_hash": "a" * 64,
                "mapping_manifest_hash": "b" * 64,
                "calendar_manifest_hash": "c" * 64,
                "l2_stock_fact_manifest_hash": "d" * 64,
                "feature_domain_policy_sha256": TEST_POLICY_SHA256,
                "formula_version": C010_FORMULA_VERSION,
            },
        )


def _global_preprocess_with_nonzero_inactive_transform() -> dict[str, object]:
    return {
        "family": "winsor_zscore_1_99_train_global_v1",
        "winsor_low": [-3.0] * 19 + [-0.12704857933790217],
        "winsor_high": [3.0] * 19 + [-0.00268461666241596],
        "center": [0.0] * 19 + [-0.041761032442194194],
        "scale": [1.0] * 19 + [0.0231404684253839],
    }


def test_target_projection_accepts_raw_zero_after_approved_global_preprocess_maps_it_nonzero() -> None:
    raw = np.ones((180, 20), dtype=np.float64)
    raw[:, 19] = 0.0
    preprocess = _global_preprocess_with_nonzero_inactive_transform()
    preprocessed = training_subject._apply_preprocess(raw, preprocess)
    assert np.all(preprocessed[:, 19] != 0.0)

    receipt, projected = build_projection_receipt(
        family="autocycle_all_core",
        level="L2",
        sector_code=TARGET_SECTOR,
        full_feature_names=ALL_CORE_FEATURES,
        preprocess=preprocess,
        raw_observations=raw,
        preprocessed_observations=preprocessed,
        train_input_manifest={
            "dataset_manifest_hash": "a" * 64,
            "mapping_manifest_hash": "b" * 64,
            "calendar_manifest_hash": "c" * 64,
            "l2_stock_fact_manifest_hash": "d" * 64,
            "feature_domain_policy_sha256": TEST_POLICY_SHA256,
            "formula_version": C010_FORMULA_VERSION,
        },
    )

    assert projected.shape == (180, 19)
    evidence = receipt["inactive_exact_zero_evidence"]
    assert receipt["inactive_exact_zero"] is True
    assert evidence["raw_exact_zero"] is True
    assert evidence["raw"]["all_values_zero"] is True
    assert evidence["preprocessed_exact_zero_required"] is False
    assert evidence["preprocessed"]["all_values_zero"] is False
    assert evidence["preprocessed_matches_approved_transform"] is True
    assert evidence["expected_preprocessed_vector_sha256"] == receipt["preprocessed_inactive_vector_sha256"]
    assert evidence["observed_preprocessed_vector_sha256"] == receipt["preprocessed_inactive_vector_sha256"]


@pytest.mark.parametrize("tampered_feature_index", [0, 19])
def test_target_projection_rejects_preprocessed_matrix_not_produced_by_approved_transform(
    tampered_feature_index: int,
) -> None:
    raw = np.ones((180, 20), dtype=np.float64)
    raw[:, 19] = 0.0
    preprocess = _global_preprocess_with_nonzero_inactive_transform()
    preprocessed = training_subject._apply_preprocess(raw, preprocess)
    preprocessed[0, tampered_feature_index] += 1.0

    with pytest.raises(StateModelSetError, match="inactive_dimension_contract_invalid"):
        build_projection_receipt(
            family="autocycle_all_core",
            level="L2",
            sector_code=TARGET_SECTOR,
            full_feature_names=ALL_CORE_FEATURES,
            preprocess=preprocess,
            raw_observations=raw,
            preprocessed_observations=preprocessed,
            train_input_manifest={
                "dataset_manifest_hash": "a" * 64,
                "mapping_manifest_hash": "b" * 64,
                "calendar_manifest_hash": "c" * 64,
                "l2_stock_fact_manifest_hash": "d" * 64,
                "feature_domain_policy_sha256": TEST_POLICY_SHA256,
                "formula_version": C010_FORMULA_VERSION,
            },
        )


@pytest.mark.parametrize(("sector_code", "expected_dimension"), [(TARGET_SECTOR, 19), ("801011.SI", 20)])
def test_formal_autocycle_l2_fit_applies_full_preprocess_then_fixed_projection(
    monkeypatch, sector_code: str, expected_dimension: int
) -> None:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(120))
    observations = np.ones((120, 20), dtype=np.float64)
    if sector_code == TARGET_SECTOR:
        observations[:, 19] = 0.0
    manifest = _train_manifest(
        dates,
        observations,
        sector_code=sector_code,
        direct_sector_level="L2",
    )
    manifest.update(
        {
            "l2_stock_fact_manifest_hash": "d" * 64,
            "formula_version": C010_FORMULA_VERSION,
        }
    )
    item = B3TrainOnlySeries(
        sector_code=sector_code,
        sector_name=sector_code,
        train_observations=observations,
        train_dates=dates,
        pit_l2_constituents=(sector_code,),
        pit_constituent_manifest_hash="a" * 64,
        observation_manifest_hash="b" * 64,
        train_input_manifest=manifest,
    )
    preprocess = {
        "family": "winsor_zscore_1_99_train_global_v1",
        "winsor_low": [-3.0] * 20,
        "winsor_high": [3.0] * 20,
        "center": [0.0] * 20,
        "scale": [1.0] * 20,
    }
    captured = {}

    def fake_fit(series, *, train, seed):
        captured["shape"] = train.shape
        return training_subject.B3CoreFitEvidence(
            initialization={},
            monitor_evidence={},
            likelihood={},
            covariance={},
            train_occupancy={},
            startprob=np.asarray([1 / 3, 1 / 3, 1 / 3]),
            transmat=np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            means=np.zeros((3, train.shape[1])),
            covars=np.ones((3, train.shape[1])),
            terminal_likelihood=-1.0,
            model_entry_status="accepted",
            model_entry_valid=True,
        )

    monkeypatch.setattr(training_subject, "fit_b3_preprocessed_train_only", fake_fit)
    entry, model = training_subject._fit_b3_train_only(
        item,
        family="autocycle_all_core",
        level="L2",
        feature_names=ALL_CORE_FEATURES,
        preprocess=preprocess,
        seed=42,
        numeric_environment={"scope": "test"},
        dimension_contract_version=MIXED_DIMENSION_CONTRACT_VERSION,
    )

    assert captured["shape"] == (120, expected_dimension)
    assert model.means.shape == (3, expected_dimension)
    assert entry["feature_count"] == 20
    assert entry["likelihood_feature_count"] == expected_dimension
    assert entry["projection_sha256"] == model.projection_receipt["projection_sha256"]
    evidence = model.projection_receipt["inactive_exact_zero_evidence"]
    assert evidence["observation_rows"] == 120
    assert evidence["inactive_feature_count"] == (1 if sector_code == TARGET_SECTOR else 0)
    assert evidence["raw"]["all_values_zero"] is True
    assert evidence["preprocessed"]["all_values_zero"] is True
    assert len(model.projection_receipt["source_profile_receipt_sha256"]) == 64
    if sector_code == TARGET_SECTOR:
        assert model.projection_receipt["source_profile_receipt_sha256"] == TARGET_SOURCE_PROFILE_RECEIPT_SHA256


def test_train_only_builder_does_not_materialize_validation_or_future_utility() -> None:
    dates = pd.bdate_range("2022-01-03", periods=120)
    codes = [f"L1-{index:02d}" for index in range(31)]
    index = pd.MultiIndex.from_product([dates, codes], names=["trade_date", "l1_code"])
    panel = pd.DataFrame(index=index)
    panel["l1_name"] = [f"Sector {code}" for _, code in index]
    for feature_index, feature in enumerate(BASE_FEATURES):
        panel[feature] = np.arange(len(panel), dtype=np.float64) + feature_index + 1.0
    constituents = {
        code: {"schema_version": "pit_v1", "l2_codes": [f"L2-{position:03d}"]} for position, code in enumerate(codes)
    }
    result = build_train_only_series(
        panel,
        feature_names=BASE_FEATURES,
        train_start=dates[0].date(),
        train_end=dates[-1].date(),
        constituent_manifest=constituents,
        expected_sector_count=31,
        direct_sector_level="L1",
    )
    assert len(result) == 31
    assert not hasattr(result[codes[0]], "validation_observations")
    assert not hasattr(result[codes[0]], "validation_future_utility")


def test_train_only_coverage_audit_collects_all_sectors_without_fit_or_validation() -> None:
    dates = pd.bdate_range("2022-01-03", periods=120)
    codes = [f"L1-{index:02d}" for index in range(31)]
    index = pd.MultiIndex.from_product([dates, codes], names=["trade_date", "l1_code"])
    panel = pd.DataFrame(index=index)
    for feature_index, feature in enumerate(ALL_CORE_FEATURES):
        panel[feature] = np.arange(len(panel), dtype=np.float64) + feature_index + 1.0
    failing_code = codes[0]
    failing_index = panel.xs(failing_code, level="l1_code").index[10:]
    panel.loc[(failing_index, failing_code), list(ALL_CORE_FEATURES)] = np.nan

    report = audit_train_only_coverage(
        panel,
        feature_names=ALL_CORE_FEATURES,
        train_start=dates[0].date(),
        train_end=dates[-1].date(),
        expected_sector_count=31,
        direct_sector_level="L1",
    )

    assert report["entry_count"] == 31
    assert report["minimum_observed_train_row_count"] == 10
    assert report["maximum_observed_train_row_count"] == 120
    assert report["insufficient_sector_codes"] == [failing_code]
    assert report["train_coverage_valid"] is False
    assert report["failure_reason_codes"] == ["hmm_risk_model_train_observation_coverage_insufficient"]
    assert report["fit_performed"] is False
    assert report["validation_accessed"] is False
    assert report["selection_performed"] is False


def test_formal_fit_uses_monitor_terminal_likelihood_and_never_validation(monkeypatch) -> None:
    class _Monitor:
        converged = True
        iter = 2
        n_iter = 300
        tol = 0.01
        history = deque([-1.0, -0.995])

    class _GaussianHMM:
        def __init__(self, **kwargs) -> None:
            assert kwargs["min_covar"] == 0.0
            assert kwargs["covars_weight"] == 2.0
            assert kwargs["init_params"] == ""
            self.monitor_ = _Monitor()

        @property
        def covars_(self):
            raise AssertionError("public covariance representation must not be read after fit")

        @covars_.setter
        def covars_(self, value):
            self._covars_ = np.asarray(value, dtype=np.float64)

        def fit(self, train):
            return self

    monkeypatch.setattr("hmmlearn.hmm.GaussianHMM", _GaussianHMM)
    monkeypatch.setattr(
        training_subject,
        "_manual_b3_diag04_initialization",
        lambda train, sector_reference_variance, random_seed: (
            np.full(3, 1 / 3),
            np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            np.asarray([[-1.0] * 7, [0.0] * 7, [1.0] * 7]),
            np.ones((3, 7)),
            {"schema_version": "diagnostic_init", "cluster_counts": [60, 60, 60]},
        ),
    )
    monkeypatch.setattr(training_subject, "_sector_local_reference_variance", lambda train: np.ones(7))
    covariance_evidence = {
        "raw_covars": np.ones((3, 7)).tolist(),
        "sector_local_reference_variance_R_sj": np.ones(7).tolist(),
        "state_posterior_mass": [60.0, 60.0, 60.0],
        "posterior_second_moment_about_fitted_mean": np.ones((3, 7)).tolist(),
        "nu": 1.0,
        "postfit_projection_performed": False,
    }
    observed_raw_covariance = {}

    def covariance_audit(model, train, raw_covars, sector_reference_variance):
        del model, train, sector_reference_variance
        observed_raw_covariance["value"] = raw_covars.copy()
        return (
            covariance_evidence,
            np.full((180, 3), 1 / 3),
            -123.0,
        )

    monkeypatch.setattr(training_subject, "_b3_diag04_covariance_evidence", covariance_audit)
    states = np.asarray([index % 3 for index in range(180)])
    train_posteriors = np.zeros((180, 3))
    train_posteriors[np.arange(180), states] = 1.0
    monkeypatch.setattr(training_subject, "causal_forward_posteriors", lambda *args, **kwargs: train_posteriors)
    item = B3TrainOnlySeries(
        sector_code="S001",
        sector_name="Sector 1",
        train_observations=np.ones((180, 7)),
        train_dates=tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180)),
        pit_l2_constituents=("L2-001",),
        pit_constituent_manifest_hash="a" * 64,
        observation_manifest_hash="b" * 64,
        train_input_manifest=_train_manifest(
            tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180)),
            np.ones((180, 7)),
        ),
    )
    entry, _ = training_subject._fit_b3_train_only(
        item,
        family="legacy_covfix",
        level="L1",
        feature_names=BASE_FEATURES,
        preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
        seed=42,
        numeric_environment={"packages": {"hmmlearn": "0.3.3"}},
    )

    assert entry["final_train_log_likelihood"] == -0.995
    assert entry["final_train_log_likelihood_source"] == "monitor_history_terminal_value"
    assert entry["validation_accessed"] is False
    assert entry["future_utility_accessed"] is False
    assert entry["parameter_profile"]["numeric_contract_status"] == "USER_APPROVED_FORMAL_CONTRACT"
    assert np.array_equal(observed_raw_covariance["value"], np.ones((3, 7)))


def _refit03_training_item(feature_count: int = 2) -> B3TrainOnlySeries:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index) for index in range(120))
    observations = np.ones((120, feature_count), dtype=np.float64)
    return B3TrainOnlySeries(
        sector_code="S001",
        sector_name="Sector 1",
        train_observations=observations,
        train_dates=dates,
        pit_l2_constituents=("L2-001",),
        pit_constituent_manifest_hash="a" * 64,
        observation_manifest_hash="b" * 64,
        train_input_manifest=_train_manifest(dates, observations),
    )


def _install_refit03_training_model(monkeypatch, *, feature_count: int, raw_after_fit):
    class _Monitor:
        converged = True
        iter = 2
        n_iter = 300
        tol = 0.01
        history = deque([-1.0, -0.995])

    class _GaussianHMM:
        def __init__(self, **kwargs) -> None:
            del kwargs
            self.monitor_ = _Monitor()

        @property
        def covars_(self):
            raise AssertionError("public covariance representation must not be read after fit")

        @covars_.setter
        def covars_(self, value):
            self.initial_covars = np.asarray(value, dtype=np.float64)

        def fit(self, train):
            del train
            if raw_after_fit is not None:
                self._covars_ = np.asarray(raw_after_fit, dtype=np.float64)
            return self

    monkeypatch.setattr("hmmlearn.hmm.GaussianHMM", _GaussianHMM)
    monkeypatch.setattr(training_subject, "_sector_local_reference_variance", lambda train: np.ones(feature_count))
    monkeypatch.setattr(
        training_subject,
        "_manual_b3_diag04_initialization",
        lambda train, sector_reference_variance, random_seed: (
            np.full(3, 1 / 3),
            np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            np.asarray([[-1.0] * feature_count, [0.0] * feature_count, [1.0] * feature_count]),
            np.ones((3, feature_count)),
            {
                "schema_version": "hmm_risk_c008_b3_diag04_manual_initialization_v1",
                "cluster_counts": [40, 40, 40],
                "sector_local_reference_variance_R_sj": [1.0] * feature_count,
                "nu": 1.0,
            },
        ),
    )


def test_refit03_missing_private_covariance_never_falls_back_to_public_representation(monkeypatch) -> None:
    item = _refit03_training_item()
    _install_refit03_training_model(monkeypatch, feature_count=2, raw_after_fit=None)

    with pytest.raises(B3TrainingStageError) as exc_info:
        training_subject.fit_b3_preprocessed_train_only(
            item,
            train=item.train_observations,
            seed=42,
        )

    error = exc_info.value
    assert error.stage == "covariance"
    assert error.reason_code == "hmm_risk_model_covariance_raw_type_invalid"
    raw = error.stage_evidence["raw_covariance_evidence"]
    assert raw["actual_python_type"] == "builtins.NoneType"
    assert raw["evidence_unavailable_reason"] == "gaussian_hmm_internal_diag_covars_missing"
    assert error.stage_evidence["stage_specific_cause_evidence"]["covariance_status"] == "failed"


def test_refit03_likelihood_failure_preserves_raw_covariance_and_marks_d4_unavailable(monkeypatch) -> None:
    item = _refit03_training_item()
    _install_refit03_training_model(
        monkeypatch,
        feature_count=2,
        raw_after_fit=np.full((3, 2), 0.5, dtype=np.float64),
    )
    monkeypatch.setattr(
        training_subject,
        "evaluate_likelihood_acceptance",
        lambda evidence: (_ for _ in ()).throw(ValueError("likelihood receipt invalid")),
    )

    with pytest.raises(B3TrainingStageError) as exc_info:
        training_subject.fit_b3_preprocessed_train_only(
            item,
            train=item.train_observations,
            seed=42,
        )

    evidence = exc_info.value.stage_evidence
    assert exc_info.value.stage == "likelihood"
    assert evidence["completed_stages"] == ["initialization", "fit", "raw_covariance_capture", "monitor"]
    assert evidence["raw_covariance_evidence"]["raw_validity"] is True
    cause = evidence["stage_specific_cause_evidence"]
    assert cause["d4_derived_evidence_status"] == "not_computable_posterior_audit_unavailable"
    assert cause["covariance_status"] == "insufficient_evidence"
    assert cause["dynamic_lower_reference"] is None


@pytest.mark.parametrize(
    ("raw_after_fit", "posterior_failure_kind", "derived_status", "covariance_status"),
    [
        (
            np.zeros((3, 2), dtype=np.float64),
            None,
            "not_computable_raw_covariance_invalid",
            "failed",
        ),
        (
            np.full((3, 2), 0.5, dtype=np.float64),
            "unavailable",
            "not_computable_posterior_audit_unavailable",
            "insufficient_evidence",
        ),
        (
            np.full((3, 2), 0.5, dtype=np.float64),
            "invalid",
            "not_computable_posterior_audit_invalid",
            "failed",
        ),
    ],
)
def test_refit03_actual_covariance_failure_mapping_is_typed_and_never_fabricates_derived_fields(
    monkeypatch,
    raw_after_fit,
    posterior_failure_kind,
    derived_status,
    covariance_status,
) -> None:
    item = _refit03_training_item()
    _install_refit03_training_model(monkeypatch, feature_count=2, raw_after_fit=raw_after_fit)
    if posterior_failure_kind is not None:
        failure = StateModelSetError("posterior audit failed")
        failure.stage = "smoothed_posterior_audit"
        failure.evidence = (
            {"error_type": "ValueError", "error": "unavailable"}
            if posterior_failure_kind == "unavailable"
            else {"shape": [120, 2]}
        )
        monkeypatch.setattr(
            training_subject,
            "_b3_diag04_covariance_evidence",
            lambda *args, **kwargs: (_ for _ in ()).throw(failure),
        )

    with pytest.raises(B3TrainingStageError) as exc_info:
        training_subject.fit_b3_preprocessed_train_only(
            item,
            train=item.train_observations,
            seed=42,
        )

    evidence = exc_info.value.stage_evidence
    assert exc_info.value.stage == "covariance"
    assert evidence["completed_stages"] == [
        "initialization",
        "fit",
        "raw_covariance_capture",
        "monitor",
        "likelihood",
    ]
    cause = evidence["stage_specific_cause_evidence"]
    assert cause["d4_derived_evidence_status"] == derived_status
    assert cause["covariance_status"] == covariance_status
    assert cause["covariance_valid"] is False
    assert cause["state_posterior_mass"] is None
    assert cause["dynamic_lower_reference"] is None
    assert cause["dynamic_upper_reference"] is None


def test_preprocessed_initialization_probe_uses_formal_authority_without_hmm_fit(monkeypatch) -> None:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180))
    observations = np.ones((180, 7), dtype=np.float64)
    item = B3TrainOnlySeries(
        sector_code="S001",
        sector_name="Sector 1",
        train_observations=observations,
        train_dates=dates,
        pit_l2_constituents=("L2-001",),
        pit_constituent_manifest_hash="a" * 64,
        observation_manifest_hash="b" * 64,
        train_input_manifest=_train_manifest(dates, observations),
    )
    monkeypatch.setattr(training_subject, "_sector_local_reference_variance", lambda train: np.ones(7))
    monkeypatch.setattr(
        training_subject,
        "_manual_b3_diag04_initialization",
        lambda train, sector_reference_variance, random_seed: (
            np.full(3, 1 / 3),
            np.asarray([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
            np.asarray([[-1.0] * 7, [0.0] * 7, [1.0] * 7]),
            np.ones((3, 7)),
            {"schema_version": "diagnostic_init", "cluster_counts": [60, 60, 60]},
        ),
    )

    evidence = training_subject.prepare_b3_preprocessed_train_only_initialization(
        item,
        train=observations,
        seed=42,
    )

    assert evidence["schema_version"] == "hmm_risk_b3_manual_initialization_v1"
    assert evidence["contract_version"] == D3_CONTRACT_VERSION
    assert evidence["diagnostic_source_contract"] == "diagnostic_init"
    assert evidence["formal_initialization_contract_applied"] is True


def _train_only_level() -> dict[str, B3TrainOnlySeries]:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(120))
    return {
        f"S{index:03d}": B3TrainOnlySeries(
            sector_code=f"S{index:03d}",
            sector_name=f"Sector {index}",
            train_observations=np.ones((120, 7), dtype=np.float64),
            train_dates=dates,
            pit_l2_constituents=(f"L2-{index:03d}",),
            pit_constituent_manifest_hash="a" * 64,
            observation_manifest_hash="b" * 64,
            train_input_manifest=_train_manifest(
                dates,
                np.ones((120, 7), dtype=np.float64),
                sector_code=f"S{index:03d}",
            ),
        )
        for index in range(31)
    }


def test_level_repeat_records_only_typed_candidate_failures_and_propagates_programming_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        training_subject,
        "c008_b3_diag04_fixed_numeric_environment",
        lambda: {"packages": {"hmmlearn": "0.3.3"}},
    )
    monkeypatch.setattr(
        training_subject,
        "_fit_b3_train_only",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            B3TrainingStageError("covariance", "hmm_risk_model_covariance_invalid", ValueError("invalid"))
        ),
    )
    repeat, models = run_level_repeat(
        _train_only_level(),
        family="legacy_covfix",
        level="L1",
        feature_names=BASE_FEATURES,
        preprocess_family="identity",
        process_identity="test_process",
    )
    assert models == {}
    assert len(repeat["entries"]) == 8 * 31
    assert {entry["failure_stage"] for entry in repeat["entries"]} == {"covariance"}
    assert {tuple(entry["failure_reason_codes"]) for entry in repeat["entries"]} == {
        ("hmm_risk_model_covariance_invalid",)
    }

    monkeypatch.setattr(
        training_subject,
        "_fit_b3_train_only",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("programming defect")),
    )
    with pytest.raises(RuntimeError, match="programming defect"):
        run_level_repeat(
            _train_only_level(),
            family="legacy_covfix",
            level="L1",
            feature_names=BASE_FEATURES,
            preprocess_family="identity",
            process_identity="test_process",
        )


def _validation_dates() -> tuple[date, ...]:
    available = pd.bdate_range("2024-07-01", "2025-03-31")
    indexes = np.linspace(0, len(available) - 1, 182, dtype=int)
    return tuple(available[index].date() for index in indexes)


def _semantic_receipt(model_hash: str, *, level: str, sector_code: str) -> dict:
    dates = _validation_dates()
    states = [(index // 3) % 3 for index in range(182)]
    posterior = np.zeros((182, 3), dtype=np.float64)
    posterior[np.arange(182), states] = 1.0
    values = np.asarray([(-0.02, 0.0, 0.02)[state] for state in states], dtype=np.float64)
    utility = {
        "excess_return_5d": values,
        "excess_return_10d": values,
        "excess_return_20d": values,
        "source_cutoff": "2025-04-30",
        "formula_version": "hmm_risk_hard_future_excess_035_035_030_v1",
    }
    component_hashes = {
        key: canonical_sha256(values.tolist()) for key in ("excess_return_5d", "excess_return_10d", "excess_return_20d")
    }
    encoded_dates = [item.isoformat() for item in dates]
    manifest = {
        "schema_version": "hmm_risk_d6_frozen_input_manifest_v1",
        "direct_sector_level": level,
        "sector_code": sector_code,
        "benchmark_identity": "000300.SH",
        "validation_observation_sha256": "f" * 64,
        "validation_dates": encoded_dates,
        "validation_dates_sha256": canonical_sha256(encoded_dates),
        "dataset_manifest_hash": "e" * 64,
        "mapping_manifest_hash": "f" * 64,
        "calendar_manifest_hash": "1" * 64,
        "l2_stock_fact_manifest_hash": "2" * 64,
        "feature_domain_policy_sha256": TEST_POLICY_SHA256,
        "source_cutoff": utility["source_cutoff"],
        "formula_version": utility["formula_version"],
        "utility_component_sha256": component_hashes,
        "combined_utility_sha256": canonical_sha256((0.35 * values + 0.35 * values + 0.30 * values).tolist()),
    }
    return evaluate_semantic_validation(
        posterior,
        dates,
        utility,
        frozen_input_manifest=manifest,
        selected_model_payload_sha256=model_hash,
    )


def _training_receipt(model: B3FittedModel) -> dict:
    dates = tuple(date(2022, 1, 3) + timedelta(days=index * 7) for index in range(180))
    states = [index % 3 for index in range(180)]
    posterior = np.zeros((180, 3), dtype=np.float64)
    posterior[np.arange(180), states] = 1.0
    likelihood = evaluate_likelihood_acceptance(
        {"converged": True, "iterations": 2, "maximum_iterations": 300, "history": [-1.0, -0.995]}
    )
    effective_count = int(model.means.shape[1])
    covariance = evaluate_covariance_acceptance(
        {
            "raw_covars": np.ones((3, effective_count)).tolist(),
            "sector_local_reference_variance_R_sj": [1.0] * effective_count,
            "state_posterior_mass": [60.0, 60.0, 60.0],
            "posterior_second_moment_about_fitted_mean": np.ones((3, effective_count)).tolist(),
            "train_rows": 180,
            "nu": 1.0,
            "postfit_projection_performed": False,
        }
    )
    occupancy = evaluate_train_occupancy(
        posterior,
        dates,
        frozen_input_manifest=_train_manifest(
            dates,
            np.ones((180, len(model.feature_names)), dtype=np.float64),
            sector_code=model.sector_code,
            direct_sector_level=model.level,
        ),
    )
    body = {
        "schema_version": (
            MIXED_TRAINING_ENTRY_SCHEMA_VERSION
            if model.projection_receipt is not None
            else "hmm_risk_b3_training_entry_receipt_v1"
        ),
        "family": model.family,
        "level": model.level,
        "seed": model.seed,
        "sector_code": model.sector_code,
        "fit_status": "accepted",
        "model_entry_status": "accepted",
        "model_entry_valid": True,
        "model_payload_sha256": model.model_payload_sha256,
        "likelihood": likelihood,
        "covariance": covariance,
        "train_occupancy": occupancy,
        "final_train_log_likelihood": -0.995,
        "training_rows": 180,
        "feature_count": len(model.feature_names),
    }
    if model.projection_receipt is not None:
        body.update(
            {
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "likelihood_feature_count": effective_count,
                "projection_receipt": dict(model.projection_receipt),
                "projection_sha256": model.projection_receipt["projection_sha256"],
            }
        )
    return {**body, "entry_receipt_sha256": canonical_sha256(body)}


def _selection(family: str, level: str, training_receipts: list[dict]) -> dict:
    codes = [str(receipt["sector_code"]) for receipt in training_receipts]
    hashes = [str(receipt["entry_receipt_sha256"]) for receipt in training_receipts]
    mixed_dimension = family == "autocycle_all_core" and level == "L2"
    aggregate = {"minimum": -1.0, "median": -1.0, "mean": -1.0}
    if mixed_dimension:
        score_receipts = []
        scores = []
        for receipt in training_receipts:
            final = float(receipt["final_train_log_likelihood"])
            rows = int(receipt["training_rows"])
            dimension = int(receipt["likelihood_feature_count"])
            score = final / (rows * dimension)
            score_body = {
                "sector_code": receipt["sector_code"],
                "score": score,
                "training_rows": rows,
                "final_train_log_likelihood": final,
                "effective_dimension": dimension,
                "denominator": rows * dimension,
                "formula": "final_train_log_likelihood/(training_rows*effective_dimension)",
                "schema_version": "hmm_risk_b3_d5_effective_dimension_score_receipt_v1",
                "dimension_contract_version": MIXED_DIMENSION_CONTRACT_VERSION,
                "projection_sha256": receipt["projection_sha256"],
            }
            score_receipts.append({**score_body, "score_sha256": canonical_sha256(score_body)})
            scores.append(score)
        ordered_scores = sorted(scores)
        aggregate = {
            "minimum": ordered_scores[0],
            "median": ordered_scores[len(ordered_scores) // 2],
            "mean": math.fsum(scores) / len(scores),
            "ordered_sector_scores": score_receipts,
        }
    candidates = [
        {
            "seed": seed,
            "schedule_index": index,
            "eligible": seed == 42,
            "aggregate": aggregate if seed == 42 else None,
            "entry_receipt_hashes": hashes if seed == 42 else [],
            "warning_reason_codes": [],
        }
        for index, seed in enumerate(RESTART_SCHEDULE)
    ]
    body = {
        "contract_version": D5_SELECTION_VERSION,
        "failure_reason_codes": [],
        "blocking_reason_codes": [],
        "warning_reason_codes": [],
        "primary_reason_code": None,
        "evidence": {
            "family": family,
            "level": level,
            "feature_domain_policy_sha256": TEST_POLICY_SHA256,
            "selected_seed": 42,
            "selected_schedule_index": 0,
            "canonical_sector_codes": codes,
            "canonical_sector_set_sha256": canonical_sha256(codes),
            "schedule": list(RESTART_SCHEDULE),
            "feature_count": 7 if family == "legacy_covfix" else 20,
            "repeat_entries_sha256": canonical_sha256(training_receipts),
            "candidates": candidates,
            "lexicographic_filters": [
                {"component": component, "best": -1.0, "survivor_seeds": [42]}
                for component in ("minimum", "median", "mean")
            ],
            "validation_accessed": False,
            "future_utility_accessed": False,
            "semantic_labelability_accessed": False,
            "d6_status_accessed": False,
            "selection_followed_by_refit": False,
        },
        "level_selection_status": "accepted",
        "level_selection_valid": True,
    }
    if mixed_dimension:
        body["evidence"]["dimension_contract_version"] = MIXED_DIMENSION_CONTRACT_VERSION
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _selected_artifact(family: str, level: str) -> tuple[dict, dict]:
    count = 31 if level == "L1" else 131
    codes = [f"{level}-{index:03d}" for index in range(count)]
    if family == "autocycle_all_core" and level == "L2":
        codes[-1] = TARGET_SECTOR
        codes = sorted(codes)
    entries = []
    training_receipts = []
    for code in codes:
        model = _model(family=family, level=level, code=code)
        training_receipt = _training_receipt(model)
        training_receipts.append(training_receipt)
        semantic_mapping = {"0": "fading", "1": "neutral", "2": "trending"}
        semantic = _semantic_receipt(model.model_payload_sha256, level=level, sector_code=model.sector_code)
        entry_body = {
            **model.payload(),
            "training_receipt": training_receipt,
            "semantic": semantic,
            "validation_accessed_after_selection": True,
            "future_utility_accessed_after_selection": True,
            "selection_reexecuted": False,
            "semantic_mapping": semantic_mapping,
        }
        entries.append({**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)})
    selection = _selection(family, level, training_receipts)
    body = {
        "schema_version": (
            MIXED_LEVEL_SCHEMA_VERSION
            if family == "autocycle_all_core" and level == "L2"
            else "hmm_risk_b3_selected_level_artifact_v1"
        ),
        "family": family,
        "level": level,
        "selected_seed": 42,
        "selection_receipt_sha256": selection["receipt_sha256"],
        "status": "accepted",
        "entry_count": count,
        "entries": entries,
        "selection_reexecuted": False,
        "ready": False,
    }
    if family == "autocycle_all_core" and level == "L2":
        body.update(
            build_level_dimension_identity(
                entries,
                family=family,
                level=level,
                expected_sector_codes=codes,
            )
        )
    return {**body, "artifact_sha256": canonical_sha256(body)}, selection


def test_ready_writer_requires_both_families_and_direct_levels(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}
    artifacts = {key: pair[0] for key, pair in pairs.items()}
    selections = {key: pair[1] for key, pair in pairs.items()}
    policy_manifest = TEST_POLICY_MANIFEST
    manifest_path = write_b3_ready_model_set(
        tmp_path,
        selected_artifacts=artifacts,
        selection_receipts=selections,
        dataset_manifest_hash="a" * 64,
        mapping_manifest_hash="b" * 64,
        calendar_manifest_hash="c" * 64,
        l2_stock_fact_manifest_hash="d" * 64,
        semantic_dataset_manifest_hash="e" * 64,
        semantic_mapping_manifest_hash="f" * 64,
        semantic_calendar_manifest_hash="1" * 64,
        semantic_l2_stock_fact_manifest_hash="2" * 64,
        feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
        feature_domain_policy_manifest=policy_manifest,
        producer_commit="c" * 40,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    readback = read_b3_ready_model_set(manifest_path)
    assert readback == manifest
    assert manifest["schema_version"] == "hmm_risk_state_model_set_v1"
    assert manifest["status"] == "READY"
    assert len(manifest["layers"]) == 4
    assert manifest["feature_domain_policy_sha256"] == policy_manifest["receipt_sha256"]
    assert manifest["feature_domain_policy_manifest"] == policy_manifest
    assert manifest["dataset_manifest_hash"] == "a" * 64
    assert manifest["semantic_dataset_manifest_hash"] == "e" * 64
    assert manifest["semantic_mapping_manifest_hash"] == "f" * 64
    assert manifest["semantic_calendar_manifest_hash"] == "1" * 64
    assert manifest["semantic_l2_stock_fact_manifest_hash"] == "2" * 64
    invalid_policy = json.loads(json.dumps(policy_manifest))
    invalid_entry = invalid_policy["contributor_ledger"][0]
    invalid_entry["moneyflow_contributor_eligible"] = "true"
    invalid_entry_body = {key: value for key, value in invalid_entry.items() if key != "entry_sha256"}
    invalid_entry["entry_sha256"] = canonical_sha256(invalid_entry_body)
    invalid_policy["contributor_ledger_sha256"] = canonical_sha256(invalid_policy["contributor_ledger"])
    invalid_policy_body = {key: value for key, value in invalid_policy.items() if key != "receipt_sha256"}
    invalid_policy["receipt_sha256"] = canonical_sha256(invalid_policy_body)
    with pytest.raises(StateModelSetError, match="policy contributor ledger identity is invalid"):
        write_b3_ready_model_set(
            tmp_path / "invalid-policy",
            selected_artifacts=artifacts,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=invalid_policy["receipt_sha256"],
            feature_domain_policy_manifest=invalid_policy,
            producer_commit="c" * 40,
        )

    incomplete = dict(artifacts)
    incomplete.pop(("legacy_covfix", "L2"))
    with pytest.raises(StateModelSetError, match="both families and both direct levels"):
        write_b3_ready_model_set(
            tmp_path / "incomplete",
            selected_artifacts=incomplete,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
            feature_domain_policy_manifest=policy_manifest,
            producer_commit="c" * 40,
        )


def test_selected_level_readback_rejects_self_consistent_score_receipt_drift(tmp_path) -> None:
    artifact, selection = _selected_artifact("autocycle_all_core", "L2")
    selection = json.loads(json.dumps(selection))
    artifact = json.loads(json.dumps(artifact))
    selected = next(candidate for candidate in selection["evidence"]["candidates"] if candidate["seed"] == 42)
    score_receipt = selected["aggregate"]["ordered_sector_scores"][0]
    score_receipt["denominator"] += 1
    score_body = {key: value for key, value in score_receipt.items() if key != "score_sha256"}
    score_receipt["score_sha256"] = canonical_sha256(score_body)
    selection_body = {key: value for key, value in selection.items() if key != "receipt_sha256"}
    selection["receipt_sha256"] = canonical_sha256(selection_body)
    artifact["selection_receipt_sha256"] = selection["receipt_sha256"]
    artifact_body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    artifact["artifact_sha256"] = canonical_sha256(artifact_body)
    artifact_path = tmp_path / "selected.json"
    artifact_path.write_bytes(canonical_json_bytes(artifact))

    with pytest.raises(StateModelSetError, match="selection_contract_unsatisfied"):
        read_b3_selected_level_artifact(
            artifact_path,
            selection=selection,
            family="autocycle_all_core",
            level="L2",
            expected_count=131,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=TEST_POLICY_SHA256,
        )


def test_ready_writer_rejects_semantic_lineage_drift(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}

    with pytest.raises(StateModelSetError, match="frozen input lineage"):
        write_b3_ready_model_set(
            tmp_path,
            selected_artifacts={key: pair[0] for key, pair in pairs.items()},
            selection_receipts={key: pair[1] for key, pair in pairs.items()},
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="9" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=TEST_POLICY_MANIFEST["receipt_sha256"],
            feature_domain_policy_manifest=TEST_POLICY_MANIFEST,
            producer_commit="c" * 40,
        )


def test_ready_writer_rejects_rehashed_policy_source_identity_drift(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}
    policy_manifest = _rehash_policy({**TEST_POLICY_MANIFEST, "calendar_manifest_hash": "9" * 64})

    with pytest.raises(StateModelSetError, match="policy source identity"):
        write_b3_ready_model_set(
            tmp_path,
            selected_artifacts={key: pair[0] for key, pair in pairs.items()},
            selection_receipts={key: pair[1] for key, pair in pairs.items()},
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
            feature_domain_policy_manifest=policy_manifest,
            producer_commit="c" * 40,
        )


def test_ready_writer_rejects_declared_count_without_durable_entries(tmp_path) -> None:
    keys = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    pairs = {key: _selected_artifact(*key) for key in keys}
    artifacts = {key: pair[0] for key, pair in pairs.items()}
    selections = {key: pair[1] for key, pair in pairs.items()}
    target = dict(artifacts[("legacy_covfix", "L1")])
    target["entries"] = []
    target_body = {key: value for key, value in target.items() if key != "artifact_sha256"}
    target["artifact_sha256"] = canonical_sha256(target_body)
    artifacts[("legacy_covfix", "L1")] = target
    policy_manifest = TEST_POLICY_MANIFEST

    with pytest.raises(StateModelSetError, match="selected entry count"):
        write_b3_ready_model_set(
            tmp_path,
            selected_artifacts=artifacts,
            selection_receipts=selections,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=policy_manifest["receipt_sha256"],
            feature_domain_policy_manifest=policy_manifest,
            producer_commit="c" * 40,
        )


def test_ready_layer_rejects_rehashed_but_empty_semantic_evidence() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")
    entry = dict(artifact["entries"][0])
    semantic = dict(entry["semantic"])
    semantic_evidence = dict(semantic["semantic_evidence"])
    semantic_evidence["evidence"] = {}
    semantic_evidence_body = {key: value for key, value in semantic_evidence.items() if key != "receipt_sha256"}
    semantic["semantic_evidence"] = {
        **semantic_evidence_body,
        "receipt_sha256": canonical_sha256(semantic_evidence_body),
    }
    entry["semantic"] = semantic
    entry_body = {key: value for key, value in entry.items() if key != "selected_entry_sha256"}
    artifact["entries"][0] = {**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)}
    artifact_body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    artifact["artifact_sha256"] = canonical_sha256(artifact_body)

    with pytest.raises(StateModelSetError, match="semantic evidence is not accepted"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=TEST_POLICY_SHA256,
        )


def test_ready_layer_rejects_selection_receipt_hashes_not_linked_to_durable_training_receipts() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")
    selection = json.loads(json.dumps(selection))
    selected = next(candidate for candidate in selection["evidence"]["candidates"] if candidate["seed"] == 42)
    selected["entry_receipt_hashes"] = ["0" * 64 for _ in range(31)]
    selection_body = {key: value for key, value in selection.items() if key != "receipt_sha256"}
    selection = {**selection_body, "receipt_sha256": canonical_sha256(selection_body)}
    artifact["selection_receipt_sha256"] = selection["receipt_sha256"]
    artifact_body = {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    artifact["artifact_sha256"] = canonical_sha256(artifact_body)

    with pytest.raises(StateModelSetError, match="selection receipt lineage is invalid"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256=TEST_POLICY_SHA256,
        )


def test_ready_layer_rejects_feature_domain_policy_lineage_drift() -> None:
    artifact, selection = _selected_artifact("legacy_covfix", "L1")

    with pytest.raises(StateModelSetError, match="selection contract is invalid"):
        training_subject._validate_ready_layer(
            artifact,
            selection,
            family="legacy_covfix",
            level="L1",
            expected_count=31,
            dataset_manifest_hash="a" * 64,
            mapping_manifest_hash="b" * 64,
            calendar_manifest_hash="c" * 64,
            l2_stock_fact_manifest_hash="d" * 64,
            semantic_dataset_manifest_hash="e" * 64,
            semantic_mapping_manifest_hash="f" * 64,
            semantic_calendar_manifest_hash="1" * 64,
            semantic_l2_stock_fact_manifest_hash="2" * 64,
            feature_domain_policy_sha256="0" * 64,
        )
