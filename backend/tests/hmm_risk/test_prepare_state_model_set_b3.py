from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, BASE_FEATURES, StateModelSetError
from scripts.hmm_risk import prepare_state_model_set as subject


def _request() -> dict:
    dataset = {"schema_version": "dataset_v1", "calendar_benchmark": {"schema_version": "calendar_v1"}}
    mapping = {"schema_version": "mapping_v1"}
    l2_stock_fact = {"schema_version": "l2_dataset_v1"}
    return {
        "schema_version": subject.REQUEST_SCHEMA,
        "source": {
            "universe_key": "frozen-universe",
            "universe_rule_version": "frozen-rule",
            "source_start": "2020-07-30",
            "source_end": "2025-04-30",
            "security_identity_manifest_path": ("backend/services/hmm_risk/manifests/security_source_identity_v1.json"),
            "security_identity_manifest_sha256": ("24e0070fd97e00e5021eafc295426144b5b2eb3f7d76d4828aab18fe6d21358f"),
            "provider_absence_manifest_path": ("backend/services/hmm_risk/manifests/provider_absence_v1.json"),
            "provider_absence_manifest_sha256": ("717b899cbc5cebfa41f9ffe9d4fe32055f033bc93d1712d5da6a983a6a93e886"),
        },
        "producer_commit": "c" * 40,
        "dataset_manifest_hash": subject.canonical_sha256(dataset),
        "mapping_manifest_hash": subject.canonical_sha256(mapping),
        "l2_stock_fact_manifest_hash": subject.canonical_sha256(l2_stock_fact),
        "families": [
            {
                "family": "legacy_covfix",
                "feature_names": list(BASE_FEATURES),
                "preprocess_family": "identity",
                "train_start": "2022-01-01",
                "train_end": "2024-06-30",
                "validation_start": "2024-07-01",
                "validation_end": "2025-03-31",
            },
            {
                "family": "autocycle_all_core",
                "feature_names": list(ALL_CORE_FEATURES),
                "preprocess_family": "winsor_zscore_1_99_train_global_v1",
                "train_start": "2022-01-01",
                "train_end": "2024-06-30",
                "validation_start": "2024-07-01",
                "validation_end": "2025-03-31",
            },
        ],
    }


def _preflight_inputs() -> dict:
    values = {
        "database": {"host": "127.0.0.1", "port": 5432, "dbname": "aistock"},
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "stock_facts": {
                "schema_version": "l1_facts_v1",
                "aggregate_row_count": 33_221,
                "invalid_l1_date_count": 2_491,
                "moneyflow_provider_absence_count": 502,
                "moneyflow_provider_absence_key_sha256": "1" * 64,
                "moneyflow_alias_resolution_count": 591,
                "moneyflow_alias_resolution_key_sha256": "2" * 64,
                "circ_mv_asof_stale_count": 466,
                "circ_mv_asof_max_staleness_trading_days": 9,
                "circ_mv_asof_stale_key_sha256": "3" * 64,
                "circ_mv_lookback_contract_version": "hmm_risk_causal_circ_mv_source_window_v1",
                "circ_mv_history_start": "2020-07-30",
                "circ_mv_pit_boundary_crossing_count": 1_073,
                "circ_mv_pit_boundary_crossing_available_count": 1_073,
                "circ_mv_pit_boundary_crossing_invalid_count": 0,
                "circ_mv_pit_boundary_crossing_key_sha256": "4" * 64,
            },
            "calendar_benchmark": {"schema_version": "calendar_v1", "row_count": 601},
            "security_source_identity": {
                "schema_version": "hmm_risk_security_source_identity_manifest_v1",
                "manifest_sha256": "a" * 64,
            },
            "provider_absence_authority": {
                "schema_version": "hmm_risk_provider_absence_manifest_v1",
                "manifest_sha256": "b" * 64,
            },
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {
            "schema_version": "l2_dataset_v1",
            "aggregate_row_count": 145_805,
            "invalid_sector_date_count": 4_067,
            "moneyflow_provider_absence_count": 502,
            "moneyflow_provider_absence_key_sha256": "1" * 64,
            "moneyflow_alias_resolution_count": 591,
            "moneyflow_alias_resolution_key_sha256": "2" * 64,
            "circ_mv_asof_stale_count": 466,
            "circ_mv_asof_max_staleness_trading_days": 9,
            "circ_mv_asof_stale_key_sha256": "3" * 64,
            "circ_mv_lookback_contract_version": "hmm_risk_causal_circ_mv_source_window_v1",
            "circ_mv_history_start": "2020-07-30",
            "circ_mv_pit_boundary_crossing_count": 1_073,
            "circ_mv_pit_boundary_crossing_available_count": 1_073,
            "circ_mv_pit_boundary_crossing_invalid_count": 0,
            "circ_mv_pit_boundary_crossing_key_sha256": "4" * 64,
        },
        "panel": [object()] * 35_712,
        "l2_panel": [object()] * 150_912,
        "security_identity_manifest": {
            "schema_version": "hmm_risk_security_source_identity_manifest_v1",
            "manifest_sha256": "a" * 64,
        },
        "provider_absence_manifest": {
            "schema_version": "hmm_risk_provider_absence_manifest_v1",
            "manifest_sha256": "b" * 64,
        },
    }
    definition = {
        "schema_version": subject.C010_FORMULA_VERSION,
        "feature_domain_policy_version": subject.C010_POLICY_VERSION,
        "diagnostic_only": False,
        "moneyflow_mandatory_fields": [
            "buy_sm_amount_cny",
            "sell_sm_amount_cny",
            "buy_elg_amount_cny",
            "sell_elg_amount_cny",
            "net_mf_amount_cny",
        ],
    }

    def entry(value: dict) -> dict:
        return {**value, "entry_sha256": subject.canonical_sha256(value)}

    def receipt(value: dict) -> dict:
        return {**value, "receipt_sha256": subject.canonical_sha256(value)}

    eligibility_entries = [
        entry({"canonical_ts_code": symbol, "moneyflow_contributor_eligible": True})
        for symbol in ("000001.SZ", "000002.SZ")
    ]
    domain_entry = entry({"direct_sector_level": "L1", "sector_code": "801010.SI", "trade_date": "2022-01-04"})
    l2_domain_entry = entry({"direct_sector_level": "L2", "sector_code": "801012.SI", "trade_date": "2022-01-04"})
    trading_dates = tuple(date(2022, 1, 1) + timedelta(days=index) for index in range(601))
    cross_entries = [entry({"index": index}) for index in range(4 * len(trading_dates))]
    eligibility = receipt(
        {
            "schema_version": "hmm_risk_c010_train_observation_eligibility_v1",
            "entry_count": len(eligibility_entries),
            "entries": eligibility_entries,
            "excluded_moneyflow_symbols": ["689009.SH"],
            "diagnostic_only": False,
            "formal_policy_activated": True,
        }
    )
    aggregate = receipt(
        {
            "schema_version": "hmm_risk_c010_feature_domain_aggregate_evidence_v1",
            "l1_aggregate_count": 1,
            "l2_aggregate_count": 1,
            "l1_domain_receipts": [domain_entry],
            "l2_domain_receipts": [l2_domain_entry],
            "l1_invalid_price_domain": [],
            "l2_invalid_price_domain": [],
            "formal_policy_activated": True,
        }
    )
    l1_cross = receipt(
        {
            "schema_version": "hmm_risk_c010_feature_cross_section_receipt_set_v1",
            "direct_sector_level": "L1",
            "expected_sector_count": 31,
            "entry_count": len(cross_entries),
            "entries": cross_entries,
            "diagnostic_only": False,
        }
    )
    l2_cross = receipt(
        {
            "schema_version": "hmm_risk_c010_feature_cross_section_receipt_set_v1",
            "direct_sector_level": "L2",
            "expected_sector_count": 131,
            "entry_count": len(cross_entries),
            "entries": cross_entries,
            "diagnostic_only": False,
        }
    )
    values["c010_diagnostic"] = {
        "eligibility": eligibility,
        "aggregate_evidence": aggregate,
        "l1_cross_section_evidence": l1_cross,
        "l2_cross_section_evidence": l2_cross,
        "l1_feature_definition": dict(definition),
        "l2_feature_definition": dict(definition),
    }
    values["trading_dates"] = trading_dates
    return values


def _approve_preflight_template(monkeypatch, request: dict) -> None:
    monkeypatch.setattr(
        subject,
        "B3_APPROVED_FROZEN_IDENTITIES",
        {
            "dataset_manifest_hash": request["dataset_manifest_hash"],
            "mapping_manifest_hash": request["mapping_manifest_hash"],
            "l2_stock_fact_manifest_hash": request["l2_stock_fact_manifest_hash"],
        },
    )


def _coverage_preflight(*, valid: bool = True, policy_sha256: str | None = None) -> dict:
    body = {
        "schema_version": "hmm_risk_b3_train_coverage_preflight_set_v1",
        "feature_domain_policy_sha256": policy_sha256,
        "formula_version": subject.C010_FORMULA_VERSION if policy_sha256 else None,
        "reports": {},
        "report_count": 4,
        "train_coverage_valid": valid,
        "failure_reason_codes": [] if valid else ["hmm_risk_model_train_observation_coverage_insufficient"],
        "fit_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
    }
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


def test_legacy_fixed_seed_ready_writer_is_disabled() -> None:
    with pytest.raises(StateModelSetError, match="legacy fixed-seed preparation is disabled"):
        subject.prepare({}, artifact_root=None, output_root=None, db_prefix="TDX_DB_DEV_")


def test_formal_producer_identity_rejects_dirty_worktree(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_git_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=" M scripts/hmm_risk/prepare_state_model_set.py\n"),
    )

    with pytest.raises(StateModelSetError, match="formal B3 producer worktree must be clean"):
        subject._formal_producer_commit()


def test_formal_b3_rejects_well_formed_but_unapproved_frozen_identity() -> None:
    identities = dict(subject.B3_APPROVED_FROZEN_IDENTITIES)
    identities["dataset_manifest_hash"] = "f" * 64

    with pytest.raises(StateModelSetError, match="formal B3 frozen identity mismatch: dataset_manifest_hash"):
        subject._require_approved_b3_identities(identities)


def test_source_loader_requires_explicit_identity_and_provider_absence_manifests() -> None:
    with pytest.raises(StateModelSetError, match="security_identity_manifest_path"):
        subject._load_security_identity_manifest({})
    with pytest.raises(StateModelSetError, match="provider_absence_manifest_path"):
        subject._load_provider_absence_manifest({})


def test_preflight_rejects_live_manifest_drift_before_candidate_ready(monkeypatch) -> None:
    inputs = _preflight_inputs()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)

    with pytest.raises(StateModelSetError, match="formal B3 frozen identity mismatch"):
        subject.prepare_b3_preflight_candidate(_request(), db_prefix="TDX_DB_")


def test_preflight_freezes_current_identities_without_fit_selection_or_writes(monkeypatch) -> None:
    request = _request()
    old_producer = request["producer_commit"]
    inputs = _preflight_inputs()
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(policy_sha256=inputs["feature_domain_policy_sha256"]),
    )

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    candidate = report["request_candidate"]
    assert report["schema_version"] == subject.C010_FORMAL_PREFLIGHT_SCHEMA
    assert report["status"] == "candidate_ready"
    assert report["source_template_producer_commit"] == old_producer
    assert candidate["producer_commit"] == "d" * 40
    assert candidate["dataset_manifest_hash"] == subject.canonical_sha256(inputs["dataset_manifest"])
    assert candidate["mapping_manifest_hash"] == subject.canonical_sha256(inputs["mapping_manifest"])
    assert candidate["l2_stock_fact_manifest_hash"] == subject.canonical_sha256(inputs["l2_stock_fact_manifest"])
    assert candidate["feature_domain_policy_sha256"] == report["feature_domain_policy_sha256"]
    assert candidate["feature_domain_policy_manifest"] == report["feature_domain_policy_manifest"]
    assert report["feature_domain_policy_evidence"]["eligibility"]["entry_count"] == 2
    assert report["feature_domain_policy_evidence"]["aggregate"]["l1_aggregate_count"] == 1
    assert report["feature_domain_policy_manifest"]["feature_order_by_family"] == {
        "legacy_covfix": list(BASE_FEATURES),
        "autocycle_all_core": list(ALL_CORE_FEATURES),
    }
    assert (
        candidate["feature_domain_policy_manifest"]["aggregate_receipt"]
        == inputs["c010_diagnostic"]["aggregate_evidence"]
    )
    assert (
        candidate["feature_domain_policy_manifest"]["l1_cross_section_receipt"]
        == inputs["c010_diagnostic"]["l1_cross_section_evidence"]
    )
    assert candidate["parent_frozen_identities"] == subject.B3_APPROVED_FROZEN_IDENTITIES
    assert report["formula_version"] == subject.C010_FORMULA_VERSION
    assert report["train_trading_date_count"] == 601
    assert candidate["train_coverage_contract_version"] == subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION
    assert candidate["train_coverage_receipt_sha256"] == report["train_coverage"]["receipt_sha256"]
    assert report["request_candidate_sha256"] == subject.canonical_sha256(candidate)
    assert report["l1_sector_count"] == 31
    assert report["l1_aggregate_row_count"] == 33_221
    assert report["l1_invalid_sector_date_count"] == 2_491
    assert report["l1_panel_row_count"] == 35_712
    assert report["l2_sector_count"] == 131
    assert report["l2_aggregate_row_count"] == 145_805
    assert report["l2_invalid_sector_date_count"] == 4_067
    assert report["l2_panel_row_count"] == 150_912
    for field in (
        "fit_performed",
        "selection_performed",
        "formal_acceptance_thresholds_applied",
        "hard_semantic_authority_changed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    ):
        assert report[field] is False
    assert request["producer_commit"] == old_producer


def test_formal_preflight_freezes_stock_facts_to_train_window_and_preserves_circ_mv_history(
    monkeypatch,
) -> None:
    request = _request()
    inputs = _preflight_inputs()
    observed: dict[str, object] = {"sources": []}
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)

    def load_inputs(train_request, *, db_prefix, c010_formal=False):
        observed["sources"].append(dict(train_request["source"]))
        observed["db_prefix"] = db_prefix
        observed["c010_formal"] = c010_formal
        return inputs

    monkeypatch.setattr(subject, "_load_l1_source_inputs", load_inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda values, req: _coverage_preflight(policy_sha256=values["feature_domain_policy_sha256"]),
    )

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    assert observed["sources"][0]["source_start"] == "2022-01-01"
    assert observed["sources"][0]["source_end"] == "2024-06-30"
    assert observed["sources"][0]["circ_mv_history_start"] == "2020-07-30"
    assert observed["sources"][1]["source_start"] == "2022-01-01"
    assert observed["sources"][1]["source_end"] == "2025-04-30"
    assert observed["sources"][1]["circ_mv_history_start"] == "2020-07-30"
    assert observed["db_prefix"] == "TDX_DB_"
    assert observed["c010_formal"] is True
    assert report["request_candidate"]["source"]["source_start"] == "2022-01-01"
    assert report["request_candidate"]["source"]["source_end"] == "2024-06-30"
    assert report["request_candidate"]["source"]["circ_mv_history_start"] == "2020-07-30"
    assert report["request_candidate"]["semantic_source"] == observed["sources"][1]
    assert report["validation_start"] == "2024-07-01"
    assert report["validation_end"] == "2025-03-31"
    assert request["source"]["source_start"] == "2020-07-30"
    assert "circ_mv_history_start" not in request["source"]


def test_formal_preflight_freezes_missing_approved_validation_window(monkeypatch) -> None:
    request = _request()
    for family in request["families"]:
        family.pop("validation_start")
        family.pop("validation_end")
    inputs = _preflight_inputs()
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_load_l1_source_inputs",
        lambda train_request, *, db_prefix, c010_formal=False: inputs,
    )
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda values, req: _coverage_preflight(policy_sha256=values["feature_domain_policy_sha256"]),
    )

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    assert report["validation_start"] == "2024-07-01"
    assert report["validation_end"] == "2025-03-31"
    assert {
        (family["validation_start"], family["validation_end"]) for family in report["request_candidate"]["families"]
    } == {("2024-07-01", "2025-03-31")}
    assert all("validation_start" not in family for family in request["families"])


def test_formal_preflight_rejects_unapproved_validation_window_before_source_load(monkeypatch) -> None:
    request = _request()
    request["families"][0]["validation_start"] = "2024-07-02"
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    source_load_called = False

    def unexpected_source_load(*args, **kwargs):
        nonlocal source_load_called
        source_load_called = True
        raise AssertionError("source load must not start for an unapproved validation window")

    monkeypatch.setattr(subject, "_load_l1_source_inputs", unexpected_source_load)

    with pytest.raises(StateModelSetError, match="formal B3 validation window mismatch"):
        subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")
    assert source_load_called is False


def test_formal_preflight_rejects_source_that_does_not_cover_semantic_watermark(monkeypatch) -> None:
    request = _request()
    request["source"]["source_end"] = "2025-03-31"
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    source_load_called = False

    def unexpected_source_load(*args, **kwargs):
        nonlocal source_load_called
        source_load_called = True
        raise AssertionError("source load must not expand an immutable source window")

    monkeypatch.setattr(subject, "_load_l1_source_inputs", unexpected_source_load)

    with pytest.raises(StateModelSetError, match="semantic window escapes the immutable source window"):
        subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")
    assert source_load_called is False


def test_preflight_blocks_insufficient_train_coverage_without_request_candidate(monkeypatch) -> None:
    request = _request()
    inputs = _preflight_inputs()
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(
            valid=False,
            policy_sha256=inputs["feature_domain_policy_sha256"],
        ),
    )

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    assert report["status"] == "blocked"
    assert report["request_candidate"] is None
    assert report["request_candidate_sha256"] is None
    assert report["train_coverage_valid"] is False
    assert report["failure_reason_codes"] == ["hmm_risk_model_train_observation_coverage_insufficient"]
    assert report["fit_performed"] is False


def test_c009_preflight_uses_immutable_train_window_and_never_runs_model_stages(monkeypatch) -> None:
    request = _request()
    inputs = _preflight_inputs()
    observed: dict[str, object] = {}
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "e" * 40)

    def load_inputs(train_request, *, db_prefix):
        observed["source"] = dict(train_request["source"])
        observed["db_prefix"] = db_prefix
        return inputs

    monkeypatch.setattr(subject, "_load_l1_source_inputs", load_inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda values, req: _coverage_preflight())

    report = subject.prepare_c009_stock_fact_preflight(request, db_prefix="TDX_DB_")

    assert report["schema_version"] == subject.C009_STOCK_FACT_PREFLIGHT_SCHEMA
    assert report["status"] == "preflight_complete"
    assert observed["source"]["source_start"] == "2022-01-01"
    assert observed["source"]["source_end"] == "2024-06-30"
    assert observed["source"]["circ_mv_history_start"] == "2020-07-30"
    assert observed["db_prefix"] == "TDX_DB_"
    assert report["trading_date_count"] == 601
    assert report["source_statistics"]["moneyflow_provider_absence_count"] == 502
    assert report["source_statistics"]["moneyflow_alias_resolution_count"] == 591
    assert report["source_statistics"]["circ_mv_asof_stale_count"] == 466
    assert report["source_statistics"]["circ_mv_pit_boundary_crossing_count"] == 1_073
    assert report["source_statistics"]["circ_mv_pit_boundary_crossing_available_count"] == 1_073
    assert report["source_statistics"]["circ_mv_pit_boundary_crossing_invalid_count"] == 0
    assert report["provider_absence_authority"]["manifest_sha256"] == "b" * 64
    assert report["approved_source_coverage_contract_applied"] is True
    for field in (
        "fit_performed",
        "selection_performed",
        "d5_performed",
        "d6_performed",
        "formal_model_acceptance_thresholds_applied",
        "hard_semantic_authority_changed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    ):
        assert report[field] is False


def test_c009_preflight_rejects_l1_l2_source_evidence_drift(monkeypatch) -> None:
    inputs = _preflight_inputs()
    inputs["l2_stock_fact_manifest"]["moneyflow_provider_absence_count"] = 501
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "e" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda values, req: _coverage_preflight())

    with pytest.raises(StateModelSetError, match="C-009 L1/L2 source evidence mismatch"):
        subject.prepare_c009_stock_fact_preflight(_request(), db_prefix="TDX_DB_")


def test_c009_preflight_rejects_l1_l2_circ_mv_crossing_evidence_drift(monkeypatch) -> None:
    inputs = _preflight_inputs()
    inputs["l2_stock_fact_manifest"]["circ_mv_pit_boundary_crossing_available_count"] = 1_072
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "e" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda values, req: _coverage_preflight())

    with pytest.raises(StateModelSetError, match="C-009 L1/L2 source evidence mismatch"):
        subject.prepare_c009_stock_fact_preflight(_request(), db_prefix="TDX_DB_")


def test_c010_diagnostic_compares_baseline_and_masks_without_model_actions(monkeypatch) -> None:
    inputs = _preflight_inputs()
    inputs["c010_diagnostic"] = {
        "eligibility": {
            "schema_version": "hmm_risk_c010_train_observation_eligibility_v1",
            "excluded_moneyflow_symbols": ["689009.SH"],
            "pit_universe_changed": False,
            "selection_universe_changed": False,
            "runtime_prediction_eligibility_changed": False,
            "diagnostic_only": True,
        },
        "aggregate_evidence": {
            "impacted_l1_codes": ["801880.SI"],
            "impacted_l2_codes": ["801881.SI"],
            "formal_policy_activated": False,
        },
        "l1_panel": object(),
        "l2_panel": object(),
        "l1_feature_definition": {"cross_section_contract": "coverage_aware_diagnostic"},
        "l2_feature_definition": {"cross_section_contract": "coverage_aware_diagnostic"},
        "l1_cross_section_evidence": {"receipt_sha256": "7" * 64},
        "l2_cross_section_evidence": {"receipt_sha256": "8" * 64},
    }
    observed = {}
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "f" * 40)

    def load_inputs(request, *, db_prefix, c010_diagnostic=False):
        observed["source"] = dict(request["source"])
        observed["db_prefix"] = db_prefix
        observed["diagnostic"] = c010_diagnostic
        return inputs

    monkeypatch.setattr(subject, "_load_l1_source_inputs", load_inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda values, request: _coverage_preflight(valid=False),
    )

    def audit(panel, **kwargs):
        return {
            "schema_version": "hmm_risk_c010_feature_mask_candidate_set_v1",
            "family": kwargs["family"],
            "direct_sector_level": kwargs["direct_sector_level"],
            "feature_mask_candidate_valid": True,
            "fit_performed": False,
            "selection_performed": False,
            "formal_policy_activated": False,
        }

    monkeypatch.setattr(subject, "audit_feature_mask_candidates", audit)

    report = subject.prepare_c010_observation_eligibility_diagnostic(_request(), db_prefix="TDX_DB_")

    assert report["schema_version"] == subject.C010_OBSERVATION_ELIGIBILITY_SCHEMA
    assert report["status"] == "diagnostic_complete"
    assert report["feature_mask_candidate_valid"] is True
    assert report["baseline_train_coverage"]["train_coverage_valid"] is False
    assert report["observation_eligibility"]["excluded_moneyflow_symbols"] == ["689009.SH"]
    assert observed == {
        "source": {
            **_request()["source"],
            "source_start": "2022-01-01",
            "source_end": "2024-06-30",
            "circ_mv_history_start": "2020-07-30",
        },
        "db_prefix": "TDX_DB_",
        "diagnostic": True,
    }
    for field in (
        "pit_universe_changed",
        "selection_universe_changed",
        "runtime_prediction_eligibility_changed",
        "formal_policy_activated",
        "fit_performed",
        "selection_performed",
        "d6_performed",
        "validation_accessed",
        "future_utility_accessed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    ):
        assert report[field] is False


def test_request_template_loader_accepts_unfrozen_template_but_formal_loader_rejects_it(tmp_path) -> None:
    request = _request()
    for field in ("dataset_manifest_hash", "mapping_manifest_hash", "l2_stock_fact_manifest_hash"):
        request.pop(field)
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")

    assert subject._load_request_template(request_path) == request
    with pytest.raises(StateModelSetError, match="dataset_manifest_hash must be a SHA-256 identity"):
        subject._load_request(request_path)


def test_formal_request_rejects_missing_train_coverage_identity() -> None:
    with pytest.raises(StateModelSetError, match="train coverage identity is missing or invalid"):
        subject._require_formal_train_coverage_identity(_request())


def test_main_preflight_writes_immutable_candidate_and_receipt(monkeypatch, tmp_path, capsys) -> None:
    request = _request()
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    candidate_path = tmp_path / "candidate.json"
    report_path = tmp_path / "preflight.json"
    inputs = _preflight_inputs()
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(policy_sha256=inputs["feature_domain_policy_sha256"]),
    )
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(
        subject,
        "parse_args",
        lambda: SimpleNamespace(
            request=str(request_path),
            env_file=str(tmp_path / "unused.env"),
            db_env_prefix="TDX_DB_",
            b3_preflight_output=str(report_path),
            b3_request_candidate_output=str(candidate_path),
        ),
    )

    assert subject.main() == 0

    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    preflight = json.loads(report_path.read_text(encoding="utf-8"))
    cli_receipt = json.loads(capsys.readouterr().out)
    assert subject.canonical_sha256(candidate) == preflight["request_candidate_sha256"]
    assert cli_receipt["report_sha256"] == subject.canonical_sha256(preflight)
    assert cli_receipt["request_candidate_sha256"] == subject.canonical_sha256(candidate)
    assert cli_receipt["fit_performed"] is False
    assert cli_receipt["selection_performed"] is False
    assert cli_receipt["ready_artifact_write_performed"] is False


def test_main_blocked_preflight_does_not_overwrite_stale_candidate(monkeypatch, tmp_path, capsys) -> None:
    request = _request()
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text('{"stale":true}\n', encoding="utf-8")
    report_path = tmp_path / "preflight.json"
    inputs = _preflight_inputs()
    _approve_preflight_template(monkeypatch, request)
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda manifest: dict(manifest))
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(
            valid=False,
            policy_sha256=inputs["feature_domain_policy_sha256"],
        ),
    )
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(
        subject,
        "parse_args",
        lambda: SimpleNamespace(
            request=str(request_path),
            env_file=str(tmp_path / "unused.env"),
            db_env_prefix="TDX_DB_",
            b3_preflight_output=str(report_path),
            b3_request_candidate_output=str(candidate_path),
        ),
    )

    assert subject.main() == 1

    preflight = json.loads(report_path.read_text(encoding="utf-8"))
    cli_receipt = json.loads(capsys.readouterr().out)
    assert candidate_path.read_text(encoding="utf-8") == '{"stale":true}\n'
    assert preflight["status"] == "blocked"
    assert preflight["request_candidate"] is None
    assert cli_receipt["request_candidate_path"] is None
    assert cli_receipt["request_candidate_sha256"] is None


def test_formal_parent_persists_typed_child_failure_receipt(monkeypatch, tmp_path) -> None:
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preparation_output=str(tmp_path / "formal-receipt.json"),
    )
    stderr = json.dumps(
        {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "status": "failed",
            "error_type": "StateModelSetError",
            "error": "801010.SI train-only observation coverage is insufficient: 10",
        }
    ).encode()
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout=b"", stderr=stderr),
    )
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda request: None)
    monkeypatch.setattr(subject, "_require_formal_train_coverage_identity", lambda request: None)
    monkeypatch.setattr(subject, "_require_formal_semantic_identity", lambda request: None)
    monkeypatch.setattr(subject, "_load_verified_formal_semantic_inputs", lambda request, db_prefix: {})

    with pytest.raises(StateModelSetError, match="801010.SI train-only observation coverage is insufficient: 10"):
        subject.run_b3_repeated(args, _request())

    failure_path = tmp_path / "formal-receipt.fresh_process_1.failure.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["error_type"] == "StateModelSetError"
    assert failure["error"] == "801010.SI train-only observation coverage is insufficient: 10"
    assert failure["fit_grid_completed"] is False
    assert failure["selection_performed"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_formal_parent_rejects_missing_validation_window_before_fresh_process(monkeypatch, tmp_path) -> None:
    request = _request()
    for family in request["families"]:
        family.pop("validation_start")
        family.pop("validation_end")
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preparation_output=str(tmp_path / "formal-receipt.json"),
    )
    subprocess_called = False

    def unexpected_subprocess(*args, **kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        raise AssertionError("fresh process must not start without the approved validation window")

    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda value: None)
    monkeypatch.setattr(subject, "_require_formal_train_coverage_identity", lambda value: None)
    monkeypatch.setattr(subject.subprocess, "run", unexpected_subprocess)

    with pytest.raises(StateModelSetError, match="formal B3 validation window is missing"):
        subject.run_b3_repeated(args, request)
    assert subprocess_called is False


def test_formal_parent_rejects_missing_semantic_identity_before_fresh_process(monkeypatch, tmp_path) -> None:
    request = _request()
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preparation_output=str(tmp_path / "formal-receipt.json"),
    )
    subprocess_called = False

    def unexpected_subprocess(*args, **kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        raise AssertionError("fresh process must not start without frozen semantic identity")

    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda value: None)
    monkeypatch.setattr(subject, "_require_formal_train_coverage_identity", lambda value: None)
    monkeypatch.setattr(subject.subprocess, "run", unexpected_subprocess)

    with pytest.raises(StateModelSetError, match="formal B3 semantic source identity is missing"):
        subject.run_b3_repeated(args, request)
    assert subprocess_called is False


def test_formal_semantic_identity_rejects_source_window_drift() -> None:
    request = _request()
    request["semantic_source"] = subject._b3_semantic_source_request(request)["source"]
    request.update(
        {
            "semantic_dataset_manifest_hash": "1" * 64,
            "semantic_mapping_manifest_hash": "2" * 64,
            "semantic_calendar_manifest_hash": "3" * 64,
            "semantic_l2_stock_fact_manifest_hash": "4" * 64,
        }
    )
    subject._require_formal_semantic_identity(request)

    request["semantic_source"]["source_end"] = "2025-03-31"
    with pytest.raises(StateModelSetError, match="formal B3 semantic source identity mismatch"):
        subject._require_formal_semantic_identity(request)


def test_formal_semantic_identity_accepts_train_only_candidate_source() -> None:
    request = _request()
    request["semantic_source"] = subject._b3_semantic_source_request(request)["source"]
    request["source"]["source_start"] = "2022-01-01"
    request["source"]["source_end"] = "2024-06-30"
    request["source"]["circ_mv_history_start"] = "2020-07-30"
    request.update(
        {
            "semantic_dataset_manifest_hash": "1" * 64,
            "semantic_mapping_manifest_hash": "2" * 64,
            "semantic_calendar_manifest_hash": "3" * 64,
            "semantic_l2_stock_fact_manifest_hash": "4" * 64,
        }
    )

    subject._require_formal_semantic_identity(request)


def test_formal_parent_rejects_semantic_hash_drift_before_fresh_process(monkeypatch, tmp_path) -> None:
    request = _request()
    request["semantic_source"] = subject._b3_semantic_source_request(request)["source"]
    request.update(
        {
            "semantic_dataset_manifest_hash": "1" * 64,
            "semantic_mapping_manifest_hash": "2" * 64,
            "semantic_calendar_manifest_hash": "3" * 64,
            "semantic_l2_stock_fact_manifest_hash": "4" * 64,
        }
    )
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preparation_output=str(tmp_path / "formal-receipt.json"),
    )
    subprocess_called = False

    def unexpected_subprocess(*args, **kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        raise AssertionError("fresh process must not start after semantic hash drift")

    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda value: None)
    monkeypatch.setattr(subject, "_require_formal_train_coverage_identity", lambda value: None)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda *args, **kwargs: _preflight_inputs())
    monkeypatch.setattr(subject.subprocess, "run", unexpected_subprocess)

    with pytest.raises(StateModelSetError, match="semantic input drifted"):
        subject.run_b3_repeated(args, request)
    assert subprocess_called is False


def test_child_failure_receipt_bounds_untrusted_error_text(monkeypatch, tmp_path) -> None:
    args = SimpleNamespace(b3_preparation_output=str(tmp_path / "formal-receipt.json"))
    stderr = json.dumps(
        {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "error_type": "E" * 500,
            "error": "x" * 5000,
        }
    ).encode()

    _, failure = subject._persist_b3_child_failure(
        args,
        process_identity="fresh_process_1",
        returncode=1,
        stdout=b"",
        stderr=stderr,
    )

    assert len(failure["error_type"]) == 256
    assert len(failure["error"]) == 4000


def test_formal_single_pass_runs_both_families_and_levels_without_selection_or_validation(monkeypatch) -> None:
    request = _request()
    coverage = _coverage_preflight(policy_sha256="f" * 64)
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": coverage["receipt_sha256"],
            "feature_domain_policy_manifest": {"receipt_sha256": "f" * 64},
            "feature_domain_policy_sha256": "f" * 64,
        }
    )
    inputs = {
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda request: None)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_c010_policy_manifest",
        lambda values, request, producer_commit: request["feature_domain_policy_manifest"],
    )
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda inputs, request: coverage)
    monkeypatch.setattr(
        subject,
        "_direct_train_series_for_family",
        lambda inputs, family: {"L1": {"L1": object()}, "L2": {"L2": object()}},
    )
    calls = []

    def fake_repeat(series, *, family, level, feature_names, preprocess_family, process_identity):
        calls.append((family, level, process_identity, len(feature_names), preprocess_family))
        return ({"family": family, "level": level, "schedule": list(range(42, 50))}, {})

    monkeypatch.setattr(subject, "run_level_repeat", fake_repeat)
    receipt = subject.prepare_b3_single_pass(
        request,
        db_prefix="TDX_DB_DEV_",
        process_identity="fresh_process_1",
    )

    assert len(calls) == 4
    assert set(receipt["level_repeats"]) == {
        "legacy_covfix:L1",
        "legacy_covfix:L2",
        "autocycle_all_core:L1",
        "autocycle_all_core:L2",
    }
    assert receipt["selection_performed"] is False
    assert receipt["validation_accessed_for_selection"] is False
    assert receipt["future_utility_accessed_for_selection"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_formal_single_pass_rejects_frozen_manifest_drift(monkeypatch) -> None:
    request = _request()
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": "a" * 64,
            "feature_domain_policy_manifest": {"receipt_sha256": "f" * 64},
            "feature_domain_policy_sha256": "f" * 64,
        }
    )
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda request: None)
    monkeypatch.setattr(
        subject,
        "_load_l1_source_inputs",
        lambda request, db_prefix, c010_formal=False: {
            "dataset_manifest": {"schema_version": "drifted"},
            "mapping_manifest": {"schema_version": "mapping_v1"},
            "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
        },
    )
    with pytest.raises(StateModelSetError, match="dataset manifest hash mismatch"):
        subject.prepare_b3_single_pass(
            request,
            db_prefix="TDX_DB_DEV_",
            process_identity="fresh_process_1",
        )


def test_formal_single_pass_rejects_stale_train_coverage_receipt_before_fit(monkeypatch) -> None:
    request = _request()
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": "a" * 64,
            "feature_domain_policy_manifest": {"receipt_sha256": "f" * 64},
            "feature_domain_policy_sha256": "f" * 64,
        }
    )
    inputs = {
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda request: None)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(
        subject,
        "_c010_policy_manifest",
        lambda values, request, producer_commit: request["feature_domain_policy_manifest"],
    )
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(policy_sha256="f" * 64),
    )
    fit_called = False

    def unexpected_fit(*args, **kwargs):
        nonlocal fit_called
        fit_called = True
        raise AssertionError("fit must not start for a stale coverage receipt")

    monkeypatch.setattr(subject, "run_level_repeat", unexpected_fit)

    with pytest.raises(StateModelSetError, match="train coverage receipt hash mismatch"):
        subject.prepare_b3_single_pass(
            request,
            db_prefix="TDX_DB_DEV_",
            process_identity="fresh_process_1",
        )
    assert fit_called is False
