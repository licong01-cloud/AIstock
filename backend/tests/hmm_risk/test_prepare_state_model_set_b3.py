from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.hmm_risk.provider_absence import (
    MONEYFLOW_DATASET,
    MONEYFLOW_MISSING_FIELDS,
    ProviderAbsenceEvidence,
)
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


def _minimal_c010_policy() -> dict:
    return {
        "receipt_sha256": "f" * 64,
        "provider_absence_partition_receipt": {"receipt_sha256": "e" * 64},
        "provider_absence_partition_receipt_sha256": "e" * 64,
    }


class _C010Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


class _C010Connection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return _C010Cursor(self.rows)


class _C010Resolution:
    def __init__(self, symbol: str, dataset: str):
        self.security_identity_id = f"canonical:{symbol}"
        self.source_ts_code = symbol
        self._symbol = symbol
        self._dataset = dataset

    def evidence(self):
        return {
            "security_identity_id": self.security_identity_id,
            "canonical_ts_code": self._symbol,
            "source_dataset": self._dataset,
            "source_ts_code": self.source_ts_code,
            "resolution_kind": "canonical_same_code",
        }


class _C010SecurityManifest:
    manifest_sha256 = "a" * 64

    def alias_rows(self, source_dataset):
        return []

    def resolve(self, symbol, trade_date, source_dataset):
        return _C010Resolution(symbol, source_dataset)

    def evidence(self):
        return {
            "schema_version": "hmm_risk_security_source_identity_manifest_v1",
            "manifest_sha256": self.manifest_sha256,
        }


def _c010_absence(symbol: str, trade_date_value: date) -> ProviderAbsenceEvidence:
    body = {
        "canonical_ts_code": symbol,
        "source_dataset": MONEYFLOW_DATASET,
        "source_ts_code": symbol,
        "trade_date": trade_date_value.isoformat(),
        "missing_fields": list(MONEYFLOW_MISSING_FIELDS),
        "provider_audit_receipt_sha256": "b" * 64,
    }
    return ProviderAbsenceEvidence(
        canonical_ts_code=symbol,
        source_dataset=MONEYFLOW_DATASET,
        source_ts_code=symbol,
        trade_date=trade_date_value,
        missing_fields=MONEYFLOW_MISSING_FIELDS,
        provider_audit_receipt_sha256="b" * 64,
        row_hash=subject.canonical_sha256(body),
    )


def test_c010_partition_keeps_known_sw_domain_out_key_without_fabricating_sector_identity() -> None:
    trade_date_value = date(2023, 5, 22)
    absence = _c010_absence("002951.SZ", trade_date_value)
    provider_manifest = SimpleNamespace(
        rows=(absence,),
        evidence=lambda: {"schema_version": "hmm_risk_provider_absence_manifest_v1", "manifest_sha256": "b" * 64},
    )
    source_spec = SimpleNamespace(universe_key="frozen", universe_rule_version="rule-v1")
    source_state = {"column_contract_sha256": "c" * 64, "source_state": "ready"}
    rows = [
        (
            "002951.SZ",
            trade_date_value,
            [{"ts_code": "002951.SZ", "eligible_start": "2022-01-01", "eligible_end": None}],
            [{"ts_code": "002951.SZ", "trade_date": "2023-05-22", "close_li": 12345}],
            [],
        )
    ]

    partition, _ = subject._c010_provider_absence_partition(
        _C010Connection(rows),
        source_spec,
        security_identity_manifest=_C010SecurityManifest(),
        provider_absence_manifest=provider_manifest,
        source_state=source_state,
        mapping_manifest={"schema_version": "hmm_risk_pit_mapping_manifest_v1", "source_jsonl_sha256": "d" * 64},
        train_start=date(2022, 1, 1),
        train_end=date(2024, 6, 30),
        formal_policy=True,
    )

    assert partition["p_all_entry_count"] == 1
    assert partition["p_in_entry_count"] == 0
    assert partition["p_out_entry_count"] == 1
    assert partition["entries"][0]["failed_predicates"] == [
        "sw_l1_identity_valid",
        "sw_l2_identity_valid",
    ]
    assert partition["entries"][0]["primary_reason_code"] == ("hmm_risk_c010_sw_identity_unavailable_for_opportunity")


def test_c010_expected_opportunity_receipt_requires_unique_direct_l1_l2_mapping() -> None:
    authority = subject.canonical_authority_identity("test", {"version": "v1"})
    source_spec = SimpleNamespace(universe_key="frozen")
    valid_mapping = {
        "source_ts_code": "000001.SZ",
        "source_l1_code": "801010",
        "source_l2_code": "801011",
        "in_date": "2020-01-01",
        "out_date": None,
        "l1_code": "801010.SI",
        "l2_code": "801011.SI",
    }
    receipt = subject._c010_expected_opportunity_receipt(
        _C010Connection([("000001.SZ", date(2022, 1, 4), [valid_mapping])]),
        source_spec,
        security_identity_manifest=_C010SecurityManifest(),
        train_start=date(2022, 1, 1),
        train_end=date(2024, 6, 30),
        authority_identities=[authority],
    )
    assert receipt["opportunity_key_count"] == 1
    assert receipt["entries"][0]["opportunity_dates"] == ["2022-01-04"]

    with pytest.raises(StateModelSetError, match="opportunity mapping is not unique"):
        subject._c010_expected_opportunity_receipt(
            _C010Connection(
                [
                    (
                        "000001.SZ",
                        date(2022, 1, 4),
                        [valid_mapping, {**valid_mapping, "l2_code": "801012.SI"}],
                    )
                ]
            ),
            source_spec,
            security_identity_manifest=_C010SecurityManifest(),
            train_start=date(2022, 1, 1),
            train_end=date(2024, 6, 30),
            authority_identities=[authority],
        )


def test_main_c010_a5_preflight_writes_compact_readonly_receipt(monkeypatch, tmp_path, capsys) -> None:
    output = tmp_path / "c010-a5-preflight.json"
    partition = {
        "p_all_entry_count": 502,
        "p_in_entry_count": 501,
        "p_out_entry_count": 1,
    }
    report = {
        "status": "preflight_complete",
        "provider_absence_partition_receipt": partition,
        "provider_absence_partition_receipt_sha256": "a" * 64,
        "known_sw_domain_out_verified": True,
    }
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request_template", lambda path: _request())
    monkeypatch.setattr(subject, "prepare_c010_a5_domain_partition_preflight", lambda request, db_prefix: report)
    monkeypatch.setattr(subject, "_write_diagnostic_report", lambda path, value: "b" * 64)
    monkeypatch.setattr(
        subject,
        "parse_args",
        lambda: SimpleNamespace(
            request=str(tmp_path / "request.json"),
            env_file=str(tmp_path / "env"),
            db_env_prefix="TDX_DB_",
            b3_preflight_output=None,
            c009_stock_fact_preflight_output=None,
            c010_observation_eligibility_output=None,
            c010_a5_domain_partition_output=str(output),
            b3_request_candidate_output=None,
        ),
    )

    assert subject.main() == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "preflight_complete"
    assert receipt["p_all_entry_count"] == 502
    assert receipt["p_out_entry_count"] == 1
    assert receipt["known_sw_domain_out_verified"] is True
    assert receipt["fit_performed"] is False
    assert receipt["database_write_performed"] is False


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


def _blocker_pass_payload() -> bytes:
    numeric_environment = {"packages": {"hmmlearn": "0.3.3"}, "thread_env": {"OMP_NUM_THREADS": "1"}}
    body = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_pass_v1",
        "diagnostic_producer_commit": "d" * 40,
        "target_manifest_sha256": "1" * 64,
        "targeted_evidence": [{"diagnostic_entry_sha256": "2" * 64}],
        "fit_count": 174,
        "numeric_environment": numeric_environment,
        "numeric_environment_sha256": subject.canonical_sha256(numeric_environment),
    }
    value = {**body, "pass_receipt_sha256": subject.canonical_sha256(body)}
    return subject.canonical_json_bytes(value)


def _blocker_args(tmp_path) -> SimpleNamespace:
    formal = tmp_path / "formal.json"
    formal.write_text("{}", encoding="utf-8")
    return SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_formal_report=str(formal),
    )


def _remediation_args(tmp_path) -> SimpleNamespace:
    formal = tmp_path / "formal.json"
    blocker = tmp_path / "blocker.json"
    formal.write_text("{}", encoding="utf-8")
    blocker.write_text("{}", encoding="utf-8")
    return SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preflight_output=None,
        b3_request_candidate_output=None,
        b3_blocker_diagnostic_output=None,
        b3_remediation_diag02_output=str(tmp_path / "remediation.json"),
        b3_formal_report=str(formal),
        b3_blocker_report=str(blocker),
        b3_target_manifest_sha256="",
        _b3_blocker_diag01_child=False,
        _c008_b3_diag02_child=False,
        _c008_b3_diag04_child=False,
        _b3_child=False,
        b3_preparation_output=None,
        c008_b3_diag04_output=None,
        c008_b3_diag02_output=None,
        c008_diagnostic_output=None,
        c008_b1_diagnostic_output=None,
    )


def test_main_remediation_diag02_persists_no_fit_receipt(monkeypatch, tmp_path, capsys) -> None:
    args = _remediation_args(tmp_path)
    body = {
        "schema_version": "hmm_risk_c008_b3_remediation_diag02_v1",
        "status": "diagnostic_complete",
        "diagnostic_contract": "C-008-B3-REMEDIATION-DIAG-02",
        "profile_manifest": {"profile_count": 324},
        "completed_entry_analysis": {"entry_count": 163},
        "initialization_source_evidence": {"entry_count": 11},
        "hmm_refit_performed": False,
        "selection_performed": False,
        "validation_accessed": False,
        "formal_acceptance_reexecuted": False,
        "threshold_changed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    report = {**body, "receipt_sha256": subject.canonical_sha256(body)}
    monkeypatch.setattr(subject, "parse_args", lambda: args)
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "prepare_b3_remediation_diag02", lambda *a, **k: report)

    assert subject.main() == 0

    persisted = json.loads((tmp_path / "remediation.json").read_text(encoding="utf-8"))
    receipt = json.loads(capsys.readouterr().out)
    assert persisted == report
    assert receipt["profile_count"] == 324
    assert receipt["completed_entry_count"] == 163
    assert receipt["initialization_failure_count"] == 11
    assert receipt["hmm_refit_performed"] is False
    assert receipt["validation_accessed"] is False
    assert receipt["ready_artifact_write_performed"] is False


def _blocker_numeric_environment(*, thread_pools=None) -> dict:
    return {
        "schema_version": "hmm_risk_c008_b3_diag04_numeric_environment_v1",
        "scope": "same_host_same_fixed_numeric_environment_only",
        "python_version": "3.13.5",
        "python_implementation": "CPython",
        "python_executable": "C:/Miniconda/envs/AIstock/python.exe",
        "packages": {
            "numpy": "2.4.0",
            "scipy": "1.16.3",
            "scikit-learn": "1.8.0",
            "hmmlearn": "0.3.3",
            "threadpoolctl": "3.6.0",
        },
        "thread_env": {
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "VECLIB_MAXIMUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        },
        "thread_pools": [
            {
                "user_api": "blas",
                "internal_api": "openblas",
                "num_threads": 1,
                "prefix": "libscipy_openblas",
            }
        ]
        if thread_pools is None
        else thread_pools,
    }


def _valid_blocker_child_value(numeric_environment: dict) -> tuple[dict, dict]:
    target_entry = {
        "role": "control",
        "family": "legacy_covfix",
        "level": "L2",
        "seed": 42,
        "sector_code": "L2-001",
        "source_entry_receipt_sha256": "7" * 64,
        "formal_failed_stages": [],
    }
    target = {
        "formal_report_sha256": subject.B3_BLOCKER_FORMAL_AUTHORITY["report_sha256"],
        "target_manifest_sha256": "1" * 64,
        "parameter_profile_sha256": "2" * 64,
        "target_pair_count": 1,
        "targets": [target_entry],
    }
    evidence_body = {
        **target_entry,
        "status": "fit_completed",
        "formal_entry_receipt_reproduced": True,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
    }
    value = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_pass_v1",
        "diagnostic_producer_commit": "d" * 40,
        "formal_producer_commit": subject.B3_BLOCKER_FORMAL_AUTHORITY["producer_commit"],
        "formal_report_sha256": target["formal_report_sha256"],
        "target_manifest_sha256": target["target_manifest_sha256"],
        "dataset_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["dataset_manifest_hash"],
        "mapping_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["mapping_manifest_hash"],
        "calendar_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["calendar_manifest_hash"],
        "l2_stock_fact_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["l2_stock_fact_manifest_hash"],
        "feature_domain_policy_sha256": subject.B3_BLOCKER_FORMAL_AUTHORITY["feature_domain_policy_sha256"],
        "formula_version": subject.B3_BLOCKER_FORMAL_AUTHORITY["formula_version"],
        "parameter_profile_sha256": target["parameter_profile_sha256"],
        "numeric_environment": numeric_environment,
        "numeric_environment_sha256": subject.canonical_sha256(numeric_environment),
        "fit_count": 1,
        "targeted_evidence": [{**evidence_body, "diagnostic_entry_sha256": subject.canonical_sha256(evidence_body)}],
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "acceptance_decision_reexecuted": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return value, target


def test_blocker_child_pass_validator_rejects_incomplete_success(monkeypatch) -> None:
    target_entry = {
        "role": "control",
        "family": "legacy_covfix",
        "level": "L2",
        "seed": 42,
        "sector_code": "L2-001",
        "source_entry_receipt_sha256": "7" * 64,
        "formal_failed_stages": [],
    }
    target = {
        "formal_report_sha256": subject.B3_BLOCKER_FORMAL_AUTHORITY["report_sha256"],
        "target_manifest_sha256": "1" * 64,
        "parameter_profile_sha256": "2" * 64,
        "target_pair_count": 1,
        "targets": [target_entry],
    }
    evidence_body = {
        **target_entry,
        "status": "fit_completed",
        "formal_entry_receipt_reproduced": True,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
    }
    numeric_environment = _blocker_numeric_environment()
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: deepcopy(numeric_environment))
    value = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_pass_v1",
        "diagnostic_producer_commit": "d" * 40,
        "formal_producer_commit": subject.B3_BLOCKER_FORMAL_AUTHORITY["producer_commit"],
        "formal_report_sha256": target["formal_report_sha256"],
        "target_manifest_sha256": target["target_manifest_sha256"],
        "dataset_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["dataset_manifest_hash"],
        "mapping_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["mapping_manifest_hash"],
        "calendar_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["calendar_manifest_hash"],
        "l2_stock_fact_manifest_hash": subject.B3_BLOCKER_FORMAL_AUTHORITY["l2_stock_fact_manifest_hash"],
        "feature_domain_policy_sha256": subject.B3_BLOCKER_FORMAL_AUTHORITY["feature_domain_policy_sha256"],
        "formula_version": subject.B3_BLOCKER_FORMAL_AUTHORITY["formula_version"],
        "parameter_profile_sha256": target["parameter_profile_sha256"],
        "numeric_environment": numeric_environment,
        "numeric_environment_sha256": subject.canonical_sha256(numeric_environment),
        "fit_count": 1,
        "targeted_evidence": [{**evidence_body, "diagnostic_entry_sha256": subject.canonical_sha256(evidence_body)}],
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "acceptance_decision_reexecuted": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    subject._validate_b3_blocker_pass(value, target)

    incomplete = deepcopy(value)
    incomplete["targeted_evidence"] = []
    with pytest.raises(StateModelSetError, match="evidence count is invalid"):
        subject._validate_b3_blocker_pass(incomplete, target)


def test_blocker_child_accepts_additional_post_fit_single_thread_pool(monkeypatch) -> None:
    parent_environment = _blocker_numeric_environment()
    child_environment = _blocker_numeric_environment(
        thread_pools=[
            *parent_environment["thread_pools"],
            {
                "user_api": "openmp",
                "internal_api": "openmp",
                "num_threads": 1,
                "prefix": "libomp",
            },
        ]
    )
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: parent_environment)
    value, target = _valid_blocker_child_value(child_environment)

    subject._validate_b3_blocker_pass(value, target)


def test_blocker_child_rejects_non_single_thread_post_fit_pool(monkeypatch) -> None:
    parent_environment = _blocker_numeric_environment()
    child_environment = _blocker_numeric_environment(
        thread_pools=[{"user_api": "openmp", "internal_api": "openmp", "num_threads": 2}]
    )
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: parent_environment)
    value, target = _valid_blocker_child_value(child_environment)

    with pytest.raises(StateModelSetError, match="thread pools are not single-threaded"):
        subject._validate_b3_blocker_pass(value, target)


def test_d1_numeric_environment_accepts_different_single_thread_pool_inventory(monkeypatch) -> None:
    current_environment = _blocker_numeric_environment(
        thread_pools=[
            {
                "user_api": "blas",
                "internal_api": "openblas",
                "num_threads": 1,
                "prefix": "libscipy_openblas",
            }
        ]
    )
    frozen_environment = _blocker_numeric_environment(
        thread_pools=[
            *current_environment["thread_pools"],
            {
                "user_api": "openmp",
                "internal_api": "openmp",
                "num_threads": 1,
                "prefix": "libomp",
            },
        ]
    )
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: current_environment)

    subject._validate_b3_d1_numeric_environment_authority(
        current_environment,
        frozen_environment,
    )


def test_d1_numeric_environment_rejects_frozen_stable_identity_drift(monkeypatch) -> None:
    current_environment = _blocker_numeric_environment()
    frozen_environment = deepcopy(current_environment)
    frozen_environment["packages"]["hmmlearn"] = "0.3.4"
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: current_environment)

    with pytest.raises(subject.D1InactiveDimensionError, match="numeric environment identity is invalid") as captured:
        subject._validate_b3_d1_numeric_environment_authority(
            current_environment,
            frozen_environment,
        )
    assert captured.value.reason_code == "hmm_risk_model_inactive_dimension_authority_mismatch"


def test_d1_numeric_environment_rejects_non_single_thread_frozen_pool(monkeypatch) -> None:
    current_environment = _blocker_numeric_environment()
    frozen_environment = _blocker_numeric_environment(
        thread_pools=[
            {
                "user_api": "openmp",
                "internal_api": "openmp",
                "num_threads": 2,
                "prefix": "libomp",
            }
        ]
    )
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: current_environment)

    with pytest.raises(subject.D1InactiveDimensionError, match="thread pools are not single-threaded") as captured:
        subject._validate_b3_d1_numeric_environment_authority(
            current_environment,
            frozen_environment,
        )
    assert captured.value.reason_code == "hmm_risk_model_inactive_dimension_authority_mismatch"


def test_blocker_child_rejects_numeric_environment_hash_mismatch(monkeypatch) -> None:
    numeric_environment = _blocker_numeric_environment()
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: numeric_environment)
    value, target = _valid_blocker_child_value(numeric_environment)
    value["numeric_environment_sha256"] = "0" * 64

    with pytest.raises(StateModelSetError, match="numeric environment identity is invalid"):
        subject._validate_b3_blocker_pass(value, target)


@pytest.mark.parametrize("field", ["python_executable", "packages", "thread_env"])
def test_blocker_child_rejects_stable_numeric_environment_drift(monkeypatch, field) -> None:
    parent_environment = _blocker_numeric_environment()
    child_environment = deepcopy(parent_environment)
    if field == "python_executable":
        child_environment[field] = "C:/other/python.exe"
    else:
        child_environment[field] = {**child_environment[field], next(iter(child_environment[field])): "drifted"}
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: parent_environment)
    value, target = _valid_blocker_child_value(child_environment)

    with pytest.raises(StateModelSetError, match="numeric environment identity is invalid"):
        subject._validate_b3_blocker_pass(value, target)


def test_blocker_parent_rejects_non_bitwise_fresh_process_payloads(monkeypatch, tmp_path) -> None:
    args = _blocker_args(tmp_path)
    target = {"target_manifest_sha256": "1" * 64}
    first = _blocker_pass_payload()
    second_value = json.loads(first)
    second_body = {key: value for key, value in second_value.items() if key != "pass_receipt_sha256"}
    second_body["diagnostic_producer_commit"] = "e" * 40
    second = subject.canonical_json_bytes({**second_body, "pass_receipt_sha256": subject.canonical_sha256(second_body)})
    payloads = iter((first, second))
    monkeypatch.setattr(subject, "derive_b3_blocker_target_manifest", lambda report: target)
    monkeypatch.setattr(subject, "_validate_b3_blocker_pass", lambda value, manifest: None)
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=next(payloads), stderr=b""),
    )

    with pytest.raises(StateModelSetError, match="canonical payloads differ"):
        subject.run_b3_blocker_diag01_repeated(args, _request())


def test_blocker_parent_keeps_d4_and_d6_replay_read_only(monkeypatch, tmp_path) -> None:
    args = _blocker_args(tmp_path)
    target = {
        "target_manifest_sha256": "1" * 64,
        "target_pair_count": 174,
        "fits_per_process": 174,
        "total_fit_budget": 348,
        "fresh_process_count": 2,
        "formal_report_sha256": subject.B3_BLOCKER_FORMAL_AUTHORITY["report_sha256"],
    }
    payload = _blocker_pass_payload()
    monkeypatch.setattr(subject, "derive_b3_blocker_target_manifest", lambda report: target)
    monkeypatch.setattr(subject, "_validate_b3_blocker_pass", lambda value, manifest: None)
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=payload, stderr=b""),
    )
    monkeypatch.setattr(subject, "_load_verified_formal_semantic_inputs", lambda request, db_prefix: {})
    monkeypatch.setattr(
        subject,
        "_semantic_input_identities",
        lambda inputs: {
            field: subject.B3_BLOCKER_FORMAL_AUTHORITY[field]
            for field in (
                "semantic_dataset_manifest_hash",
                "semantic_mapping_manifest_hash",
                "semantic_calendar_manifest_hash",
                "semantic_l2_stock_fact_manifest_hash",
            )
        },
    )
    monkeypatch.setattr(subject, "_direct_series_for_family", lambda inputs, family: {"L1": {}})
    monkeypatch.setattr(
        subject,
        "replay_b3_blocker_selected_d6",
        lambda report, series, manifest: [{"d6_replay_sha256": str(index) * 64} for index in (3, 4, 5)],
    )
    monkeypatch.setattr(
        subject,
        "build_b3_blocker_matched_comparisons",
        lambda evidence: {"receipt_sha256": "6" * 64},
    )

    report = subject.run_b3_blocker_diag01_repeated(args, _request())

    assert report["status"] == "diagnostic_complete"
    assert report["observed_total_fit_count"] == 348
    assert report["canonical_payload_bitwise_equal"] is True
    assert report["d6_replay_count"] == 3
    assert report["selection_performed"] is False
    assert report["acceptance_decision_reexecuted"] is False
    assert report["model_write_performed"] is False
    assert report["ready_artifact_write_performed"] is False


def test_formal_single_pass_runs_both_families_and_levels_without_selection_or_validation(monkeypatch) -> None:
    request = _request()
    coverage = _coverage_preflight(policy_sha256="f" * 64)
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": coverage["receipt_sha256"],
            "feature_domain_policy_manifest": _minimal_c010_policy(),
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
            "feature_domain_policy_manifest": _minimal_c010_policy(),
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
            "feature_domain_policy_manifest": _minimal_c010_policy(),
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


def _p6_series() -> dict[str, object]:
    return {f"L2-{index:03d}": object() for index in range(subject.B3_P6_EXPECTED_SECTOR_COUNT)}


def _p6_accepted_selection(*, policy_sha256: str, codes: tuple[str, ...] | None = None) -> dict:
    canonical_codes = tuple(sorted(_p6_series())) if codes is None else codes
    evidence = {
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "feature_domain_policy_sha256": policy_sha256,
        "canonical_sector_codes": list(canonical_codes),
        "canonical_sector_set_sha256": subject.canonical_sha256(list(canonical_codes)),
        "schedule": list(subject.RESTART_SCHEDULE),
        "feature_count": len(subject.ALL_CORE_FEATURES),
        "repeat_entries_sha256": "4" * 64,
        "candidates": [],
        "lexicographic_filters": [],
        "selected_seed": 43,
        "selected_schedule_index": subject.RESTART_SCHEDULE.index(43),
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_followed_by_refit": False,
    }
    body = {
        "contract_version": subject.D5_SELECTION_VERSION,
        "failure_reason_codes": [],
        "blocking_reason_codes": [],
        "warning_reason_codes": [],
        "primary_reason_code": None,
        "evidence": evidence,
        "level_selection_status": "accepted",
        "level_selection_valid": True,
    }
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


def _p6_repeat(process_identity: str) -> dict:
    codes = tuple(sorted(_p6_series()))
    return {
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "process_identity": process_identity,
        "schedule": list(subject.RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "canonical_sector_set_sha256": subject.canonical_sha256(list(codes)),
        "feature_names": list(subject.ALL_CORE_FEATURES),
        "entries": [{"seed": seed, "sector_code": code} for seed in subject.RESTART_SCHEDULE for code in codes],
    }


def _p6_child_payload(process_identity: str, *, inputs: dict, policy: dict) -> bytes:
    repeat = _p6_repeat(process_identity)
    entry_count = subject.B3_P6_EXPECTED_SECTOR_COUNT * len(subject.RESTART_SCHEDULE)
    body = {
        "schema_version": subject.B3_P6_SINGLE_PASS_SCHEMA,
        "producer_commit": "c" * 40,
        "process_identity": process_identity,
        "target_family": subject.B3_P6_FAMILY,
        "target_level": subject.B3_P6_LEVEL,
        "feature_names": list(subject.ALL_CORE_FEATURES),
        "preprocess_family": "winsor_zscore_1_99_train_global_v1",
        "planned_fit_count": entry_count,
        "terminal_entry_count": entry_count,
        "dataset_manifest_hash": subject.canonical_sha256(inputs["dataset_manifest"]),
        "mapping_manifest_hash": subject.canonical_sha256(inputs["mapping_manifest"]),
        "calendar_manifest_hash": subject.canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
        "l2_stock_fact_manifest_hash": subject.canonical_sha256(inputs["l2_stock_fact_manifest"]),
        "feature_domain_policy_sha256": policy["receipt_sha256"],
        "feature_domain_policy_manifest": policy,
        "provider_absence_partition_receipt": policy["provider_absence_partition_receipt"],
        "provider_absence_partition_receipt_sha256": policy["provider_absence_partition_receipt_sha256"],
        "formula_version": subject.C010_FORMULA_VERSION,
        "level_repeat": repeat,
        "selection_performed": False,
        "validation_accessed_for_selection": False,
        "future_utility_accessed_for_selection": False,
        "artifact_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return subject.canonical_json_bytes({**body, "single_pass_receipt_sha256": subject.canonical_sha256(body)})


def _p6_expected_closure(inputs: dict, policy: dict) -> dict:
    family = next(item for item in _request()["families"] if item["family"] == subject.B3_P6_FAMILY)
    codes = tuple(sorted(_p6_series()))
    return {
        "family": family,
        "feature_names": tuple(family["feature_names"]),
        "preprocess_family": family["preprocess_family"],
        "canonical_sector_codes": codes,
        "canonical_sector_set_sha256": subject.canonical_sha256(list(codes)),
        "feature_count": len(family["feature_names"]),
        "entry_count": len(codes) * len(subject.RESTART_SCHEDULE),
        "authority_keys": {
            "producer_commit": "c" * 40,
            "dataset_manifest_hash": subject.canonical_sha256(inputs["dataset_manifest"]),
            "mapping_manifest_hash": subject.canonical_sha256(inputs["mapping_manifest"]),
            "calendar_manifest_hash": subject.canonical_sha256(inputs["dataset_manifest"]["calendar_benchmark"]),
            "l2_stock_fact_manifest_hash": subject.canonical_sha256(inputs["l2_stock_fact_manifest"]),
            "feature_domain_policy_sha256": policy["receipt_sha256"],
            "feature_domain_policy_manifest": policy,
            "provider_absence_partition_receipt": policy["provider_absence_partition_receipt"],
            "provider_absence_partition_receipt_sha256": policy["provider_absence_partition_receipt_sha256"],
            "formula_version": subject.C010_FORMULA_VERSION,
        },
    }


def _p6_parent_setup(monkeypatch, tmp_path):
    request = _request()
    policy = _minimal_c010_policy()
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": "a" * 64,
            "feature_domain_policy_manifest": policy,
            "feature_domain_policy_sha256": policy["receipt_sha256"],
            "semantic_dataset_manifest_hash": "1" * 64,
            "semantic_mapping_manifest_hash": "2" * 64,
            "semantic_calendar_manifest_hash": "3" * 64,
            "semantic_l2_stock_fact_manifest_hash": "4" * 64,
            "semantic_source": {
                "dataset_manifest_hash": "1" * 64,
                "mapping_manifest_hash": "2" * 64,
                "calendar_manifest_hash": "3" * 64,
                "l2_stock_fact_manifest_hash": "4" * 64,
            },
        }
    )
    inputs = {
        "dataset_manifest": {"schema_version": "dataset_v1", "calendar_benchmark": {"schema_version": "calendar_v1"}},
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    semantic_identities = {
        "semantic_dataset_manifest_hash": "1" * 64,
        "semantic_mapping_manifest_hash": "2" * 64,
        "semantic_calendar_manifest_hash": "3" * 64,
        "semantic_l2_stock_fact_manifest_hash": "4" * 64,
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_git_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_approved_b3_windows", lambda request: None)
    monkeypatch.setattr(subject, "_require_formal_semantic_identity", lambda request: None)
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda request: None)
    monkeypatch.setattr(subject, "_require_formal_train_coverage_identity", lambda request: None)
    monkeypatch.setattr(subject, "_load_verified_formal_semantic_inputs", lambda request, db_prefix: {})
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix, c010_formal=False: inputs)
    monkeypatch.setattr(subject, "_c010_policy_manifest", lambda values, request, producer_commit: policy)
    monkeypatch.setattr(subject, "_semantic_input_identities", lambda values: semantic_identities)
    monkeypatch.setattr(
        subject,
        "_b3_p6_closure_from_inputs",
        lambda inputs, request, *, policy: _p6_expected_closure(inputs, policy),
    )
    monkeypatch.setattr(subject, "_direct_l2_series_for_family", lambda values, family: _p6_series())
    monkeypatch.setattr(
        subject,
        "_direct_series_for_family",
        lambda *args, **kwargs: pytest.fail("P6 D6 must use the L2-only semantic series constructor"),
    )
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "models"),
        env_file=str(tmp_path / ".env"),
        db_env_prefix="TDX_DB_DEV_",
        b3_p6_autocycle_l2_output=str(tmp_path / "p6.json"),
    )
    return request, policy, inputs, args


def test_p6_single_pass_runs_only_autocycle_l2_exact_grid(monkeypatch) -> None:
    series = _p6_series()
    authority = {
        "producer_commit": "c" * 40,
        "inputs": {},
        "dataset_manifest_hash": "1" * 64,
        "mapping_manifest_hash": "2" * 64,
        "calendar_manifest_hash": "3" * 64,
        "l2_stock_fact_manifest_hash": "4" * 64,
        "feature_domain_policy": _minimal_c010_policy(),
        "families": _request()["families"],
    }
    calls = []
    monkeypatch.setattr(subject, "_load_b3_formal_train_authority", lambda request, db_prefix: authority)
    monkeypatch.setattr(subject, "_direct_l2_train_series_for_family", lambda inputs, family: series)
    monkeypatch.setattr(
        subject,
        "_direct_train_series_for_family",
        lambda *args, **kwargs: pytest.fail("P6 child must use the L2-only train series constructor"),
    )

    def fake_repeat(values, *, family, level, feature_names, preprocess_family, process_identity):
        calls.append((values, family, level, process_identity))
        return _p6_repeat(process_identity), {}

    monkeypatch.setattr(subject, "run_level_repeat", fake_repeat)
    receipt = subject.prepare_b3_p6_autocycle_l2_single_pass(
        _request(),
        db_prefix="TDX_DB_DEV_",
        process_identity="fresh_process_1",
    )

    assert calls == [(series, "autocycle_all_core", "L2", "fresh_process_1")]
    assert receipt["planned_fit_count"] == 1048
    assert receipt["terminal_entry_count"] == 1048
    assert receipt["selection_performed"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_p6_l2_only_train_constructor_never_touches_l1(monkeypatch) -> None:
    inputs = {
        "panel": "L1_PANEL_SENTINEL",
        "constituents": "L1_CONSTITUENTS_SENTINEL",
        "l2_panel": "L2_PANEL_SENTINEL",
    }
    family = next(item for item in _request()["families"] if item["family"] == subject.B3_P6_FAMILY)
    captured = {}

    def guarded_build_train_only_series(
        panel,
        *,
        feature_names,
        train_start,
        train_end,
        constituent_manifest,
        expected_sector_count,
        direct_sector_level,
        frozen_input_identity=None,
    ):
        captured["panel"] = panel
        captured["constituent_manifest"] = constituent_manifest
        captured["direct_sector_level"] = direct_sector_level
        captured["expected_sector_count"] = expected_sector_count
        return _p6_series()

    monkeypatch.setattr(subject, "build_train_only_series", guarded_build_train_only_series)
    monkeypatch.setattr(
        subject,
        "_direct_l2_constituents",
        lambda inputs: {"L2-000": {"l2_codes": ["L2-000"]}},
    )
    monkeypatch.setattr(subject, "_frozen_input_identity", lambda inputs: {})
    monkeypatch.setattr(subject, "_date", lambda value, label: value)

    series = subject._direct_l2_train_series_for_family(inputs, family)

    assert captured["panel"] == "L2_PANEL_SENTINEL"
    assert captured["constituent_manifest"] == {"L2-000": {"l2_codes": ["L2-000"]}}
    assert captured["direct_sector_level"] == "L2"
    assert captured["expected_sector_count"] == subject.B3_P6_EXPECTED_SECTOR_COUNT
    assert set(series) == set(_p6_series())


def test_p6_l2_only_validation_constructor_never_touches_l1(monkeypatch) -> None:
    inputs = {
        "panel": "L1_PANEL_SENTINEL",
        "constituents": "L1_CONSTITUENTS_SENTINEL",
        "l2_panel": "L2_PANEL_SENTINEL",
    }
    family = next(item for item in _request()["families"] if item["family"] == subject.B3_P6_FAMILY)
    captured = {}

    def guarded_build_l1_training_series(
        panel,
        *,
        feature_names,
        train_start,
        train_end,
        validation_start,
        validation_end,
        constituent_manifest_by_l1,
        expected_sector_count,
        direct_sector_level,
        frozen_input_identity=None,
        validation_calendar_dates=None,
    ):
        captured["panel"] = panel
        captured["constituent_manifest_by_l1"] = constituent_manifest_by_l1
        captured["direct_sector_level"] = direct_sector_level
        captured["expected_sector_count"] = expected_sector_count
        captured["validation_calendar_dates"] = validation_calendar_dates
        return _p6_series()

    monkeypatch.setattr(subject, "build_l1_training_series", guarded_build_l1_training_series)
    monkeypatch.setattr(
        subject,
        "_direct_l2_constituents",
        lambda inputs: {"L2-000": {"l2_codes": ["L2-000"]}},
    )
    monkeypatch.setattr(subject, "_frozen_input_identity", lambda inputs: {})
    monkeypatch.setattr(
        subject,
        "_validation_calendar_dates_from_manifest",
        lambda inputs, *, validation_start, validation_end: (validation_start, validation_end),
    )
    monkeypatch.setattr(subject, "_date", lambda value, label: value)

    series = subject._direct_l2_series_for_family(inputs, family)

    assert captured["panel"] == "L2_PANEL_SENTINEL"
    assert captured["constituent_manifest_by_l1"] == {"L2-000": {"l2_codes": ["L2-000"]}}
    assert captured["direct_sector_level"] == "L2"
    assert captured["expected_sector_count"] == subject.B3_P6_EXPECTED_SECTOR_COUNT
    assert captured["validation_calendar_dates"] == (family["validation_start"], family["validation_end"])
    assert set(series) == set(_p6_series())


def _p6_drift_fixture() -> tuple[dict, dict, dict]:
    policy = _minimal_c010_policy()
    inputs = {
        "dataset_manifest": {"schema_version": "dataset_v1", "calendar_benchmark": {"schema_version": "calendar_v1"}},
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    return inputs, policy, _p6_expected_closure(inputs, policy)


def _p6_rehash_child(value: dict) -> dict:
    body = {key: item for key, item in value.items() if key != "single_pass_receipt_sha256"}
    value["single_pass_receipt_sha256"] = subject.canonical_sha256(body)
    return value


def test_p6_zero_refit_child_validator_uses_frozen_training_producer(monkeypatch) -> None:
    inputs, policy, closure = _p6_drift_fixture()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))

    subject._validate_b3_p6_child_payload(
        value,
        process_identity="fresh_process_1",
        expected=closure,
        expected_producer_commit="c" * 40,
    )

    with pytest.raises(StateModelSetError, match="child receipt is invalid"):
        subject._validate_b3_p6_child_payload(
            value,
            process_identity="fresh_process_1",
            expected=closure,
            expected_producer_commit="e" * 40,
        )


def test_p6_child_validator_rejects_self_hashed_feature_names_drift(monkeypatch) -> None:
    inputs, policy, closure = _p6_drift_fixture()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    value["feature_names"] = list(subject.BASE_FEATURES)
    value["level_repeat"]["feature_names"] = list(subject.BASE_FEATURES)
    _p6_rehash_child(value)

    with pytest.raises(StateModelSetError, match="feature_names differ"):
        subject._validate_b3_p6_child_payload(value, process_identity="fresh_process_1", expected=closure)


def test_p6_child_validator_rejects_self_hashed_preprocess_family_drift(monkeypatch) -> None:
    inputs, policy, closure = _p6_drift_fixture()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    value["preprocess_family"] = "identity"
    _p6_rehash_child(value)

    with pytest.raises(StateModelSetError, match="preprocess family differs"):
        subject._validate_b3_p6_child_payload(value, process_identity="fresh_process_1", expected=closure)


def test_p6_child_validator_rejects_self_hashed_replaced_l2_sector(monkeypatch) -> None:
    inputs, policy, closure = _p6_drift_fixture()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    codes = value["level_repeat"]["canonical_sector_codes"]
    codes.remove("L2-000")
    codes.append("L2-999")
    value["level_repeat"]["canonical_sector_set_sha256"] = subject.canonical_sha256(codes)
    _p6_rehash_child(value)

    with pytest.raises(StateModelSetError, match="canonical L2 sector set differs"):
        subject._validate_b3_p6_child_payload(value, process_identity="fresh_process_1", expected=closure)


def test_p6_child_validator_rejects_self_hashed_missing_l2_sector(monkeypatch) -> None:
    inputs, policy, closure = _p6_drift_fixture()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    codes = value["level_repeat"]["canonical_sector_codes"]
    value["level_repeat"]["canonical_sector_codes"] = codes[:-1]
    value["level_repeat"]["canonical_sector_set_sha256"] = subject.canonical_sha256(codes[:-1])
    _p6_rehash_child(value)

    with pytest.raises(StateModelSetError, match="child receipt is invalid"):
        subject._validate_b3_p6_child_payload(value, process_identity="fresh_process_1", expected=closure)


def test_p6_parent_fails_closed_before_d5_on_child_feature_drift(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    drifted = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    drifted["feature_names"] = list(subject.BASE_FEATURES)
    drifted["level_repeat"]["feature_names"] = list(subject.BASE_FEATURES)
    _p6_rehash_child(drifted)
    payloads = [
        subject.canonical_json_bytes(drifted),
        _p6_child_payload("fresh_process_2", inputs=inputs, policy=policy),
    ]
    call_index = 0

    def fake_run(command, *, check, capture_output, env, timeout):
        nonlocal call_index
        payload = payloads[call_index]
        call_index += 1
        return SimpleNamespace(returncode=0, stdout=payload, stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    monkeypatch.setattr(
        subject,
        "select_level_restart",
        lambda *args, **kwargs: pytest.fail("D5 must not run on child authority drift"),
    )

    with pytest.raises(StateModelSetError, match="feature_names differ"):
        subject.run_b3_p6_autocycle_l2_repeated(args, request)


def test_p6_parent_fails_closed_on_two_process_sector_mismatch(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    second = json.loads(_p6_child_payload("fresh_process_2", inputs=inputs, policy=policy))
    codes = second["level_repeat"]["canonical_sector_codes"]
    codes.remove("L2-001")
    codes.append("L2-998")
    second["level_repeat"]["canonical_sector_set_sha256"] = subject.canonical_sha256(codes)
    _p6_rehash_child(second)
    payloads = [
        _p6_child_payload("fresh_process_1", inputs=inputs, policy=policy),
        subject.canonical_json_bytes(second),
    ]
    call_index = 0

    def fake_run(command, *, check, capture_output, env, timeout):
        nonlocal call_index
        payload = payloads[call_index]
        call_index += 1
        return SimpleNamespace(returncode=0, stdout=payload, stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    monkeypatch.setattr(
        subject,
        "select_level_restart",
        lambda *args, **kwargs: pytest.fail("D5 must not run when fresh-process authorities differ"),
    )

    with pytest.raises(StateModelSetError, match="canonical L2 sector set differs"):
        subject.run_b3_p6_autocycle_l2_repeated(args, request)


def _p6_cli_setup(monkeypatch, tmp_path, output_path) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text("{}", encoding="utf-8")
    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        subject.sys,
        "argv",
        [
            str(Path(subject.__file__).resolve()),
            "--request",
            str(request_path),
            "--output-root",
            str(tmp_path / "models"),
            "--env-file",
            str(env_path),
            "--db-env-prefix",
            "TDX_DB_DEV_",
            "--b3-p6-autocycle-l2-output",
            str(output_path),
        ],
    )
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_git_commit", lambda: "c" * 40)


def _p6_write_child_receipts(tmp_path, output_path) -> None:
    policy = _minimal_c010_policy()
    inputs = {
        "dataset_manifest": {"schema_version": "dataset_v1", "calendar_benchmark": {"schema_version": "calendar_v1"}},
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    args = SimpleNamespace(b3_p6_autocycle_l2_output=str(output_path))
    for process_identity in ("fresh_process_1", "fresh_process_2"):
        receipt = json.loads(_p6_child_payload(process_identity, inputs=inputs, policy=policy))
        subject._write_diagnostic_report(subject._b3_p6_process_receipt_path(args, process_identity), receipt)


def _p6_accepted_report() -> dict:
    return {
        "status": "accepted",
        "planned_fit_count": 2096,
        "terminal_entry_count": 2096,
        "selection_performed": True,
        "selection": {"level_selection_status": "accepted", "receipt_sha256": "a" * 64},
        "d6_performed_after_selection": True,
        "selected_level_artifact_write_performed": True,
        "ready_artifact_write_performed": False,
    }


def test_p6_cli_report_write_failure_writes_durable_parent_failure(monkeypatch, tmp_path) -> None:
    output_path = tmp_path / "p6.json"
    _p6_cli_setup(monkeypatch, tmp_path, output_path)
    _p6_write_child_receipts(tmp_path, output_path)
    monkeypatch.setattr(subject, "run_b3_p6_autocycle_l2_repeated", lambda args, request: _p6_accepted_report())
    real_write = subject._write_diagnostic_report

    def failing_write(path, report):
        if Path(path).resolve() == output_path.resolve():
            raise OSError("simulated report write failure")
        return real_write(path, report)

    monkeypatch.setattr(subject, "_write_diagnostic_report", failing_write)

    assert subject.main() == 1

    failure = json.loads((tmp_path / "p6.parent.failure.json").read_text(encoding="utf-8"))
    assert failure["schema_version"] == subject.B3_P6_FAILURE_SCHEMA
    assert failure["status"] == "failed"
    assert failure["failure_stage"] == "report_write"
    assert failure["verified_process_count"] == 2
    assert failure["terminal_entry_count"] == 2096
    assert failure["fit_grid_completed"] is True
    assert failure["selection_performed"] is True
    assert failure["selection_status"] == "accepted"
    assert failure["d6_performed_after_selection"] is True
    assert failure["selected_level_artifact_write_performed"] is True
    assert failure["phase2_ready"] is False
    assert failure["ready_artifact_write_performed"] is False
    assert not output_path.exists()


def test_p6_cli_report_readback_mismatch_writes_durable_parent_failure(monkeypatch, tmp_path) -> None:
    output_path = tmp_path / "p6.json"
    _p6_cli_setup(monkeypatch, tmp_path, output_path)
    _p6_write_child_receipts(tmp_path, output_path)
    monkeypatch.setattr(subject, "run_b3_p6_autocycle_l2_repeated", lambda args, request: _p6_accepted_report())
    real_readback = subject._load_json_mapping

    def tampered_readback(path, *, label):
        if Path(path).resolve() == output_path.resolve():
            value = json.loads(Path(path).read_text(encoding="utf-8"))
            value["_tampered"] = True
            return value
        return real_readback(path, label=label)

    monkeypatch.setattr(subject, "_load_json_mapping", tampered_readback)

    assert subject.main() == 1

    failure = json.loads((tmp_path / "p6.parent.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_stage"] == "report_readback"
    assert failure["verified_process_count"] == 2
    assert failure["terminal_entry_count"] == 2096
    assert failure["fit_grid_completed"] is True
    assert failure["selection_performed"] is True
    assert failure["selection_status"] == "accepted"
    assert failure["d6_performed_after_selection"] is True
    assert failure["phase2_ready"] is False
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")).get("_tampered") is None


def test_p6_cli_report_collision_preserves_existing_report(monkeypatch, tmp_path) -> None:
    output_path = tmp_path / "p6.json"
    _p6_cli_setup(monkeypatch, tmp_path, output_path)
    _p6_write_child_receipts(tmp_path, output_path)
    old_report = {"status": "blocked", "old": True, "planned_fit_count": 2096, "terminal_entry_count": 2096}
    output_path.write_bytes(subject.canonical_json_bytes(old_report))
    monkeypatch.setattr(subject, "run_b3_p6_autocycle_l2_repeated", lambda args, request: _p6_accepted_report())

    assert subject.main() == 1

    assert json.loads(output_path.read_text(encoding="utf-8")) == old_report
    failure = json.loads((tmp_path / "p6.parent.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_stage"] == "report_write"
    assert failure["status"] == "failed"
    assert failure["selection_performed"] is True
    assert failure["d6_performed_after_selection"] is True
    assert failure["phase2_ready"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_p6_cli_execution_failure_writes_parent_failure_with_unknown_states(monkeypatch, tmp_path) -> None:
    output_path = tmp_path / "p6.json"
    _p6_cli_setup(monkeypatch, tmp_path, output_path)

    def boom(args, request):
        raise StateModelSetError("D6 failed before report")

    monkeypatch.setattr(subject, "run_b3_p6_autocycle_l2_repeated", boom)

    assert subject.main() == 1

    failure = json.loads((tmp_path / "p6.parent.failure.json").read_text(encoding="utf-8"))
    assert failure["failure_stage"] == "execution"
    assert failure["error_type"] == "StateModelSetError"
    assert failure["selection_performed"] is None
    assert failure["selection_status"] == "unknown_due_parent_failure"
    assert failure["d6_performed_after_selection"] is None
    assert failure["selected_level_artifact_write_performed"] is None
    assert failure["d5_checkpoint_status"] == "missing"
    assert failure["phase2_ready"] is False
    assert failure["ready_artifact_write_performed"] is False
    assert not output_path.exists()


def test_p6_d5_checkpoint_is_durable_before_semantic_failure_and_recovers_authority(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    payloads = [
        _p6_child_payload("fresh_process_1", inputs=inputs, policy=policy),
        _p6_child_payload("fresh_process_2", inputs=inputs, policy=policy),
    ]
    call_index = 0

    def fake_run(command, *, check, capture_output, env, timeout):
        nonlocal call_index
        payload = payloads[call_index]
        call_index += 1
        return SimpleNamespace(returncode=0, stdout=payload, stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    selection = _p6_accepted_selection(policy_sha256=policy["receipt_sha256"])
    training_body = {
        "schema_version": subject.B3_P6_D5_TRAINING_ARTIFACT_SCHEMA,
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "selected_seed": 43,
        "selection_receipt_sha256": selection["receipt_sha256"],
        "entry_count": subject.B3_P6_EXPECTED_SECTOR_COUNT,
        "entries": [],
        "selection_reexecuted": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "ready": False,
    }
    training_artifact = {**training_body, "artifact_sha256": subject.canonical_sha256(training_body)}
    monkeypatch.setattr(subject, "select_level_restart", lambda *args, **kwargs: selection)
    monkeypatch.setattr(
        subject,
        "_build_b3_p6_selected_training_artifact",
        lambda frozen_selection, repeat: training_artifact,
    )
    monkeypatch.setattr(subject, "_p6_zero_refit_training_authority", lambda report: ({}, {}, {}, ()))
    monkeypatch.setattr(
        subject,
        "_load_verified_formal_semantic_inputs",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            StateModelSetError("causal posterior normalization failed at row 0")
        ),
    )

    with pytest.raises(StateModelSetError, match="causal posterior normalization failed") as excinfo:
        subject.run_b3_p6_autocycle_l2_repeated(args, request)

    checkpoint_path = subject._b3_p6_d5_checkpoint_path(args)
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["selection"]["receipt_sha256"] == selection["receipt_sha256"]
    assert checkpoint["semantic_source_accessed_after_selection"] is False
    assert checkpoint["d6_performed_after_selection"] is False
    assert checkpoint["ready_artifact_write_performed"] is False

    failure = subject._build_b3_p6_parent_failure(args, excinfo.value)
    assert failure["d5_checkpoint_status"] == "verified"
    assert failure["d5_checkpoint_receipt_sha256"] == checkpoint["receipt_sha256"]
    assert failure["selection_performed"] is True
    assert failure["selection_status"] == "accepted"
    assert failure["d6_performed_after_selection"] is None
    assert failure["ready_artifact_write_performed"] is False
    assert subject._resolve_p6_zero_refit_training_authority(failure) == checkpoint
    drifted_body = {
        **{key: value for key, value in failure.items() if key != "receipt_sha256"},
        "d5_checkpoint_receipt_sha256": "0" * 64,
    }
    drifted = {**drifted_body, "receipt_sha256": subject.canonical_sha256(drifted_body)}
    with pytest.raises(StateModelSetError, match="checkpoint receipt differs"):
        subject._resolve_p6_zero_refit_training_authority(drifted)

    drifted_checkpoint = {**checkpoint, "fresh_process_receipt_hashes": ["f" * 64, "e" * 64]}
    monkeypatch.setattr(subject, "_load_b3_p6_d5_checkpoint", lambda path: drifted_checkpoint)
    invalid_failure = subject._build_b3_p6_parent_failure(args, excinfo.value)
    assert invalid_failure["d5_checkpoint_status"] == "invalid"
    assert invalid_failure["d5_checkpoint_receipt_sha256"] is None
    assert invalid_failure["selection_performed"] is None
    assert invalid_failure["selection_status"] == "unknown_due_parent_failure"


def test_p6_zero_refit_historical_failure_without_d5_checkpoint_fails_closed() -> None:
    body = {
        "schema_version": subject.B3_P6_FAILURE_SCHEMA,
        "status": "failed",
        "selection_status": "unknown_due_parent_failure",
        "ready_artifact_write_performed": False,
    }
    failure = {**body, "receipt_sha256": subject.canonical_sha256(body)}

    with pytest.raises(StateModelSetError, match="D5 checkpoint is missing; D5 re-execution is prohibited"):
        subject._resolve_p6_zero_refit_training_authority(failure)


def test_p6_zero_refit_accepts_complete_d5_checkpoint_without_reselection(monkeypatch, tmp_path) -> None:
    codes = tuple(sorted(_p6_series()))
    selection = _p6_accepted_selection(policy_sha256="3" * 64, codes=codes)
    entries = []
    for code in codes:
        model_body = {
            "family": subject.B3_P6_FAMILY,
            "level": subject.B3_P6_LEVEL,
            "seed": 43,
            "sector_code": code,
        }
        model = {**model_body, "model_payload_sha256": subject.canonical_sha256(model_body)}
        training_body = {
            "seed": 43,
            "sector_code": code,
            "model_entry_status": "accepted",
            "model_entry_valid": True,
            "model_payload_sha256": model["model_payload_sha256"],
        }
        training = {**training_body, "entry_receipt_sha256": subject.canonical_sha256(training_body)}
        entry_body = {**model, "training_receipt": training}
        entries.append({**entry_body, "selected_entry_sha256": subject.canonical_sha256(entry_body)})
    training_artifact_body = {
        "schema_version": subject.B3_P6_D5_TRAINING_ARTIFACT_SCHEMA,
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "selected_seed": 43,
        "selection_receipt_sha256": selection["receipt_sha256"],
        "entry_count": len(entries),
        "entries": entries,
        "selection_reexecuted": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "ready": False,
    }
    training_artifact = {
        **training_artifact_body,
        "artifact_sha256": subject.canonical_sha256(training_artifact_body),
    }
    checkpoint_body = {
        "schema_version": subject.B3_P6_D5_CHECKPOINT_SCHEMA,
        "status": "selected",
        "producer_commit": "a" * 40,
        "target_family": subject.B3_P6_FAMILY,
        "target_level": subject.B3_P6_LEVEL,
        "planned_fit_count": 2096,
        "terminal_entry_count": 2096,
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "semantic_dataset_manifest_hash": "e" * 64,
        "semantic_mapping_manifest_hash": "f" * 64,
        "semantic_calendar_manifest_hash": "1" * 64,
        "semantic_l2_stock_fact_manifest_hash": "2" * 64,
        "feature_domain_policy_sha256": "3" * 64,
        "formula_version": subject.C010_FORMULA_VERSION,
        "fresh_process_receipt_hashes": ["1" * 64, "2" * 64],
        "fresh_process_receipt_paths": [
            str((tmp_path / "fresh_process_1.json").resolve()),
            str((tmp_path / "fresh_process_2.json").resolve()),
        ],
        "selection": selection,
        "selected_training_artifact": training_artifact,
        "selection_performed": True,
        "selection_used_validation": False,
        "selection_used_future_utility": False,
        "selection_followed_by_refit": False,
        "semantic_source_accessed_after_selection": False,
        "d6_performed_after_selection": False,
        "selected_level_artifact_write_performed": False,
        "family_model_set_status": "blocked",
        "phase2_ready": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    checkpoint = {**checkpoint_body, "receipt_sha256": subject.canonical_sha256(checkpoint_body)}
    fitted = {
        (43, code): SimpleNamespace(model_payload_sha256=entries[index]["model_payload_sha256"])
        for index, code in enumerate(codes)
    }
    monkeypatch.setattr(subject, "models_from_repeat", lambda repeat: fitted)
    monkeypatch.setattr(
        subject,
        "select_level_restart",
        lambda *args, **kwargs: pytest.fail("D5 must not be re-executed while reading its checkpoint"),
    )

    checkpoint_path = tmp_path / "p6.d5.checkpoint.json"
    subject._write_diagnostic_report(checkpoint_path, checkpoint)
    assert subject._load_b3_p6_d5_checkpoint(checkpoint_path) == checkpoint

    frozen_selection, frozen_models, repeat, frozen_codes = subject._p6_zero_refit_training_authority(checkpoint)

    assert frozen_selection == selection
    assert frozen_models == fitted
    assert frozen_codes == codes
    expected_models = [
        {key: entry[key] for key in ("family", "level", "seed", "sector_code", "model_payload_sha256")}
        for entry in entries
    ]
    assert repeat["model_payload_sha256"] == subject.canonical_sha256(expected_models)

    incomplete_training_body = {
        **training_artifact_body,
        "entries": entries[:-1],
    }
    incomplete_training = {
        **incomplete_training_body,
        "artifact_sha256": subject.canonical_sha256(incomplete_training_body),
    }
    incomplete_checkpoint_body = {
        **checkpoint_body,
        "selected_training_artifact": incomplete_training,
    }
    incomplete_checkpoint = {
        **incomplete_checkpoint_body,
        "receipt_sha256": subject.canonical_sha256(incomplete_checkpoint_body),
    }
    incomplete_path = tmp_path / "p6.incomplete.d5.checkpoint.json"
    subject._write_diagnostic_report(incomplete_path, incomplete_checkpoint)
    with pytest.raises(StateModelSetError, match="exactly 131 selected entries"):
        subject._load_b3_p6_d5_checkpoint(incomplete_path)

    for field in ("phase2_ready", "database_write_performed", "runtime_action_performed"):
        forbidden_body = {**checkpoint_body, field: True}
        forbidden = {**forbidden_body, "receipt_sha256": subject.canonical_sha256(forbidden_body)}
        forbidden_path = tmp_path / f"p6.{field}.d5.checkpoint.json"
        subject._write_diagnostic_report(forbidden_path, forbidden)
        with pytest.raises(StateModelSetError, match="checkpoint authority is invalid"):
            subject._load_b3_p6_d5_checkpoint(forbidden_path)
        with pytest.raises(StateModelSetError, match="checkpoint authority is invalid"):
            subject._resolve_p6_zero_refit_training_authority(forbidden)

    training_ready_body = {**training_artifact_body, "ready": True}
    training_ready = {
        **training_ready_body,
        "artifact_sha256": subject.canonical_sha256(training_ready_body),
    }
    training_ready_checkpoint_body = {
        **checkpoint_body,
        "selected_training_artifact": training_ready,
    }
    training_ready_checkpoint = {
        **training_ready_checkpoint_body,
        "receipt_sha256": subject.canonical_sha256(training_ready_checkpoint_body),
    }
    training_ready_path = tmp_path / "p6.training-ready.d5.checkpoint.json"
    subject._write_diagnostic_report(training_ready_path, training_ready_checkpoint)
    with pytest.raises(StateModelSetError, match="checkpoint authority is invalid"):
        subject._load_b3_p6_d5_checkpoint(training_ready_path)

    drifted_training_receipt_body = {
        **{key: value for key, value in entries[0]["training_receipt"].items() if key != "entry_receipt_sha256"},
        "model_entry_status": "failed",
        "model_entry_valid": False,
    }
    drifted_training_receipt = {
        **drifted_training_receipt_body,
        "entry_receipt_sha256": subject.canonical_sha256(drifted_training_receipt_body),
    }
    drifted_entry_body = {
        **{key: value for key, value in entries[0].items() if key != "selected_entry_sha256"},
        "training_receipt": drifted_training_receipt,
    }
    drifted_entry = {
        **drifted_entry_body,
        "selected_entry_sha256": subject.canonical_sha256(drifted_entry_body),
    }
    drifted_training_body = {
        **training_artifact_body,
        "entries": [drifted_entry, *entries[1:]],
    }
    drifted_training = {
        **drifted_training_body,
        "artifact_sha256": subject.canonical_sha256(drifted_training_body),
    }
    drifted_checkpoint_body = {
        **checkpoint_body,
        "selected_training_artifact": drifted_training,
    }
    drifted_checkpoint = {
        **drifted_checkpoint_body,
        "receipt_sha256": subject.canonical_sha256(drifted_checkpoint_body),
    }
    drifted_checkpoint_path = tmp_path / "p6.failed-training-receipt.d5.checkpoint.json"
    subject._write_diagnostic_report(drifted_checkpoint_path, drifted_checkpoint)
    with pytest.raises(StateModelSetError, match="training receipt readback failed"):
        subject._load_b3_p6_d5_checkpoint(drifted_checkpoint_path)
    with pytest.raises(StateModelSetError, match="training receipt readback failed"):
        subject._resolve_p6_zero_refit_training_authority(drifted_checkpoint)

    validation_evidence = {**selection["evidence"], "validation_accessed": True}
    validation_selection_body = {
        **{key: value for key, value in selection.items() if key != "receipt_sha256"},
        "evidence": validation_evidence,
    }
    validation_selection = {
        **validation_selection_body,
        "receipt_sha256": subject.canonical_sha256(validation_selection_body),
    }
    validation_training_body = {
        **training_artifact_body,
        "selection_receipt_sha256": validation_selection["receipt_sha256"],
    }
    validation_training = {
        **validation_training_body,
        "artifact_sha256": subject.canonical_sha256(validation_training_body),
    }
    validation_checkpoint_body = {
        **checkpoint_body,
        "selection": validation_selection,
        "selected_training_artifact": validation_training,
    }
    validation_checkpoint = {
        **validation_checkpoint_body,
        "receipt_sha256": subject.canonical_sha256(validation_checkpoint_body),
    }
    validation_checkpoint_path = tmp_path / "p6.validation-accessed.d5.checkpoint.json"
    subject._write_diagnostic_report(validation_checkpoint_path, validation_checkpoint)
    with pytest.raises(StateModelSetError, match="checkpoint authority is invalid"):
        subject._load_b3_p6_d5_checkpoint(validation_checkpoint_path)


def test_p6_selected_training_checkpoint_builder_freezes_exact_d5_models_without_d6(monkeypatch) -> None:
    codes = tuple(sorted(_p6_series()))
    selection = _p6_accepted_selection(policy_sha256="9" * 64, codes=codes)
    entries = []
    fitted = {}
    for code in codes:
        model_body = {
            "family": subject.B3_P6_FAMILY,
            "level": subject.B3_P6_LEVEL,
            "seed": 43,
            "sector_code": code,
        }
        model = {**model_body, "model_payload_sha256": subject.canonical_sha256(model_body)}
        training_body = {
            "seed": 43,
            "sector_code": code,
            "model_entry_status": "accepted",
            "model_entry_valid": True,
            "model_payload_sha256": model["model_payload_sha256"],
        }
        entries.append({**training_body, "entry_receipt_sha256": subject.canonical_sha256(training_body)})
        fitted[(43, code)] = SimpleNamespace(
            model_payload_sha256=model["model_payload_sha256"],
            payload=lambda value=model: value,
        )
    repeat = {
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "schedule": list(subject.RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "entries": entries,
    }
    monkeypatch.setattr(subject, "models_from_repeat", lambda value: fitted)
    monkeypatch.setattr(
        subject,
        "_load_verified_formal_semantic_inputs",
        lambda *args, **kwargs: pytest.fail("D5 checkpoint builder must not access D6 inputs"),
    )

    artifact = subject._build_b3_p6_selected_training_artifact(selection, repeat)

    assert artifact["schema_version"] == subject.B3_P6_D5_TRAINING_ARTIFACT_SCHEMA
    assert artifact["entry_count"] == subject.B3_P6_EXPECTED_SECTOR_COUNT
    assert artifact["selected_seed"] == 43
    assert artifact["validation_accessed"] is False
    assert artifact["future_utility_accessed"] is False
    assert artifact["ready"] is False
    assert artifact["artifact_sha256"] == subject.canonical_sha256(
        {key: value for key, value in artifact.items() if key != "artifact_sha256"}
    )


def test_p6_parent_runs_two_exact_children_and_blocks_without_d5_candidate(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        subject,
        "_load_verified_formal_semantic_inputs",
        lambda *args, **kwargs: pytest.fail("D5 blocked must not access semantic validation inputs"),
    )
    payloads = [
        _p6_child_payload("fresh_process_1", inputs=inputs, policy=policy),
        _p6_child_payload("fresh_process_2", inputs=inputs, policy=policy),
    ]
    child_calls = []

    def fake_run(command, *, check, capture_output, env, timeout):
        child_calls.append((command, dict(env), timeout))
        return SimpleNamespace(returncode=0, stdout=payloads[len(child_calls) - 1], stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    selection_body = {
        "level_selection_valid": False,
        "level_selection_status": "blocked",
        "failure_reason_codes": ["hmm_risk_model_selection_unavailable"],
    }
    selection = {**selection_body, "receipt_sha256": subject.canonical_sha256(selection_body)}
    monkeypatch.setattr(subject, "select_level_restart", lambda *args, **kwargs: selection)
    monkeypatch.setattr(
        subject,
        "build_selected_level_artifact",
        lambda *args, **kwargs: pytest.fail("D6 must not run without an accepted D5 candidate"),
    )

    report = subject.run_b3_p6_autocycle_l2_repeated(args, request)

    assert len(child_calls) == 2
    assert all(
        call[1][key] == "1"
        for call in child_calls
        for key in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        )
    )
    assert report["planned_fit_count"] == 2096
    assert report["terminal_entry_count"] == 2096
    assert report["status"] == "blocked"
    assert report["semantic_source_accessed_after_selection"] is False
    assert report["d6_performed_after_selection"] is False
    assert report["family_model_set_status"] == "blocked"
    assert report["phase2_ready"] is False
    assert report["ready_artifact_write_performed"] is False


def test_p6_parent_persists_only_accepted_selected_level_and_never_ready(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    semantic_load_count = 0

    def load_semantic_after_selection(request, db_prefix):
        nonlocal semantic_load_count
        semantic_load_count += 1
        return {}

    monkeypatch.setattr(subject, "_load_verified_formal_semantic_inputs", load_semantic_after_selection)
    payloads = [
        _p6_child_payload("fresh_process_1", inputs=inputs, policy=policy),
        _p6_child_payload("fresh_process_2", inputs=inputs, policy=policy),
    ]
    call_index = 0

    def fake_run(command, *, check, capture_output, env, timeout):
        nonlocal call_index
        payload = payloads[call_index]
        call_index += 1
        return SimpleNamespace(returncode=0, stdout=payload, stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    selection = _p6_accepted_selection(policy_sha256=policy["receipt_sha256"])
    training_body = {
        "schema_version": subject.B3_P6_D5_TRAINING_ARTIFACT_SCHEMA,
        "family": subject.B3_P6_FAMILY,
        "level": subject.B3_P6_LEVEL,
        "selected_seed": 43,
        "selection_receipt_sha256": selection["receipt_sha256"],
        "entry_count": subject.B3_P6_EXPECTED_SECTOR_COUNT,
        "entries": [],
        "selection_reexecuted": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "ready": False,
    }
    training_artifact = {
        **training_body,
        "artifact_sha256": subject.canonical_sha256(training_body),
    }
    artifact_body = {"schema_version": "test", "status": "accepted", "family": "autocycle_all_core", "level": "L2"}
    artifact = {**artifact_body, "artifact_sha256": subject.canonical_sha256(artifact_body)}
    monkeypatch.setattr(subject, "select_level_restart", lambda *args, **kwargs: selection)
    monkeypatch.setattr(
        subject,
        "_build_b3_p6_selected_training_artifact",
        lambda frozen_selection, repeat: training_artifact,
    )
    monkeypatch.setattr(subject, "_p6_zero_refit_training_authority", lambda report: ({}, {}, {}, ()))
    monkeypatch.setattr(subject, "models_from_repeat", lambda repeat: {})
    monkeypatch.setattr(subject, "build_selected_level_artifact", lambda *args, **kwargs: artifact)
    monkeypatch.setattr(subject, "read_b3_selected_level_artifact", lambda path, **kwargs: artifact)

    report = subject.run_b3_p6_autocycle_l2_repeated(args, request)

    artifact_path = Path(report["selected_level_artifact_path"])
    checkpoint_path = Path(report["d5_checkpoint_path"])
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert report["status"] == "accepted"
    assert report["d5_checkpoint_write_performed"] is True
    assert checkpoint["status"] == "selected"
    assert checkpoint["selection"]["receipt_sha256"] == selection["receipt_sha256"]
    assert checkpoint["d6_performed_after_selection"] is False
    assert checkpoint["selected_level_artifact_write_performed"] is False
    assert checkpoint["ready_artifact_write_performed"] is False
    assert semantic_load_count == 1
    assert report["semantic_source_accessed_after_selection"] is True
    assert report["d6_performed_after_selection"] is True
    assert report["selected_level_artifact_write_performed"] is True
    assert artifact_path.read_bytes() == subject.canonical_json_bytes(artifact)
    assert report["family_model_set_status"] == "blocked"
    assert report["phase2_ready"] is False
    assert report["ready_manifest_path"] is None
    assert report["ready_artifact_write_performed"] is False


def test_p6_d6_zero_refit_replay_preserves_model_and_selection_lineage(monkeypatch, tmp_path) -> None:
    request = _request()
    policy_sha256 = "9" * 64
    request["feature_domain_policy_sha256"] = policy_sha256
    semantic_identities = {
        "semantic_dataset_manifest_hash": "1" * 64,
        "semantic_mapping_manifest_hash": "2" * 64,
        "semantic_calendar_manifest_hash": "3" * 64,
        "semantic_l2_stock_fact_manifest_hash": "4" * 64,
    }
    request.update(semantic_identities)
    codes = tuple(sorted(_p6_series()))
    model_hashes = {code: subject.canonical_sha256({"code": code}) for code in codes}
    models = {(43, code): SimpleNamespace(model_payload_sha256=model_hashes[code]) for code in codes}
    selection = {
        "receipt_sha256": "8" * 64,
        "evidence": {"selected_seed": 43},
        "level_selection_valid": True,
    }
    monkeypatch.setattr(
        subject,
        "_p6_zero_refit_training_authority",
        lambda report: (selection, models, {"entries": []}, codes),
    )
    monkeypatch.setattr(subject, "_require_approved_b3_windows", lambda value: None)
    monkeypatch.setattr(subject, "_require_formal_semantic_identity", lambda value: None)
    monkeypatch.setattr(subject, "_require_c010_policy_identity", lambda value: None)
    monkeypatch.setattr(subject, "_validate_b3_p6_child_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(subject, "_load_verified_formal_semantic_inputs", lambda value, db_prefix: {})
    monkeypatch.setattr(subject, "_semantic_input_identities", lambda value: semantic_identities)
    monkeypatch.setattr(
        subject,
        "_direct_l2_series_for_family",
        lambda value, family: {
            code: SimpleNamespace(validation_input_manifest={"schema_version": "v2", "code": code}) for code in codes
        },
    )
    selected_artifact = {
        "status": "accepted",
        "entries": [{"model_payload_sha256": model_hashes[code]} for code in codes],
    }
    monkeypatch.setattr(
        subject,
        "build_selected_level_artifact",
        lambda frozen_selection, frozen_models, series, repeat: selected_artifact,
    )
    selected_path = tmp_path / "selected.json"
    monkeypatch.setattr(subject, "_write_b3_p6_selected_level_artifact", lambda root, value: selected_path)
    monkeypatch.setattr(subject, "read_b3_selected_level_artifact", lambda *args, **kwargs: selected_artifact)
    monkeypatch.setattr(subject, "_git_commit", lambda: "b" * 40)
    monkeypatch.setattr(
        subject,
        "select_level_restart",
        lambda *args, **kwargs: pytest.fail("zero-refit replay must not execute D5"),
    )
    train_identities = {
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "d" * 64,
        "feature_domain_policy_sha256": policy_sha256,
    }
    child_paths = []
    child_hashes = []
    for index, process_identity in enumerate(("fresh_process_1", "fresh_process_2"), start=1):
        child_hash = str(index) * 64
        child_path = tmp_path / f"{process_identity}.json"
        subject._write_diagnostic_report(
            child_path,
            {"single_pass_receipt_sha256": child_hash, **train_identities},
        )
        child_paths.append(str(child_path))
        child_hashes.append(child_hash)
    parent_report = {
        "schema_version": subject.B3_P6_REPORT_SCHEMA,
        "receipt_sha256": "e" * 64,
        "fresh_process_receipt_paths": child_paths,
        "fresh_process_receipt_hashes": child_hashes,
        **train_identities,
        **semantic_identities,
        "producer_commit": "a" * 40,
    }
    args = SimpleNamespace(db_env_prefix="TDX_DB_", output_root=str(tmp_path))

    report = subject.run_b3_p6_d6_zero_refit_replay(args, request, parent_report)

    assert report["status"] == "accepted"
    assert report["fit_performed"] is False
    assert report["refit_count"] == 0
    assert report["selection_reexecuted"] is False
    assert report["selected_seed_unchanged"] is True
    assert report["model_parameter_hashes_unchanged"] is True
    assert report["selected_model_payload_hashes"] == [model_hashes[code] for code in codes]
    assert report["ready_artifact_write_performed"] is False
    assert report["phase2_ready"] is False

    child_drift = {**parent_report, "fresh_process_receipt_hashes": ["0" * 64, child_hashes[1]]}
    with pytest.raises(StateModelSetError, match="fresh-process receipt hash differs"):
        subject.run_b3_p6_d6_zero_refit_replay(args, request, child_drift)

    monkeypatch.setattr(
        subject,
        "_semantic_input_identities",
        lambda value: {**semantic_identities, "semantic_calendar_manifest_hash": "0" * 64},
    )
    with pytest.raises(StateModelSetError, match="semantic source authority drifted"):
        subject.run_b3_p6_d6_zero_refit_replay(args, request, parent_report)


def test_p6_child_failure_receipt_never_claims_selection_or_ready(tmp_path) -> None:
    args = SimpleNamespace(b3_p6_autocycle_l2_output=str(tmp_path / "p6.json"))
    path, receipt = subject._persist_b3_p6_child_failure(
        args,
        process_identity="fresh_process_1",
        returncode=2,
        stdout=b"partial",
        stderr=b'{"error_type":"covariance","error":"failed"}',
    )

    assert path.is_file()
    assert receipt["fit_grid_completed"] is False
    assert receipt["selection_performed"] is False
    assert receipt["selected_level_artifact_write_performed"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_p6_child_validator_rejects_self_hashed_target_scope_drift(monkeypatch) -> None:
    policy = _minimal_c010_policy()
    inputs = {
        "dataset_manifest": {"calendar_benchmark": {}},
        "mapping_manifest": {},
        "l2_stock_fact_manifest": {},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    value = json.loads(_p6_child_payload("fresh_process_1", inputs=inputs, policy=policy))
    value["target_level"] = "L1"
    body = {key: item for key, item in value.items() if key != "single_pass_receipt_sha256"}
    value["single_pass_receipt_sha256"] = subject.canonical_sha256(body)

    with pytest.raises(StateModelSetError, match="child receipt is invalid"):
        subject._validate_b3_p6_child_payload(value, process_identity="fresh_process_1")


def test_p6_mode_isolation_rejects_legacy_full_child_combination(tmp_path) -> None:
    args = SimpleNamespace(
        b3_p6_autocycle_l2_output=str(tmp_path / "p6.json"),
        b3_request_candidate_output=None,
        b3_process_identity="",
        _b3_child=True,
        _c008_b3_diag02_child=False,
        _c008_b3_diag04_child=False,
        _b3_blocker_diag01_child=False,
        _b3_d1_controlled_child=False,
    )

    with pytest.raises(StateModelSetError, match="cannot be combined"):
        subject._require_b3_p6_mode_isolation(args, p6_parent=True, p6_child=False)


@pytest.mark.parametrize(
    "field",
    [
        *subject.B3_HIDDEN_CHILD_ARGUMENTS,
        "_b3_p6_autocycle_l2_child",
        "b3_process_identity",
        "b3_d1_producer_commit",
        "b3_d1_current_authority_sha256",
        "b3_d1_historical_reference_sha256",
    ],
)
def test_p6_zero_refit_mode_isolation_rejects_every_child_identity(field) -> None:
    values = {
        **{name: False for name in subject.B3_HIDDEN_CHILD_ARGUMENTS},
        "_b3_p6_autocycle_l2_child": False,
        "b3_process_identity": "",
        "b3_d1_producer_commit": "",
        "b3_d1_current_authority_sha256": "",
        "b3_d1_historical_reference_sha256": "",
    }
    values[field] = True if field.startswith("_") else "x"

    with pytest.raises(StateModelSetError, match="cannot be combined with another child mode"):
        subject._require_b3_p6_zero_refit_mode_isolation(SimpleNamespace(**values))


def test_p6_zero_refit_cli_rejects_hidden_diagnostic_child_before_dispatch(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prepare_state_model_set.py",
            "--request",
            "ignored.json",
            "--output-root",
            ".",
            "--env-file",
            "ignored.env",
            "--db-env-prefix",
            "TEST_",
            "--b3-p6-d6-zero-refit-output",
            "zero.json",
            "--b3-p6-parent-report",
            "parent.json",
            "--_c008-b3-diag02-child",
        ],
    )
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: {})
    monkeypatch.setattr(
        subject,
        "diagnose_c008_b3_diag02",
        lambda *args, **kwargs: pytest.fail("hidden diagnostic child must not shadow zero-refit"),
    )

    assert subject.main() == 1
    assert "cannot be combined with another child mode" in capsys.readouterr().err


@pytest.mark.parametrize("mode", ["c008", "c008_b1", "diag02", "diag04"])
def test_historical_c008_entrypoints_use_dense_diagnostic_constructor(monkeypatch, mode) -> None:
    producer = "c" * 40
    request = {
        "producer_commit": producer,
        "families": [
            {
                "family": "legacy_covfix",
                "feature_names": list(BASE_FEATURES),
                "preprocess_family": "identity",
            }
        ],
    }
    inputs = {
        "source_spec": SimpleNamespace(universe_key="frozen-universe"),
        "database": {},
        "panel": object(),
        "constituents": {},
        "dataset_manifest": {},
        "mapping_manifest": {},
        "feature_definition": {},
        "l2_stock_fact_manifest": {},
    }
    spec = SimpleNamespace(
        family="legacy_covfix",
        family_version="v1",
        candidate_ids=(),
        train_start=date(2022, 1, 1),
        train_end=date(2024, 6, 30),
        validation_start=date(2024, 7, 1),
        validation_end=date(2025, 3, 31),
        preprocess_family="identity",
    )
    dense_series = {"801010.SI": object()}
    monkeypatch.setattr(subject, "_git_commit", lambda: producer)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda *args, **kwargs: inputs)
    monkeypatch.setattr(subject, "_family_spec", lambda *args, **kwargs: spec)
    monkeypatch.setattr(subject, "_frozen_input_identity", lambda value: {})
    monkeypatch.setattr(
        subject,
        "build_l1_training_series",
        lambda *args, **kwargs: pytest.fail("historical diagnostics must not consume the D6 calendar constructor"),
    )
    monkeypatch.setattr(
        subject,
        "build_legacy_dense_diagnostic_series",
        lambda *args, **kwargs: dense_series,
    )
    for name in (
        "diagnose_l1_seed_grid",
        "diagnose_l1_seed_grid_b1",
        "diagnose_l1_seed_grid_b3_diag02",
        "diagnose_l1_seed_grid_b3_diag04",
    ):
        monkeypatch.setattr(
            subject,
            name,
            lambda series, **kwargs: {"dense_constructor_used": series is dense_series},
        )
    monkeypatch.setattr(subject, "diagnostic_runtime_versions", lambda: {})
    monkeypatch.setattr(subject, "c008_b3_diag02_fixed_numeric_environment", lambda: {})
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: {})

    if mode == "c008":
        report = subject.diagnose_c008(request, db_prefix="TEST_")
    elif mode == "c008_b1":
        report = subject.diagnose_c008_b1(request, db_prefix="TEST_")
    elif mode == "diag02":
        report = subject.diagnose_c008_b3_diag02(request, db_prefix="TEST_")
    else:
        report = subject.diagnose_c008_b3_diag04(request, db_prefix="TEST_")
    assert report["families"][0]["diagnostic"]["dense_constructor_used"] is True


def test_p6_second_process_timeout_preserves_first_receipt_without_selection(monkeypatch, tmp_path) -> None:
    request, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    first_payload = _p6_child_payload("fresh_process_1", inputs=inputs, policy=policy)
    calls = 0

    def fake_run(command, *, check, capture_output, env, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            return SimpleNamespace(returncode=0, stdout=first_payload, stderr=b"")
        raise subject.subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    with pytest.raises(StateModelSetError, match="could not complete process=fresh_process_2"):
        subject.run_b3_p6_autocycle_l2_repeated(args, request)

    failure_path = tmp_path / "p6.fresh_process_2.failure.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["completed_process_count"] == 1
    assert len(failure["completed_process_receipt_hashes"]) == 1
    assert failure["selection_performed"] is False
    assert failure["selected_level_artifact_write_performed"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_p6_zero_return_with_invalid_receipt_persists_child_evidence(monkeypatch, tmp_path) -> None:
    request, _, _, args = _p6_parent_setup(monkeypatch, tmp_path)
    stdout = b'{"not":"canonical"}\n'
    stderr = b"child diagnostic context"
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr),
    )

    with pytest.raises(StateModelSetError, match="child receipt failed process=fresh_process_1"):
        subject.run_b3_p6_autocycle_l2_repeated(args, request)

    failure = json.loads((tmp_path / "p6.fresh_process_1.failure.json").read_text(encoding="utf-8"))
    assert failure["returncode"] == 0
    assert failure["stdout_byte_count"] == len(stdout)
    assert failure["stdout_sha256"] == subject.sha256_bytes(stdout)
    assert failure["stderr_byte_count"] == len(stderr)
    assert failure["stderr_sha256"] == subject.sha256_bytes(stderr)
    assert failure["fit_grid_completed"] is False
    assert failure["selection_performed"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_p6_parent_failure_after_two_processes_never_fabricates_selection_state(monkeypatch, tmp_path) -> None:
    _, policy, inputs, args = _p6_parent_setup(monkeypatch, tmp_path)
    for process_identity in ("fresh_process_1", "fresh_process_2"):
        receipt = json.loads(_p6_child_payload(process_identity, inputs=inputs, policy=policy))
        subject._write_diagnostic_report(subject._b3_p6_process_receipt_path(args, process_identity), receipt)

    failure = subject._build_b3_p6_parent_failure(args, StateModelSetError("D5 readback failed"))

    assert failure["verified_process_count"] == 2
    assert failure["terminal_entry_count"] == 2096
    assert failure["fit_grid_completed"] is True
    assert failure["selection_performed"] is None
    assert failure["selection_status"] == "unknown_due_parent_failure"
    assert failure["d6_performed_after_selection"] is None
    assert failure["phase2_ready"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_p6_cli_routes_only_to_level_local_executor(monkeypatch, tmp_path, capsys) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text("{}", encoding="utf-8")
    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")
    output_path = tmp_path / "p6.json"
    report = {
        "status": "blocked",
        "planned_fit_count": 2096,
        "terminal_entry_count": 2096,
        "selection_performed": True,
        "d6_performed_after_selection": False,
        "selected_level_artifact_write_performed": False,
        "ready_artifact_write_performed": False,
    }
    monkeypatch.setattr(
        subject.sys,
        "argv",
        [
            str(Path(subject.__file__).resolve()),
            "--request",
            str(request_path),
            "--output-root",
            str(tmp_path / "models"),
            "--env-file",
            str(env_path),
            "--db-env-prefix",
            "TDX_DB_DEV_",
            "--b3-p6-autocycle-l2-output",
            str(output_path),
        ],
    )
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "run_b3_p6_autocycle_l2_repeated", lambda args, request: report)
    monkeypatch.setattr(
        subject,
        "run_b3_repeated",
        lambda *args, **kwargs: pytest.fail("P6 CLI must never invoke the historical full-grid executor"),
    )

    assert subject.main() == 1

    receipt = json.loads(capsys.readouterr().out)
    assert receipt["schema_version"] == subject.B3_P6_CLI_SCHEMA
    assert receipt["planned_fit_count"] == 2096
    assert receipt["terminal_entry_count"] == 2096
    assert receipt["ready_artifact_write_performed"] is False
    assert output_path.read_bytes() == subject.canonical_json_bytes(report) + b"\n"


def _d1_args(tmp_path) -> SimpleNamespace:
    values = _remediation_args(tmp_path)
    remediation = tmp_path / "source-remediation.json"
    remediation.write_text("{}", encoding="utf-8")
    c010_a5 = tmp_path / "c010-a5-domain-partition.json"
    c010_a5.write_text("{}", encoding="utf-8")
    artifact_root = tmp_path / "model-sets"
    values.b3_remediation_diag02_output = None
    values.b3_d1_controlled_refit_output = str(artifact_root / "d1-controlled-refit.json")
    values.b3_remediation_report = str(remediation)
    values.c010_a5_domain_partition_report = str(c010_a5)
    values._b3_d1_controlled_child = False
    values.b3_process_identity = ""
    values.b3_d1_producer_commit = ""
    values.b3_d1_frozen_input_bundle = ""
    values.b3_d1_frozen_input_bundle_sha256 = ""
    values.c009_stock_fact_preflight_output = None
    values.c010_observation_eligibility_output = None
    return values


def _d1_process_payload(process_identity: str) -> bytes:
    body = {"process_identity": process_identity}
    value = {**body, "process_receipt_sha256": subject.canonical_sha256(body)}
    return subject.canonical_json_bytes(value)


def _d1_c010_a5_report() -> dict:
    def receipt(body: dict) -> dict:
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    partition = receipt(
        {
            "schema_version": "hmm_risk_c010_provider_absence_domain_partition_v1",
            "p_all_entry_count": 502,
            "p_in_entry_count": 501,
            "p_out_entry_count": 1,
        }
    )
    eligibility = receipt({"schema_version": "hmm_risk_c010_train_observation_eligibility_v2"})
    opportunity = receipt({"schema_version": "hmm_risk_c010_expected_opportunity_dates_v2"})
    body = {
        "schema_version": subject.C010_A5_DOMAIN_PARTITION_PREFLIGHT_SCHEMA,
        "status": "preflight_complete",
        "producer_commit": "a" * 40,
        "train_trading_date_count": subject.C010_APPROVED_TRAIN_TRADING_DATE_COUNT,
        "train_trading_date_sha256": subject.C010_APPROVED_TRAIN_TRADING_DATE_SHA256,
        "mapping_manifest_sha256": "4" * 64,
        "security_identity_manifest_sha256": "5" * 64,
        "provider_absence_manifest_sha256": "6" * 64,
        "provider_absence_partition_receipt": partition,
        "provider_absence_partition_receipt_sha256": partition["receipt_sha256"],
        "observation_eligibility_receipt": eligibility,
        "observation_eligibility_receipt_sha256": eligibility["receipt_sha256"],
        "expected_opportunity_receipt": opportunity,
        "expected_opportunity_receipt_sha256": opportunity["receipt_sha256"],
        "known_sw_domain_out_verified": True,
        "partition_complete": True,
        "fit_performed": False,
        "selection_performed": False,
        "d6_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return receipt(body)


def test_d1_c010_a5_authority_is_exact_and_tamper_evident(monkeypatch) -> None:
    report = _d1_c010_a5_report()
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256(report))
    monkeypatch.setattr(
        subject,
        "B3_D1_C010_A5_PARTITION_SHA256",
        report["provider_absence_partition_receipt_sha256"],
    )
    monkeypatch.setattr(subject, "B3_D1_C010_A5_MAPPING_SHA256", report["mapping_manifest_sha256"])

    assert subject._validate_b3_d1_c010_a5_authority(report) == report

    tampered = deepcopy(report)
    tampered["partition_complete"] = False
    tampered_body = {key: value for key, value in tampered.items() if key != "receipt_sha256"}
    tampered["receipt_sha256"] = subject.canonical_sha256(tampered_body)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256(tampered))
    with pytest.raises(StateModelSetError, match="approved 601-day v2 preflight"):
        subject._validate_b3_d1_c010_a5_authority(tampered)


def test_d1_current_c010_a5_authority_is_a_separate_immutable_revision(monkeypatch) -> None:
    report = _d1_c010_a5_report()
    report_identity = subject.canonical_sha256(report)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_CURRENT_REPORT_SHA256", report_identity)
    monkeypatch.setattr(
        subject,
        "B3_D1_C010_A5_CURRENT_PARTITION_SHA256",
        report["provider_absence_partition_receipt_sha256"],
    )
    monkeypatch.setattr(
        subject,
        "B3_D1_C010_A5_CURRENT_MAPPING_SHA256",
        report["mapping_manifest_sha256"],
    )

    assert subject._validate_b3_d1_c010_a5_authority(report) == report

    monkeypatch.setattr(subject, "B3_D1_C010_A5_CURRENT_MAPPING_SHA256", "9" * 64)
    with pytest.raises(StateModelSetError, match="approved 601-day v2 preflight"):
        subject._validate_b3_d1_c010_a5_authority(report)


def test_d1_historical_request_remains_v1_and_cannot_be_silently_relabelled(monkeypatch) -> None:
    request = _request()
    authority = dict(subject.B3_BLOCKER_FORMAL_AUTHORITY)
    for field in (
        "producer_commit",
        "dataset_manifest_hash",
        "mapping_manifest_hash",
        "l2_stock_fact_manifest_hash",
        "semantic_dataset_manifest_hash",
        "semantic_mapping_manifest_hash",
        "semantic_calendar_manifest_hash",
        "semantic_l2_stock_fact_manifest_hash",
        "feature_domain_policy_sha256",
    ):
        request[field] = authority[field]
    request["parent_frozen_identities"] = subject.B3_APPROVED_FROZEN_IDENTITIES
    request["train_coverage_contract_version"] = subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION
    request["train_coverage_receipt_sha256"] = "1" * 64
    historical_policy = {
        "schema_version": subject.C010_POLICY_VERSION_V1,
        "receipt_sha256": authority["feature_domain_policy_sha256"],
    }
    request["feature_domain_policy_manifest"] = historical_policy
    target_manifest = {
        "formal_producer_commit": authority["producer_commit"],
        "parameter_profile_sha256": subject.canonical_sha256(subject.formal_b3_parameter_profile()),
    }
    monkeypatch.setattr(subject, "validate_c010_policy_manifest", lambda value: dict(value))

    assert (
        subject._validate_b3_d1_historical_request_authority(request, target_manifest=target_manifest)
        == historical_policy
    )

    request["feature_domain_policy_manifest"] = {
        **historical_policy,
        "schema_version": subject.C010_POLICY_VERSION,
    }
    with pytest.raises(StateModelSetError, match="historical formal request authority is invalid"):
        subject._validate_b3_d1_historical_request_authority(request, target_manifest=target_manifest)


def test_d1_v2_migration_uses_a5_mapping_identity_and_historical_non_mapping_identities(monkeypatch) -> None:
    request = _request()
    a5_report = _d1_c010_a5_report()
    mapping = {"mapping": "a5"}
    calendar = {"calendar": "historical"}
    l2_stock_fact = {"l2": "historical"}
    a5_report["mapping_manifest_sha256"] = subject.canonical_sha256(mapping)
    historical_policy = {
        "schema_version": subject.C010_POLICY_VERSION_V1,
        "l2_stock_fact_manifest_hash": subject.canonical_sha256(l2_stock_fact),
        "calendar_manifest_hash": subject.canonical_sha256(calendar),
    }
    current_policy = {
        "schema_version": subject.C010_POLICY_VERSION,
        "receipt_sha256": "7" * 64,
        "mapping_manifest_hash": a5_report["mapping_manifest_sha256"],
        "l2_stock_fact_manifest_hash": historical_policy["l2_stock_fact_manifest_hash"],
        "calendar_manifest_hash": historical_policy["calendar_manifest_hash"],
        "security_identity_manifest_sha256": a5_report["security_identity_manifest_sha256"],
        "provider_absence_manifest_sha256": a5_report["provider_absence_manifest_sha256"],
        "provider_absence_partition_receipt": a5_report["provider_absence_partition_receipt"],
        "provider_absence_partition_receipt_sha256": a5_report["provider_absence_partition_receipt_sha256"],
        "expected_opportunity_receipt": a5_report["expected_opportunity_receipt"],
        "expected_opportunity_receipt_sha256": a5_report["expected_opportunity_receipt_sha256"],
        "eligibility_receipt": a5_report["observation_eligibility_receipt"],
        "eligibility_receipt_sha256": a5_report["observation_eligibility_receipt_sha256"],
    }
    authority = dict(subject.B3_BLOCKER_FORMAL_AUTHORITY)
    authority["calendar_manifest_hash"] = historical_policy["calendar_manifest_hash"]
    authority["l2_stock_fact_manifest_hash"] = historical_policy["l2_stock_fact_manifest_hash"]
    monkeypatch.setattr(subject, "B3_BLOCKER_FORMAL_AUTHORITY", authority)
    monkeypatch.setattr(
        subject,
        "_validate_b3_d1_historical_request_authority",
        lambda request, target_manifest: historical_policy,
    )
    monkeypatch.setattr(subject, "_validate_b3_d1_c010_a5_authority", lambda report: report)
    monkeypatch.setattr(
        subject,
        "_load_l1_source_inputs",
        lambda request, db_prefix, c010_formal: {
            "mapping_manifest": mapping,
            "dataset_manifest": {"calendar_benchmark": calendar},
            "l2_stock_fact_manifest": l2_stock_fact,
        },
    )
    monkeypatch.setattr(subject, "_c010_policy_manifest", lambda *args, **kwargs: current_policy)

    loaded_inputs, identities = subject._load_b3_d1_train_inputs(
        request,
        db_prefix="TDX_DB_",
        target_manifest={},
        c010_a5_report=a5_report,
        producer_commit="8" * 40,
    )

    assert identities["mapping_manifest_hash"] == a5_report["mapping_manifest_sha256"]
    assert identities["c010_feature_domain_policy_sha256"] == current_policy["receipt_sha256"]
    assert loaded_inputs["feature_domain_policy_sha256"] == current_policy["receipt_sha256"]

    drifted = deepcopy(a5_report)
    drifted["mapping_manifest_sha256"] = "9" * 64
    with pytest.raises(StateModelSetError, match="mapping_manifest_hash"):
        subject._load_b3_d1_train_inputs(
            request,
            db_prefix="TDX_DB_",
            target_manifest={},
            c010_a5_report=drifted,
            producer_commit="8" * 40,
        )


def test_d1_lineage_migration_normalizes_authority_identity_order_but_rejects_business_drift() -> None:
    def receipt(body: dict) -> dict:
        return {**body, "receipt_sha256": subject.canonical_sha256(body)}

    pit_authority = {
        "authority_type": "stock_universe_pit_state_and_spans",
        "identity_sha256": "1" * 64,
        "authority": {"universe_key": "frozen", "validated_status": "ready"},
    }
    mapping_authority = {
        "authority_type": "sw_index_member_and_classify_mapping",
        "identity_sha256": "2" * 64,
        "authority": {"canonical_l1_count": 31, "canonical_l2_count": 131},
    }
    approved_opportunity = receipt(
        {
            "schema_version": "hmm_risk_c010_expected_opportunity_dates_v2",
            "authority_identities": [pit_authority, mapping_authority],
        }
    )
    current_opportunity = receipt(
        {
            "schema_version": "hmm_risk_c010_expected_opportunity_dates_v2",
            "authority_identities": [
                {**mapping_authority, "identity_sha256": "0" * 64},
                {**pit_authority, "identity_sha256": "f" * 64},
            ],
        }
    )
    a5_report = _d1_c010_a5_report()
    a5_report["expected_opportunity_receipt"] = approved_opportunity
    a5_report["expected_opportunity_receipt_sha256"] = approved_opportunity["receipt_sha256"]
    current_policy = {
        "provider_absence_partition_receipt": a5_report["provider_absence_partition_receipt"],
        "expected_opportunity_receipt": current_opportunity,
        "eligibility_receipt": a5_report["observation_eligibility_receipt"],
    }

    migration = subject._build_b3_d1_c010_a5_lineage_migration_receipt(
        current_policy,
        a5_report,
        producer_commit="8" * 40,
    )

    opportunity_pair = migration["receipt_pairs"]["expected_opportunity"]
    assert opportunity_pair["approved_semantic_payload_sha256"] == opportunity_pair["current_semantic_payload_sha256"]

    drifted_policy = deepcopy(current_policy)
    drifted_policy["expected_opportunity_receipt"]["authority_identities"][0]["authority"]["canonical_l2_count"] = 130
    with pytest.raises(StateModelSetError, match="expected_opportunity business payload drifted"):
        subject._build_b3_d1_c010_a5_lineage_migration_receipt(
            drifted_policy,
            a5_report,
            producer_commit="8" * 40,
        )


def test_d1_controlled_parent_runs_exactly_two_fresh_processes_with_fixed_threads(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256({}))
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "_b3_d1_frozen_authority", lambda *args: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: {
            "current_authority": {"current_profile_eligible": True, "receipt_sha256": "a" * 64},
            "historical_reference": {"receipt_sha256": "b" * 64},
            "frozen_input_bundle_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        subject,
        "_parse_b3_d1_child_payload",
        lambda payload, process_identity, producer_commit: {
            "process_identity": process_identity,
            "producer_commit": producer_commit,
        },
    )
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_run(command, *, check, capture_output, env, timeout):
        process_identity = command[command.index("--b3-process-identity") + 1]
        assert command[command.index("--c010-a5-domain-partition-report") + 1] == str(
            tmp_path / "c010-a5-domain-partition.json"
        )
        assert command[command.index("--b3-d1-current-authority-sha256") + 1] == "a" * 64
        assert command[command.index("--b3-d1-historical-reference-sha256") + 1] == "b" * 64
        assert command[command.index("--b3-d1-frozen-input-bundle-sha256") + 1] == "c" * 64
        assert command[command.index("--b3-d1-frozen-input-bundle") + 1].endswith(
            "d1-controlled-refit.frozen-input.json"
        )
        calls.append(
            (
                process_identity,
                {
                    key: env[key]
                    for key in (
                        "OMP_NUM_THREADS",
                        "OPENBLAS_NUM_THREADS",
                        "MKL_NUM_THREADS",
                        "VECLIB_MAXIMUM_THREADS",
                        "NUMEXPR_NUM_THREADS",
                    )
                },
            )
        )
        return SimpleNamespace(returncode=0, stdout=_d1_process_payload(process_identity), stderr=b"")

    monkeypatch.setattr(subject.subprocess, "run", fake_run)
    monkeypatch.setattr(
        subject,
        "build_b3_d1_refit02_report",
        lambda first, second, producer_commit: {
            "first": first["process_identity"],
            "second": second["process_identity"],
            "producer_commit": producer_commit,
        },
    )

    report = subject.run_b3_d1_controlled_repeated(args)

    assert calls == [
        ("fresh_process_1", {key: "1" for key in calls[0][1]}),
        ("fresh_process_2", {key: "1" for key in calls[1][1]}),
    ]
    assert report == {"first": "fresh_process_1", "second": "fresh_process_2", "producer_commit": "d" * 40}


def test_d1_current_a5_parent_selects_v8_report_and_current_source_authority(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    current_report = {"revision": "current-a5"}
    monkeypatch.setattr(
        subject,
        "B3_D1_C010_A5_CURRENT_REPORT_SHA256",
        subject.canonical_sha256(current_report),
    )
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(
        subject,
        "_load_json_mapping",
        lambda path, label: current_report if label == "C-010-A5 domain-partition report" else {},
    )
    monkeypatch.setattr(subject, "_b3_d1_frozen_authority", lambda *values: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *values, **kwargs: {
            "current_authority": {"current_profile_eligible": False, "receipt_sha256": "a" * 64},
            "historical_reference": {"receipt_sha256": "b" * 64},
            "frozen_input_bundle_sha256": "c" * 64,
        },
    )

    def fake_not_applicable(authority, reference, *, producer_commit, schema_version):
        assert schema_version == subject.B3_D1_REFIT03_REPORT_SCHEMA_VERSION
        assert args.b3_d1_source_authority == subject.B3_D1_CURRENT_SOURCE_AUTHORITY
        return {"status": "not_applicable", "producer_commit": producer_commit}

    monkeypatch.setattr(subject, "build_b3_d1_refit02_not_applicable_report", fake_not_applicable)
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *values, **kwargs: pytest.fail("not-applicable current authority must not spawn a child"),
    )

    assert subject.run_b3_d1_controlled_repeated(args) == {
        "status": "not_applicable",
        "producer_commit": "d" * 40,
    }


def test_d1_existing_frozen_bundle_requires_explicit_hash_before_source_or_children(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    bundle_path = Path(args.b3_d1_controlled_refit_output).with_name("d1-controlled-refit.frozen-input.json")
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: pytest.fail("missing bundle hash must fail before source loading"),
    )
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("missing bundle hash must fail before child startup"),
    )

    with pytest.raises(subject.D1InactiveDimensionError, match="requires --b3-d1-frozen-input-bundle-sha256"):
        subject.run_b3_d1_controlled_repeated(args)


def test_d1_verified_frozen_bundle_replay_does_not_query_mutable_source(monkeypatch, tmp_path) -> None:
    bundle_path = tmp_path / "frozen-input.json"
    bundle_path.write_text("{}\n", encoding="utf-8")
    treatment = object()
    harness = object()
    preprocess = {"family": "identity"}
    bundle = {
        "bundle_sha256": "f" * 64,
        "parsed_roles": {
            "treatment_19d": treatment,
            "harness_identity20_positive": harness,
        },
        "preprocess": preprocess,
        "current_policy_sha256": "d" * 64,
        "current_authority_sha256": "a" * 64,
        "historical_reference_sha256": "b" * 64,
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "1" * 40)
    monkeypatch.setattr(
        subject,
        "_b3_d1_frozen_authority",
        lambda *args: {
            "numeric_environment": {},
            "profiles": {
                subject.B3_D1_TREATMENT_SECTOR: {"train_input_manifest": {}},
                subject.B3_D1_CONTROL_SECTOR: {"train_input_manifest": {}},
            },
        },
    )
    monkeypatch.setattr(subject, "c008_b3_diag04_fixed_numeric_environment", lambda: {})
    monkeypatch.setattr(subject, "_validate_b3_d1_numeric_environment_authority", lambda *args: None)
    monkeypatch.setattr(subject, "derive_b3_blocker_target_manifest", lambda report: {})
    monkeypatch.setattr(subject, "_validate_b3_d1_historical_request_authority", lambda *args, **kwargs: {})
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "validate_b3_d1_refit03_frozen_input_bundle", lambda value: bundle)
    monkeypatch.setattr(
        subject,
        "_load_b3_d1_train_inputs",
        lambda *args, **kwargs: pytest.fail("verified frozen replay must not query mutable source"),
    )
    monkeypatch.setattr(
        subject,
        "build_b3_d1_refit02_current_a5_authority",
        lambda **kwargs: {"receipt_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        subject,
        "build_b3_d1_refit02_historical_reference_receipt",
        lambda **kwargs: {"receipt_sha256": "b" * 64},
    )

    prepared = subject._prepare_b3_d1_refit02_authority(
        _request(),
        {},
        {},
        {},
        {},
        db_prefix="TDX_DB_",
        producer_commit="1" * 40,
        frozen_input_bundle_path=bundle_path,
        expected_frozen_input_bundle_sha256="f" * 64,
    )

    assert prepared["treatment_item"] is treatment
    assert prepared["harness_item"] is harness
    assert prepared["frozen_input_bundle_sha256"] == "f" * 64


def test_d1_controlled_child_payload_must_be_canonical_and_bound_to_process_identity(monkeypatch) -> None:
    monkeypatch.setattr(
        subject,
        "validate_b3_d1_refit02_process_receipt",
        lambda value, expected_process_identity, expected_producer_commit: value,
    )
    parsed = subject._parse_b3_d1_child_payload(
        _d1_process_payload("fresh_process_1"),
        process_identity="fresh_process_1",
        producer_commit="d" * 40,
    )
    assert parsed["process_identity"] == "fresh_process_1"

    with pytest.raises(StateModelSetError, match="receipt identity"):
        subject._parse_b3_d1_child_payload(
            _d1_process_payload("fresh_process_2"),
            process_identity="fresh_process_1",
            producer_commit="d" * 40,
        )
    with pytest.raises(StateModelSetError, match="canonical JSON"):
        subject._parse_b3_d1_child_payload(
            _d1_process_payload("fresh_process_1") + b"\n",
            process_identity="fresh_process_1",
            producer_commit="d" * 40,
        )


def test_d1_refit02_parent_returns_not_applicable_without_spawning_children(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256({}))
    current = {"current_profile_eligible": False, "receipt_sha256": "a" * 64}
    historical = {"receipt_sha256": "b" * 64}
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "_b3_d1_frozen_authority", lambda *values: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: {
            "current_authority": current,
            "historical_reference": historical,
            "frozen_input_bundle_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        subject,
        "build_b3_d1_refit02_not_applicable_report",
        lambda authority, reference, producer_commit: {
            "status": "not_applicable",
            "authority": authority,
            "reference": reference,
            "producer_commit": producer_commit,
        },
    )
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("not-applicable preflight must not spawn a child"),
    )

    report = subject.run_b3_d1_controlled_repeated(args)

    assert report["status"] == "not_applicable"
    assert report["authority"] == current
    assert report["reference"] == historical


def test_d1_refit02_child_rejects_parent_authority_drift_before_first_fit(monkeypatch) -> None:
    current = {"current_profile_eligible": True, "receipt_sha256": "a" * 64}
    historical = {"receipt_sha256": "b" * 64}
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: {
            "current_authority": current,
            "historical_reference": historical,
        },
    )
    monkeypatch.setattr(
        subject,
        "run_b3_d1_refit02_process",
        lambda **kwargs: pytest.fail("authority mismatch must fail before the first fit"),
    )

    with pytest.raises(subject.D1InactiveDimensionError, match="differs from the parent preflight"):
        subject.prepare_b3_d1_controlled_pass(
            _request(),
            {},
            {},
            {},
            {},
            db_prefix="TDX_DB_",
            process_identity="fresh_process_1",
            producer_commit="d" * 40,
            expected_current_authority_sha256="c" * 64,
            expected_historical_reference_sha256="b" * 64,
            frozen_input_bundle_path=Path("frozen-input.json"),
            expected_frozen_input_bundle_sha256="f" * 64,
        )


def test_main_d1_controlled_mode_persists_diagnostic_without_selection_or_model_writes(
    monkeypatch, tmp_path, capsys
) -> None:
    args = _d1_args(tmp_path)
    body = {
        "schema_version": subject.B3_D1_REFIT02_REPORT_SCHEMA_VERSION,
        "diagnostic_contract": "C-008-B3-D1-REFIT-03-COVARIANCE-DIAG-01",
        "producer_commit": "d" * 40,
        "status": "diagnostic_complete",
        "mechanism_assessment": "inconclusive",
        "covariance_pattern_assessment": "inactive_coordinate_pattern_consistent",
        "d5_compatibility_evidence_ready": False,
        "attempt_count": 48,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    report = {**body, "receipt_sha256": subject.canonical_sha256(body)}
    monkeypatch.setattr(subject, "parse_args", lambda: args)
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "run_b3_d1_controlled_repeated", lambda value: report)

    def persist_validated_report(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")
        return subject.canonical_sha256(value)

    monkeypatch.setattr(subject, "write_b3_d1_controlled_refit_report", persist_validated_report)

    assert subject.main() == 0

    persisted = json.loads((tmp_path / "model-sets" / "d1-controlled-refit.json").read_text(encoding="utf-8"))
    receipt = json.loads(capsys.readouterr().out)
    assert persisted == report
    assert receipt["attempt_count"] == 48
    assert receipt["selection_performed"] is False
    assert receipt["model_write_performed"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_main_d1_controlled_mode_requires_explicit_c010_a5_authority(monkeypatch, tmp_path, capsys) -> None:
    args = _d1_args(tmp_path)
    args.c010_a5_domain_partition_report = None
    monkeypatch.setattr(subject, "parse_args", lambda: args)
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())

    assert subject.main() == 1
    assert "--c010-a5-domain-partition-report" in capsys.readouterr().err


def test_main_d1_controlled_mode_persists_typed_failure_without_fake_attempt_count(
    monkeypatch, tmp_path, capsys
) -> None:
    args = _d1_args(tmp_path)
    monkeypatch.setattr(subject, "parse_args", lambda: args)
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_git_commit", lambda: "d" * 40)
    child_failure = subject._b3_d1_child_failure_receipt(
        process_identity="fresh_process_1",
        producer_commit="d" * 40,
        returncode=1,
        stdout=b"",
        stderr=b"",
        fit_budget_completion_unknown=True,
        error=subject.D1InactiveDimensionError(
            "hmm_risk_model_inactive_dimension_authority_mismatch",
            "frozen authority drift",
        ),
    )
    monkeypatch.setattr(
        subject,
        "run_b3_d1_controlled_repeated",
        lambda value: (_ for _ in ()).throw(
            subject.B3D1ControlledProcessError(
                "hmm_risk_model_inactive_dimension_authority_mismatch",
                "frozen authority drift",
                completed_processes=[],
                failed_process_receipt=child_failure,
            )
        ),
    )

    assert subject.main() == 1

    persisted = json.loads((tmp_path / "model-sets" / "d1-controlled-refit.json").read_text(encoding="utf-8"))
    assert persisted["status"] == "diagnostic_failed"
    assert persisted["mechanism_assessment"] == "inconclusive"
    assert persisted["mechanism_assessment_reason_codes"] == ["hmm_risk_model_inactive_dimension_authority_mismatch"]
    assert persisted["attempt_count"] == 0
    assert persisted["completed_process_count"] == 0
    assert persisted["process_receipts"] == []
    assert persisted["failed_process_receipt"] is None
    assert persisted["source_authority"] == subject.B3_D1_SOURCE_AUTHORITY
    assert persisted["fit_budget_completion_unknown"] is False
    assert persisted["selection_performed"] is False
    assert persisted["ready_artifact_write_performed"] is False
    assert "frozen authority drift" in capsys.readouterr().err


def test_d1_frozen_authority_rejects_rehashed_nonapproved_remediation_before_projection(monkeypatch) -> None:
    body = {"formal_source_commit": "0" * 40}
    remediation = {**body, "receipt_sha256": subject.canonical_sha256(body)}
    monkeypatch.setattr(subject, "_validate_b3_d1_c010_a5_authority", lambda report: report)
    monkeypatch.setattr(subject, "validate_b3_remediation_authorities", lambda formal, blocker: None)

    with pytest.raises(StateModelSetError, match="approved canonical artifact"):
        subject._b3_d1_frozen_authority({}, {}, remediation, {})


def test_d1_report_path_must_be_repo_external_and_contained_by_artifact_root(tmp_path) -> None:
    args = _d1_args(tmp_path)
    assert subject._resolve_b3_d1_report_path(args) == (tmp_path / "model-sets" / "d1-controlled-refit.json").resolve()

    args.b3_d1_controlled_refit_output = str(tmp_path / "outside.json")
    with pytest.raises(StateModelSetError, match="contained"):
        subject._resolve_b3_d1_report_path(args)

    args.output_root = str(subject.ROOT / "tmp" / "d1-artifacts")
    args.b3_d1_controlled_refit_output = str(subject.ROOT / "tmp" / "d1-artifacts" / "report.json")
    with pytest.raises(StateModelSetError, match="outside the repository"):
        subject._resolve_b3_d1_report_path(args)


def test_d1_second_child_failure_preserves_first_process_and_typed_reason(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256({}))
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "_b3_d1_frozen_authority", lambda *values: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: {
            "current_authority": {"current_profile_eligible": True, "receipt_sha256": "a" * 64},
            "historical_reference": {"receipt_sha256": "b" * 64},
            "frozen_input_bundle_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        subject,
        "_parse_b3_d1_child_payload",
        lambda payload, process_identity, producer_commit: {
            "process_identity": process_identity,
            "producer_commit": producer_commit,
            "attempt_count": 16,
        },
    )
    calls = 0

    def fake_run(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return SimpleNamespace(returncode=0, stdout=b"{}", stderr=b"")
        error = {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "status": "failed",
            "error_type": "D1InactiveDimensionError",
            "error": "numeric environment drift",
            "reason_code": "hmm_risk_model_inactive_dimension_authority_mismatch",
        }
        return SimpleNamespace(returncode=1, stdout=b"", stderr=json.dumps(error).encode("utf-8"))

    monkeypatch.setattr(subject.subprocess, "run", fake_run)

    with pytest.raises(subject.B3D1ControlledProcessError) as captured:
        subject.run_b3_d1_controlled_repeated(args)

    assert captured.value.reason_code == "hmm_risk_model_inactive_dimension_authority_mismatch"
    assert captured.value.completed_processes == [
        {"process_identity": "fresh_process_1", "producer_commit": "d" * 40, "attempt_count": 16}
    ]
    assert captured.value.failed_process_receipt["process_identity"] == "fresh_process_2"
    assert captured.value.failed_process_receipt["producer_commit"] == "d" * 40
    assert captured.value.failed_process_receipt["source_authority"] == subject.B3_D1_SOURCE_AUTHORITY
    assert captured.value.failed_process_receipt["reason_code"] == (
        "hmm_risk_model_inactive_dimension_authority_mismatch"
    )


def test_d1_parent_finalize_failure_preserves_both_completed_processes(monkeypatch, tmp_path) -> None:
    args = _d1_args(tmp_path)
    monkeypatch.setattr(subject, "B3_D1_C010_A5_REPORT_SHA256", subject.canonical_sha256({}))
    monkeypatch.setattr(subject, "_load_request", lambda path: _request())
    monkeypatch.setattr(subject, "_load_json_mapping", lambda path, label: {})
    monkeypatch.setattr(subject, "_b3_d1_frozen_authority", lambda *values: {})
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject,
        "_prepare_b3_d1_refit02_authority",
        lambda *args, **kwargs: {
            "current_authority": {"current_profile_eligible": True, "receipt_sha256": "a" * 64},
            "historical_reference": {"receipt_sha256": "b" * 64},
            "frozen_input_bundle_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        subject,
        "_parse_b3_d1_child_payload",
        lambda payload, process_identity, producer_commit: {
            "process_identity": process_identity,
            "producer_commit": producer_commit,
            "attempt_count": 16,
        },
    )
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b"{}", stderr=b""),
    )
    monkeypatch.setattr(
        subject,
        "build_b3_d1_refit02_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subject.D1InactiveDimensionError(
                "hmm_risk_model_inactive_dimension_contract_invalid",
                "parent readback mismatch",
            )
        ),
    )

    with pytest.raises(subject.B3D1ControlledProcessError) as captured:
        subject.run_b3_d1_controlled_repeated(args)

    assert len(captured.value.completed_processes) == 2
    assert [value["process_identity"] for value in captured.value.completed_processes] == [
        "fresh_process_1",
        "fresh_process_2",
    ]
    assert captured.value.failed_process_receipt["process_identity"] == "parent_finalize"
    assert captured.value.failed_process_receipt["fit_budget_completion_unknown"] is False
