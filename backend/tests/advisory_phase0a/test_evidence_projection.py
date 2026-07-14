from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import UTC, date, datetime

import pytest

from backend.services.advisory_phase0a.evidence_projection import (
    canonical_evidence_json_sha256,
    validate_projected_daily_evidence_v2,
    validate_projected_historical_evidence_v2,
)
from backend.services.advisory_phase0a.evidence_projection_postgres import (
    AdvisoryPostgresEvidenceProjection,
    AdvisoryPostgresEvidenceSnapshot,
    AdvisoryProjectionReadOnlyError,
    _SQL,
)
from backend.services.advisory_phase0a.resolvers import _selection_run_content_hash
from backend.services.selection_center.models import SelectionRun
from backend.services.selection_center.prospective_evidence import DailySelectionEvidenceV2Payload


class _Cursor:
    def __init__(self, *, readonly: str, isolation: str = "repeatable read") -> None:
        self.readonly = readonly
        self.isolation = isolation
        self.executed: list[str] = []
        self._last = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, _params=None) -> None:
        self._last = str(sql)
        self.executed.append(self._last)

    def fetchone(self):
        if "current_setting('transaction_read_only')" in self._last:
            return {"transaction_read_only": self.readonly, "transaction_isolation": self.isolation}
        return None


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_instance = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_instance


def test_projection_registry_contains_fixed_nonlocking_selects_only() -> None:
    for query in _SQL.values():
        normalized = " ".join(query.strip().upper().split())
        assert normalized.startswith("SELECT ")
        assert " FOR UPDATE" not in normalized
        assert " FOR SHARE" not in normalized
    assert "review_policy_json" not in _SQL["program"]
    assert "review_policy" in _SQL["program"]


def test_projection_requires_postgresql_read_only_confirmation() -> None:
    cursor = _Cursor(readonly="on")

    @contextmanager
    def factory():
        yield _Connection(cursor)

    with AdvisoryPostgresEvidenceProjection(factory).snapshot():
        pass
    assert any("REPEATABLE READ READ ONLY" in statement for statement in cursor.executed)

    off = _Cursor(readonly="off")

    @contextmanager
    def off_factory():
        yield _Connection(off)

    with pytest.raises(AdvisoryProjectionReadOnlyError):
        with AdvisoryPostgresEvidenceProjection(off_factory).snapshot():
            pass

    wrong_isolation = _Cursor(readonly="on", isolation="read committed")

    @contextmanager
    def wrong_isolation_factory():
        yield _Connection(wrong_isolation)

    with pytest.raises(AdvisoryProjectionReadOnlyError):
        with AdvisoryPostgresEvidenceProjection(wrong_isolation_factory).snapshot():
            pass


def _valid_dse_payload() -> dict:
    decision_date = date(2026, 7, 10)
    target_date = date(2026, 7, 13)
    observed_at = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)
    base_config = {"binding": "unit"}
    request_override = {}
    date_enforced = {"trade_date": decision_date.isoformat()}
    selection_config = {"selection": {"top_k": 20}}
    package_config = {
        "runtime_profile": selection_config,
        "selection_artifact_config": {"cutoff_date": decision_date.isoformat()},
    }

    stages = {
        "alpha_raw": {
            "stage": "alpha_raw",
            "status": "COMPLETE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "candidates": [],
            "exclusions": [],
        },
        "hmm_adjusted": {
            "stage": "hmm_adjusted",
            "status": "NOT_APPLICABLE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "candidates": [],
            "exclusions": [],
        },
        "risk_policy_adjusted": {
            "stage": "risk_policy_adjusted",
            "status": "COMPLETE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "candidates": [],
            "exclusions": [],
        },
        "selection_effective": {
            "stage": "selection_effective",
            "status": "COMPLETE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "candidates": [],
            "exclusions": [],
        },
        "advisory_model": {
            "stage": "advisory_model",
            "status": "NOT_APPLICABLE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "candidates": [],
            "exclusions": [],
        },
    }
    universe_layers = [
        {
            "layer": layer,
            "status": "NOT_APPLICABLE",
            "input_count": 0,
            "output_count": 0,
            "excluded_count": 0,
            "exclusion_reason_counts": {},
        }
        for layer in (
            "listed_universe",
            "seasoned_universe",
            "pit_st_delist_risk_universe",
            "package_eligible_universe",
            "risk_can_buy_universe",
            "tradability_industry_universe",
        )
    ]
    return {
        "schema_version": "daily_selection_evidence_v2",
        "evidence_contract": {
            "capture_mode": "PROSPECTIVE",
            "capture_status": "COMPLETE",
            "execution_origin": "ADVISORY_RUN",
            "prospective_eligible": True,
            "research_scope": "HISTORICAL_RESEARCH_ONLY",
            "execution_prohibited": True,
            "market_data_scope": "DB_HISTORICAL",
            "serializer_version": "unit-json-v1",
            "producer_code_release_id": "unit-release",
            "producer_code_release_hash": "1" * 64,
            "captured_at": observed_at,
            "reason_codes": [],
        },
        "decision_clock": {
            "decision_as_of_trade_date": decision_date,
            "selection_as_of_trade_date": decision_date,
            "target_trade_date": target_date,
            "effective_entry_trade_date": target_date,
            "score_trade_date": decision_date,
            "reference_price_trade_date": decision_date,
            "requested_selection_as_of_trade_date": decision_date,
            "requested_cutoff_date": decision_date,
            "effective_cutoff_date": decision_date,
            "decision_cutoff_ts": observed_at,
            "data_available_at": observed_at,
            "decision_generated_at": observed_at,
            "timezone": "Asia/Shanghai",
            "calendar_version": "market.trading_calendar.v1",
            "calendar_hash": "2" * 64,
            "calendar_source": "market.trading_calendar",
            "is_immediately_previous_trade_date": True,
            "immediate_after_data_refresh": True,
        },
        "point_in_time_context": {"cutoff_date": decision_date},
        "runtime_profile": selection_config,
        "runtime_profile_binding": {"profile_version_id": "profile-v1"},
        "selection_artifact_config": {"cutoff_date": decision_date},
        "phase0a_effective_config_chain": {
            "binding_base_config": base_config,
            "binding_base_config_hash": canonical_evidence_json_sha256(base_config),
            "binding_base_source_id": "binding-unit",
            "binding_base_source_version": "v1",
            "binding_base_source_hash": "3" * 64,
            "binding_base_available_at": observed_at,
            "binding_base_effective_from_trade_date": decision_date,
            "request_override_config": request_override,
            "request_override_hash": canonical_evidence_json_sha256(request_override),
            "date_enforced_config": date_enforced,
            "date_enforced_version": "v1",
            "date_enforced_hash": canonical_evidence_json_sha256(date_enforced),
            "selection_normalized_config": selection_config,
            "selection_normalized_config_hash": canonical_evidence_json_sha256(selection_config),
            "package_effective_config": package_config,
            "package_effective_config_hash": canonical_evidence_json_sha256(package_config),
            "runtime_profile_version_id": "profile-v1",
            "runtime_profile_hash": "4" * 64,
            "selection_adapter_version": "adapter-v1",
            "query_template_version": "query-v1",
            "provider_version": "provider-v1",
            "code_release_id": "unit-release",
            "code_release_hash": "5" * 64,
            "overridden_field_paths_by_layer": {"request_override": []},
            "final_effective_config_hash": canonical_evidence_json_sha256(package_config),
        },
        "phase0a_hmm_metadata": {"enabled": False},
        "phase0a_risk_policy_metadata": {"enabled": False},
        "phase0a_universe_evidence": {
            "layers": universe_layers,
            "package_cohort": {"status": "PERSISTED"},
        },
        "phase0a_package_lineage": {"package_id": "pkg", "manifest_sha256": "6" * 64},
        "phase0a_asset_closure": [],
        "phase0a_source_evidence": [
            {
                "source_role": "market_history",
                "dataset_id": "market.kline_daily_raw",
                "row_count": 1,
                "content_hash": "7" * 64,
                "available_at": observed_at,
            }
        ],
        "phase0a_candidate_lineage": {
            "selection_score_artifact_id": "artifact",
            "selection_score_artifact_sha256": "a" * 64,
            "selection_score_artifact_payload_sha256": "b" * 64,
        },
        "phase0a_stage_evidence": stages,
        "candidate_outcome": "VALID_NO_CANDIDATE",
        "selected_candidates": [],
        "excluded_candidates": [],
    }


def test_projection_dse_validation_is_never_weaker_than_producer_contract() -> None:
    payload = _valid_dse_payload()
    assert DailySelectionEvidenceV2Payload.model_validate(payload)
    assert validate_projected_daily_evidence_v2(payload) is not None
    assert validate_projected_historical_evidence_v2(payload) is not None
    assert canonical_evidence_json_sha256({"value": 1}) == canonical_evidence_json_sha256({"value": 1})

    invalid_payloads = []
    missing_stage = deepcopy(payload)
    missing_stage["phase0a_stage_evidence"].pop("risk_policy_adjusted")
    invalid_payloads.append(missing_stage)
    invalid_clock = deepcopy(payload)
    invalid_clock["decision_clock"]["target_trade_date"] = invalid_clock["decision_clock"]["decision_as_of_trade_date"]
    invalid_payloads.append(invalid_clock)
    invalid_config = deepcopy(payload)
    invalid_config["phase0a_effective_config_chain"]["package_effective_config_hash"] = "f" * 64
    invalid_payloads.append(invalid_config)
    invalid_universe = deepcopy(payload)
    invalid_universe["phase0a_universe_evidence"]["layers"].reverse()
    invalid_payloads.append(invalid_universe)
    invalid_source = deepcopy(payload)
    invalid_source["phase0a_source_evidence"][0].pop("available_at")
    invalid_payloads.append(invalid_source)

    for invalid in invalid_payloads:
        with pytest.raises(ValueError):
            DailySelectionEvidenceV2Payload.model_validate(invalid)
        assert validate_projected_daily_evidence_v2(invalid) is None
        assert validate_projected_historical_evidence_v2(invalid) is None


def test_projection_rejects_old_dse_subset_even_with_artifact_lineage() -> None:
    payload = {
        "schema_version": "daily_selection_evidence_v2",
        "evidence_contract": {"prospective_eligible": True},
        "phase0a_candidate_lineage": {
            "selection_score_artifact_id": "artifact",
            "selection_score_artifact_sha256": "a" * 64,
            "selection_score_artifact_payload_sha256": "b" * 64,
        },
    }
    assert validate_projected_daily_evidence_v2(payload) is None


def test_historical_projection_rejects_malformed_nested_source_or_candidate_rows() -> None:
    payload = _valid_dse_payload()
    assert validate_projected_historical_evidence_v2(payload) is not None

    malformed_source = {**payload, "phase0a_source_evidence": ["not-a-source-receipt"]}
    assert validate_projected_historical_evidence_v2(malformed_source) is None

    malformed_candidate = {**payload, "candidate_outcome": "CANDIDATES_PRESENT", "selected_candidates": ["not-a-candidate"]}
    assert validate_projected_historical_evidence_v2(malformed_candidate) is None


def test_projection_reads_selection_run_from_normalized_selection_tables() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def execute(self, sql, _params=None) -> None:
            self.last_sql = str(sql)

        def fetchone(self):
            if self.last_sql == _SQL["selection_run"]:
                return {
                    "run_id": "sel_1",
                    "mode": "single_package",
                    "trade_date": date(2026, 7, 1),
                    "data_source": "DB_HISTORICAL",
                    "package_ids": ["pkg_1"],
                    "runtime_config": {"top_k": 20},
                    "status": "SUCCEEDED",
                    "valid_no_candidate": False,
                    "no_candidate_reason": None,
                    "error_json": None,
                    "created_at": datetime(2026, 7, 1, 8, 0, tzinfo=UTC),
                    "completed_at": None,
                }
            return None

        def fetchall(self):
            if self.last_sql == _SQL["selection_run_package_results"]:
                return [
                    {
                        "package_id": "pkg_1",
                        "manifest_sha256": "a" * 64,
                        "symbol": "000001.SZ",
                        "score": 0.7,
                        "rank": 1,
                        "target_weight": None,
                        "target_quantity": None,
                        "reference_price": 10.5,
                        "component_scores": {
                            "alpha": 0.7,
                            "selection_result_display": {
                                "stock_name": "Unit Bank",
                                "selection_entry_price": 10.2,
                                "selection_entry_price_source": "daily_close",
                                "selection_entry_price_time": "2026-07-01T15:00:00+08:00",
                            },
                            "selection_price_guidance": {
                                "signal_ref_price": 10.1,
                                "entry_band": {"green": {"max_price": 10.3}},
                                "stop_loss_zone": {"hard_stop_price": 9.5},
                                "guidance_status": "RULE_DEFAULT",
                                "price_guard_policy_sha256": "b" * 64,
                            },
                        },
                        "reason": "selected",
                        "suggested_entry_price_band": None,
                        "suggested_stop_loss_zone": None,
                        "guidance_status": None,
                        "price_guard_policy_sha256": None,
                    }
                ]
            if self.last_sql == _SQL["selection_run_aggregate_results"]:
                return []
            if self.last_sql == _SQL["selection_run_manifest_lineage"]:
                return [{"package_id": "pkg_1", "manifest_sha256": "a" * 64}]
            if self.last_sql == _SQL["selection_run_excluded_results"]:
                return [
                    {
                        "package_id": "pkg_1",
                        "manifest_sha256": "a" * 64,
                        "symbol": "000002.SZ",
                        "score": 0.5,
                        "raw_rank": 2,
                        "reason": "risk_excluded",
                        "source": "risk_policy",
                        "context": {"policy": "unit"},
                    }
                ]
            return []

    run = AdvisoryPostgresEvidenceSnapshot(Cursor()).get_run("sel_1")

    assert "FROM selection.run" in _SQL["selection_run"]
    assert "selection.selection_run" not in _SQL["selection_run"]
    assert run.manifest_sha256_by_package == {"pkg_1": "a" * 64}
    candidate = run.package_results["pkg_1"][0]
    assert candidate.component_scores["alpha"] == 0.7
    assert candidate.stock_name == "Unit Bank"
    assert candidate.reference_price == 10.2
    assert candidate.signal_ref_price == 10.1
    assert candidate.suggested_entry_price_band == {"green": {"max_price": 10.3}}
    assert candidate.suggested_stop_loss_zone == {"hard_stop_price": 9.5}
    assert candidate.price_guard_policy_sha256 == "b" * 64
    assert run.excluded_results["pkg_1"][0].reason == "risk_excluded"
    producer_run = SelectionRun.model_validate(run.model_dump(mode="python"))
    assert producer_run.model_dump(mode="json") == run.model_dump(mode="json")
    assert _selection_run_content_hash(run) == _selection_run_content_hash(producer_run)


def test_projection_reads_selection_artifact_by_exact_immutable_id() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def execute(self, sql, _params=None) -> None:
            self.last_sql = str(sql)

        def fetchone(self):
            if self.last_sql != _SQL["selection_artifact_by_id"]:
                return None
            return {
                "artifact_id": "artifact_1",
                "package_id": "pkg_1",
                "manifest_sha256": "a" * 64,
                "trade_date": date(2026, 7, 1),
                "data_source": "DB_HISTORICAL",
                "runtime_config_hash": "b" * 64,
                "scores_json": [],
                "artifact_sha256": "c" * 64,
                "score_count": 0,
                "universe_count": 0,
                "top_score_symbol": None,
                "status": "SUCCEEDED",
                "metadata": {},
                "artifact_contract_version": "selection_score_artifact_v2",
                "artifact_payload_sha256": "d" * 64,
                "artifact_input_context_hash": "e" * 64,
                "source_revision_set_hash": "f" * 64,
                "asset_closure_hash": "0" * 64,
                "created_at": None,
            }

    artifact = AdvisoryPostgresEvidenceSnapshot(Cursor()).get_selection_score_artifact("artifact_1")

    assert "WHERE artifact_id = %s" in _SQL["selection_artifact_by_id"]
    assert artifact.artifact_id == "artifact_1"


def test_projection_rejects_ambiguous_historical_receipt_reference() -> None:
    class Snapshot(AdvisoryPostgresEvidenceSnapshot):
        def __init__(self) -> None:
            pass

        def _one(self, query_id, _params):
            if query_id == "historical_receipt_by_receipt_id":
                return {"receipt_id": "receipt_a"}
            if query_id == "historical_receipt_by_batch_id":
                return {"receipt_id": "receipt_b"}
            raise AssertionError(f"unexpected query: {query_id}")

    with pytest.raises(AdvisoryProjectionReadOnlyError, match="ambiguous"):
        Snapshot().get_historical_receipt("ambiguous")
