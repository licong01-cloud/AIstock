"""Frozen C2 parent-lane capacity study inputs; no execution or account engine."""

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Annotated, Any, Literal

from pydantic import Field

from . import natural_period_context as context
from . import natural_period_results as natural
from .contracts import Contract, Policy, SafeId, Sha256, digest, payload
from .period_context import read, require
from .replay import FileRef
from .research import freeze
from .store import IntradayStore

STUDY_ID = "C2_PARENT_LANE_CAPACITY_PROBE_V1"
DESIGN = {"commit": "59b1b7ea0edeff932d8a85fcebdb0917871d52db",
          "path": "docs/architecture/position_timing_dispatch_coupling_f1_20261113.md",
          "git_blob": "3890ced18ab30a1982a6bb4c795232be7b52e5e5"}
TRADE_DATES = tuple("2026-10-" + d for d in
                    ("08", "09", "12", "13", "14", "15", "16", "19", "20", "21", "22", "23", "26", "27", "28", "29", "30"))
FROZEN = {
    "analysis_spec": "9b623ffcf15a8635c9741b0f6b0c98187b90b4e1cc145c91685ae80121db2a66",
    "candidate_manifest": "2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958",
    "selection_inventory": "d55b4fff01b79f55c223e35b6f185200410290ce9921468f68b90b6aada6b6fc",
    "price_templates": "5a080214d341996031bd8d1d9636ef3abf314acf48f0b9894d070577db4cda1a",
    "cost_policy": "d9f8c97ea4593206b0509fcfeb5046a20925c84c618a9a3d3061f7945d9f3041",
    "opening": "4e6c1704dad9784b390b85af93086557314bee199422f89f6ef6e9d2a3786eef",
    "producer_request": "d25efd225b5f5260cbd213563fb51203de300496bea226203fcf2aaa0e78856b",
}
BASELINE_ID = "lsnatural_918ee66460fb7689bff74509891690c8c293712d"
C2_SOURCE = "70e459c569020f09880d91c1f16d43e9e5d951f3"
UNCERTAINTY = {"block_trading_days": 5, "draws": 2000, "seed": 20261113, "family_size": 1}
FUNDING_EVIDENCE = {"path": "/mnt/x/AIstock_temp/localsim-dispatch-capacity-20261113/contract_gap_evidence_r1.json",
    "sha256": "3e88e736be12a8d918e0919b2ebd237a7af6f24971aa34a7d5d83c43de146a1e"}
Commit = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Dates = Annotated[tuple[str, ...], Field(min_length=17, max_length=17)]


class CapacityPreparationSpec(Contract):
    schema_version: Literal["position_timing_dispatch_capacity_prepare_spec_v1"] = "position_timing_dispatch_capacity_prepare_spec_v1"
    baseline_analysis_spec: FileRef


class CapacityAccount(Contract):
    account_id: SafeId
    arm_id: Literal["C2", "C2L"]
    opening: FileRef
    days: tuple[FileRef, ...] = Field(min_length=17, max_length=17)


class CapacityProducerRequest(Contract):
    schema_version: Literal["localsim_dispatch_capacity_request_v1"] = "localsim_dispatch_capacity_request_v1"
    producer: Literal["LOCALSIM"] = "LOCALSIM"
    producer_source_commit: Commit
    source_identity: dict[str, Any]
    research_plan: FileRef
    account_id: SafeId
    common_opening: FileRef
    initial_opening: FileRef
    trade_dates: Dates
    candidate_manifest: FileRef
    selection_inventory: FileRef
    price_templates: FileRef
    cost_policy: FileRef
    package_id: SafeId
    package_manifest_sha256: Sha256
    capacity_control_sha256: Sha256
    execution_contract: dict[str, Any]
    new_capital_added_cny: Literal["0"] = "0"
    jan_september_account_days_recomputed: Literal[0] = 0
    identity: Sha256


class CapacityFacts(Contract):
    schema_version: Literal["position_timing_dispatch_capacity_facts_v1"] = "position_timing_dispatch_capacity_facts_v1"
    research_plan: FileRef
    producer_request: FileRef
    source_inspection: FileRef
    producer_source_commit: Commit
    study_identity: Sha256
    source_batch_verified: Literal[True]
    outcomes_read_stage: Literal["AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"]
    nav_observation_scope: Literal["ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS"]
    accounts: tuple[CapacityAccount, ...] = Field(min_length=2, max_length=2)


class CapacitySourceInspection(Contract):
    schema_version: Literal["localsim_dispatch_capacity_source_inspection_v1"] = "localsim_dispatch_capacity_source_inspection_v1"
    stage: Literal["COMPLETE_SOURCE_BATCH_INSPECTED"]
    research_plan: FileRef
    producer_request: FileRef
    producer_source_commit: Commit
    study_identity: Sha256
    accounts_sha256: Sha256
    capacity_control_sha256: Sha256
    execution_contract_sha256: Sha256
    baseline_source_identity_sha256: Sha256
    legacy_serial_regression: FileRef
    economic_compatibility: Literal["PRODUCER_ATTESTED_BOUND_LEGACY_REGRESSION"]
    source_refs: tuple[FileRef, ...] = Field(min_length=1, max_length=50000)


class CapacityArtifactRetry(Contract):
    schema_version: Literal["localsim_dispatch_capacity_artifact_retry_v1"] = "localsim_dispatch_capacity_artifact_retry_v1"
    status: Literal["EXACT_ARTIFACT_RETRY_VERIFIED"]
    research_plan: FileRef
    facts: FileRef
    producer_request: FileRef
    source_inspection: FileRef
    verified_new_account_days: Literal[17]
    reused_baseline_account_days: Literal[17]
    recomputed_account_days: Literal[0]
    new_economic_commits: Literal[0]
    jan_september_account_days_recomputed: Literal[0] = 0


class CapacityAnalysisSpec(Contract):
    schema_version: Literal["position_timing_dispatch_capacity_analysis_spec_v1"] = "position_timing_dispatch_capacity_analysis_spec_v1"
    research_plan: FileRef
    facts: FileRef
    producer_artifact_retry: FileRef


def capacity_control():
    return {"schema_version": "position_timing_parent_lane_capacity_control_v1", "control_id": STUDY_ID,
        "baseline_capacity": "ACCOUNT_SINGLE_OPERATOR", "capacity": "PARENT_PLAN_INDEPENDENT_OPERATORS",
        "lane_key": ["account_id", "parent_plan_id"], "reaction_delay_seconds": 60,
        "minimum_action_interval_seconds": 60, "maximum_actions_per_lane_dispatch": 1,
        "queue_order": "CANCEL_THEN_SELL_THEN_BUY_READY_FIFO", "one_inflight_per_lane": True,
        "same_clock_matching_passes": 1, "shared_cash_and_positions": True,
        "recovery_unit": "ATOMIC_COMPLETED_DAY", "production_default_changed": False}


def frozen_policy():
    return Policy.model_validate({"schema_version": "position_timing_intraday_policy_v1",
        "policy_id": "BOUNDED_REVERSION_V1", "source_commit": C2_SOURCE, "algorithm_version": "20261023.v1",
        "allow_split": False, "mean_bars": 5, "wait_trading_minutes": 15, "cooldown_trading_minutes": 5,
        "max_batches": 3, "decision_ttl_seconds": 90, "closure_delay_seconds": 5, "max_lag_slots": 1,
        "session_version": "CN_A_CONTINUOUS_WINDOW_V1"})


def funding_boundary():
    """Both arms retain native cash semantics, not a new reservation engine."""
    return {"schema_version": "position_timing_native_funding_boundary_v1",
        "boundary_id": "NATIVE_COMMITTED_LEDGER_AT_MATCH_V1", "applies_to_arms": ["C2", "C2L"],
        "predispatch_cash_reservation": "NONE",
        "decision_account_authority": "VISIBLE_EXECUTION_VIEW_AND_CONFIRMED_FEEDBACK",
        "quantity_authority": "EXISTING_NATIVE_TARGET_REMAINING_AND_LOT_RULES",
        "matching_cash_authority": "ONE_SHARED_NATIVE_COMMITTED_LEDGER_INCLUDING_FEES",
        "committed_sell_proceeds_before_feedback": "AVAILABLE_TO_ALREADY_SUBMITTED_BUY_AT_NATIVE_MATCH",
        "unmatched_or_future_sell_proceeds_available": False, "leverage_allowed": False,
        "decision_uses_invisible_feedback": False,
        "matching_order": "NATIVE_SELL_BEFORE_BUY_THEN_SYMBOL_ORDER_ID",
        "cash_shortfall": "EXISTING_AFFORDABLE_FILL_AND_CAPITAL_LIMITED_REMAINDER",
        "partial_fill_and_cancel": "NATIVE_LIFECYCLE_NO_SYNTHETIC_RESERVE_OR_RELEASE",
        "native_source_commit": "db774c64dca0973c3cd6b29cdebadc0865055913",
        "native_source_symbols": ["localsim.runtime.LocalSimBackend._fit_buy_fills_to_available_cash",
            "localsim.timing.TimingExecution.step", "localsim.human_portfolio.run_human_portfolio_day"],
        "amends_design": "position_timing_dispatch_coupling_f1_20261113.md#funding-boundary",
        "owner_gap_evidence": FUNDING_EVIDENCE}


def implementation_identity():
    return natural.implementation_identity() | {"intraday/dispatch_coupling_research.py": hashlib.sha256(
        Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}


def hash_tree(value):
    """Source implementation identities contain content hashes, not labels."""
    return isinstance(value, dict) and bool(value) and all(hash_tree(v) if isinstance(v, dict) else
        isinstance(v, str) and re.fullmatch(r"[a-f0-9]{64}", v) is not None for v in value.values())


def _baseline(spec):
    require(spec.baseline_analysis_spec.sha256 == FROZEN["analysis_spec"], "CAPACITY_FROZEN_SPEC")
    original = natural.NaturalAnalysisSpec.model_validate(read(spec.baseline_analysis_spec))
    c, facts, refs = natural.source_preflight(original)
    r, p = c["request"], c["preflight"]
    require(tuple(p["trade_dates"]) == TRADE_DATES and r["native_target_count"] == 850 and
        r["package_id"] == "pkg_c8d3d51fb6d24be89cfb3a50c30c80e4" and
        r["manifest_sha256"] == "5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9" and
        all(r[k]["sha256"] == FROZEN[k] for k in
            ("candidate_manifest", "selection_inventory", "cost_policy")) and
        facts.price_templates.sha256 == FROZEN["price_templates"] and
        facts.producer_request.sha256 == FROZEN["producer_request"], "CAPACITY_FROZEN_POPULATION")
    accounts = [a for a in facts.accounts if a.arm_id == "C2"]
    require(len(accounts) == 1 and accounts[0].account_id == BASELINE_ID and
        accounts[0].opening.sha256 == FROZEN["opening"], "CAPACITY_BASELINE_IDENTITY")
    account = accounts[0]
    opening = next(a for a in c["openings"] if a["account_id"] == account.account_id)
    human = {"schema_version": "localsim_human_operation_policy_v1", "policy_id": "localsim_human_operation_v1",
        "operator_scope": "ACCOUNT", "reaction_delay_seconds": 60, "minimum_action_interval_seconds": 60,
        "maximum_actions_per_dispatch": 1, "queue_order": "CANCEL_THEN_SELL_THEN_BUY_READY_FIFO",
        "allow_split": False, "automatic_reprice": False, "new_order_requires_known_account": True,
        "random_response_model": "disabled"}
    request = read(facts.producer_request)
    require(opening["operation"]["human_policy"] == human and request["timing_source_commit"] == C2_SOURCE and
        hash_tree(request["source_identity"]) and
        r["execution_contract"]["human"]["capacity"] == "ACCOUNT_SINGLE_OPERATOR" and
        digest(frozen_policy()) == "8e456686452cc2e53b0e922cf915b53308e1b40f17358c2fff41b656228d8e79",
        "CAPACITY_BASE_POLICY")
    return c, facts, account, original, request, context.pins(refs, spec)


def _producer_contracts():
    models = (CapacityProducerRequest, CapacityFacts, CapacitySourceInspection, CapacityArtifactRetry, CapacityAnalysisSpec)
    return {"schema_version": "position_timing_dispatch_capacity_producer_contracts_v2",
        "plan_schema_version": "position_timing_dispatch_capacity_plan_v2",
        "funding_boundary": funding_boundary(), "funding_boundary_sha256": digest(funding_boundary()),
        "models": {m.__name__: m.model_json_schema() for m in models},
        "opening": {"schema_version": "localsim_dispatch_capacity_opening_v1",
            "source": "PLAN_COMMON_OPENING", "arm_id": "C2L", "account_id": "PLAN_CANDIDATE_ACCOUNT_ID",
            "operation": "PLAN_OPERATION", "research_plan": "PLAN_FILEREF",
            "portfolio_id_replacement": "ONLY_IDS_EQUAL_TO_COMMON_OPENING_ACCOUNT_ID",
            "other_fields": "EXACTLY_PRESERVE_ALL_FIELDS_MARKS_AND_HISTORICAL_ANCHORS"},
        "new_day": {"schema_version": "localsim_dispatch_capacity_day_v1", "required_fields": [
            "account_id", "study_identity", "trade_date", "rule", "delay_seconds", "operation", "terminal_as_of",
            "terminal_nav_cny", "cash_cny", "positions", "fees_cny", "nav_observations", "parents", "view_counts",
            "human_operation_state", "capacity_control_sha256", "execution_contract_sha256"],
            "statistics_fields": "SAME_NATIVE_PERIOD_NAV_PARENT_AND_ACTION_PROJECTIONS_NO_QUEUE_RESTORE"},
        "parent_lane_state": {"schema_version": "localsim_parent_lane_operation_state_v1",
            "required_fields": ["account_id", "capacity_control_sha256", "actions", "lanes", "state_sha256"],
            "lane_fields": ["parent_plan_id", "action_ids", "last_action_at"],
            "state_sha256": "CANONICAL_SHA256_EXCLUDING_STATE_SHA256_FIELD"},
        "legacy_serial_regression": {"required_fields": ["status", "producer_source_commit", "baseline_producer_request",
            "baseline_source_identity_sha256", "base_execution_contract_sha256", "funding_boundary_sha256",
            "account_days_recomputed", "checks"],
            "status": "PASSED", "account_days_recomputed": 0, "checks": "NONEMPTY_LIST_OF_DIRECT_TEST_IDENTIFIERS",
            "evidence_scope": "PRODUCER_ATTESTED_DIRECT_REGRESSION_NOT_INDEPENDENT_OLD_ACCOUNT_REPLAY"}}


def _body(spec, source_commit, c, facts, account, original, request, refs, contract_ref):
    require(re.fullmatch(r"[a-f0-9]{40}", source_commit) is not None, "CAPACITY_SOURCE_COMMIT")
    r, control, funding = c["request"], capacity_control(), funding_boundary()
    opening = next(a for a in c["openings"] if a["account_id"] == account.account_id)
    implementation = implementation_identity()
    new_id = "lslane_" + digest([spec, source_commit, implementation, control, account.account_id])[:40]
    refs = context.pins(refs, contract_ref, funding)
    return payload({"schema_version": "position_timing_dispatch_capacity_plan_v2", "research_id": STUDY_ID,
        "approved_design": DESIGN, "preparation_spec": spec, "timing_source_commit": source_commit,
        "implementation": implementation, "baseline_analysis_spec": spec.baseline_analysis_spec,
        "source_delivery": original.source_delivery, "preflight": facts.preflight,
        "preparation_request": c["preflight"]["preparation_request"],
        "baseline_producer_request": facts.producer_request, "baseline_study_identity": facts.study_identity,
        "baseline_source_identity": request["source_identity"], "baseline_account": account,
        "common_opening": account.opening, "candidate_account_id": new_id,
        "profiles": [{"arm_id": "C2", "capacity": "ACCOUNT_SINGLE_OPERATOR", "reuse": True},
                     {"arm_id": "C2L", "capacity": control["capacity"], "reuse": False}],
        "policy": frozen_policy(), "policy_sha256": digest(frozen_policy()),
        "capacity_control": control, "capacity_control_sha256": digest(control),
        "funding_boundary": funding, "funding_boundary_sha256": digest(funding),
        "base_operation": opening["operation"], "base_execution_contract": r["execution_contract"],
        "operation": {"schema_version": "localsim_dispatch_capacity_operation_v1",
            "base_operation": opening["operation"], "capacity_control": control, "capacity_control_sha256": digest(control)},
        "execution_contract": {"schema_version": "localsim_dispatch_capacity_execution_contract_v1",
            "base_execution_contract": r["execution_contract"], "capacity_control": control,
            "capacity_control_sha256": digest(control),
            "funding_boundary": funding, "funding_boundary_sha256": digest(funding)},
        "trade_dates": TRADE_DATES, "native_target_count": 850, "package": c["package"],
        "package_id": r["package_id"], "package_manifest_sha256": r["manifest_sha256"],
        "candidate_manifest": r["candidate_manifest"], "selection_inventory": r["selection_inventory"],
        "price_manifest": original.price_manifest, "price_templates": facts.price_templates, "cost_policy": r["cost_policy"],
        "new_account_days": 17, "reused_baseline_account_days": 17,
        "comparisons": [{"arm_id": "C2L", "baseline_arm_id": "C2", "primary": True}],
        "uncertainty": UNCERTAINTY, "source_refs": refs,
        "producer_contracts": contract_ref,
        "hypothesis_origin": "EXPLORATORY_AFTER_OBSERVED_BASELINE_NOT_SEALED_HOLDOUT",
        "c2l_outcomes_read": False, "account_replay_started": False,
        "quantity_matched": False, "pure_timing_attribution": False, "economic_confirmation": False,
        "database_accessed": False, "network_accessed": False, "runtime_action_performed": False})


def prepare(spec: CapacityPreparationSpec, source_commit: str, timing_root):
    require(re.fullmatch(r"[a-f0-9]{40}", source_commit) is not None, "CAPACITY_SOURCE_COMMIT")
    values = _baseline(spec)
    contracts = _producer_contracts()
    contract_ref = freeze(IntradayStore(timing_root).path("dispatch_capacity_contracts", digest(contracts)), contracts)
    body = _body(spec, source_commit, *values, contract_ref)
    context.verify(context.pins(body["source_refs"]))
    path = IntradayStore(timing_root).path("dispatch_capacity_plans", digest(body))
    existed = path.exists()
    result = freeze(path, body | {"identity": digest(body)})
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "plan": payload(result),
        "new_account_days": 17, "reused_baseline_account_days": 17, "c2l_outcomes_read": False,
        "account_replay_started": False, "source_file_count": len(body["source_refs"])}


def inspect_dispatch_capacity_plan(ref: FileRef):
    """Public read-only seam. Validate full content pins, never run an account."""
    ref = FileRef.model_validate(ref)
    value = read(ref, "position_timing_dispatch_capacity_plan_v2")
    spec = CapacityPreparationSpec.model_validate(value["preparation_spec"])
    contract_ref = FileRef.model_validate(value["producer_contracts"])
    require(read(contract_ref) == _producer_contracts(), "CAPACITY_PRODUCER_CONTRACTS")
    values = _baseline(spec)
    body = _body(spec, value["timing_source_commit"], *values, contract_ref)
    require(value == body | {"identity": digest(body)}, "CAPACITY_PLAN_IDENTITY")
    context.verify(context.pins(body["source_refs"], ref))
    return {"integrity": "VERIFIED", "plan_ref": payload(ref), "plan": value,
        "source_file_count": len(body["source_refs"])+1, "new_account_days": 17, "reused_baseline_account_days": 17,
        "c2l_outcomes_read": False, "account_replay_started": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "inspect"))
    parser.add_argument("--input", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--timing-root")
    args = parser.parse_args()
    ref = FileRef(path=args.input, sha256=args.sha256)
    if args.command == "prepare":
        if not args.source_commit or not args.timing_root:
            parser.error("prepare requires --source-commit and --timing-root")
        result = prepare(CapacityPreparationSpec.model_validate(read(ref)), args.source_commit, args.timing_root)
    else:
        inspected = inspect_dispatch_capacity_plan(ref)
        result = {k: v for k, v in inspected.items() if k != "plan"} | {"identity": inspected["plan"]["identity"]}
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
