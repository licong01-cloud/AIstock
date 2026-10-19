from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.localsim_replay import local_sim_calendar_snapshot_sha256
from backend.services.simulation_runtime.localsim_replay_recovery import (
    ECONOMIC_TABLES, RECOVERY_KEY, LocalSimReplayRecoverRequestV1, LocalSimReplayRecoveryService,
    recovery_rebuild_receipt, require_no_economic_effects, validate_replay_recovery_check,
)
from backend.services.simulation_runtime.models import canonical_json_sha256, SimulationBrokerBackend, SimulationDailyRunStatus
from backend.services.simulation_runtime.scheduler import SimulationLifecycleScheduler
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError


NOW = datetime(2026, 10, 15, tzinfo=UTC)
DAYS = (date(2026, 1, 20), date(2026, 1, 21))


def _snapshot():
    return {
        "job": {"replay_job_id": "replay-original", "version": 8, "status": "FAILED_TERMINAL",
                "simulation_account_id": "account", "binding_id": "binding", "release_id": "release",
                "next_trade_date": "2026-01-21", "completed_trade_date": "2026-01-20",
                "start_trade_date": "2026-01-20", "end_trade_date": "2026-01-21",
                "historical_source_id": "market.kline_minute_raw.v1", "historical_source_sha256": "a" * 64,
                "calendar_snapshot_sha256": local_sim_calendar_snapshot_sha256(DAYS),
                "day_engine_contract_id": "simulation_daily_engine_v1", "failure_code": "BrokerConnectivityError",
                "failure_context": {"trade_date": "2026-01-21"}, "live_release_id": None,
                "live_binding_id": None, "activation_trade_date": None},
        "account": {"account_id": "account", "status": "ACTIVE"},
        "account_in_flight_runs": [],
        "binding": {"binding_hash": "b", "strategy_id": "account", "broker_backend": "local_sim",
                    "release_id": "release", "release_hash": "r", "package_id": "package", "manifest_sha256": "m"},
        "release": {"release_id": "release", "release_hash": "r", "package_id": "package", "manifest_sha256": "m"},
        "run": {"run_id": "original-run", "status": "FAILED_RETRYABLE", "broker_backend": "local_sim",
                "strategy_id": "account", "binding_id": "binding", "release_id": "release",
                "binding_hash": "b", "release_hash": "r", "package_id": "package", "manifest_sha256": "m",
                "trade_date": "2026-01-21", "execution_plan_id": "old-plan", "execution_plan_hash": "p",
                "selection_evidence_id": "old-evidence", "selection_artifact_hash": "e",
                "run_payload_json": {"submit_failure": {"type": "BrokerConnectivityError",
                    "stage": "LOCAL_SIM_SUBMIT_FAILED", "context": {
                        "cause": "market.stk_limit pre_close is required and cannot be derived",
                        "source": "DB_HISTORICAL", "symbol": "001205.SZ", "trade_date": "2026-01-21"}}}},
        "plan": {"plan_hash": "p", "binding_id": "binding", "target_trade_date": "2026-01-21",
                 "selection_evidence_id": "old-evidence", "selection_evidence_hash": "e"},
        "evidence_hash": "e", "economic_counts": {k: 0 for k in ECONOMIC_TABLES},
    }


def _service(snapshot, *, market_error=None):
    calls = []

    def load(**kwargs):
        calls.append(kwargs)
        if market_error:
            raise market_error
        return SimpleNamespace(symbol=kwargs["symbol"], trade_date=kwargs["trade_date"],
                               source=MinuteDataSource.DB_HISTORICAL,
                               market_context={"prev_close": 15.30}, minute_bars=[object()])

    source = SimpleNamespace(historical_source_id="market.kline_minute_raw.v1", historical_source_sha256="a" * 64,
                             trading_days=DAYS, current_trading_date=NOW.date())
    service = LocalSimReplayRecoveryService(
        repository=SimpleNamespace(get_replay_recovery_snapshot=lambda _: deepcopy(snapshot)),
        source_authority=SimpleNamespace(resolve=lambda **_: source),
        market_provider=SimpleNamespace(load_symbol_input=load), clock=lambda: NOW,
    )
    return service, calls


def _preselection_snapshot():
    snapshot = _snapshot()
    path = "/mnt/x/AIstock_temp/LOCALSIM/" + "a" * 64 + "/lsselection_" + "b" * 32 + "/node/" + "c" * 32 + "/path-witness.txt"
    command = ["wsl", "-d", "Ubuntu", "--", "cat", path]
    snapshot["job"]["failure_code"] = "TimeoutExpired"
    snapshot["job"]["failure_context"]["message"] = f"Command '{command}' timed out after 20 seconds"
    snapshot["binding"]["binding_id"] = "binding"
    snapshot.update(run=None, plan=None, evidence_hash=None,
                    preselection_absence={"run_id": "simrun_" + "d" * 16, "account_day_runs": []})
    return snapshot


def test_no_run_timeout_recovers_only_after_native_zero_effect_and_path_readback(monkeypatch):
    from backend.services.simulation_runtime import localsim_selection_inference as transport

    calls = []
    monkeypatch.setattr(transport, "probe_localsim_wsl_witness", lambda **kw: calls.append(kw) or {"witness_sha256": "f" * 64})
    snapshot = _preselection_snapshot()
    original = deepcopy(snapshot)
    service, market_calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert check.ready and check.run_id is None and check.native_input["recovery_phase"] == "PRESELECTION"
    assert check.native_input["candidate_run_id"] == snapshot["preselection_absence"]["run_id"]
    assert not market_calls and snapshot == original
    assert service.inspect("replay-original").snapshot_sha256 == check.snapshot_sha256
    assert calls[0]["run_id"] == "lsselection_" + "b" * 32
    assert str(calls[0]["witness"]).replace("\\", "/").startswith("X:/AIstock_temp/LOCALSIM/")


@pytest.mark.parametrize("section,key,value,reason", [
    ("job", "failure_code", "UnexpectedError", "FAILURE_UNSUPPORTED"),
    ("job", "status", "FAILED_RETRYABLE", "FAILURE_UNSUPPORTED"),
    ("binding", "binding_id", "other", "FROZEN_IDENTITY_CONFLICT"),
    ("binding", "release_hash", "other", "FROZEN_IDENTITY_CONFLICT"),
    ("account", "status", "CLOSED", "FROZEN_IDENTITY_CONFLICT"),
    ("preselection_absence", "account_day_runs", [{"run_id": "other"}], "PRESELECTION_ABSENCE_UNPROVEN"),
    ("preselection_absence", "run_id", "unknown", "PRESELECTION_ABSENCE_UNPROVEN"),
    ("economic_counts", "fills", 1, "ECONOMIC_FACTS_PRESENT"),
    ("economic_counts", "orders", -1, "ECONOMIC_FACTS_PRESENT"),
    ("job", "completed_trade_date", None, "SOURCE_IDENTITY_CONFLICT"),
])
def test_no_run_recovery_does_not_generalize_to_unknown_failure_or_effects(section, key, value, reason):
    snapshot = _preselection_snapshot()
    snapshot[section][key] = value
    service, _ = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_" + reason]


@pytest.mark.parametrize("message", [
    "unrelated timeout", "Command '['wsl', '-d', 'Ubuntu', 'bash', '-lc', 'secret']' timed out after 20 seconds",
    "Command '['wsl', '-d', 'Ubuntu', '--', 'cat', '/mnt/c/private/file']' timed out after 20 seconds",
])
def test_no_run_recovery_refuses_unrecognized_transport_commands(message):
    snapshot = _preselection_snapshot()
    snapshot["job"]["failure_context"]["message"] = message
    service, _ = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_FAILURE_UNSUPPORTED"]


def test_no_run_recovery_remains_blocked_when_wsl_probe_still_fails(monkeypatch):
    from backend.services.simulation_runtime import localsim_selection_inference as transport

    def blocked(**_):
        raise DataUnavailableError("unavailable", context={"reason_code": "LOCALSIM_WSL_X_PATH_TIMEOUT"})

    monkeypatch.setattr(transport, "probe_localsim_wsl_witness", blocked)
    service, _ = _service(_preselection_snapshot())
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_WSL_X_PATH_TIMEOUT"]


def test_repaired_native_input_is_ready_without_editing_original_job_or_day():
    snapshot = _snapshot()
    before = deepcopy(snapshot)
    service, calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert check.ready and check.native_input["prev_close"] == 15.30
    assert check.version == 8 and check.run_id == "original-run"
    assert calls == [{"symbol": "001205.SZ", "trade_date": DAYS[1], "source": MinuteDataSource.DB_HISTORICAL,
                      "min_bars": 1, "require_suspend_status": True}]
    assert snapshot == before
    assert service.inspect("replay-original").snapshot_sha256 == check.snapshot_sha256


def _missing_minute_snapshot():
    snapshot = _snapshot()
    snapshot["run"]["run_payload_json"]["submit_failure"]["context"]["cause"] = (
        "historical DB returned no minute bars"
    )
    return snapshot


def test_repaired_missing_minute_uses_original_native_source_without_changing_any_facts():
    snapshot = _missing_minute_snapshot()
    original = deepcopy(snapshot)
    service, calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert check.ready and check.reason_codes == []
    assert check.native_input == {"symbol": "001205.SZ", "trade_date": "2026-01-21",
                                "source": "DB_HISTORICAL", "prev_close": 15.30, "bar_count": 1}
    assert calls == [{"symbol": "001205.SZ", "trade_date": DAYS[1], "source": MinuteDataSource.DB_HISTORICAL,
                      "min_bars": 1, "require_suspend_status": True}]
    assert snapshot == original
    assert service.inspect("replay-original").snapshot_sha256 == check.snapshot_sha256


@pytest.mark.parametrize("error", [
    DataUnavailableError("historical DB returned no minute bars"),
    DataUnavailableError("no authoritative pre_close", context={"reason_code": "STK_LIMIT_PRE_CLOSE_INVALID"}),
])
def test_missing_minute_recovery_keeps_real_input_gaps_blocked(error):
    snapshot = _missing_minute_snapshot()
    service, calls = _service(snapshot, market_error=error)
    check = service.inspect("replay-original")
    assert not check.ready and check.reason_codes == [error.context.get("reason_code", "DataUnavailableError")]
    assert len(calls) == 1 and check.native_input == {}


@pytest.mark.parametrize("field,value", [
    ("source", "TDX_REALTIME"), ("source", "DATASET_FILES"),
    ("cause", "unknown minute error"), ("cause", "insufficient minute bars for requested execution context"),
    ("cause", []), ("cause", {}),
    ("trade_date", "2026-01-20"), ("symbol", ""),
])
def test_minute_recovery_does_not_accept_other_sources_causes_or_failure_identity(field, value):
    snapshot = _missing_minute_snapshot()
    snapshot["run"]["run_payload_json"]["submit_failure"]["context"][field] = value
    service, calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert not check.ready and not calls


@pytest.mark.parametrize("table", ECONOMIC_TABLES)
def test_minute_recovery_refuses_all_existing_economic_effects(table):
    snapshot = _missing_minute_snapshot()
    snapshot["economic_counts"][table] = 1
    service, calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert check.reason_codes == ["LOCALSIM_REPLAY_RECOVERY_ECONOMIC_FACTS_PRESENT"] and not calls


def test_minute_recovery_rechecks_native_input_on_every_inspection():
    snapshot = _missing_minute_snapshot()
    service, _ = _service(snapshot)
    first = service.inspect("replay-original")
    assert first.ready

    def unavailable(**_):
        raise DataUnavailableError("historical DB returned no minute bars")

    service.market_provider.load_symbol_input = unavailable
    second = service.inspect("replay-original")
    assert not second.ready and second.snapshot_sha256 != first.snapshot_sha256


def test_ready_minute_recovery_json_has_an_endpoint_owned_workflow_semantic_contract():
    from scripts.aistock_issue_workflow import _business_smoke_semantic_contract

    service, _ = _service(_missing_minute_snapshot())
    payload = service.inspect("replay-original").model_dump(mode="json")
    contract, validator = _business_smoke_semantic_contract(
        "/api/v1/simulation-runtime/localsim/replays/replay-original/recovery")
    assert contract == "localsim_replay_recovery"
    assert validator(payload)[0] == "passed"


@pytest.mark.parametrize("fault", [
    "old_http_ok", "blocked", "effects", "missing_effect_count", "wrong_day", "realtime",
    "zero_bars", "bool_bars", "nan_price", "zero_price", "no_snapshot", "wrong_version_type",
])
def test_recovery_post_restart_semantic_rejects_false_positive_transport_success(fault):
    service, _ = _service(_missing_minute_snapshot())
    payload = service.inspect("replay-original").model_dump(mode="json")
    if fault == "old_http_ok":
        payload = {"ok": True}
    elif fault == "blocked":
        payload.update(ready=False, reason_codes=["DataUnavailableError"])
    elif fault == "effects":
        payload["economic_counts"]["fills"] = 1
    elif fault == "missing_effect_count":
        payload["economic_counts"].pop("orders")
    elif fault == "wrong_day":
        payload["native_input"]["trade_date"] = "2026-01-20"
    elif fault == "realtime":
        payload["native_input"]["source"] = "TDX_REALTIME"
    elif fault == "zero_bars":
        payload["native_input"]["bar_count"] = 0
    elif fault == "bool_bars":
        payload["native_input"]["bar_count"] = True
    elif fault == "nan_price":
        payload["native_input"]["prev_close"] = "NaN"
    elif fault == "zero_price":
        payload["native_input"]["prev_close"] = 0
    elif fault == "no_snapshot":
        payload["snapshot_sha256"] = ""
    else:
        payload["version"] = "8"
    assert validate_replay_recovery_check(payload)[0] == "failed"


@pytest.mark.parametrize("activity,reason", [
    ("missing", "LOCALSIM_REPLAY_RECOVERY_ACCOUNT_ACTIVITY_UNKNOWN"),
    (None, "LOCALSIM_REPLAY_RECOVERY_ACCOUNT_ACTIVITY_UNKNOWN"),
    ({}, "LOCALSIM_REPLAY_RECOVERY_ACCOUNT_ACTIVITY_UNKNOWN"),
    ([{"run_id": "different-own-run"}], "LOCALSIM_REPLAY_RECOVERY_ACCOUNT_RUN_IN_FLIGHT"),
])
def test_recovery_refuses_unknown_or_in_flight_target_account_activity(activity, reason):
    snapshot = _snapshot()
    if activity == "missing":
        snapshot.pop("account_in_flight_runs")
    else:
        snapshot["account_in_flight_runs"] = activity
    before = deepcopy(snapshot)
    service, calls = _service(snapshot)
    check = service.inspect("replay-original")
    assert not check.ready and check.reason_codes == [reason]
    assert calls == [] and snapshot == before


def test_recovery_refuses_an_account_identity_different_from_the_activity_scope():
    snapshot = _snapshot()
    snapshot["account"]["account_id"] = "another-account"
    service, _ = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_FROZEN_IDENTITY_CONFLICT"]


def test_true_missing_pre_close_stays_blocked_including_legal_suspension():
    error = DataUnavailableError("no authoritative pre_close", context={"reason_code": "STK_LIMIT_PRE_CLOSE_INVALID"})
    service, _ = _service(_snapshot(), market_error=error)
    check = service.inspect("replay-original")
    assert not check.ready and check.reason_codes == ["STK_LIMIT_PRE_CLOSE_INVALID"]
    assert not check.native_input  # No previous mark, cost or made-up quote is substituted.


@pytest.mark.parametrize("table", ECONOMIC_TABLES)
def test_any_actual_economic_fact_blocks_even_when_broker_called_is_absent(table):
    snapshot = _snapshot()
    snapshot["economic_counts"][table] = 1
    service, calls = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_ECONOMIC_FACTS_PRESENT"]
    assert not calls


@pytest.mark.parametrize("section,field,value,reason", [
    ("job", "status", "CANCELLED", "STATE_INVALID"),
    ("job", "live_binding_id", "live", "STATE_INVALID"),
    ("job", "completed_trade_date", None, "SOURCE_IDENTITY_CONFLICT"),
    ("job", "historical_source_sha256", "wrong", "SOURCE_IDENTITY_CONFLICT"),
    ("job", "calendar_snapshot_sha256", "wrong", "SOURCE_IDENTITY_CONFLICT"),
    ("run", "status", "SUCCEEDED", "DAILY_RUN_INVALID"),
    ("run", "binding_hash", "wrong", "FROZEN_IDENTITY_CONFLICT"),
    ("run", "trade_date", "2026-01-20", "FROZEN_IDENTITY_CONFLICT"),
    ("binding", "manifest_sha256", "wrong", "FROZEN_IDENTITY_CONFLICT"),
    ("plan", "selection_evidence_hash", "wrong", "FROZEN_IDENTITY_CONFLICT"),
    ("job", "failure_code", "UnexpectedError", "FAILURE_UNSUPPORTED"),
])
def test_recovery_rejects_other_days_states_identities_and_failures(section, field, value, reason):
    snapshot = _snapshot()
    snapshot[section][field] = value
    service, _ = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_" + reason]


@pytest.mark.parametrize("patch", [
    {"broker_called": True}, {"broker_called": "false"}, {"submitted_intents": 1},
    {"local_sim_projection_outbox_v1": {"status": "PENDING"}},
    {"local_sim_execution_states_v1": {"state": "ACTIVE"}}, {"broker_order_handles": ["handle"]},
    {"broker_side_effect_state": "UNKNOWN"}, {"failed_intents": 1}, {"failed_intents": "unknown"},
    {"qmt_batch_id": "unexpected-cross-backend-evidence"}, {"reconcile_after_submit": {"pending": True}},
])
def test_unprojected_or_unknown_broker_effects_are_not_discarded(patch):
    snapshot = _snapshot()
    snapshot["run"]["run_payload_json"].update(patch)
    service, _ = _service(snapshot)
    assert service.inspect("replay-original").reason_codes == ["LOCALSIM_REPLAY_RECOVERY_SIDE_EFFECTS_UNPROVEN"]


def test_live_retry_lease_blocks_recovery_but_current_scheduler_claim_allows_rebuild():
    payload = {"simulation_scheduler_retry_claims_v1": {"claims": {
        "retry": {"lease_until": (NOW + timedelta(minutes=1)).isoformat()}}}}
    counts = {k: 0 for k in ECONOMIC_TABLES}
    with pytest.raises(InvalidStateTransitionError, match="claim"):
        require_no_economic_effects(payload, counts, now=NOW)
    require_no_economic_effects(payload, counts, now=NOW, check_retry_claims=False)


def test_original_plan_rebuild_requires_hashed_lineage_and_authoritative_second_check():
    row = _snapshot()["run"]
    row["trade_date"] = DAYS[1]
    row["status"] = SimulationDailyRunStatus.FAILED_RETRYABLE
    receipt = {"schema_version": RECOVERY_KEY, "run_id": row["run_id"], "binding_id": row["binding_id"],
               "release_id": row["release_id"], "trade_date": DAYS[1].isoformat(),
               "plan_id": row["execution_plan_id"], "plan_hash": row["execution_plan_hash"],
               "evidence_id": row["selection_evidence_id"], "evidence_hash": row["selection_artifact_hash"]}
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    row["run_payload_json"][RECOVERY_KEY] = receipt
    run = SimpleNamespace(**row)
    calls = []
    scheduler = SimulationLifecycleScheduler.__new__(SimulationLifecycleScheduler)
    scheduler.repository = SimpleNamespace(require_localsim_side_effect_free_rebuild=lambda r: calls.append(r.run_id))
    binding = SimpleNamespace(broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    assert scheduler._should_rebuild_localsim_plan_after_side_effect_free_failure(
        binding=binding, run=run, submit=True, trade_date=DAYS[1], as_of_time=NOW)
    assert calls == [run.run_id]
    run.execution_plan_hash = "changed"
    with pytest.raises(InvalidStateTransitionError, match="lineage"):
        recovery_rebuild_receipt(run)


def test_recover_uses_transactional_repository_without_running_a_day():
    service, calls = _service(_snapshot())
    captured = {}
    service.repository.recover_terminal_replay = lambda **kw: captured.update(kw) or "original-job"
    request = LocalSimReplayRecoverRequestV1(expected_version=8, snapshot_sha256="f" * 64)
    assert service.recover("replay-original", request) == "original-job"
    assert captured["expected_version"] == 8 and captured["expected_snapshot_sha256"] == "f" * 64
    assert not calls


def _old_memory_batch():
    snapshot = _snapshot()
    run, plan = snapshot["run"], snapshot["plan"]
    plan["portfolio_id"] = "account"
    body = {k: run[k] for k in ("binding_id", "binding_hash", "release_id", "release_hash", "strategy_id", "package_id")}
    body.update(portfolio_id="account", target_trade_date="2026-01-21", intents=[{"intent_id": "intent-old"}])
    plan["plan_payload_json"] = body
    plan["plan_hash"] = run["execution_plan_hash"] = canonical_json_sha256(body)
    payload = run["run_payload_json"]
    payload.update(broker_called=False, submitted_intents=0, failed_intents=1,
        broker_order_handles=[{"handle_id": "lsh_original", "intent_id": "intent-old", "backend_id": "local_sim",
                               "submitted_at": (NOW - timedelta(hours=2)).isoformat()}])
    payload["submit_failure"] = {"type": "DataUnavailableError", "stage": "LOCAL_SIM_PERSISTENCE_FAILED",
        "context": {"reason_code": "STK_LIMIT_PRE_CLOSE_INVALID", "symbol": "001205.SZ", "trade_date": "2026-01-21",
                    "economic_commit_staged": False, "broker_called": False, "submitted_intents": 0, "failed_intents": 1}}
    return snapshot


def test_native_old_memory_retirement_preserves_original_evidence_and_is_deterministic():
    from backend.services.simulation_runtime.localsim_replay_recovery import (
        RETIREMENT_KEY, apply_handle_retirement, proposed_handle_retirement,
    )
    snapshot = _old_memory_batch()
    original = deepcopy(snapshot)
    kwargs = {"process_started_at": NOW - timedelta(hours=1), "now": NOW}
    proof = proposed_handle_retirement(snapshot, **kwargs)
    assert proposed_handle_retirement(snapshot, **kwargs) == proof
    retired = apply_handle_retirement(snapshot["run"]["run_payload_json"], proof, retired_at=NOW)
    assert retired[RETIREMENT_KEY]["original_payload"] == original["run"]["run_payload_json"]
    assert snapshot == original and "broker_order_handles" not in retired and "failed_intents" not in retired
    require_no_economic_effects(retired, snapshot["economic_counts"], now=NOW)
    retired[RETIREMENT_KEY]["original_payload"]["failed_intents"] = 0
    with pytest.raises(InvalidStateTransitionError, match="retirement"):
        require_no_economic_effects(retired, snapshot["economic_counts"], now=NOW)


def _repeated_mark_memory_batch():
    from backend.services.simulation_runtime.localsim_replay_recovery import (
        apply_handle_retirement, proposed_handle_retirement,
    )
    snapshot = _old_memory_batch()
    proof = proposed_handle_retirement(snapshot, process_started_at=NOW - timedelta(minutes=105), now=NOW)
    payload = apply_handle_retirement(snapshot["run"]["run_payload_json"], proof,
                                      retired_at=NOW - timedelta(minutes=90))
    snapshot["run"]["run_payload_json"] = payload
    snapshot["plan"]["plan_payload_json"]["intents"] = [{"intent_id": "intent-new"}]
    snapshot["plan"]["plan_hash"] = snapshot["run"]["execution_plan_hash"] = canonical_json_sha256(
        snapshot["plan"]["plan_payload_json"])
    snapshot["run"]["execution_plan_id"] = snapshot["plan"]["plan_id"] = "plan-new"
    payload.update(broker_called=False, submitted_intents=0, failed_intents=1,
        broker_order_handles=[{"handle_id": "lsh_new", "intent_id": "intent-new", "backend_id": "local_sim",
                               "submitted_at": (NOW - timedelta(hours=1)).isoformat()}],
        submit_failure={"type": "DataUnavailableError", "stage": "LOCAL_SIM_MARK_VALIDATION_FAILED",
            "context": {"reason_code": "LOCALSIM_SUSPENDED_DAILY_FACT_INVALID", "symbol": "001205.SZ",
                        "trade_date": "2026-01-21", "economic_commit_staged": False,
                        "broker_called": False, "submitted_intents": 0, "failed_intents": 1}})
    return snapshot


def test_repeated_native_retirement_preserves_both_plans_and_all_original_payloads():
    from backend.services.simulation_runtime.localsim_replay_recovery import (
        RETIREMENT_KEY, apply_handle_retirement, proposed_handle_retirement,
    )
    snapshot = _repeated_mark_memory_batch()
    original = deepcopy(snapshot)
    proof = proposed_handle_retirement(snapshot, process_started_at=NOW - timedelta(minutes=30), now=NOW)
    retired = apply_handle_retirement(snapshot["run"]["run_payload_json"], proof, retired_at=NOW)
    latest = retired[RETIREMENT_KEY]
    previous = latest["original_payload"][RETIREMENT_KEY]
    assert previous == original["run"]["run_payload_json"][RETIREMENT_KEY]
    assert latest["predecessor_retirement_sha256"] == previous["receipt_sha256"]
    assert latest["plan_id"] == "plan-new" and latest["plan_hash"] != previous["plan_hash"]
    assert snapshot == original and "broker_order_handles" not in retired
    require_no_economic_effects(retired, snapshot["economic_counts"], now=NOW)
    previous["original_payload"]["failed_intents"] = 0
    latest["original_payload_sha256"] = canonical_json_sha256(latest["original_payload"])
    latest["receipt_sha256"] = canonical_json_sha256({k: v for k, v in latest.items() if k != "receipt_sha256"})
    with pytest.raises(InvalidStateTransitionError, match="retirement"):
        require_no_economic_effects(retired, snapshot["economic_counts"], now=NOW)


@pytest.mark.parametrize("mutation", ["reused_handle", "old_time", "prior_hash", "prior_identity", "live_process"])
def test_repeated_native_retirement_rejects_reused_or_unproven_batches(mutation):
    from backend.services.simulation_runtime.localsim_replay_recovery import RETIREMENT_KEY, proposed_handle_retirement

    snapshot = _repeated_mark_memory_batch()
    payload = snapshot["run"]["run_payload_json"]
    started = NOW - timedelta(minutes=30)
    if mutation == "reused_handle":
        payload["broker_order_handles"][0]["handle_id"] = "lsh_original"
    elif mutation == "old_time":
        payload["broker_order_handles"][0]["submitted_at"] = (NOW - timedelta(hours=2)).isoformat()
    elif mutation == "prior_hash":
        payload[RETIREMENT_KEY]["original_payload"]["failed_intents"] = 0
    elif mutation == "prior_identity":
        prior = payload[RETIREMENT_KEY]
        prior["run_id"] = "different-run"
        prior["receipt_sha256"] = canonical_json_sha256({k: v for k, v in prior.items() if k != "receipt_sha256"})
    elif mutation == "live_process":
        started = NOW - timedelta(hours=2)
    with pytest.raises(InvalidStateTransitionError):
        proposed_handle_retirement(snapshot, process_started_at=started, now=NOW)


@pytest.mark.parametrize("suspended", [False, True])
@pytest.mark.parametrize("replay_status", ["FAILED_TERMINAL", "FAILED_RETRYABLE"])
def test_mark_consumer_failure_recovery_requires_native_full_halt(suspended, replay_status):
    from backend.services.simulation_execution.localsim.suspension import REFERENCE_KEY
    from backend.tests.simulation_execution.test_localsim_runtime import _halt_reference

    snapshot = _repeated_mark_memory_batch()
    snapshot["job"]["status"] = replay_status
    service, calls = _service(snapshot)
    service.process_started_at = NOW - timedelta(minutes=30)
    service.suspension_provider = SimpleNamespace(load_statuses=lambda **_: {
        "001205.SZ": {"suspend_status": {"is_suspended": True},
                      REFERENCE_KEY: _halt_reference("001205.SZ", DAYS[1])}} if suspended else {})
    check = service.inspect("replay-original")
    assert check.ready == suspended and not calls
    if suspended:
        assert check.native_input["eligibility"] == "FULL_DAY_SUSPENDED"
    else:
        assert check.reason_codes == ["LOCALSIM_REPLAY_RECOVERY_SUSPENSION_UNPROVEN"]


def test_retryable_recovery_does_not_widen_other_failure_types():
    snapshot = _old_memory_batch()
    snapshot["job"]["status"] = "FAILED_RETRYABLE"
    service, calls = _service(snapshot)
    service.process_started_at = NOW - timedelta(hours=1)
    check = service.inspect("replay-original")
    assert not check.ready and check.reason_codes == ["LOCALSIM_REPLAY_RECOVERY_STATE_INVALID"]
    assert not calls


@pytest.mark.parametrize("submit", [False, True])
def test_restarted_scheduler_preserves_failed_memory_batch_until_formal_recovery(submit):
    snapshot = _repeated_mark_memory_batch()
    run = SimpleNamespace(**{**snapshot["run"], "status": SimulationDailyRunStatus.FAILED_RETRYABLE,
                              "trade_date": DAYS[1]})
    original = deepcopy(run.run_payload_json)
    scheduler = SimulationLifecycleScheduler.__new__(SimulationLifecycleScheduler)
    binding = SimpleNamespace(broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    if submit:
        with pytest.raises(DataUnavailableError) as exc_info:
            scheduler._should_rebuild_localsim_plan_after_side_effect_free_failure(
                binding=binding, run=run, submit=submit, trade_date=DAYS[1], as_of_time=NOW)
        assert exc_info.value.context["reason_code"] == "LOCALSIM_REPLAY_RECOVERY_REQUIRED"
    else:
        assert not scheduler._should_rebuild_localsim_plan_after_side_effect_free_failure(
            binding=binding, run=run, submit=submit, trade_date=DAYS[1], as_of_time=NOW)
    assert run.run_payload_json == original


@pytest.mark.parametrize("mutation", ["live_process", "unknown", "qmt", "economic", "outbox", "intent",
                                      "plan_hash", "staged", "duplicate", "lease"])
def test_old_handle_retirement_rejects_unproven_or_live_evidence(mutation):
    from backend.services.simulation_runtime.localsim_replay_recovery import proposed_handle_retirement

    snapshot = _old_memory_batch()
    p = snapshot["run"]["run_payload_json"]
    started = NOW - timedelta(hours=1)
    if mutation == "live_process":
        started = NOW - timedelta(hours=3)
    elif mutation == "unknown":
        p["broker_side_effect_state"] = "UNKNOWN"
    elif mutation == "qmt":
        p["broker_order_handles"][0]["backend_id"] = "miniqmt_sim"
    elif mutation == "economic":
        snapshot["economic_counts"]["fills"] = 1
    elif mutation == "outbox":
        p["local_sim_projection_outbox_v1"] = {"status": "PENDING"}
    elif mutation == "intent":
        p["broker_order_handles"][0]["intent_id"] = "different"
    elif mutation == "plan_hash":
        snapshot["plan"]["plan_payload_json"]["package_id"] = "different"
    elif mutation == "staged":
        p["submit_failure"]["context"]["economic_commit_staged"] = True
    elif mutation == "duplicate":
        p["broker_order_handles"] *= 2
    elif mutation == "lease":
        p["simulation_scheduler_retry_claims_v1"] = {"claims": {
            "retry": {"lease_until": (NOW + timedelta(seconds=1)).isoformat()}}}
    with pytest.raises(InvalidStateTransitionError):
        proposed_handle_retirement(snapshot, process_started_at=started, now=NOW)


def test_full_halt_recovery_uses_separate_C_reference_without_loading_missing_pre_close():
    from backend.tests.simulation_execution.test_localsim_runtime import _halt_reference
    from backend.services.simulation_execution.localsim.suspension import REFERENCE_KEY

    snapshot = _snapshot()
    service, calls = _service(snapshot, market_error=DataUnavailableError("strict pre_close remains missing"))
    service.suspension_provider = SimpleNamespace(load_statuses=lambda **_: {
        "001205.SZ": {"suspend_status": {"is_suspended": True},
                      REFERENCE_KEY: _halt_reference("001205.SZ", DAYS[1])}})
    check = service.inspect("replay-original")
    assert check.ready and check.native_input["eligibility"] == "FULL_DAY_SUSPENDED"
    assert "prev_close" not in check.native_input and not calls


@pytest.mark.parametrize("retirement", [None, "invalid", {"plan_id": "different-plan"}])
def test_rebuild_rejects_missing_or_changed_retirement_lineage(retirement):
    from backend.services.simulation_runtime.localsim_replay_recovery import RETIREMENT_KEY

    row = _snapshot()["run"]
    row["trade_date"] = DAYS[1]
    receipt = {"schema_version": RECOVERY_KEY, "run_id": row["run_id"], "binding_id": row["binding_id"],
               "release_id": row["release_id"], "trade_date": DAYS[1].isoformat(),
               "plan_id": row["execution_plan_id"], "plan_hash": row["execution_plan_hash"],
               "evidence_id": row["selection_evidence_id"], "evidence_hash": row["selection_artifact_hash"],
               "retirement_sha256": "a" * 64}
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    row["run_payload_json"][RECOVERY_KEY] = receipt
    if retirement is not None:
        row["run_payload_json"][RETIREMENT_KEY] = retirement
    with pytest.raises(InvalidStateTransitionError, match="retirement lineage"):
        recovery_rebuild_receipt(SimpleNamespace(**row))
