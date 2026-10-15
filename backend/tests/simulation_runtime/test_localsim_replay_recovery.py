from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.localsim_replay import local_sim_calendar_snapshot_sha256
from backend.services.simulation_runtime.localsim_replay_recovery import (
    ECONOMIC_TABLES, RECOVERY_KEY, LocalSimReplayRecoverRequestV1, LocalSimReplayRecoveryService,
    recovery_rebuild_receipt, require_no_economic_effects,
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
        "account": {"status": "ACTIVE"},
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
