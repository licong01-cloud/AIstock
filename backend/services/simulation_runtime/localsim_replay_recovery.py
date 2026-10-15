"""Native, fail-closed recovery of a side-effect-free historical replay day."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .models import canonical_json_sha256
from .localsim_replay import local_sim_calendar_snapshot_sha256


RECOVERY_KEY = "localsim_replay_recovery_v1"
RECOVERY_SCHEMA = "localsim_replay_recovery_v1"
RETIREMENT_KEY = "localsim_memory_handle_retirement_v1"
ECONOMIC_TABLES = ("run", "orders", "order_events", "fills", "cash_ledger", "positions", "daily_snapshots")
ECONOMIC_PAYLOAD_KEYS = (
    "local_sim_execution_states_v1", "local_sim_economic_receipts_v1",
    "local_sim_projection_outbox_v1", "local_sim_projection_receipts_v1",
    "local_sim_economic_generation", "local_sim_persistence", "broker_order_handles",
    "broker_side_effect_state", "miniqmt_side_effect_state", "miniqmt_submit_timeout",
    "qmt_batch_id", "qmt_batch_result", "reconcile_after_submit", "tail_handling",
)


class LocalSimReplayRecoverRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["localsim_replay_recover_request_v1"] = "localsim_replay_recover_request_v1"
    expected_version: int = Field(ge=1)
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class LocalSimReplayRecoveryCheckV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["localsim_replay_recovery_check_v1"] = "localsim_replay_recovery_check_v1"
    replay_job_id: str
    version: int
    ready: bool
    reason_codes: list[str]
    snapshot_sha256: str
    run_id: str | None = None
    trade_date: date | None = None
    economic_counts: dict[str, int]
    native_input: dict[str, Any] = Field(default_factory=dict)


def economic_fact_counts(cur: Any, run_id: str) -> dict[str, int]:
    """Inspect actual native facts, including unprojected paper-side effects."""
    counts = {}
    for table in ECONOMIC_TABLES:
        cur.execute(f"SELECT count(*) AS count FROM paper_v2.{table} WHERE run_id = %s", (run_id,))
        counts[table] = int(cur.fetchone()["count"])
    return counts


def require_no_economic_effects(
    payload: dict[str, Any], counts: dict[str, int], *, now: datetime, check_retry_claims: bool = True,
) -> None:
    retirement = payload.get(RETIREMENT_KEY)
    if retirement is not None:
        original = retirement.get("original_payload") if isinstance(retirement, dict) else None
        if (not isinstance(original, dict)
                or retirement.get("schema_version") != RETIREMENT_KEY
                or retirement.get("receipt_sha256") != canonical_json_sha256(
                    {k: v for k, v in retirement.items() if k != "receipt_sha256"})
                or retirement.get("original_payload_sha256") != canonical_json_sha256(original)
                or payload.get("broker_order_handles") or payload.get("failed_intents", 0) != 0):
            raise InvalidStateTransitionError(
                "LocalSIM handle retirement evidence is invalid",
                context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_RETIREMENT_INVALID"},
            )
    if set(counts) != set(ECONOMIC_TABLES) or any(type(v) is not int or v != 0 for v in counts.values()):
        raise InvalidStateTransitionError(
            "LocalSIM recovery requires zero authoritative economic facts",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_ECONOMIC_FACTS_PRESENT"},
        )
    if ((payload.get("broker_called") is not None and payload.get("broker_called") is not False)
            or payload.get("submitted_intents", 0) != 0
            or payload.get("failed_intents", 0) != 0
            or any(payload.get(k) for k in ECONOMIC_PAYLOAD_KEYS)):
        raise InvalidStateTransitionError(
            "LocalSIM recovery cannot discard submitted or durable runtime evidence",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_SIDE_EFFECTS_UNPROVEN"},
        )
    if not check_retry_claims:
        return  # The scheduler already owns the current retry claim while rebuilding.
    claims = payload.get("simulation_scheduler_retry_claims_v1", {})
    if not isinstance(claims, dict) or not isinstance(claims.get("claims", {}), dict):
        raise InvalidStateTransitionError(
            "LocalSIM retry claim evidence is invalid",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_CLAIM_INVALID"},
        )
    for claim in claims.get("claims", {}).values():
        try:
            lease = datetime.fromisoformat(claim["lease_until"])
            active = lease.tzinfo is None or lease > now
        except (TypeError, KeyError, ValueError):
            active = True
        if active:
            raise InvalidStateTransitionError(
                "LocalSIM failed-day retry claim is still active or unproven",
                context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_CLAIM_ACTIVE"},
            )


def proposed_handle_retirement(snapshot: dict[str, Any], *, process_started_at: datetime, now: datetime) -> dict | None:
    """Prove a previous-process, rolled-back LocalSIM memory batch; preserve its full evidence."""
    run, plan = snapshot["run"], snapshot["plan"]
    payload = run["run_payload_json"]
    handles = payload.get("broker_order_handles")
    if not handles:
        return None
    failure = payload.get("submit_failure", {})
    context = failure.get("context", {})
    plan_payload = plan.get("plan_payload_json", {})
    intents = plan_payload.get("intents", [])
    try:
        intent_ids = [i["intent_id"] for i in intents]
        handle_ids = [h["handle_id"] for h in handles]
        matched = [h["intent_id"] for h in handles]
        dates = [datetime.fromisoformat(h["submitted_at"]) for h in handles]
        valid = (
            failure.get("type") == "DataUnavailableError"
            and failure.get("stage") == "LOCAL_SIM_PERSISTENCE_FAILED"
            and context.get("reason_code") == "STK_LIMIT_PRE_CLOSE_INVALID"
            and context.get("economic_commit_staged") is False
            and context.get("broker_called") is False and context.get("submitted_intents") == 0
            and type(payload.get("failed_intents")) is int
            and payload["failed_intents"] == context.get("failed_intents") == len(handles) == len(intents)
            and len(intents) > 0 and len(set(intent_ids)) == len(intents)
            and len(set(handle_ids)) == len(handles) and sorted(matched) == sorted(intent_ids)
            and all(h.get("backend_id") == "local_sim" and h["handle_id"].startswith("lsh_") for h in handles)
            and all(d.tzinfo is not None and d < process_started_at <= now for d in dates)
            and canonical_json_sha256(plan_payload) == run["execution_plan_hash"]
            and plan_payload.get("portfolio_id") == plan["portfolio_id"]
            and all(plan_payload.get(k) == run[k] for k in (
                "binding_id", "binding_hash", "release_id", "release_hash", "strategy_id", "package_id"))
            and plan_payload.get("target_trade_date") == run["trade_date"]
            and payload.get(RETIREMENT_KEY) is None
        )
    except (KeyError, TypeError, AttributeError, ValueError):
        valid = False
    if not valid:
        raise InvalidStateTransitionError(
            "LocalSIM old handles are not proven to be a rolled-back native memory batch",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_SIDE_EFFECTS_UNPROVEN"},
        )
    operational = deepcopy(payload)
    operational.pop("broker_order_handles")
    operational.pop("failed_intents")
    require_no_economic_effects(operational, snapshot["economic_counts"], now=now)
    return {
        "schema_version": RETIREMENT_KEY, "run_id": run["run_id"],
        "plan_id": run["execution_plan_id"], "plan_hash": run["execution_plan_hash"],
        "binding_id": run["binding_id"], "release_id": run["release_id"], "trade_date": run["trade_date"],
        "evidence_id": run["selection_evidence_id"], "evidence_hash": run["selection_artifact_hash"],
        "process_started_at": process_started_at.isoformat(),
        "original_payload": deepcopy(payload), "original_payload_sha256": canonical_json_sha256(payload),
        "economic_counts": dict(snapshot["economic_counts"]),
    }


def apply_handle_retirement(payload: dict[str, Any], proof: dict[str, Any], *, retired_at: datetime) -> dict[str, Any]:
    if proof["original_payload_sha256"] != canonical_json_sha256(payload):
        raise InvalidStateTransitionError(
            "LocalSIM old handle evidence changed before retirement",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_RETIREMENT_INVALID"},
        )
    receipt = {**deepcopy(proof), "retired_at": retired_at.isoformat()}
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    operational = deepcopy(payload)
    operational.pop("broker_order_handles")
    operational.pop("failed_intents")
    operational[RETIREMENT_KEY] = receipt
    return operational


def recovery_rebuild_receipt(run: Any) -> dict[str, Any] | None:
    receipt = run.run_payload_json.get(RECOVERY_KEY)
    if receipt is None:
        return None
    fields = {"run_id": run.run_id, "binding_id": run.binding_id, "release_id": run.release_id,
              "trade_date": run.trade_date.isoformat(), "plan_id": run.execution_plan_id,
              "plan_hash": run.execution_plan_hash, "evidence_id": run.selection_evidence_id,
              "evidence_hash": run.selection_artifact_hash}
    if (not isinstance(receipt, dict) or receipt.get("schema_version") != RECOVERY_SCHEMA
            or any(receipt.get(k) != v for k, v in fields.items())
            or receipt.get("receipt_sha256") != canonical_json_sha256(
                {k: v for k, v in receipt.items() if k != "receipt_sha256"})):
        raise InvalidStateTransitionError(
            "LocalSIM replay recovery lineage no longer matches its failed-day inputs",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_LINEAGE_CONFLICT"},
        )
    retirement = run.run_payload_json.get(RETIREMENT_KEY)
    if ((retirement is None and receipt.get("retirement_sha256") is not None)
            or (retirement is not None and (
                not isinstance(retirement, dict)
                or any(retirement.get(k) != v for k, v in fields.items())
                or receipt.get("retirement_sha256") != retirement.get("receipt_sha256")))):
        raise InvalidStateTransitionError(
            "LocalSIM retirement lineage no longer matches its recovery receipt",
            context={"reason_code": "LOCALSIM_REPLAY_RECOVERY_RETIREMENT_INVALID"},
        )
    return receipt


class LocalSimReplayRecoveryService:
    def __init__(self, *, repository: Any, source_authority: Any, market_provider: Any, clock=None,
                 suspension_provider=None, process_started_at: datetime | None = None):
        self.repository, self.source_authority, self.market_provider = repository, source_authority, market_provider
        self.clock = clock or (lambda: datetime.now(UTC))
        if process_started_at is None:
            import psutil

            process_started_at = datetime.fromtimestamp(psutil.Process().create_time(), UTC)
        self.process_started_at = process_started_at
        self.suspension_provider = suspension_provider
        from backend.services.simulation_data.minute_provider import SimulationMinuteMarketDataProvider

        if suspension_provider is None and isinstance(market_provider, SimulationMinuteMarketDataProvider):
            from backend.services.simulation_execution.localsim.suspension import LocalSimHistoricalSuspensionProvider

            self.suspension_provider = LocalSimHistoricalSuspensionProvider(conn_factory=market_provider.conn_factory)

    def inspect(self, replay_job_id: str) -> LocalSimReplayRecoveryCheckV1:
        return self.evaluate(self.repository.get_replay_recovery_snapshot(replay_job_id))

    def evaluate(self, snapshot: dict[str, Any]) -> LocalSimReplayRecoveryCheckV1:
        job, run = snapshot["job"], snapshot.get("run")
        reasons: list[str] = []
        native: dict[str, Any] = {}
        counts = snapshot["economic_counts"]
        try:
            if (job["status"] != "FAILED_TERMINAL" or job["next_trade_date"] is None
                    or job["live_release_id"] or job["live_binding_id"] or job["activation_trade_date"]):
                self._invalid("LOCALSIM_REPLAY_RECOVERY_STATE_INVALID")
            source = self.source_authority.resolve(
                historical_source_id=job["historical_source_id"], start_trade_date=date.fromisoformat(job["start_trade_date"]),
                end_trade_date=date.fromisoformat(job["end_trade_date"]), require_latest_completed=False,
            )
            next_day = date.fromisoformat(job["next_trade_date"])
            index = source.trading_days.index(next_day)
            completed = source.trading_days[index - 1].isoformat() if index else None
            if (source.historical_source_id != job["historical_source_id"]
                    or source.historical_source_sha256 != job["historical_source_sha256"]
                    or local_sim_calendar_snapshot_sha256(source.trading_days) != job["calendar_snapshot_sha256"]
                    or job["day_engine_contract_id"] != "simulation_daily_engine_v1"
                    or completed != job["completed_trade_date"] or next_day >= source.current_trading_date):
                self._invalid("LOCALSIM_REPLAY_RECOVERY_SOURCE_IDENTITY_CONFLICT")
            if not run or run["status"] != "FAILED_RETRYABLE" or run["broker_backend"] != "local_sim":
                self._invalid("LOCALSIM_REPLAY_RECOVERY_DAILY_RUN_INVALID")
            binding, release, plan = snapshot["binding"], snapshot["release"], snapshot.get("plan")
            if (snapshot["account"]["status"] != "ACTIVE"
                    or run["trade_date"] != job["next_trade_date"]
                    or job["failure_context"].get("trade_date") != job["next_trade_date"]
                    or run["strategy_id"] != job["simulation_account_id"]
                    or run["binding_id"] != job["binding_id"] or run["release_id"] != job["release_id"]
                    or run["binding_hash"] != binding["binding_hash"]
                    or run["release_hash"] != release["release_hash"]
                    or binding["strategy_id"] != run["strategy_id"]
                    or binding["broker_backend"] != "local_sim"
                    or binding["release_id"] != release["release_id"]
                    or binding["release_hash"] != release["release_hash"]
                    or binding["package_id"] != run["package_id"]
                    or binding["manifest_sha256"] != run["manifest_sha256"]
                    or release["package_id"] != run["package_id"]
                    or release["manifest_sha256"] != run["manifest_sha256"]
                    or not plan or plan["plan_hash"] != run["execution_plan_hash"]
                    or plan["binding_id"] != run["binding_id"]
                    or plan["target_trade_date"] != run["trade_date"]
                    or plan["selection_evidence_id"] != run["selection_evidence_id"]
                    or plan["selection_evidence_hash"] != run["selection_artifact_hash"]
                    or snapshot["evidence_hash"] != run["selection_artifact_hash"]):
                self._invalid("LOCALSIM_REPLAY_RECOVERY_FROZEN_IDENTITY_CONFLICT")
            retirement = proposed_handle_retirement(snapshot, process_started_at=self.process_started_at, now=self.clock())
            if retirement is not None:
                native[RETIREMENT_KEY] = retirement
                operational = apply_handle_retirement(run["run_payload_json"], retirement, retired_at=self.clock())
            else:
                operational = run["run_payload_json"]
            require_no_economic_effects(operational, counts, now=self.clock())
            failure = run["run_payload_json"].get("submit_failure", {})
            context = failure.get("context", {})
            exact_message = "market.stk_limit pre_close is required and cannot be derived"
            if not (
                (failure.get("type") == "DataUnavailableError"
                 and failure.get("stage") == "LOCAL_SIM_PERSISTENCE_FAILED"
                 and context.get("reason_code") == "STK_LIMIT_PRE_CLOSE_INVALID")
                or (failure.get("type") == "BrokerConnectivityError"
                    and failure.get("stage") == "LOCAL_SIM_SUBMIT_FAILED"
                    and context.get("source") == "DB_HISTORICAL" and context.get("cause") == exact_message)
            ) or job["failure_code"] not in {"DataUnavailableError", "BrokerConnectivityError", "_BindingRetryAttemptError"}:
                self._invalid("LOCALSIM_REPLAY_RECOVERY_FAILURE_UNSUPPORTED")
            symbol = context.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip() or context.get("trade_date") != run["trade_date"]:
                self._invalid("LOCALSIM_REPLAY_RECOVERY_FAILURE_IDENTITY_INVALID")
            from backend.services.simulation_data.contracts import MinuteDataSource

            status = (self.suspension_provider.load_statuses(
                symbols=[symbol], trade_date=next_day, valuation_symbols=[symbol],
            ).get(symbol)
                      if self.suspension_provider is not None else None)
            if status and status.get("suspend_status", {}).get("is_suspended") is True:
                from backend.services.simulation_execution.localsim.suspension import (
                    NATIVE_PRE_CLOSE_KEY, REFERENCE_KEY, validate_suspended_native_pre_close,
                    validate_suspension_reference,
                )
                native.update({"symbol": symbol, "trade_date": next_day.isoformat(), "source": "DB_HISTORICAL",
                               "eligibility": "FULL_DAY_SUSPENDED"})
                if NATIVE_PRE_CLOSE_KEY in status:
                    native["valuation_native_pre_close"] = validate_suspended_native_pre_close(
                        status[NATIVE_PRE_CLOSE_KEY], symbol=symbol, trade_date=next_day,
                    )
                else:
                    native["valuation_reference"] = validate_suspension_reference(
                        status.get(REFERENCE_KEY, {}), symbol=symbol, trade_date=next_day,
                    )
            else:
                market = self.market_provider.load_symbol_input(
                    symbol=symbol, trade_date=next_day, source=MinuteDataSource.DB_HISTORICAL,
                    min_bars=1, require_suspend_status=True,
                )
                native.update({"symbol": market.symbol, "trade_date": market.trade_date.isoformat(),
                               "source": market.source.value, "prev_close": market.market_context.get("prev_close"),
                               "bar_count": len(market.minute_bars)})
                if (native["symbol"] != symbol or native["trade_date"] != run["trade_date"]
                        or native["source"] != "DB_HISTORICAL" or not native["bar_count"]):
                    self._invalid("LOCALSIM_REPLAY_RECOVERY_NATIVE_INPUT_CONFLICT")
        except (DataUnavailableError, InvalidStateTransitionError) as exc:
            reasons.append(str(exc.context.get("reason_code") or type(exc).__name__))
        except ValueError:
            reasons.append("LOCALSIM_REPLAY_RECOVERY_SOURCE_IDENTITY_CONFLICT")
        digest = canonical_json_sha256({"snapshot": snapshot, "native_input": native, "reason_codes": reasons})
        return LocalSimReplayRecoveryCheckV1(
            replay_job_id=job["replay_job_id"], version=job["version"], ready=not reasons,
            reason_codes=reasons, snapshot_sha256=digest, run_id=run["run_id"] if run else None,
            trade_date=job["next_trade_date"], economic_counts=counts, native_input=native,
        )

    def recover(self, replay_job_id: str, request: LocalSimReplayRecoverRequestV1):
        return self.repository.recover_terminal_replay(
            replay_job_id=replay_job_id, expected_version=request.expected_version,
            expected_snapshot_sha256=request.snapshot_sha256, evaluate=self.evaluate, updated_at=self.clock(),
        )

    @staticmethod
    def _invalid(reason: str) -> None:
        raise InvalidStateTransitionError("LocalSIM replay recovery is blocked", context={"reason_code": reason})
