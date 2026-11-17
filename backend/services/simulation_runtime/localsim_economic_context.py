"""Read a committed LocalSIM account generation while valuation is pending.

Cash and lots belong to the same economic generation. A missing market mark
must never make a later plan reuse pre-fill lots alongside post-fill cash.
This reader performs no execution, projection or economic commit.
"""
from __future__ import annotations

import math
from zoneinfo import ZoneInfo

from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.simulation_execution.localsim.economic import local_sim_fact_payload
from backend.services.simulation_execution.localsim.models import (
    LocalSimEconomicReceiptV1,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionOutboxStatus,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import AccountSnapshot, PositionLot


def pending_economic_context(*, runtime_repository, paper_repository, strategy_id, portfolio_id, trade_date):
    run = runtime_repository.get_latest_local_sim_economic_run(
        strategy_id=strategy_id, portfolio_id=portfolio_id, before_or_on=trade_date)
    if run is None:
        return None
    try:
        raw = run.run_payload_json["local_sim_projection_outbox_v1"]
        if not all(raw.get(key) for key in ("outbox_hash", "projection_payload_hash", "outbox_id")):
            raise ValueError("frozen outbox hashes missing")
        outbox = LocalSimProjectionOutboxV1.model_validate(raw)
        if ((outbox.run_id, outbox.plan_id) != (run.run_id, run.execution_plan_id)
                or run.strategy_id != strategy_id or run.trade_date > trade_date
                or outbox.projection_payload.get("portfolio_id") != portfolio_id):
            raise ValueError("account generation scope conflicts")
        if outbox.status == LocalSimProjectionOutboxStatus.PROJECTED:
            return None
        payload = outbox.projection_payload
        schemas = {
            "local_sim_valuation_pending_projection_payload_v1": "local_sim_valuation_pending_economic_facts_v1",
            "local_sim_projection_payload_v1": "local_sim_economic_facts_v1",
            "local_sim_waiting_projection_payload_v1": "local_sim_waiting_economic_facts_v1",
        }
        fact_schema = schemas.get(payload.get("schema_version"))
        if fact_schema is None:
            raise ValueError("unsupported pending economic generation schema")
        valuation_pending = fact_schema == "local_sim_valuation_pending_economic_facts_v1"
        receipt_raw = run.run_payload_json["local_sim_economic_receipts_v1"][outbox.receipt_id]
        if not receipt_raw.get("receipt_hash"):
            raise ValueError("frozen receipt hash missing")
        receipt = LocalSimEconomicReceiptV1.model_validate(receipt_raw)
        facts = receipt.economic_facts
        identity = (run.run_id, run.binding_id, run.execution_plan_id, run.trade_date.isoformat())
        if (tuple(payload.get(k) for k in ("run_id", "binding_id", "plan_id", "trade_date")) != identity
                or tuple(facts.get(k) for k in ("run_id", "binding_id", "plan_id", "trade_date")) != identity
                or (receipt.run_id, receipt.binding_id, receipt.plan_id, receipt.trade_date.isoformat()) != identity
                or (outbox.run_id, outbox.plan_id) != (run.run_id, run.execution_plan_id)
                or receipt.generation != outbox.generation or receipt.economic_hash != outbox.economic_hash
                or payload.get("economic_hash") != outbox.economic_hash
                or payload.get("portfolio_id") != portfolio_id or payload.get("strategy_id") != strategy_id
                or run.strategy_id != strategy_id or run.trade_date > trade_date
                or facts.get("schema_version") != fact_schema
                or (valuation_pending and (payload.get("projection_kind") != "VALUATION_PENDING"
                    or facts.get("fact_kind") != "ECONOMIC_FACTS_WITH_VALUATION_PENDING"))):
            raise ValueError("committed generation identity conflicts")
        lots = [PositionLot.model_validate(item) for item in payload["positions"]]
        positions = {lot.symbol: lot for lot in lots}
        hashes = {s: canonical_json_sha256(local_sim_fact_payload(p, fact_type="position"))
                  for s, p in sorted(positions.items())}
        if fact_schema == "local_sim_economic_facts_v1":
            account = AccountSnapshot.model_validate(payload["account_snapshot"])
            if (account.portfolio_id != portfolio_id
                    or account.snapshot_time.astimezone(ZoneInfo("Asia/Shanghai")).date() != run.trade_date
                    or facts.get("account_snapshot_hash") != canonical_json_sha256(
                        local_sim_fact_payload(account, fact_type="account_snapshot"))):
                raise ValueError("committed account snapshot conflicts")
            raw_cash = account.cash
        else:
            raw_cash = payload["cash_reference"]
            if facts.get("cash_reference") != raw_cash or payload.get("position_hashes") != hashes:
                raise ValueError("committed cash or position hashes conflict")
        cash = float(raw_cash)
        if (len(positions) != len(lots) or any(p.portfolio_id != portfolio_id for p in lots)
                or any(p.trade_date > run.trade_date for p in lots)
                or isinstance(raw_cash, bool) or not math.isfinite(cash) or cash < 0
                or facts.get("position_hashes") != hashes):
            raise ValueError("committed lots or cash conflict")
        states = runtime_repository.list_local_sim_execution_states(run.run_id, authoritative=True)
        if facts.get("state_hashes") != {s.state_id: s.state_hash for s in states}:
            raise ValueError("durable execution state generation conflicts")
        runtime_repository.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=outbox)
        paper_repository.readback_local_sim_economic_facts(
            run_id=run.run_id, order_ids=set(facts["order_hashes"]), fill_ids=set(facts["fill_hashes"]),
            order_event_ids=set(facts["order_event_hashes"]), cash_fill_ids=set(facts["cash_entry_hashes"]))
    except Exception as exc:
        raise DataUnavailableError("LocalSIM committed account generation cannot be verified",
            context={"reason_code": "LOCALSIM_ACCOUNT_GENERATION_UNVERIFIED", "run_id": run.run_id,
                     "portfolio_id": portfolio_id}) from exc
    return positions, cash, {"source": "verified_valuation_pending_economic_generation", "run_id": run.run_id,
        "outbox_id": outbox.outbox_id, "generation": outbox.generation, "economic_hash": outbox.economic_hash,
        "economic_recomputation_performed": False, "new_economic_commits": 0}
