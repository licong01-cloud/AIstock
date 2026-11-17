"""Isolated correction replay over frozen signals and the native causal broker.

This module is an offline entry, never imported by a scheduler or router. It
publishes an immutable account version and cannot activate or replace accounts.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
from pathlib import Path
from types import SimpleNamespace

from filelock import FileLock

from backend.services.selection_center.models import SignalSnapshot
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.simulation_data.daily_context_provider import DailyTradingContextProvider
from backend.services.simulation_execution.localsim.runtime import LocalSimBackend
from backend.services.simulation_execution.localsim.models import LocalSimMarketMarkV1
from backend.services.strategy_package.execution_policy import validate_frozen_execution_policy_snapshot
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.runtime import TargetPositionEngine
from backend.services.trading_core.ledger import FeeModel
from backend.services.trading_core.models import OrderSide, PositionLot

from .decision import RebalanceIntentService
from .localsim_reconstruction_inputs import (
    FrozenHistoricalMarketProvider, TZ, file_ref, money, publish, read, require,
    trusted_opening,
)


def implementation_identity():
    from . import decision, localsim_reconstruction_inputs
    from backend.services.simulation_execution.localsim import runtime
    from backend.services.trading_core import ledger, minute_execution
    from backend.services.strategy_package import runtime as target_runtime
    files = [Path(__file__), Path(localsim_reconstruction_inputs.__file__), Path(decision.__file__),
             Path(runtime.__file__), Path(ledger.__file__), Path(minute_execution.__file__), Path(target_runtime.__file__)]
    return {p.name + ":" + p.parent.name: hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for p in files}


def _sources(request):
    require(request["schema_version"] == "localsim_reconstruction_request_v1", "REQUEST_SCHEMA")
    require(request["implementation_identity"] == implementation_identity(), "IMPLEMENTATION_CHANGED")
    require(request["new_capital"] == "0" and request["activate_account"] is False
            and request["production_write"] is False and request["selection_inference"] is False,
            "REQUEST_EFFECTS")
    if "request_hash" in request:
        body = {k: v for k, v in request.items() if k not in {
            "request_hash", "account_version_id", "source_inspection", "origin_initial_cash"}}
        require(canonical_json_sha256(body) == request["request_hash"]
                and request["account_version_id"] == "lsrecon_" + request["request_hash"][:24], "REQUEST_HASH")
    seen = {}
    for pin in request["source_refs"]:
        require(pin["path"] not in seen or seen[pin["path"]] == pin, "SOURCE_PIN_CONFLICT")
        seen[pin["path"]] = pin
    for pin in seen.values():
        require(file_ref(pin["path"]) == pin, "SOURCE_CHANGED")
    opening = read(request["opening"])
    require(opening["source_account_id"] == request["source_account_id"] and opening["new_capital"] == "0",
            "OPENING_SCOPE")
    source_facts = read(opening["source_facts"])
    proof = trusted_opening(source_facts, opening["trade_date"])
    require(all(opening[k] == v for k, v in proof.items()), "OPENING_PROOF_CHANGED")
    days = []
    for pin in request["day_inputs"]:
        value = read(pin)
        require(value["input_hash"] == canonical_json_sha256({k: v for k, v in value.items() if k != "input_hash"}),
                "DAY_INPUT_HASH")
        require(value["trade_date"] == value["selection"]["target_trade_date"], "SELECTION_DATE")
        require(value["selection"]["manifest_sha256"] == request["manifest"]["manifest_sha256"], "MANIFEST_IDENTITY")
        require(value["selection"]["package_id"] == request["manifest"]["package_id"], "PACKAGE_IDENTITY")
        require(value["original_plan_identity"]["selection_evidence_id"] == value["selection"]["evidence_id"]
                and value["original_plan_identity"]["selection_evidence_hash"] == value["selection"]["artifact_hash"],
                "ORIGINAL_SELECTION_IDENTITY")
        FrozenHistoricalMarketProvider(value)
        days.append(value)
    dates = [d["trade_date"] for d in days]
    require(dates == request["trading_days"] == sorted(set(dates)) and bool(dates)
            and dates[0] > opening["trade_date"] and date.fromisoformat(dates[-1]) < datetime.now(TZ).date(),
            "CALENDAR_SCOPE")
    expected_dates = [str(d["cal_date"]) for d in source_facts["calendar"]
                      if opening["trade_date"] < str(d["cal_date"]) <= dates[-1]]
    require(expected_dates == dates, "SKIPPED_TRADING_DAY")
    require(set(request["fee_policy"]) == {"open_cost", "close_cost", "min_cost"}
            and all(money(v) >= 0 for v in request["fee_policy"].values()), "FEE_POLICY")
    policy = validate_frozen_execution_policy_snapshot(request["execution_policy"])
    require(policy["policy_json"]["algo_code"] == "TWAP", "LEGACY_EXECUTION_POLICY_UNSUPPORTED")
    return opening, days


def prepare(*, output_root, source_account_id, opening, day_inputs, trading_days,
            manifest, execution_policy, fee_policy, source_refs):
    root = Path(output_root).resolve()
    source_refs = list({p["path"]: p for p in [*source_refs, opening, *day_inputs]}.values())
    request = {"schema_version": "localsim_reconstruction_request_v1", "output_root": str(root),
        "source_account_id": source_account_id, "opening": opening, "day_inputs": day_inputs,
        "trading_days": trading_days, "manifest": manifest, "execution_policy": execution_policy,
        "fee_policy": fee_policy, "source_refs": source_refs, "implementation_identity": implementation_identity(),
        "execution_contract": "NATIVE_CAUSAL_HISTORICAL_MINUTE_LOOP", "new_capital": "0",
        "activate_account": False, "production_write": False, "selection_inference": False}
    _sources(request)
    digest = canonical_json_sha256(request)
    request.update(request_hash=digest, account_version_id="lsrecon_" + digest[:24])
    with FileLock(str(root) + ".lock"):
        inspection = publish(root / "source_inspection.json", {"schema_version": "localsim_reconstruction_source_inspection_v1",
            "status": "COMPLETE", "request_hash": digest, "source_refs": source_refs, "day_count": len(trading_days),
            "economics_read": False, "selection_inference": False, "production_writes": 0})
        request["source_inspection"] = inspection
        return publish(root / "request.json", request)


def _initial(opening, version):
    positions = {p["symbol"]: PositionLot(portfolio_id=version, symbol=p["symbol"], quantity=p["quantity"],
        available_quantity=p["available_quantity"], avg_cost=float(p["avg_cost"]), trade_date=opening["trade_date"])
        for p in opening["positions"]}
    return {"cash": opening["cash"], "nav": opening["nav"], "positions": {
        s: p.model_dump(mode="json") for s, p in positions.items()}}


def replay_day(request, day_input, previous):
    """One account day, using one native ledger and chronological feedback."""
    provider = FrozenHistoricalMarketProvider(day_input)
    day = provider.context.trade_date
    version = request["account_version_id"]
    manifest = StrategyPackageManifest.model_validate(request["manifest"])
    positions = {s: PositionLot.model_validate(p) for s, p in previous["positions"].items()}
    fee = FeeModel(**request["fee_policy"])
    backend = LocalSimBackend(portfolio_id=version, package_id=manifest.package_id,
        initial_cash=float(request["origin_initial_cash"]), initial_available_cash=float(previous["cash"]),
        initial_positions=positions, fee_model=fee, manifest=manifest, data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy=request["execution_policy"], market_data_provider=provider, causal_historical_replay=True)
    backend._ledger.settle_trade_date(day)
    current = backend.query_positions()
    prices = {s: f.pre_close for s, f in provider.context.symbols.items()}
    equity = money(previous["cash"]) + sum((money(prices[s]) * p.quantity for s, p in current.items()), Decimal(0))
    selected = day_input["selection"]
    snapshot = SignalSnapshot(snapshot_id="frozen_" + selected["evidence_id"], package_id=manifest.package_id,
        manifest_sha256=manifest.manifest_sha256, trade_date=day, data_source=MinuteDataSource.DB_HISTORICAL.value,
        candidates=selected["selected_candidates"], runtime_config=selected["runtime_profile"])
    targets = TargetPositionEngine().build_targets(snapshot=snapshot, total_equity=float(equity),
        top_k=len(snapshot.candidates), manifest=manifest, current_positions=current, current_prices=prices)
    rebalance = RebalanceIntentService().build_order_intents(package_id=manifest.package_id,
        portfolio_id=version, strategy_id=version, trade_date=day, current_positions=current,
        target_positions=targets, current_prices=prices,
        pre_trade_tradability=DailyTradingContextProvider.to_pre_trade_statuses(provider.context))
    start, end = (datetime.combine(day, t, TZ) for t in (time(9, 30), time(15)))
    plan_id = "recon_plan_" + canonical_json_sha256([request["request_hash"], day_input["input_hash"]])[:24]
    plan = SimpleNamespace(plan_id=plan_id, target_trade_date=day, intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions, plan_payload_json={
            "daily_trading_context": provider.context.model_dump(mode="json"),
            "local_sim_execution_causality": {"eligible_bar_after": (start - timedelta(microseconds=1)).isoformat()}})
    backend.configure_execution_runtime(run_id="recon_run_" + plan_id[11:], binding_id=version)
    backend.bind_execution_plan(plan=plan, as_of_time=start)
    fills, flows = {}, []
    for intent in sorted(rebalance.order_intents, key=lambda i: (i.side != OrderSide.SELL, i.symbol, i.intent_id)):
        backend.submit_order_intent(intent)
    for minute in range(1, 331):
        clock = start + timedelta(minutes=minute)
        if clock <= datetime.combine(day, time(11, 30), TZ) or clock >= datetime.combine(day, time(13), TZ):
            backend.advance_realtime_execution(as_of_time=clock)
            tick = backend.export_execution_snapshot()
            for fill in tick["fills"]:
                require(fill.fill_id not in fills, "DUPLICATE_TICK_FILL")
                fills[fill.fill_id] = fill
            flows.extend(tick["cash_entries"])
    exported = backend.export_execution_snapshot()
    require(len(exported["execution_states"]) == len(exported["orders"]) == len(rebalance.order_intents)
            and all(s.is_terminal for s in exported["execution_states"]), "DAY_NOT_TERMINAL")
    marks = {s: provider.markets[s]["bars"][-1].close if provider.markets[s]["bars"]
             else provider.context.symbols[s].pre_close for s, p in backend.query_positions().items() if p.quantity}
    account = backend._ledger.account_snapshot(prices=marks, snapshot_time=end)
    closing = {s: p.model_dump(mode="json") for s, p in backend.query_positions().items()}
    cash_entries = [{k: str(v) if isinstance(v, Decimal) else v.isoformat() if isinstance(v, date)
                     else v.value if hasattr(v, "value") else v for k, v in vars(c).items()}
                    for c in flows]
    result = {"schema_version": "localsim_reconstruction_day_v1", "account_version_id": version,
        "trade_date": day.isoformat(), "request_hash": request["request_hash"], "input_hash": day_input["input_hash"],
        "opening_state_hash": canonical_json_sha256(previous), "cash": str(account.cash), "nav": str(account.nav),
        "positions": closing, "marks": marks, "snapshot_time": end.isoformat(),
        "orders": [o.model_dump(mode="json") for o in exported["orders"]],
        "fills": [f.model_dump(mode="json") for f in fills.values()], "cash_entries": cash_entries,
        "execution_states": [s.model_dump(mode="json") for s in exported["execution_states"]],
        "trading_rule_decisions": [d.model_dump(mode="json") for d in rebalance.trading_rule_decisions],
        "origin_initial_cash": request["origin_initial_cash"], "new_capital": "0",
        "valuation_day_pnl": str(money(account.nav) - money(previous["nav"])),
        "cumulative_pnl": str(money(account.nav) - money(request["origin_initial_cash"]))}
    verify_day(result, previous)
    return result


def verify_day(result, previous):
    cash = money(previous["cash"])
    expected = {s: p["quantity"] for s, p in previous["positions"].items()}
    by_id = {f["fill_id"]: f for f in result["fills"]}
    require(len(by_id) == len(result["fills"]) and len(result["cash_entries"]) == len(by_id)
            and {c["fill_id"] for c in result["cash_entries"]} == set(by_id), "DAY_FILL_CASH_SET")
    for entry in result["cash_entries"]:
        fill = by_id[entry["fill_id"]]
        require(entry["side"] == fill["side"] and entry["symbol"] == fill["symbol"], "DAY_FILL_CASH_SCOPE")
        expected[fill["symbol"]] = expected.get(fill["symbol"], 0) + fill["quantity"] * (1 if fill["side"] == "BUY" else -1)
        notional = money(fill["price"]) * fill["quantity"]
        require(abs(notional - money(entry["notional"])) <= Decimal("0.02") and money(entry["fee"]) >= 0
                and abs(money(entry["cash_delta"]) - (
                    -money(entry["notional"]) - money(entry["fee"]) if fill["side"] == "BUY"
                    else money(entry["notional"]) - money(entry["fee"]))) <= Decimal("0.02"), "DAY_CASH_SIGN_OR_FEE")
        cash += money(entry["cash_delta"])
        require(cash >= 0 and abs(cash - money(entry["cash_after"])) <= Decimal("0.02")
                and expected[fill["symbol"]] >= 0, "DAY_CAPITAL_OR_OVERSELL")
    require(abs(cash - money(result["cash"])) <= Decimal("0.02"), "DAY_CASH_BRIDGE")
    require(all(expected.get(s, 0) == result["positions"].get(s, {}).get("quantity", 0)
                for s in set(expected) | set(result["positions"])), "DAY_INVENTORY_BRIDGE")
    nav = money(result["cash"]) + sum((money(result["marks"][s]) * p["quantity"]
        for s, p in result["positions"].items() if p["quantity"]), Decimal(0))
    require(abs(nav - money(result["nav"])) <= Decimal("0.02"), "DAY_NAV_BRIDGE")
    require(abs(nav - money(previous["nav"]) - money(result["valuation_day_pnl"])) <= Decimal("0.02")
            and abs(nav - money(result["origin_initial_cash"]) - money(result["cumulative_pnl"])) <= Decimal("0.02"),
            "DAY_PNL_BRIDGE")


def _state(result):
    return {k: deepcopy(result[k]) for k in ("cash", "nav", "positions")}


def _inspect(request, opening, inputs):
    root = Path(request["output_root"])
    previous = _initial(opening, request["account_version_id"])
    refs = []
    missing = False
    for value in inputs:
        path = root / "days" / (value["trade_date"] + ".json")
        if not path.exists():
            missing = True
            continue
        require(not missing, "NONCONTIGUOUS_RESULT")
        pin = file_ref(path)
        day = read(pin)
        require(day["request_hash"] == request["request_hash"] and day["input_hash"] == value["input_hash"]
                and day["trade_date"] == value["trade_date"]
                and day["account_version_id"] == request["account_version_id"]
                and day["opening_state_hash"] == canonical_json_sha256(previous), "DAY_IDENTITY")
        verify_day(day, previous)
        refs.append(pin)
        previous = _state(day)
    return refs, previous


def run(request_ref, *, stop_after_days=None):
    request = read(request_ref)
    opening, inputs = _sources(request)  # Complete preflight before any result read.
    request["origin_initial_cash"] = opening["origin_initial_cash"]
    root = Path(request["output_root"])
    with FileLock(str(root) + ".lock"):
        refs, previous = _inspect(request, opening, inputs)
        reused = len(refs)
        remaining = inputs[reused:]
        if stop_after_days is not None:
            require(stop_after_days >= 0, "INVALID_BATCH_SIZE")
            remaining = remaining[:stop_after_days]
        for value in remaining:
            result = replay_day(request, value, previous)
            refs.append(publish(root / "days" / (value["trade_date"] + ".json"), result))
            previous = _state(result)
        complete = len(refs) == len(inputs)
        if complete:
            publish(root / "facts.json", {"schema_version": "localsim_reconstruction_facts_v1",
                "request": request_ref, "opening": request["opening"], "days": refs,
                "account_version_id": request["account_version_id"], "source_account_id": request["source_account_id"],
                "original_accounts_modified": False, "new_capital": "0", "economic_confirmation": False})
        return {"status": "COMPLETE" if complete else "CONTINUABLE", "completed_days": len(refs),
                "reused_days": reused, "recomputed_days": len(remaining), "production_writes": 0,
                "new_database_economic_commits": 0}


def inspect(request_ref):
    request = read(request_ref)
    opening, inputs = _sources(request)
    with FileLock(str(Path(request["output_root"])) + ".lock"):
        refs, _ = _inspect(request, opening, inputs)
        return {"schema_version": "localsim_reconstruction_inspection_v1", "request": request_ref,
                "complete": len(refs) == len(inputs), "days": refs, "account_days": len(refs),
                "economic_recomputation_performed": False, "new_database_economic_commits": 0}


def exact_retry(request_ref):
    before = inspect(request_ref)
    require(before["complete"], "RETRY_REQUIRES_COMPLETE")
    retried = run(request_ref)
    after = inspect(request_ref)
    require(before == after and retried["recomputed_days"] == 0, "RETRY_MUTATED")
    return {"schema_version": "localsim_reconstruction_exact_retry_v1", "request": request_ref,
            "status": "EXACT_ARTIFACT_RETRY_VERIFIED", "facts": file_ref(Path(read(request_ref)["output_root"]) / "facts.json"),
            "account_days": before["account_days"], "recomputed_days": 0, "new_database_economic_commits": 0,
            "new_capital": "0", "original_accounts_modified": False}


def valuation_states(facts):
    """Keep a conserved native book; fill a no-trade day by explicit carry."""
    require(facts["native"] is True, "VALUATION_NATIVE_BOOK_REQUIRED")
    days = [str(d["cal_date"]) for d in facts["calendar"]
            if str(d["cal_date"]) < datetime.now(TZ).date().isoformat()]
    require(days == sorted(set(days)), "VALUATION_CALENDAR")
    snapshot_days = {str(s["trade_date"]) for s in facts["snapshots"]}
    previous = None
    states = []
    for day in days:
        if day in snapshot_days:
            state = trusted_opening(facts, day, require_close=False)
            previous = state
        else:
            require(previous is not None and not any(str(c["trade_date"]) == day for c in facts["cash"])
                    and not any(str(r["trade_date"]) == day for r in facts["runs"]), "VALUATION_MISSING_DAY_EFFECTS")
            state = deepcopy(previous)
            state.update(trade_date=day, state_carry={"from_trade_date": previous["trade_date"],
                "source_snapshot_run_id": previous["source_snapshot"]["run_id"],
                "paper_runs": 0, "cash_entries": 0, "fills": 0})
        states.append(state)
    return {"schema_version": "localsim_existing_book_valuation_states_v1",
            "account_id": facts["account"]["account_id"], "days": states, "account_execution": False}


def revalue_existing(*, states_ref, marks_ref, source_refs, output_root):
    """Reprice authentic books only; no strategy, order submission or ledger writes."""
    # Validate all sources before consuming or publishing new valuation facts.
    for pin in source_refs:
        require(file_ref(pin["path"]) == pin, "VALUATION_SOURCE_CHANGED")
    states, marks = read(states_ref), read(marks_ref)
    require([s["trade_date"] for s in states["days"]] == [m["trade_date"] for m in marks["days"]],
            "VALUATION_DATE_SET")
    source_facts = read(states["source_facts"])
    proof = valuation_states(source_facts)
    require(all(states[k] == v for k, v in proof.items()), "VALUATION_STATE_PROOF")
    identity = {"schema_version": "localsim_existing_book_valuation_request_v1", "states": states_ref,
                "marks": marks_ref, "source_refs": source_refs, "implementation_identity": implementation_identity()}
    digest = canonical_json_sha256(identity)
    root = Path(output_root).resolve()
    with FileLock(str(root) + ".lock"):
        request = publish(root / "request.json", identity)
        existing = root / "facts.json"
        validated = []
        for state, item in zip(states["days"], marks["days"], strict=True):
            held = {p["symbol"] for p in state["positions"] if p["quantity"]}
            typed = [LocalSimMarketMarkV1.model_validate(m) for m in item["marks"]]
            require({m.symbol for m in typed} == held and len(typed) == len(held)
                    and all(m.source == MinuteDataSource.DB_HISTORICAL.value
                        and m.as_of_time.astimezone(TZ).date().isoformat() == state["trade_date"]
                        and m.as_of_time.astimezone(TZ).time() == time(15) for m in typed), "VALUATION_MARK_SCOPE")
            validated.append((state, {m.symbol: m for m in typed}))
        if existing.exists():
            completion = read(file_ref(root / "completion.json"))
            result = read(completion["facts"])
            require(completion["facts"] == file_ref(existing)
                    and result["request_hash"] == digest and result["request"] == request, "VALUATION_RESULT_IDENTITY")
            return {"facts": completion["facts"], "recomputed_days": 0, "account_execution": False,
                    "new_database_economic_commits": 0, "source_preflight_complete": True}
        rows = []
        previous_nav = money(source_facts["account"]["initial_capital"])
        for state, prices in validated:
            value = sum((money(prices[p["symbol"]].price) * p["quantity"]
                         for p in state["positions"] if p["quantity"]), Decimal(0))
            nav = money(state["cash"]) + value
            rows.append({"trade_date": state["trade_date"], "cash": state["cash"], "market_value": str(value),
                "nav": str(nav), "valuation_day_pnl": str(nav - previous_nav),
                "cumulative_nav_pnl": str(nav - money(state["origin_initial_cash"])),
                "holding_unrealized_pnl": str(sum(((money(prices[p["symbol"]].price) - money(p["avg_cost"])) * p["quantity"]
                    for p in state["positions"] if p["quantity"]), Decimal(0))),
                "source_snapshot": state["source_snapshot"], "state_carry": state.get("state_carry"),
                "positions": state["positions"], "marks": [m.model_dump(mode="json") for m in prices.values()]})
            previous_nav = nav
        pin = publish(existing, {"schema_version": "localsim_existing_book_valuation_facts_v1",
            "request_hash": digest, "request": request, "account_id": states["account_id"], "days": rows,
            "account_execution": False, "original_accounts_modified": False, "new_capital": "0",
            "new_database_economic_commits": 0, "economic_confirmation": False})
        publish(root / "completion.json", {"facts": pin, "request_hash": digest})
        return {"facts": pin, "recomputed_days": len(rows), "account_execution": False,
                "new_database_economic_commits": 0, "source_preflight_complete": True}
