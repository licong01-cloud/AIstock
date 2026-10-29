"""Immutable, file-only, independent LocalSIM native/human account studies."""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from time import perf_counter

from filelock import FileLock, Timeout

from backend.services.position_timing.intraday.contracts import digest, payload
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import ARMS, DELAYS, CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot

from .human_operation import HumanOperationPolicy, operation_snapshot, validate_operation_configuration
from .economic import local_sim_fact_payload
from .human_portfolio import run_human_portfolio_day
from .native_research import native_backend_for, run_native_portfolio_day, run_cash_only_portfolio_day
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_campaign import build_timing_plan_events, inspect_price_templates, validate_campaign_request
from .timing_continuation import inspect_successor_sources
from .timing_portfolio import attach_native_quantity_evidence, commit_portfolio_day, compile_native_intents, validate_day_ledger, prepare_portfolio_commit
from .timing_research import ref, source_identity
from .timing_store import TimingFileRepository, publish, read_json


REQUEST_SCHEMA = "localsim_dual_mode_study_v1"
RESULT_SCHEMA = "localsim_dual_mode_results_v1"


def implementation_identity():
    root = Path(__file__).resolve().parents[4]
    names = ["human_operation.py", "human_portfolio.py", "native_research.py", "dual_mode_research.py",
             "timing_portfolio.py", "timing_campaign.py", "timing_continuation.py", "research_performance.py"]
    files = {"native": source_identity(), "dual_mode": {
        name: hashlib.sha256((root / "backend/services/simulation_execution/localsim" / name)
                            .read_bytes().replace(b"\r\n", b"\n")).hexdigest() for name in names}}
    files["native_execution"] = {name: hashlib.sha256((root / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                                  for name in ("backend/execution_algos/twap_algo.py", "backend/services/strategy_package/execution_policy.py")}
    # The producer commit binds the repository; the executable fingerprint
    # must also notice an owner-only upgrade when an old day prefix resumes.
    shared = {p for directory in ("backend/services/position_timing/intraday", "backend/services/trading_core")
              for p in (root / directory).glob("*.py")}
    shared.update(root / name for name in (
        "backend/services/strategy_package/runtime.py", "backend/services/strategy_package/models.py",
        "backend/services/strategy_package/manifest.py", "backend/services/selection_center/models.py",
        "backend/services/simulation_runtime/decision.py", "backend/execution_algos/base_algo.py",
        "backend/execution_algos/registry.py"))
    files["shared_authorities"] = {p.relative_to(root).as_posix():hashlib.sha256(
        p.read_bytes().replace(b"\r\n",b"\n")).hexdigest() for p in sorted(shared)}
    return files


def execution_contract():
    return {"schema_version": "localsim_dual_mode_execution_contract_v1",
            "native": {"execution_policy": local_sim_twap_only_policy_snapshot(), "proxy": "NATIVE_CAUSAL_MINUTE_TWAP"},
            "human": {"proxy": "NEXT_ELIGIBLE_BAR_OPEN", "strict_after": "HUMAN_DISPATCH_AT",
                      "split_allowed": False, "automatic_reprice": False, "capacity": "ACCOUNT_SINGLE_OPERATOR"},
            "visibility": "ASSUMED_BAR_END_PLUS_5_SECONDS", "recovery_unit": "ATOMIC_COMPLETED_DAY",
            "terminal_local": "14:57", "valuation": "LAST_COMPLETED_VISIBLE_RAW_MARK"}


def profiles_for(matrix):
    if matrix not in {"FULL_13", "BASIC_2"}:
        raise ValueError("LOCALSIM_DUAL_MODE_MATRIX_INVALID")
    profiles = [{"operation": operation_snapshot(), "rule": "LOCALSIM_NATIVE_TWAP", "delay_seconds": None}]
    combinations = [(r,d) for r in ARMS for d in DELAYS] if matrix == "FULL_13" else [(ARMS[0],60)]
    return profiles + [{"operation": operation_snapshot(HumanOperationPolicy(reaction_delay_seconds=d)),
                        "rule": r, "delay_seconds": d} for r,d in combinations]


def _source_inspect(source_request: FileRef, successor_ref: FileRef | None,
                    input_candidate: FileRef | None = None):
    if successor_ref is not None and input_candidate is not None:
        raise ValueError("LOCALSIM_DUAL_MODE_INPUT_BINDING_CONFLICT")
    original = validate_campaign_request(source_request)
    inspected = inspect_price_templates(FileRef.model_validate(original["preflight"]),
                                         FileRef.model_validate(original["price_templates"]))
    candidate = FileRef.model_validate(inspected["preflight"]["inputs"]["candidate_manifest"])
    if successor_ref is not None:
        successor = read_json(checked_file(successor_ref))
        fields = {k: v for k, v in successor.items() if k not in {"identity", "release_identity"}}
        if (successor.get("schema_version") != "localsim_timing_input_successor_v1"
                or digest(fields) != successor.get("identity")
                or successor.get("request") != source_request.model_dump()
                or successor.get("original_candidate") != candidate.model_dump()
                or successor.get("price_templates") != original["price_templates"]):
            raise ValueError("LOCALSIM_DUAL_MODE_INPUT_SUCCESSOR_INVALID")
        candidate = FileRef.model_validate(successor["candidate"])
        inspect_successor_sources(inspected, candidate)  # Price/PIT pins; no old account results.
    if input_candidate is not None:
        # A fresh study has no economic prefix to migrate. Bind its explicit
        # candidate only after the same complete price/PIT equivalence check;
        # neither the source request nor an old run's successor is rewritten.
        checked_file(input_candidate)
        inspect_successor_sources(inspected, input_candidate)
        candidate = input_candidate
    return original, inspected, candidate


def freeze_study(*, source_request: FileRef, output_root: Path, producer_commit: str,
                 input_successor: FileRef | None = None, short_window_days: int | None = None,
                 matrix: str = "FULL_13", input_candidate: FileRef | None = None) -> dict:
    if not re.fullmatch(r"[a-f0-9]{40}", producer_commit) or matrix not in {"FULL_13", "BASIC_2"}:
        raise ValueError("LOCALSIM_DUAL_MODE_REQUEST_CONFIG_INVALID")
    original, inspected, candidate = _source_inspect(source_request, input_successor, input_candidate)
    days = list(inspected["days"])
    if days != original["trade_dates"] or not days:
        raise ValueError("LOCALSIM_DUAL_MODE_SOURCE_DATES_CONFLICT")
    if short_window_days is not None:
        if type(short_window_days) is not int or not 1 <= short_window_days < len(days):
            raise ValueError("LOCALSIM_DUAL_MODE_SHORT_WINDOW_INVALID")
        days = days[:short_window_days]
    population = profiles_for(matrix)
    request = {"schema_version": REQUEST_SCHEMA, "producer": "LOCALSIM", "producer_source_commit": producer_commit,
               "source_request": source_request.model_dump(),
               "input_successor": input_successor.model_dump() if input_successor else None,
               "candidate_manifest": candidate.model_dump(), "source_identity": implementation_identity(),
               "initial_cash_cny": "10000000", "trade_dates": days, "matrix": matrix,
               "purpose": "ENGINEERING_SHORT_WINDOW" if short_window_days else "CONTINUOUS_HISTORY",
               "population": population, "execution_contract": execution_contract(),
               "manifest_sha256": inspected["preflight"]["manifest_sha256"],
               "package_id": inspected["preflight"]["package_id"],
               "package_snapshot": inspected["preflight"]["inputs"]["package"],
               "comparison": "COMMON_EXOGENOUS_TARGETS_FEEDBACK_DIVERGENT_QUANTITIES",
               "families": ["NATIVE_VS_HUMAN_WORKFLOW", "HUMAN_MATCHED_RULES"],
               "common_terminal_local": "14:57", "max_drawdown_definition": "POSITIVE_PEAK_TO_TROUGH_FRACTION"}
    if input_candidate is not None:
        request["input_candidate"] = input_candidate.model_dump()
    request["identity"] = digest(request)
    request["release_identity"] = "localsim_dual_mode_v1_" + request["identity"][:24]
    path = output_root / request["identity"] / "request.json"
    publish(path, request)
    return {"status": "FROZEN", "request": ref(path), "account_count": len(population), "outcomes_read": False}


def inspect_study(request_ref: FileRef):
    request = read_json(checked_file(request_ref))
    fields = {k: v for k, v in request.items() if k not in {"identity", "release_identity"}}
    if (request.get("schema_version") != REQUEST_SCHEMA or digest(fields) != request.get("identity")
            or request.get("release_identity") != "localsim_dual_mode_v1_" + request["identity"][:24]
            or request["source_identity"] != implementation_identity()):
        raise ValueError("LOCALSIM_DUAL_MODE_REQUEST_IDENTITY_CONFLICT")
    _, inspected, candidate_ref = _source_inspect(FileRef.model_validate(request["source_request"]),
                                                FileRef.model_validate(request["input_successor"]) if request["input_successor"] else None,
                                                FileRef.model_validate(request["input_candidate"]) if "input_candidate" in request else None)
    if (candidate_ref.model_dump() != request["candidate_manifest"]
            or request["initial_cash_cny"] != "10000000" or request["common_terminal_local"] != "14:57"
            or request["manifest_sha256"] != inspected["preflight"]["manifest_sha256"]
            or request["package_snapshot"] != inspected["preflight"]["inputs"]["package"]
            or request["package_id"] != inspected["preflight"]["package_id"]
            or not request["trade_dates"]
            or request["purpose"] not in {"CONTINUOUS_HISTORY", "ENGINEERING_SHORT_WINDOW"}
            or request["purpose"] == "CONTINUOUS_HISTORY" and len(request["trade_dates"]) != len(inspected["days"])
            or request["trade_dates"] != list(inspected["days"])[:len(request["trade_dates"])]) :
        raise ValueError("LOCALSIM_DUAL_MODE_INPUT_IDENTITY_CONFLICT")
    if (digest(request["population"]) != digest(profiles_for(request["matrix"]))
            or digest(request["execution_contract"]) != digest(execution_contract())):
        raise ValueError("LOCALSIM_DUAL_MODE_POPULATION_CONFLICT")
    for profile in request["population"]:
        mode = validate_operation_configuration(profile["operation"], algo_code=(
            "TWAP" if profile["rule"] == "LOCALSIM_NATIVE_TWAP" else "POSITION_TIMING_INTRADAY_V1"))
        if mode["operation_mode"] == "HUMAN_SIM" and profile["delay_seconds"] != mode["human_policy"]["reaction_delay_seconds"]:
            raise ValueError("LOCALSIM_DUAL_MODE_DELAY_CONFLICT")
    return request, inspected, candidate_ref


def summarize_accounts(accounts, histories=None):
    rows = []
    native = next(a for a in accounts if a["operation"]["operation_mode"] == "NATIVE")
    humans = {a["delay_seconds"]: a for a in accounts if a["rule"] == "FIRST_OBSERVABLE_V1"}
    for account in accounts:
        if account is native:
            continue
        for family, baseline in [("NATIVE_VS_HUMAN_WORKFLOW", native),
                                  ("HUMAN_MATCHED_RULES", humans.get(account["delay_seconds"]))]:
            if baseline is None:
                continue
            paired = {}
            if histories is not None:
                def plan_rows(which):
                    return {(d["trade_date"], p["symbol"], p["side"]): p
                            for d in histories[which["account_id"]] for p in d["parents"]}
                left, right = plan_rows(account), plan_rows(baseline)
                for side in ("ALL", "BUY", "SELL"):
                    # Decimal sums depend on order. Freeze parent order so a
                    # fresh process regenerates the same immutable report bytes.
                    population = sorted(k for k in left.keys() | right.keys() if side == "ALL" or k[2] == side)
                    both = [k for k in population if k in left and k in right
                            and left[k]["filled_quantity"] > 0 and right[k]["filled_quantity"] > 0]
                    differences = [(Decimal(str(left[k]["average_price_raw"])) / Decimal(str(right[k]["average_price_raw"])) - 1)
                                   * (1 if k[2] == "BUY" else -1) * 10000 for k in both]
                    paired[side] = {"paired_count": len(both), "union_plan_count": len(population),
                                    "missing_fraction": str(1-Decimal(len(both))/len(population)) if population else None,
                                    "conditional_price_cost_bps": str(sum(differences)/len(differences)) if differences else None,
                                    "net_increment_cny": None, "mdd_reduction": None,
                                    "attribution": "CONTINUOUS_FEEDBACK_NOT_SIDE_SEPARABLE"}
            rows.append({"family": family, "account_id": account["account_id"], "baseline_account_id": baseline["account_id"],
                         "rule": account["rule"], "delay_seconds": account["delay_seconds"],
                         "net_increment_cny": str(Decimal(account["terminal_nav_cny"]) - Decimal(baseline["terminal_nav_cny"])),
                         "mdd_reduction": str(Decimal(baseline["max_drawdown"]) - Decimal(account["max_drawdown"])),
                         "fee_difference_cny": str(Decimal(account["fees_cny"]) - Decimal(baseline["fees_cny"])),
                         "conditional_execution": paired,
                         "quantity_matched": False, "pure_timing_attribution": False})
    return rows


def run_study(request_ref: FileRef, *, stop_after_account_days: int | None = None,
              performance: ResearchPerformanceOptions | None = None, measure_performance: bool = False) -> dict:
    if performance is not None and not isinstance(performance, ResearchPerformanceOptions):
        raise ValueError("LOCALSIM_RESEARCH_PERFORMANCE_OPTIONS_INVALID")
    performance = performance or ResearchPerformanceOptions()
    if type(measure_performance) is not bool:
        raise ValueError("LOCALSIM_RESEARCH_PERFORMANCE_OPTIONS_INVALID")
    metrics = {"source_inspection_seconds": 0.0, "market_preparation_seconds": 0.0, "commit_readback_seconds": 0.0}
    started = perf_counter()
    root = checked_file(request_ref).parent
    try:
        with FileLock(root / "study-writer.lock", timeout=0):
            try:
                with ResearchAccountPool(performance.account_workers) as pool, ExitStack() as resources:
                    result = _run_study_locked(request_ref, stop_after_account_days=stop_after_account_days,
                                                performance=performance, pool=pool, metrics=metrics, resources=resources)
                if measure_performance:
                    result = {**result, "performance": {"schema_version": "localsim_research_performance_v1",
                        "batch_days": performance.batch_days, "account_workers": performance.account_workers,
                        "optimized": performance.optimized, "wall_seconds": perf_counter() - started,
                        "account_compute_seconds_sum": pool.compute_seconds, **metrics}}
                return result
            except Exception as exc:
                # Bounded, immutable operational evidence, separate from economics.
                message = str(exc).split(":", 1)[0]
                reason = message if re.fullmatch(r"[A-Z][A-Z0-9_]{1,150}", message) else "OFFLINE_RESEARCH_ERROR"
                failure = {"schema_version": "localsim_dual_mode_failure_v1", "request": request_ref.model_dump(),
                           "exception_type": getattr(exc, "native_exception_type", type(exc).__name__), "reason_code": reason,
                           "generation_file_count": len(list((root / "journals").glob("*/generations/*.json"))),
                           "economic_result_published": (root / "result_receipt.json").exists()}
                publish(root / "operational" / (digest(failure) + ".json"), failure)
                raise
    except Timeout:
        return {"status": "IN_PROGRESS", "computation_performed": False, "outcomes_read": False}


def _account_day_identity(*, request_identity: str, account_id: str, trade_date: str, prior: dict) -> str:
    """Bind causal inputs independently of receipt paths and processing clocks.

    The immutable previous-day FileRef and native journal retain full physical
    lineage validation. Only the prior state consumed by account computation
    enters its logical identity, so resource/output choices preserve plan IDs.
    """
    opening = {"cash_cny": prior["cash_cny"], "positions": prior["positions"],
               "marks_raw_cny": prior.get("marks_raw_cny", {})}
    return digest(["localsim_dual_mode_account_day_v2", request_identity, account_id, trade_date, opening])


def _compute_account_day(job: dict) -> dict:
    """Only compute native facts; a worker cannot publish a journal or result."""
    initial, prior = job["initial"], job["prior"]
    markets, projection, cost = job["markets"], job["projection"], job["cost"]
    day, identity, manifest = job["day"], job["identity"], job["manifest"]
    account_input = {**initial, "cash_cny": prior["cash_cny"], "positions": prior["positions"]}
    prices = {s: m.day_facts.reference_price for s, m in markets.items() if m.day_facts.reference_price is not None}
    prices.update({r["symbol"]: Decimal(str(r["reference_price"])) for r in projection["aggregate_results"]
                   if r.get("reference_price") is not None})
    for symbol in prior["positions"]:
        if symbol not in prices:
            if not markets[symbol].day_facts.is_suspended or symbol not in prior.get("marks_raw_cny", {}):
                raise ValueError("LOCALSIM_DUAL_MODE_HELD_MARK_MISSING")
            prices[symbol] = Decimal(prior["marks_raw_cny"][symbol])
    if markets:
        backend, targets, native = compile_native_intents(projection=projection, manifest=manifest,
            account=account_input, markets=markets, prices=prices, cost=cost)
    else:
        clock = projection["runtime_config"]["point_in_time_context"]
        if (projection.get("valid_no_candidate") is not True or account_input["positions"]
                or projection["manifest_sha256_by_package"] != {account_input["package_id"]:job["manifest_sha256"]}
                or clock["effective_trade_date"] != day.isoformat() or clock["cutoff_date"] >= day.isoformat()):
            raise ValueError("LOCALSIM_DUAL_MODE_EMPTY_MARKET_WITHOUT_AUTHORITY")
        from backend.services.simulation_runtime.decision import RebalanceIntentService
        targets = []
        native = RebalanceIntentService().build_order_intents(package_id=account_input["package_id"],
            portfolio_id=initial["account_id"], strategy_id=initial["account_id"], trade_date=day, current_positions={},
            target_positions=[], current_prices={})
        backend = native_backend_for(account=account_input, markets={}, cost=cost, manifest=manifest,
                                     source_identity=identity, cash_only_trade_date=day)
    unavailable = []
    if not markets:
        human_policy = HumanOperationPolicy.model_validate(initial["operation"]["human_policy"]) if initial["operation"]["operation_mode"] == "HUMAN_SIM" else None
        result = run_cash_only_portfolio_day(backend=backend, trade_date=day, human_policy=human_policy)
    elif initial["operation"]["operation_mode"] == "NATIVE":
        backend = native_backend_for(account=account_input, markets=markets, cost=cost,
                                       manifest=manifest, source_identity=identity)
        result = run_native_portfolio_day(backend=backend, markets=markets, intents=native.order_intents,
            initial_marks=prices, run_id=job["run_id"], source_commit=job["producer_commit"], optimized=job["optimized"])
    else:
        source = job["source"]
        events, unavailable = build_timing_plan_events(intents=native.order_intents,
            series=source["series"], rule=initial["rule"], source_commit=job["producer_commit"],
            day=day, quantity_source=identity, unknown_references=source["unknown_references"])
        result = run_human_portfolio_day(backend=backend, markets=markets, events=events,
            initial_marks=prices, policy=HumanOperationPolicy.model_validate(initial["operation"]["human_policy"]),
            run_id=job["run_id"], optimized=job["optimized"])
    attach_native_quantity_evidence(result, targets=targets, native=native,
        projection_ref=job["projection_ref"], price_policy_sha256=job["price_policy_sha256"], unavailable=unavailable)
    result["account"].update(operation=initial["operation"], rule=initial["rule"], delay_seconds=initial["delay_seconds"])
    commit = prepare_portfolio_commit(result=result, run_id=job["run_id"], input_identity=identity, day_index=0)
    return {"account": result["account"], "commit_request": commit}


def _run_study_locked(request_ref: FileRef, *, stop_after_account_days: int | None = None,
                      performance: ResearchPerformanceOptions, pool: ResearchAccountPool, metrics: dict, resources: ExitStack) -> dict:
    """Resume only complete day journals; exact retry reads immutable results."""
    inspect_started = perf_counter()
    request, inspected, candidate_ref = inspect_study(request_ref)
    preflight = inspected["preflight"]
    package = read_json(checked_file(FileRef.model_validate(preflight["inputs"]["package"])))["package"]
    if compute_manifest_json_sha256(package["manifest"]) != request["manifest_sha256"]:
        raise ValueError("LOCALSIM_DUAL_MODE_MANIFEST_CHANGED")
    manifest = StrategyPackageManifest.model_validate(package["manifest"])
    cost_ref = FileRef.model_validate(preflight["inputs"]["cost_policy"])
    cost = CostContract.model_validate_json(checked_file(cost_ref).read_text(encoding="utf-8"))
    candidate = resources.enter_context(ResearchMarketWorkspace(CandidateReader(candidate_ref), performance.batch_days))
    root = checked_file(request_ref).parent
    inspection = {"schema_version": "localsim_dual_mode_source_inspection_v1", "request": request_ref.model_dump(),
                  "candidate_manifest": candidate_ref.model_dump(), "stage": "COMPLETE_SOURCE_BATCH_INSPECTED",
                  "source_identity": request["source_identity"], "trade_dates": request["trade_dates"],
                  "price_policy": inspected["templates"].policy_document.model_dump()}
    publish(root / "source_inspection.json", inspection)
    metrics["source_inspection_seconds"] = perf_counter() - inspect_started
    completed = root / "results.json"
    initials = []
    for profile in request["population"]:
        account_id = "lsdm_" + digest([request["identity"], profile])[:40]
        initials.append({"account_id": account_id, "portfolio_id": account_id, "package_id": request["package_id"],
                         "manifest_sha256": request["manifest_sha256"], "origin_initial_cash_cny": "10000000",
                         "cash_cny": "10000000", "positions": {}, **profile})
    histories = {i["account_id"]: [] for i in initials}
    references = {i["account_id"]: [] for i in initials}
    computed = 0
    prepared_projections = {}
    for index, day_string in enumerate(request["trade_dates"]):
        market_started = perf_counter()
        day = date.fromisoformat(day_string)
        source = inspected["days"][day_string]
        projection_ref = FileRef.model_validate(source["native"]["projection"])
        projection_path = checked_file(projection_ref)
        projection = prepared_projections.get(day_string) or read_json(projection_path)
        symbols = {r["symbol"] for r in projection["aggregate_results"]}
        for rows in histories.values():
            if rows:
                symbols.update(rows[-1]["positions"])
        if performance.batch_days > 1 and index % performance.batch_days == 0:
            window = request["trade_dates"][index:index + performance.batch_days]
            prepared_projections = {}
            physical_symbols = set(symbols)
            for future_day in window:
                future_ref = FileRef.model_validate(inspected["days"][future_day]["native"]["projection"])
                future_projection = read_json(checked_file(future_ref))
                prepared_projections[future_day] = future_projection
                physical_symbols.update(r["symbol"] for r in future_projection["aggregate_results"])
            candidate.prepare_window(date.fromisoformat(window[0]), date.fromisoformat(window[-1]), sorted(physical_symbols))
        markets = {s: candidate.market(s, day) for s in sorted(symbols)}
        candidate.verify_pins()
        metrics["market_preparation_seconds"] += perf_counter() - market_started
        jobs, missing_count = [], 0
        remaining = None if stop_after_account_days is None else max(0, stop_after_account_days - computed)
        for initial in initials:
            account_id = initial["account_id"]
            prior = histories[account_id][-1] if histories[account_id] else initial
            identity = _account_day_identity(request_identity=request["identity"], account_id=account_id,
                                             trade_date=day_string, prior=prior)
            run_id = "lsdm_" + identity[:48]
            if ((root / "journals" / run_id / "generations/00000001.json").exists()
                    or remaining is not None and missing_count >= remaining):
                jobs.append(None)
                continue
            missing_count += 1
            jobs.append({"initial": initial, "prior": prior, "markets": markets, "projection": projection,
                         "projection_ref": projection_ref.model_dump(), "source": source, "cost": cost,
                         "manifest": manifest, "manifest_sha256": request["manifest_sha256"],
                         "producer_commit": request["producer_source_commit"], "day": day,
                         "identity": identity, "run_id": run_id, "optimized": performance.optimized,
                         "price_policy_sha256": inspected["templates"].policy_document.sha256})
        computations = pool.prepare(jobs)
        for account_index, initial in enumerate(initials):
            account_id = initial["account_id"]
            prior = histories[account_id][-1] if histories[account_id] else initial
            previous = references[account_id][-1] if references[account_id] else None
            identity = _account_day_identity(request_identity=request["identity"], account_id=account_id,
                                             trade_date=day_string, prior=prior)
            store = TimingFileRepository(root / "journals", "lsdm_" + identity[:48])
            path = root / "days" / account_id / (day_string + ".json")
            record = store.latest()
            if record is not None:
                if (record["generation"] != 1 or record["checkpoint"]["extra"] != {"input_identity": identity, "day_index": 0}
                        or record["checkpoint"]["account"]["portfolio_id"] != account_id
                        or record["checkpoint"]["account"]["trade_date"] != day_string):
                    raise ValueError("LOCALSIM_DUAL_MODE_DAY_IDENTITY_CONFLICT")
                validate_day_ledger(record, initial=initial, cost=cost)
                account = record["checkpoint"]["account"]
                if Decimal(account["initial_account"]["cash_cny"]) != Decimal(prior["cash_cny"]) or account["initial_account"]["positions"] != prior["positions"]:
                    # T+1 is settled by the existing native ledger at day start.
                    from .timing_research import backend_for
                    opening = backend_for({**initial, "cash_cny": prior["cash_cny"], "positions": prior["positions"]},
                                          next(iter(markets.values())), cost)
                    opening._ledger.settle_trade_date(day)
                    actual = {s: p.model_dump(mode="json") for s, p in opening._ledger.positions.items()}
                    if Decimal(account["initial_account"]["cash_cny"]) != Decimal(prior["cash_cny"]) or account["initial_account"]["positions"] != actual:
                        raise ValueError("LOCALSIM_DUAL_MODE_OPENING_ACCOUNT_CONFLICT")
            else:
                if completed.exists() or path.exists():
                    raise ValueError("LOCALSIM_DUAL_MODE_COMMITTED_PREFIX_MISSING")
                if stop_after_account_days is not None and computed >= stop_after_account_days:
                    return {"status": "CHECKPOINTED", "computed_account_days": computed, "outcomes_read": False}
                result = computations.result(account_index)
                commit_started = perf_counter()
                account_result = result["account"]
                if (account_result["portfolio_id"] != account_id or account_result["package_id"] != initial["package_id"]
                        or account_result["trade_date"] != day_string or account_result["operation"] != initial["operation"]
                        or account_result["rule"] != initial["rule"] or account_result["delay_seconds"] != initial["delay_seconds"]):
                    raise ValueError("LOCALSIM_RESEARCH_WORKER_ACCOUNT_SCOPE_CONFLICT")
                commit = result["commit_request"]
                # A failed replay must not publish even a completed-day journal.
                validate_day_ledger({"checkpoint":{"account":result["account"]}, "facts":{
                    "fills":{f.fill_id:local_sim_fact_payload(f, fact_type="fill") for f in commit.fills},
                    "cash_entries":{e.fill_id:local_sim_fact_payload(e, fact_type="cash_entry") for e in commit.cash_entries}}},
                    initial=initial, cost=cost)
                commit_portfolio_day(store=store, result=result, input_identity=identity, day_index=0)
                record = store.latest()
                validate_day_ledger(record, initial=initial, cost=cost)
                account = record["checkpoint"]["account"]
                computed += 1
                metrics["commit_readback_seconds"] += perf_counter() - commit_started
            summary = {"schema_version": "localsim_dual_mode_day_v1", "study_identity": request["identity"],
                       "account_id": account_id, "day_index": index, "previous_day": previous,
                       "account_receipt": ref(store.root / "generations/00000001.json"), **account,
                       "view_counts": {"positions": len(account["positions"]), "pnl": 1,
                                       "trades": len(record["facts"]["fills"]), "orders": len(record["facts"]["orders"]),
                                       "actions": len(account.get("human_operation_state", {}).get("actions", []))}}
            publish(path, summary)
            # Keep restart inputs and economic observations, not duplicated
            # multi-MB order/queue audit snapshots for every completed day.
            histories[account_id].append({k: summary[k] for k in (
                "trade_date", "cash_cny", "positions", "marks_raw_cny", "nav_observations",
                "terminal_nav_cny", "initial_nav_cny", "fees_cny", "view_counts")} | {
                "human_operation_metrics": summary.get("human_operation_metrics", {}),
                "parents": [{k:p[k] for k in ("symbol", "side", "target_quantity", "filled_quantity", "status",
                                                "reason_codes", "average_price_raw", "weighted_wait_seconds")}
                            for p in summary["parents"]]})
            references[account_id].append(ref(path))
    accounts = []
    for initial in initials:
        days = histories[initial["account_id"]]
        values = [Decimal("10000000")] + [Decimal(n["nav_cny"]) for d in days for n in d["nav_observations"]]
        peak, mdd = values[0], Decimal(0)
        for value in values:
            peak = max(peak, value)
            mdd = max(mdd, (peak-value)/peak)
        statuses, reasons = Counter(), Counter()
        for day in days:
            for parent in day["parents"]:
                statuses[parent["status"]] += 1
                reasons.update(parent["reason_codes"])
        execution_by_side = {}
        for side in ("ALL", "BUY", "SELL"):
            parents = [p for d in days for p in d["parents"] if side == "ALL" or p["side"] == side]
            quantity = sum(p["filled_quantity"] for p in parents)
            execution_by_side[side] = {"plan_count": len(parents),
                "filled_plan_count": sum(p["status"] == "FILLED" for p in parents),
                "target_quantity": sum(p["target_quantity"] for p in parents), "filled_quantity": quantity,
                "quantity_weighted_wait_seconds": str(sum(Decimal(p["weighted_wait_seconds"]) for p in parents)/quantity) if quantity else None}
        accounts.append({"account_id": initial["account_id"], "operation": initial["operation"], "rule": initial["rule"],
                         "delay_seconds": initial["delay_seconds"], "initial_cash_cny": "10000000",
                         "initial_account_sha256": digest({"cash_cny": "10000000", "positions": {}}),
                         "terminal_nav_cny": str(values[-1]), "max_drawdown": str(mdd),
                         "fees_cny": str(sum((Decimal(d["fees_cny"]) for d in days), Decimal(0))),
                         "status_counts": dict(statuses), "reason_counts": dict(reasons),
                         "execution_by_side": execution_by_side,
                         "daily_nav": [{"date": d["trade_date"], "nav_cny": d["terminal_nav_cny"],
                                        "opening_nav_cny": d["initial_nav_cny"], "fees_cny": d["fees_cny"]} for d in days],
                         "day_views": [{"date":d["trade_date"], "counts":d["view_counts"]} for d in days],
                         "human_operation_metrics": {key: sum(d.get("human_operation_metrics", {}).get(key, 0) for d in days)
                                                     for key in ("action_count", "expired_actions", "pending_cancellation_at_deadline", "cancellation_races")},
                         "days": references[initial["account_id"]]})
    results = {"schema_version": RESULT_SCHEMA, "producer": "LOCALSIM", "request": request_ref.model_dump(),
               "producer_source_commit": request["producer_source_commit"], "release_identity": request["release_identity"],
               "source_inspection": ref(root / "source_inspection.json"),
               "study_identity": request["identity"], "manifest_sha256": request["manifest_sha256"],
               "cost_policy_file_sha256": cost_ref.sha256, "execution_contract_sha256": digest(request["execution_contract"]),
               "initial_cash_cny": "10000000", "trade_dates": request["trade_dates"],
               "max_drawdown_definition": request["max_drawdown_definition"], "accounts": accounts,
               "comparisons": summarize_accounts(accounts, histories), "purpose": request["purpose"],
               "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION", "economic_confirmation": False,
               "production_account_created": False, "database_accessed": False, "dataset_mutated": False,
               "market_network_accessed": False, "service_process_control": False}
    existed = completed.exists()
    publish(completed, payload(results))
    publish(root / "result_receipt.json", {"schema_version": "localsim_dual_mode_result_receipt_v1",
                                           "request": request_ref.model_dump(), "results": ref(completed)})
    return {"status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "results": ref(completed),
            "computed_account_days": computed, "account_count": len(accounts),
            "outcomes_read_stage": results["outcomes_read_stage"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--source-request", required=True)
    freeze.add_argument("--source-request-sha256", required=True)
    freeze.add_argument("--input-successor")
    freeze.add_argument("--input-successor-sha256")
    freeze.add_argument("--candidate")
    freeze.add_argument("--candidate-sha256")
    freeze.add_argument("--output-root", type=Path, required=True)
    freeze.add_argument("--producer-commit", required=True)
    freeze.add_argument("--short-window-days", type=int)
    freeze.add_argument("--matrix", choices=["FULL_13", "BASIC_2"], default="FULL_13")
    run = sub.add_parser("run")
    run.add_argument("--request", required=True)
    run.add_argument("--request-sha256", required=True)
    run.add_argument("--stop-after-account-days", type=int)
    run.add_argument("--batch-days", type=int, default=1)
    run.add_argument("--account-workers", type=int, default=1)
    run.add_argument("--optimized", action="store_true")
    run.add_argument("--measure-performance", action="store_true")
    args = parser.parse_args()
    if args.command == "freeze":
        if bool(args.input_successor) != bool(args.input_successor_sha256):
            parser.error("input successor path and complete SHA must be supplied together")
        if bool(args.candidate) != bool(args.candidate_sha256):
            parser.error("candidate path and complete SHA must be supplied together")
        if args.input_successor and args.candidate:
            parser.error("an existing successor and a fresh candidate are mutually exclusive")
        result = freeze_study(source_request=FileRef(path=args.source_request, sha256=args.source_request_sha256),
            input_successor=FileRef(path=args.input_successor, sha256=args.input_successor_sha256) if args.input_successor else None,
            input_candidate=FileRef(path=args.candidate, sha256=args.candidate_sha256) if args.candidate else None,
            output_root=args.output_root, producer_commit=args.producer_commit,
            short_window_days=args.short_window_days, matrix=args.matrix)
    else:
        result = run_study(FileRef(path=args.request, sha256=args.request_sha256),
                           stop_after_account_days=args.stop_after_account_days,
                           performance=ResearchPerformanceOptions(batch_days=args.batch_days,
                               account_workers=args.account_workers, optimized=args.optimized),
                           measure_performance=args.measure_performance)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
