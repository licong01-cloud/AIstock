"""Independent, file-only account replay from a frozen period opening."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from time import perf_counter

from filelock import FileLock

from backend.services.position_timing.intraday.contracts import CHINA, digest, payload
from backend.services.position_timing.intraday.continuous_results import PeriodAnalysisSpec, period_source_preflight
from backend.services.position_timing.intraday.period_context import load_period_context
from backend.services.position_timing.intraday.price_plan_campaign import inspect as inspect_price_manifest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot

from . import dual_mode_research as dual
from .models import LocalSimExecutionStateV1
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_campaign import inspect_price_templates
from .timing_portfolio import validate_day_ledger
from .timing_research import fee_model, ref
from .timing_store import TimingFileRepository, publish, read_json

REQUEST_SCHEMA = "localsim_period_study_v2"
RESULT_SCHEMA = "localsim_period_results_v2"
DAY_SCHEMA = "localsim_period_day_v2"


def require(condition, code):
    if not condition:
        raise ValueError("LOCALSIM_PERIOD_RESEARCH_" + code)


def read(source):
    return read_json(checked_file(FileRef.model_validate(source)))


def implementation_identity():
    return {
        "shared_account_engine": dual.implementation_identity(),
        "period_runner": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
    }


def _sources(preflight_ref, templates_ref, manifest_ref):
    preflight, preparation, openings, period_refs = load_period_context(preflight_ref)
    manifest = read(manifest_ref)
    require(manifest["spec"]["preflight"] == preflight_ref.model_dump(), "PRICE_MANIFEST_SCOPE")
    inspected_manifest = inspect_price_manifest(manifest_ref)
    require(inspected_manifest["templates"] == templates_ref.model_dump(), "PRICE_MANIFEST_TEMPLATES")
    inspected = inspect_price_templates(preflight_ref, templates_ref)
    require(
        inspected["stage"] == "COMPLETE_SOURCE_BATCH_INSPECTED" and inspected["outcomes_read"] is False,
        "SOURCE_INSPECTION_INCOMPLETE",
    )
    refs = [*period_refs, *inspected["source_refs"], manifest_ref]
    # The frozen v2 preparation has already audited these raw market semantics.
    # Revalidate every consumed file's exact bytes; do not replay its parent days.
    refs += [FileRef.model_validate(r) for r in preflight["market_source_preflight"]["candidate_source_pins"]]
    unique = {}
    for source in refs:
        require(source.path not in unique or unique[source.path].sha256 == source.sha256, "SOURCE_PIN_CONFLICT")
        unique[source.path] = source
    for source in unique.values():
        checked_file(source)
    inspected["source_refs"] = [r for _, r in sorted(unique.items())]
    return preflight, preparation, openings, inspected


def freeze_period(
    *, preflight_ref: FileRef, templates_ref: FileRef, manifest_ref: FileRef, output_root: Path, producer_commit: str
) -> dict:
    require(re.fullmatch(r"[a-f0-9]{40}", producer_commit), "PRODUCER_INVALID")
    p, preparation, openings, inspected = _sources(preflight_ref, templates_ref, manifest_ref)
    destination = output_root.resolve()
    protected_roots = (
        Path(preflight_ref.path).parent,
        Path(manifest_ref.path).parent,
        Path(p["inputs"]["candidate_manifest"]["path"]).parent,
    )
    require(
        all(destination != r.resolve() and r.resolve() not in destination.parents for r in protected_roots),
        "OUTPUT_OVERLAPS_INPUT",
    )
    request = {
        "schema_version": REQUEST_SCHEMA,
        "producer": "LOCALSIM",
        "producer_source_commit": producer_commit,
        "source_identity": implementation_identity(),
        "preflight": preflight_ref.model_dump(),
        "price_templates": templates_ref.model_dump(),
        "price_manifest": manifest_ref.model_dump(),
        "price_policy": inspected["templates"].policy_document.model_dump(),
        "preparation_request": p["preparation_request"],
        "initial_accounts": p["initial_accounts"],
        "package_id": p["package_id"],
        "manifest_sha256": p["manifest_sha256"],
        "candidate_manifest": p["inputs"]["candidate_manifest"],
        "cost_policy": p["inputs"]["cost_policy"],
        "execution_contract": preparation["execution_contract"],
        "trade_dates": p["trade_dates"],
        "population": [{k: a[k] for k in ("account_id", "rule", "delay_seconds", "operation")} for a in openings],
        "opening_semantics": "INHERIT_FROZEN_PARENT_NO_CAPITAL_ADDED",
        "common_terminal_local": "14:57",
        "decision_ttl_seconds": 90,
        "primary_delay_seconds": 60,
        "account_count": 13,
        "max_drawdown_definition": "POSITIVE_PEAK_TO_TROUGH_FRACTION",
        "purpose": "INDEPENDENT_PERIOD_CONTINUATION",
    }
    request["identity"] = digest(request)
    request["release_identity"] = "localsim_period_v2_" + request["identity"][:24]
    root = output_root / request["identity"]
    publish(root / "request.json", request)
    return {
        "status": "FROZEN",
        "request": ref(root / "request.json"),
        "account_count": 13,
        "trade_day_count": len(p["trade_dates"]),
        "replay_started": False,
    }


def _inspect_request(request_ref):
    request = read(request_ref)
    identity = digest({k: v for k, v in request.items() if k not in {"identity", "release_identity"}})
    require(
        request["schema_version"] == REQUEST_SCHEMA
        and request["identity"] == identity
        and request["release_identity"] == "localsim_period_v2_" + identity[:24]
        and request["source_identity"] == implementation_identity(),
        "REQUEST_IDENTITY_CONFLICT",
    )
    p, preparation, openings, inspected = _sources(
        FileRef.model_validate(request["preflight"]),
        FileRef.model_validate(request["price_templates"]),
        FileRef.model_validate(request["price_manifest"]),
    )
    require(
        request["preparation_request"] == p["preparation_request"]
        and request["initial_accounts"] == p["initial_accounts"]
        and request["trade_dates"] == p["trade_dates"]
        and request["execution_contract"] == preparation["execution_contract"] == dual.execution_contract()
        and request["cost_policy"] == p["inputs"]["cost_policy"]
        and request["candidate_manifest"] == p["inputs"]["candidate_manifest"]
        and request["manifest_sha256"] == p["manifest_sha256"]
        and request["package_id"] == p["package_id"]
        and request["population"]
        == [{k: a[k] for k in ("account_id", "rule", "delay_seconds", "operation")} for a in openings]
        and request["price_policy"] == inspected["templates"].policy_document.model_dump(),
        "INPUT_BINDING_CONFLICT",
    )
    require(
        request["producer"] == "LOCALSIM"
        and re.fullmatch(r"[a-f0-9]{40}", request["producer_source_commit"])
        and request["account_count"] == len(openings) == 13
        and request["opening_semantics"] == "INHERIT_FROZEN_PARENT_NO_CAPITAL_ADDED"
        and request["common_terminal_local"] == "14:57"
        and request["decision_ttl_seconds"] == 90
        and request["primary_delay_seconds"] == 60
        and request["max_drawdown_definition"] == "POSITIVE_PEAK_TO_TROUGH_FRACTION"
        and request["purpose"] == "INDEPENDENT_PERIOD_CONTINUATION",
        "STUDY_CONTRACT_CONFLICT",
    )
    return request, p, openings, inspected


def _raw_source(request_ref, request, inspected):
    return {
        "schema_version": "localsim_period_raw_source_inspection_v2",
        "request": request_ref.model_dump(),
        "preflight": request["preflight"],
        "price_templates": request["price_templates"],
        "price_manifest": request["price_manifest"],
        "candidate_manifest": request["candidate_manifest"],
        "trade_dates": request["trade_dates"],
        "producer_source_commit": request["producer_source_commit"],
        "source_identity": request["source_identity"],
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED",
        "source_refs": [r.model_dump() for r in inspected["source_refs"]],
        "new_period_outcomes_read": False,
        "cost_policy_file_sha256": request["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(request["execution_contract"]),
    }


def _initials(request, openings):
    return [
        {
            "account_id": a["account_id"],
            "portfolio_id": a["account_id"],
            "package_id": request["package_id"],
            "manifest_sha256": request["manifest_sha256"],
            "origin_initial_cash_cny": a["origin_initial_cash_cny"],
            "cash_cny": a["cash_cny"],
            "positions": a["positions"],
            "marks_raw_cny": a["opening_marks_raw_cny"],
            **{k: a[k] for k in ("rule", "delay_seconds", "operation")},
        }
        for a in openings
    ]


def _publish_results(root, request_ref, request, p, openings, days_result):
    accounts = [
        {
            "account_id": a["account_id"],
            "opening_account": p["initial_accounts"][index],
            "days": days_result["references"][a["account_id"]],
        }
        for index, a in enumerate(openings)
    ]
    results = {
        "schema_version": RESULT_SCHEMA,
        "producer": "LOCALSIM",
        "request": request_ref.model_dump(),
        "study_identity": request["identity"],
        "release_identity": request["release_identity"],
        "producer_source_commit": request["producer_source_commit"],
        "preflight": request["preflight"],
        "price_templates": request["price_templates"],
        "trade_dates": request["trade_dates"],
        "accounts": accounts,
        "account_count": 13,
        "account_day_count": sum(len(a["days"]) for a in accounts),
        "cost_policy_file_sha256": request["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(request["execution_contract"]),
        "raw_source_inspection": ref(root / "raw_source_inspection.json"),
        "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
        "economic_confirmation": False,
        "production_account_created": False,
        "database_accessed": False,
        "dataset_mutated": False,
        "market_network_accessed": False,
        "service_process_control": False,
    }
    publish(root / "results.json", results)
    facts_accounts = [{"account_id": a["account_id"], "days": a["days"]} for a in accounts]
    source = {
        "schema_version": "localsim_period_final_source_inspection_v2",
        "stage": "COMPLETE_SOURCE_BATCH_INSPECTED",
        "preflight": request["preflight"],
        "price_templates": request["price_templates"],
        "candidate_manifest": request["candidate_manifest"],
        "trade_dates": request["trade_dates"],
        "producer_source_commit": request["producer_source_commit"],
        "account_days_sha256": digest(facts_accounts),
        "results": ref(root / "results.json"),
        "raw_source_inspection": ref(root / "raw_source_inspection.json"),
        "cost_policy_file_sha256": request["cost_policy"]["sha256"],
        "execution_contract_sha256": digest(request["execution_contract"]),
    }
    publish(root / "source_inspection.json", source)
    facts = {
        "schema_version": "position_timing_period_facts_v1",
        "producer": "LOCALSIM",
        "preflight": request["preflight"],
        "price_templates": request["price_templates"],
        "producer_source_commit": request["producer_source_commit"],
        "source_batch_verified": True,
        "source_inspection": ref(root / "source_inspection.json"),
        "accounts": facts_accounts,
        "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
        "nav_observation_scope": "ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS",
    }
    publish(root / "facts.json", facts)
    spec = PeriodAnalysisSpec(
        preflight=FileRef.model_validate(request["preflight"]),
        price_templates=FileRef.model_validate(request["price_templates"]),
        facts=FileRef.model_validate(ref(root / "facts.json")),
    )
    publish(root / "analysis_spec.json", spec.model_dump(mode="json"))
    receipt = {
        "schema_version": "localsim_period_result_receipt_v2",
        "request": request_ref.model_dump(),
        **{
            key: ref(root / file)
            for key, file in (
                ("results", "results.json"),
                ("facts", "facts.json"),
                ("analysis_spec", "analysis_spec.json"),
                ("source_inspection", "source_inspection.json"),
            )
        },
    }
    publish(root / "result_receipt.json", receipt)
    return receipt


def run_period(request_ref: FileRef, *, performance=None, stop_after_account_days=None, economic_retry=False):
    performance = performance or ResearchPerformanceOptions()
    require(isinstance(performance, ResearchPerformanceOptions) and type(economic_retry) is bool, "OPTIONS_INVALID")
    stub = read(request_ref)
    require(
        stub.get("schema_version") == REQUEST_SCHEMA
        and stub.get("identity") == digest({k: v for k, v in stub.items() if k not in {"identity", "release_identity"}})
        and Path(request_ref.path).name == "request.json"
        and Path(request_ref.path).parent.name == stub["identity"],
        "REQUEST_SCOPE_CONFLICT",
    )
    require(
        stop_after_account_days is None or type(stop_after_account_days) is int and stop_after_account_days >= 0,
        "STOP_LIMIT_INVALID",
    )
    root = checked_file(request_ref).parent
    started = perf_counter()
    metrics = {"market_preparation_seconds": 0.0, "commit_readback_seconds": 0.0}
    try:
        with FileLock(root / "study-writer.lock", timeout=0):
            request, p, openings, inspected = _inspect_request(request_ref)
            raw = _raw_source(request_ref, request, inspected)
            publish(root / "raw_source_inspection.json", raw)
            if economic_retry:
                require((root / "result_receipt.json").is_file(), "ECONOMIC_RETRY_REQUIRES_COMPLETE_RESULT")
                if (root / "economic_retry_receipt.json").is_file():
                    prior = read(ref(root / "economic_retry_receipt.json"))
                    require(
                        prior["request"] == request_ref.model_dump()
                        and prior["status"] == "EXACT_RETRY_VERIFIED"
                        and prior["result_receipt"] == ref(root / "result_receipt.json"),
                        "ECONOMIC_RETRY_RECEIPT_CONFLICT",
                    )
                    _inspect_completed(request_ref, request, p, openings, inspected)
                    return {
                        "status": "ALREADY_VERIFIED",
                        "recomputed_account_days": 0,
                        "economic_retry_receipt": ref(root / "economic_retry_receipt.json"),
                    }
            elif (root / "result_receipt.json").is_file():
                verified = _inspect_completed(request_ref, request, p, openings, inspected)
                return {**verified, "status": "ALREADY_MATERIALIZED"}
            package = read(p["inputs"]["package"])["package"]
            require(compute_manifest_json_sha256(package["manifest"]) == request["manifest_sha256"], "PACKAGE_CONFLICT")
            manifest = StrategyPackageManifest.model_validate(package["manifest"])
            cost = CostContract.model_validate(read(request["cost_policy"]))
            with ResearchAccountPool(performance.account_workers) as pool, ExitStack() as resources:
                candidate = resources.enter_context(
                    ResearchMarketWorkspace(
                        CandidateReader(FileRef.model_validate(request["candidate_manifest"])), performance.batch_days
                    )
                )
                days_result = dual.execute_account_days(
                    request=request,
                    inspected=inspected,
                    initials=_initials(request, openings),
                    manifest=manifest,
                    cost=cost,
                    candidate=candidate,
                    root=root,
                    pool=pool,
                    metrics=metrics,
                    performance=performance,
                    stop_after_account_days=stop_after_account_days,
                    verify_recompute=economic_retry,
                    day_schema=DAY_SCHEMA,
                )
                if "status" in days_result:
                    return days_result
                candidate.verify_pins()
            for source in inspected["source_refs"]:
                checked_file(source)
            require(request["source_identity"] == implementation_identity(), "SOURCE_CHANGED_DURING_RUN")
            receipt = _publish_results(root, request_ref, request, p, openings, days_result)
            if economic_retry:
                expected = len(openings) * len(request["trade_dates"])
                require(
                    days_result["recomputed"] == expected and days_result["computed"] == 0, "ECONOMIC_RETRY_POPULATION"
                )
                retry = {
                    "schema_version": "localsim_period_economic_retry_receipt_v2",
                    "status": "EXACT_RETRY_VERIFIED",
                    "request": request_ref.model_dump(),
                    "result_receipt": ref(root / "result_receipt.json"),
                    "results": receipt["results"],
                    "recomputed_account_days": expected,
                    "new_committed_account_days": 0,
                    "comparison": "EXACT_NATIVE_ACCOUNT_AND_CANONICAL_ORDERS_FILLS_EVENTS_CASH_ENTRIES",
                    "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION",
                }
                publish(root / "economic_retry_receipt.json", retry)
            return {
                "status": "EXACT_RETRY_VERIFIED" if economic_retry else "MATERIALIZED",
                **receipt,
                "computed_account_days": days_result["computed"],
                "recomputed_account_days": days_result["recomputed"],
                "economic_retry_receipt": ref(root / "economic_retry_receipt.json") if economic_retry else None,
                "wall_seconds": perf_counter() - started,
                "metrics": metrics,
            }
    except BaseException as exc:
        error = {
            "schema_version": "localsim_period_failure_receipt_v2",
            "request": request_ref.model_dump(),
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "economic_retry": economic_retry,
        }
        publish(root / "failures" / (digest(error) + ".json"), error)
        raise


def inspect_period(request_ref: FileRef):
    request, p, openings, inspected = _inspect_request(request_ref)
    return _inspect_completed(request_ref, request, p, openings, inspected)


def _inspect_completed(request_ref, request, p, openings, inspected):
    root = Path(request_ref.path).parent
    require(
        read(ref(root / "raw_source_inspection.json")) == _raw_source(request_ref, request, inspected),
        "RAW_SOURCE_RECEIPT_CONFLICT",
    )
    receipt = read(ref(root / "result_receipt.json"))
    require(
        receipt
        == {
            "schema_version": "localsim_period_result_receipt_v2",
            "request": request_ref.model_dump(),
            **{
                key: ref(root / name)
                for key, name in (
                    ("results", "results.json"),
                    ("facts", "facts.json"),
                    ("analysis_spec", "analysis_spec.json"),
                    ("source_inspection", "source_inspection.json"),
                )
            },
        },
        "RESULT_RECEIPT_CONFLICT",
    )
    results = read(receipt["results"])
    require(
        results["schema_version"] == RESULT_SCHEMA
        and results["study_identity"] == request["identity"]
        and results["request"] == request_ref.model_dump()
        and results["account_count"] == len(openings)
        and results["account_day_count"] == len(openings) * len(request["trade_dates"])
        and results["raw_source_inspection"] == ref(root / "raw_source_inspection.json")
        and all(
            results[key] == request[key]
            for key in (
                "producer",
                "producer_source_commit",
                "release_identity",
                "preflight",
                "price_templates",
                "trade_dates",
            )
        )
        and results["cost_policy_file_sha256"] == request["cost_policy"]["sha256"]
        and results["execution_contract_sha256"] == digest(request["execution_contract"]),
        "RESULT_IDENTITY_CONFLICT",
    )
    by_id = {a["account_id"]: a for a in results["accounts"]}
    require(
        len(results["accounts"]) == len(by_id) == len(openings) and set(by_id) == {a["account_id"] for a in openings},
        "ACCOUNT_POPULATION_CONFLICT",
    )
    cost = CostContract.model_validate(read(request["cost_policy"]))
    initials = _initials(request, openings)
    count = 0
    for initial, opening_ref in zip(initials, request["initial_accounts"], strict=True):
        aid, prior, previous = initial["account_id"], initial, None
        account = by_id[aid]
        require(account["opening_account"] == opening_ref, "RESULT_OPENING_CONFLICT")
        require(len(account["days"]) == len(request["trade_dates"]), "DAY_POPULATION_CONFLICT")
        for index, (stamp, day_ref) in enumerate(zip(request["trade_dates"], account["days"], strict=True)):
            require(Path(day_ref["path"]).resolve() == root / "days" / aid / (stamp + ".json"), "DAY_SCOPE_CONFLICT")
            row = read(day_ref)
            identity = dual._account_day_identity(
                request_identity=request["identity"], account_id=aid, trade_date=stamp, prior=prior
            )
            store = TimingFileRepository(root / "journals", "lsdm_" + identity[:48])
            require(
                row["schema_version"] == DAY_SCHEMA
                and row["study_identity"] == request["identity"]
                and row["day_index"] == index
                and row["previous_day"] == previous
                and row["account_id"] == aid
                and row["trade_date"] == stamp
                and all(row[key] == initial[key] for key in ("rule", "delay_seconds", "operation"))
                and row["account_receipt"] == ref(store.root / "generations/00000001.json"),
                "DAY_LINEAGE_CONFLICT",
            )
            record = store.latest()
            require(
                record is not None
                and record["checkpoint"]["extra"] == {"input_identity": identity, "day_index": 0}
                # The day envelope is v2; its native economic account stays v1.
                and all(row.get(k) == v for k, v in record["checkpoint"]["account"].items() if k != "schema_version")
                and all(LocalSimExecutionStateV1.model_validate(s).is_terminal for s in record["states"].values()),
                "DAY_JOURNAL_CONFLICT",
            )
            ledger = InMemoryLedger(
                portfolio_id=aid, initial_cash=Decimal(initial["origin_initial_cash_cny"]), fee_model=fee_model(cost)
            )
            ledger.cash = Decimal(prior["cash_cny"])
            ledger.positions = {s: PositionLot.model_validate(v) for s, v in prior["positions"].items()}
            ledger.settle_trade_date(date.fromisoformat(stamp))
            require(
                Decimal(row["initial_account"]["cash_cny"]) == ledger.cash_decimal
                and row["initial_account"]["positions"]
                == {s: v.model_dump(mode="json") for s, v in ledger.positions.items()},
                "OPENING_ACCOUNT_CONFLICT",
            )
            validate_day_ledger(record, initial=initial, cost=cost)
            terminal = datetime.combine(date.fromisoformat(stamp), time(14, 57), CHINA)
            last = None
            for observation in row["nav_observations"]:
                clock = datetime.fromisoformat(observation["as_of"])
                nav = Decimal(observation["nav_cny"])
                require(
                    clock.tzinfo is not None
                    and clock.astimezone(CHINA).date().isoformat() == stamp
                    and datetime.combine(date.fromisoformat(stamp), time(9, 30), CHINA) <= clock <= terminal
                    and (last is None or clock > last)
                    and nav.is_finite()
                    and nav > 0,
                    "NAV_OBSERVATION_INVALID",
                )
                last = clock
            require(
                last == terminal == datetime.fromisoformat(row["terminal_as_of"])
                and Decimal(row["nav_observations"][-1]["nav_cny"]) == Decimal(row["terminal_nav_cny"]),
                "COMMON_TERMINAL_CONFLICT",
            )
            ledger.cash = Decimal(row["cash_cny"])
            ledger.positions = {s: PositionLot.model_validate(v) for s, v in row["positions"].items()}
            nav = ledger.account_snapshot(
                prices={s: float(v) for s, v in row["marks_raw_cny"].items()}, snapshot_time=terminal
            ).nav
            require(Decimal(str(nav)) == Decimal(row["terminal_nav_cny"]), "TERMINAL_VALUATION_CONFLICT")
            prior, previous, count = row, day_ref, count + 1
    spec = PeriodAnalysisSpec.model_validate(read(receipt["analysis_spec"]))
    require(
        spec.model_dump(mode="json")
        == PeriodAnalysisSpec(
            preflight=FileRef.model_validate(request["preflight"]),
            price_templates=FileRef.model_validate(request["price_templates"]),
            facts=FileRef.model_validate(receipt["facts"]),
        ).model_dump(mode="json"),
        "ANALYSIS_SPEC_CONFLICT",
    )
    facts = read(receipt["facts"])
    require(
        facts["source_inspection"] == receipt["source_inspection"]
        and facts["accounts"] == [{"account_id": a["account_id"], "days": a["days"]} for a in results["accounts"]],
        "FACTS_RESULT_CONFLICT",
    )
    period_source_preflight(spec)
    return {
        "status": "VERIFIED",
        "account_count": len(openings),
        "verified_account_days": count,
        "computed_account_days": 0,
        "request": request_ref.model_dump(),
        **receipt,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    for name in ("preflight", "templates", "manifest"):
        freeze.add_argument("--" + name, required=True)
        freeze.add_argument("--" + name + "-sha256", required=True)
    freeze.add_argument("--output-root", type=Path, required=True)
    freeze.add_argument("--producer-commit", required=True)
    for name in ("run", "inspect", "economic-retry"):
        command = commands.add_parser(name)
        command.add_argument("--request", required=True)
        command.add_argument("--request-sha256", required=True)
        if name != "inspect":
            command.add_argument("--batch-days", type=int, default=3)
            command.add_argument("--account-workers", type=int, default=2)
            command.add_argument("--optimized", action="store_true")
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze_period(
            preflight_ref=FileRef(path=args.preflight, sha256=args.preflight_sha256),
            templates_ref=FileRef(path=args.templates, sha256=args.templates_sha256),
            manifest_ref=FileRef(path=args.manifest, sha256=args.manifest_sha256),
            output_root=args.output_root,
            producer_commit=args.producer_commit,
        )
    else:
        request_ref = FileRef(path=args.request, sha256=args.request_sha256)
        result = (
            inspect_period(request_ref)
            if args.command == "inspect"
            else run_period(
                request_ref,
                performance=ResearchPerformanceOptions(
                    batch_days=args.batch_days, account_workers=args.account_workers, optimized=args.optimized
                ),
                economic_retry=args.command == "economic-retry",
            )
        )
    print(json.dumps(payload(result), ensure_ascii=False))


if __name__ == "__main__":
    main()
