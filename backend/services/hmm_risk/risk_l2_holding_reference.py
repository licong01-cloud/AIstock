"""Approved retrospective holding reference; sealed signals, no new inference."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, timedelta
import math
from pathlib import Path

import numpy as np

from backend.services.dataset_release.shared_sector_context import (
    load_release_sw_l2_code_map,
    load_sector_quote_availability,
)
from backend.services.hmm_risk import risk_l2_value_continuous as continuous
from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.stock_fact_repository import _build_circ_mv_evidence

VERSION = "hmm_risk_l2_holding_reference_v1"
PINS = {
    "original_request": "edab09e7cb00559a72192ccb12225e773c8571b31c0e4170bb5a0786a05ab2ea",
    "features": "09e8b296000c1e0d8dac540deed23f49e44283b42eb2ff28622444f0c1b88356",
    "sealed_1": "a7552ea9215d62d7a21252c8c56613797ab00aef092b0d3a12a88f74dd9b4236",
    "sealed_2": "a7552ea9215d62d7a21252c8c56613797ab00aef092b0d3a12a88f74dd9b4236",
    "acceptance": "d8f095f4070a0895a59ebb275a89b93ce94c1153fb87a5bd7c7411af904947e5",
}
CONTRACT = {
    "version": VERSION,
    "release_binding": continuous.BINDING,
    "original_pins": PINS,
    "original_contract_sha256": canonical_sha256(continuous.CONTRACT),
    "reference_basis": "GROSS_SYNTHETIC_L2_HOLDING_REFERENCE",
    "cohort_as_of": "q=prev_open(p); p=prev_open(u)",
    "return_formula": "close_yuan(u)/official_prev_close_yuan(u)-1",
    "return_dates": ["2026-04-02", "2026-08-31"],
    "planned_return_dates": 103,
    "budget_denominator": 131,
    "warning_threshold": 0.20,
    "arms": list(continuous.ARMS),
    "cost_bps": list(continuous.value.COST_BPS),
    "cost_scope": "INDUSTRY_OVERLAY_ONLY_NOT_INTERNAL_BASKET_OR_EXECUTION_COST",
    "independence_status": "ALREADY_CONSUMED_HISTORY_DEVELOPMENT_ONLY",
    "historical_as_known_verified": False,
    "fits": 0,
    "new_inference_calls": 0,
    "new_tail_accessed": False,
    "forward_confirmed": False,
    "net_value_status": "UNASSESSED",
    "product_activation": False,
}
require = continuous.require
verify = continuous.verify_receipt
finite = continuous.value.finite


@contextmanager
def zero_compute():
    """Use the existing fit/filter/predict/DB/active-profile/network poisons."""
    with continuous.history.no_training_or_external_actions(), continuous.value.zero_compute():
        yield


def ordinary(path):
    path = Path(path)
    require(path.is_absolute() and path.is_file(), "explicit absolute regular input required")
    require(not any(continuous.inputs.reader._is_link(p) for p in (path, *path.parents)), "indirect input")
    return path


def load_inputs(request):
    verify(request)
    require(
        set(request) == {"schema_version", "contract", "assets", "asset_locator_rebind", "receipt_sha256"}
        and request["schema_version"] == VERSION + "_request"
        and request["contract"] == CONTRACT
        and set(request["assets"]) == set(PINS),
        "holding-reference request differs from approved authority",
        "identity_mismatch",
    )
    original = {k: continuous.inputs._asset(ordinary(request["assets"][k]), pin) for k, pin in PINS.items()}
    old, bundle, sealed, accepted = (
        original["original_request"],
        original["features"],
        original["sealed_1"],
        original["acceptance"],
    )
    continuous.validate_bundle(old, bundle)
    signals = continuous.validate_sealed(bundle, sealed)
    continuous.validate_sealed(bundle, original["sealed_2"])
    require(
        Path(request["assets"]["sealed_1"]).read_bytes() == Path(request["assets"]["sealed_2"]).read_bytes()
        and accepted["schema_version"] == continuous.VERSION + "_acceptance"
        and accepted["contract"] == continuous.CONTRACT
        and accepted["execution_status"] == "COMPLETED"
        and accepted["two_process_bitwise_equal"] is True
        and accepted["request_sha256"] == old["receipt_sha256"]
        and accepted["feature_sha256"] == bundle["receipt_sha256"]
        and accepted["source_identity"] == bundle["source_identity"]
        and accepted["model_sha256"] == continuous.CONTRACT["model_sha256"]
        and accepted["result"]["prediction_sha256"] == sealed["receipt_sha256"]
        and accepted["result"]["status"] == "INSUFFICIENT_REFERENCE_PATH"
        and type(accepted["fits"]) is int
        and accepted["fits"] == 0,
        "original acceptance/sealed linkage differs",
        "identity_mismatch",
    )
    source, _, _ = continuous._source(old)
    rebind = request["asset_locator_rebind"]
    require(
        isinstance(rebind, dict) and set(rebind) <= {"provider_absence_manifest", "security_identity_manifest"},
        "unapproved locator rebind",
    )
    for key in ("provider_absence_manifest", "security_identity_manifest"):
        label = "provider_absence" if key.startswith("provider") else "security_identity"
        expected_bytes = bundle["source_identity"]["release_identity"]["metadata_sha256"][label]
        expected_canonical = source["frozen"][label + "_sha256"]
        path = old[key]
        if key in rebind:
            row = rebind[key]
            require(
                set(row) == {"old_locator", "new_locator", "file_sha256", "canonical_sha256"}
                and row["old_locator"] == path
                and row["file_sha256"] == expected_bytes
                and row["canonical_sha256"] == expected_canonical,
                "locator rebind changed authority",
                "identity_mismatch",
            )
            path = row["new_locator"]
        path = ordinary(path)
        stamp = continuous.inputs._stamp(path)
        require(reader._sha256_file(path) == expected_bytes, "locator file identity differs", "identity_mismatch")
        loader = (
            load_provider_absence_manifest if label == "provider_absence" else load_security_source_identity_manifest
        )
        loader(path, expected_sha256=expected_canonical)
        require(continuous.inputs._stamp(path) == stamp, "locator changed during read", "identity_mismatch")
        source["source"][key] = str(path)
    return original, source, signals


def schedule(bundle):
    plan = continuous.history.schedule(bundle["calendar"])
    calendar = bundle["calendar"]
    return [
        {"q": calendar[plan["positions"][p] - 1], "p": p, "u": u}
        for p, u in zip(plan["days"][:-1], plan["days"][1:], strict=True)
    ]


def _months(start, end):
    while start <= end:
        following = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        stop = min(end, following - timedelta(days=1))
        yield start, stop
        start = following


def _basic_history(assets, needed, first, last):
    """Read only required dates; seek true older context only for unresolved keys."""
    history = defaultdict(list)

    def load(start, end, wanted):
        frame = reader._load_fixed_h5_window(
            assets["files"]["daily_basic"],
            expected_columns=reader._DAILY_BASIC_COLUMNS,
            expected_dtype="<f4",
            start=start,
            end=end,
            labels_prevalidated=True,
        )
        frame = frame[frame.index.get_level_values(1).isin(wanted)]
        _, updates = reader._daily_basic_lookup(frame)
        for day, rows in updates.items():
            for symbol, value, status, reason in rows:
                history[symbol].append((day, value, status, reason))

    for start, end in _months(first, last):
        load(start, end, needed)
    unresolved = {
        symbol for symbol, earliest in needed.items() if not any(row[0] <= earliest for row in history[symbol])
    }
    prior_end = first - timedelta(days=1)
    while unresolved and prior_end >= reader.SOURCE_START:
        prior_start = max(reader.SOURCE_START, prior_end.replace(day=1))
        load(prior_start, prior_end, unresolved)
        # Latest *fact*, including an invalid fact, wins; never backtrack to a
        # convenient older finite value. No synthetic row or forward-fill.
        unresolved = {symbol for symbol in unresolved if not any(row[0] < first for row in history[symbol])}
        prior_end = prior_start - timedelta(days=1)
    return {s: sorted(rows) for s, rows in history.items()}


def freeze_cohorts(ledger, catalog, spans, projection, security, history, calendar):
    """Freeze all q-members and weights BEFORE any p/u quote can be read."""
    positions = {day: i for i, day in enumerate(calendar)}
    indexed = {s: ([r[0] for r in rows], rows) for s, rows in history.items()}
    rows = []
    for times in ledger:
        q, p = date.fromisoformat(times["q"]), date.fromisoformat(times["p"])
        members = defaultdict(list)
        for symbol, intervals in sorted(spans.items()):
            active = [span for span in intervals if span[0] <= q <= span[1]]
            if not active:
                continue
            require(len(active) == 1, "overlapping PIT member intervals", "identity_mismatch")
            mapped = projection.resolve(symbol, q)
            require(
                mapped.status == "resolved" and mapped.l2_code in catalog,
                f"PIT industry unresolved: {symbol}/{q}",
                "identity_mismatch",
            )
            resolution = security.resolve(symbol, q, "market.daily_basic")
            dates, values = indexed.get(resolution.source_ts_code, ([], []))
            index = bisect_right(dates, q) - 1
            fact = values[index] if index >= 0 else None
            evidence = _build_circ_mv_evidence(
                raw_value=None if fact is None else fact[1],
                source_date=None if fact is None else fact[0],
                staleness_trading_days=None if fact is None else positions[p] - positions[fact[0]],
                trade_date=p,
                pit_eligible_start=active[0][0],
                history_start=reader.SOURCE_START,
            )
            value = evidence.accepted_value
            members[mapped.l2_code].append(
                {
                    **times,
                    "sector_code": mapped.l2_code,
                    "symbol": symbol,
                    "circ_mv": None if value is None else float(value),
                    "circ_source_date": None if evidence.source_date is None else evidence.source_date.isoformat(),
                    "circ_reason": evidence.reason_code,
                    "pit_start": active[0][0].isoformat(),
                    "pit_end": active[0][1].isoformat(),
                    "security_row_sha256": resolution.row_hash,
                    "projection_sha256": canonical_sha256(reader._industry_projection_identity(mapped)),
                }
            )
        for code in catalog:
            group = members[code]
            complete = bool(group) and all(finite(r["circ_mv"]) and r["circ_mv"] > 0 for r in group)
            denominator = math.fsum(r["circ_mv"] for r in group) if complete else None
            require(
                denominator is None or math.isfinite(denominator) and denominator > 0,
                "invalid derived weight denominator",
                "formula_invalid",
            )
            rows.extend({**r, "weight": r["circ_mv"] / denominator if complete else None} for r in group)
    return sorted(rows, key=lambda r: (r["u"], r["sector_code"], r["symbol"]))


def _quotes(assets, cohorts, calendar):
    needed = defaultdict(set)
    for row in cohorts:
        needed[row["symbol"]].update((row["p"], row["u"]))
    result, inventory = {}, []
    for symbol, labels in sorted(needed.items()):
        root = assets["qlib_root"] / "features" / reader._qlib_code_directory(symbol)
        paths = {field: ordinary(root / f"{field}.day.bin") for field in reader.QLIB_STOCK_FIELDS}
        stamps = {field: continuous.inputs._stamp(path) for field, path in paths.items()}
        with paths["close"].open("rb") as handle:
            first = float(np.frombuffer(handle.read(4), dtype="<f4")[0])
        count = stamps["close"][0] // 4 - 1
        require(
            math.isfinite(first)
            and first.is_integer()
            and 0 <= first < len(calendar)
            and int(first) + count <= len(calendar),
            "quote range escapes calendar",
        )
        available = set(calendar[int(first) : int(first) + count])
        dates = sorted(date.fromisoformat(d) for d in labels if date.fromisoformat(d) in available)
        if dates:
            # These are valuation dates of ALREADY held stocks, intentionally
            # not today's selection spans. The release/PIT universe is unchanged.
            raw = reader._read_qlib_stock_rows(
                assets["qlib_root"],
                symbol=symbol,
                calendar=calendar,
                active_spans=[(d, d) for d in dates],
                window_start=dates[0],
                window_end=dates[-1],
            )
            for item in raw:
                day = reader._date_from_yyyymmdd(item["trade_date"], "reference quote date").isoformat()
                factor, close, previous = (float(item[k]) for k in ("factor", "close", "prev_close"))
                # Reference valuation needs these prices, not unrelated volume,
                # limits or moneyflow. NaN source facts remain explicit NA.
                close = close / factor if finite(factor) and factor > 0 and finite(close) and close > 0 else None
                previous = previous if finite(previous) and previous > 0 else None
                require(close is None or finite(close) and close > 0, "non-finite derived raw price", "formula_invalid")
                result[(symbol, day)] = {"close": close, "prev_close": previous}
        continuous.inputs._stable_files(paths, stamps)
        inventory.append([symbol, {k: list(v) for k, v in sorted(stamps.items())}])
    return result, canonical_sha256(inventory)


def reference_returns(ledger, catalog, cohorts, quotes, suspension, spans):
    """Pure arithmetic/source-status readback, with no outcome-based selection."""
    grouped = defaultdict(list)
    require(
        cohorts == sorted(cohorts, key=lambda r: (r["u"], r["sector_code"], r["symbol"])), "cohort rows not canonical"
    )
    keys = set()
    by_u = {r["u"]: r for r in ledger}
    for row in cohorts:
        times = by_u.get(row["u"])
        key = (row["u"], row["sector_code"], row["symbol"])
        require(
            key not in keys
            and times is not None
            and row["sector_code"] in catalog
            and all(row[k] == times[k] for k in ("q", "p", "u"))
            and row["pit_start"] <= row["q"] <= row["pit_end"],
            "invalid/duplicate cohort key",
        )
        keys.add(key)
        require(
            row["circ_source_date"] is None or reader.SOURCE_START.isoformat() <= row["circ_source_date"] <= row["q"],
            "future/out-of-window market cap fact",
            "identity_mismatch",
        )
        require(row["circ_mv"] is None or finite(row["circ_mv"]) and row["circ_mv"] > 0, "invalid market cap structure")
        grouped[(row["u"], row["sector_code"])].append(row)
    returns, statuses, member_values = {}, [], []
    for times in ledger:
        u, p = times["u"], times["p"]
        returns[u] = {}
        for code in catalog:
            group, reasons, values = grouped[(u, code)], [], []
            if not group:
                reasons.append("REFERENCE_COHORT_EMPTY")
            weights_valid = bool(group) and all(row["circ_mv"] is not None for row in group)
            denominator = math.fsum(row["circ_mv"] for row in group) if weights_valid else None
            for row in group:
                weight = row["weight"]
                if not weights_valid:
                    require(weight is None, "partial cohort was silently renormalized")
                    reason = "REFERENCE_WEIGHT_UNAVAILABLE"
                else:
                    require(
                        finite(weight) and weight > 0 and weight == row["circ_mv"] / denominator,
                        "cohort weight differs",
                        "formula_invalid",
                    )
                    reason = None
                start = quotes.get((row["symbol"], p), {})
                end = quotes.get((row["symbol"], u), {})
                r = None
                if reason is None:
                    if start.get("close") is None:
                        reason = "REFERENCE_START_VALUATION_UNAVAILABLE"
                    elif end.get("close") is None or end.get("prev_close") is None:
                        day = date.fromisoformat(u)
                        if (day, row["symbol"]) in suspension:
                            reason = "REFERENCE_SUSPENSION_VALUATION_UNAVAILABLE"
                        elif not any(a <= day <= b for a, b in spans[row["symbol"]]):
                            reason = "REFERENCE_EXIT_VALUATION_UNAVAILABLE"
                        else:
                            reason = "REFERENCE_QUOTE_UNAVAILABLE"
                    else:
                        require(
                            all(finite(v) and v > 0 for v in (start["close"], end["close"], end["prev_close"])),
                            "invalid valuation structure",
                        )
                        r = end["close"] / end["prev_close"] - 1
                        require(finite(r), "non-finite derived return", "formula_invalid")
                if reason:
                    reasons.append(reason)
                else:
                    values.append(weight * r)
                member_values.append(
                    {
                        "u": u,
                        "sector_code": code,
                        "symbol": row["symbol"],
                        "return": r,
                        "reason_code": reason,
                        "suspended": (date.fromisoformat(u), row["symbol"]) in suspension,
                        "in_pit_at_return": any(a <= date.fromisoformat(u) <= b for a, b in spans[row["symbol"]]),
                    }
                )
            returns[u][code] = None if reasons else math.fsum(values)
            require(
                returns[u][code] is None or finite(returns[u][code]), "invalid derived sector return", "formula_invalid"
            )
            statuses.append({**times, "sector_code": code, "members": len(group), "reason_codes": sorted(set(reasons))})
    return returns, statuses, member_values


def boundary_audit(ledger, spans):
    """Explain natural entry boundaries without using them to choose holdings."""
    rows = []
    for times in ledger:
        members = {
            k: {
                symbol
                for symbol, intervals in spans.items()
                if any(a <= date.fromisoformat(times[k]) <= b for a, b in intervals)
            }
            for k in ("q", "p", "u")
        }
        rows.append(
            {
                **times,
                "q_member_count": len(members["q"]),
                "later_entrants_not_held": sorted((members["p"] | members["u"]) - members["q"]),
                "q_members_outside_u_pit": sorted(members["q"] - members["u"]),
            }
        )
    return rows


def build_facts(request, original, source):
    """One bounded parent read. Children consume its pinned immutable facts only."""
    bundle = original["features"]
    assets = reader.load_rotation_l1_direct_v2_source_assets(
        Path(source["source"]["candidate_root"]),
        security_identity_manifest=Path(source["source"]["security_identity_manifest"]),
        provider_absence_manifest=Path(source["source"]["provider_absence_manifest"]),
        data_window_end=continuous.history.END,
        frozen_release_binding=continuous.BINDING,
    )
    require(
        assets["release_identity"] == bundle["source_identity"]["release_identity"],
        "reference release differs from frozen predictions",
        "identity_mismatch",
    )
    source_paths = {
        **assets["files"],
        "calendar": assets["qlib_root"] / "calendars/day.txt",
        "pit_universe": assets["instrument_universe_path"],
        "dataset_manifest": assets["release_root"] / "qe_dataset_manifest.json",
    }
    stamps = {k: continuous.inputs._stamp(p) for k, p in source_paths.items()}
    calendar = list(reader._load_qlib_calendar(assets["qlib_root"] / "calendars/day.txt"))
    require([d.isoformat() for d in calendar] == bundle["calendar"], "reference calendar differs", "identity_mismatch")
    ledger, catalog = schedule(bundle), bundle["catalog"]
    adapter = reader._industry_adapter(
        source["frozen"]["industry_authority"], forbidden_roots=(Path(__file__).resolve().parents[3],)
    )
    require(
        list(reader._canonical_sector_codes(adapter)[1]) == catalog, "official L2 catalog differs", "identity_mismatch"
    )
    projection = reader._IndustryProjectionIndex(adapter, calendar=calendar)
    security = reader._SecurityResolutionIndex(
        load_security_source_identity_manifest(
            assets["files"]["security_identity"], expected_sha256=source["frozen"]["security_identity_sha256"]
        )
    )
    manifest = reader._read_json_object(assets["release_root"] / "qe_dataset_manifest.json")
    quote_files = {}
    for name in ("sector_code_map", "sector_quote_availability"):
        pin = manifest["components"][name]
        quote_files[name] = continuous.inputs.reader._require_file(assets["release_root"], pin["path"], pin["sha256"])
        source_paths[name] = quote_files[name]
        stamps[name] = continuous.inputs._stamp(quote_files[name])
    mapping = load_release_sw_l2_code_map(quote_files["sector_code_map"])
    quote_authority = load_sector_quote_availability(
        quote_files["sector_quote_availability"], code_map=mapping, required_end=continuous.history.END
    )
    require(list(mapping.member_backed_codes) == catalog, "shared code map/C013 differ", "identity_mismatch")
    spans = reader._parse_instrument_spans(assets["instrument_universe_path"])
    needed = {}
    for times in ledger:
        q = date.fromisoformat(times["q"])
        for symbol, intervals in spans.items():
            if any(a <= q <= b for a, b in intervals):
                key = security.resolve(symbol, q, "market.daily_basic").source_ts_code
                needed[key] = min(q, needed.get(key, q))
    basic = _basic_history(assets, needed, date.fromisoformat(ledger[0]["q"]), date.fromisoformat(ledger[-1]["q"]))
    cohorts = freeze_cohorts(ledger, catalog, spans, projection, security, basic, calendar)
    cohort_sha = canonical_sha256(cohorts)
    quotes, quote_inventory_sha = _quotes(assets, cohorts, calendar)
    suspension = reader._load_suspend_keys(
        assets["files"]["suspend_data"],
        assets["files"]["suspend_manifest"],
        calendar=calendar,
        expected_release_cutoff=assets["release_cutoff"],
        expected_universe_key=assets["universe_key"],
        bounded=True,
    )
    returns, statuses, member_values = reference_returns(ledger, catalog, cohorts, quotes, suspension, spans)
    continuous.inputs._stable_files(source_paths, stamps)
    require(canonical_sha256(cohorts) == cohort_sha, "cohort changed after quote access", "identity_mismatch")
    return receipt(
        {
            "schema_version": VERSION + "_facts",
            "contract": CONTRACT,
            "request_sha256": request["receipt_sha256"],
            "catalog": catalog,
            "ledger": ledger,
            "original_pins": PINS,
            "source_identity": {
                "release_identity": assets["release_identity"],
                "source_inventory_sha256": assets["inventory"]["inventory_sha256"],
                "original_inventory_sha256": bundle["source_identity"]["source_inventory_sha256"],
                "quote_availability_sha256": manifest["components"]["sector_quote_availability"]["sha256"],
                "quote_availability_digest": quote_authority.quote_availability_digest,
                "industry_identity": source["frozen"]["industry_authority"]["identity"],
                "selected_quote_inventory_sha256": quote_inventory_sha,
            },
            "cohort_sha256": cohort_sha,
            "cohorts": cohorts,
            "quotes": [{"symbol": s, "day": d, **v} for (s, d), v in sorted(quotes.items())],
            "suspension_keys": [
                [d.isoformat(), s] for d, s in sorted(suspension) if ledger[0]["p"] <= d.isoformat() <= ledger[-1]["u"]
            ],
            "valuation_spans": {
                s: [[a.isoformat(), b.isoformat()] for a, b in values] for s, values in sorted(spans.items())
            },
            "natural_boundary_audit": boundary_audit(ledger, spans),
            "returns": returns,
            "industry_status": statuses,
            "member_values": member_values,
        }
    )


def evaluate(request, original, signals, facts, expected_facts_sha256):
    verify(facts, expected_facts_sha256)
    bundle = original["features"]
    require(
        facts["schema_version"] == VERSION + "_facts"
        and facts["contract"] == CONTRACT
        and facts["request_sha256"] == request["receipt_sha256"]
        and facts["original_pins"] == PINS
        and facts["catalog"] == bundle["catalog"]
        and facts["ledger"] == schedule(bundle)
        and facts["source_identity"]["release_identity"] == bundle["source_identity"]["release_identity"]
        and canonical_sha256(facts["cohorts"]) == facts["cohort_sha256"],
        "facts differ from parent/original authority",
        "identity_mismatch",
    )
    quotes = {(r["symbol"], r["day"]): {k: r[k] for k in ("close", "prev_close")} for r in facts["quotes"]}
    require(len(quotes) == len(facts["quotes"]), "duplicate quote key")
    spans = {
        s: [(date.fromisoformat(a), date.fromisoformat(b)) for a, b in values]
        for s, values in facts["valuation_spans"].items()
    }
    suspension = {(date.fromisoformat(d), s) for d, s in facts["suspension_keys"]}
    require(
        facts["natural_boundary_audit"] == boundary_audit(facts["ledger"], spans),
        "natural boundary readback differs",
        "identity_mismatch",
    )
    rebuilt = reference_returns(facts["ledger"], bundle["catalog"], facts["cohorts"], quotes, suspension, spans)
    require(
        rebuilt == (facts["returns"], facts["industry_status"], facts["member_values"]),
        "reference business readback differs",
        "formula_invalid",
    )
    compared = continuous.compare(
        bundle["catalog"], continuous.history.schedule(bundle["calendar"])["days"], signals, facts["returns"]
    )
    complete = compared["complete_reference_path"]
    costs = compared["cost_sensitivity"]
    return receipt(
        {
            **{k: v for k, v in compared.items() if k != "receipt_sha256"},
            "schema_version": VERSION + "_result",
            "contract": CONTRACT,
            "status": "REFERENCE_PATH_COMPLETE" if complete else "INSUFFICIENT_REFERENCE_PATH",
            "risk_observation_status": compared["status"],
            "reference_basis": CONTRACT["reference_basis"],
            "prediction_sha256": PINS["sealed_1"],
            "outcome_sha256": facts["receipt_sha256"],
            "new_inference_calls": 0,
            "product_activation": False,
            "cost_comparisons": {
                a + "_minus_" + b: {
                    str(bp): {
                        m: costs[a][str(bp)][0][m] - costs[b][str(bp)][0][m] if complete else None
                        for m in continuous.METRICS
                    }
                    for bp in continuous.value.COST_BPS
                }
                for a, b in (("C", "B"), ("C", "R"), ("C", "X_C"), ("R", "X_R"))
            },
        }
    )
