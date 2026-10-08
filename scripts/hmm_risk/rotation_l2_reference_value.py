"""Approved four-arm, self-financing L2 index reference replay (never a fit)."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import hashlib
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_price_return_supervised as return_model
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as rank_model
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json

VERSION = "hmm_risk_l2_rotation_reference_value_v1"
ARMS = ("rank", "return", "delta", "no_order")
COSTS = (0, 5, 10, 20)
HOLD_DAYS = 10
RANK_PINS = return_model.REFERENCE_PINS
RETURN_PINS = {
    "acceptance_sha256": "767f81ea422fcb8da5021d2f45d12060498804fd2f6579c282295b4a7fdef86c",
    "model_hash": "858f41a9d2e4e22e8c88268602cd6c01cfb995ef46e47aa3c26d4176446a02b8",
    "model_parameter_sha256": "c2952173c9415f0cb177bb22a4fc1a256dd13fc9f4c75aef8c205d565611d7ab",
    "prediction_sha256": "b349f5c66312ac5d3eaf6fdf3ce777ec8a4afcbeef2d8e4d92427ed665a7f176",
    "outcome_sha256": "3aed11d1fd9c99c2c0ffe170bcd8283966dc0c5b40c46c0022a140e2e95ce7c5",
    "input_hash": "73c64bf199e3d2d1df73df5f9c6cc818ecff129e4a28ccb74344eb4515da8427",
}
BYTE_PINS = {
    "rank": "c2654ecdb1356848484c7f51d0cf6599f0013bf2d8b5377fdfbe8a03f81294dc",
    "return": "bbcdf30d7174b6c3759b49fa6aa1bfea81e48ef855886e69c54b774e29ef9a9d",
}
CONTRACT = {
    "version": VERSION,
    "population": "ex_ante_common_available_native_trending_not_ui_subset",
    "entry": "decision_close_reference_not_executable_price",
    "mature_decisions": 222,
    "decision_start": "2025-04-16",
    "decision_end": "2026-03-17",
    "valuation_end": "2026-03-31",
    "cohorts": 10,
    "hold_open_returns": 10,
    "cohort_rotation": "decision_sequence_mod_10",
    "weights": "equal_entry_notional_fixed_shares_no_cross_cohort_netting",
    "cost_bps_one_way": list(COSTS),
    "buy": "cash/(1+c)",
    "sell": "notional*(1-c)",
    "cash_interest": 0,
    "paired_hac_lag": 9,
    "selection_basis": "RETROSPECTIVE_DEVELOPMENT_SELECTED",
}


def require(condition: bool, message: str, suffix: str = "identity_invalid") -> None:
    if not condition:
        raise baseline.RotationL2Error("hmm_risk_rotation_l2_value_" + suffix, message)


def _number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _sealed_file(path: Path, expected_byte: str | None = None) -> dict[str, Any]:
    return_model._ordinary(path)
    if expected_byte is not None:
        require(hashlib.sha256(path.read_bytes()).hexdigest() == expected_byte, "sealed file byte SHA differs")
    return read_json(path)


def load_frozen(
    input_path: Path, rank_path: Path, return_path: Path
) -> tuple[dict[str, Any], list[str], dict[str, dict[str, list[str]]], dict[tuple[str, str], float | None]]:
    try:
        return _load_frozen(input_path, rank_path, return_path)
    except (ValueError, TypeError, KeyError, OSError) as exc:
        raise baseline.RotationL2Error("hmm_risk_rotation_l2_value_identity_invalid", str(exc)) from exc


def _load_frozen(
    input_path: Path, rank_path: Path, return_path: Path
) -> tuple[dict[str, Any], list[str], dict[str, dict[str, list[str]]], dict[tuple[str, str], float | None]]:
    """Read only the original bounded bundle and sealed model results, not source HDF/DB."""
    original = _sealed_file(input_path)
    require(original.get("input_hash") == return_model.ORIGINAL_INPUT_HASH, "original input pin differs")
    parsed = rank_model.validate_input(original)
    rank = _sealed_file(rank_path, BYTE_PINS["rank"])
    candidate = _sealed_file(return_path, BYTE_PINS["return"])
    rank_model._reference(rank_path, variant=rank_model, pins=RANK_PINS)
    rank_model._reference(return_path, variant=return_model, pins=RETURN_PINS)
    require(rank["input_identity"] == original["source"]["identity"], "rank source authority differs")
    for key in ("mapping_hash", "quote_authority_hash"):
        require(rank[key] == candidate[key] == parsed["identity"][key], key + " differs")
    fields = ("trade_date", "sector_code", "outcome_status", "relative_return_10d")
    facts = [{k: r[k] for k in fields if k in r} for r in rank["predictions"]]
    require(facts == [{k: r[k] for k in fields if k in r} for r in candidate["predictions"]], "outcome facts differ")
    delta, _, pred = ridge.feature_rows(rank_model._old_bundle(original))
    delta = [{k: v for k, v in r.items() if k != "x"} for r in delta if r["trade_date"] >= pred[0].isoformat()]
    evaluated = return_model._evaluate(delta, {"rows": facts}, list(parsed["calendar"]))
    require(evaluated["metrics"] == rank["baseline_metrics"], "zero-fit delta sealed metrics differ")
    days = [d.isoformat() for d in pred]
    decisions = days[:-HOLD_DAYS]
    require(
        len(decisions) == 222
        and decisions[0] == CONTRACT["decision_start"]
        and decisions[-1] == CONTRACT["decision_end"]
        and days[-1] == CONTRACT["valuation_end"],
        "approved 222/232 date ledger differs",
    )
    directory = set(parsed["catalog_codes"])
    require(len(directory) == 131, "131 L2 denominator differs")
    grids = {}
    for arm, rows in (("rank", rank["predictions"]), ("return", candidate["predictions"]), ("delta", delta)):
        grid: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
        for row in rows:
            day, code = row["trade_date"], row["sector_code"]
            require(day in days and code in directory and code not in grid[day], "prediction directory invalid")
            require(row["availability"] in {"available", "unavailable"}, "unknown availability")
            if row["availability"] == "available":
                require(
                    row["forecast_state"] in {"trending", "neutral", "fading"} and _number(row["rotation_score"]),
                    "available prediction state/score invalid",
                )
            grid[day][code] = row
        require(
            set(grid) == set(days) and all(set(r) == directory for r in grid.values()), "full prediction grid differs"
        )
        grids[arm] = grid
    groups = {arm: {} for arm in ARMS}
    for day in decisions:
        common = set.intersection(
            *({c for c, r in grids[a][day].items() if r["availability"] == "available"} for a in ARMS[:3])
        )
        groups["no_order"][day] = sorted(common)
        for arm in ARMS[:3]:
            groups[arm][day] = sorted(c for c in common if grids[arm][day][c]["forecast_state"] == "trending")
    # Fit input carries training labels only. Read the existing bounded evaluation
    # view from the same pinned release, without rebuilding inputs or predictions.
    quote_facts = rank_model.read_evaluation_facts(original)
    ridge.verify(quote_facts, "outcome_sha256")
    require(quote_facts["outcome_sha256"] == RANK_PINS["outcome_sha256"], "frozen label quote view differs")
    quotes: dict[tuple[str, str], float | None] = {}
    for row in quote_facts["sector_returns"]:
        day, code = row["trade_date"], row["sector_code"]
        require(day <= CONTRACT["valuation_end"], "tail quote in bounded bundle")
        if day not in days:
            continue
        key = day, code
        require(code in directory and key not in quotes, "quote identity duplicated/unknown")
        value = row["pct_change"]
        if row["quote_available"] is False:
            require(value is None, "finite quote outside availability authority", "quote_invalid")
            quotes[key] = None
        else:
            require(
                row["quote_available"] is True and _number(value) and 1 + value / 100 > 0,
                "required quote invalid",
                "quote_invalid",
            )
            quotes[key] = value / 100
    require(
        set(quotes) == {(d, c) for d in days for c in directory}, "valuation quote grid incomplete", "quote_invalid"
    )
    pins = {
        "original_input_hash": original["input_hash"],
        "rank": dict(RANK_PINS),
        "return": dict(RETURN_PINS),
        "source_identity": parsed["identity"],
        "sector_count": 131,
        "mature_decisions": 222,
        "common_population_sha256": canonical_sha256(groups["no_order"]),
        "consumption_groups_sha256": canonical_sha256(groups),
    }
    return pins, days, groups, quotes


def replay_path(
    days: Sequence[str],
    decisions: Sequence[str],
    groups: Mapping[str, Sequence[str]],
    quotes: Mapping[tuple[str, str], float | None],
    *,
    cost_bps: int,
) -> list[dict[str, Any]]:
    """Ten isolated sleeves; legitimate held NA makes cash proceeds unknowable, never reset."""
    require(cost_bps in COSTS and type(cost_bps) is int, "unapproved cost")
    require(
        list(days) == sorted(set(days)) and list(decisions) == list(days[: len(decisions)]), "calendar order differs"
    )
    require(len(days) == len(decisions) + HOLD_DAYS and set(groups) == set(decisions), "maturity ledger differs")
    require(all(date.fromisoformat(d) < date(2026, 4, 1) for d in days), "tail date forbidden")
    c = cost_bps / 10000
    sleeves = [{"cash": 0.1, "assets": {}, "exit_index": None, "unknown": False} for _ in range(HOLD_DAYS)]
    path, previous_nav = [], 1.0
    for index, day in enumerate(days):
        buy = sell = fees = 0.0
        legal_na = []
        for cohort, sleeve in enumerate(sleeves):
            if sleeve["unknown"]:
                continue
            for code, wealth in list(sleeve["assets"].items()):
                require((day, code) in quotes, "held quote absent", "quote_invalid")
                r = quotes[day, code]
                if r is None:
                    sleeve["unknown"] = True
                    legal_na.append({"cohort": cohort, "sector_code": code})
                else:
                    require(_number(r) and 1 + r > 0, "held quote nonfinite/impossible", "quote_invalid")
                    sleeve["assets"][code] = wealth * (1 + r)
            if sleeve["unknown"]:
                continue
            if sleeve["exit_index"] == index:
                notional = math.fsum(sleeve["assets"].values())
                sell += notional
                fees += notional * c
                sleeve["cash"] = notional * (1 - c)
                sleeve["assets"] = {}
                sleeve["exit_index"] = None
        entry_codes = []
        if index < len(decisions):
            sleeve = sleeves[index % HOLD_DAYS]
            require(not sleeve["assets"] or sleeve["unknown"], "cohort reinvested before exit")
            codes = list(groups[day])
            require(codes == sorted(set(codes)), "entry group is not canonical unique")
            if not sleeve["unknown"] and codes:
                for code in codes:
                    require((day, code) in quotes, "entry quote absent", "quote_invalid")
                    if quotes[day, code] is None:
                        sleeve["unknown"] = True
                        legal_na.append({"cohort": index % HOLD_DAYS, "sector_code": code})
                if not sleeve["unknown"]:
                    notional = sleeve["cash"] / (1 + c)
                    buy += notional
                    fees += notional * c
                    sleeve["assets"] = {code: notional / len(codes) for code in codes}
                    sleeve["cash"] = 0.0
                    sleeve["exit_index"] = index + HOLD_DAYS
                    entry_codes = codes
        known = not any(s["unknown"] for s in sleeves)
        assets = math.fsum(math.fsum(s["assets"].values()) for s in sleeves) if known else None
        cash = math.fsum(s["cash"] for s in sleeves) if known else None
        nav = cash + assets if known else None
        if known:
            require(_number(nav) and nav > 0, "nonfinite/nonpositive reference wealth", "quote_invalid")
        daily_return = nav / previous_nav - 1 if nav is not None and previous_nav is not None else None
        path.append(
            {
                "trade_date": day,
                "nav": nav,
                "daily_return": daily_return,
                "cash": cash,
                "exposure": assets / nav if known else None,
                "buy_notional": buy if known else None,
                "sell_notional": sell if known else None,
                "cost": fees if known else None,
                "entry_count": len(entry_codes),
                "entry_cohort": index % HOLD_DAYS if index < len(decisions) else None,
                "legal_held_quote_na": legal_na,
                "valuation_status": "AVAILABLE" if known else "LEGAL_HELD_QUOTE_NA_PATH_UNAVAILABLE",
            }
        )
        previous_nav = nav
    return path


def _summary(path: Sequence[Mapping[str, Any]], *, initial_nav: float | None = None) -> dict[str, Any]:
    valid = [r for r in path if r["nav"] is not None]
    complete = len(valid) == len(path) and bool(path) and initial_nav is not None
    peak, mdd = initial_nav, 0.0
    for row in valid:
        if peak is None:
            peak = row["nav"]
        peak = max(peak, row["nav"])
        mdd = min(mdd, row["nav"] / peak - 1)
    return {
        "status": "AVAILABLE" if complete else "INSUFFICIENT_REFERENCE_PATH",
        "date_count": len(path),
        "valued_dates": len(valid),
        "unavailable_dates": len(path) - len(valid),
        "cumulative_return": path[-1]["nav"] / initial_nav - 1 if complete else None,
        "max_drawdown": mdd if complete else None,
        "mean_exposure": math.fsum(r["exposure"] for r in valid) / len(valid) if valid else None,
        "mean_cash": math.fsum(r["cash"] for r in valid) / len(valid) if valid else None,
        "buy_notional": math.fsum(r["buy_notional"] for r in valid),
        "sell_notional": math.fsum(r["sell_notional"] for r in valid),
        "two_way_turnover_initial_nav": math.fsum(r["buy_notional"] + r["sell_notional"] for r in valid),
        "cost_paid": math.fsum(r["cost"] for r in valid),
        "activity_totals_basis": "FULL_PATH" if complete else "KNOWN_VALUATIONS_ONLY_NOT_FULL_PERIOD_TOTAL",
    }


def summarize(path: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result = _summary(path, initial_nav=1.0)
    result["blocks"] = {}
    for name, first, last in ridge.BLOCKS:
        rows = [r for r in path if first.isoformat() <= r["trade_date"] <= last.isoformat()]
        before = [r for r in path if r["trade_date"] < first.isoformat()]
        start_nav = before[-1]["nav"] if before else 1.0
        result["blocks"][name] = _summary(rows, initial_nav=start_nav)
    segments, current = [], []
    for row in path:
        if row["nav"] is not None:
            current.append(row)
        elif current:
            segments.append(current)
            current = []
    if current:
        segments.append(current)
    result["continuous_valued_blocks"] = [
        {
            "start": rows[0]["trade_date"],
            "end": rows[-1]["trade_date"],
            "dates": len(rows),
            "nav_start": rows[0]["nav"],
            "nav_end": rows[-1]["nav"],
        }
        for rows in segments
    ]
    return result


def paired(
    days: Sequence[str], first: Sequence[Mapping[str, Any]], second: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    require(
        [r["trade_date"] for r in first] == list(days) == [r["trade_date"] for r in second], "paired calendar differs"
    )
    diffs = {
        date.fromisoformat(d): a["daily_return"] - b["daily_return"]
        for d, a, b in zip(days, first, second, strict=True)
        if a["daily_return"] is not None and b["daily_return"] is not None
    }
    calendar = [date.fromisoformat(d) for d in days]
    return {
        "paired_dates": len(diffs),
        "unavailable_dates": len(days) - len(diffs),
        "daily_difference": {d.isoformat(): v for d, v in diffs.items()},
        "hac": baseline._newey_west(calendar, diffs, lag=9),
        "mean_exposure_difference": math.fsum(
            a["exposure"] - b["exposure"]
            for a, b in zip(first, second, strict=True)
            if a["daily_return"] is not None and b["daily_return"] is not None
        )
        / len(diffs)
        if diffs
        else None,
        "blocks": {
            n: baseline._newey_west(calendar, {d: v for d, v in diffs.items() if lo <= d <= hi}, lag=9)
            for n, lo, hi in ridge.BLOCKS
        },
    }


def execute(*, input_path: Path, rank_path: Path, return_path: Path, executor_commit: str) -> dict[str, Any]:
    pins, days, groups, quotes = load_frozen(input_path, rank_path, return_path)
    decisions = days[:-HOLD_DAYS]
    costs = {}
    for bps in COSTS:
        paths = {a: replay_path(days, decisions, groups[a], quotes, cost_bps=bps) for a in ARMS}
        costs[str(bps)] = {
            "arms": {a: {"summary": summarize(paths[a]), "daily": paths[a]} for a in ARMS},
            "paired": {
                a + "_minus_" + b: paired(days, paths[a], paths[b])
                for a, b in (
                    ("rank", "no_order"),
                    ("return", "no_order"),
                    ("delta", "no_order"),
                    ("return", "rank"),
                    ("return", "delta"),
                )
            },
        }
    body = {
        "schema_version": VERSION + "_result",
        "contract": CONTRACT,
        "executor_commit": executor_commit,
        "source_pins": pins,
        "decision_start": decisions[0],
        "decision_end": decisions[-1],
        "decision_count": len(decisions),
        "valuation_start": days[0],
        "valuation_end": days[-1],
        "valuation_days": len(days),
        "daily_population": {d: {a: len(groups[a][d]) for a in ARMS} for d in decisions},
        "cost_paths": costs,
        "status": "REFERENCE_REPLAY_COMPLETED"
        if all(v["summary"]["status"] == "AVAILABLE" for c in costs.values() for v in c["arms"].values())
        else "INSUFFICIENT_REFERENCE_PATH",
        "new_fits": 0,
        "new_filter_calls": 0,
        "new_predict_calls": 0,
        "tail_accessed": False,
        "database_access": False,
        "dataset_write": False,
        "runtime_action": False,
        "qe_net_value_status": "UNASSESSED",
        "production_adoption": False,
        "selection_basis": CONTRACT["selection_basis"],
        "limitations": [
            "OFFICIAL_INDEX_REFERENCE_NOT_EXECUTABLE_STOCK_RETURNS",
            "ASSUMED_ONE_WAY_COST_NOT_ACTUAL_FEES",
            "HAC_NOT_ADJUSTED_FOR_RESEARCH_SELECTION_HISTORY",
            "EXPOSURE_DIFFERENCES_NOT_FORCED_EQUAL",
        ],
    }
    return ridge.seal(body, "result_sha256")
