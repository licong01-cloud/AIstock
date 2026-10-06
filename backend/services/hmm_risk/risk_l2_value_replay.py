"""Approved L2 warning-to-cash reference replay. No model or database execution."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import math
from pathlib import Path
from typing import Mapping
from unittest.mock import patch

from backend.services.hmm_risk import risk_l2, risk_l2_prediction as original
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt

VERSION = "hmm_risk_l2_value_replay_v1"
APPROVED_PINS = {
    "acceptance_hash": "88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26",
    "sealed_hash": "255cf2ee7dbbbc11738108fde9202c7bdee7168be5eb1a5085737bb6d5704b77",
    "features_hash": "2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70",
    "facts_hash": "3101fcd9152eb87b8b207ac60071df5d241043cecb71fb5008d9a3f2dcd562f1",
    "model_hash": "37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d",
    "input_hash": "9fd18ad4ee25efd8a76b3aaf0e07e26edee1c39fe31c05b365322c9ea41ef1dc",
    "mapping_hash": "4e061da6773fdd364c86b2c75fb517132f55e0537c1513057cf9c3f6c8e6828e",
}
ARMS = ("B", "R", "X")
COST_BPS = (0, 5, 10, 20)


class ReplayError(RuntimeError):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason_code = "hmm_risk_l2_value_" + reason


def require(condition: bool, message: str, reason: str = "input_invalid") -> None:
    if not condition:
        raise ReplayError(reason, message)


def finite(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


@contextmanager
def zero_compute():
    """Poison the actual producer, estimator and database entrypoints in each child."""

    def forbidden(*args, **kwargs):
        raise ReplayError("forbidden_action", "fit/predict/rebuild/database is forbidden")

    targets = (
        "backend.services.hmm_risk.risk_l2.fit_predict",
        "backend.services.hmm_risk.risk_l2.prepare_file_inputs",
        "backend.services.hmm_risk.risk_l2.drawdown_outcomes",
        "sklearn.linear_model.LogisticRegression.fit",
        "sklearn.linear_model.LogisticRegression.predict",
        "sklearn.linear_model.LogisticRegression.predict_proba",
        "backend.db.pg_pool.get_conn",
        "backend.services.hmm_risk.risk_l2_prediction.get_conn",
        "psycopg2.connect",
    )
    with ExitStack() as stack:
        for target in targets:
            stack.enter_context(patch(target, forbidden))
        yield


def load_inputs(request_path: Path, expected_hash: str) -> tuple[dict, list[str], list[str], dict, dict]:
    """Read only four explicitly pinned original assets, reusing the original validator."""
    try:
        request, request_stamp = original._read(request_path)
        verify_receipt(request, expected_hash)
        require(request.get("schema_version") == VERSION + "_request", "unknown request schema")
        require(all(request.get(k) == v for k, v in APPROVED_PINS.items()), "approved pins differ", "identity_mismatch")
        assets, stamps = {}, {}
        for name in ("acceptance", "sealed", "features", "facts"):
            path = Path(request[name + "_path"])
            assets[name], stamps[name] = original._read(path)
            verify_receipt(assets[name], request[name + "_hash"])
        acceptance, features, facts = (assets[k] for k in ("acceptance", "features", "facts"))
        identity = features["input_identity"]
        import_request = {
            **request,
            "schema_version": original.REQUEST_SCHEMA,
            "executor_commit": acceptance["executor_commit"],
            "pit_bundle_sha256": identity["pit_bundle_sha256"],
            "dataset_manifest_sha256": identity["release_identity"]["dataset_manifest_sha256"],
        }
        product = original.product_from_assets(import_request, acceptance, assets["sealed"], features)
        days, catalog = product.run["dates"], product.run["catalog"]
        require(
            len(days) == 424 and days[0] == "2024-07-01" and days[-1] == "2026-03-31",
            "approved 424 decision days differ",
        )
        require(
            facts["schema_version"] == risk_l2.VERSION + "_outcome_facts"
            and facts["feature_sha256"] == request["features_hash"]
            and facts["calendar"] == features["calendar"]
            and facts["catalog"] == catalog
            and facts["source_identity"] == identity
            and facts["tail_accessed"] is False
            and sorted(facts["returns"]) == days[1:],
            "facts identity/calendar or exact 423-day population differs",
            "identity_mismatch",
        )
        signals = {d: {} for d in days}
        for row in product.rows:
            signals[row["trade_date"]][row["sector_code"]] = {
                k: row[k] for k in ("as_of_date", "probability", "warning", "availability", "reason_code")
            }
        for name in assets:
            original._stable(Path(request[name + "_path"]), stamps[name])
        original._stable(request_path, request_stamp)
        return request, catalog, days, signals, facts["returns"]
    except ReplayError:
        raise
    except (
        original.RiskL2PredictionError,
        FormalStateError,
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        raise ReplayError("input_invalid", "original file-only evidence failed validation") from exc


def _budgets(catalog: list[str], signals: Mapping) -> tuple[dict, list[str], list[str]]:
    eligible, warnings = [], []
    for code in catalog:
        row = signals[code]
        p = row["probability"]
        if row["availability"] == "available":
            require(
                finite(p)
                and 0 <= p <= 1
                and type(row["warning"]) is bool
                and row["warning"] == (p >= 0.20)
                and row["reason_code"] is None,
                "probability/warning is invalid",
            )
            eligible.append(code)
            if row["warning"]:
                warnings.append(code)
        else:
            require(
                row["availability"] == "unavailable"
                and p is None
                and row["warning"] is None
                and isinstance(row["reason_code"], str)
                and bool(row["reason_code"].strip()),
                "unavailable signal lost its typed nulls",
            )
    e, w = set(eligible), set(warnings)
    b = 1.0 / 131
    matched = (len(e) - len(w)) / (131 * len(e)) if e else 0.0
    targets = {
        "B": {c: b if c in e else 0.0 for c in catalog},
        "R": {c: b if c in e and c not in w else 0.0 for c in catalog},
        "X": {c: matched if c in e else 0.0 for c in catalog},
    }
    require(
        math.isclose(math.fsum(targets["R"].values()), math.fsum(targets["X"].values()), abs_tol=1e-12),
        "matched exposure differs",
        "formula_invalid",
    )
    require(
        all(0 <= math.fsum(a.values()) <= 1 + 1e-12 for a in targets.values()), "budget exceeds one", "formula_invalid"
    )
    return targets, eligible, warnings


def _blocks(dates: list[str], values: list[float | None]) -> list[dict]:
    blocks = []
    current = None
    depleted = False
    for day, value in zip(dates, values, strict=True):
        if depleted:
            break
        if value is None:
            current = None
            continue
        if current is None:
            current = {
                "start": day,
                "end": day,
                "date_count": 0,
                "wealth": 1.0,
                "peak": 1.0,
                "maximum_drawdown": 0.0,
                "worst_daily_return": value,
                "downside_squared_loss": 0.0,
                "capital_depleted": False,
            }
            blocks.append(current)
        current["end"] = day
        current["date_count"] += 1
        current["wealth"] *= max(0.0, 1 + value)
        require(math.isfinite(current["wealth"]), "reference wealth is non-finite", "numeric_invalid")
        current["peak"] = max(current["peak"], current["wealth"])
        current["maximum_drawdown"] = min(current["maximum_drawdown"], current["wealth"] / current["peak"] - 1)
        current["worst_daily_return"] = min(current["worst_daily_return"], value)
        current["downside_squared_loss"] += min(0.0, value) ** 2
        if value <= -1:
            current["capital_depleted"] = depleted = True
    return [
        {**{k: v for k, v in block.items() if k not in {"wealth", "peak"}}, "cumulative_return": block["wealth"] - 1}
        for block in blocks
    ]


def _distribution(values: list[float]) -> dict:
    ordered = sorted(values)

    def quantile(q):
        if not ordered:
            return None
        p = (len(ordered) - 1) * q
        lower = int(p)
        return ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower]) * (p - lower)

    return {
        "count": len(values),
        "mean": math.fsum(values) / len(values) if values else None,
        "positive_share": sum(v > 0 for v in values) / len(values) if values else None,
        "quantiles": {str(q): quantile(q) for q in (0, 0.25, 0.5, 0.75, 1)},
    }


def replay(catalog: list[str], days: list[str], signals: Mapping, returns: Mapping) -> dict:
    """Pure reference arithmetic; production loader independently enforces all 423 days."""
    require(len(catalog) == 131 and catalog == sorted(set(catalog)), "131 canonical sectors required")
    require(
        all(original._iso(d) == d for d in days) and days == sorted(set(days)) and len(days) >= 2,
        "decision calendar is invalid",
    )
    require(set(signals) == set(days) and set(returns) == set(days[1:]), "dates cannot be omitted, added or shortened")
    for day in days:
        require(set(signals[day]) == set(catalog), "signal catalog differs")
        _budgets(catalog, signals[day])
        as_of = {signals[day][c]["as_of_date"] for c in catalog}
        require(len(as_of) == 1 and original._iso(next(iter(as_of))) < day, "signal as-of is not prior")
        if day != days[0]:
            require(next(iter(as_of)) == days[days.index(day) - 1], "signal as-of differs from frozen prior open date")
    for values in returns.values():
        require(set(values) == set(catalog), "return catalog differs")
        require(
            all(v is None or (finite(v) and v >= -1) for v in values.values()),
            "return must be legal NA or finite >= -1",
            "numeric_invalid",
        )
    paths = {a: [] for a in ARMS}
    costs = {a: {bp: [] for bp in COST_BPS} for a in ARMS}
    previous = {a: {c: 0.0 for c in catalog} for a in ARMS}
    depleted = {a: False for a in ARMS}
    daily, warning_returns = [], []
    missing_warning_returns = 0
    for s, t in zip(days[:-1], days[1:], strict=True):
        targets, eligible, warnings = _budgets(catalog, signals[s])
        values = returns[t]
        missing = [c for c in eligible if values[c] is None]
        paired = not missing
        for code in warnings:
            if values[code] is None:
                missing_warning_returns += 1
            else:
                warning_returns.append(values[code])
        arms = {}
        for a in ARMS:
            weights = targets[a]
            exposure = math.fsum(weights.values())
            turnover = math.fsum(abs(weights[c] - previous[a][c]) for c in catalog) if previous[a] is not None else None
            gross = math.fsum(weights[c] * values[c] for c in catalog if weights[c] != 0) if paired else None
            if depleted[a]:
                gross, turnover = None, None
            paths[a].append(gross)
            sensitivity = {
                str(bp): gross
                if bp == 0
                else gross - bp / 10000 * turnover
                if gross is not None and turnover is not None
                else None
                for bp in COST_BPS
            }
            for bp in COST_BPS:
                costs[a][bp].append(sensitivity[str(bp)])
            arms[a] = {
                "risk_budget": exposure,
                "cash_budget": 1 - exposure,
                "gross_return": gross,
                "one_sided_risk_turnover": turnover if paired else None,
                "cost_sensitivity_return": sensitivity,
                "target_weights_sha256": canonical_sha256(weights),
            }
            if gross is None:
                previous[a] = None
                arms[a]["closing_cash_budget"] = None
            elif gross <= -1:
                depleted[a], previous[a] = True, None
                arms[a]["closing_cash_budget"] = None
            else:
                previous[a] = {c: weights[c] * (1 + values[c]) / (1 + gross) if weights[c] else 0.0 for c in catalog}
                require(all(finite(v) for v in previous[a].values()), "drifted budget is non-finite", "numeric_invalid")
                cash = (1 - exposure) / (1 + gross)
                require(
                    finite(cash) and math.isclose(math.fsum(previous[a].values()) + cash, 1.0, abs_tol=1e-12),
                    "closing risk/cash budget differs",
                    "formula_invalid",
                )
                arms[a]["closing_cash_budget"] = cash
        daily.append(
            {
                "return_date": t,
                "signal_date": s,
                "signal_as_of": signals[s][catalog[0]]["as_of_date"],
                "catalog_count": 131,
                "eligible_count": len(eligible),
                "warning_count": len(warnings),
                "input_unavailable_cash_count": 131 - len(eligible),
                "paired_return_available": all(arms[a]["gross_return"] is not None for a in ARMS),
                "reason_code": "LEGAL_REFERENCE_RETURN_NA"
                if missing
                else "REFERENCE_CAPITAL_DEPLETED"
                if any(depleted.values())
                else None,
                "missing_held_return_sectors": missing,
                "arms": arms,
            }
        )
    dates = days[1:]
    paired_paths = {
        a: [v if d["paired_return_available"] else None for d, v in zip(daily, paths[a], strict=True)] for a in ARMS
    }
    gross_blocks = {a: _blocks(dates, paired_paths[a]) for a in ARMS}
    complete = all(v is not None for values in paths.values() for v in values) and not any(depleted.values())
    exposure_exists = any(d["eligible_count"] > 0 for d in daily)
    summary = {a: gross_blocks[a][0] if complete else None for a in ARMS}
    comparisons = {}
    for comparator in ("B", "X"):
        comparisons["R_minus_" + comparator] = {
            field: summary["R"][field] - summary[comparator][field] if complete else None
            for field in ("cumulative_return", "maximum_drawdown", "worst_daily_return", "downside_squared_loss")
        }
    block_comparisons = []
    for b, r, x in zip(*(gross_blocks[a] for a in ARMS), strict=True):
        require(
            all((v["start"], v["end"], v["date_count"]) == (r["start"], r["end"], r["date_count"]) for v in (b, x)),
            "paired blocks differ",
            "formula_invalid",
        )
        block_comparisons.append(
            {
                "start": r["start"],
                "end": r["end"],
                "date_count": r["date_count"],
                "diagnostic_only_not_full_path": not complete,
                **{
                    "R_minus_" + name: {
                        field: r[field] - other[field]
                        for field in (
                            "cumulative_return",
                            "maximum_drawdown",
                            "worst_daily_return",
                            "downside_squared_loss",
                        )
                    }
                    for name, other in (("B", b), ("X", x))
                },
            }
        )
    status = (
        "INSUFFICIENT_REFERENCE_PATH"
        if not complete or not exposure_exists
        else (
            "REFERENCE_RISK_REDUCTION_OBSERVED"
            if comparisons["R_minus_X"]["maximum_drawdown"] > 0
            else "REFERENCE_RISK_REDUCTION_NOT_OBSERVED"
        )
    )
    return receipt(
        {
            "schema_version": VERSION + "_result",
            "status": status,
            "reference_basis": "GROSS_SYNTHETIC_L2_REFERENCE",
            "validation_basis": risk_l2.BASIS,
            "net_value_status": "UNASSESSED",
            "forward_confirmed": False,
            "planned_return_dates": len(dates),
            "return_start": dates[0],
            "return_end": dates[-1],
            "sector_count": 131,
            "paired_return_dates": sum(d["paired_return_available"] for d in daily),
            "legal_na_dates": [d["return_date"] for d in daily if d["missing_held_return_sectors"]],
            "reference_capital_depleted": any(depleted.values()),
            "complete_reference_path": complete,
            "prediction_availability": {
                "planned_sector_dates": 131 * len(daily),
                "available_sector_dates": sum(d["eligible_count"] for d in daily),
                "unavailable_policy": "INPUT_UNAVAILABLE_CASH",
            },
            "gross_full_path": summary,
            "gross_blocks": gross_blocks,
            "comparisons": comparisons,
            "paired_block_comparisons": block_comparisons,
            "exposure": {
                a: {
                    "mean_risk_budget": math.fsum(d["arms"][a]["risk_budget"] for d in daily) / len(daily),
                    "final_risk_budget": daily[-1]["arms"][a]["risk_budget"],
                    "known_turnover_sum": math.fsum(
                        d["arms"][a]["one_sided_risk_turnover"]
                        for d in daily
                        if d["arms"][a]["one_sided_risk_turnover"] is not None
                    ),
                    "unknown_turnover_dates": sum(d["arms"][a]["one_sided_risk_turnover"] is None for d in daily),
                }
                for a in ARMS
            },
            "opportunity_cost": {
                "R_minus_B_cumulative_return": comparisons["R_minus_B"]["cumulative_return"],
                "warning_return_distribution": _distribution(warning_returns),
                "warning_return_na_count": missing_warning_returns,
                "foregone_positive_budget_return_sum": math.fsum(max(0, v) / 131 for v in warning_returns),
            },
            "cost_sensitivity": {
                "basis": "ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET",
                "one_sided_cost_bps": list(COST_BPS),
                "paths": {a: {str(bp): _blocks(dates, costs[a][bp]) for bp in COST_BPS} for a in ARMS},
                "break_even": {"status": "NOT_COMPUTABLE", "reason": "REAL_EXECUTION_COST_AND_BREAK_EVEN_NOT_MODELLED"},
            },
            "daily": daily,
            "new_fits": 0,
            "new_filter_calls": 0,
            "new_predict_calls": 0,
            "database_access": False,
            "tail_accessed": False,
            "dataset_write": False,
            "runtime_action": False,
        }
    )


def execute(request_path: Path, expected_hash: str) -> dict:
    with zero_compute():
        request, catalog, days, signals, returns = load_inputs(request_path, expected_hash)
        result = replay(catalog, days, signals, returns)
        return receipt(
            {
                **{k: v for k, v in result.items() if k != "receipt_sha256"},
                "request_sha256": expected_hash,
                "source_pins": APPROVED_PINS,
                "source_paths": {k: request[k + "_path"] for k in ("acceptance", "sealed", "features", "facts")},
                "zero_compute_poison_active": True,
            }
        )
