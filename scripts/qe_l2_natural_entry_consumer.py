"""Approved pre-rounding natural new-buy budgets; no target/order generation."""

from __future__ import annotations
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import pandas as pd
from qe_l2_continuous_consumer import ContinuousInputs, finite
from qe_l2_relative_consumer import RelativeInputs
from qe_l2_natural_entry_admission import CONTRACT, validate_admission
from qe_l2_risk_admission import canonical_sha
from qe_l2_risk_consumer import checked_asset, require, symbol_key, validate_execution_inputs


class NaturalEntryInputs:
    def __init__(self, manifest_file):
        self.manifest = m = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
        validate_admission(m)
        if "execution_files_sha256" in m:
            code = m["execution_files_sha256"]
            require(
                isinstance(code, dict)
                and code
                and canonical_sha({"files": code, "contract": m["execution_contract"]}) == m["execution_sha256"],
                "QE_L2_NATURAL_EXECUTION_CODE_IDENTITY",
            )
            for name, digest in code.items():
                require(
                    not Path(name).is_absolute()
                    and ".." not in Path(name).parts
                    and name.endswith(".py")
                    and hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest,
                    "QE_L2_NATURAL_EXECUTION_CODE_DRIFT",
                    name,
                )
        require((m["start"], m["end"], m["decision_days"]) == ("2026-04-01", "2026-09-30", 125), "QE_L2_NATURAL_WINDOW")
        fields = {"loss", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
        require(set(m["assets"]) == fields, "QE_L2_NATURAL_ASSET_FIELDS")
        paths = {k: checked_asset(v) for k, v in m["assets"].items() if k != "loss" or m["arm"] == "R-entry"}
        ContinuousInputs._validate_dataset(m)
        self._calendar = pd.DatetimeIndex(pd.to_datetime(paths["calendar"].read_text().splitlines()))
        require(self._calendar.is_unique and self._calendar.is_monotonic_increasing, "QE_L2_NATURAL_CALENDAR")
        selected = self._calendar[(self._calendar >= m["start"]) & (self._calendar <= m["end"])]
        require(
            len(selected) == 125
            and self._calendar.get_loc(selected[0]) > 0
            and str(selected[0].date()) == m["start"]
            and str(selected[-1].date()) == m["end"],
            "QE_L2_NATURAL_WINDOW",
        )
        self.as_of = {str(d.date()): str(self._calendar[self._calendar.get_loc(d) - 1].date()) for d in selected}
        authority = json.loads(paths["authority"].read_text(encoding="utf-8"))
        catalog = {r["canonical_l2_code"] for r in authority["l2_projection"]["rows"]}
        require(
            len(catalog) == 131
            and canonical_sha(authority) == m["authority_canonical_sha256"]
            and authority["research_basis"]["active_mode"] == "historical_replay"
            and authority["research_basis"]["historical_classification_basis"] == "stable_taxonomy_backcast",
            "QE_L2_NATURAL_AUTHORITY",
        )
        self.mapping = ContinuousInputs._mapping(self, paths, authority, catalog, m)
        self.risk = (
            RelativeInputs._loss(self, json.loads(paths["loss"].read_text(encoding="utf-8")), catalog)
            if m["arm"] == "R-entry"
            else {}
        )
        self.receipt = {
            "contract": CONTRACT,
            "arm": m["arm"],
            "seed": m["seed"],
            "days": 125,
            "industries": 131,
            "rows": len(self.risk),
            "new_fits": 0,
            "new_inferences": 0,
            "historical_as_known": False,
            "untouched_holdout": False,
        }

    def risk_context(self, day, stocks):
        require(day in self.as_of, "QE_L2_NATURAL_DECISION_DAY")
        if self.manifest["arm"] == "B0":
            return None
        sectors = {}
        for stock in stocks:
            key = self.as_of[day], symbol_key(stock)
            require(key in self.mapping.index, "QE_L2_NATURAL_MAPPING_MISSING", str(key))
            row = self.mapping.loc[key]
            require(row.mapping_status == "resolved", "QE_L2_NATURAL_MAPPING_UNAVAILABLE", str(key))
            sectors[stock] = row.l2_code
        return {
            "sector_rows": {s: deepcopy(row) for (d, s), row in self.risk.items() if d == day},
            "stock_sectors": sectors,
        }


def adjust_natural_entry_budgets(
    decision_date, as_of, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, original_caps, risk_context
):
    """Return new values and an audit; never mutate inputs or generate orders."""
    require(isinstance(decision_date, str) and isinstance(as_of, str) and as_of < decision_date, "QE_L2_NATURAL_CLOCK")
    require(
        isinstance(ordered_targets, list)
        and all(isinstance(s, str) for s in ordered_targets)
        and len(ordered_targets) == len(set(ordered_targets)),
        "QE_L2_NATURAL_TARGET_ORDER",
    )
    require(
        isinstance(natural_new_buys, list)
        and len(natural_new_buys) == len(set(natural_new_buys))
        and set(natural_new_buys) <= set(ordered_targets)
        and isinstance(base_budgets_cny, dict)
        and set(base_budgets_cny) == set(natural_new_buys),
        "QE_L2_NATURAL_NEW_BUY_KEYS",
    )
    nav = finite(nav_cny, "nav")
    require(
        nav > 0
        and isinstance(original_caps, dict)
        and set(original_caps) == {"max_weight", "max_single_order_value"}
        and original_caps["max_weight"] == 0.05,
        "QE_L2_NATURAL_CAPS",
    )
    single_cap = finite(original_caps["max_single_order_value"], "max_single_order_value")
    require(single_cap > 0, "QE_L2_NATURAL_CAPS")
    base = {s: finite(v, s) for s, v in base_budgets_cny.items()}
    cap = min(0.05 * nav, single_cap)
    require(all(v / nav <= cap / nav + 1e-12 for v in base.values()), "QE_L2_NATURAL_BASE_CAP")
    ranks, rows = {}, {}
    if risk_context is not None:
        require(
            isinstance(risk_context, dict) and set(risk_context) == {"sector_rows", "stock_sectors"},
            "QE_L2_NATURAL_RISK_CONTEXT",
        )
        rows = risk_context["sector_rows"]
        sectors = risk_context["stock_sectors"]
        require(
            isinstance(rows, dict) and len(rows) == 131 and isinstance(sectors, dict) and set(sectors) == set(base),
            "QE_L2_NATURAL_FULL_POPULATION",
        )
        losses = {}
        for sector, row in rows.items():
            require(
                isinstance(row, dict)
                and row["sector_code"] == sector
                and row["trade_date"] == decision_date
                and row["as_of_date"] == as_of
                and row["availability"] == "available"
                and row["reason_code"] is None,
                "QE_L2_NATURAL_RISK_UNAVAILABLE",
            )
            losses[sector] = finite(row["expected_loss"], sector, maximum=1)
        sector_ranks = (pd.Series(losses).rank(method="average") - 1) / 130
        require(set(sectors.values()) <= set(rows), "QE_L2_NATURAL_MAPPING_MISSING")
        ranks = {s: float(sector_ranks[sectors[s]]) for s in base}
    result, groups = dict(base), []
    half = (len(ordered_targets) + 1) // 2
    for targets in (ordered_targets[:half], ordered_targets[half:]):
        stocks = [s for s in targets if s in base]
        total = math.fsum(base[s] for s in stocks)
        mu = math.fsum(base[s] * ranks[s] for s in stocks) / total if total and ranks else None
        for stock in stocks:
            value = base[stock] if mu is None else base[stock] * (1 + 0.10 * (mu - ranks[stock]))
            require(
                math.isfinite(value)
                and 0 <= value
                and 0.90 * base[stock] / nav - 1e-12 <= value / nav <= 1.10 * base[stock] / nav + 1e-12
                and value / nav <= cap / nav + 1e-12,
                "QE_L2_NATURAL_TARGET_CAP",
            )
            result[stock] = value
        require(abs(math.fsum(result[s] for s in stocks) / nav - total / nav) <= 1e-12, "QE_L2_NATURAL_GROUP_BUDGET")
        groups.append(
            {
                "target_group": list(targets),
                "new_buys": stocks,
                "base_budget_cny": total,
                "adjusted_budget_cny": math.fsum(result[s] for s in stocks),
                "risk_mean_rank": mu,
            }
        )
    return result, {
        "trade_date": decision_date,
        "as_of_date": as_of,
        "ordered_targets": list(ordered_targets),
        "natural_new_buys": list(natural_new_buys),
        "nav_cny": nav,
        "original_caps": dict(original_caps),
        "base_budgets_cny": base,
        "adjusted_budgets_cny": dict(result),
        "groups": groups,
        "risk_applied": risk_context is not None,
        "risk_inputs": deepcopy(rows),
        "stock_ranks": ranks,
        "actual_fill_claimed": False,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    parser.add_argument("--config", required=True)
    parser.add_argument("--prediction", required=True)
    args = parser.parse_args()
    inputs = NaturalEntryInputs(args.manifest)
    require(bool(inputs.manifest.get("execution_files_sha256")), "QE_L2_NATURAL_EXECUTION_CODE_REQUIRED")
    validate_execution_inputs(inputs.manifest, args.config, args.prediction)
    import yaml

    conf = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    actual = json.loads(
        json.dumps(
            {k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}, default=lambda x: x.isoformat()
        )
    )
    require(actual == inputs.manifest["execution_contract"], "QE_L2_NATURAL_EXECUTION_CONTRACT_DRIFT")
    print(json.dumps(inputs.receipt, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
