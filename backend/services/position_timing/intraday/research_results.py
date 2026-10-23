"""Validate and compare LocalSIM-owned economic results; never create fills."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .contracts import CHINA, Contract, NonNegative, Positive, Quantity, Sha256, Timestamp, digest, payload, utc
from .replay import FileRef
from .research_inputs import checked_file
from .store import IntradayError, IntradayStore


class EconomicRow(Contract):
    case_id: Sha256
    initial_account_sha256: Sha256
    initial_nav_cny: Positive
    target_quantity: Quantity
    target_notional_cny: Positive
    filled_quantity: Quantity
    average_price_raw: Positive | None
    fees_cny: NonNegative
    terminal_as_of: Timestamp
    terminal_nav_cny: NonNegative | None
    max_drawdown: NonNegative | None
    mean_wait_seconds: NonNegative | None
    status: Literal["FILLED", "PARTIAL", "NO_FILL", "UNAVAILABLE"]
    reason_code: str = Field(min_length=1)
    account_receipt_sha256: Sha256

    @model_validator(mode="after")
    def consistent(self):
        if self.target_quantity == 0 or self.filled_quantity > self.target_quantity:
            raise ValueError("invalid target/fill quantities")
        if (self.filled_quantity > 0) != (self.average_price_raw is not None):
            raise ValueError("fill price must exist exactly when quantity is positive")
        if (self.status == "FILLED" and self.filled_quantity != self.target_quantity
                or self.status == "PARTIAL" and not 0 < self.filled_quantity < self.target_quantity
                or self.status == "NO_FILL" and self.filled_quantity != 0):
            raise ValueError("status does not match quantity")
        if self.status != "UNAVAILABLE" and (self.terminal_nav_cny is None or self.max_drawdown is None):
            raise ValueError("all completed evaluations, including no-fill, need terminal valuation and MDD")
        if self.max_drawdown is not None and self.max_drawdown > 1:
            raise ValueError("MDD must be a nonnegative fraction <= 1")
        if self.filled_quantity and self.mean_wait_seconds is None:
            raise ValueError("filled rows need observed wait")
        return self


class EconomicResults(Contract):
    schema_version: Literal["position_timing_localsim_research_results_v1"] = "position_timing_localsim_research_results_v1"
    producer: Literal["LOCALSIM"]
    producer_source_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    release_identity: str = Field(min_length=1)
    study_manifest_sha256: Sha256
    cost_policy_file_sha256: Sha256
    execution_contract_sha256: Sha256
    max_drawdown_definition: Literal["POSITIVE_PEAK_TO_TROUGH_FRACTION"]
    rows: tuple[EconomicRow, ...]


def summarize(consumption: dict, results: EconomicResults) -> dict:
    from .research import ARMS, DELAYS

    expected = {c["case_id"]: c for c in consumption["cases"] if c["applicable"]}
    rows = {row.case_id: row for row in results.rows}
    if len(rows) != len(results.rows) or set(rows) != set(expected):
        raise IntradayError("RESEARCH_RESULT_POPULATION_MISMATCH", 422)
    keyed = {(case["plan_id"], case["policy_id"], case["delay_seconds"]): rows[key] for key, case in expected.items()}
    paired = []
    for key, case in expected.items():
        row = rows[key]
        baseline = keyed[(case["plan_id"], ARMS[0], case["delay_seconds"])]
        terminal = utc(datetime.combine(datetime.fromisoformat(case["trade_date"]).date(), time(14, 57), CHINA))
        if row.target_quantity != case["target_quantity"] or row.terminal_as_of != terminal:
            raise IntradayError("RESEARCH_RESULT_TARGET_OR_TERMINAL_MISMATCH", 422)
        attributes = ("initial_account_sha256", "initial_nav_cny", "target_quantity", "target_notional_cny", "terminal_as_of")
        if any(getattr(row, name) != getattr(baseline, name) for name in attributes):
            raise IntradayError("RESEARCH_RESULT_NOT_MATCHED_TO_BASELINE", 422)
        usable = row.status != "UNAVAILABLE" and baseline.status != "UNAVAILABLE"
        paired_price = usable and row.filled_quantity > 0 and baseline.filled_quantity > 0
        sign = Decimal(1) if case["side"] == "BUY" else Decimal(-1)
        paired.append({**case, "result_status": row.status, "paired": usable,
                       "filled": row.filled_quantity > 0, "fully_completed": row.status == "FILLED",
                       "weight_cny": row.target_notional_cny,
                       "net_lift_bps": (row.terminal_nav_cny - baseline.terminal_nav_cny) / row.initial_nav_cny * 10000 if usable else None,
                       "price_lift_bps": sign * (baseline.average_price_raw - row.average_price_raw) / baseline.average_price_raw * 10000 if paired_price else None,
                       "mdd_reduction": baseline.max_drawdown - row.max_drawdown if usable else None,
                       "mean_wait_seconds": row.mean_wait_seconds, "fees_cny": row.fees_cny,
                       "reason_code": row.reason_code})
    summaries = []
    for arm in ARMS:
        for delay in DELAYS:
            for side in ("ALL", "BUY", "SELL"):
                population = [c for c in consumption["cases"] if c["policy_id"] == arm and c["delay_seconds"] == delay and (side == "ALL" or c["side"] == side)]
                selected = [r for r in paired if r["policy_id"] == arm and r["delay_seconds"] == delay and (side == "ALL" or r["side"] == side)]
                summary = {"policy_id": arm, "delay_seconds": delay, "side": side,
                           "original_plan_count": len(population), "applicable_count": len(selected),
                           "not_applicable_count": sum(not c["applicable"] for c in population),
                           "status_counts": dict(Counter(r["result_status"] for r in selected)),
                           "filled_count": sum(r["filled"] for r in selected),
                           "fully_completed_count": sum(r["fully_completed"] for r in selected)}
                for metric in ("net_lift_bps", "price_lift_bps", "mdd_reduction", "mean_wait_seconds", "fees_cny"):
                    observed = [r for r in selected if r[metric] is not None]
                    summary[metric] = {"count": len(observed),
                                       "equal_weighted": sum(r[metric] for r in observed) / len(observed) if observed else None,
                                       "target_notional_weighted": sum(r[metric] * r["weight_cny"] for r in observed) / sum(r["weight_cny"] for r in observed) if observed else None}
                summaries.append(summary)
    return {"paired_rows": paired, "summaries": summaries, "primary_delay_seconds": 60,
            "inference": "DESCRIPTIVE_ONLY_NO_AUTOMATIC_PROMOTION", "native_twap": "NOT_PROVIDED"}


def compare(bundle, results_ref: FileRef):
    from .research import inspect

    root = Path(bundle).resolve()
    verified = inspect(root)  # Entire source population and every child before reading outcomes.
    checked = checked_file(results_ref)
    results = EconomicResults.model_validate_json(checked.read_text(encoding="utf-8"))
    checked_file(results_ref)
    store = IntradayStore(root.parent.parent.parent.parent)
    consumption = store.read(root / "consumption.json")
    if (results.study_manifest_sha256 != verified["manifest_sha256"]
            or results.cost_policy_file_sha256 != consumption["cost_policy"]["sha256"]
            or results.execution_contract_sha256 != consumption["execution_contract_sha256"]):
        raise IntradayError("RESEARCH_RESULT_CONTRACT_IDENTITY_MISMATCH", 422)
    report = {"schema_version": "position_timing_intraday_comparison_v1", "study_manifest_sha256": verified["manifest_sha256"],
              "input_kind": verified["receipt"]["input_kind"], "result_file": payload(results_ref),
              "producer_source_commit": results.producer_source_commit, "release_identity": results.release_identity,
              "outcomes_read_stage": "AFTER_COMPLETE_SOURCE_AND_BUNDLE_INSPECTION",
              "outcomes_read": True, "economic_confirmation": False,
              "account_economics_simulated_here": False, **payload(summarize(consumption, results))}
    path = root.parent.parent / "comparisons" / (digest(report) + ".json")
    existed = path.exists()
    store.write(path, report)
    return {"comparison": str(path), "comparison_sha256": digest(report),
            "status": "ALREADY_MATERIALIZED" if existed else "MATERIALIZED", "report": report}
