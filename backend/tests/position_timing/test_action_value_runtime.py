from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value import FEATURE_ORDER, TZ, cutoff_on
from backend.services.position_timing.action_value_model import HEADS, fit_local_model, write_local_model
from backend.services.position_timing.action_value_runtime import (
    current_model_advice,
    materialize_model_advice,
)
from backend.services.position_timing.artifact_store import PositionTimingArtifactStore
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


def _training_rows() -> pd.DataFrame:
    rng = np.random.default_rng(29)
    rows = pd.DataFrame(rng.normal(size=(400, len(FEATURE_ORDER))), columns=FEATURE_ORDER)
    rows["holding_age_missing"] = 0.0
    rows["entry_cost_missing"] = 0.0
    rows["objective"] = np.tile(HEADS, 200)
    rows["net_action_value_bps"] = rows["return_20d_bps"]
    rows["decision_as_of"] = cutoff_on(date(2024, 1, 2))
    rows["label_available_at"] = cutoff_on(date(2024, 2, 2))
    return rows


def _published_research(root: Path) -> str:
    model = fit_local_model(
        _training_rows(),
        cutoff=cutoff_on(date(2024, 3, 29)),
        available_at=datetime.now(TZ),
        source_sha256="1" * 64,
        request_sha256="2" * 64,
        source_commit="a" * 40,
        temporal_mode="LIVE_FINAL_FIT",
    )
    write_local_model(model, timing_root=root)
    bundle = root / "research" / "action_value_v2" / "bundles" / ("2" * 64)
    receipt = {
        "schema_version": "position_timing_action_value_receipt_v2",
        "final_model_sha256": model.metadata["model_sha256"],
        "effect_evidence": "INCONCLUSIVE",
        "population": {"symbols": ["000001.SZ"]},
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    PositionTimingArtifactStore._publish_immutable(
        bundle / "receipt.json", canonical_json_bytes(receipt) + b"\n"
    )
    current = {
        "schema_version": "position_timing_action_value_current_research_v2",
        "model_sha256": model.metadata["model_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "bundle_path": bundle.as_posix(),
        "effect_evidence": "INCONCLUSIVE",
        "advice_tier": "EXPERIMENTAL_MODEL_ADVICE",
        "updated_at": datetime.now(TZ).isoformat(),
    }
    current["state_sha256"] = canonical_sha256(current)
    PositionTimingArtifactStore._atomic_replace(
        root / "research" / "action_value_v2" / "current.json",
        canonical_json_bytes(current) + b"\n",
    )
    return model.metadata["model_sha256"]


def _snapshot(calendar: tuple[date, ...], symbols: list[str], captured_at: datetime) -> dict:
    rows = {}
    for symbol_index, symbol in enumerate(symbols):
        values = {}
        for index, day in enumerate(calendar):
            close = 10 + symbol_index + index * 0.01
            values[day.isoformat()] = {
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1_000_000 + index,
                "adj_factor": 1,
                "feature_available_at": cutoff_on(day).isoformat(),
            }
        rows[symbol] = values
    benchmark_rows = {
        day.isoformat(): {
            "close": 3000 + index,
            "feature_available_at": cutoff_on(day).isoformat(),
        }
        for index, day in enumerate(calendar)
    }
    identity = {
        "source": "TEST_CURRENT_EOD_CAPTURE",
        "rows_sha256": canonical_sha256({"rows": rows, "benchmark_rows": benchmark_rows}),
    }
    identity["identity_sha256"] = canonical_sha256(identity)
    return {
        "rows": rows,
        "benchmark_rows": benchmark_rows,
        "identity": identity,
        "adjustment_identity": {"source": "test-factor"},
        "captured_at": captured_at.isoformat(),
    }


def test_materializes_per_stock_experimental_advice_without_cards_or_alerts(tmp_path: Path) -> None:
    root = tmp_path / "timing"
    model_sha256 = _published_research(root)
    calendar = tuple(pd.bdate_range(end="2026-09-08", periods=80).date)
    decision_as_of = cutoff_on(calendar[-1])
    now = decision_as_of.replace(hour=20, minute=5)
    symbols = ["000001.SZ", "600000.SH"]
    snapshot = _snapshot(calendar, symbols, now)
    result = materialize_model_advice(
        timing_root=root,
        now=now,
        decision_date=calendar[-1],
        decision_as_of=decision_as_of,
        target_date=date(2026, 9, 9),
        calendar=calendar,
        members=(
            {
                "canonical_symbol": "000001.SZ",
                "display_name": "平安银行",
                "primary_source_role": "HOLDING",
                "holding": {"quantity": 1000, "cost_price": 10},
                "intent": {},
            },
            {
                "canonical_symbol": "600000.SH",
                "display_name": "浦发银行",
                "primary_source_role": "WATCHLIST",
                "holding": {},
                "intent": {
                    "planned_full_notional_cny": "120000",
                    "desired_target_exposure": "0.5",
                },
            },
        ),
        snapshot_loader=lambda *_: snapshot,
    )
    assert result["status"] == "MATERIALIZED"
    advice = result["advice_set"]
    assert advice["model_sha256"] == model_sha256
    assert len(advice["items"]) == 2
    assert {item["canonical_symbol"] for item in advice["items"]} == set(symbols)
    assert advice["formal_card_changed"] is False
    assert advice["alert_emitted"] is False and advice["order_created"] is False
    assert advice["items"][0]["sizing_status"] == "DIRECTION_ONLY"
    assert advice["items"][0]["research_population_status"] == "IN_FROZEN_RESEARCH_SAMPLE"
    assert advice["items"][0]["planned_delta_qty"] is None
    assert advice["items"][1]["sizing_status"] == "PERSONALIZED_QUANTITY_ESTIMATE"
    assert advice["items"][1]["research_population_status"] == "OUT_OF_RESEARCH_SAMPLE_EXTRAPOLATION"
    assert advice["items"][1]["candidate_action_values"]
    assert not (root / "cards").exists() and not (root / "events").exists()

    retry = materialize_model_advice(
        timing_root=root,
        now=now,
        decision_date=calendar[-1],
        decision_as_of=decision_as_of,
        target_date=date(2026, 9, 9),
        calendar=calendar,
        members=(),
        snapshot_loader=None,
    )
    assert retry["status"] == "ALREADY_MATERIALIZED"
    assert retry["advice_set"]["advice_sha256"] == advice["advice_sha256"]


def test_get_is_read_only_and_before_cutoff_does_not_capture(tmp_path: Path) -> None:
    root = tmp_path / "timing"
    assert current_model_advice(timing_root=root, now=datetime.now(TZ))["status"] == "NO_MODEL_ADVICE"
    assert not root.exists()
    calendar = tuple(pd.bdate_range(end="2026-09-08", periods=80).date)
    calls = []
    result = materialize_model_advice(
        timing_root=root,
        now=cutoff_on(calendar[-1]).replace(hour=19),
        decision_date=calendar[-1],
        decision_as_of=cutoff_on(calendar[-1]),
        target_date=date(2026, 9, 9),
        calendar=calendar,
        members=(),
        snapshot_loader=lambda *_: calls.append(1),
    )
    assert result["status"] == "DECISION_CUTOFF_NOT_REACHED"
    assert calls == [] and not root.exists()
