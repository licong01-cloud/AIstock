"""Verify the native file-calendar receipt at seed and parent boundaries."""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from backend.services.strategy_package.multi_alpha_live import _aggregate_parent_leg_input_context
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.strategy_package.test_multi_alpha_live_selection import (
    TRADE_DATE, _calendar_context, _score_rows, _sha256,
)


def _frozen_calendar_context(binding: str = "a" * 64, *, required_window: int = 61) -> dict:
    """The actual InferenceEngine file-calendar receipt contract."""
    calendar_hash = _sha256({
        "dataset_id": "dataset_files.day_calendar",
        "effective_trade_date": TRADE_DATE.isoformat(),
        "calendar_version": binding,
        "calendar_source": "FROZEN_DATASET_FILES",
    })
    window = {"window_start_date": "2024-03-01", "required_window": required_window,
              "window_resolution": "model_required_window"}
    window_hash = _sha256({"calendar_identity_hash": calendar_hash, **window})
    return {
        "requested_trade_date": TRADE_DATE.isoformat(),
        "effective_trade_date": TRADE_DATE.isoformat(),
        "score_trade_date": TRADE_DATE.isoformat(), "pit_mode": "frozen_candidate",
        "calendar_source": "FROZEN_DATASET_FILES", "calendar_version": binding,
        "calendar_identity_hash": calendar_hash, "frozen_file_binding_hash": binding,
        "calendar_hash": window_hash, "window_lineage_hash": window_hash,
        "universe_input_hash": "b" * 64, **window,
    }


def _aggregate_contexts(*contexts: dict) -> dict:
    return _aggregate_parent_leg_input_context(
        leg_executions={str(i): SimpleNamespace(live_result=SimpleNamespace(
            input_context=context, universe_count=80, scores=_score_rows()))
            for i, context in enumerate(contexts)}, requested_trade_date=TRADE_DATE,
    )


def test_frozen_file_calendar_survives_seed_and_parent_aggregation() -> None:
    context = _aggregate_contexts(_frozen_calendar_context(), _frozen_calendar_context(required_window=260))
    assert context["calendar_version"] == context["frozen_file_binding_hash"] == "a" * 64
    assert context["calendar_source"] == "FROZEN_DATASET_FILES"
    assert context["calendar_identity_hash"] == _frozen_calendar_context()["calendar_identity_hash"]
    assert [v["required_window"] for v in context["per_leg_window_lineage"].values()] == [61, 260]
    # The seed ensemble carries the first seed receipt plus every seed lineage.
    nested = {**_frozen_calendar_context(), "seed_input_context": context}
    parent = _aggregate_contexts(nested, _frozen_calendar_context(required_window=260))
    assert parent["frozen_file_binding_hash"] == context["frozen_file_binding_hash"]
    assert parent["calendar_identity_hash"] == context["calendar_identity_hash"]


@pytest.mark.parametrize("change", [
    {"frozen_file_binding_hash": None},
    {"frozen_file_binding_hash": "b" * 64},
    {"frozen_file_binding_hash": "g" * 64, "calendar_version": "g" * 64},
    {"frozen_file_binding_hash": "A" * 64, "calendar_version": "A" * 64},
    {"calendar_identity_hash": "0" * 64},
    {"calendar_hash": "0" * 64},
    {"window_lineage_hash": "0" * 64},
])
def test_frozen_calendar_rejects_missing_binding_or_tampered_receipt(change: dict) -> None:
    context = _frozen_calendar_context()
    context.update(change)
    with pytest.raises(DataUnavailableError) as error:
        _aggregate_contexts(context)
    assert error.value.context["reason_code"] == "ADVISORY_PHASE0A2C_SOURCE_RECEIPT_INCOMPLETE"


def test_frozen_calendar_rejects_transport_name_used_as_dataset_id() -> None:
    context = _frozen_calendar_context()
    context["calendar_identity_hash"] = _sha256({
        "dataset_id": "FROZEN_DATASET_FILES", "calendar_source": "FROZEN_DATASET_FILES",
        "calendar_version": context["calendar_version"], "effective_trade_date": TRADE_DATE.isoformat(),
    })
    with pytest.raises(DataUnavailableError, match="calendar identity hash is invalid"):
        _aggregate_contexts(context)


def test_frozen_calendar_rejects_individually_valid_different_bindings() -> None:
    with pytest.raises(DataUnavailableError) as error:
        _aggregate_contexts(_frozen_calendar_context(), _frozen_calendar_context("c" * 64))
    assert error.value.context["reason_code"] == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"


def test_database_calendar_receipts_remain_compatible_and_cannot_mix_with_files() -> None:
    context = _frozen_calendar_context()
    context.update(_calendar_context(trade_date=TRADE_DATE, window_start_date="2024-03-01", required_window=61))
    context.pop("frozen_file_binding_hash")
    result = _aggregate_contexts(context, deepcopy(context))
    assert result["calendar_source"] == "market.trading_calendar"
    assert "frozen_file_binding_hash" not in result
    with pytest.raises(DataUnavailableError) as error:
        _aggregate_contexts(context, _frozen_calendar_context())
    assert error.value.context["reason_code"] == "ADVISORY_PHASE0A2C_LINEAGE_MISMATCH"
