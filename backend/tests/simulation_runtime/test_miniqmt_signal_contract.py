from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from backend.services.simulation_runtime import (
    AlphaSignalBook,
    AlphaSignalItem,
    ExecutionPathNotCanonicalError,
    MiniQMTExecutionRuntimeRequest,
    OperatorCommand,
    StrategySlotTarget,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


TRADE_DATE = date(2026, 6, 9)


def _signal_book(**overrides) -> AlphaSignalBook:
    payload = {
        "package_id": "pkg_alpha_signal",
        "manifest_sha256": "manifest_alpha_signal",
        "trade_date": TRADE_DATE,
        "cutoff_date": date(2026, 6, 8),
        "as_of": datetime(2026, 6, 9, 9, 25, tzinfo=UTC),
        "release_id": "srr_alpha_signal",
        "release_hash": "hash_alpha_signal",
        "source_type": "strategy_package",
        "data_source": "TDX_REALTIME",
        "items": [
            AlphaSignalItem(
                symbol="000001.SZ",
                side="BUY",
                rank=1,
                score=0.91,
                target_weight=0.20,
                confidence=0.8,
                reason="daily_top_rank",
                exposures={"industry": "bank"},
            )
        ],
        "risk_tags": {"style": "small_cap"},
        "metadata": {"selection_run_id": "sel_001"},
    }
    payload.update(overrides)
    return AlphaSignalBook(**payload)


def _slot_target(book: AlphaSignalBook, **overrides) -> StrategySlotTarget:
    payload = {
        "account_group_id": "ag_minqmt_main_sim",
        "strategy_slot_id": "slot_alpha_001",
        "strategy_id": "strategy_alpha_001",
        "package_id": book.package_id,
        "alpha_signal_book_id": book.book_id,
        "target_trade_date": book.trade_date,
        "capital_allocation": 1_000_000,
        "desired_weights": {"000001.SZ": 0.20},
        "metadata": {"capacity_model": "funds_and_trading_rules_only"},
    }
    payload.update(overrides)
    return StrategySlotTarget(**payload)


def test_alpha_signal_book_is_canonical_and_broker_neutral() -> None:
    book = _signal_book()

    assert book.book_id == f"asb_{book.signal_hash[:16]}"
    assert book.items[0].signal_id == f"asig_{book.items[0].signal_hash[:16]}"
    dumped = book.model_dump(mode="json", exclude_none=True)
    assert "broker_account_id" not in str(dumped)
    assert "order_remark" not in str(dumped)
    assert dumped["items"][0]["symbol"] == "000001.SZ"


@pytest.mark.parametrize(
    "bad_payload",
    [
        {"broker_account_id": "QMT_SIM_ACCOUNT"},
        {"items": [{"symbol": "000001.SZ", "side": "BUY", "order_remark": "legacy"}]},
        {"metadata": {"nested": {"execution_algo_code": "V25_1_SMALL_CAP"}}},
        {"risk_tags": {"raw_packet": {"order_status": 57}}},
    ],
)
def test_alpha_signal_book_rejects_broker_order_execution_and_native_fields(bad_payload: dict) -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="AlphaSignalBook cannot contain broker") as exc_info:
        _signal_book(**bad_payload)

    assert exc_info.value.context["forbidden_paths"]


def test_runtime_request_accepts_multiple_slots_without_count_gate() -> None:
    book_a = _signal_book()
    book_b = _signal_book(package_id="pkg_alpha_signal_b", manifest_sha256="manifest_alpha_signal_b")
    request = MiniQMTExecutionRuntimeRequest(
        account_group_id="ag_minqmt_main_sim",
        trade_date=TRADE_DATE,
        alpha_signal_books=[book_a, book_b],
        strategy_slot_targets=[
            _slot_target(book_a, strategy_slot_id="slot_alpha_001", strategy_id="strategy_alpha_001"),
            _slot_target(book_b, strategy_slot_id="slot_alpha_002", strategy_id="strategy_alpha_002"),
        ],
    )

    assert request.runtime_owner == "MiniQMTExecutionRuntime"
    assert len(request.strategy_slot_targets) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"runtime_owner": "LegacyMiniQMTSimBackend"},
        {"metadata": {"path_owner": "MiniQMTExecutionBridge", "max_concurrent_packages": 2}},
    ],
)
def test_runtime_request_rejects_noncanonical_path_and_fixed_strategy_count_gate(payload: dict) -> None:
    book = _signal_book()
    base = {
        "account_group_id": "ag_minqmt_main_sim",
        "trade_date": TRADE_DATE,
        "alpha_signal_books": [book],
        "strategy_slot_targets": [_slot_target(book)],
    }
    base.update(payload)

    with pytest.raises((ExecutionPathNotCanonicalError, RuntimeConfigInvalidError, ValidationError)):
        MiniQMTExecutionRuntimeRequest(**base)


def test_operator_command_contract_requires_runtime_audit_inputs() -> None:
    command = OperatorCommand(
        command_type="REPLACE_ALPHA_SIGNAL_BOOK",
        account_group_id="ag_minqmt_main_sim",
        alpha_signal_book_id="asb_replacement",
        requested_by="operator",
        reason="switch to new alpha source after flattening",
    )

    assert command.command_id.startswith("opcmd_")

    with pytest.raises(ValueError, match="strategy_slot_id"):
        OperatorCommand(command_type="RESET_STRATEGY_SLOT", account_group_id="ag_minqmt_main_sim", reason="manual reset")
