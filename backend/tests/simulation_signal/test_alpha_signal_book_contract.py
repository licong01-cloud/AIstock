"""Broker-neutral alpha signal and operator command contracts."""


from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.simulation_runtime import AlphaSignalBook, AlphaSignalItem, OperatorCommand
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
