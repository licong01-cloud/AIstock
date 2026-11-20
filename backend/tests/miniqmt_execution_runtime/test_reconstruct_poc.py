from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from collections import Counter

from backend.services.qmt_strategy_ledger.models import (
    AnomalyType,
    FrozenCashAction,
    OrderLifecycle,
)
from backend.services.qmt_strategy_ledger.reconstruct import reconstruct_ledger

FIXTURE = Path("backend/tests/qmt_strategy_ledger/fixtures/miniqmt_poc_20260518_summary.json")


def _load_fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_reconstructs_strategy_lots_for_20260518_overlap_poc() -> None:
    payload = _load_fixture()
    snapshot = reconstruct_ledger(
        orders=payload["orders"],
        trades=payload["trades"],
        account_id=payload["metadata"]["account_id"],
        trade_date=payload["metadata"]["trade_date"],
    )

    expected = payload["expected"]["positions"]
    for strategy_name, symbols in expected.items():
        for symbol, quantity in symbols.items():
            assert snapshot.position_quantity(strategy_name, symbol) == quantity

    assert list(snapshot.overlap_symbols) == payload["expected"]["overlap_symbols"]
    assert snapshot.account_id == "62266303"
    assert snapshot.trade_date == "2026-05-18"


def test_rejected_t1_sell_orders_do_not_reduce_reconstructed_lots() -> None:
    payload = _load_fixture()
    snapshot = reconstruct_ledger(
        orders=payload["orders"],
        trades=payload["trades"],
        account_id=payload["metadata"]["account_id"],
        trade_date=payload["metadata"]["trade_date"],
    )

    rejected_sells = [
        order
        for order in snapshot.orders
        if order.order_type == 24 and order.lifecycle == OrderLifecycle.REJECTED
    ]
    assert len(rejected_sells) == 4
    assert all(order.traded_volume == 0 for order in rejected_sells)
    assert snapshot.position_quantity("pocD_A2a9_132553", "001358.SZ") == 7600
    assert snapshot.position_quantity("pocD_Bcfa_132553", "301314.SZ") == 3800


def test_order_status_and_frozen_cash_actions_are_classified() -> None:
    payload = _load_fixture()
    snapshot = reconstruct_ledger(
        orders=payload["orders"],
        trades=payload["trades"],
        account_id=payload["metadata"]["account_id"],
        trade_date=payload["metadata"]["trade_date"],
    )

    assert snapshot.summary()["order_status_counts"] == payload["expected"]["status_counts"]

    open_order = next(order for order in snapshot.orders if order.order_id == "1082167345")
    assert open_order.lifecycle == OrderLifecycle.OPEN
    assert open_order.frozen_cash_action == FrozenCashAction.KEEP_BUY_FREEZE
    assert open_order.estimated_remaining_notional == 199424

    cancelled_orders = [order for order in snapshot.orders if order.lifecycle == OrderLifecycle.CANCELLED]
    assert len(cancelled_orders) == 5
    assert any(order.frozen_cash_action == FrozenCashAction.RELEASE_REMAINING_BUY_FREEZE for order in cancelled_orders)


def test_blank_strategy_and_duplicate_remark_are_reported_as_anomalies() -> None:
    payload = _load_fixture()
    snapshot = reconstruct_ledger(
        orders=payload["orders"],
        trades=payload["trades"],
        account_id=payload["metadata"]["account_id"],
        trade_date=payload["metadata"]["trade_date"],
    )

    anomaly_counts = Counter(item.anomaly_type for item in snapshot.anomalies)
    assert anomaly_counts[AnomalyType.BLANK_STRATEGY_NAME] >= 1
    assert anomaly_counts[AnomalyType.DUPLICATE_ORDER_REMARK] == 2

    blank = [item for item in snapshot.anomalies if item.anomaly_type == AnomalyType.BLANK_STRATEGY_NAME]
    assert any(item.order_id == "1082171355" for item in blank)

    duplicate = [item for item in snapshot.anomalies if item.anomaly_type == AnomalyType.DUPLICATE_ORDER_REMARK]
    assert {item.order_id for item in duplicate} == {"1082171496", "1082171497"}


def test_report_script_writes_json_and_markdown(tmp_path: Path) -> None:
    json_out = tmp_path / "ledger_report.json"
    md_out = tmp_path / "ledger_report.md"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/qmt_strategy_ledger_reconstruct_poc.py",
            "--fixture",
            str(FIXTURE),
            "--out",
            str(json_out),
            "--markdown-out",
            str(md_out),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "orders_count" in result.stdout
    report = json.loads(json_out.read_text(encoding="utf-8"))
    assert report["summary"]["overlap_symbols"] == ["001358.SZ", "301314.SZ"]
    assert md_out.read_text(encoding="utf-8").startswith("# MiniQMT Strategy Ledger Reconstruction Report")
