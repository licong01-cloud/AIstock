from __future__ import annotations

import ast
from datetime import date
from pathlib import Path

import pytest

from backend.services.simulation_runtime import (
    AlphaSignalBook,
    AlphaSignalItem,
    ExecutionPathNotCanonicalError,
    MiniQMTExecutionRuntimeRequest,
    StrategySlotTarget,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


TRADE_DATE = date(2026, 6, 9)


def _book(package_id: str = "pkg_path_unique") -> AlphaSignalBook:
    return AlphaSignalBook(
        package_id=package_id,
        manifest_sha256=f"manifest_{package_id}",
        trade_date=TRADE_DATE,
        cutoff_date=date(2026, 6, 8),
        source_type="strategy_package",
        items=[AlphaSignalItem(symbol="000001.SZ", side="BUY", rank=1, target_weight=0.5)],
    )


def _target(book: AlphaSignalBook, slot: str) -> StrategySlotTarget:
    return StrategySlotTarget(
        account_group_id="ag_minqmt_main_sim",
        strategy_slot_id=slot,
        strategy_id=f"strategy_{slot}",
        package_id=book.package_id,
        alpha_signal_book_id=book.book_id,
        target_trade_date=book.trade_date,
        capital_allocation=500_000,
        desired_weights={"000001.SZ": 0.5},
    )


def test_n1_and_n_many_share_the_same_runtime_request_contract() -> None:
    one_book = _book("pkg_one")
    many_books = [_book("pkg_a"), _book("pkg_b"), _book("pkg_c")]

    single = MiniQMTExecutionRuntimeRequest(
        account_group_id="ag_minqmt_main_sim",
        trade_date=TRADE_DATE,
        alpha_signal_books=[one_book],
        strategy_slot_targets=[_target(one_book, "slot_one")],
    )
    multi = MiniQMTExecutionRuntimeRequest(
        account_group_id="ag_minqmt_main_sim",
        trade_date=TRADE_DATE,
        alpha_signal_books=many_books,
        strategy_slot_targets=[_target(book, f"slot_{index}") for index, book in enumerate(many_books, start=1)],
    )

    assert single.runtime_owner == multi.runtime_owner == "MiniQMTExecutionRuntime"
    assert single.schema_version == multi.schema_version == "miniqmt_execution_runtime_request_v1"
    assert len(single.strategy_slot_targets) == 1
    assert len(multi.strategy_slot_targets) == 3


@pytest.mark.parametrize(
    "runtime_owner",
    ["MiniQMTSimBackend", "MiniQMTExecutionBridge", "raw_qmt_order", "PaperV2DayRunner"],
)
def test_noncanonical_miniqmt_product_path_is_rejected(runtime_owner: str) -> None:
    book = _book()

    with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
        MiniQMTExecutionRuntimeRequest(
            runtime_owner=runtime_owner,
            account_group_id="ag_minqmt_main_sim",
            trade_date=TRADE_DATE,
            alpha_signal_books=[book],
            strategy_slot_targets=[_target(book, "slot_one")],
        )

    assert exc_info.value.error_code == "EXECUTION_PATH_NOT_CANONICAL"
    assert exc_info.value.context["required_runtime_owner"] == "MiniQMTExecutionRuntime"


def test_fixed_strategy_count_product_gate_is_rejected_at_runtime_contract_boundary() -> None:
    book = _book()

    with pytest.raises(RuntimeConfigInvalidError, match="strategy count must be governed") as exc_info:
        MiniQMTExecutionRuntimeRequest(
            account_group_id="ag_minqmt_main_sim",
            trade_date=TRADE_DATE,
            alpha_signal_books=[book],
            strategy_slot_targets=[_target(book, "slot_one")],
            metadata={"max_concurrent_packages": 2},
        )

    assert exc_info.value.context["allowed_gate"] == "funds_and_trading_rules_only"


def test_product_route_has_no_paper_or_raw_qmt_broker_write_owner() -> None:
    retired_product_modules = (
        Path("backend/routers/qmt.py"),
        Path("backend/services/paper_trading_v2/broker/minqmtsim.py"),
        Path("backend/services/paper_trading_v2/day_runner.py"),
    )
    forbidden_attribute_calls = {"place_order", "cancel_order"}
    violations: list[str] = []
    for path in retired_product_modules:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in forbidden_attribute_calls
            ):
                receiver = ast.unparse(node.func.value)
                if "qmt" in receiver.lower() or "_get_client" in receiver:
                    violations.append(
                        f"{path}:{node.lineno}:{receiver}.{node.func.attr}"
                    )
    assert violations == []

    day_runner_source = retired_product_modules[2].read_text(encoding="utf-8")
    assert "execute_paper_vnpy_intent" not in day_runner_source
    assert "MINIQMT_VNPY_STYLE_EXECUTION_COMPLETED" not in day_runner_source

    gateway_source = Path(
        "backend/services/miniqmt_execution_runtime/gateway.py"
    ).read_text(encoding="utf-8")
    assert 'getattr(self.qmt_client, "place_order", None)' in gateway_source
    assert 'getattr(self.qmt_client, "cancel_order", None)' in gateway_source
