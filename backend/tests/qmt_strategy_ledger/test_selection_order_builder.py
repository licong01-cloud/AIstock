from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.models import (
    BindingStatus,
    PositionLotRecord,
    StrategyPackageBinding,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.selection_order_builder import SelectionOrderBuilder, SelectionOrderBuildConfig
from backend.services.selection_center.models import SelectionCandidate, SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.trading_core.errors import DataUnavailableError


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


@dataclass
class FakeSelectionReader:
    run: SelectionRun

    def get_run(self, run_id: str) -> SelectionRun:
        assert run_id == self.run.run_id
        return self.run


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repo


def _account(strategy_id: str, strategy_name: str) -> VirtualAccount:
    return VirtualAccount(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        display_name=f"{strategy_name} display",
        account_id=ACCOUNT_ID,
        mode="SIM",
        initial_cash=Decimal("10000000"),
        cash=Decimal("10000000"),
        status=VirtualAccountStatus.ENABLED,
    )


def _binding(target_weight: Decimal | None = Decimal("0.02"), top_k: int | None = None) -> StrategyPackageBinding:
    return StrategyPackageBinding(
        binding_id="bind_a",
        strategy_id="strat_a",
        package_id="pkg_a",
        manifest_sha256="sha_a",
        selection_run_id="sel_a",
        trade_date=TRADE_DATE,
        target_weight=target_weight,
        top_k=top_k,
        binding_status=BindingStatus.ACTIVE,
    )


def _selection_run(candidates: list[SelectionCandidate], *, status: SelectionRunStatus = SelectionRunStatus.SUCCEEDED) -> SelectionRun:
    return SelectionRun(
        run_id="sel_a",
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        package_ids=["pkg_a"],
        status=status,
        package_results={"pkg_a": candidates},
        manifest_sha256_by_package={"pkg_a": "sha_a"},
        completed_at=None,
    )


def _lot(
    *,
    lot_id: str,
    symbol: str,
    quantity: int,
    available_quantity: int,
    strategy_id: str = "strat_a",
    avg_cost: Decimal = Decimal("10"),
    metadata: dict | None = None,
) -> PositionLotRecord:
    return PositionLotRecord(
        lot_id=lot_id,
        strategy_id=strategy_id,
        symbol=symbol,
        open_trade_id=f"trade_{lot_id}",
        open_date=TRADE_DATE,
        quantity=quantity,
        available_quantity=available_quantity,
        remaining_quantity=quantity,
        avg_cost=avg_cost,
        cost_amount=avg_cost * Decimal(quantity),
        account_id=ACCOUNT_ID,
        metadata=metadata or {},
    )


def test_selection_order_builder_uses_candidate_target_quantity_first() -> None:
    repo = _repo()
    binding = repo.create_package_binding(_binding(target_weight=Decimal("0.02")))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                target_weight=0.02,
                reference_price=123.45,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert len(result.requests) == 1
    request = result.requests[0]
    assert request.quantity == 1000
    assert request.order_type == 23
    assert request.package_id == "pkg_a"
    assert request.selection_run_id == "sel_a"
    assert request.order_remark.startswith("qmtpkg_poc_strategy_a_a_300604SZ")


def test_selection_order_builder_sizes_from_target_weight_and_board_lot() -> None:
    repo = _repo()
    binding = repo.create_package_binding(_binding(target_weight=Decimal("0.02")))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_weight=0.02,
                reference_price=26.31,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert result.requests[0].quantity == 7600
    assert result.requests[0].price == Decimal("26.310000")


def test_selection_order_builder_preserves_star_market_increment_after_minimum() -> None:
    repo = _repo()
    binding = repo.create_package_binding(_binding(target_weight=Decimal("0.02")))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="688379.SH",
                score=0.9,
                rank=1,
                target_weight=0.02,
                reference_price=73.89,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert result.requests[0].quantity == 2706
    assert result.requests[0].price == Decimal("73.890000")


def test_selection_order_builder_uses_current_lots_to_build_sell_delta() -> None:
    repo = _repo()
    repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_a",
            strategy_id="strat_a",
            symbol="300604.SZ",
            open_trade_id="trade_a",
            open_date=TRADE_DATE,
            quantity=1500,
            available_quantity=1500,
            remaining_quantity=1500,
            avg_cost=Decimal("10"),
            cost_amount=Decimal("15000"),
            account_id=ACCOUNT_ID,
        )
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                reference_price=10,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert result.requests[0].side == "SELL"
    assert result.requests[0].order_type == 24
    assert result.requests[0].quantity == 500


def test_selection_order_builder_sells_dropped_holding_with_available_lot() -> None:
    repo = _repo()
    repo.create_position_lot(
        _lot(
            lot_id="lot_drop",
            symbol="300604.SZ",
            quantity=1000,
            available_quantity=1000,
            metadata={"reference_price": "12.34"},
        )
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300054.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                reference_price=20,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    dropped_sell = next(request for request in result.requests if request.symbol == "300604.SZ")
    dropped_sell_payload = next(request for request in result.to_dict()["requests"] if request["symbol"] == "300604.SZ")
    assert dropped_sell.side == "SELL"
    assert dropped_sell.order_type == 24
    assert dropped_sell.quantity == 1000
    assert dropped_sell.price == Decimal("12.340000")
    assert dropped_sell.metadata["rebalance_reason"] == "DROPPED_FROM_SELECTION"
    assert dropped_sell.metadata["target_quantity"] == 0
    assert dropped_sell.metadata["current_quantity"] == 1000
    assert dropped_sell.metadata["available_quantity"] == 1000
    assert dropped_sell_payload["metadata"]["rebalance_reason"] == "DROPPED_FROM_SELECTION"


def test_selection_order_builder_skips_dropped_holding_without_available_lot() -> None:
    repo = _repo()
    repo.create_position_lot(
        _lot(
            lot_id="lot_t0",
            symbol="300604.SZ",
            quantity=1000,
            available_quantity=0,
        )
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300054.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                reference_price=20,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert all(request.symbol != "300604.SZ" for request in result.requests)
    skip = next(item for item in result.skipped if item["symbol"] == "300604.SZ")
    assert skip["reason"] == "NO_AVAILABLE_LOT_FOR_DROPPED_HOLDING"
    assert skip["requested_quantity"] == 1000
    assert skip["available_quantity"] == 0


def test_selection_order_builder_skips_dropped_fixed_price_sell_without_reference_price() -> None:
    repo = _repo()
    repo.create_position_lot(
        _lot(lot_id="lot_no_price", symbol="300604.SZ", quantity=1000, available_quantity=1000)
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300054.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                reference_price=20,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding,
        config=SelectionOrderBuildConfig(price_type=11),
    )

    assert all(request.symbol != "300604.SZ" for request in result.requests)
    skip = next(item for item in result.skipped if item["symbol"] == "300604.SZ")
    assert skip["reason"] == "MISSING_REFERENCE_PRICE_FOR_DROPPED_HOLDING"
    assert skip["price_type"] == 11


def test_selection_order_builder_caps_overweight_sell_to_available_quantity() -> None:
    repo = _repo()
    repo.create_position_lot(
        _lot(lot_id="lot_overweight", symbol="300604.SZ", quantity=1500, available_quantity=500)
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_quantity=0,
                reference_price=10,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert result.requests[0].side == "SELL"
    assert result.requests[0].quantity == 500
    assert result.requests[0].metadata["requested_quantity"] == 1500
    assert result.requests[0].metadata["available_quantity"] == 500
    skip = next(item for item in result.skipped if item["reason"] == "SELL_QUANTITY_CAPPED_BY_AVAILABLE_LOT")
    assert skip["blocked_quantity"] == 1000


def test_selection_order_builder_equal_target_skips_and_below_target_buys_with_board_lot() -> None:
    repo = _repo()
    repo.create_position_lot(
        _lot(lot_id="lot_equal", symbol="300604.SZ", quantity=1000, available_quantity=1000)
    )
    repo.create_position_lot(
        _lot(lot_id="lot_below", symbol="300054.SZ", quantity=950, available_quantity=950)
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(symbol="300604.SZ", score=0.9, rank=1, target_quantity=1000, reference_price=10),
            SelectionCandidate(symbol="300054.SZ", score=0.8, rank=2, target_quantity=1100, reference_price=10),
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert [request.symbol for request in result.requests] == ["300054.SZ"]
    assert result.requests[0].side == "BUY"
    assert result.requests[0].quantity == 100
    assert result.requests[0].metadata["requested_quantity"] == 150
    equal_skip = next(item for item in result.skipped if item["reason"] == "ALREADY_AT_TARGET")
    assert equal_skip["symbol"] == "300604.SZ"
    residual_skip = next(item for item in result.skipped if item["reason"] == "BUY_BOARD_LOT_RESIDUAL_NOT_EMITTED")
    assert residual_skip["residual_quantity"] == 50


def test_selection_order_builder_ignores_other_strategy_same_symbol_lots() -> None:
    repo = _repo()
    repo.create_virtual_account(_account("strat_b", "poc_strategy_b"))
    repo.create_position_lot(
        _lot(lot_id="lot_other_strategy", strategy_id="strat_b", symbol="300604.SZ", quantity=2000, available_quantity=2000)
    )
    binding = repo.create_package_binding(_binding(target_weight=None))
    run = _selection_run(
        [
            SelectionCandidate(
                symbol="300604.SZ",
                score=0.9,
                rank=1,
                target_quantity=1000,
                reference_price=10,
            )
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert len(result.requests) == 1
    assert result.requests[0].side == "BUY"
    assert result.requests[0].quantity == 1000
    assert result.requests[0].strategy_name == "poc_strategy_a"
    assert result.requests[0].order_remark.startswith("qmtpkg_poc_strategy_a_a_300604SZ")


def test_selection_order_builder_honors_top_k_without_broker_call() -> None:
    repo = _repo()
    binding = repo.create_package_binding(_binding(target_weight=Decimal("0.02"), top_k=2))
    run = _selection_run(
        [
            SelectionCandidate(symbol="300604.SZ", score=0.9, rank=1, target_weight=0.02, reference_price=20),
            SelectionCandidate(symbol="300054.SZ", score=0.8, rank=2, target_weight=0.02, reference_price=25),
            SelectionCandidate(symbol="300371.SZ", score=0.7, rank=3, target_weight=0.02, reference_price=30),
        ]
    )

    result = SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(run)).build_for_binding(
        binding=binding
    )

    assert [request.symbol for request in result.requests] == ["300604.SZ", "300054.SZ"]


def test_selection_order_builder_fails_fast_without_price_or_succeeded_run() -> None:
    repo = _repo()
    binding = repo.create_package_binding(_binding(target_weight=Decimal("0.02")))
    missing_price = _selection_run(
        [SelectionCandidate(symbol="300604.SZ", score=0.9, rank=1, target_weight=0.02, reference_price=10)]
    )
    missing_price.package_results["pkg_a"][0] = missing_price.package_results["pkg_a"][0].model_copy(
        update={"reference_price": None}
    )

    with pytest.raises(DataUnavailableError, match="reference_price is required"):
        SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(missing_price)).build_for_binding(
            binding=binding
        )

    failed_run = _selection_run(
        [SelectionCandidate(symbol="300604.SZ", score=0.9, rank=1, target_weight=0.02, reference_price=10)],
        status=SelectionRunStatus.FAILED,
    )
    with pytest.raises(DataUnavailableError, match="selection run is not succeeded"):
        SelectionOrderBuilder(repository=repo, selection_reader=FakeSelectionReader(failed_run)).build_for_binding(
            binding=binding
        )
