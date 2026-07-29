from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime
from pathlib import Path
import shutil

import pytest

from backend.execution_algos.vnpy_compat import facade_projection as projection

from backend.execution_algos.vnpy_compat.facade_projection import (
    AlgoStatus,
    ContractData,
    Direction,
    Exchange,
    Offset,
    OrderData,
    OrderType,
    Status,
    TickData,
    TradeData,
    build_pinned_round_to_v1,
    build_vnpy_facade_dto_mappings_v1,
    dto_mapping_set_sha256_v1,
    project_contract_data_v1,
    project_order_status_v1,
    readback_vnpy_facade_dto_mappings_v1,
)
from backend.execution_algos.vnpy_compat.locked_surface import PINNED_SOURCE_ROOT
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    NormalizedOrderStatusV1,
)


def test_projection_dtos_are_exact_frozen_and_enum_backed() -> None:
    tick = TickData(
        vt_symbol="600000.SSE",
        datetime=datetime.fromisoformat("2026-07-29T09:30:00+08:00"),
        bid_price_1=10.0,
        bid_volume_1=100.0,
        ask_price_1=10.01,
        ask_volume_1=200.0,
        last_price=10.0,
        limit_up=11.0,
        limit_down=9.0,
    )
    order = OrderData(
        vt_orderid="mqorder_1",
        status=Status.PARTTRADED,
        traded=100.0,
        price=10.0,
    )
    trade = TradeData(
        vt_orderid="mqorder_1",
        vt_tradeid="mqtrade_1",
        price=10.0,
        volume=100.0,
        datetime=None,
    )
    contract = ContractData(
        symbol="600000",
        exchange=Exchange.SSE,
        gateway_name="MINIQMT_SIM",
        min_volume=100.0,
        pricetick=0.01,
    )

    assert tick.vt_symbol == "600000.SSE"
    assert order.is_active() is True
    assert trade.datetime is None
    assert contract.exchange is Exchange.SSE
    assert Direction.LONG.value == "多"
    assert Offset.NONE.value == ""
    assert OrderType.LIMIT.value == "限价"
    assert AlgoStatus.RUNNING.value == "运行"
    with pytest.raises(FrozenInstanceError):
        tick.last_price = 9.9  # type: ignore[misc]
    with pytest.raises(TypeError):
        TickData(vt_symbol="600000.SSE")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("normalized", "expected"),
    [
        (NormalizedOrderStatusV1.ACCEPTED, Status.NOTTRADED),
        (NormalizedOrderStatusV1.PARTIALLY_FILLED, Status.PARTTRADED),
        (NormalizedOrderStatusV1.FILLED, Status.ALLTRADED),
        (NormalizedOrderStatusV1.CANCELLED, Status.CANCELLED),
        (NormalizedOrderStatusV1.REJECTED, Status.REJECTED),
    ],
)
def test_order_status_projection_is_exact(
    normalized: NormalizedOrderStatusV1,
    expected: Status,
) -> None:
    assert project_order_status_v1(normalized) is expected


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [("600000.SH", Exchange.SSE), ("000001.SZ", Exchange.SZSE), ("430047.BJ", Exchange.BSE)],
)
def test_contract_projection_uses_exact_exchange_table(symbol: str, expected: Exchange) -> None:
    contract = project_contract_data_v1(
        symbol=symbol,
        gateway_name="MINIQMT_SIM",
        min_volume="100",
        pricetick_decimal="0.01",
    )
    assert contract.exchange is expected
    assert contract.symbol == symbol[:6]


def test_round_to_is_built_from_pinned_ast_and_rejects_invalid_target() -> None:
    round_to = build_pinned_round_to_v1()

    assert round_to(149.0, 100.0) == 100.0
    assert round_to(150.0, 100.0) == 200.0
    assert round_to(250.0, 100.0) == 200.0
    with pytest.raises(ValueError, match="finite positive"):
        round_to(100.0, 0.0)
    with pytest.raises(TypeError, match="bool"):
        round_to(True, 100.0)


def test_projection_dtos_fail_loud_on_malformed_values() -> None:
    aware = datetime.fromisoformat("2026-07-29T09:30:00+08:00")
    tick_values = {
        "vt_symbol": "600000.SSE",
        "datetime": aware,
        "bid_price_1": 10,
        "bid_volume_1": 100,
        "ask_price_1": 10.01,
        "ask_volume_1": 100,
        "last_price": 10,
        "limit_up": 11,
        "limit_down": 9,
    }
    for update in (
        {"vt_symbol": ""},
        {"datetime": datetime(2026, 7, 29, 9, 30)},
        {"bid_price_1": "10"},
        {"bid_price_1": float("inf")},
        {"bid_volume_1": -1},
    ):
        with pytest.raises((TypeError, ValueError)):
            TickData(**{**tick_values, **update})

    for values in (
        {"vt_orderid": "", "status": Status.NOTTRADED, "traded": 0, "price": 10},
        {"vt_orderid": "order", "status": "NOTTRADED", "traded": 0, "price": 10},
        {"vt_orderid": "order", "status": Status.NOTTRADED, "traded": -1, "price": 10},
        {"vt_orderid": "order", "status": Status.NOTTRADED, "traded": 0, "price": 0},
    ):
        with pytest.raises((TypeError, ValueError)):
            OrderData(**values)  # type: ignore[arg-type]
    assert OrderData("order", Status.ALLTRADED, 1, 10).is_active() is False

    for values in (
        {"vt_orderid": "", "vt_tradeid": "trade", "price": 10, "volume": 1, "datetime": None},
        {"vt_orderid": "order", "vt_tradeid": "", "price": 10, "volume": 1, "datetime": None},
        {
            "vt_orderid": "order",
            "vt_tradeid": "trade",
            "price": 10,
            "volume": 1,
            "datetime": datetime(2026, 7, 29),
        },
    ):
        with pytest.raises((TypeError, ValueError)):
            TradeData(**values)

    for values in (
        {"symbol": "60000", "exchange": Exchange.SSE, "gateway_name": "qmt", "min_volume": 100, "pricetick": 0.01},
        {"symbol": "600000", "exchange": "SSE", "gateway_name": "qmt", "min_volume": 100, "pricetick": 0.01},
        {"symbol": "600000", "exchange": Exchange.SSE, "gateway_name": " qmt", "min_volume": 100, "pricetick": 0.01},
    ):
        with pytest.raises((TypeError, ValueError)):
            ContractData(**values)  # type: ignore[arg-type]


def test_projection_public_readback_and_source_failures_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TypeError, match="NormalizedOrderStatusV1"):
        project_order_status_v1("ACCEPTED")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="not normalized"):
        project_contract_data_v1(symbol="600000", gateway_name="qmt", min_volume="100", pricetick_decimal="0.01")
    with pytest.raises(ValueError, match="unsupported"):
        project_contract_data_v1(symbol="600000.HK", gateway_name="qmt", min_volume="100", pricetick_decimal="0.01")
    with pytest.raises(TypeError, match="pathlib.Path"):
        build_pinned_round_to_v1(str(PINNED_SOURCE_ROOT))  # type: ignore[arg-type]

    source_root = tmp_path / "pinned"
    shutil.copytree(PINNED_SOURCE_ROOT, source_root)
    utility = source_root / "vnpy_core/vnpy/trader/utility.py"
    utility.unlink()
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        build_pinned_round_to_v1(source_root)
    shutil.copy2(PINNED_SOURCE_ROOT / "vnpy_core/vnpy/trader/utility.py", utility)
    utility.write_bytes(utility.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        build_pinned_round_to_v1(source_root)

    utility.write_bytes(b"not valid python \\x80")
    monkeypatch.setattr(projection, "_UTILITY_SIZE", len(utility.read_bytes()))
    monkeypatch.setattr(projection, "_UTILITY_SHA256", __import__("hashlib").sha256(utility.read_bytes()).hexdigest())
    with pytest.raises(ValueError, match="cannot be parsed"):
        build_pinned_round_to_v1(source_root)

    mappings = build_vnpy_facade_dto_mappings_v1()
    assert readback_vnpy_facade_dto_mappings_v1([item.model_dump(mode="python") for item in mappings]) == mappings
    with pytest.raises(TypeError, match="tuple or JSON list"):
        readback_vnpy_facade_dto_mappings_v1({})
    with pytest.raises(ValueError, match="unique and sorted"):
        dto_mapping_set_sha256_v1((mappings[1], mappings[0], *mappings[2:]))
    with pytest.raises(ValueError, match="DTO_MAPPING_INVALID"):
        readback_vnpy_facade_dto_mappings_v1([item.model_dump(mode="python") for item in mappings[:-1]])
